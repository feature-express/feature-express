use std::collections::BTreeMap;
use std::iter::FromIterator;
use std::ops::{Add, Sub};
use std::str::FromStr;
use std::sync::Arc;

use crate::algo::is_sorted::is_sorted;
use crate::map::{HashMap, HashSet};
use anyhow::{anyhow, bail, Context, Result};
use chrono::{Duration, NaiveDateTime};
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use vec1::{vec1, Vec1};

use crate::ast::core::Expr;
use crate::event::{EntityID, EntityType, Event};
use crate::event_store::EventStore;
use crate::interval::DatePart;
use crate::map::Entry;
use crate::types::{Entities, EventID, Timestamp};
use crate::vec1::Vec1Wrapper;
use schemars::{schema_for, JsonSchema};

/// TODO: rewrite observation dates
///
/// The whole structure of observation dates must be changed.
/// It should be simplified where observation dates are generated ahead of time
/// the output of observation dates is simple it is:
///
/// ```rust
/// pub type ObsDateOutput = HashMap<Entities, Vec<ObservationTime>>;
/// ```
///
/// where ObservationDate is
///
/// ```rust
/// struct ObservationTime {
///    datetime: NaiveDateTime,
///    event_id: Option<EventID>
/// }
/// ```
///
/// If the EventID is passed then the event is available using @ prefix.
///
/// Materialization takes memory but is so much easier than the generators.
///
/// Also there is another thing worth mentioning is the default scope of the analyzed events
/// The scope can be such that we always take all the previous events as the history.
/// This is perhaps the best generic approach because it allows us to write things like this
///
/// Extract the category for the movie entity
///
/// LAST(movie.category) OVER past WHERE event_type = "movie" and entities.movie_id = @entities.movie_id as last_movie_category,
/// VALUES(entities.movie_id) OVER last 1 year WHERE event_type = "movie" and movie.category = last_movie_category as same_category_of_movies,
/// AVG(box_office) OVER last 1 year WHERE event_type = "box_office" and entities.movie_id IN same_category_of_movies,
///
/// Make it a parameter of more complex query
///
/// AVG(boxoffice) OVER last 1 year WHERE event_type = "box_office" and
///
/// Notes from chatgpt about caching
///
/// This is a classic scenario where a graph data structure would be useful. Each node of your
/// graph represents a query, and the directed edges represent dependencies between the queries.
/// Once you have this graph, you can perform a topological sort to determine a valid order of
/// execution for your queries.
///
/// In this sorted graph, for each node:
///
/// - If it's marked with @ (depends on a specific entity), execute the query without caching.
/// - If it's not marked with @, execute the query and cache the result. The cache key would be
///   the combination of the literal values that the query depends on.

#[derive(Clone, Debug)]
pub struct ObservationDates {
    pub inner: HashMap<Entities, Vec1Wrapper<ObservationTime>>,
}

#[derive(
    Clone, Hash, PartialEq, Eq, Debug, PartialOrd, Ord, Serialize, Deserialize, JsonSchema,
)]
pub struct ObservationTime {
    pub datetime: NaiveDateTime,
    pub event_id: Option<EventID>,
}

impl From<NaiveDateTime> for ObservationTime {
    fn from(value: NaiveDateTime) -> Self {
        Self {
            datetime: value,
            event_id: None,
        }
    }
}

#[derive(Clone, Debug)]
pub struct ObsDate {
    pub inner: Vec1<ObservationTime>,
}

impl ObsDate {
    pub fn get_dates(&self) -> Vec<Timestamp> {
        self.inner.iter().map(|e| e.datetime).collect_vec()
    }
}

/// Common trait for Observation Dates
pub trait ObservationDatesT {
    fn get_datetimes(
        &self,
        entites_id: &Entities,
        events: &[Arc<Event>],
    ) -> Result<Vec<ObservationTime>>;
}

/// We can provide observation dates either as a generator or an explicit map of entity -> timestamps
/// When it comes to observation dates they are very important from the standpoint of
/// feature engineering in general. Basically what we have here are 2 ways of looking at the problem
///
/// 1) Entity-based - We want to model things independently on events. For example a customer that does
///    transactions is not a regular time-series generator but more sparse type of
///    data. In cases like these we are just saying. Pick some dates throughout the customer
///    history and try to predict things into the future
///
/// 2) Event-based - We want to model things that are the events themselves - or at least they
///    are avaiable in the evaluation context.
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
#[allow(dead_code)]
pub enum ObservationDatesConfig {
    // aggregation key = HashSet<EntityTypes>
    Interval(Interval),
    Fixed(Fixed),
    EntitySpecific(EntitySpecific),
    // aggregation key = None
    // this one is for extracting the observation time with entities ids
    AllEvents,
    AllEventsByEntity(HashSet<EntityType>),
    // this variant also is for entites but it uses some condition to filter the events
    ConditionalEvents(ConditionalEvents),
    EntitiesEventSpecific(EntitiesEventSpecific),
}

impl ObservationDatesConfig {
    // Helper function to handle the common pattern of inserting ObservationTime into HashMap
    fn insert_into_dates(
        obs_dates: &mut HashMap<Entities, Vec1Wrapper<ObservationTime>>,
        entities: Entities,
        obs_time: ObservationTime,
    ) {
        let entry = obs_dates.entry(entities.clone());
        match entry {
            Entry::Occupied(mut e) => {
                let v = e.get_mut();
                v.0.push(obs_time);
            }
            Entry::Vacant(e) => {
                e.insert(Vec1Wrapper(vec1![obs_time]));
            }
        }
    }

    pub fn materialize_observation_dates(
        &mut self,
        event_store: Box<dyn EventStore>,
    ) -> Result<ObservationDates> {
        let mut observation_dates = match self {
            ObservationDatesConfig::ConditionalEvents(conditional) => {
                let condition_expr = Expr::from_str(&conditional.condition)
                    .map_err(|_| anyhow!("Error parsing condition"))
                    .context(format!(
                        "Cannot parse conditional expressiong {}",
                        conditional.condition
                    ))?;
                let hm = HashMap::new();
                let events = event_store.filter_events(&Box::new(condition_expr), &hm)?;
                let mut hm: HashMap<Entities, Vec1Wrapper<ObservationTime>> = HashMap::new();
                for event in events {
                    let obs_time = ObservationTime {
                        datetime: event.event_time,
                        event_id: event.event_id.clone(),
                    };
                    Self::insert_into_dates(&mut hm, event.entities.clone(), obs_time);
                }
                ObservationDates { inner: hm }
            }
            ObservationDatesConfig::Interval(interval) => {
                let entities = event_store.get_entities(&None);
                let mut obs_dates: HashMap<Entities, Vec1Wrapper<ObservationTime>> = HashMap::new();
                for entity in entities {
                    let entities = btreemap!(entity.typ.clone() => EntityID(entity.id.clone()));
                    if let Some(events) = event_store.query_entity(&entities, None) {
                        if let (Some(first), Some(last)) = (events.first(), events.last()) {
                            // add and sub 1 millisecond to always include all events
                            let first_ts = first.0.sub(Duration::milliseconds(1));
                            let last_ts = last.0.add(Duration::milliseconds(1));
                            let datetimes = interval.generate_from_start_end(first_ts, last_ts);
                            for datetime in datetimes {
                                let obs_time = ObservationTime {
                                    datetime,
                                    event_id: None,
                                };
                                Self::insert_into_dates(&mut obs_dates, entities.clone(), obs_time);
                            }
                        }
                    }
                }
                ObservationDates { inner: obs_dates }
            }
            ObservationDatesConfig::Fixed(fixed) => {
                let entities = event_store.get_entities(&None);
                let mut obs_dates: HashMap<Entities, Vec1Wrapper<ObservationTime>> = HashMap::new();
                for entity in entities {
                    if fixed.entity_types.contains(&entity.typ) {
                        for dt in &fixed.dates {
                            let entities =
                                btreemap!(entity.typ.clone() => EntityID(entity.id.clone()));
                            let obs_time = ObservationTime {
                                datetime: *dt,
                                event_id: None,
                            };
                            Self::insert_into_dates(&mut obs_dates, entities.clone(), obs_time);
                        }
                    }
                }
                ObservationDates { inner: obs_dates }
            }
            ObservationDatesConfig::EntitySpecific(entity_specific) => ObservationDates {
                inner: entity_specific.dates.clone(),
            },
            ObservationDatesConfig::EntitiesEventSpecific(ent) => {
                let mut obs_dates: HashMap<Entities, Vec1Wrapper<ObservationTime>> = HashMap::new();
                for event in event_store.all_events_sorted().iter().flatten() {
                    if let Some(dates) = ent.dates.get(&event.entities) {
                        let event_ids = dates.iter().map(|e| e.1.clone()).collect_vec();
                        let event_id = event.event_id.clone().ok_or(anyhow!(
                            "Event must have an ID to use specific events as observation dates"
                        ))?;
                        if event_ids.contains(&event_id) {
                            let obs_time = ObservationTime {
                                datetime: event.event_time,
                                event_id: Some(event_id),
                            };
                            Self::insert_into_dates(
                                &mut obs_dates,
                                event.entities.clone(),
                                obs_time,
                            );
                        }
                    }
                }
                ObservationDates { inner: obs_dates }
            }
            ObservationDatesConfig::AllEvents => {
                let mut obs_dates: HashMap<Entities, Vec1Wrapper<ObservationTime>> = HashMap::new();
                for event in event_store.all_events_sorted().iter().flatten() {
                    let obs_time = ObservationTime {
                        datetime: event.event_time,
                        event_id: Some(event.event_id.clone().ok_or(anyhow!(
                            "To use this observation type the events should have an event_id"
                        ))?),
                    };
                    Self::insert_into_dates(&mut obs_dates, event.entities.clone(), obs_time);
                }
                ObservationDates { inner: obs_dates }
            }
            ObservationDatesConfig::AllEventsByEntity(entity_types) => {
                let mut obs_dates: HashMap<Entities, Vec1Wrapper<ObservationTime>> = HashMap::new();
                for event in event_store.all_events_sorted().iter().flatten() {
                    let event_entity_types = HashSet::from_iter(event.entities.keys().cloned());
                    if event_entity_types == *entity_types {
                        let obs_time = ObservationTime {
                            datetime: event.event_time,
                            event_id: Some(event.event_id.clone().ok_or(anyhow!(
                                "To use this observation type the events should have an event_id"
                            ))?),
                        };
                        Self::insert_into_dates(&mut obs_dates, event.entities.clone(), obs_time);
                    }
                }
                ObservationDates { inner: obs_dates }
            }
        };

        #[cfg(test)]
        {
            self.check_if_sorted(&mut observation_dates)?;
        }

        // println!("Observation dates {:?}", observation_dates);

        Ok(observation_dates)
    }

    fn check_if_sorted(&self, observation_dates: &mut ObservationDates) -> Result<()> {
        for (entities, obs_dt) in observation_dates.inner.iter() {
            if !is_sorted(obs_dt.0.iter()) {
                bail!("The output of {:?} is not sorted", self);
            }
        }
        Ok(())
    }
}

/// This type of observation dates means that the observation dates are based on a specific
/// condition based on the where clause applied to the [Event]. Probably the proper way to
/// handle this type of thing. Is that the user provides the where clause and
/// we only compile it when we need to calculate the features.
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
pub struct ConditionalEvents {
    pub entity_types: HashSet<EntityType>,
    pub condition: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
pub struct Fixed {
    pub entity_types: HashSet<EntityType>,
    pub dates: Vec<Timestamp>,
}

impl Fixed {
    pub fn new_from_str_vec(entity_types: HashSet<EntityType>, dates: Vec<String>) -> Self {
        Self {
            entity_types,
            dates: dates
                .iter()
                .map(|s| NaiveDateTime::from_str(s).expect("Cannot read string as date"))
                .collect(),
        }
    }
}

impl ObservationDatesT for Fixed {
    fn get_datetimes(&self, _: &Entities, _: &[Arc<Event>]) -> Result<Vec<ObservationTime>> {
        Ok(self.dates.iter().map(|dt| (*dt).into()).collect())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
pub struct EntitySpecific {
    pub dates: HashMap<Entities, Vec1Wrapper<ObservationTime>>,
}

/// This struct represents the choice of specific events that will serve
/// as the source for the observation dates and also be a source of the event.
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
pub struct EntitiesEventSpecific {
    pub dates: HashMap<BTreeMap<EntityType, EntityID>, Vec<(Timestamp, EventID)>>,
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
pub struct Interval {
    pub entity_types: HashSet<EntityType>,
    pub date_part: DatePart,
    pub nth: i64,
}

impl Interval {
    fn generate_from_start_end(
        &self,
        start_dt: NaiveDateTime,
        end_dt: NaiveDateTime,
    ) -> Vec<NaiveDateTime> {
        let mut dt = start_dt;
        let mut datetimes = vec![];
        while dt < end_dt {
            datetimes.push(dt);
            dt = match self.date_part {
                DatePart::Millisecond => dt.add(Duration::milliseconds(self.nth)),
                DatePart::Second => dt.add(Duration::seconds(self.nth)),
                DatePart::Minute => dt.add(Duration::minutes(self.nth)),
                DatePart::Hour => dt.add(Duration::hours(self.nth)),
                DatePart::Day => dt.add(Duration::days(self.nth)),
                DatePart::Week => dt.add(Duration::weeks(self.nth)),
                DatePart::All => panic!("NBetween cannot be used with undefined end date"),
            }
        }
        datetimes.push(end_dt);
        datetimes
    }
}

#[cfg(test)]
mod tests {
    use chrono::{TimeZone, Utc};

    use super::*;

    #[test]
    fn test_n_between() {
        let nbetween = Interval {
            entity_types: Default::default(),
            date_part: DatePart::Day,
            nth: 10,
        };
        let start_dt = Utc.ymd(2021, 1, 1).and_hms(0, 0, 0).naive_utc();
        let end_dt = Utc.ymd(2021, 2, 1).and_hms(0, 0, 0).naive_utc();
        let datetimes = nbetween.generate_from_start_end(start_dt, end_dt);
        assert!(datetimes.len() == 5);
    }
}
