use crate::map::HashMap;
use crate::sstring::SmallString;
use chrono::{Duration, NaiveDateTime, Utc};
use rand::seq::SliceRandom;
use rand::Rng;

use crate::event::{AttributeName, Entity, Event, EventType};
use crate::value::Value;

pub fn random_datetime_within_last_year() -> NaiveDateTime {
    let mut rng = rand::thread_rng();
    let days_in_past: i64 = rng.gen_range(0..365);
    let secs_in_day: i64 = rng.gen_range(0..86400);
    let duration = Duration::days(days_in_past) + Duration::seconds(secs_in_day);
    let datetime = Utc::now().naive_utc() - duration;
    datetime
}

fn random_attrs(
    event_type: &str,
    rng: &mut rand::rngs::ThreadRng,
) -> HashMap<AttributeName, Value> {
    // Generate attributes based on the event_type
    match event_type {
        "LoginEvent" => hashmap![
            "loginTime".into() => Value::DateTime(random_datetime_within_last_year()),
            "device".into() => Value::Map(hashmap![
                "deviceType".into() => Value::Str("Mobile".into()),
                "screenSize".into() => Value::VecInt(vec![1080, 2400]),
                "browser".into() => Value::Str("Chrome".into())
            ].into_iter().map(|(k, v)| (k, Box::new(v))).collect())
        ],
        "LogoutEvent" => hashmap![
            "logoutTime".into() => Value::DateTime(random_datetime_within_last_year()),
            "clientInfo".into() => Value::Map(hashmap![
                "clientID".into() => Value::Str("Client001".into()),
                "clientOS".into() => Value::Str("Android".into())
            ].into_iter().map(|(k, v)| (k, Box::new(v))).collect())
        ],
        "PurchaseEvent" => hashmap![
            "purchaseAmount".into() => Value::Num(rng.gen_range(10.0..100.0)),
            "itemsBought".into() => Value::VecStr(vec!["ItemA".into(), "ItemB".into(), "ItemC".into()]),
            "purchaseTime".into() => Value::DateTime(random_datetime_within_last_year())
        ],
        "ViewEvent" => hashmap![
            "viewedItem".into() => Value::Str("Item".into()),
            "viewTime".into() => Value::DateTime(random_datetime_within_last_year()),
            "viewStats".into() => Value::MapNum(hashmap![
                "viewDuration".into() => rng.gen_range(5.0..15.0),
                "viewCount".into() => rng.gen_range(1.0..10.0)
            ])
        ],
        _ => HashMap::new(),
    }
}

pub fn generate_random_events(n: usize) -> Vec<Event> {
    let mut rng = rand::thread_rng();

    // Define a limited pool of IDs
    let entity_ids_pool: Vec<_> = (1..=1)
        .map(|id| Entity {
            typ: "user".into(),
            id: SmallString::from(format!("ID{:03}", id)),
        })
        .collect();

    // Define distinct event types
    let event_types_pool: Vec<SmallString> =
        vec!["LoginEvent", "LogoutEvent", "PurchaseEvent", "ViewEvent"]
            .into_iter()
            .map(Into::into)
            .collect();

    // Generate a Vec<Event> with n events
    let mut events = Vec::with_capacity(n);
    for _ in 0..n {
        // Randomly pick an entity_id
        let entity_id = entity_ids_pool.choose(&mut rng).unwrap().clone();

        // Randomly pick an event type
        let event_type = event_types_pool.choose(&mut rng).unwrap().clone();

        // Generate event_time
        let event_time = random_datetime_within_last_year();

        // Generate attrs based on the event_type
        let attrs = random_attrs(event_type.as_str(), &mut rng);

        // Create the Event
        let event = Event {
            event_type: EventType(event_type),
            event_time: event_time,
            entities: btreemap![entity_id.typ.into() => entity_id.id.into()],
            event_id: Some(format!("EventID{:03}", rng.gen_range(1..100)).into()),
            experiment_id: None,
            attrs: Some(attrs),
        };

        events.push(event);
    }

    events
}

#[cfg(test)]
mod tests {
    use crate::map::HashSet;
    use itertools::iproduct;

    use crate::dataframe::DataFrame;
    use crate::eval::EvalContext;
    use crate::event::EntityType;
    use crate::event_index::{EventContext, EventScopeConfig, QueryConfig, RawQuery};
    use crate::event_store::EventStore;
    use crate::interval::DatePart;
    use crate::obs_dates::{ConditionalEvents, Fixed, Interval, ObsDate, ObservationDatesConfig};

    use super::*;

    #[test]
    fn test_event_based_mode() {}

    // #[test]
    // fn test_various_features() {
    //     EventStoreSettings { parallel: false, include_events_on_obs_date: false }
    //     let mut event_context = generate_event_context();
    //     let features = event_context.extract_records_from_expr(
    //         ObservationDates::Conditional(Conditional{condition: "event_type = 'PurchaseEvent'".to_string()}),
    //         // ObservationDates::Interval(Interval { date_part: DatePart::Day, nth: 30 }),
    //         vec![
    //             "obs_dt".into(),
    //             "entity_id".into(),
    //             "last(event_time) OVER past".into(),
    //             "first(event_time) OVER future".into(),
    //             "last(event_time) OVER past WHERE event_type = 'PurchaseEvent'".into(),
    //             "first(event_time) OVER future WHERE event_type = 'PurchaseEvent'".into(),
    //             "COUNT(*) OVER past WHERE event_type = 'PurchaseEvent'".into(),
    //             "COUNT(*) OVER future WHERE event_type = 'PurchaseEvent'".into(),
    //             "sum(purchaseAmount) over future where event_type = 'PurchaseEvent' as target".into(),
    //             "sum(1) over last 3 days where event_type = 'PurchaseEvent' ".into(),
    //             "last(event_time) over last 3 days where event_type = 'ViewEvent' group by viewedItem".into(),
    //             "last(event_time) over last 3 days where event_type = 'ViewEvent' group by viewedItem".into(),
    //             "sum(purchaseAmount) over last 3 days where event_type = 'PurchaseEvent'".into(),
    //             "avg(purchaseAmount) over last 3 days where event_type = 'PurchaseEvent'".into(),
    //             "count(*) over last 10 days where event_type = 'LoginEvent' as f1".into(),
    //             "count(*) over last 10 days where event_type = 'LoginEvent' group by device.deviceType".into(),
    //             "count(*) over last 10 days where event_type = 'LoginEvent' group by device.browser".into(),
    //             "last(loginTime) over last 10 days where event_type = 'LoginEvent' as f4".into(),
    //             "last(loginTime) over last 10 days where event_type = 'LoginEvent' group by device.deviceType".into(),
    //             "last(loginTime) over last 10 days where event_type = 'LoginEvent' group by device.browser".into()
    //         ],
    //         None,
    //         None
    //     ).unwrap();
    //
    //     println!("{:?}", features.1.first().unwrap());
    //     let dataframe = DataFrame::new(features.0, features.1);
    //     dataframe.head(100).display();
    // }

    /// Tests whether the counts of features that are calculated for the past and future
    /// sum up to the total events.
    #[test]
    fn test_consistent_counts() {
        let settings_vec = vec![
            QueryConfig {
                parallel: false,
                include_events_on_obs_date: false,
            },
            QueryConfig {
                parallel: true,
                include_events_on_obs_date: false,
            },
            QueryConfig {
                parallel: false,
                include_events_on_obs_date: true,
            },
            QueryConfig {
                parallel: true,
                include_events_on_obs_date: true,
            },
        ];

        let mut entity_types = HashSet::new();
        entity_types.insert(EntityType("user".into()));

        let obs_dates_vec = vec![
            ObservationDatesConfig::Interval(Interval {
                entity_types: entity_types.clone(),
                date_part: DatePart::Day,
                nth: 7,
            }),
            ObservationDatesConfig::Interval(Interval {
                entity_types: entity_types.clone(),
                date_part: DatePart::Day,
                nth: 30,
            }),
            ObservationDatesConfig::Interval(Interval {
                entity_types: entity_types.clone(),
                date_part: DatePart::Day,
                nth: 999999,
            }),
        ];

        let entity_query = EventScopeConfig::RelatedEntitiesEvents(vec![EntityType("user".into())]);
        let query_config = QueryConfig::default();
        for (settings, obs_dates) in iproduct!(settings_vec.iter(), obs_dates_vec.iter()) {
            let mut event_context = generate_event_context(5, settings.clone());
            let features = event_context
                .extract_records_from_expr(
                    obs_dates.clone(),
                    entity_query.clone(),
                    RawQuery::SelectExpr(
                        r#"
                    SELECT
                        @entities.user as user,
                        obs_dt as obs_dt,
                        max(event_time) over past as last_event_time,
                        @count_past := count(*) over past,
                        @count := @count_past,
                        @count_future := count(*) over future,
                        @count_past as count_past_1,
                        @count_future as count_future_1,
                        @count_past + @count_future as count_all_1,
                        count(*) over past as count_past_2,
                        count(*) over future as count_future_2,
                        count(*) over past as count_past_3,
                        count(*) over future as count_future_3,
                        (count(*) over past) = (count(*) over past) as always_eq,
                        (count(*) over past) = @count_past as always_eq_2,
                        (count(*) over past) + (count(*) over future) as count_all_2
                     FOR
                        @entities := user"#
                            .into(),
                    ),
                    &query_config,
                    None,
                    None,
                )
                .unwrap();

            let dataframe = DataFrame::new(features.0, features.1);
            dataframe.head(10).display();
            let dataframe_dedup = dataframe.drop_duplicates(vec!["user".into()]).unwrap();

            let n_events = event_context.event_store.get_n_events();
            let count_all = dataframe_dedup.col("count_all_1").unwrap().sum() as usize;
            let count_past = dataframe_dedup.col("count_past_1").unwrap().sum() as usize;
            let count_future = dataframe_dedup.col("count_future_1").unwrap().sum() as usize;
            println!(
                "settings {:?} obs dates {:?} all {} past {} future {}",
                settings, obs_dates, count_all, count_past, count_future
            );
            assert_eq!(n_events, count_all);
            assert_eq!(count_all, count_past + count_future);
        }
    }

    #[test]
    fn test_non_existing_attribute() {
        let settings = QueryConfig {
            parallel: false,
            include_events_on_obs_date: false,
        };
        let mut entity_types = HashSet::new();
        entity_types.insert(EntityType("user".into()));
        let obs_dates = ObservationDatesConfig::Interval(Interval {
            entity_types,
            date_part: DatePart::Day,
            nth: 7,
        });
        let mut event_context = generate_event_context(100, settings.clone());
        let entity_query = EventScopeConfig::RelatedEntitiesEvents(vec![EntityType("user".into())]);
        let query_config = QueryConfig::default();
        let features = event_context.extract_records_from_expr(
            obs_dates.clone(),
            entity_query,
            RawQuery::VecExpr(vec![
                "@user".into(),
                "obs_dt".into(),
                "sum(purchaseAmounts) over last 3 days where event_type = 'PurchaseEvent'".into(),
            ]),
            &query_config,
            None,
            None,
        );
        assert!(features.is_err());
    }

    fn generate_event_context(n_events: usize, event_store_settings: QueryConfig) -> EventContext {
        let events = generate_random_events(n_events);
        let mut event_context = EventContext::new_memory();
        let event_clone = events.first().unwrap().clone();
        for event in events {
            event_context.new_event(event);
        }
        event_context
    }
}
