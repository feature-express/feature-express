use std::convert::TryFrom;
use std::str::FromStr;
use std::sync::Arc;

use crate::ast::core::{AggrExpr, Expr};
use crate::map::HashMap;
use crate::sstring::SmallString;
use anyhow::{anyhow, bail, Context, Error, Result};
use chrono::NaiveDateTime;
use itertools::Itertools;
use rayon::prelude::*;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::eval::{eval_context_dispatcher, EvalContext};
use crate::event::{EntityType, Event};
use crate::event_store::row_event_store::memory_event_store::MemoryEventStore;
use crate::event_store::{EventStore, EventStoreImpl};
use crate::features::{Feature, FeatureExtractor, Features};
use crate::interval::NaiveDateTimeInterval;
use crate::obs_dates::{ObsDate, ObservationDates, ObservationDatesConfig};
use crate::types::{Entities, Timestamp};
use crate::utils::transpose_vv;
use crate::value::Value;

#[derive(Debug, Clone)]
pub struct Query {
    pub features: Vec<Feature>,
    pub event_scope: EventScopeConfig,
}

#[derive(Debug)]
pub enum RawQuery {
    VecExpr(Vec<String>),
    SelectExpr(String),
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct QueryConfig {
    #[serde(default)]
    pub parallel: bool,
    #[serde(default)]
    pub include_events_on_obs_date: bool,
}

impl Default for QueryConfig {
    fn default() -> Self {
        QueryConfig {
            parallel: false,
            include_events_on_obs_date: false,
        }
    }
}

/// This enum controls the configuration of the events that are considered when calculating
/// the features. There are a couple of options:
/// - AllEvents means that when calculating the features all events are considered which is very slow
/// - RelatedEntitiesEvents (this one seems strange - I don't completely understand why it exists)
/// - FromObsDates - in this mode the entities ids are taken from pairs (observation time, entity_id)
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub enum EventScopeConfig {
    RelatedEntitiesEvents(Vec<EntityType>),
    AllEvents,
}

impl Default for EventScopeConfig {
    fn default() -> Self {
        EventScopeConfig::AllEvents
    }
}

impl RawQuery {
    pub fn to_features_vec(&self) -> Result<Vec<Feature>> {
        let features_vec: Result<Vec<Feature>> = match self {
            RawQuery::VecExpr(query) => {
                let features_result: Result<Vec<_>> = query
                    .iter()
                    .map(|expr_str| {
                        Feature::from_str(expr_str).map_err(|e| {
                            anyhow!(
                                "Failed to parse feature from string. Full traceback:\n\n{:#}",
                                e
                            )
                        })
                    })
                    .collect();
                let features = features_result?;
                Ok(features)
            }
            RawQuery::SelectExpr(query) => {
                let expr = Expr::from_str(query.as_str())?;
                if let Expr::Select(select) = expr {
                    let features: Vec<_> = select
                        .expressions
                        .iter()
                        .map(|e| (*e).clone().into())
                        .collect();
                    Ok(features)
                } else {
                    bail!("Cannot parse query");
                }
            }
        };
        features_vec
    }
}

impl From<Vec<String>> for RawQuery {
    fn from(value: Vec<String>) -> Self {
        RawQuery::VecExpr(value)
    }
}

#[derive(Debug)]
pub struct FeatureDef {
    pub name: String,
    pub interval: NaiveDateTimeInterval,
    pub extractor: Box<dyn FeatureExtractor>,
}

#[derive(Debug)]
pub struct EventContext {
    /// index for keeping the events
    pub event_store: EventStoreImpl,
}

impl Default for EventContext {
    fn default() -> Self {
        EventContext {
            event_store: EventStoreImpl::default(),
        }
    }
}

impl EventContext {
    pub fn new_memory() -> EventContext {
        Self {
            event_store: EventStoreImpl::MemoryEventStore(MemoryEventStore::new()),
        }
    }

    // pub fn new_postgres(
    //     settings: EventStoreSettings,
    //     postgres_config: PostgresEventStoreConfig,
    // ) -> EventContext {
    //     Self {
    //         event_store: EventStoreImpl::PostgresEventStore(PostgresEventStore::new(
    //             postgres_config,
    //         )),
    //         settings: settings.clone(),
    //     }
    // }

    pub fn new_event(&mut self, event: Event) -> Result<()> {
        self.event_store.insert(event)?;
        Ok(())
    }

    pub fn query(&mut self, _query: String) -> Result<Vec<String>, Vec<Vec<Value>>> {
        todo!()
    }

    pub fn extract_records_from_expr(
        &mut self,
        obs_dates: ObservationDatesConfig,
        event_scope_config: EventScopeConfig,
        query: RawQuery,
        query_config: &QueryConfig,
        experiment_id: Option<SmallString>,
        chunk_size: Option<usize>,
    ) -> Result<(Vec<String>, Vec<Vec<Value>>)> {
        let obs_dates_materialized = obs_dates
            .clone()
            .materialize_observation_dates(Box::new(self.event_store.clone()), query_config)?;

        let features = Features::try_from(query)?;

        let entities: Vec<_> = obs_dates_materialized.inner.keys().collect_vec();

        let results: Vec<_> = match (query_config.parallel, chunk_size) {
            (true, Some(chunk_size)) => entities
                .par_chunks(chunk_size)
                .try_fold(Vec::new, |mut acc, entity_ids| {
                    for entity in entity_ids {
                        let res = self.extract_features_for_entity(
                            &obs_dates_materialized,
                            &experiment_id,
                            &features,
                            entity,
                            query_config,
                            &event_scope_config,
                        )?;
                        acc.push(res);
                    }
                    Ok::<Vec<Vec<Vec<Value>>>, Error>(acc)
                })
                .try_reduce(Vec::new, |mut a, mut b| {
                    a.append(&mut b);
                    Ok(a)
                })?,
            (true, None) => entities
                .par_iter()
                .try_fold(Vec::new, |mut acc, entity_id| {
                    let res = self.extract_features_for_entity(
                        &obs_dates_materialized,
                        &experiment_id,
                        &features,
                        entity_id,
                        query_config,
                        &event_scope_config,
                    )?;
                    acc.push(res);
                    Ok::<Vec<Vec<Vec<Value>>>, Error>(acc)
                })
                .try_reduce(Vec::new, |mut a, mut b| {
                    a.append(&mut b);
                    Ok(a)
                })?
                .into_iter()
                .collect_vec(),
            (false, _) => {
                let mut results = Vec::new();
                for entity_id in entities.iter() {
                    let output = self.extract_features_for_entity(
                        &obs_dates_materialized,
                        &experiment_id,
                        &features,
                        entity_id,
                        query_config,
                        &event_scope_config,
                    )?;
                    results.push(output);
                }
                results
            }
        };

        let variable_assign_feature_names = features
            .features
            .iter()
            .map(|feature| {
                (
                    matches!(feature.expr, Expr::VariableAssign(_, _)),
                    feature.get_name(),
                )
            })
            .collect_vec();

        let feature_names = variable_assign_feature_names
            .iter()
            .filter(|(var_assign, _feature_name)| !*var_assign)
            .map(|(_var_assign, feature_name)| feature_name)
            .cloned()
            .collect_vec();

        Ok((feature_names, results.concat()))
    }

    pub fn extract_features_from_expr(
        &mut self,
        obs_dates: &ObservationDatesConfig,
        entity_query: EventScopeConfig,
        query: RawQuery,
        query_config: &QueryConfig,
        experiment_id: Option<SmallString>,
        chunk_size: Option<usize>,
    ) -> Result<HashMap<String, Vec<Value>>> {
        // let obs_dates_materialied = obs_dates
        //     .clone()
        //     .materialize_observation_dates(Box::new(self.event_store.clone()))?;
        let (feature_names, results) = self.extract_records_from_expr(
            obs_dates.clone(),
            entity_query,
            query,
            query_config,
            experiment_id,
            chunk_size,
        )?;
        let result_concat = transpose_vv(results);
        Ok(feature_names
            .iter()
            .zip(result_concat)
            .map(|(feature_name, v)| (feature_name.clone(), v))
            .collect())
    }

    fn extract_features_for_entity(
        &self,
        obs_dates: &ObservationDates,
        experiment_id: &Option<SmallString>,
        features: &Features,
        entities: &Entities,
        query_config: &QueryConfig,
        event_query_config: &EventScopeConfig,
    ) -> Result<Vec<Vec<Value>>> {
        // let events = match event_query_config {
        //     EventQueryConfig::RelatedEntitiesEvents(entities_vec) => {
        //         // TODO: this can be done before for all calls of this method
        //         let mut new_entities = BTreeMap::new();
        //         entities.iter().for_each(|(k,v)| {
        //             if entities_vec.contains(k) {
        //                 new_entities.insert(k.clone(), v.clone());
        //             }
        //         });
        //         let events = self.event_store
        //             .query_entity(&new_entities, experiment_id.clone())
        //             .ok_or(anyhow!(""))?;
        //         self.concat_events(events)
        //     }
        //     EventQueryConfig::AllEvents => {
        //         self.event_store
        //             .all_events_sorted()?
        //     }
        // };

        self.extract_features_for_entity_many_obsdates(
            obs_dates,
            event_query_config,
            features,
            entities,
            query_config,
            experiment_id,
        )
    }

    fn extract_features_for_entity_many_obsdates(
        &self,
        obs_dates: &ObservationDates,
        event_query_config: &EventScopeConfig,
        features: &Features,
        entities: &Entities,
        query_config: &QueryConfig,
        experiment_id: &Option<SmallString>,
    ) -> Result<Vec<Vec<Value>>> {
        // Number of real features excluding variable assignments
        // we need to map the original feature index to the real features index
        let feature_index_mapping: HashMap<usize, usize> = features
            .features
            .iter()
            .enumerate()
            .map(|(orig_index, feature)| {
                (
                    orig_index,
                    feature,
                    !matches!(feature.expr, Expr::VariableAssign(_, _)),
                )
            })
            .filter(|(_orig_index, _feature, is_real)| *is_real)
            .map(|(orig_index, _feature, _is_real)| orig_index)
            .enumerate()
            .map(|(a, b)| (b, a))
            .collect();

        let n_real_features = feature_index_mapping.len();

        // allocate 2d matrix with values
        if let Some(obs_datetime) = obs_dates.inner.get(entities) {
            let mut value_matrix: Vec<Vec<Value>> =
                vec![vec![Value::None; obs_datetime.0.len()]; n_real_features];
            let context = EvalContext {
                entities: Some(entities.clone()),
                experiment_id: experiment_id.clone(),
                query_config: Some(query_config),
                obs_date: Some(ObsDate {
                    inner: obs_datetime.0.clone(),
                }),
                event_index: Some(self),
                event_types: vec![],
                event: None,
                obs_time: None,
                event_on_obs_date: None,
                event_query_config: Some(event_query_config.clone()),
            };

            let mut stored_variables: HashMap<SmallString, HashMap<Timestamp, Value>> =
                HashMap::new();
            for feature_index in &features.calculation_order {
                let feature = features
                    .features
                    .get(*feature_index)
                    .context("Cannot extract feature")?;
                let expr_result_many =
                    eval_context_dispatcher(&(feature.expr), &context, &stored_variables)?;

                match &feature.expr {
                    Expr::VariableAssign(variable_name, _) => {
                        stored_variables.insert(variable_name.clone(), expr_result_many);
                    }
                    _ => {
                        let sorted_vec = expr_result_many
                            .into_iter()
                            .sorted_by_key(|(ts, _value)| *ts)
                            .map(|(_ts, value)| value)
                            .collect_vec();
                        value_matrix[feature_index_mapping[&feature_index]] = sorted_vec;
                    }
                }
            }
            Ok(transpose_vv(value_matrix))
        } else {
            Ok(vec![])
        }
    }

    pub fn concat_events(
        &self,
        interval_events: Vec<(NaiveDateTime, Vec<Arc<Event>>)>,
    ) -> Vec<Arc<Event>> {
        let mut interval_events_concat = vec![];
        for (_, events_chunk) in interval_events {
            for e in events_chunk {
                interval_events_concat.push(e);
            }
        }
        interval_events_concat
    }
}

/// Checks if the event_type index can be used for the expression so
/// it checks whether the condition (where clause) is an obvious reference
/// to event_type equals to some literal
pub fn check_agg_event_type_index(expr: &AggrExpr) -> Option<String> {
    if let Some(from) = expr.from.clone() {
        Some(from)
    } else if let Some(cond) = expr.cond.clone() {
        check_event_type_index(*cond)
    } else {
        None
    }
}

/// Returns the Some(event_type) if the condition includes event_type == 'a'
pub fn check_event_type_index(expr: Expr) -> Option<String> {
    // plan index usage only if the expression is
    // event_type == 'str'
    // event_type == 'str' and num > 0
    match expr {
        Expr::Eq(lhs, rhs) => match (*lhs, *rhs) {
            (Expr::EventType, Expr::LitStr(event_type)) => Some(event_type),
            (Expr::LitStr(event_type), Expr::EventType) => Some(event_type),
            _ => None,
        },
        Expr::And(lhs, rhs) => {
            let lhs_ = check_event_type_index(*lhs);
            if lhs_.is_some() {
                lhs_
            } else {
                check_event_type_index(*rhs)
            }
        }
        _ => None,
    }
}

#[cfg(test)]
mod test {
    use chrono::Utc;

    use crate::event::{AttributeName, EventType};

    use super::*;

    #[derive(Debug)]
    pub struct EventTest {
        pub id: usize,
        pub time: NaiveDateTime,
    }

    impl Default for EventTest {
        fn default() -> Self {
            Self {
                id: Default::default(),
                time: Utc::now().naive_utc(),
            }
        }
    }

    #[test]
    fn test_default_event() {
        let _ = Event::default();
    }

    #[test]
    fn test_context() {
        let mut event_context = EventContext::default();

        let event = Event {
            event_type: EventType("test".into()),
            event_time: Utc::now().naive_utc(),
            entities: btreemap!["location".into() => "a".into()],
            event_id: None,
            experiment_id: None,
            attrs: Some(
                hashmap! {a!("bool") => Value::Bool(true), a!("bool2") => Value::Bool(false)},
            ),
        };

        event_context.new_event(event);
    }

    #[test]
    fn check_if_expr_uses_event_type_index() {
        let expr = Expr::from_str("last(num1) over past where event_type = 'test'").unwrap();
        let expr = convert_to_aggrexpr(expr);
        assert_eq!(check_agg_event_type_index(&expr), Some("test".into()));

        let expr =
            Expr::from_str("last(num1) over past where event_type = 'test' and num1 > 100.0")
                .unwrap();
        let expr = convert_to_aggrexpr(expr);
        assert_eq!(check_agg_event_type_index(&expr), Some("test".into()));

        let expr = Expr::from_str("last(num1) over past where event_type = 'test' or num1 > 100.0")
            .unwrap();
        let expr = convert_to_aggrexpr(expr);
        assert_eq!(check_agg_event_type_index(&expr), None);
    }

    #[test]
    fn check_if_expr_uses_event_type_from() {
        let expr = Expr::from_str("last(num1) over past from test").unwrap();
        let expr = convert_to_aggrexpr(expr);
        assert_eq!(check_agg_event_type_index(&expr), Some("test".into()));

        let expr = Expr::from_str("last(num1) over past from test where num1 > 100.0").unwrap();
        let expr = convert_to_aggrexpr(expr);
        assert_eq!(check_agg_event_type_index(&expr), Some("test".into()));
    }

    fn convert_to_aggrexpr(expr: Expr) -> AggrExpr {
        let expr = match expr {
            Expr::Aggr(expr) => Some(expr),
            _ => None,
        }
        .unwrap();
        expr
    }
}
