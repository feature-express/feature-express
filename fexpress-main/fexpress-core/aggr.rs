use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::sync::Arc;

use crate::ast::core::AggrExpr;
use crate::sstring::SmallString;
use anyhow::{Context, Result};
use chrono::NaiveDateTime;

use crate::eval::{eval_simple_expr, extract_interval_events, EvalContext};
use crate::event::Event;

use crate::interval::NaiveDateTimeInterval;
use crate::map::HashMap;
use crate::partial_agg::*;
use crate::types::Timestamp;
use crate::value::Value;

/// This is a structure that represents a row of data that needs to be aggregated
#[derive(Debug)]
struct AggrEvalRow {
    pub aggr_eval: Value,
    pub groupby_eval: Option<Value>,
    pub having_eval: Option<Value>,
}

/// This calculates aggregation in a moving window by observation dates
/// First it maps the events so that the components of the aggregation:
/// - inside expression
/// -
#[allow(clippy::suspicious_operation_groupings)]
pub fn eval_agg_using_partial_agg(
    agg: &AggrExpr,
    context: &EvalContext,
    stored_variables: &HashMap<SmallString, HashMap<Timestamp, Value>>,
) -> Result<HashMap<NaiveDateTime, Value>> {
    let obs_dates: Vec<_> = context.get_sorted_obs_dates()?;

    let intervals: Vec<_> = obs_dates
        .iter()
        .filter_map(|obs_date| Some(agg.when.materialize_interval(obs_date)))
        .flatten()
        .collect();

    let first_interval_start = intervals
        .first()
        .expect("obs_dates must be non empty")
        .start_dt;

    let last_interval_end = intervals
        .last()
        .expect("obs_dates must be non empty")
        .end_dt;

    let all_obs_date_interval = NaiveDateTimeInterval {
        start_dt: first_interval_start,
        end_dt: last_interval_end,
    };

    let interval_events = extract_interval_events(agg, context, &all_obs_date_interval);
    let aggr_table = prepare_aggregation_input(agg, context, interval_events, stored_variables)?;
    let mut result = HashMap::new();

    let mut partial_agg_state = PartialAggregateWrapper::new(agg.agg_func.clone());
    let mut last_interval = intervals
        .first()
        .context("Failed to find interval")?
        .clone();

    for (i, (obs_date, interval)) in obs_dates.iter().zip(intervals.iter()).enumerate() {
        // first initialize the aggregation table
        if i == 0 {
            for (ts, vs) in aggr_table.range(interval.start_dt_safe()..interval.end_dt_safe()) {
                for v in vs {
                    partial_agg_state.update(v.aggr_eval.clone(), ts.clone())
                }
            }
        } else {
            // look for interval to subtract and add
            // b0-----------e0
            //       b1------------e1
            // subtract <b0, b1)
            // add      (e0, e1>

            // I think there is a decision to make here if the overlapping region is large then it
            // makes sense to subtract the non overlapping start but if the region is small
            // then it makes sense to construct the new partial aggregate from scratch

            let subtract = (interval.start_dt > last_interval.start_dt)
                & (last_interval.end_dt > interval.start_dt);

            if subtract {
                for (ts, vs) in
                    aggr_table.range(last_interval.start_dt_safe()..interval.start_dt_safe())
                {
                    // construct partial state to subtract
                    let mut partial_agg_sub = PartialAggregateWrapper::new(agg.agg_func.clone());
                    for v in vs {
                        partial_agg_sub.update(v.aggr_eval.clone(), ts.clone())
                    }
                    partial_agg_state.subtract_inplace(&partial_agg_sub);
                }
            }

            match interval.start_dt.cmp(&last_interval.end_dt) {
                Ordering::Greater | Ordering::Equal => {
                    partial_agg_state = PartialAggregateWrapper::new(agg.agg_func.clone());
                    for (ts, vs) in
                        aggr_table.range(interval.start_dt_safe()..interval.end_dt_safe())
                    {
                        for v in vs {
                            partial_agg_state.update(v.aggr_eval.clone(), ts.clone());
                        }
                    }
                }
                Ordering::Less => {
                    for (ts, vs) in
                        aggr_table.range(last_interval.end_dt_safe()..interval.end_dt_safe())
                    {
                        for v in vs {
                            partial_agg_state.update(v.aggr_eval.clone(), ts.clone());
                        }
                    }
                }
            }
        }
        result.insert(*obs_date, partial_agg_state.evaluate());
        last_interval = interval.clone();
    }

    Ok(result)
}

/// Creates a table with precalulcalated aggregation expressions:
/// - where (the table only includes events passing the condition)
/// - aggregated value
/// - groupby
/// - having
fn prepare_aggregation_input(
    agg: &AggrExpr,
    context: &EvalContext,
    interval_events: Option<Vec<(NaiveDateTime, Vec<Arc<Event>>)>>,
    stored_variables: &HashMap<SmallString, HashMap<Timestamp, Value>>,
) -> Result<BTreeMap<NaiveDateTime, Vec<AggrEvalRow>>> {
    let mut data: BTreeMap<NaiveDateTime, Vec<AggrEvalRow>> = BTreeMap::new();
    if let Some(events_vec) = interval_events {
        for (event_time, events) in events_vec {
            for event in events {
                let cond_eval = agg
                    .cond
                    .as_ref()
                    .cloned()
                    .map(|cond_expr| {
                        eval_simple_expr(&cond_expr, Some(&event), Some(context), stored_variables)
                            .context("Cannot evaluate groupby expression")
                    })
                    .transpose()?;

                if (cond_eval == Some(Value::Bool(true))) | agg.cond.is_none() {
                    let aggr_eval = eval_simple_expr(
                        &agg.agg_expr,
                        Some(&event),
                        Some(context),
                        stored_variables,
                    )
                    .context("Cannot evaluate expression")?;

                    let groupby_eval = agg
                        .groupby
                        .as_ref()
                        .cloned()
                        .map(|groupby_expr| {
                            eval_simple_expr(
                                &groupby_expr,
                                Some(&event),
                                Some(context),
                                stored_variables,
                            )
                            .context("Cannot evaluate groupby expression")
                        })
                        .transpose()?;

                    let having_eval = agg
                        .having
                        .as_ref()
                        .cloned()
                        .map(|having_expr| {
                            eval_simple_expr(
                                &(having_expr.expr),
                                Some(&event),
                                Some(context),
                                stored_variables,
                            )
                            .context("Cannot evaluate groupby expression")
                        })
                        .transpose()?;

                    // None values shouldn't be aggregated as well
                    if aggr_eval != Value::None {
                        let aggr_row = AggrEvalRow {
                            aggr_eval,
                            groupby_eval,
                            having_eval,
                        };
                        data.entry(event_time).or_default().push(aggr_row);
                    }
                }
            }
        }
    }
    Ok(data)
}

#[cfg(test)]
mod tests {
    use std::ops::Add;
    use std::str::FromStr;

    use crate::ast::core::Expr;
    use crate::sstring::SmallString;
    use chrono::{Duration, TimeZone, Utc};
    use itertools::Itertools;
    use ordered_float::OrderedFloat;
    use vec1::vec1;

    use crate::datetime_utils::add_ms;
    use crate::eval::eval_agg;
    use crate::event::{AttributeName, Entity, EntityType, EventType};
    use crate::event_index::{check_event_type_index, EventContext, EventScopeConfig, QueryConfig};
    use crate::obs_dates::{ObsDate, ObservationTime};
    use crate::types::FLOAT;

    use super::*;

    /// This calculates aggregation in a moving window by observation dates
    /// but in a naive fashion so it can be tested agains "smart" version
    fn eval_agg_naive(
        agg: &AggrExpr,
        context: &EvalContext,
    ) -> Result<HashMap<NaiveDateTime, Value>> {
        let obs_dates: Vec<_> = context
            .obs_date
            .clone()
            .context("Need observation dates filled here")?
            .get_dates()
            .iter()
            .cloned()
            .sorted()
            .collect_vec()
            .clone();

        let mut result = HashMap::new();
        let event_types: Vec<SmallString> = if let Some(cond) = &agg.cond {
            if let Some(event_type) = check_event_type_index(*(cond).clone()) {
                vec![SmallString::from(event_type)]
            } else {
                vec![]
            }
        } else {
            vec![]
        };

        let query_config = QueryConfig::default();

        for obs_date in obs_dates.iter() {
            let context = EvalContext {
                event_index: context.event_index,
                query_config: Some(&query_config),
                event_query_config: context.event_query_config.clone(),
                entities: context.entities.clone(),
                experiment_id: context.experiment_id.clone(),
                obs_date: None,
                obs_time: Some(ObservationTime {
                    datetime: obs_date.clone(),
                    event_id: None,
                }),
                event_types: event_types.clone(),
                event: None,
                event_on_obs_date: None,
            };
            let stored_variables = HashMap::new();
            let value = eval_agg(&agg, &context, &stored_variables)?;
            result.insert(*obs_date, value);
        }

        Ok(result)
    }

    fn get_event(v: FLOAT, c: String, p: FLOAT) -> Event {
        Event {
            event_type: EventType("".into()),
            event_time: get_obs_date(v as i64),
            entities: btreemap!["location".into() => "a".into()],
            attrs: Some(hashmap! {
                a!("temp") => Value::Num(v),
                a!("pressure") => Value::Num(p),
                a!("tempint") => Value::Int(v as crate::types::INT),
                a!("type") => Value::Str(SmallString::from(c)),
                a!("dict") => Value::MapNum(hashmap!{a!("m") => 1.0})
            }),
            ..Default::default()
        }
    }

    fn get_obs_date(v: i64) -> NaiveDateTime {
        Utc.ymd(2020, 1, 1)
            .and_hms(0, 0, 0)
            .add(Duration::days(v))
            .naive_utc()
    }

    fn get_events() -> Vec<Event> {
        vec![
            get_event(1.0, "a".into(), 1.0),
            get_event(2.0, "b".into(), 2.0),
            get_event(3.0, "c".into(), 3.0),
            get_event(4.0, "d".into(), 4.0),
            get_event(5.0, "e".into(), 5.0),
            get_event(6.0, "f".into(), 6.0),
        ]
    }

    fn get_event_context() -> EventContext {
        let mut event_context = EventContext::new_memory();
        let events = get_events();
        for event in events {
            event_context.new_event(event);
        }
        event_context
    }

    #[test]
    fn test_partial_agg_cases() {
        let event_context = get_event_context();
        let query_config = QueryConfig::default();
        let context = EvalContext {
            event_index: Some(&event_context),
            query_config: Some(&query_config),
            event_query_config: Some(EventScopeConfig::AllEvents),
            entities: Some(
                Entity {
                    typ: EntityType("a".into()),
                    id: "b".into(),
                }
                .into(),
            ),
            experiment_id: None,
            obs_date: Some(ObsDate {
                inner: vec1![
                    add_ms(get_obs_date(1)).into(),
                    add_ms(get_obs_date(2)).into(),
                    add_ms(get_obs_date(3)).into(),
                    add_ms(get_obs_date(4)).into(),
                    add_ms(get_obs_date(5)).into(),
                    add_ms(get_obs_date(6)).into(),
                ],
            }),
            obs_time: None,
            event_types: vec![],
            event: None,
            event_on_obs_date: None,
        };

        for agg in vec![
            "avg", "sum", "count", "var", "stdev", "min", "max", "first", "last", "argmin",
            "argmax", "mode",
        ] {
            for interval in vec![
                "last 2 days",
                "past",
                "last 1 days",
                "last 3 days",
                "last 1 seconds",
                "future",
            ] {
                let aggr_expr =
                    match Expr::from_str(format!("{}(pressure) over {}", agg, interval).as_str())
                        .unwrap()
                    {
                        Expr::Aggr(a) => Some(a),
                        _ => None,
                    }
                    .unwrap();
                let stored_variables = HashMap::new();
                let result =
                    eval_agg_using_partial_agg(&aggr_expr, &context, &stored_variables).unwrap();
                let result_naive = eval_agg_naive(&aggr_expr, &context).unwrap();
                for (k, _) in result.iter() {
                    let a: Option<FLOAT> = result.get(k).unwrap().clone().into();
                    let b: Option<FLOAT> = result_naive.get(k).unwrap().clone().into();
                    match (a, b) {
                        (Some(a), Some(b)) => assert!((a - b).abs() < 1e06),
                        (Some(a), None) if a.abs() > 1e06 => panic!("{:?} != {:?}", a, b),
                        (Some(a), None) if a.abs() <= 1e06 => (),
                        (Some(0.0), None) => (),
                        (None, Some(0.0)) => (),
                        (None, Some(b)) => panic!("{:?} != {:?}", a, b),
                        _ => (),
                    }
                }
            }
        }
    }
}
