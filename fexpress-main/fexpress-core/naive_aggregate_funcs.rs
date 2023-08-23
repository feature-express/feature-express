use crate::ast::core::BExpr;
use crate::eval;
use crate::eval::ValueVectorType;
use crate::map::HashMap;
use crate::stats::Stats;
use crate::types::{Timestamp, FLOAT, INT};
use crate::value::{Value, ValueType, ValueWithTimestamp};
use anyhow::{anyhow, bail, Context, Error, Result};

pub fn nth(
    event_expr_vec: &Vec<ValueWithTimestamp>,
    stored_variables: &HashMap<String, HashMap<Timestamp, Value>>,
    n_expr: &BExpr,
) -> Result<Value> {
    let n_value = eval::eval_simple_expr(n_expr, None, None, stored_variables)
        .context("Cannot parse nth expression argument")?;
    let n_opt: Option<INT> = n_value.into();
    if let Some(n) = n_opt {
        let v = if n >= 0 {
            event_expr_vec.get(n as usize)
        } else {
            let vec_len = event_expr_vec.len();
            let n_end = (-n as usize) + 1_usize;
            if (-n) as usize >= vec_len {
                None
            } else {
                event_expr_vec.get(vec_len - n_end + 1)
            }
        };
        if let Some(v) = v {
            Ok(v.value.clone())
        } else {
            Ok(Value::None)
        }
    } else {
        bail!("Cannot evaluate nth n_expr {:?} as integer", n_expr);
    }
}

pub fn last(event_expr_vec: &Vec<ValueWithTimestamp>) -> anyhow::Result<Value, Error> {
    let v = event_expr_vec.last();
    if let Some(v) = v {
        Ok(v.value.clone())
    } else {
        Ok(Value::None)
    }
}

pub fn stdev(event_expr_vec: &Vec<ValueWithTimestamp>) -> anyhow::Result<Value, Error> {
    let v = eval::extract_num_vector(event_expr_vec);
    if !v.is_empty() {
        Ok(Value::Num(v.std_dev() as FLOAT))
    } else {
        Ok(Value::None)
    }
}

pub fn var(event_expr_vec: &Vec<ValueWithTimestamp>) -> anyhow::Result<Value, Error> {
    let v = eval::extract_num_vector(event_expr_vec);
    if !v.is_empty() {
        Ok(Value::Num(v.var() as FLOAT))
    } else {
        Ok(Value::None)
    }
}

pub fn median(event_expr_vec: &Vec<ValueWithTimestamp>) -> anyhow::Result<Value, Error> {
    let v = eval::extract_num_vector(event_expr_vec);
    if !v.is_empty() {
        Ok(Value::Num(v.median() as FLOAT))
    } else {
        Ok(Value::None)
    }
}

pub fn mean(event_expr_vec: &Vec<ValueWithTimestamp>) -> anyhow::Result<Value, Error> {
    let v = eval::extract_num_vector(event_expr_vec);
    if !v.is_empty() {
        Ok(Value::Num(v.mean() as FLOAT))
    } else {
        Ok(Value::None)
    }
}

pub fn max(event_expr_vec: &Vec<ValueWithTimestamp>) -> anyhow::Result<Value> {
    if !event_expr_vec.is_empty() {
        eval::calc_mixed_agg(
            event_expr_vec,
            |v| Ok(v.max()),
            |v| Ok(v.iter().max().context("Cannot extract maximum")?.clone()),
            |v| Ok(*v.iter().max().context("Cannot extract maximum")?),
        )
    } else {
        Ok(Value::None)
    }
}

pub fn min(event_expr_vec: &Vec<ValueWithTimestamp>) -> anyhow::Result<Value> {
    if !event_expr_vec.is_empty() {
        eval::calc_mixed_agg(
            event_expr_vec,
            |v| Ok(v.min()),
            |v| Ok(v.iter().min().context("Cannot extract minimum")?.clone()),
            |v| Ok(*v.iter().min().context("Cannot extract minimum")?),
        )
    } else {
        Ok(Value::None)
    }
}

pub fn sum(event_expr_vec: &Vec<ValueWithTimestamp>) -> anyhow::Result<Value, Error> {
    let v = eval::extract_num_vector(event_expr_vec);
    if !v.is_empty() {
        Ok(Value::Num(v.sum() as FLOAT))
    } else {
        Ok(Value::None)
    }
}

pub fn product(event_expr_vec: &Vec<ValueWithTimestamp>) -> anyhow::Result<Value, Error> {
    let v = eval::extract_num_vector(event_expr_vec);
    if !v.is_empty() {
        Ok(Value::Num(v.iter().fold(1.0, |acc, &x| acc * x) as FLOAT))
    } else {
        Ok(Value::None)
    }
}

pub fn count(event_expr_vec: &Vec<ValueWithTimestamp>) -> anyhow::Result<Value, Error> {
    Ok(Value::Int(event_expr_vec.len() as INT))
}

pub fn time_of_last(event_expr_vec: &Vec<ValueWithTimestamp>) -> Result<Value> {
    for el in event_expr_vec.iter().rev() {
        if let (Value::Bool(v), ts) = (el.clone().value, el.ts) {
            if v {
                return Ok(Value::DateTime(ts));
            }
        }
    }
    Ok(Value::None)
}

pub fn first(event_expr_vec: &Vec<ValueWithTimestamp>) -> Result<Value, Error> {
    let v = event_expr_vec.first();
    if let Some(v) = v {
        Ok(v.value.clone())
    } else {
        Ok(Value::None)
    }
}

pub fn time_of_first(event_expr_vec: &Vec<ValueWithTimestamp>) -> Result<Value> {
    for el in event_expr_vec {
        if let (Value::Bool(v), ts) = (&el.value, el.ts) {
            if *v {
                return Ok(Value::DateTime(ts));
            }
        }
    }
    Ok(Value::None)
}

pub fn values(event_expr_vec: &Vec<ValueWithTimestamp>) -> Result<Value, Error> {
    let vec_type = eval::classify_expr_result_vector(&event_expr_vec);
    match vec_type {
        ValueVectorType::SingleType(_type) => match _type {
            ValueType::Int | ValueType::Num | ValueType::Bool => {
                Ok(Value::VecNum(eval::extract_num_vector(event_expr_vec)))
            }
            ValueType::Str | ValueType::Date | ValueType::DateTime => {
                Ok(Value::VecStr(eval::extract_str_vector(event_expr_vec)))
            }
            _ => Err(anyhow!("Unhandled value type")),
        },
        _ => Ok(Value::None),
    }
}

pub fn avg_days_between(event_expr_vec: &Vec<ValueWithTimestamp>) -> Result<Value> {
    if event_expr_vec.len() < 2 {
        return Ok(Value::None);
    }
    let mut timestamps: Vec<_> = event_expr_vec
        .iter()
        .filter_map(|el| match el.value {
            Value::Bool(v) => {
                if v {
                    Some(el.ts)
                } else {
                    None
                }
            }
            _ => None,
        })
        .collect();
    timestamps.sort();
    let diffs: Vec<_> = timestamps.windows(2).map(|w| w[1] - w[0]).collect();
    let sum: i64 = diffs.iter().map(|duration| duration.num_days()).sum();
    let avg = sum as FLOAT / diffs.len() as FLOAT;
    Ok(Value::Num(avg))
}

pub fn time_of_next(event_expr_vec: &Vec<ValueWithTimestamp>) -> Result<Value> {
    for el in event_expr_vec {
        if let (Value::Bool(v), ts) = (&el.value, el.ts) {
            if *v {
                return Ok(Value::DateTime(ts));
            }
        }
    }
    Ok(Value::None)
}

// argmax - when did the first maximum happen
pub fn argmax(event_expr_vec: &Vec<ValueWithTimestamp>) -> Result<Value> {
    if let Some(max_val) = event_expr_vec
        .iter()
        .max_by(|a, b| a.value.partial_cmp(&b.value).unwrap())
    {
        return Ok(Value::DateTime(max_val.ts));
    }
    Ok(Value::None)
}

// argmin - when did the first minimum happen
pub fn argmin(event_expr_vec: &Vec<ValueWithTimestamp>) -> Result<Value> {
    if let Some(min_val) = event_expr_vec
        .iter()
        .min_by(|a, b| a.value.partial_cmp(&b.value).unwrap())
    {
        return Ok(Value::DateTime(min_val.ts));
    }
    Ok(Value::None)
}

// mode - generic function most common value
pub fn mode(event_expr_vec: &Vec<ValueWithTimestamp>) -> Result<Value> {
    let mut map: HashMap<Value, INT> = HashMap::new();
    for el in event_expr_vec {
        *map.entry(el.value.clone()).or_insert(0) += 1;
    }
    map.into_iter()
        .max_by_key(|(_, count)| *count)
        .map(|(val, _)| val)
        .ok_or_else(|| anyhow!("Failed to determine mode"))
}

// any - checks if any value is true (Value::Bool(bool_value))
pub fn any(event_expr_vec: &Vec<ValueWithTimestamp>) -> Result<Value> {
    for el in event_expr_vec {
        if let Value::Bool(v) = el.value {
            if v {
                return Ok(Value::Bool(true));
            }
        }
    }
    Ok(Value::Bool(false))
}

// all - checks if all values are true (Value::Bool(bool_value))
pub fn all(event_expr_vec: &Vec<ValueWithTimestamp>) -> Result<Value> {
    for el in event_expr_vec {
        if let Value::Bool(v) = el.value {
            if !v {
                return Ok(Value::Bool(false));
            }
        }
    }
    Ok(Value::Bool(true))
}

// maxconsecutivetrue - counts the maximum number of consecutive true values
pub fn max_consecutive_true(event_expr_vec: &Vec<ValueWithTimestamp>) -> Result<Value> {
    let mut max_count = 0;
    let mut current_count = 0;
    for el in event_expr_vec {
        if let Value::Bool(v) = el.value {
            if v {
                current_count += 1;
            } else {
                if current_count > max_count {
                    max_count = current_count;
                }
                current_count = 0;
            }
        }
    }
    if current_count > max_count {
        max_count = current_count;
    }
    Ok(Value::Int(max_count as INT))
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::value::ValueWithTimestamp;
    use chrono::NaiveDateTime;

    #[test]
    fn test_product() {
        let v = vec![
            ValueWithTimestamp {
                value: Value::Int(1),
                ts: NaiveDateTime::default(),
            },
            ValueWithTimestamp {
                value: Value::Int(2),
                ts: NaiveDateTime::default(),
            },
            ValueWithTimestamp {
                value: Value::Int(3),
                ts: NaiveDateTime::default(),
            },
            ValueWithTimestamp {
                value: Value::Int(4),
                ts: NaiveDateTime::default(),
            },
            ValueWithTimestamp {
                value: Value::Int(5),
                ts: NaiveDateTime::default(),
            },
        ];
        assert_eq!(product(&v).unwrap(), Value::Num(120.0));
    }
}
