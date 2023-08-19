use std::collections::BTreeMap;
use std::ops::*;
use std::sync::Arc;

use anyhow::{anyhow, bail, Context, Error, Result};
use chrono::NaiveDateTime;
use itertools::Itertools;

use vec1::Vec1;

use crate::aggr::eval_agg_using_partial_agg;
use crate::ast::core::{AggrExpr, AggregateFunction, BExpr, Expr, ExprFunc, HavingExprType};
use crate::evaluation::date;
use crate::evaluation::date::{
    eval_current_date, eval_current_time, eval_date_add, eval_date_part, eval_date_sub, eval_day,
    eval_day_of_year, eval_extract, eval_format_date, eval_hour, eval_is_end_of_month,
    eval_is_start_of_month, eval_is_weekend, eval_microsecond, eval_minute, eval_month, eval_now,
    eval_quarter, eval_second, eval_week, eval_weekday, eval_year,
};
use crate::evaluation::regex::{
    eval_regex_count, eval_regex_extract, eval_regex_match, eval_regex_replace, eval_regex_split,
};
use crate::evaluation::text::{
    eval_concat, eval_contains, eval_ends_with, eval_len, eval_lower, eval_replace,
    eval_starts_with, eval_substr, eval_trim, eval_upper,
};
use crate::event::{AttributeKey, AttributeName, EntityType, Event, EventType};
use crate::event_index::{check_agg_event_type_index, EventContext, EventScopeConfig, QueryConfig};
use crate::event_store::EventStore;
use crate::interval::NaiveDateTimeInterval;
use crate::map::HashMap;
use crate::obs_dates::{ObsDate, ObservationTime};
use crate::parser::expr_parser::parse_untyped_attr;
use crate::sstring::SmallString;
use crate::stats::Stats;
use crate::types::{Entities, Timestamp, FLOAT, INT};
use crate::value::{Value, ValueType, ValueWithAlias, ValueWithTimestamp};

#[derive(Default, Debug)]
pub struct EvalContext<'a> {
    pub event_index: Option<&'a EventContext>,
    pub event_query_config: Option<EventScopeConfig>,
    pub query_config: Option<&'a QueryConfig>,
    pub entities: Option<Entities>,
    pub experiment_id: Option<SmallString>,
    pub obs_date: Option<ObsDate>,
    pub obs_time: Option<ObservationTime>,
    pub event_types: Vec<SmallString>,
    pub event: Option<Arc<Event>>,
    pub event_on_obs_date: Option<Arc<Event>>,
}

impl EvalContext<'_> {
    pub fn get_sorted_obs_dates(&self) -> Result<Vec<Timestamp>> {
        Ok(self
            .obs_date
            .clone()
            .context("Needs observation datest")?
            .get_dates()
            .iter()
            .cloned()
            .sorted()
            .collect())
    }
}

pub fn eval_simple_expr_with_ts(
    expr: &Expr,
    event: Option<&Event>,
    context: Option<&EvalContext>,
    stored_variables: &HashMap<SmallString, HashMap<Timestamp, Value>>,
) -> Result<ValueWithTimestamp> {
    let event = event.context("event is None")?;
    Ok(ValueWithTimestamp {
        value: eval_simple_expr(expr, Some(event), context, stored_variables)?,
        ts: event.event_time,
    })
}

// this is a function to decide whether to use optimized expression evaluation
// for certain types of expressions. For example if we have provided an expression
// which can be evaluated in a faster way knowing multiple observation dates
// like partial aggregates then we need to dispatch it to these functions
//
// TODO: we can also handle cases like division
pub fn eval_context_dispatcher(
    expr: &Expr,
    context: &EvalContext,
    stored_variables: &HashMap<SmallString, HashMap<Timestamp, Value>>,
) -> Result<HashMap<Timestamp, Value>> {
    let result = match expr {
        Expr::Aggr(ref agg_expr) => {
            match agg_expr.agg_func {
                AggregateFunction::Count
                | AggregateFunction::Sum
                | AggregateFunction::Avg
                | AggregateFunction::StDev
                | AggregateFunction::Min
                | AggregateFunction::Var => {
                    // optimized version of
                    if agg_expr.having.is_none() && agg_expr.groupby.is_none() {
                        eval_agg_using_partial_agg(agg_expr, context, stored_variables)?
                    } else {
                        eval_expr_many_obsdates(context, expr, stored_variables)?
                    }
                }
                _ => eval_expr_many_obsdates(context, expr, stored_variables)?,
            }
        }
        expr => eval_expr_many_obsdates(context, expr, stored_variables)?,
    };
    Ok(result)
}

// non vectorized implementation of expression evaluation
fn eval_expr_many_obsdates(
    context: &EvalContext,
    expr: &Expr,
    stored_variables: &HashMap<SmallString, HashMap<Timestamp, Value>>,
) -> Result<HashMap<NaiveDateTime, Value>> {
    let mut result = HashMap::new();
    let query_config = QueryConfig::default();
    for obs_date in &context
        .obs_date
        .as_ref()
        .context("need observation dates")?
        .inner
    {
        let event = if let Some(event_id) = &obs_date.event_id {
            if let Some(event) = context
                .event_index
                .ok_or(anyhow!("index needed"))?
                .event_store
                .get_event_by_id(&event_id)
            {
                Some(event)
            } else {
                None
            }
        } else {
            None
        };

        let context = EvalContext {
            event_index: context.event_index,
            query_config: Some(&query_config),
            event_query_config: context.event_query_config.clone(),
            entities: context.entities.clone(),
            experiment_id: context.experiment_id.clone(),
            obs_time: Some(obs_date.clone()),
            obs_date: Some(ObsDate {
                inner: Vec1::new((*obs_date).clone().into()),
            }),
            event_types: vec![],
            event: event.clone(),
            event_on_obs_date: event.clone(),
        };

        match event {
            Some(event) => {
                let event_ref = event.clone();
                result.insert(
                    obs_date.datetime,
                    eval_simple_expr(expr, Some(&*event_ref), Some(&context), stored_variables)
                        .with_context(|| format!("Cannot evaluate expression {:?}", expr))?,
                );
            }
            None => {
                result.insert(
                    obs_date.datetime,
                    eval_simple_expr(expr, None, Some(&context), stored_variables)
                        .with_context(|| format!("Cannot evaluate expression {:?}", expr))?,
                );
            }
        }
    }
    Ok(result)
}

pub fn eval_and_convert_to_float(
    expr: &Expr,
    event: Option<&Event>,
    context: Option<&EvalContext>,
    stored_variables: &HashMap<SmallString, HashMap<Timestamp, Value>>,
) -> Result<FLOAT> {
    match eval_simple_expr(expr, event, context, stored_variables)? {
        Value::Int(a) => Ok(a as FLOAT),
        Value::Num(a) => Ok(a),
        v => bail!("Cannot convert {:?} to Float", v),
    }
}

pub fn eval_simple_expr(
    expr: &Expr,
    event: Option<&Event>,
    context: Option<&EvalContext>,
    stored_variables: &HashMap<SmallString, HashMap<Timestamp, Value>>,
) -> Result<Value> {
    macro_rules! f64fun1 {
        ($lhs:ident, $method:tt, $out:path) => {{
            match eval_and_convert_to_float($lhs, event, context, stored_variables) {
                Ok(a) => Ok($out(a.$method())),
                _ => bail!("Cannot apply function"),
            }
        }};
    }

    macro_rules! f64fun2 {
        ($lhs:ident, $rhs:ident, $method:ident, $out:path) => {{
            let lhs_eval = eval_and_convert_to_float($lhs, event, context, stored_variables);
            let rhs_eval = eval_and_convert_to_float($rhs, event, context, stored_variables);
            match (lhs_eval, rhs_eval) {
                (Ok(a), Ok(b)) => Ok($out(a.$method(&b))),
                _ => bail!("Cannot apply function"),
            }
        }};
    }

    macro_rules! f64fun3 {
        ($value:ident, $arg1:ident, $arg2:ident, $method:ident, $out:path) => {{
            let value_eval = eval_and_convert_to_float($value, event, context, stored_variables);
            let arg1_eval = eval_and_convert_to_float($arg1, event, context, stored_variables);
            let arg2_eval = eval_and_convert_to_float($arg2, event, context, stored_variables);
            match (value_eval, arg1_eval, arg2_eval) {
                (Ok(a), Ok(b), Ok(c)) => Ok($out(a.$method(b, c))),
                _ => bail!("Cannot apply function"),
            }
        }};
    }

    macro_rules! f64fun2noref {
        ($lhs:ident, $rhs:ident, $method:ident, $out:path) => {{
            let lhs_eval = eval_and_convert_to_float($lhs, event, context, stored_variables);
            let rhs_eval = eval_and_convert_to_float($rhs, event, context, stored_variables);
            match (lhs_eval, rhs_eval) {
                (Ok(a), Ok(b)) => Ok($out(a.$method(b))),
                (Err(e), _) | (_, Err(e)) => bail!("Cannot apply function: {:?}", e),
            }
        }};
    }

    macro_rules! extract_attr {
        ($event:expr, $name:ident, $result:path) => {{
            let v = $event
                .extract_attribute()
                .attrs
                .as_ref()
                .map(|m| m.get(&($name).to_kstring()).clone())
                .flatten();
            match v {
                Some($result(v)) => Ok($result(v.clone())),
                _ => Ok(Value::None),
            }
        }};
    }

    let event_with_context = event.context("Event wasn't provided during the evaluation");
    let context_with_context = context.context("Context wasn't provided during the evaluation");

    let result = match expr {
        // TODO: tere should be attribute provider
        Expr::EventType => Ok(Value::Str(event_with_context?.event_type.0.clone())),
        Expr::EventTime => Ok(Value::DateTime(event_with_context?.event_time)),
        Expr::EventId => Ok(Value::Str(SmallString::from(
            event_with_context?
                .event_id
                .clone()
                .map(|s| s.to_string())
                .unwrap_or_else(|| "".into()),
        ))),
        Expr::ObservationDate => Ok(Value::DateTime(
            context_with_context?
                .obs_time
                .clone()
                .context("Need observation date here")?
                .datetime,
        )),
        Expr::EntityId(typ) => {
            let entities_map = context_with_context?
                .entities
                .as_ref()
                .ok_or(anyhow!("Entities needed"))?
                .clone();
            Ok(Value::Str(
                entities_map
                    .get(typ)
                    .with_context(|| format!("Cannot extract entity type {:?}", typ))?
                    .0
                    .clone(),
            ))
        }
        Expr::AttrBool(attribute)
        | Expr::AttrNum(attribute)
        | Expr::AttrInt(attribute)
        | Expr::AttrMapNum(attribute)
        | Expr::AttrMapStr(attribute)
        | Expr::AttrStr(attribute)
        | Expr::AttrVecStr(attribute)
        | Expr::AttrVecInt(attribute)
        | Expr::AttrVecNum(attribute)
        | Expr::AttrVecBool(attribute)
        | Expr::AttrDate(attribute)
        | Expr::AttrDateTime(attribute) => {
            let event = event_with_context?;

            let result = event.extract_attribute(attribute).with_context(|| format!(
                "Cannot extract attribute {:?} from event {:?}",
                attribute, event
            ));
            match result {
                Ok(v) => Ok(v),
                // TODO: temporary solution must check if the attribute exists in the schema
                Err(_) => Ok(Value::None),
            }
        }
        Expr::AttrUntyped(ref attribute) => {
            let eval_context = context.with_context(|| format!("Context needed to evaluate untyped attribute {:?} when evaluating expression {:?}", attribute.clone(), expr))?;
            Ok(evaluate_attribute_key(
                event,
                attribute,
                eval_context,
                stored_variables,
            )?)
        }
        Expr::ContextAttr(ref attribute) => {
            evaluate_context_attribute(expr, context, stored_variables, &attribute)
        }
        Expr::LitBool(v) => Ok(Value::Bool(*v)),
        Expr::LitNum(v) => Ok(Value::Num(**v)),
        Expr::LitInt(v) => Ok(Value::Int(*v)),
        Expr::LitStr(v) => Ok(Value::Str(SmallString::from(v))),
        Expr::LitDate(v) => Ok(Value::Date(*v)),
        Expr::LitDateTime(v) => Ok(Value::DateTime(*v)),

        Expr::Add(lhs, rhs) => f64fun2!(lhs, rhs, add, Value::Num),
        Expr::Sub(lhs, rhs) => f64fun2!(lhs, rhs, sub, Value::Num),
        Expr::Mul(lhs, rhs) => f64fun2!(lhs, rhs, mul, Value::Num),
        Expr::Div(lhs, rhs) => f64fun2!(lhs, rhs, div, Value::Num),

        Expr::Eq(lhs, rhs) => eval_eq(event, lhs, rhs, context, stored_variables),
        Expr::Neq(lhs, rhs) => eval_neq(event, lhs, rhs, context, stored_variables),
        Expr::GreaterEq(lhs, rhs) => f64fun2!(lhs, rhs, ge, Value::Bool),
        Expr::LessEq(lhs, rhs) => f64fun2!(lhs, rhs, le, Value::Bool),
        Expr::Greater(lhs, rhs) => f64fun2!(lhs, rhs, gt, Value::Bool),
        Expr::Less(lhs, rhs) => f64fun2!(lhs, rhs, lt, Value::Bool),

        // this shouldn't be really implemented because it is only evaluated as a part of aggregate
        Expr::Alias(alias, expr) => eval_alias(event, context, alias, expr, stored_variables),
        Expr::Wildcard => Ok(Value::Wildcard),
        Expr::Having(_) => unimplemented!(),
        Expr::None => Ok(Value::None),

        Expr::And(lhs, rhs) => eval_and(event, context, stored_variables, lhs, rhs),
        Expr::Or(lhs, rhs) => eval_or(event, context, stored_variables, lhs, rhs),
        Expr::Not(expr) => eval_not(event, expr, context, stored_variables),

        Expr::Aggr(aggr) => eval_agg(
            aggr,
            context.ok_or(anyhow!(
                "Context wasn't provided for evaluating aggregate expression"
            ))?,
            stored_variables,
        ),
        Expr::In(needle, haystack) => eval_in(event, context, needle, haystack, stored_variables),
        Expr::NotIn(needle, haystack) => {
            eval_in(event, context, needle, haystack, stored_variables).map(|result| match result {
                Value::Bool(v) => Value::Bool(!v),
                _ => unreachable!(),
            })
        }

        Expr::Function(exprfunc) => match exprfunc {
            ExprFunc::Floor(lhs) => f64fun1!(lhs, floor, Value::Num),
            ExprFunc::Ceil(lhs) => f64fun1!(lhs, ceil, Value::Num),
            ExprFunc::Round(lhs) => f64fun1!(lhs, round, Value::Num),
            ExprFunc::Trunc(lhs) => f64fun1!(lhs, trunc, Value::Num),
            ExprFunc::Fract(lhs) => f64fun1!(lhs, fract, Value::Num),
            ExprFunc::Abs(lhs) => f64fun1!(lhs, abs, Value::Num),
            ExprFunc::Signum(lhs) => f64fun1!(lhs, signum, Value::Num),
            ExprFunc::DivEuclid(lhs, rhs) => f64fun2noref!(lhs, rhs, div_euclid, Value::Num),
            ExprFunc::RemEuclid(lhs, rhs) => f64fun2noref!(lhs, rhs, rem_euclid, Value::Num),
            ExprFunc::Powf(lhs, rhs) => f64fun2noref!(lhs, rhs, powf, Value::Num),
            ExprFunc::Sqrt(lhs) => f64fun1!(lhs, sqrt, Value::Num),
            ExprFunc::Exp(lhs) => f64fun1!(lhs, exp, Value::Num),
            ExprFunc::Exp2(lhs) => f64fun1!(lhs, exp2, Value::Num),
            ExprFunc::Ln(lhs) => f64fun1!(lhs, ln, Value::Num),
            ExprFunc::Log(_, _) => unimplemented!(),
            ExprFunc::Log2(lhs) => f64fun1!(lhs, log2, Value::Num),
            ExprFunc::Log10(lhs) => f64fun1!(lhs, log10, Value::Num),
            ExprFunc::Sin(lhs) => f64fun1!(lhs, sin, Value::Num),
            ExprFunc::Cos(lhs) => f64fun1!(lhs, cos, Value::Num),
            ExprFunc::Tan(lhs) => f64fun1!(lhs, tan, Value::Num),
            ExprFunc::Asin(lhs) => f64fun1!(lhs, asin, Value::Num),
            ExprFunc::Acos(lhs) => f64fun1!(lhs, acos, Value::Num),
            ExprFunc::Atan(lhs) => f64fun1!(lhs, atan, Value::Num),
            ExprFunc::Expm1(lhs) => f64fun1!(lhs, exp_m1, Value::Num),
            ExprFunc::Ln1p(lhs) => f64fun1!(lhs, ln_1p, Value::Num),
            ExprFunc::Sinh(lhs) => f64fun1!(lhs, sinh, Value::Num),
            ExprFunc::Cosh(lhs) => f64fun1!(lhs, cosh, Value::Num),
            ExprFunc::Asinh(lhs) => f64fun1!(lhs, asinh, Value::Num),
            ExprFunc::Acosh(lhs) => f64fun1!(lhs, acosh, Value::Num),
            ExprFunc::Atanh(lhs) => f64fun1!(lhs, atanh, Value::Num),
            ExprFunc::Clamp(value, min, max) => f64fun3!(value, min, max, clamp, Value::Num),

            // control flow
            ExprFunc::If(cond, if_true, if_false) => {
                eval_if(event, context, stored_variables, cond, if_true, if_false)
            }

            // date functions
            ExprFunc::Date(expr) => date::eval_to_date(event, context, stored_variables, expr),
            ExprFunc::DateDiff(d1expr, d2expr) => {
                date::eval_date_diff(event, context, stored_variables, d1expr, d2expr)
            }
            ExprFunc::Year(d1expr) => eval_year(event, context, stored_variables, d1expr),
            ExprFunc::Month(d1expr) => eval_month(event, context, stored_variables, d1expr),
            ExprFunc::Day(d1expr) => eval_day(event, context, stored_variables, d1expr),
            ExprFunc::Week(d1expr) => eval_week(event, context, stored_variables, d1expr),
            ExprFunc::Weekday(d1expr) => eval_weekday(event, context, stored_variables, d1expr),
            ExprFunc::DayOfYear(d1expr) => {
                eval_day_of_year(event, context, stored_variables, d1expr)
            }
            ExprFunc::Quarter(d1expr) => eval_quarter(event, context, stored_variables, d1expr),
            ExprFunc::IsStartOfMonth(d1expr) => {
                eval_is_start_of_month(event, context, stored_variables, d1expr)
            }
            ExprFunc::IsEndOfMonth(d1expr) => {
                eval_is_end_of_month(event, context, stored_variables, d1expr)
            }
            ExprFunc::IsWeekend(d1expr) => {
                eval_is_weekend(event, context, stored_variables, d1expr)
            }
            ExprFunc::DateAdd(expr, days) => {
                eval_date_add(event, context, stored_variables, expr, days)
            }
            ExprFunc::DateSub(expr, days) => {
                eval_date_sub(event, context, stored_variables, expr, days)
            }
            ExprFunc::Hour(expr) => eval_hour(event, context, stored_variables, expr),
            ExprFunc::Minute(expr) => eval_minute(event, context, stored_variables, expr),
            ExprFunc::Second(expr) => eval_second(event, context, stored_variables, expr),
            ExprFunc::Microsecond(expr) => eval_microsecond(event, context, stored_variables, expr),
            ExprFunc::DatePart(expr, part) => {
                eval_date_part(event, context, stored_variables, expr, part)
            }
            ExprFunc::Extract(expr, part) => {
                eval_extract(event, context, stored_variables, expr, part)
            }
            ExprFunc::FormatDate(expr, format) => {
                eval_format_date(event, context, stored_variables, expr, format)
            }
            ExprFunc::Now => eval_now(event, context),
            ExprFunc::CurrentDate => eval_current_date(event, context),
            ExprFunc::CurrentTime => eval_current_time(event, context),

            // text functions
            ExprFunc::Len(expr) => eval_len(event, context, stored_variables, expr),
            ExprFunc::Substr(expr, start, length) => {
                eval_substr(event, context, stored_variables, expr, start, length)
            }
            ExprFunc::Concat(expr1, expr2) => {
                eval_concat(event, context, stored_variables, expr1, expr2)
            }
            ExprFunc::Trim(expr) => eval_trim(event, context, stored_variables, expr),
            ExprFunc::Lower(expr) => eval_lower(event, context, stored_variables, expr),
            ExprFunc::Upper(expr) => eval_upper(event, context, stored_variables, expr),
            ExprFunc::Replace(expr, from, to) => {
                eval_replace(event, context, stored_variables, expr, from, to)
            }
            ExprFunc::StartsWith(expr, sub_expr) => {
                eval_starts_with(event, context, stored_variables, expr, sub_expr)
            }
            ExprFunc::EndsWith(expr, sub_expr) => {
                eval_ends_with(event, context, stored_variables, expr, sub_expr)
            }
            ExprFunc::Contains(expr, sub_expr) => {
                eval_contains(event, context, stored_variables, expr, sub_expr)
            }

            // null handling
            ExprFunc::Coalesce(expr1, expr2) => {
                eval_coalesce(event, context, stored_variables, expr1, expr2)
            }
            ExprFunc::RegexMatch(expr1, expr2) => {
                eval_regex_match(event, context, stored_variables, expr1, expr2)
            }
            ExprFunc::RegexExtract(expr1, expr2) => {
                eval_regex_extract(event, context, stored_variables, expr1, expr2)
            }
            ExprFunc::RegexReplace(expr1, expr2, expr3) => {
                eval_regex_replace(event, context, stored_variables, expr1, expr2, expr3)
            }
            ExprFunc::RegexSplit(expr1, expr2) => {
                eval_regex_split(event, context, stored_variables, expr1, expr2)
            }
            ExprFunc::RegexCount(expr1, expr2) => {
                eval_regex_count(event, context, stored_variables, expr1, expr2)
            }
        },

        Expr::TupleLitBool(v) => Ok(Value::VecBool(v.clone())),
        Expr::TupleLitNum(v) => Ok(Value::VecNum(v.iter().map(|v| **v).collect_vec())),
        Expr::TupleLitInt(v) => Ok(Value::VecInt(v.clone())),
        Expr::TupleLitStr(v) => Ok(Value::VecStr(
            v.iter().map(|v| SmallString::from(v).clone()).collect_vec(),
        )),
        Expr::ParsingError(e) => Err(anyhow!(e.clone())),
        Expr::VariableAssign(_variable_name, expression) => {
            eval_simple_expr(expression, event, context, stored_variables)
        }
        Expr::Select(_select_expr) => todo!(),
        Expr::Cons(_lhs, _rhs) => todo!(),
        Expr::FullQuery(_full_query) => panic!("Full queries are evaluated using a different path"),
    };
    result
}

fn evaluate_context_attribute(
    expr: &Expr,
    context: Option<&EvalContext>,
    stored_variables: &HashMap<SmallString, HashMap<Timestamp, Value>>,
    attribute: &&AttributeKey,
) -> Result<Value, Error> {
    // first check if the single attribute is not a stored variable
    if let AttributeKey::Nested(nested) = attribute {
        let first_element = nested.first();
        let second_element = nested.get(1).context(
            "Cannot extract second element of the nested attribute key - shouldn't happen",
        )?;
        if first_element == "entities" {
            let entity_type = EntityType(second_element.clone());
            let eval_context = context.with_context(|| format!("Context needed to evaluate context attributes {:?} when evaluating expression {:?}", attribute.clone(), expr))?;
            let entity_id = eval_context
                .entities
                .as_ref()
                .ok_or(anyhow!("Entities needed"))?
                .get(&entity_type)
                .with_context(|| format!(
                    "Cannot find entity type {:?} to evaluate expression {:?}",
                    entity_type, expr
                ))?;
            return Ok(Value::Str(entity_id.0.clone()));
        }
    }

    let eval_context = context.with_context(|| {
        format!(
            "Context needed to evaluate context attributes {:?} when evaluating expression {:?}",
            attribute.clone(),
            expr
        )
    })?;
    // here there are 2 options either the @attribute refers to the currently evaluated event or the current obs date event
    if let AttributeKey::Single(single_attribute) = &attribute {
        if let Some(stored_data) = stored_variables.get(single_attribute) {
            if let Some(obs_time) = eval_context.obs_time.clone() {
                if let Some(value) = stored_data.get(&obs_time.datetime) {
                    return Ok(value.clone());
                }
            } else {
                bail!(
                    "Cannot evaluate a variable {:?} without an observation date",
                    attribute
                );
            }
        }
    }

    // mind that this is not the currently evaluated event but the context event that the observation date is attached to
    let context_event = eval_context.event_on_obs_date.as_ref().ok_or(anyhow!(
        "Context needed to evaluate context attributes {:?} when evaluating expression {:?}",
        attribute.clone(),
        expr
    ))?;
    Ok(evaluate_attribute_key(
        Some(&context_event),
        attribute,
        eval_context,
        stored_variables,
    )?)
}

fn evaluate_attribute_key(
    event: Option<&Event>,
    attribute: &AttributeKey,
    eval_context: &EvalContext,
    stored_variables: &HashMap<SmallString, HashMap<Timestamp, Value>>,
) -> Result<Value> {
    match attribute {
        AttributeKey::Single(_) => Ok(evaluate_untyped_attribute(
            event,
            eval_context,
            attribute.clone(),
            stored_variables,
        )?),
        AttributeKey::Nested(attr_vec) => {
            if attr_vec.first() == "entities" {
                let entity_type = EntityType(attr_vec.get(1).context("Nested attribute with the first elemenent entities should have a second element signifying the entity type")?.clone());
                let entity_id = event
                    .context("Event should be provided to extract entities key")?
                    .entities
                    .get(&entity_type)
                    .with_context(|| format!(
                        "Failed to extract entity type {:?} from {:?}",
                        entity_type, eval_context.entities
                    ))?;
                Ok(Value::Str(entity_id.0.clone()))
            } else {
                Ok(evaluate_untyped_attribute(
                    event,
                    eval_context,
                    attribute.clone(),
                    stored_variables,
                )?)
            }
        }
    }
}

fn eval_alias(
    event: Option<&Event>,
    context: Option<&EvalContext>,
    alias: &SmallString,
    expr: &BExpr,
    stored_variables: &HashMap<SmallString, HashMap<Timestamp, Value>>,
) -> Result<Value> {
    let v = eval_simple_expr(expr, event, context, stored_variables);
    v.map(|v| {
        Value::ValueWithAlias(Box::new(ValueWithAlias {
            alias: Some(alias.clone()),
            value: v,
        }))
    })
}

fn eval_coalesce(
    event: Option<&Event>,
    context: Option<&EvalContext>,
    stored_variables: &HashMap<SmallString, HashMap<Timestamp, Value>>,
    expr1: &BExpr,
    expr2: &BExpr,
) -> Result<Value> {
    let value1 = eval_simple_expr(expr1, event, context, stored_variables)?;
    if !value1.is_null() {
        return Ok(value1);
    }
    let value2 = eval_simple_expr(expr2, event, context, stored_variables)?;
    Ok(value2) // If value1 is null, we return value2 regardless of whether it is null or not
}

fn evaluate_untyped_attribute(
    event: Option<&Event>,
    context: &EvalContext,
    attribute: AttributeKey,
    stored_variables: &HashMap<SmallString, HashMap<Timestamp, Value>>,
) -> Result<Value> {
    // We can infer the valuetype from the schema of all events
    // if the attribute name has only one value type for all events
    // TODO: It can be done even better if we also utilized the index
    //       from the where expression.

    let attribute_name = a!(attribute.to_kstring());
    let event_index = context.event_index.ok_or(anyhow!("event index needed"))?;
    let value_types: Vec<ValueType> = event_index
        .event_store
        .get_attribute_value_type(&attribute_name)
        .with_context(|| {
            format!(
                "Cannot find attribute name {:} in the schema - available attributes are {:?}",
                attribute,
                event_index.event_store.get_schema()
            )
        })?
        .into_iter()
        .collect();

    if let Some(value_type) = value_types.first() {
        let expr = parse_untyped_attr(attribute, value_type);
        eval_simple_expr(&expr, event, Some(context), stored_variables)
    } else {
        bail!("Cannot guess attribute type, you must specify the attribute type in the expression");
    }
}

macro_rules! eval_date_part {
    ($name:ident, $expr:expr, $ret:ident) => {
        fn $name(
            event: Option<&Event>,
            context: Option<&EvalContext>,
            d1expr: &BExpr,
        ) -> Result<Value> {
            let maybe_date: Option<NaiveDate> = eval_simple_expr(d1expr, event, context)?.into();
            if let Some(date) = maybe_date {
                Ok(Value::$ret($expr(date)))
            } else {
                Ok(Value::None)
            }
        }
    };
}

fn eval_or(
    event: Option<&Event>,
    context: Option<&EvalContext>,
    stored_variables: &HashMap<SmallString, HashMap<Timestamp, Value>>,
    lhs: &BExpr,
    rhs: &BExpr,
) -> Result<Value> {
    let lhs_eval = eval_simple_expr(lhs, event, context, stored_variables)?;
    let rhs_eval = eval_simple_expr(rhs, event, context, stored_variables)?;
    match (lhs_eval, rhs_eval) {
        (Value::Bool(lhs_lit), Value::Bool(rhs_lit)) => Ok(Value::Bool(lhs_lit || rhs_lit)),
        _ => Ok(Value::Bool(false)),
    }
}

fn eval_in(
    event: Option<&Event>,
    context: Option<&EvalContext>,
    needle: &BExpr,
    haystack: &BExpr,
    stored_variables: &HashMap<SmallString, HashMap<Timestamp, Value>>,
) -> Result<Value> {
    let needle_eval = eval_simple_expr(needle, event, context, stored_variables)?;
    let haystack_eval = eval_simple_expr(haystack, event, context, stored_variables)?;
    let needle_eval_type: ValueType = needle_eval.clone().into();
    let haystack_eval_type: ValueType = haystack_eval.clone().into();
    match (&needle_eval, &haystack_eval) {
        (Value::Num(lhs_lit), Value::VecNum(rhs_lit)) => Ok(Value::Bool(rhs_lit.contains(lhs_lit))),
        (Value::Int(lhs_lit), Value::VecInt(rhs_lit)) => Ok(Value::Bool(rhs_lit.contains(lhs_lit))),
        (Value::Str(lhs_lit), Value::VecStr(rhs_lit)) => Ok(Value::Bool(rhs_lit.contains(lhs_lit))),
        _ => Err(anyhow!("Can only compare the values and lists of the same type. In your example the value is {}, and the list is {}", needle_eval_type, haystack_eval_type)),
    }
}

fn eval_and(
    event: Option<&Event>,
    context: Option<&EvalContext>,
    stored_variables: &HashMap<SmallString, HashMap<Timestamp, Value>>,
    lhs: &BExpr,
    rhs: &BExpr,
) -> Result<Value> {
    let lhs_eval = eval_simple_expr(lhs, event, context, stored_variables)?;
    let rhs_eval = eval_simple_expr(rhs, event, context, stored_variables)?;
    match (lhs_eval, rhs_eval) {
        (Value::Bool(lhs_lit), Value::Bool(rhs_lit)) => Ok(Value::Bool(lhs_lit && rhs_lit)),
        _ => Ok(Value::Bool(false)),
    }
}

fn eval_if(
    event: Option<&Event>,
    context: Option<&EvalContext>,
    stored_variables: &HashMap<SmallString, HashMap<Timestamp, Value>>,
    cond: &BExpr,
    if_true: &BExpr,
    if_false: &BExpr,
) -> Result<Value> {
    let cond_eval = eval_simple_expr(cond, event, context, stored_variables)?;
    match cond_eval {
        Value::Bool(true) => eval_simple_expr(if_true, event, context, stored_variables),
        Value::Bool(false) => eval_simple_expr(if_false, event, context, stored_variables),
        _ => Err(anyhow!("Condition must be true/false")),
    }
}

fn eval_not(
    event: Option<&Event>,
    expr: &BExpr,
    context: Option<&EvalContext>,
    stored_variables: &HashMap<SmallString, HashMap<Timestamp, Value>>,
) -> Result<Value> {
    let result = eval_simple_expr(expr, event, context, stored_variables)?;
    match result {
        Value::Bool(v) => Ok(Value::Bool(v.not())),
        _ => bail!("Not expression must be true/false"),
    }
}

pub fn eval_agg(
    agg: &AggrExpr,
    context: &EvalContext,
    stored_variables: &HashMap<SmallString, HashMap<Timestamp, Value>>,
) -> Result<Value> {
    let interval = agg
        .when
        .materialize_interval(
            &context
                .obs_time
                .clone()
                .ok_or(anyhow!("Cannot extract date"))?
                .datetime,
        )
        .context("Couldn't parse the interval")?;

    let interval_events = extract_interval_events(agg, context, &interval);

    if agg.groupby.is_some() && agg.having.is_some() {
        bail!("Group by and Having cannot be defined in the same aggregation");
    }

    if let Some(interval_events) = interval_events {
        let interval_events_concat = context
            .event_index
            .ok_or(anyhow!("event index needed"))?
            .concat_events(interval_events);
        if agg.groupby.is_some() {
            eval_groupby_agg(agg, &interval_events_concat, context, stored_variables)
        } else if agg.having.is_some() {
            eval_having_agg(agg, &interval_events_concat, context, stored_variables)
        } else {
            eval_agg_without_having(agg, &interval_events_concat, context, stored_variables)
        }
    } else {
        Ok(Value::None)
    }
}

pub fn eval_agg_without_having(
    agg: &AggrExpr,
    interval_events_concat: &[Arc<Event>],
    context: &EvalContext,
    stored_variables: &HashMap<SmallString, HashMap<Timestamp, Value>>,
) -> Result<Value> {
    let filtered_event_expr_vec =
        get_filtered_events(agg, interval_events_concat, context, stored_variables)?;
    let event_expr_vec: Vec<_> = filtered_event_expr_vec
        .iter()
        .filter(|event| {
            evaluate_where_expr(agg.cond.as_ref(), event, context, stored_variables)
                .unwrap_or(false)
        })
        .filter_map(|event| {
            eval_simple_expr_with_ts(
                &(agg.agg_expr),
                Some(event),
                Some(context),
                stored_variables,
            )
            .ok()
        })
        .collect();
    calc_agg(&agg.agg_func, event_expr_vec, stored_variables)
}

pub fn eval_having_agg(
    agg: &AggrExpr,
    interval_events_concat: &[Arc<Event>],
    context: &EvalContext,
    stored_variables: &HashMap<SmallString, HashMap<Timestamp, Value>>,
) -> Result<Value> {
    let filtered_event_expr_vec =
        get_filtered_events(agg, interval_events_concat, context, stored_variables)?;
    let having = agg
        .having
        .as_ref()
        .ok_or(anyhow!("Having is obligatory here"))?;
    let event_expr_vec: Vec<_> = filtered_event_expr_vec
        .iter()
        .filter_map(|event| {
            tuple_of_options_to_option(
                eval_simple_expr_with_ts(
                    &(agg.agg_expr),
                    Some(event),
                    Some(context),
                    stored_variables,
                )
                .ok(),
                eval_simple_expr(&having.expr, Some(event), Some(context), stored_variables).ok(),
            )
        })
        .collect();

    let extreme = match &having.typ {
        HavingExprType::MIN => event_expr_vec
            .iter()
            .map(|(_, b)| b)
            .min()
            .ok_or(anyhow!("Cannot calculate min out of an empty iterators"))?,
        HavingExprType::MAX => event_expr_vec
            .iter()
            .map(|(_, b)| b)
            .max()
            .ok_or(anyhow!("Cannot calculate max out of an empty iterators"))?,
    };

    let event_expr_vec = event_expr_vec
        .iter()
        .filter_map(|(a, b)| if b == extreme { Some(a.clone()) } else { None })
        .collect();
    calc_agg(&agg.agg_func, event_expr_vec, stored_variables)
}

pub fn eval_groupby_agg(
    agg: &AggrExpr,
    interval_events_concat: &[Arc<Event>],
    context: &EvalContext,
    stored_variables: &HashMap<SmallString, HashMap<Timestamp, Value>>,
) -> Result<Value> {
    /*
    TODO rewrite checking where with a loop
     */

    let filtered_event_expr_vec =
        get_filtered_events(agg, interval_events_concat, context, stored_variables)?;
    let groupby = agg
        .groupby
        .as_ref()
        .ok_or(anyhow!("Group by is obligatory here"))?;
    let event_expr_vec: Vec<_> = filtered_event_expr_vec
        .iter()
        .filter_map(|event| {
            let groupby_result =
                eval_simple_expr_with_ts(groupby, Some(event), Some(context), stored_variables);
            let expr_result = eval_simple_expr_with_ts(
                &agg.agg_expr,
                Some(event),
                Some(context),
                stored_variables,
            );

            if let (Ok(groupby), Ok(expr)) = (groupby_result, expr_result) {
                Some((groupby.value.to_string(), expr))
            } else {
                None
            }
        })
        .collect();

    let mut groupby_results = HashMap::<String, Vec<ValueWithTimestamp>>::new();
    for (k, v) in event_expr_vec {
        groupby_results.entry(k.clone()).or_default().push(v);
    }

    let mut results: HashMap<AttributeName, Box<Value>> = HashMap::new();
    for (k, v) in groupby_results.iter() {
        let agg = calc_agg(&agg.agg_func, v.to_vec(), stored_variables);
        match agg {
            Ok(v) => {
                results.insert(a!(k.clone()), Box::new(v));
            }
            Err(e) => bail!("Error evaluating aggregation: {:?}", e),
        }
    }

    Ok(Value::Map(results))
}

fn get_filtered_events(
    agg: &AggrExpr,
    interval_events_concat: &[Arc<Event>],
    context: &EvalContext,
    stored_variables: &HashMap<SmallString, HashMap<Timestamp, Value>>,
) -> Result<Vec<Arc<Event>>> {
    let mut filtered_event_expr_vec = Vec::new();
    for event in interval_events_concat.iter() {
        match evaluate_where_expr(agg.cond.as_ref(), event, context, stored_variables) {
            Ok(true) => filtered_event_expr_vec.push(event.clone()),
            Ok(false) => {}
            Err(e) => bail!("Error evaluating where expression: {:?}", e),
        }
    }
    Ok(filtered_event_expr_vec)
}

fn tuple_of_options_to_option<A, B>(opt_1: Option<A>, opt_2: Option<B>) -> Option<(A, B)> {
    match (opt_1, opt_2) {
        (Some(a), Some(b)) => Some((a, b)),
        _ => None,
    }
}

/// Extracts events for a given aggregate expression.
/// Tries to use the entity_id, event_type index if possible
pub fn extract_interval_events(
    agg_expr: &AggrExpr,
    context: &EvalContext,
    interval: &NaiveDateTimeInterval,
) -> Option<Vec<(NaiveDateTime, Vec<Arc<Event>>)>> {
    match &context.event_query_config.as_ref()? {
        EventScopeConfig::RelatedEntitiesEvents(selected_entities) => {
            let mut new_entities = BTreeMap::new();
            context.entities.as_ref()?.iter().for_each(|(k, v)| {
                if selected_entities.contains(k) {
                    new_entities.insert(k.clone(), v.clone());
                }
            });
            let event_type_index_name = check_agg_event_type_index(agg_expr);
            let interval_events: Option<Vec<_>> = match event_type_index_name {
                Some(event_type) => context.event_index?.event_store.query_entity_event_type(
                    context.entities.as_ref()?,
                    &EventType(from_string!(event_type)),
                    interval,
                    context.query_config?,
                    &context.experiment_id,
                ),
                None => context.event_index?.event_store.query_entity_interval(
                    context.entities.as_ref()?,
                    interval,
                    context.query_config?,
                    &context.experiment_id,
                ),
            };
            interval_events
        }
        EventScopeConfig::AllEvents => context
            .event_index?
            .event_store
            .query_interval(interval, context.query_config?),
    }
}

fn evaluate_where_expr(
    where_expr: Option<&BExpr>,
    event: &Event,
    context: &EvalContext,
    stored_variables: &HashMap<SmallString, HashMap<Timestamp, Value>>,
) -> Result<bool> {
    if let Some(where_expr) = where_expr {
        let eval_where =
            eval_simple_expr(where_expr, Some(event), Some(context), stored_variables)?;
        match eval_where {
            Value::Bool(v) => Ok(v),
            _ => Ok(false),
        }
    } else {
        Ok(true)
    }
}

fn calc_agg(
    func: &AggregateFunction,
    event_expr_vec: Vec<ValueWithTimestamp>,
    stored_variables: &HashMap<SmallString, HashMap<Timestamp, Value>>,
) -> Result<Value> {
    match func {
        AggregateFunction::Count => Ok(Value::Int(event_expr_vec.len() as INT)),
        AggregateFunction::Sum => {
            let v = extract_num_vector(event_expr_vec);
            if !v.is_empty() {
                Ok(Value::Num(v.sum() as FLOAT))
            } else {
                Ok(Value::None)
            }
        }
        AggregateFunction::Min => {
            if !event_expr_vec.is_empty() {
                calc_mixed_agg(
                    event_expr_vec,
                    |v| Ok(v.min()),
                    |v| Ok(v.iter().min().context("Cannot extract minimum")?.clone()),
                    |v| Ok(*v.iter().min().context("Cannot extract minimum")?),
                )
            } else {
                Ok(Value::None)
            }
        }
        AggregateFunction::Max => {
            if !event_expr_vec.is_empty() {
                calc_mixed_agg(
                    event_expr_vec,
                    |v| Ok(v.max()),
                    |v| Ok(v.iter().max().context("Cannot extract maximum")?.clone()),
                    |v| Ok(*v.iter().max().context("Cannot extract maximum")?),
                )
            } else {
                Ok(Value::None)
            }
        }
        AggregateFunction::Avg => {
            let v = extract_num_vector(event_expr_vec);
            if !v.is_empty() {
                Ok(Value::Num(v.mean() as FLOAT))
            } else {
                Ok(Value::None)
            }
        }
        AggregateFunction::Median => {
            let v = extract_num_vector(event_expr_vec);
            if !v.is_empty() {
                Ok(Value::Num(v.median() as FLOAT))
            } else {
                Ok(Value::None)
            }
        }
        AggregateFunction::Var => {
            let v = extract_num_vector(event_expr_vec);
            if !v.is_empty() {
                Ok(Value::Num(v.var() as FLOAT))
            } else {
                Ok(Value::None)
            }
        }
        AggregateFunction::StDev => {
            let v = extract_num_vector(event_expr_vec);
            if !v.is_empty() {
                Ok(Value::Num(v.std_dev() as FLOAT))
            } else {
                Ok(Value::None)
            }
        }
        AggregateFunction::Last => {
            let v = event_expr_vec.last();
            if let Some(v) = v {
                Ok(v.value.clone())
            } else {
                Ok(Value::None)
            }
        }
        AggregateFunction::Nth(n_expr) => {
            let n_value = eval_simple_expr(n_expr, None, None, stored_variables)
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
        AggregateFunction::First => {
            let v = event_expr_vec.first();
            if let Some(v) = v {
                Ok(v.value.clone())
            } else {
                Ok(Value::None)
            }
        }
        AggregateFunction::TimeOfLast => {
            for el in event_expr_vec.iter().rev() {
                if let (Value::Bool(v), ts) = (el.clone().value, el.ts) {
                    if v {
                        return Ok(Value::DateTime(ts));
                    }
                }
            }
            Ok(Value::None)
        }
        AggregateFunction::TimeOfFirst => {
            for el in event_expr_vec {
                if let (Value::Bool(v), ts) = (el.value, el.ts) {
                    if v {
                        return Ok(Value::DateTime(ts));
                    }
                }
            }
            Ok(Value::None)
        }
        AggregateFunction::TimeOfNext => {
            for el in event_expr_vec {
                if let (Value::Bool(v), ts) = (el.value, el.ts) {
                    if v {
                        return Ok(Value::DateTime(ts));
                    }
                }
            }
            Ok(Value::None)
        }
        AggregateFunction::AvgDaysBetween => {
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
        AggregateFunction::Values => {
            let vec_type = classify_expr_result_vector(&event_expr_vec);
            match vec_type {
                ValueVectorType::SingleType(_type) => match _type {
                    ValueType::Int | ValueType::Num | ValueType::Bool => {
                        Ok(Value::VecNum(extract_num_vector(event_expr_vec)))
                    }
                    ValueType::Str | ValueType::Date | ValueType::DateTime => {
                        Ok(Value::VecStr(extract_str_vector(event_expr_vec)))
                    }
                    _ => Err(anyhow!("Unhandled value type")),
                },
                _ => Ok(Value::None),
            }
        }
    }
}

#[derive(Clone, Debug, Hash)]
pub enum ValueVectorType {
    None,
    Mixed,
    SingleType(ValueType),
}

pub fn extract_num_vector(event_expr_vec: Vec<ValueWithTimestamp>) -> Vec<FLOAT> {
    event_expr_vec
        .iter()
        .map(|v| extract_inner_value(&(v.value)))
        .collect()
}

fn extract_inner_value(x: &Value) -> FLOAT {
    match x {
        Value::Bool(v) => (*v as i64) as FLOAT,
        Value::Num(v) => *v,
        Value::Int(v) => *v as FLOAT,
        Value::Str(_) => 0.0 as FLOAT,
        Value::Map(_) => 0.0 as FLOAT,
        Value::None => 0.0 as FLOAT,
        Value::Date(_) => 0.0 as FLOAT,
        Value::DateTime(_) => 0.0 as FLOAT,
        Value::VecStr(_) => unimplemented!(),
        Value::VecInt(_) => unimplemented!(),
        Value::VecNum(_) => unimplemented!(),
        Value::MapNum(_) => unimplemented!(),
        Value::ValueWithAlias(v) => extract_inner_value(&(v.value)),
        Value::MapStr(_) => unimplemented!(),
        Value::Wildcard => FLOAT::NAN,
        Value::VecBool(_) => unimplemented!(),
        Value::NotCalculatedYet => unimplemented!(),
    }
}

pub fn extract_str_vector(event_expr_vec: Vec<ValueWithTimestamp>) -> Vec<SmallString> {
    event_expr_vec
        .iter()
        .filter_map(|x| match &x.value {
            Value::Str(s) => Some(s.clone()),
            Value::Date(d) => Some(SmallString::from(d.to_string())),
            Value::DateTime(d) => Some(SmallString::from(d.to_string())),
            _ => None,
        })
        .collect()
}

pub fn extract_dt_vector(event_expr_vec: Vec<ValueWithTimestamp>) -> Vec<NaiveDateTime> {
    event_expr_vec
        .iter()
        .filter_map(|x| match &x.value {
            Value::Date(dt) => dt.and_hms_opt(0, 0, 0),
            Value::DateTime(dt) => Some(*dt),
            _ => None,
        })
        .collect()
}

pub fn classify_expr_result_vector(v: &[ValueWithTimestamp]) -> ValueVectorType {
    let mut type_counter: HashMap<ValueType, usize> = HashMap::new();
    for e in v.iter() {
        let result_type: ValueType = e.value.clone().into();
        if result_type != ValueType::None {
            *(type_counter.entry(result_type).or_default()) += 1;
        }
    }

    if type_counter.is_empty() {
        ValueVectorType::None
    } else {
        let keys: Vec<_> = type_counter.keys().collect();
        let first = keys.first();
        match first {
            Some(value) => ValueVectorType::SingleType((*value).clone()),
            None => ValueVectorType::Mixed,
        }
    }
}

/// This is used where a function like minimum or maximum can be calculated on more then
/// one type. First we need to classify whether the vector has results of the same type
/// and classify it. Aggregation is only possible if the results are of a single type excluding
/// None.
fn calc_mixed_agg(
    event_expr_vec: Vec<ValueWithTimestamp>,
    num_agg: fn(Vec<FLOAT>) -> Result<FLOAT>,
    str_agg: fn(Vec<SmallString>) -> Result<SmallString>,
    dt_agg: fn(Vec<NaiveDateTime>) -> Result<NaiveDateTime>,
) -> Result<Value> {
    let vec_type = classify_expr_result_vector(&event_expr_vec);
    match vec_type {
        ValueVectorType::None => Ok(Value::None),
        ValueVectorType::Mixed => Ok(Value::None),
        ValueVectorType::SingleType(result_type) => match result_type {
            ValueType::Num | ValueType::Int => {
                let v = extract_num_vector(event_expr_vec);
                if !v.is_empty() {
                    Ok(Value::Num(num_agg(v)?))
                } else {
                    Ok(Value::None)
                }
            }
            ValueType::Str => {
                let v = extract_str_vector(event_expr_vec);
                if !v.is_empty() {
                    Ok(Value::Str(str_agg(v)?))
                } else {
                    Ok(Value::None)
                }
            }
            ValueType::Date | ValueType::DateTime => {
                let v = extract_dt_vector(event_expr_vec);
                if !v.is_empty() {
                    Ok(Value::DateTime(dt_agg(v)?))
                } else {
                    Ok(Value::None)
                }
            }
            _ => Ok(Value::None),
        },
    }
}

fn eval_eq(
    event: Option<&Event>,
    lhs: &Expr,
    rhs: &Expr,
    context: Option<&EvalContext>,
    stored_variables: &HashMap<SmallString, HashMap<Timestamp, Value>>,
) -> Result<Value> {
    let lhs_eval = eval_simple_expr(lhs, event, context, stored_variables)?;
    let rhs_eval = eval_simple_expr(rhs, event, context, stored_variables)?;
    match (lhs_eval, rhs_eval) {
        (Value::Str(a), Value::Str(b)) => Ok(Value::Bool(a == b)),
        (Value::Int(a), Value::Int(b)) => Ok(Value::Bool(a == b)),
        (Value::Num(a), Value::Num(b)) => Ok(Value::Bool(is_float_equal(&a, &b))),
        (Value::Int(a), Value::Num(b)) => Ok(Value::Bool(is_float_equal(&(a as FLOAT), &b))),
        (Value::Num(a), Value::Int(b)) => Ok(Value::Bool(is_float_equal(&a, &(b as FLOAT)))),
        (Value::Bool(a), Value::Bool(b)) => Ok(Value::Bool(a == b)),
        (Value::None, _) => Ok(Value::Bool(false)),
        (_, Value::None) => Ok(Value::Bool(false)),
        (a, b) => Err(anyhow!("Incomparable types {:?} == {:?}", a, b)),
    }
}

fn eval_neq(
    event: Option<&Event>,
    lhs: &Expr,
    rhs: &Expr,
    context: Option<&EvalContext>,
    stored_variables: &HashMap<SmallString, HashMap<Timestamp, Value>>,
) -> Result<Value> {
    eval_eq(event, lhs, rhs, context, stored_variables).map(|b| match b {
        Value::Bool(b) => Ok(Value::Bool(!b)),
        a => Ok(a),
    })?
}

fn is_float_equal(a: &FLOAT, b: &FLOAT) -> bool {
    FLOAT::abs(a - b) < FLOAT::EPSILON
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::str::FromStr;

    use chrono::{Duration, TimeZone, Utc};
    use ordered_float::OrderedFloat;

    use crate::event::{AttributeName, Entity};
    use crate::event_index::EventContext;
    use crate::event_index::QueryConfig;

    use super::*;

    #[test]
    pub fn test_add_expr() {
        let event = Event {
            event_type: EventType("".into()),
            event_time: Utc::now().naive_utc(),
            entities: btreemap!["location".into() => "a".into()],
            event_id: None,
            experiment_id: None,
            attrs: None,
        };

        let lhs = Expr::LitInt(1);
        let rhs = Expr::LitNum(OrderedFloat(2.5));
        let hm = HashMap::new();
        let result = eval_simple_expr(
            &Expr::Function(ExprFunc::Round(Box::new(Expr::Add(
                Box::new(lhs),
                Box::new(rhs),
            )))),
            Some(&event),
            None,
            &hm,
        );
        let result = eval_simple_expr(
            &Expr::Greater(Box::new(Expr::LitInt(2)), Box::new(Expr::LitInt(1))),
            Some(&event),
            None,
            &hm,
        );
    }

    #[test]
    pub fn test_if() {
        let event = Event {
            event_type: EventType("".into()),
            event_time: Utc::now().naive_utc(),
            entities: btreemap!["location".into() => "a".into()],
            event_id: None,
            experiment_id: None,
            attrs: None,
        };

        let hm = HashMap::new();
        let result = eval_simple_expr(
            &Expr::Function(ExprFunc::If(
                Box::new(Expr::Greater(
                    Box::new(Expr::LitInt(2)),
                    Box::new(Expr::LitInt(0)),
                )),
                Box::new(Expr::LitInt(1)),
                Box::new(Expr::LitInt(0)),
            )),
            Some(&event),
            None,
            &hm,
        )
        .unwrap();
        assert_eq!(result, Value::Int(1));
    }

    #[test]
    fn test_agg() {
        let mut event_context = EventContext::new_memory();
        let query_config = QueryConfig::default();
        let mut dt = Utc.ymd(2020, 1, 1).and_hms(0, 0, 0).naive_utc();
        for i in 0..=1000 {
            let event = Event {
                event_type: EventType("pressure".into()),
                event_time: dt,
                entities: btreemap!["location".into() => "a".into()],
                attrs: Some(hashmap! {a!("pressure") => Value::Num(i as FLOAT)}),
                ..Default::default()
            };
            event_context.new_event(event);
            dt = dt.add(Duration::minutes(1))
        }

        let datetime = Utc.ymd(2020, 1, 1).and_hms(1, 0, 0).naive_utc().into();
        let event_query_config =
            EventScopeConfig::RelatedEntitiesEvents(vec![EntityType("1".into())]);
        let context = EvalContext {
            entities: Some(
                Entity {
                    typ: "1".into(),
                    id: "1".into(),
                }
                .into(),
            ),
            query_config: Some(&query_config),
            experiment_id: None,
            obs_date: None,
            event_index: Some(&event_context),
            event_types: vec![],
            event: None,
            obs_time: Some(ObservationTime {
                datetime,
                event_id: None,
            }),
            event_on_obs_date: None,
            event_query_config: Some(event_query_config),
        };

        let hm = HashMap::new();
        let fql = r#"(avg(pressure) over past) / (avg(pressure) over last 10 minutes)"#;
        let expr = Expr::from_str(fql).unwrap();
        let result = eval_simple_expr(&expr, None, Some(&context), &hm);
    }

    #[test]
    fn test_event_time() {
        let mut event_context = EventContext::new_memory();
        let query_config = QueryConfig::default();
        let mut dt = Utc.ymd(2020, 1, 1).and_hms(0, 0, 0).naive_utc();
        for _ in 0..=2 {
            let event = Event {
                event_type: EventType("pressure".into()),
                event_time: dt,
                entities: BTreeMap::new(),
                ..Default::default()
            };
            event_context.new_event(event);
            dt = dt.add(Duration::days(1))
        }

        let expr = Expr::from_str("max(event_time) over past").unwrap();

        let datetime = Utc.ymd(2020, 1, 10).and_hms(0, 0, 0).naive_utc().into();

        let context = EvalContext {
            entities: Some(
                Entity {
                    typ: "".into(),
                    id: "1".into(),
                }
                .into(),
            ),
            query_config: Some(&query_config),
            experiment_id: None,
            obs_date: None,
            event_index: Some(&event_context),
            event_types: vec![],
            event: None,
            obs_time: Some(ObservationTime {
                datetime,
                event_id: None,
            }),
            event_on_obs_date: None,
            event_query_config: Some(EventScopeConfig::AllEvents),
        };

        let hm = HashMap::new();
        let result = eval_simple_expr(&expr, None, Some(&context), &hm);
    }

    fn get_event(v: FLOAT, c: String, p: FLOAT, o: bool, entity_id: String) -> Event {
        Event {
            event_type: EventType("".into()),
            event_time: Utc
                .ymd(2020, 1, 1)
                .and_hms(0, 0, 0)
                .add(Duration::days(v as i64))
                .naive_utc(),
            entities: btreemap!["location".into() => SmallString::from(entity_id).into()],
            attrs: Some(hashmap! {
                a!("temp") => Value::Num(v),
                a!("pressure") => Value::Num(p),
                a!("tempint") => Value::Int(v as INT),
                a!("type") => Value::Str(SmallString::from(c)),
                a!("dict") => Value::MapNum(hashmap!{a!("m") => 1.0}),
                a!("is_overcast") => Value::Bool(o)
            }),
            ..Default::default()
        }
    }

    fn get_events() -> Vec<Event> {
        vec![
            get_event(1.0, "a".into(), 1.0, true, "a".into()),
            get_event(2.0, "b".into(), 100.0, true, "a".into()),
            get_event(3.0, "c".into(), 3.0, false, "a".into()),
            get_event(4.0, "d".into(), 5.0, true, "a".into()),
            get_event(5.0, "e".into(), 200.0, false, "a".into()),
            get_event(6.0, "f".into(), -100.0, true, "a".into()),
        ]
    }

    fn get_event_context() -> EventContext {
        let mut event_context = EventContext::default();
        let events = get_events();
        for event in events {
            event_context.new_event(event);
        }
        event_context
    }

    fn eval_expr(expr_str: String, entity_id: String) -> Value {
        let event_context = get_event_context();
        let query_config = QueryConfig::default();
        let expr = Expr::from_str(&expr_str).unwrap();
        let datetime = Utc.ymd(2020, 1, 30).and_hms(0, 0, 0).naive_utc();
        let context = EvalContext {
            entities: Some(
                Entity {
                    typ: "location".into(),
                    id: entity_id.into(),
                }
                .into(),
            ),
            query_config: Some(&query_config),
            experiment_id: None,
            obs_date: None,
            event_index: Some(&event_context),
            event_types: vec![],
            event: None,
            obs_time: Some(ObservationTime {
                datetime,
                event_id: None,
            }),
            event_on_obs_date: None,
            event_query_config: Some(EventScopeConfig::AllEvents),
        };
        let hm = HashMap::new();
        eval_simple_expr(&expr, None, Some(&context), &hm).unwrap()
    }

    #[test]
    fn test_agg_num() {
        let result = eval_expr("count(type) over past".into(), "a".into());
        assert_eq!(result, Value::Int(6));
    }

    #[test]
    fn test_in_str() {
        let result = eval_expr(
            "count(type) over past where type in ('a','b','c','d')".into(),
            "a".into(),
        );
        assert_eq!(result, Value::Int(4));
        let result = eval_expr(
            "count(*) over past where type in ('a','b','c','d')".into(),
            "a".into(),
        );
        assert_eq!(result, Value::Int(4));
    }

    #[test]
    fn test_not_in_str() {
        let result = eval_expr(
            "count(type) over past where type not in ('a','b','c','d')".into(),
            "a".into(),
        );
        assert_eq!(result, Value::Int(2));
        let result = eval_expr(
            "count(*) over past where type not in ('a','b','c','d')".into(),
            "a".into(),
        );
        assert_eq!(result, Value::Int(2));
    }

    #[test]
    fn test_in_int() {
        let result = eval_expr(
            "count(type) over past where tempint in (1,2,3,4)".into(),
            "a".into(),
        );
        assert_eq!(result, Value::Int(4));
    }

    #[test]
    fn test_not_in_int() {
        let result = eval_expr(
            "count(type) over past where tempint not in (1,2,3,4)".into(),
            "a".into(),
        );
        assert_eq!(result, Value::Int(2));
    }

    #[test]
    fn test_in_float() {
        let result = eval_expr(
            "count(type) over past where temp in (1.0,2.0,3.0, 4.0)".into(),
            "a".into(),
        );
        assert_eq!(result, Value::Int(4));
    }

    #[test]
    fn test_not_in_float() {
        let result = eval_expr(
            "count(type) over past where temp not in (1.0, 2.0 , 3.0, 4.0)".into(),
            "a".into(),
        );
        assert_eq!(result, Value::Int(2));
    }

    #[test]
    fn test_last_nested() {
        let result = eval_expr("last(dict.m) over past".into(), "a".into());
        assert_eq!(result, Value::Num(1.0));
    }

    #[test]
    fn test_last_nested_grouped() {
        let result = eval_expr("count(type) over past group by dict.m".into(), "a".into());
    }

    #[test]
    fn test_map_num() {
        let result = eval_expr("last(dict) over past".into(), "a".into());
        assert_eq!(result, Value::MapNum(hashmap! {a!("m") => 1.0}));
    }

    #[test]
    fn test_agg_nth() {
        let result = eval_expr("nth(temp, 0) over past".into(), "a".into());
        assert_eq!(result, Value::Num(1.0));
    }

    #[test]
    fn test_agg_nth_1() {
        let result = eval_expr("nth(temp, 1) over past".into(), "a".into());
        assert_eq!(result, Value::Num(2.0));
    }

    #[test]
    fn test_agg_nth_m1() {
        let result = eval_expr("nth(temp, -1) over past".into(), "a".into());
        assert_eq!(result, Value::Num(6.0));
    }

    #[test]
    fn test_agg_nth_m2() {
        let result = eval_expr("nth(temp, -2) over past".into(), "a".into());
        assert_eq!(result, Value::Num(5.0));
    }

    #[test]
    fn test_agg_nth_wrong() {
        let result = eval_expr("nth(temp, -7) over past".into(), "a".into());
        assert_eq!(result, Value::None);
    }

    #[test]
    fn test_agg_sum() {
        let result = eval_expr("sum(temp) over past".into(), "a".into());
        assert_eq!(result, Value::Num(21.0));
    }

    #[test]
    fn test_agg_sum_untyped() {
        let result = eval_expr("sum(temp) over past".into(), "a".into());
        assert_eq!(result, Value::Num(21.0));
    }

    #[test]
    fn test_agg_min() {
        let result = eval_expr("min(temp) over past".into(), "a".into());
        assert_eq!(result, Value::Num(1.0));
    }

    // TODO: I changed so that the attribute names are case sensitive
    // #[test]
    // fn test_attribute_case() {
    //     let result1 = eval_expr("min(temp) over past".into(), "a".into());
    //     let result2 = eval_expr("min(TEMP) over past".into(), "a".into());
    //     assert_eq!(result1, result2);
    // }

    #[test]
    fn test_agg_max() {
        let result = eval_expr("max(temp) over past".into(), "a".into());
        assert_eq!(result, Value::Num(6.0));
    }

    #[test]
    fn test_agg_max_str() {
        let result = eval_expr("max(type) over past".into(), "a".into());
        assert_eq!(result, Value::Str("f".into()));
    }

    #[test]
    fn test_agg_last_str() {
        let result = eval_expr("last(type) over past".into(), "a".into());
        assert_eq!(result, Value::Str("f".into()));
    }

    #[test]
    fn test_agg_min_dt() {
        let result = eval_expr("min(event_time) over past".into(), "a".into());
        assert_eq!(
            result,
            Value::DateTime(NaiveDateTime::from_str("2020-01-02T00:00:00").unwrap())
        );
    }

    #[test]
    fn test_agg_max_dt() {
        let result = eval_expr("max(event_time) over past".into(), "a".into());
        assert_eq!(
            result,
            Value::DateTime(NaiveDateTime::from_str("2020-01-07T00:00:00").unwrap())
        );
    }

    #[test]
    fn test_agg_median() {
        let result = eval_expr("median(temp) over past".into(), "a".into());
        assert_eq!(result, Value::Num(3.5));
    }

    #[test]
    fn test_agg_first() {
        let result = eval_expr("first(temp) over past".into(), "a".into());
        assert_eq!(result, Value::Num(1.0));
    }

    #[test]
    fn test_agg_last() {
        let result = eval_expr("last(temp) over past".into(), "a".into());
        assert_eq!(result, Value::Num(6.0))
    }

    #[test]
    fn test_having_min() {
        let result = eval_expr("first(type) over past having min temp".into(), "a".into());
        assert_eq!(result, Value::Str("a".into()))
    }

    #[test]
    fn test_having_max_1() {
        let result = eval_expr("first(type) over past having max temp".into(), "a".into());
        assert_eq!(result, Value::Str("f".into()));
    }

    #[test]
    fn test_having_max_2() {
        let result = eval_expr(
            "first(event_time) over past having max pressure".into(),
            "a".into(),
        );
        assert_eq!(
            result,
            Value::DateTime(NaiveDateTime::from_str("2020-01-06T00:00:00").unwrap())
        );
    }

    #[test]
    fn test_having_max_2_over_between() {
        let result = eval_expr(
            "first(event_time) over between date('2020-01-01') to date('2020-01-05') having max pressure".into(),
            "a".into(),
        );
        assert_eq!(
            result,
            Value::DateTime(NaiveDateTime::from_str("2020-01-03T00:00:00").unwrap())
        );
    }

    #[test]
    fn test_having_max_event_time() {
        let result = eval_expr(
            "first(event_time) over past having max temp".into(),
            "a".into(),
        );
        assert_eq!(
            result,
            Value::DateTime(NaiveDateTime::from_str("2020-01-07T00:00:00").unwrap())
        );
    }

    #[test]
    fn test_log() {
        let result = eval_expr("ln(1)".into(), "a".into());
        assert_eq!(result, Value::Num(0.0));
    }

    #[test]
    fn test_clamp() {
        let tests = vec![
            ("clamp(5.0, 0.0, 10.0)", Value::Num(5.0)),
            ("clamp(-1.0, 0.0, 10.0)", Value::Num(0.0)),
            ("clamp(11.0, 0.0, 10.0)", Value::Num(10.0)),
            ("clamp(5, 0.0, 10.0)", Value::Num(5.0)),
        ];

        for (input, expected) in tests {
            let result = eval_expr(input.into(), "a".into());
            assert_eq!(result, expected);
        }
    }

    #[test]
    fn test_now() {
        let result = eval_expr("now()".into(), "a".into());
    }

    #[test]
    fn test_agg_values_numeric() {
        let result = eval_expr("values(temp) over past".into(), "a".into());
        assert_eq!(result, Value::VecNum(vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0]));
    }

    #[test]
    fn test_agg_values_string() {
        let result = eval_expr("values(type) over past".into(), "a".into());
        assert_eq!(
            result,
            Value::VecStr(
                vec!["a", "b", "c", "d", "e", "f"]
                    .iter()
                    .map(|s| SmallString::from(s.to_string()))
                    .collect()
            )
        );
    }

    #[test]
    fn test_agg_values_bool() {
        let result = eval_expr("values(is_overcast) over past".into(), "a".into());
        assert_eq!(result, Value::VecNum(vec![1.0, 1.0, 0.0, 1.0, 0.0, 1.0]));
    }

    #[test]
    fn test_agg_time_of_first() {
        // Looking for the timestamp of the first event where `temp` > 0.
        let result = eval_expr("time_of_first(temp > 0) over past".into(), "a".into());
        // Assuming the first event in your `get_events` function is the first event where `temp` > 0.
        assert_eq!(
            result,
            Value::DateTime(Utc.ymd(2020, 1, 2).and_hms(0, 0, 0).naive_utc())
        );
    }

    #[test]
    fn test_agg_avg_time_between() {
        // Calculating the average time between events where `temp` > 0.
        let result = eval_expr("avg_time_between(temp > 0) over past".into(), "a".into());
        // Assuming the events where `temp` > 0 are at times 1.0, 2.0, and 4.0, the average time between them should be 1.5.
        assert_eq!(result, Value::Num(1.0));
    }

    #[test]
    fn test_agg_time_of_first_temp_gt_3() {
        // Looking for the timestamp of the first event where `temp` > 3.
        let result = eval_expr("time_of_first(temp > 3) over past".into(), "a".into());
        // Assuming the first event where `temp` > 3 in your `get_events` function is the one with timestamp 4.0.
        assert_eq!(
            result,
            Value::DateTime(Utc.ymd(2020, 1, 5).and_hms(0, 0, 0).naive_utc())
        );
    }

    #[test]
    fn test_agg_avg_time_between_temp_gt_3() {
        // Calculating the average time between events where `temp` > 3.
        let result = eval_expr("avg_time_between(temp > 3) over past".into(), "a".into());
        // Assuming the events where `temp` > 3 are at times 4.0, 5.0, and 6.0, the average time between them should be 1.0.
        assert_eq!(result, Value::Num(1.0));
    }

    #[test]
    fn test_eval_eq_for_int_and_num() {
        // given
        let lhs_vec: Vec<Expr> = vec![
            Expr::LitInt(1),
            Expr::LitNum(OrderedFloat(1.0)),
            Expr::LitInt(1),
            Expr::LitNum(OrderedFloat(1.0)),
            Expr::LitInt(1),
            Expr::LitNum(OrderedFloat(1.0)),
            Expr::LitInt(1),
            Expr::LitNum(OrderedFloat(1.0)),
        ];
        let rhs_vec: Vec<Expr> = vec![
            Expr::LitInt(1),
            Expr::LitNum(OrderedFloat(1.0)),
            Expr::LitNum(OrderedFloat(1.0)),
            Expr::LitInt(1),
            Expr::LitInt(2),
            Expr::LitNum(OrderedFloat(2.0)),
            Expr::LitNum(OrderedFloat(2.0)),
            Expr::LitInt(2),
        ];
        let expected_res: Vec<Value> = vec![true, true, true, true, false, false, false, false]
            .iter()
            .map(|v| Value::Bool(*v))
            .collect();
        let zipped = lhs_vec.iter().zip(rhs_vec.iter());

        // when
        let hm = HashMap::new();
        let res: Vec<Value> = zipped
            .map(|(l, r)| eval_eq(None, l, r, None, &hm))
            .map(|res| res.unwrap())
            .collect();

        // then
        assert_eq!(res, expected_res)
    }

    #[test]
    fn test_eval_eq_for_small_float_differences() {
        // given
        let lhs_vec: Vec<Expr> = vec![Expr::LitInt(1), Expr::LitInt(1)];
        let rhs_vec: Vec<Expr> = vec![
            Expr::LitNum(OrderedFloat(1.0 + 0.5 * FLOAT::EPSILON)),
            Expr::LitNum(OrderedFloat(1.0 + 1.5 * FLOAT::EPSILON)),
        ];
        let expected_res: Vec<Value> = vec![true, false].iter().map(|v| Value::Bool(*v)).collect();
        let zipped = lhs_vec.iter().zip(rhs_vec.iter());

        // when
        let hm = HashMap::new();
        let res: Vec<Value> = zipped
            .map(|(l, r)| eval_eq(None, l, r, None, &hm))
            .map(|res| res.unwrap())
            .collect();

        // then
        assert_eq!(res, expected_res)
    }

    #[test]
    fn test_eval_date_expr() {
        let expr = Expr::from_str("date_add(date(obs_dt), 1)").unwrap();
        let datetime = Utc.ymd(2020, 1, 30).and_hms(0, 0, 0).naive_utc();
        let stored_variables = HashMap::new();
        let context = EvalContext {
            obs_time: Some(ObservationTime {
                datetime,
                event_id: None,
            }),
            ..Default::default()
        };
        let result = eval_simple_expr(&expr, None, Some(&context), &stored_variables);
    }

    #[test]
    fn test_variable_dependencies() {}
}
