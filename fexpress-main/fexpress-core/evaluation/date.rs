use crate::map::HashMap;
use crate::sstring::SmallString;
use anyhow::{anyhow, Context, Result};
use chrono::{Datelike, NaiveDate, NaiveDateTime, Timelike, Weekday};

use crate::ast::core::BExpr;
use crate::eval::{eval_simple_expr, EvalContext};
use crate::evaluation::date_common::*;
use crate::event::Event;
use crate::types::{Timestamp, INT};
use crate::value::{Value, ValueType};

// Usage of define_single_expr_eval_fn

define_single_expr_eval_fn!(
    eval_hour,
    Value::DateTime(datetime) => datetime,
    |datetime| Ok(Value::Int(datetime_hour_common(datetime))),
    "Hour"
);

define_single_expr_eval_fn!(
    eval_minute,
    Value::DateTime(datetime) => datetime,
    |datetime| Ok(Value::Int(datetime_minute_common(datetime))),
    "Minute"
);

define_single_expr_eval_fn!(
    eval_second,
    Value::DateTime(datetime) => datetime,
    |datetime| Ok(Value::Int(datetime_second_common(datetime))),
    "Second"
);

define_single_expr_eval_fn!(
    eval_microsecond,
    Value::DateTime(datetime) => datetime,
    |datetime| Ok(Value::Int(datetime_microsecond_common(datetime))),
    "Microsecond"
);

define_single_expr_eval_fn!(
    eval_to_date,
    Value::Str(v) => v,
    |v| {
        match eval_to_date_common(&v) {
            Ok(date) => Ok(Value::Date(date)),
            Err(e) => Err(anyhow!("Error parsing date: {}", e)),
        }
    },
    "ToDate"
);

// Usage of define_double_expr_eval_fn

define_double_expr_eval_fn!(
    eval_date_add,
    Value::Date(date) => date, Value::Int(add) => add,
    |date, add| Ok(Value::Date(date_add_common(date, add))),
    "DateAdd"
);

define_double_expr_eval_fn!(
    eval_date_sub,
    Value::Date(date) => date, Value::Int(sub) => sub,
    |date,sub| Ok(Value::Date(date_sub_common(date, sub))),
    "DateSub"
);

define_double_expr_eval_fn!(
    eval_date_part,
    Value::Str(date_part) => date_part, Value::DateTime(datetime) => datetime,
    |val1, val2| {
        match eval_date_part_common(date_part, datetime) {
            Ok(int) => Ok(Value::Int(int)),
            Err(e) => Err(anyhow!("Error extracting date part: {}", e)),
        }
    },
    "DatePart"
);

define_double_expr_eval_fn!(
    eval_format_date,
    Value::Str(date_format) => date_format, Value::Date(date) => date,
    |val1, val2| Ok(Value::Str(SmallString::from(date.format(date_format.as_str()).to_string()))),
    "FormatDate"
);

pub fn eval_date_diff(
    event: Option<&Event>,
    context: Option<&EvalContext>,
    stored_variables: &HashMap<SmallString, HashMap<Timestamp, Value>>,
    d1expr: &BExpr,
    d2expr: &BExpr,
) -> Result<Value> {
    let d1_eval: Option<NaiveDate> =
        eval_simple_expr(d1expr, event, context, stored_variables)?.into();
    let d2_eval: Option<NaiveDate> =
        eval_simple_expr(d2expr, event, context, stored_variables)?.into();
    match (d1_eval, d2_eval) {
        (Some(d1), Some(d2)) => Ok(Value::Int(eval_date_diff_common(d1, d2))),
        _ => Ok(Value::None),
    }
}

define_single_expr_eval_fn!(
    eval_day,
    Value::Date(date) => date,
    |date| Ok(Value::Int(day_of_date_common(date))),
    "eval_day"
);

define_single_expr_eval_fn!(
    eval_month,
    Value::Date(date) => date,
    |date| Ok(Value::Int(month_of_date_common(date))),
    "eval_month"
);

define_single_expr_eval_fn!(
    eval_year,
    Value::Date(date) => date,
    |date| Ok(Value::Int(year_of_date_common(date))),
    "eval_year"
);

define_single_expr_eval_fn!(
    eval_weekday,
    Value::Date(date) => date,
    |date| Ok(Value::Int(weekday_of_date_common(date))),
    "eval_weekday"
);

define_single_expr_eval_fn!(
    eval_day_of_year,
    Value::Date(date) => date,
    |date| Ok(Value::Int(day_of_year_of_date_common(date))),
    "eval_day_of_year"
);

define_single_expr_eval_fn!(
    eval_quarter,
    Value::Date(date) => date,
    |date| Ok(Value::Int(quarter_of_date_common(date))),
    "eval_quarter"
);

define_single_expr_eval_fn!(
    eval_is_start_of_month,
    Value::Date(date) => date,
    |date| Ok(Value::Bool(is_start_of_month_common(date))),
    "eval_is_start_of_month"
);

define_single_expr_eval_fn!(
    eval_is_end_of_month,
    Value::Date(date) => date,
    |date| Ok(Value::Bool(is_end_of_month_common(date))),
    "eval_is_end_of_month"
);

define_single_expr_eval_fn!(
    eval_is_weekend,
    Value::Date(date) => date,
    |date| Ok(Value::Bool(is_weekend_common(date))),
    "eval_is_weekend"
);

define_single_expr_eval_fn!(
    eval_week,
    Value::Date(date) => date,
    |date| Ok(Value::Int(week_of_date_common(date))),
    "eval_week"
);

// to remove?
pub fn eval_extract(
    event: Option<&Event>,
    context: Option<&EvalContext>,
    stored_variables: &HashMap<SmallString, HashMap<Timestamp, Value>>,
    date_part_expr: &BExpr,
    date_expr: &BExpr,
) -> Result<Value> {
    // This function is very similar to DatePart
    eval_date_part(event, context, stored_variables, date_part_expr, date_expr)
}

// For Now, CurrentDate, CurrentTime we don't need any expression:
pub fn eval_now(_event: Option<&Event>, _context: Option<&EvalContext>) -> Result<Value> {
    Ok(Value::DateTime(eval_now_common()))
}

pub fn eval_current_date(_event: Option<&Event>, _context: Option<&EvalContext>) -> Result<Value> {
    Ok(Value::Date(eval_current_date_common()))
}

pub fn eval_current_time(_event: Option<&Event>, _context: Option<&EvalContext>) -> Result<Value> {
    Ok(Value::Str(SmallString::from(eval_current_time_common())))
}

#[cfg(test)]
mod tests {
    use crate::ast::core::Expr;
    use crate::sstring::SmallString;
    use chrono::{NaiveDate, NaiveDateTime};

    use super::*;

    #[test]
    pub fn test_eval_date_add() {
        let date_expr = Box::new(Expr::LitDate(NaiveDate::from_ymd(2023, 1, 1)));
        let add_expr = Box::new(Expr::LitInt(10));
        let stored_variables = HashMap::new();
        let result = eval_date_add(None, None, &stored_variables, &date_expr, &add_expr).unwrap();
        assert_eq!(result, Value::Date(NaiveDate::from_ymd(2023, 1, 11)));
    }

    #[test]
    pub fn test_eval_date_sub() {
        let date_expr = Box::new(Expr::LitDate(NaiveDate::from_ymd(2023, 1, 11)));
        let sub_expr = Box::new(Expr::LitInt(10));
        let stored_variables = HashMap::new();
        let result = eval_date_sub(None, None, &stored_variables, &date_expr, &sub_expr).unwrap();
        assert_eq!(result, Value::Date(NaiveDate::from_ymd(2023, 1, 1)));
    }

    #[test]
    pub fn test_eval_hour() {
        let datetime_expr = Box::new(Expr::LitDateTime(
            NaiveDateTime::parse_from_str("2022-10-10 00:00:00", "%Y-%m-%d %H:%M:%S").unwrap(),
        ));
        let stored_variables = HashMap::new();
        let result = eval_hour(None, None, &stored_variables, &datetime_expr).unwrap();
        assert_eq!(result, Value::Int(0));
    }

    #[test]
    pub fn test_eval_minute() {
        let datetime_expr = Box::new(Expr::LitDateTime(
            NaiveDateTime::parse_from_str("2022-10-10 00:01:00", "%Y-%m-%d %H:%M:%S").unwrap(),
        ));
        let stored_variables = HashMap::new();
        let result = eval_minute(None, None, &stored_variables, &datetime_expr).unwrap();
        assert_eq!(result, Value::Int(1));
    }

    #[test]
    pub fn test_eval_second() {
        let datetime_expr = Box::new(Expr::LitDateTime(
            NaiveDateTime::parse_from_str("2022-10-10 00:00:01", "%Y-%m-%d %H:%M:%S").unwrap(),
        ));
        let stored_variables = HashMap::new();
        let result = eval_second(None, None, &stored_variables, &&datetime_expr).unwrap();
        assert_eq!(result, Value::Int(1));
    }

    #[test]
    pub fn test_eval_microsecond() {
        let datetime_expr = Box::new(Expr::LitDateTime(
            NaiveDateTime::parse_from_str("2022-10-10 00:00:00.1", "%Y-%m-%d %H:%M:%S%.f").unwrap(),
        ));
        let stored_variables = HashMap::new();
        let result = eval_microsecond(None, None, &stored_variables, &datetime_expr).unwrap();
        assert_eq!(result, Value::Int(100000));
    }

    #[test]
    pub fn test_eval_date_part() {
        let date_part_expr = Box::new(Expr::LitStr("year".to_string()));
        let datetime_expr = Box::new(Expr::LitDateTime(
            NaiveDateTime::parse_from_str("2022-10-10 00:00:00", "%Y-%m-%d %H:%M:%S").unwrap(),
        ));
        let stored_variables = HashMap::new();
        let result = eval_date_part(
            None,
            None,
            &stored_variables,
            &date_part_expr,
            &datetime_expr,
        )
        .unwrap();
        assert_eq!(result, Value::Int(2022));
    }

    #[test]
    pub fn test_eval_extract() {
        let date_part_expr = Box::new(Expr::LitStr("year".to_string()));
        let datetime_expr = Box::new(Expr::LitDateTime(
            NaiveDateTime::parse_from_str("2022-10-10 00:00:00", "%Y-%m-%d %H:%M:%S").unwrap(),
        ));
        let stored_variables = HashMap::new();
        let result = eval_extract(
            None,
            None,
            &stored_variables,
            &date_part_expr,
            &datetime_expr,
        )
        .unwrap();
        assert_eq!(result, Value::Int(2022));
    }

    #[test]
    pub fn test_eval_format_date() {
        let format_expr = Box::new(Expr::LitStr("%Y-%m-%d".to_string()));
        let date_expr = Box::new(Expr::LitDate(NaiveDate::from_ymd(2023, 1, 1)));
        let stored_variables = HashMap::new();
        let result =
            eval_format_date(None, None, &stored_variables, &format_expr, &date_expr).unwrap();
        assert_eq!(result, Value::Str(SmallString::from("2023-01-01")));
    }

    #[test]
    pub fn test_eval_invalid_arguments() {
        let expr1 = Box::new(Expr::LitStr("hello".to_string()));
        let expr2 = Box::new(Expr::LitInt(10));
        let stored_variables = HashMap::new();
        let result = eval_date_add(None, None, &stored_variables, &expr1, &expr2);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().to_string(), "Invalid arguments for DateAdd. It expects compatible types. But the provided value types are Str, Int");
    }
}
