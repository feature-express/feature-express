use crate::map::HashMap;
use crate::sstring::SmallString;
use anyhow::{anyhow, Result};

use crate::ast::core::BExpr;
use crate::eval::{eval_simple_expr, EvalContext};
use crate::event::Event;
use crate::types::{Timestamp, INT};
use crate::value::{Value, ValueType};

define_single_expr_eval_fn! {
    eval_len,
    Value::Str(ref s) => s,
    |s: &SmallString| -> Result<Value> { Ok(Value::Int(s.len() as INT)) },
    "len"
}

define_single_expr_eval_fn! {
    eval_trim,
    Value::Str(ref s) => s,
    |s: &SmallString| -> Result<Value> { Ok(Value::Str(SmallString::from(s.trim()))) },
    "trim"
}

define_single_expr_eval_fn! {
    eval_lower,
    Value::Str(ref s) => s,
    |s: &SmallString| -> Result<Value> { Ok(Value::Str(SmallString::from(s.to_lowercase()))) },
    "lower"
}

define_single_expr_eval_fn! {
    eval_upper,
    Value::Str(ref s) => s,
    |s: &SmallString| -> Result<Value> { Ok(Value::Str(SmallString::from(s.to_uppercase()))) },
    "upper"
}

define_double_expr_eval_fn! {
    eval_concat,
    Value::Str(ref lhs_lit) => lhs_lit, Value::Str(ref rhs_lit) => rhs_lit,
    |lhs_lit: &SmallString, rhs_lit: &SmallString| -> Result<Value> {
        Ok(Value::Str(SmallString::from(format!("{}{}", lhs_lit, rhs_lit))))
    },
    "concat"
}

define_double_expr_eval_fn! {
    eval_contains,
    Value::Str(ref s) => s, Value::Str(ref find) => find,
    |s: &SmallString, find: &SmallString| -> Result<Value> {
        Ok(Value::Bool(s.contains(&*find)))
    },
    "contains"
}

define_double_expr_eval_fn! {
    eval_starts_with,
    Value::Str(ref s) => s, Value::Str(ref start) => start,
    |s: &SmallString, start: &SmallString| -> Result<Value> {
        Ok(Value::Bool(s.starts_with(&*start)))
    },
    "starts_with"
}

define_double_expr_eval_fn! {
    eval_ends_with,
    Value::Str(ref s) => s, Value::Str(ref end) => end,
    |s: &SmallString, end: &SmallString| -> Result<Value> {
        Ok(Value::Bool(s.ends_with(&*end)))
    },
    "ends_with"
}

pub fn eval_substr(
    event: Option<&Event>,
    context: Option<&EvalContext>,
    stored_variables: &HashMap<SmallString, HashMap<Timestamp, Value>>,
    expr: &BExpr,
    start: &BExpr,
    length: &BExpr,
) -> Result<Value> {
    let string = eval_simple_expr(expr, event, context, stored_variables)?;
    let start = eval_simple_expr(start, event, context, stored_variables)?;
    let length = eval_simple_expr(length, event, context, stored_variables)?;

    match (string, start, length) {
        (Value::Str(s), Value::Int(start), Value::Int(length)) => {
            let start = start as usize;
            let length = length as usize;
            if s.len() >= start + length {
                let v = s[start..start + length].to_string();
                Ok(Value::Str(SmallString::from(v)))
            } else {
                Err(anyhow!("Substring out of bounds"))
            }
        }
        (string, start, length) => {
            let string_type: ValueType = string.into();
            let start_type: ValueType = start.into();
            let length_type: ValueType = length.into();
            Err(anyhow!("Invalid arguments for substr. It expects Str, Int, Int. But the provided value types are {}, {}, {}", string_type, start_type, length_type))
        }
    }
}

pub fn eval_replace(
    event: Option<&Event>,
    context: Option<&EvalContext>,
    stored_variables: &HashMap<SmallString, HashMap<Timestamp, Value>>,
    expr: &BExpr,
    from: &BExpr,
    to: &BExpr,
) -> Result<Value> {
    let string = eval_simple_expr(expr, event, context, stored_variables)?;
    let from = eval_simple_expr(from, event, context, stored_variables)?;
    let to = eval_simple_expr(to, event, context, stored_variables)?;

    match (string, from, to) {
        (Value::Str(s), Value::Str(from), Value::Str(to)) => {
            Ok(Value::Str(SmallString::from(s.replace(&*from, &*to))))
        }
        (string, from, to) => {
            let string_type: ValueType = string.into();
            let from_type: ValueType = from.into();
            let to_type: ValueType = to.into();
            Err(anyhow!("Invalid arguments for replace. It expects Str, Str, Str. But the provided value types are {}, {}, {}", string_type, from_type, to_type))
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::ast::core::Expr;
    use crate::sstring::SmallString;
    use chrono::NaiveDate;

    use super::*;

    #[test]
    fn test_eval_len() {
        let expr = Box::new(Expr::LitStr("hello".to_string()));
        let stored_variables = HashMap::new();
        let result = eval_len(None, None, &stored_variables, &expr).unwrap();
        assert_eq!(result, Value::Int(5));
    }

    #[test]
    fn test_eval_substr() {
        let expr = Box::new(Expr::LitStr("hello".to_string()));
        let from = Box::new(Expr::LitInt(1));
        let len = Box::new(Expr::LitInt(3));
        let stored_variables = HashMap::new();
        let result = eval_substr(None, None, &stored_variables, &expr, &from, &len).unwrap();
        assert_eq!(result, Value::Str(SmallString::from("ell".to_string())));
    }

    #[test]
    fn test_eval_concat() {
        let expr1 = Box::new(Expr::LitStr("hello".to_string()));
        let expr2 = Box::new(Expr::LitStr(" world".to_string()));
        let stored_variables = HashMap::new();
        let result = eval_concat(None, None, &stored_variables, &expr1, &expr2).unwrap();
        assert_eq!(
            result,
            Value::Str(SmallString::from("hello world".to_string()))
        );
    }

    #[test]
    fn test_eval_trim() {
        let expr = Box::new(Expr::LitStr("  hello  ".to_string()));
        let stored_variables = HashMap::new();
        let result = eval_trim(None, None, &stored_variables, &expr).unwrap();
        assert_eq!(result, Value::Str(SmallString::from("hello".to_string())));
    }

    #[test]
    fn test_eval_lower() {
        let expr = Box::new(Expr::LitStr("HELLO".to_string()));
        let stored_variables = HashMap::new();
        let result = eval_lower(None, None, &stored_variables, &expr).unwrap();
        assert_eq!(result, Value::Str(SmallString::from("hello".to_string())));
    }

    #[test]
    fn test_eval_upper() {
        let expr = Box::new(Expr::LitStr("hello".to_string()));
        let stored_variables = HashMap::new();
        let result = eval_upper(None, None, &stored_variables, &expr).unwrap();
        assert_eq!(result, Value::Str(SmallString::from("HELLO".to_string())));
    }

    #[test]
    fn test_eval_replace() {
        let expr = Box::new(Expr::LitStr("hello".to_string()));
        let old = Box::new(Expr::LitStr("l".to_string()));
        let new = Box::new(Expr::LitStr("r".to_string()));
        let stored_variables = HashMap::new();
        let result = eval_replace(None, None, &stored_variables, &expr, &old, &new).unwrap();
        assert_eq!(result, Value::Str(SmallString::from("herro".to_string())));
    }

    #[test]
    fn test_eval_starts_with() {
        let expr = Box::new(Expr::LitStr("hello".to_string()));
        let start = Box::new(Expr::LitStr("he".to_string()));
        let stored_variables = HashMap::new();
        let result = eval_starts_with(None, None, &stored_variables, &expr, &start).unwrap();
        assert_eq!(result, Value::Bool(true));
    }

    #[test]
    fn test_eval_ends_with() {
        let expr = Box::new(Expr::LitStr("hello".to_string()));
        let end = Box::new(Expr::LitStr("lo".to_string()));
        let stored_variables = HashMap::new();
        let result = eval_ends_with(None, None, &stored_variables, &expr, &end).unwrap();
        assert_eq!(result, Value::Bool(true));
    }

    #[test]
    fn test_eval_contains() {
        let expr = Box::new(Expr::LitStr("hello".to_string()));
        let contain = Box::new(Expr::LitStr("ell".to_string()));
        let stored_variables = HashMap::new();
        let result = eval_contains(None, None, &stored_variables, &expr, &contain).unwrap();
        assert_eq!(result, Value::Bool(true));
    }

    #[test]
    fn test_eval_invalid_arguments() {
        let expr1 = Box::new(Expr::LitStr("hello".to_string()));
        let expr2 = Box::new(Expr::LitInt(10));
        let stored_variables = HashMap::new();
        let result = eval_concat(None, None, &stored_variables, &expr1, &expr2);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().to_string(), "Invalid arguments for concat. It expects compatible types. But the provided value types are Str, Int");
    }
}
