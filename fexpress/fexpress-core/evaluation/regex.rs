use crate::ast::core::BExpr;
use crate::map::HashMap;
use crate::sstring::SmallString;
use anyhow::{anyhow, Result};
use regex::Regex;

use crate::eval::{eval_simple_expr, EvalContext};
use crate::event::Event;
use crate::types::{Timestamp, INT};
use crate::value::{Value, ValueType};

pub fn eval_regex_match(
    event: Option<&Event>,
    context: Option<&EvalContext>,
    stored_variables: &HashMap<SmallString, HashMap<Timestamp, Value>>,
    expr: &BExpr,
    pattern: &BExpr,
) -> Result<Value> {
    let string = eval_simple_expr(expr, event, context, stored_variables)?;
    let pattern = eval_simple_expr(pattern, event, context, stored_variables)?;
    match (string, pattern) {
        (Value::Str(s), Value::Str(pattern)) => {
            let re = Regex::new(&pattern).map_err(|_| anyhow!("Invalid regex pattern"))?;
            Ok(Value::Bool(re.is_match(&s)))
        }
        (string, pattern) => {
            let string_type: ValueType = string.into();
            let pattern_type: ValueType = pattern.into();
            Err(anyhow!("Invalid arguments for regex_match. It expects Str, Str. But the provided value types are {}, {}", string_type, pattern_type))
        }
    }
}

pub fn eval_regex_extract(
    event: Option<&Event>,
    context: Option<&EvalContext>,
    stored_variables: &HashMap<SmallString, HashMap<Timestamp, Value>>,
    expr: &BExpr,
    pattern: &BExpr,
) -> Result<Value> {
    let string = eval_simple_expr(expr, event, context, stored_variables)?;
    let pattern = eval_simple_expr(pattern, event, context, stored_variables)?;
    match (string, pattern) {
        (Value::Str(s), Value::Str(pattern)) => {
            let re = Regex::new(&pattern).map_err(|_| anyhow!("Invalid regex pattern"))?;
            match re.captures(&s) {
                Some(captures) => Ok(Value::Str(SmallString::from(
                    captures.get(0).map_or("", |m| m.as_str()).to_string(),
                ))),
                None => Ok(Value::None),
            }
        }
        (string, pattern) => {
            let string_type: ValueType = string.into();
            let pattern_type: ValueType = pattern.into();
            Err(anyhow!("Invalid arguments for regex_extract. It expects Str, Str. But the provided value types are {}, {}", string_type, pattern_type))
        }
    }
}

pub fn eval_regex_replace(
    event: Option<&Event>,
    context: Option<&EvalContext>,
    stored_variables: &HashMap<SmallString, HashMap<Timestamp, Value>>,
    expr: &BExpr,
    pattern: &BExpr,
    repl: &BExpr,
) -> Result<Value> {
    let string = eval_simple_expr(expr, event, context, stored_variables)?;
    let pattern = eval_simple_expr(pattern, event, context, stored_variables)?;
    let repl = eval_simple_expr(repl, event, context, stored_variables)?;
    match (string, pattern, repl) {
        (Value::Str(s), Value::Str(pattern), Value::Str(repl)) => {
            let re = Regex::new(&pattern).map_err(|_| anyhow!("Invalid regex pattern"))?;
            let result = re.replace_all(&s.to_string(), repl.to_string()).to_string();
            Ok(Value::Str(SmallString::from(result)))
        }
        (string, pattern, repl) => {
            let string_type: ValueType = string.into();
            let pattern_type: ValueType = pattern.into();
            let repl_type: ValueType = repl.into();
            Err(anyhow!("Invalid arguments for regex_replace. It expects Str, Str, Str. But the provided value types are {}, {}, {}", string_type, pattern_type, repl_type))
        }
    }
}

pub fn eval_regex_split(
    event: Option<&Event>,
    context: Option<&EvalContext>,
    stored_variables: &HashMap<SmallString, HashMap<Timestamp, Value>>,
    expr: &BExpr,
    pattern: &BExpr,
) -> Result<Value> {
    let string = eval_simple_expr(expr, event, context, stored_variables)?;
    let pattern = eval_simple_expr(pattern, event, context, stored_variables)?;
    match (string, pattern) {
        (Value::Str(s), Value::Str(pattern)) => {
            let re = Regex::new(&pattern).map_err(|_| anyhow!("Invalid regex pattern"))?;
            let result: Vec<_> = re
                .split(&s)
                .map(|x| SmallString::from(x.to_string()))
                .collect();
            Ok(Value::VecStr(result))
        }
        (string, pattern) => {
            let string_type: ValueType = string.into();
            let pattern_type: ValueType = pattern.into();
            Err(anyhow!("Invalid arguments for regex_split. It expects Str, Str. But the provided value types are {}, {}", string_type, pattern_type))
        }
    }
}

pub fn eval_regex_count(
    event: Option<&Event>,
    context: Option<&EvalContext>,
    stored_variables: &HashMap<SmallString, HashMap<Timestamp, Value>>,
    expr: &BExpr,
    pattern: &BExpr,
) -> Result<Value> {
    let string = eval_simple_expr(expr, event, context, stored_variables)?;
    let pattern = eval_simple_expr(pattern, event, context, stored_variables)?;
    match (string, pattern) {
        (Value::Str(s), Value::Str(pattern)) => {
            let re = Regex::new(&pattern).map_err(|_| anyhow!("Invalid regex pattern"))?;
            let count = re.find_iter(&s).count();
            Ok(Value::Int(count as INT))
        }
        (string, pattern) => {
            let string_type: ValueType = string.into();
            let pattern_type: ValueType = pattern.into();
            Err(anyhow!("Invalid arguments for regex_count. It expects Str, Str. But the provided value types are {}, {}", string_type, pattern_type))
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::ast::core::Expr;
    use crate::sstring::SmallString;
    use itertools::sorted;

    use crate::value::Value;

    use super::*;

    #[test]
    fn test_eval_matches() {
        let expr = Box::new(Expr::LitStr("hello".to_string()));
        let pattern = Box::new(Expr::LitStr("l+".to_string()));
        let stored_variables = HashMap::new();
        let result = eval_regex_match(None, None, &stored_variables, &expr, &pattern).unwrap();
        assert_eq!(result, Value::Bool(true));
    }

    #[test]
    fn test_eval_replace_regex() {
        let expr = Box::new(Expr::LitStr("hello".to_string()));
        let pattern = Box::new(Expr::LitStr("l+".to_string()));
        let new = Box::new(Expr::LitStr("r".to_string()));
        let stored_variables = HashMap::new();
        let result =
            eval_regex_replace(None, None, &stored_variables, &expr, &pattern, &new).unwrap();
        assert_eq!(result, Value::Str(SmallString::from("hero".to_string())));
    }

    #[test]
    fn test_eval_extract_regex() {
        let expr = Box::new(Expr::LitStr("hello".to_string()));
        let pattern = Box::new(Expr::LitStr("(l+)".to_string()));
        let stored_variables = HashMap::new();
        let result = eval_regex_extract(None, None, &stored_variables, &expr, &pattern).unwrap();
        assert_eq!(result, Value::Str(SmallString::from("ll".to_string())));
    }

    #[test]
    fn test_eval_regex_count() {
        let expr = Box::new(Expr::LitStr("hello".to_string()));
        let pattern = Box::new(Expr::LitStr("l".to_string()));
        let stored_variables = HashMap::new();
        let result = eval_regex_count(None, None, &stored_variables, &expr, &pattern).unwrap();
        assert_eq!(result, Value::Int(2));
    }

    #[test]
    fn test_eval_split() {
        let expr = Box::new(Expr::LitStr("hello world".to_string()));
        let pattern = Box::new(Expr::LitStr(" ".to_string()));
        let stored_variables = HashMap::new();
        let result = eval_regex_split(None, None, &stored_variables, &expr, &pattern).unwrap();
        assert_eq!(
            result,
            Value::VecStr(vec![
                SmallString::from("hello".to_string()),
                SmallString::from("world".to_string())
            ])
        );
    }

    #[test]
    fn test_eval_split_with_no_matches() {
        let expr = Box::new(Expr::LitStr("hello".to_string()));
        let pattern = Box::new(Expr::LitStr(" ".to_string()));
        let stored_variables = HashMap::new();
        let result = eval_regex_split(None, None, &stored_variables, &expr, &pattern).unwrap();
        assert_eq!(
            result,
            Value::VecStr(vec![SmallString::from("hello".to_string())])
        );
    }

    #[test]
    fn test_eval_split_with_empty_string() {
        let expr = Box::new(Expr::LitStr("".to_string()));
        let pattern = Box::new(Expr::LitStr(" ".to_string()));
        let stored_variables = HashMap::new();
        let result = eval_regex_split(None, None, &stored_variables, &expr, &pattern).unwrap();
        assert_eq!(
            result,
            Value::VecStr(vec![SmallString::from("".to_string())])
        );
    }
}
