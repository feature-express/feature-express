use std::str::FromStr;

use crate::ast::core::{
    AggrExpr, AggregateFunction, Expr, ExprFunc, ExprFuncDiscriminants, HavingExpr, HavingExprType,
    SelectExpr,
};
use crate::sstring::SmallString;
use anyhow::{anyhow, bail, Context, Result};
use convert_case::{Case, Casing};
use lazy_static::lazy_static;
use ordered_float::OrderedFloat;
use pest::iterators::Pairs;
use pest::{
    iterators::Pair,
    pratt_parser::{Assoc, Op, PrattParser},
    Parser,
};
use pest_derive::Parser;
use strum::IntoEnumIterator;

use crate::event::AttributeKey;
use crate::interval::{
    BetweenDatesExpressions, Direction, DirectionOnly, FixedInterval, KeywordInterval, NewInterval,
    Unit,
};

use crate::types::{FLOAT, INT};
use crate::value::ValueType;

// TODO: lots of unwraps here

lazy_static! {
    static ref PARSER: PrattParser<Rule> = PrattParser::new()
        .op(Op::infix(Rule::comma, Assoc::Left))
        .op(Op::infix(Rule::and, Assoc::Left) | Op::infix(Rule::or, Assoc::Left))
        .op(Op::infix(Rule::eq, Assoc::Left)
            | Op::infix(Rule::neq, Assoc::Left)
            | Op::infix(Rule::lt, Assoc::Left)
            | Op::infix(Rule::lte, Assoc::Left)
            | Op::infix(Rule::gt, Assoc::Left)
            | Op::infix(Rule::gte, Assoc::Left))
        .op(Op::infix(Rule::add, Assoc::Left) | Op::infix(Rule::sub, Assoc::Left))
        .op(Op::infix(Rule::mul, Assoc::Left) | Op::infix(Rule::div, Assoc::Left))
        .op(Op::infix(Rule::pow, Assoc::Right))
        .op(Op::postfix(Rule::EOI))
        // .op(Op::postfix(Rule::comma))
        .op(Op::postfix(Rule::alias));
}

#[derive(Parser)]
#[grammar = "fexpress-core/expr.pest"]
pub struct ExprParser;

pub fn parse_untyped_attr(attr_name: AttributeKey, value_type: &ValueType) -> Expr {
    match value_type {
        ValueType::Bool => Expr::AttrBool(attr_name),
        ValueType::Num => Expr::AttrNum(attr_name),
        ValueType::Int => Expr::AttrInt(attr_name),
        ValueType::MapNum => Expr::AttrMapNum(attr_name),
        ValueType::Str => Expr::AttrStr(attr_name),
        ValueType::Date => Expr::AttrDate(attr_name),
        ValueType::DateTime => Expr::AttrDateTime(attr_name),
        ValueType::VecCat => Expr::AttrVecStr(attr_name),
        ValueType::VecNum => Expr::AttrVecNum(attr_name),
        ValueType::VecInt => Expr::AttrVecInt(attr_name),
        ValueType::VecBool => Expr::AttrVecBool(attr_name),
        ValueType::Map => Expr::AttrMapNum(attr_name),
        ValueType::MapStr => Expr::AttrMapStr(attr_name),
        ValueType::None => Expr::None,
        ValueType::Wildcard => Expr::Wildcard,
        ValueType::NotCalculatedYet => Expr::None,
    }
}

pub fn generate_ast(pairs: Pairs<Rule>) -> Expr {
    PARSER
        .map_primary(build_term)
        .map_infix(build_infix)
        .map_postfix(|lhs, op| match op.as_rule() {
            Rule::EOI => lhs,
            Rule::alias => lhs,
            Rule::comma => lhs,
            _ => unreachable!(),
        })
        .parse(pairs)
}

fn build_infix(lhs: Expr, op: Pair<Rule>, rhs: Expr) -> Expr {
    match op.as_rule() {
        Rule::add => Expr::Add(Box::new(lhs), Box::new(rhs)),
        Rule::sub => Expr::Sub(Box::new(lhs), Box::new(rhs)),
        Rule::mul => Expr::Mul(Box::new(lhs), Box::new(rhs)),
        Rule::div => Expr::Div(Box::new(lhs), Box::new(rhs)),
        Rule::pow => Expr::Function(ExprFunc::Powf(Box::new(lhs), Box::new(rhs))),
        Rule::eq => Expr::Eq(Box::new(lhs), Box::new(rhs)),
        Rule::neq => Expr::Neq(Box::new(lhs), Box::new(rhs)),
        Rule::lt => Expr::Less(Box::new(lhs), Box::new(rhs)),
        Rule::gt => Expr::Greater(Box::new(lhs), Box::new(rhs)),
        Rule::lte => Expr::LessEq(Box::new(lhs), Box::new(rhs)),
        Rule::gte => Expr::GreaterEq(Box::new(lhs), Box::new(rhs)),
        Rule::and => Expr::And(Box::new(lhs), Box::new(rhs)),
        Rule::or => Expr::Or(Box::new(lhs), Box::new(rhs)),
        Rule::comma => Expr::Cons(Box::new(lhs), Box::new(rhs)),
        rule => Expr::ParsingError(format!("Unexpected rule: {:?}", rule)),
    }
}
pub fn build_term(pair: Pair<Rule>) -> Expr {
    match pair.as_rule() {
        Rule::full_query => parse_full_query(pair.into_inner()),
        // Rule::expr_without_alias => Expr::Alias(
        //     SmallString::from(pair.as_str().to_owned()),
        //     Box::new(generate_ast(pair.into_inner())),
        // ),
        Rule::expr_without_alias => generate_ast(pair.into_inner()),
        Rule::expr_with_alias => parse_expr_with_alias(pair.into_inner()),
        Rule::lit_int => pair
            .as_str()
            .parse::<INT>()
            .map(Expr::LitInt)
            .unwrap_or_else(|e| Expr::ParsingError(format!("Failed to parse integer: {}", e))),
        Rule::lit_float => pair
            .as_str()
            .parse::<FLOAT>()
            .map(OrderedFloat)
            .map(Expr::LitNum)
            .unwrap_or_else(|e| Expr::ParsingError(format!("Failed to parse float: {}", e))),
        Rule::lit_bool => pair
            .as_str()
            .parse::<bool>()
            .map(Expr::LitBool)
            .unwrap_or_else(|e| Expr::ParsingError(format!("Failed to parse boolean: {}", e))),
        Rule::lit_bool_true => Expr::LitBool(true),
        Rule::lit_bool_false => Expr::LitBool(false),
        Rule::lit_string => Expr::LitStr(pair.into_inner().as_str().to_string()),
        Rule::lit_null => Expr::None,
        Rule::symbol_untyped_attr => {
            let attr = pair.as_str();
            if attr.starts_with('@') {
                AttributeKey::from_str(attr.trim_start_matches('@'))
                    .map(Expr::ContextAttr)
                    .unwrap_or_else(|_| Expr::ParsingError("Wrong attribute".into()))
            } else {
                AttributeKey::from_str(attr)
                    .map(Expr::AttrUntyped)
                    .unwrap_or_else(|_| Expr::ParsingError("Wrong attribute".into()))
            }
        }
        // Rule::entity_id => {
        //     Expr::EntityId(pair.into_inner().as_str().trim_start_matches("@").into())
        // }
        Rule::obs_dt => Expr::ObservationDate,
        Rule::event_id => Expr::EventId,
        Rule::event_type => Expr::EventType,
        Rule::event_time => Expr::EventTime,
        Rule::expr | Rule::groupby_expr | Rule::where_expr => generate_ast(pair.into_inner()),
        Rule::binary_expr => generate_ast(pair.into_inner()),
        Rule::func0 | Rule::func1 | Rule::func2 | Rule::func3 => {
            parse_pairs_with_error_handling(pair.into_inner(), parse_func)
        }
        Rule::having_expr => parse_having_expr(pair.into_inner()),
        Rule::aggfunc0 => parse_pair_with_error_handling(pair, parse_aggfunc0),
        Rule::aggfunc1 => parse_pair_with_error_handling(pair, parse_aggfunc1),
        Rule::attr => {
            let attr = pair.as_str();
            if attr.starts_with('@') {
                AttributeKey::from_str(attr.trim_start_matches('@'))
                    .map(Expr::ContextAttr)
                    .unwrap_or_else(|_| Expr::ParsingError("Wrong attribute".into()))
            } else {
                AttributeKey::from_str(attr)
                    .map(Expr::AttrUntyped)
                    .unwrap_or_else(|_| Expr::ParsingError("Wrong attribute".into()))
            }
        }
        Rule::funcarg => generate_ast(pair.into_inner()),
        Rule::in_expr => parse_pairs_with_error_handling(pair.into_inner(), parse_in),
        Rule::wildcard => Expr::Wildcard,
        Rule::variable_assignment => parse_variable_assignment(pair.into_inner())
            .unwrap_or_else(|_| Expr::ParsingError("Cannot parse variable assignment".into())),
        Rule::date_from_expr => generate_ast(pair.into_inner()),
        Rule::date_to_expr => generate_ast(pair.into_inner()),
        pair => Expr::ParsingError(format!("Unexpected term rule: {:?}", pair)),
    }
}

pub fn parse_variable_assignment(pairs: Pairs<Rule>) -> Result<Expr> {
    let mut pairs = pairs.clone();
    let variable = pairs.next().context("Cannot extract variable name")?;
    let variable_name = variable.as_str().trim_start_matches("@").to_owned();
    let inner_expr = build_term(pairs.next().context("Cannot extract variable definition")?);
    Ok(Expr::VariableAssign(
        SmallString::from(variable_name),
        Box::new(inner_expr),
    ))
}

pub fn parse_full_query(pairs: Pairs<Rule>) -> Expr {
    let mut expressions = vec![];
    for pair in pairs.into_iter() {
        let maybe_parsed_expression = match pair.as_rule() {
            Rule::variable_assignment => Some(build_term(pair)),
            Rule::expr_with_alias => Some(build_term(pair)),
            Rule::expr_without_alias => Some(build_term(pair)),
            _ => None,
        };
        if let Some(parsed_expression) = maybe_parsed_expression {
            expressions.push(parsed_expression)
        }
    }
    Expr::Select(SelectExpr {
        expressions: expressions,
    })
}

pub fn parse_in(pairs: Pairs<Rule>) -> Result<Expr> {
    let mut pairs = pairs.clone();
    let expr = pairs.next().ok_or(anyhow!("Missing expression"))?;
    let in_or_not_in = pairs.next().ok_or(anyhow!("Missing in/not in"))?;
    let tuple = pairs.next().ok_or(anyhow!("Missing tuple"))?;
    let expr_term = build_term(expr);
    let tuple_expr = match tuple.as_rule() {
        Rule::lit_tuple_string => {
            let mut v: Vec<String> = vec![];
            for element in tuple.into_inner() {
                let el = element
                    .clone()
                    .into_inner()
                    .next()
                    .context("Cannot build tuples")?
                    .as_span()
                    .as_str()
                    .to_string();
                v.push(el);
            }
            Expr::TupleLitStr(v)
        }
        Rule::lit_tuple_int => {
            let mut v: Vec<INT> = vec![];
            for element in tuple.into_inner() {
                let el = element.clone().as_span().as_str();
                v.push(INT::from_str(el).context("Cannot parse tuple element as INT")?);
            }
            Expr::TupleLitInt(v)
        }
        Rule::lit_tuple_bool => {
            let mut v: Vec<bool> = vec![];
            for element in tuple.into_inner() {
                let el = element
                    .clone()
                    .into_inner()
                    .next()
                    .context("Cannot build tuples")?
                    .as_span()
                    .as_str();
                v.push(bool::from_str(el).context("Cannot parse tuple element as bool")?);
            }
            Expr::TupleLitBool(v)
        }
        Rule::lit_tuple_float => {
            let mut v: Vec<OrderedFloat<FLOAT>> = vec![];
            for element in tuple.into_inner() {
                let el = element.clone().as_span().as_str();
                v.push(OrderedFloat(
                    FLOAT::from_str(el).context("Cannot parse tuple element as FLOAT")?,
                ));
            }
            Expr::TupleLitNum(v)
        }
        _ => bail!("Not implemented"),
    };

    Ok(match in_or_not_in.as_rule() {
        Rule::in_op => Expr::In(Box::new(expr_term), Box::new(tuple_expr)),
        Rule::not_in_op => Expr::NotIn(Box::new(expr_term), Box::new(tuple_expr)),
        _ => bail!("Not implemented"),
    })
}

pub fn parse_expr_with_alias(pairs: Pairs<Rule>) -> Expr {
    let mut pairs = pairs.clone();
    let main_part = pairs.next();
    let alias_part = pairs.next().and_then(|v| v.into_inner().next());
    match (main_part, alias_part) {
        (Some(main_part), Some(alias_part)) => {
            let main_expr = build_term(main_part);
            let alias = alias_part.as_str().to_string();
            Expr::Alias(SmallString::from(alias), Box::new(main_expr))
        }
        _ => Expr::ParsingError("Cannot parse alias expression".to_string()),
    }
}

// converts Result<Expr> -> Expr where the error is converted into Expr::ParsingError(err)
pub fn parse_pairs_with_error_handling<F: Fn(Pairs<Rule>) -> Result<Expr>>(
    pairs: Pairs<Rule>,
    f: F,
) -> Expr {
    match f(pairs) {
        Ok(expr) => expr,
        Err(err) => Expr::ParsingError(err.to_string()),
    }
}

pub fn parse_pair_with_error_handling<F: Fn(Pair<Rule>) -> Result<Expr>>(
    pair: Pair<Rule>,
    f: F,
) -> Expr {
    match f(pair) {
        Ok(expr) => expr,
        Err(err) => Expr::ParsingError(err.to_string()),
    }
}

pub fn parse_func(pairs: Pairs<Rule>) -> Result<Expr> {
    let mut pairs = pairs.clone();
    let name = pairs
        .next()
        .ok_or(anyhow!("Missing function name"))?
        .as_str()
        .to_ascii_lowercase();
    let mut args = Vec::new();
    for pair in pairs {
        args.push(build_term(pair));
    }
    parse_function_name(&name, args)
}

fn parse_function_name(name: &str, args: Vec<Expr>) -> Result<Expr> {
    let variant_string = name.to_lowercase();

    let variant = ExprFuncDiscriminants::iter()
        .find(|discriminant| discriminant.to_string().to_case(Case::Snake) == variant_string);

    macro_rules! zero_arg_func {
        ($func:expr) => {{
            if args.len() == 0 {
                $func
            } else {
                bail!("Wrong number of arguments for {}", variant_string);
            }
        }};
    }

    macro_rules! one_arg_func {
        ($func:expr) => {{
            if args.len() == 1 {
                $func(Box::new(args[0].clone()))
            } else {
                bail!("Wrong number of arguments for {}", variant_string);
            }
        }};
    }

    macro_rules! two_arg_func {
        ($func:expr) => {{
            if args.len() == 2 {
                $func(Box::new(args[0].clone()), Box::new(args[1].clone()))
            } else {
                bail!("Wrong number of arguments for {}", variant_string);
            }
        }};
    }

    macro_rules! three_arg_func {
        ($func:expr) => {{
            if args.len() == 3 {
                $func(
                    Box::new(args[0].clone()),
                    Box::new(args[1].clone()),
                    Box::new(args[2].clone()),
                )
            } else {
                bail!("Wrong number of arguments for {}", variant_string);
            }
        }};
    }

    let expr_func = if let Some(variant) = variant {
        match variant {
            ExprFuncDiscriminants::Now => zero_arg_func!(ExprFunc::Now),
            ExprFuncDiscriminants::CurrentDate => zero_arg_func!(ExprFunc::CurrentDate),
            ExprFuncDiscriminants::CurrentTime => zero_arg_func!(ExprFunc::CurrentTime),
            ExprFuncDiscriminants::Floor => one_arg_func!(ExprFunc::Floor),
            ExprFuncDiscriminants::Ceil => one_arg_func!(ExprFunc::Ceil),
            ExprFuncDiscriminants::Round => one_arg_func!(ExprFunc::Round),
            ExprFuncDiscriminants::Trunc => one_arg_func!(ExprFunc::Trunc),
            ExprFuncDiscriminants::Fract => one_arg_func!(ExprFunc::Fract),
            ExprFuncDiscriminants::Abs => one_arg_func!(ExprFunc::Abs),
            ExprFuncDiscriminants::Signum => one_arg_func!(ExprFunc::Signum),
            ExprFuncDiscriminants::Sqrt => one_arg_func!(ExprFunc::Sqrt),
            ExprFuncDiscriminants::Exp => one_arg_func!(ExprFunc::Exp),
            ExprFuncDiscriminants::Exp2 => one_arg_func!(ExprFunc::Exp2),
            ExprFuncDiscriminants::Ln => one_arg_func!(ExprFunc::Ln),
            ExprFuncDiscriminants::Log2 => one_arg_func!(ExprFunc::Log2),
            ExprFuncDiscriminants::Log10 => one_arg_func!(ExprFunc::Log10),
            ExprFuncDiscriminants::Sin => one_arg_func!(ExprFunc::Sin),
            ExprFuncDiscriminants::Cos => one_arg_func!(ExprFunc::Cos),
            ExprFuncDiscriminants::Tan => one_arg_func!(ExprFunc::Tan),
            ExprFuncDiscriminants::Asin => one_arg_func!(ExprFunc::Asin),
            ExprFuncDiscriminants::Acos => one_arg_func!(ExprFunc::Acos),
            ExprFuncDiscriminants::Atan => one_arg_func!(ExprFunc::Atan),
            ExprFuncDiscriminants::Expm1 => one_arg_func!(ExprFunc::Expm1),
            ExprFuncDiscriminants::Ln1p => one_arg_func!(ExprFunc::Ln1p),
            ExprFuncDiscriminants::Sinh => one_arg_func!(ExprFunc::Sinh),
            ExprFuncDiscriminants::Cosh => one_arg_func!(ExprFunc::Cosh),
            ExprFuncDiscriminants::Asinh => one_arg_func!(ExprFunc::Asinh),
            ExprFuncDiscriminants::Acosh => one_arg_func!(ExprFunc::Acosh),
            ExprFuncDiscriminants::Atanh => one_arg_func!(ExprFunc::Atanh),
            ExprFuncDiscriminants::Log => two_arg_func!(ExprFunc::Log),
            ExprFuncDiscriminants::DivEuclid => two_arg_func!(ExprFunc::DivEuclid),
            ExprFuncDiscriminants::RemEuclid => two_arg_func!(ExprFunc::RemEuclid),
            ExprFuncDiscriminants::Clamp => three_arg_func!(ExprFunc::Clamp),
            ExprFuncDiscriminants::Powf => two_arg_func!(ExprFunc::Powf),
            ExprFuncDiscriminants::If => three_arg_func!(ExprFunc::If),
            ExprFuncDiscriminants::Len => one_arg_func!(ExprFunc::Len),
            ExprFuncDiscriminants::Substr => three_arg_func!(ExprFunc::Substr),
            ExprFuncDiscriminants::Concat => two_arg_func!(ExprFunc::Concat),
            ExprFuncDiscriminants::Trim => one_arg_func!(ExprFunc::Trim),
            ExprFuncDiscriminants::Lower => one_arg_func!(ExprFunc::Lower),
            ExprFuncDiscriminants::Upper => one_arg_func!(ExprFunc::Upper),
            ExprFuncDiscriminants::Replace => three_arg_func!(ExprFunc::Replace),
            ExprFuncDiscriminants::StartsWith => two_arg_func!(ExprFunc::StartsWith),
            ExprFuncDiscriminants::EndsWith => two_arg_func!(ExprFunc::EndsWith),
            ExprFuncDiscriminants::Contains => two_arg_func!(ExprFunc::Contains),
            ExprFuncDiscriminants::DateDiff => two_arg_func!(ExprFunc::DateDiff),
            ExprFuncDiscriminants::Year => one_arg_func!(ExprFunc::Year),
            ExprFuncDiscriminants::Month => one_arg_func!(ExprFunc::Month),
            ExprFuncDiscriminants::Day => one_arg_func!(ExprFunc::Day),
            ExprFuncDiscriminants::Week => one_arg_func!(ExprFunc::Week),
            ExprFuncDiscriminants::Weekday => one_arg_func!(ExprFunc::Weekday),
            ExprFuncDiscriminants::Date => one_arg_func!(ExprFunc::Date),
            ExprFuncDiscriminants::DayOfYear => one_arg_func!(ExprFunc::DayOfYear),
            ExprFuncDiscriminants::Quarter => one_arg_func!(ExprFunc::Quarter),
            ExprFuncDiscriminants::IsStartOfMonth => one_arg_func!(ExprFunc::IsStartOfMonth),
            ExprFuncDiscriminants::IsEndOfMonth => one_arg_func!(ExprFunc::IsEndOfMonth),
            ExprFuncDiscriminants::IsWeekend => one_arg_func!(ExprFunc::IsWeekend),
            ExprFuncDiscriminants::DateAdd => two_arg_func!(ExprFunc::DateAdd),
            ExprFuncDiscriminants::DateSub => two_arg_func!(ExprFunc::DateSub),
            ExprFuncDiscriminants::Hour => one_arg_func!(ExprFunc::Hour),
            ExprFuncDiscriminants::Minute => one_arg_func!(ExprFunc::Minute),
            ExprFuncDiscriminants::Second => one_arg_func!(ExprFunc::Second),
            ExprFuncDiscriminants::Microsecond => one_arg_func!(ExprFunc::Microsecond),
            ExprFuncDiscriminants::DatePart => two_arg_func!(ExprFunc::DatePart),
            ExprFuncDiscriminants::Extract => two_arg_func!(ExprFunc::Extract),
            ExprFuncDiscriminants::FormatDate => two_arg_func!(ExprFunc::FormatDate),
            ExprFuncDiscriminants::Coalesce => two_arg_func!(ExprFunc::Coalesce),
            ExprFuncDiscriminants::RegexMatch => two_arg_func!(ExprFunc::RegexMatch),
            ExprFuncDiscriminants::RegexExtract => two_arg_func!(ExprFunc::RegexExtract),
            ExprFuncDiscriminants::RegexReplace => three_arg_func!(ExprFunc::RegexReplace),
            ExprFuncDiscriminants::RegexSplit => two_arg_func!(ExprFunc::RegexSplit),
            ExprFuncDiscriminants::RegexCount => two_arg_func!(ExprFunc::RegexCount),
        }
    } else {
        let possible_variants = ExprFuncDiscriminants::iter()
            .map(|v| v.to_string().to_case(Case::Snake))
            .filter(|v| strsim::normalized_damerau_levenshtein(v, &variant_string) > 0.75)
            .collect::<Vec<_>>()
            .join(",");
        bail!(
            "No function with the {:?} name was found. Did you mean one of these: {:?}?",
            name,
            possible_variants
        );
    };

    Ok(Expr::Function(expr_func))
}

pub fn extract_rule(pairs: Vec<Pair<Rule>>, rule: Rule) -> Option<Pair<Rule>> {
    for pair in pairs.into_iter() {
        if pair.as_rule() == rule {
            return Some(pair.clone());
        }
    }
    None
}

pub fn extract_rules(pairs: Vec<Pair<Rule>>, rule: Rule) -> Vec<Pair<Rule>> {
    pairs.into_iter().filter(|p| p.as_rule() == rule).collect()
}

pub fn extract_rule_from_pairs(pairs: Pairs<Rule>, rule: Rule) -> Option<Pair<Rule>> {
    for pair in pairs {
        if pair.as_rule() == rule {
            return Some(pair.clone());
        }
    }
    None
}

pub fn parse_interval(pair: Pair<Rule>) -> Result<NewInterval> {
    let new_interval = match pair.as_rule() {
        Rule::fixed_interval => NewInterval::FixedInterval(FixedInterval {
            direction: Direction::from_str(
                extract_rule_from_pairs(pair.clone().into_inner(), Rule::direction)
                    .with_context(|| format!("Cannot parse interval {:?}", pair))?
                    .as_str(),
            )
            .unwrap(),
            int: usize::from_str(
                extract_rule_from_pairs(pair.clone().into_inner(), Rule::integer)
                    .with_context(|| format!("Cannot parse interval {:?}", pair))?
                    .as_str(),
            )
            .unwrap(),
            unit: Unit::from_str(
                extract_rule_from_pairs(pair.clone().into_inner(), Rule::unit)
                    .with_context(|| format!("Cannot parse interval {:?}", pair))?
                    .as_str()
                    .trim(),
            )
            .unwrap(),
        }),
        Rule::direction_only => {
            let v = pair.as_str().to_ascii_lowercase();
            NewInterval::DirectionOnly(DirectionOnly::from_str(&v)?)
        }
        Rule::keyword_interval => {
            let v = pair.as_str().to_ascii_lowercase();
            NewInterval::KeywordDate(KeywordInterval::from_str(&v)?)
        }
        Rule::between_dates => {
            let date_from_expr =
                extract_rule_from_pairs(pair.clone().into_inner(), Rule::date_from_expr)
                    .with_context(|| format!("Cannot parse interval {:?}", pair))?;
            let date_from_expr = build_term(date_from_expr);
            let date_to_expr =
                extract_rule_from_pairs(pair.clone().into_inner(), Rule::date_to_expr)
                    .with_context(|| format!("Cannot parse interval {:?}", pair))?;
            let date_to_expr = build_term(date_to_expr);
            NewInterval::BetweenDatesExpressions(BetweenDatesExpressions {
                from_date: Box::new(date_from_expr),
                to_date: Box::new(date_to_expr),
            })
        }
        _ => unimplemented!(),
    };
    Ok(new_interval)
}

fn parse_aggfunc0(pair: Pair<Rule>) -> Result<Expr> {
    let inner_pairs = pair.into_inner().collect::<Vec<_>>();
    let name_binding = extract_rule(inner_pairs.clone(), Rule::funcname)
        .ok_or(anyhow!("Missing function name"))?
        .as_str()
        .to_ascii_lowercase();
    let name = name_binding.as_str();

    let arg1 = build_term(
        extract_rule(inner_pairs.clone(), Rule::funcarg)
            .ok_or(anyhow!("Missing function argument"))?,
    );

    let interval = extract_rule(inner_pairs.clone(), Rule::interval)
        .map(|p| p.into_inner().next())
        .ok_or(anyhow!("Missing interval"))?
        .map(parse_interval)
        .ok_or(anyhow!("Cannot parse interval"))??;

    let from = extract_rule(inner_pairs.clone(), Rule::from_expr)
        .map(|pair| pair.into_inner())
        .map(|v| v.as_str().to_string());

    let where_expr = extract_rule(inner_pairs.clone(), Rule::where_expr)
        .map(|pair| pair.into_inner())
        .map(generate_ast);

    let groupby_expr = extract_rule(inner_pairs.clone(), Rule::groupby_expr)
        .map(|pair| pair.into_inner())
        .map(generate_ast);

    let having_expr = extract_rule(inner_pairs.clone(), Rule::having_expr)
        .map(build_term)
        .map(|e| match e {
            Expr::Having(having_expr) => having_expr,
            _ => unreachable!(),
        });

    Ok(match_aggr0(
        name,
        arg1,
        interval,
        from.into(),
        groupby_expr,
        where_expr,
        having_expr,
    ))
}

fn parse_aggfunc1(pair: Pair<Rule>) -> Result<Expr> {
    assert_eq!(pair.as_rule(), Rule::aggfunc1);
    let inner_pairs = pair.into_inner().collect::<Vec<_>>();

    let name_binding = extract_rule(inner_pairs.clone(), Rule::funcname)
        .ok_or(anyhow!("Missing function name"))?
        .as_str()
        .to_ascii_lowercase();
    let name = name_binding.as_str();

    let args = extract_rules(inner_pairs.clone(), Rule::funcarg);
    let arg1 = args
        .get(0)
        .ok_or(anyhow!("Missing first function argument"))
        .map(|a| build_term(a.clone()))?;
    let arg2 = args
        .get(1)
        .ok_or(anyhow!("Missing second function argument"))
        .map(|a| build_term(a.clone()))?;

    let interval = extract_rule(inner_pairs.clone(), Rule::interval)
        .map(|p| p.into_inner().next())
        .ok_or(anyhow!("Missing interval"))?
        .map(parse_interval)
        .ok_or(anyhow!("Cannot parse interval"))??;

    let from = extract_rule(inner_pairs.clone(), Rule::from_expr)
        .map(|pair| pair.into_inner())
        .map(|v| v.as_str().to_string());

    let where_expr = extract_rule(inner_pairs.clone(), Rule::where_expr)
        .map(|pair| pair.into_inner())
        .map(generate_ast);

    let groupby_expr = extract_rule(inner_pairs.clone(), Rule::groupby_expr)
        .map(|pair| pair.into_inner())
        .map(generate_ast);

    let having_expr = extract_rule(inner_pairs.clone(), Rule::having_expr)
        .map(build_term)
        .map(|e| match e {
            Expr::Having(having_expr) => having_expr,
            _ => unreachable!(),
        });

    Ok(match_aggr1(
        name,
        arg1,
        arg2,
        interval,
        from,
        groupby_expr,
        where_expr,
        having_expr,
    ))
}

pub fn parse_having_expr(pairs: Pairs<Rule>) -> Expr {
    let mut pairs = pairs.clone();
    let min_or_max = parse_min_or_max(pairs.next().unwrap().as_str());
    let expr = pairs
        .next()
        .map(|pair| pair.into_inner())
        .map(generate_ast)
        .unwrap();
    Expr::Having(HavingExpr {
        typ: min_or_max,
        expr: Box::new(expr),
    })
}

pub fn parse_min_or_max(s: &str) -> HavingExprType {
    match s.to_ascii_lowercase().as_str() {
        "min" => HavingExprType::MIN,
        "max" => HavingExprType::MAX,
        _ => unreachable!(),
    }
}

fn match_aggr0(
    name: &str,
    arg1: Expr,
    interval: NewInterval,
    from: Option<SmallString>,
    groupby_expr: Option<Expr>,
    where_expr: Option<Expr>,
    having_expr: Option<HavingExpr>,
) -> Expr {
    let groupby_expr = groupby_expr.map(Box::new);
    let where_expr = where_expr.map(Box::new);

    macro_rules! create_aggr_expr {
        ($func:expr) => {
            Expr::Aggr(AggrExpr {
                agg_func: $func,
                agg_expr: Box::new(arg1),
                when: interval,
                from: from,
                groupby: groupby_expr,
                cond: where_expr,
                having: having_expr,
            })
        };
    }

    match name {
        "count" => create_aggr_expr!(AggregateFunction::Count),
        "sum" => create_aggr_expr!(AggregateFunction::Sum),
        "min" => create_aggr_expr!(AggregateFunction::Min),
        "max" => create_aggr_expr!(AggregateFunction::Max),
        "avg" => create_aggr_expr!(AggregateFunction::Avg),
        "median" => create_aggr_expr!(AggregateFunction::Median),
        "var" => create_aggr_expr!(AggregateFunction::Var),
        "stdev" => create_aggr_expr!(AggregateFunction::StDev),
        "last" => create_aggr_expr!(AggregateFunction::Last),
        "first" => create_aggr_expr!(AggregateFunction::First),
        "time_of_first" => create_aggr_expr!(AggregateFunction::TimeOfFirst),
        "time_of_last" => create_aggr_expr!(AggregateFunction::TimeOfLast),
        "time_of_next" => create_aggr_expr!(AggregateFunction::TimeOfNext),
        "avg_time_between" => create_aggr_expr!(AggregateFunction::AvgDaysBetween),
        "values" => create_aggr_expr!(AggregateFunction::Values),
        _ => panic!("not implemented {}", name),
    }
}

fn match_aggr1(
    name: &str,
    arg1: Expr,
    arg2: Expr,
    interval: NewInterval,
    from: Option<SmallString>,
    groupby_expr: Option<Expr>,
    where_expr: Option<Expr>,
    having_expr: Option<HavingExpr>,
) -> Expr {
    let groupby_expr = groupby_expr.map(Box::new);
    let where_expr = where_expr.map(Box::new);
    match name {
        "nth" => Expr::Aggr(AggrExpr {
            agg_func: AggregateFunction::Nth(Box::new(arg2)),
            agg_expr: Box::new(arg1),
            when: interval,
            from: from,
            groupby: groupby_expr,
            cond: where_expr,
            having: having_expr,
        }),
        _ => unimplemented!(),
    }
}

impl FromStr for Expr {
    type Err = anyhow::Error;
    fn from_str(s: &str) -> Result<Self> {
        let parsed = if s.to_lowercase().trim().starts_with("select") {
            ExprParser::parse(Rule::full_query, s)
        } else {
            ExprParser::parse(Rule::single_expression, s)
        };
        Ok(generate_ast(parsed?))
    }
}

#[cfg(test)]
mod tests {
    use crate::map::HashMap;
    use anyhow::Result;
    use chrono::Utc;
    use pest::error::Error;

    use crate::eval::{eval_simple_expr, EvalContext};
    use crate::event::{Entity, Event, EventType};
    use crate::event_index::{EventContext, EventScopeConfig, QueryConfig};
    use crate::features::Feature;
    use crate::obs_dates::ObsDate;
    use crate::parser::error_helper::friendly_pest_error;
    use crate::value::Value;

    use super::*;

    #[test]
    fn test_climber() {
        assert_eq!(Value::Num(6.0), eval_str("2+2*2").unwrap());
        assert_eq!(Value::Num(8.0), eval_str("(2+2)*2").unwrap());
        assert_eq!(Value::Num(10.0), eval_str("5*2").unwrap());
    }

    #[test]
    fn test_abs() {
        assert_eq!(Value::Num(6.0), eval_str("abs(-6.0)").unwrap());
        assert_eq!(Value::Num(6.0), eval_str("abs(6.0)").unwrap());
        assert_eq!(Value::Num(0.0), eval_str("abs(-0.0)").unwrap());
        assert_eq!(Value::Num(123.456), eval_str("abs(-123.456)").unwrap());
    }

    #[test]
    fn test_function_math() {
        assert_eq!(Value::Num(3.0), eval_str("abs(-6.0) / abs(2.0)").unwrap());
        assert_eq!(Value::Num(2.0), eval_str("abs(-6.0) / abs(-3.0)").unwrap());
        assert_eq!(Value::Num(0.0), eval_str("abs(-0.0) / abs(2.0)").unwrap());
    }

    #[test]
    fn test_comparison() {
        assert_eq!(Value::Bool(true), eval_str("4 > 3").unwrap());
        assert_eq!(Value::Bool(false), eval_str("3 > 3").unwrap());
        assert_eq!(Value::Bool(true), eval_str("0 < 1").unwrap());
        assert_eq!(Value::Bool(false), eval_str("1 < 1").unwrap());
        assert_eq!(Value::Bool(true), eval_str("4 >= 4").unwrap());
        assert_eq!(Value::Bool(false), eval_str("3 >= 4").unwrap());
        assert_eq!(Value::Bool(true), eval_str("0 <= 0").unwrap());
        assert_eq!(Value::Bool(false), eval_str("1 <= 0").unwrap());
        assert_eq!(Value::Bool(true), eval_str("0 == 0").unwrap());
        assert_eq!(Value::Bool(false), eval_str("0 == 1").unwrap());
    }

    #[test]
    fn test_string_comparison() {
        assert_eq!(Value::Bool(true), eval_str("'test' == 'test'").unwrap());
        assert_eq!(Value::Bool(false), eval_str("'test' == 'wrong'").unwrap());
        assert_eq!(Value::Bool(false), eval_str("'test' != 'test'").unwrap());
        assert_eq!(Value::Bool(true), eval_str("'test' != 'wrong'").unwrap());
    }

    #[test]
    fn test_coalesce() {
        assert_eq!(Value::Bool(true), eval_str("coalesce(null, true)").unwrap());
        assert_eq!(Value::None, eval_str("coalesce(null, null)").unwrap());
        assert_eq!(Value::Int(1), eval_str("coalesce(1, 2)").unwrap());
        assert_eq!(Value::Int(2), eval_str("coalesce(2, null)").unwrap());
    }

    #[test]
    fn test_and() {
        assert_eq!(Value::Bool(true), eval_str("true and true").unwrap());
        assert_eq!(
            Value::Bool(true),
            eval_str("true and true and true").unwrap()
        );
        assert_eq!(Value::Bool(true), eval_str("(0 = 0) and (4 > 3)").unwrap());
        assert_eq!(Value::Bool(false), eval_str("false and true").unwrap());
        assert_eq!(Value::Bool(false), eval_str("true and false").unwrap());
        assert_eq!(Value::Bool(false), eval_str("false and false").unwrap());
    }

    #[test]
    fn test_or() {
        assert_eq!(Value::Bool(true), eval_str("true or true").unwrap());
        assert_eq!(Value::Bool(true), eval_str("true or true or true").unwrap());
        assert_eq!(Value::Bool(true), eval_str("(0 == 0) or (0 != 0)").unwrap());
        assert_eq!(Value::Bool(true), eval_str("false or true").unwrap());
        assert_eq!(Value::Bool(true), eval_str("true or false").unwrap());
        assert_eq!(Value::Bool(false), eval_str("false or false").unwrap());
    }

    #[test]
    fn test_parse_2() {
        let successful_parse =
            ExprParser::parse(Rule::single_expression, r#"avg(pressure) over past"#);
        generate_ast(successful_parse.unwrap());
    }

    #[test]
    fn test_date_diff() {
        let result = eval_str(r#"date_diff('2021-12-31', '2021-01-01')"#).unwrap();
        assert_eq!(result, Value::Int(364))
    }

    #[test]
    fn test_date_week() {
        let result = eval_str(r#"week(date('2021-01-06'))"#).unwrap();
        assert_eq!(result, Value::Int(1))
    }

    #[test]
    fn test_date_month() {
        let result = eval_str(r#"month(date('2021-06-06'))"#).unwrap();
        assert_eq!(result, Value::Int(6))
    }

    #[test]
    fn test_date_day() {
        let result = eval_str(r#"day(date('2021-06-10'))"#).unwrap();
        assert_eq!(result, Value::Int(10))
    }

    #[test]
    fn test_date_year() {
        let result = eval_str(r#"year(date('2021-06-10'))"#).unwrap();
        assert_eq!(result, Value::Int(2021))
    }

    #[test]
    fn test_powf() {
        assert_eq!(Value::Num(4.0), eval_str("powf(2.0, 2)").unwrap());
    }

    #[test]
    fn test_parse_str() {
        assert_eq!(
            Value::Str("pressure".into()),
            eval_str(r#"'pressure'"#).unwrap()
        );
    }

    #[test]
    fn test_if() {
        assert_eq!(
            Value::Bool(true),
            eval_str("if(1 > 0, true, false)").unwrap()
        );
    }

    #[test]
    fn test_parse_event_type() {
        let successful_parse = ExprParser::parse(Rule::single_expression, "event_type == 'test'");
    }

    #[test]
    fn test_aggregate_with_event_type() {
        let successful_parse = ExprParser::parse(
            Rule::single_expression,
            "sum(if(event_type = 'test', 1, 0)) over past",
        );
        let ast = generate_ast(successful_parse.unwrap());
    }

    #[test]
    fn test_aggregate_with_groupby() {
        let successful_parse = ExprParser::parse(
            Rule::single_expression,
            "avg(pressure) over past group by pressure",
        );
        let ast = generate_ast(successful_parse.unwrap());
    }

    #[test]
    fn test_aggregate_with_groupby_alias() {
        let successful_parse = ExprParser::parse(
            Rule::single_expression,
            r#"avg(pressure) over past group by pressure as f"#,
        );
        let ast = generate_ast(successful_parse.unwrap());
        matches!(ast, Expr::Alias(_, _));
    }

    #[test]
    fn test_full_query_parse() {
        let successful_parse = ExprParser::parse(
            Rule::full_query,
            r#"
            select
                @var1 := 1+1,
                avg(pressure) over past as a,
                sum(pressure) over past as b
            for
                @entities := user"#,
        );
        match successful_parse {
            Ok(parsed) => {
                let ast = generate_ast(parsed);
                matches!(ast, Expr::Cons(_, _));
            }
            Err(e) => panic!(e.to_string()),
        }
    }

    #[test]
    fn test_full_query_hanging_comma_parse() {
        let successful_parse = ExprParser::parse(
            Rule::full_query,
            r#"
            SELECT
                @var1 := 1+1,
                avg(pressure) over past as a,
                sum(pressure) over past as b
            FOR
                @entities := user
            "#,
        );
        match successful_parse {
            Ok(parsed) => {
                let ast = generate_ast(parsed);
                matches!(ast, Expr::Cons(_, _));
            }
            Err(e) => panic!(e.to_string()),
        }
    }

    #[test]
    fn test_current_context_parsing() {
        let successful_parse =
            ExprParser::parse(Rule::single_expression, "LAST(movie.category) OVER past WHERE event_type = 'movie' and @a = 1 and entities.movie_id = entities.movie_id");
        let ast = generate_ast(successful_parse.unwrap());
    }

    #[test]
    fn test_alias() {
        let exprs = vec![
            "if(1 > 0, true, false)",
            "1",
            "1 + 1",
            "(count(type) over past) / (count(type) over future)",
            "1 + (count(type) over past) / (count(type) over future)",
            "count(type) over past",
            "count(type) over past where type in ('a', 'b', 'c', 'd')",
            "count(type) over past where type not in ('a', 'b', 'c', 'd')",
            "count(type) over past where tempint in (1, 2, 3, 4)",
            "count(type) over past where tempint not in (1, 2, 3, 4)",
            "count(type) over past where temp in (1.0, 2.0, 3.0, 4.0)",
            "count(type) over past where temp not in (1.0, 2.0, 3.0, 4.0)",
            "last(dict.m) over past",
            "count(type) over past group by dict.m",
            "last(dict) over past",
            "nth(temp, 0) over past",
            "nth(temp, 1) over past",
            "nth(temp, -1) over past",
            "nth(temp, -2) over past",
            "nth(temp, -7) over past",
            "sum(temp) over past",
            "min(temp) over past",
            "max(temp) over past",
            "max(type) over past",
            "last(type) over past",
            "min(event_time) over past",
            "max(event_time) over past",
            "median(temp) over past",
            "first(temp) over past",
            "last(temp) over past",
            "first(type) over past having min temp",
            "first(type) over past having max temp",
            "first(event_time) over past having max pressure",
            "first(event_time) over past having max temp",
        ];
        for expr in exprs {
            let aliased = format!("{} as fff", expr);
            let successful_parse = ExprParser::parse(Rule::single_expression, aliased.as_str());
            let ast = generate_ast(successful_parse.unwrap());
            println!("{:?}", ast);
            matches!(ast, Expr::Alias(_, _));
        }
    }

    #[test]
    fn test_intervals() {
        let expressions = vec![
            // direction_only
            "avg(pressure) over past where pressure == 'static'",
            // fixed_interval
            "avg(pressure) over last 10 days where pressure == 'static'",
            // keyword_interval
            "avg(pressure) over YTD where pressure == 'static'",
        ];
        for expr in expressions {
            let successful_parse = ExprParser::parse(Rule::single_expression, expr);
            let ast = generate_ast(successful_parse.unwrap());
            println!("{:?}", ast);
        }
    }

    #[test]
    fn test_aggregate_with_between_dates() {
        let successful_parse = ExprParser::parse(
            Rule::single_expression,
            "avg(pressure) over between date('2020-01-01') to date('2022-01-01') where pressure == 'static'",
        );
        let ast = generate_ast(successful_parse.unwrap());
    }

    #[test]
    fn test_aggregate_with_where() {
        let successful_parse = ExprParser::parse(
            Rule::single_expression,
            "avg(pressure) over past where pressure == 'static'",
        );
        let ast = generate_ast(successful_parse.unwrap());
    }

    #[test]
    fn test_aggregate_with_groupby_where() {
        let successful_parse = ExprParser::parse(
            Rule::single_expression,
            "avg(pressure) over past where pressure == 'static' group by pressure",
        );
        let ast = generate_ast(successful_parse.unwrap());
    }

    #[test]
    fn test_aggregate_with_having() {
        let successful_parse = ExprParser::parse(
            Rule::single_expression,
            "avg(pressure) over past having max temperature",
        );
        let ast = generate_ast(successful_parse.unwrap());
        println!("{:?}", ast);
    }

    #[test]
    fn test_parse_nth() {
        let successful_parse =
            ExprParser::parse(Rule::single_expression, "nth(pressure, 0) over past");
        let successful_parse = ExprParser::parse(Rule::single_expression, "abs(6.0)");
    }

    fn eval_str(expr_str: &str) -> Result<Value> {
        let successful_parse = ExprParser::parse(Rule::single_expression, expr_str);
        let ast = generate_ast(successful_parse.unwrap());
        let hm = HashMap::new();
        eval_simple_expr(&ast, None, None, &hm)
    }

    #[test]
    fn test_event() {
        let event = Event {
            event_type: EventType("pressure".into()),
            event_time: Utc::now().naive_utc(),
            entities: btreemap!["a".into() => "1".into()],
            attrs: Some(hashmap! {"pressure".into() => Value::Num(1.0)}),
            ..Default::default()
        };
        let datetime = Utc::now().naive_utc().into();
        let mut event_context = EventContext::default();
        let query_config = QueryConfig::default();
        event_context.new_event(event.clone());
        let context = EvalContext {
            event_index: Some(&event_context),
            event_query_config: Some(EventScopeConfig::AllEvents),
            query_config: Some(&query_config),
            entities: Some(btreemap!["a".into() => "1".into()]),
            experiment_id: None,
            obs_date: None,
            obs_time: Some(datetime),
            event_types: vec![],
            event: None,
            event_on_obs_date: None,
        };
        let successful_parse = ExprParser::parse(Rule::single_expression, "pressure");
        let ast = generate_ast(successful_parse.unwrap());
        let hm = HashMap::new();
        let _ = eval_simple_expr(&ast, Some(&event), Some(&context), &hm);
    }

    #[test]
    fn test_event_type_eval() {
        let event = Event {
            event_type: EventType("pressure".into()),
            event_time: Utc::now().naive_utc(),
            entities: btreemap!["a".into() => "1".into()],
            attrs: Some(hashmap!["pressure".into() => Value::Num(1.0)]),
            ..Default::default()
        };

        let successful_parse = ExprParser::parse(
            Rule::single_expression,
            "if(event_type == 'pressure', 1, 0)",
        );
        let ast = generate_ast(successful_parse.unwrap());
        let hm = HashMap::new();
        let result = eval_simple_expr(&ast, Some(&event), None, &hm);
    }

    #[test]
    fn test_parser() {
        let successful_parse = ExprParser::parse(Rule::single_expression, "is_empty");
        let successful_parse =
            ExprParser::parse(Rule::single_expression, "count_of_viewers + count");
    }

    #[test]
    fn test_parser_2() {
        let successful_parse = ExprParser::parse(
            Rule::single_expression,
            "count(1) over past groupby device.browser",
        );
    }

    #[test]
    fn test_parser_4() {
        let f = Feature::from_str("sum(value) over last 10 day where event_type = 'transaction'")
            .expect("feature is correct");
        let successful_parse = ExprParser::parse(
            Rule::single_expression,
            "sum(value) over last 10 day where event_type = 'transaction'",
        );
    }

    #[test]
    fn test_parser_3() {
        let f = Feature::from_str(
            "sum(value.nested) over last 10 day where event_type = 'transaction'",
        )
        .expect("feature is correct");
        let successful_parse = ExprParser::parse(
            Rule::single_expression,
            "sum(value) over last 10 day where event_type = 'transaction'",
        );
    }

    #[test]
    fn test_parse_in() {
        let successful_parse = ExprParser::parse(
            Rule::single_expression,
            "nth(pressure, 0) over past where ea in (1,2,3)",
        );
        match &successful_parse {
            Ok(_) => {}
            Err(e) => {
                _ = friendly_pest_error(e);
            }
        }
        assert!(successful_parse.is_ok(), "Parsing returned an error");
    }

    #[test]
    fn test_parse_in_2() {
        let successful_parse = ExprParser::parse(Rule::in_expr, "ea in (1,2,3)");
        assert!(successful_parse.is_ok(), "Parsing returned an error");
    }

    #[test]
    fn test_parse_in_3() {
        let successful_parse = ExprParser::parse(Rule::lit_tuple, "(1.0,2.0)");
        assert!(successful_parse.is_ok(), "Parsing returned an error");
    }
}
