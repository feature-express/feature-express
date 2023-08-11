use nom::branch::alt;
use nom::sequence::{delimited, tuple};
use nom::combinator::{map, value};
use nom::multi::separated_nonempty_list;
use nom::bytes::complete::tag;
use crate::parser_nom::functions::{aggfunc, func0, func1, func2, func3};
use crate::parser_nom::literals::literal;
use crate::parser_nom::symbols::{attr, event_time, event_type, obs_dt};

// Expressions
pub fn in_expr(input: &str) -> nom::IResult<&str, Expr> {
    map(tuple((in_term, all_in_op, lit_tuple)), |(term, op, tuple)| {
        Expr::InExpr(Box::new(term), op, tuple)
    })(input)
}

pub fn binary_expr(input: &str) -> nom::IResult<&str, Expr> {
    let (input, first_term) = term(input)?;
    let (input, exprs) = separated_nonempty_list(tuple((binary_op, term)), input)?;
    Ok((input, Expr::BinaryExpr(first_term, exprs)))
}

pub fn term(input: &str) -> nom::IResult<&str, Expr> {
    alt((
        literal,
        delimited(tag("("), binary_expr, tag(")")),
        delimited(tag("("), aggfunc, tag(")")),
        aggfunc,
        func0,
        func1,
        func2,
        func3,
        obs_dt,
        event_type,
        event_time,
        in_expr,
        attr,
    ))(input)
}

pub fn in_term(input: &str) -> nom::IResult<&str, Expr> {
    any_symbol(input)
}

pub fn expr(input: &str) -> nom::IResult<&str, Expr> {
    alt((
        binary_expr,
        literal,
        delimited(tag("("), aggfunc, tag(")")),
        aggfunc,
        func0,
        func1,
        func2,
        func3,
        obs_dt,
        event_type,
        event_time,
        in_expr,
        attr,
    ))(input)
}

// Aliasing
pub fn alias(input: &str) -> nom::IResult<&str, &str> {
    tag("as")(input)
}

pub fn expr_without_alias(input: &str) -> nom::IResult<&str, Expr> {
    expr(input)
}

pub fn expr_with_alias(input: &str) -> nom::IResult<&str, (Expr, &str)> {
    let (input, expr) = expr(input)?;
    let (input, _) = alias(input)?;
    let (input, alias_symbol) = symbol(input)?;
    Ok((input, (expr, alias_symbol)))
}

pub fn variable_assignment(input: &str) -> nom::IResult<&str, (String, Expr)> {
    let (input, variable_name) = variable_name(input)?;
    let (input, _) = tag("=")(input)?;
    let (input, expr) = expr_without_alias(input)?;
    Ok((input, (variable_name.to_string(), expr)))
}

// Final Expressions
pub fn single_expression_in_query(input: &str) -> nom::IResult<&str, Expr> {
    let (input, (_, expr, _)) = tuple((SOI, expr_with_alias, EOI))(input)?;
    Ok((input, expr))
}

pub fn full_query(input: &str) -> nom::IResult<&str, Expr> {
    let (input, (_, _, expr, _)) = tuple((SOI, select_keyword, single_expression_in_query, EOI))(input)?;
    Ok((input, expr))
}

pub fn single_expression(input: &str) -> nom::IResult<&str, Expr> {
    let (input, (_, expr, _)) = tuple((SOI, alt((expr_with_alias, expr_without_alias, variable_assignment)), EOI))(input)?;
    Ok((input, expr))
}
