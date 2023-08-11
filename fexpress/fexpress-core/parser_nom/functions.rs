use nom::character::complete::{char, multispace0};
use nom::combinator::{opt, value};
use nom::sequence::{delimited, separated_pair, tuple};
use nom::branch::alt;
use nom::multi::separated_list1;
use crate::parser::expr_parser::Rule::symbol;

// Aggregates
pub fn aggfunc_name(input: &str) -> nom::IResult<&str, &str> {
    symbol(input)
}

pub fn groupby_expr(input: &str) -> nom::IResult<&str, Expr> {
    expr(input)
}

pub fn where_expr(input: &str) -> nom::IResult<&str, Expr> {
    expr(input)
}

pub fn min_or_max(input: &str) -> nom::IResult<&str, &str> {
    alt((tag_no_case("min"), tag_no_case("max")))(input)
}

pub fn having_expr(input: &str) -> nom::IResult<&str, Expr> {
    separated_pair(min_or_max, multispace0, expr)(input)
}

// Clauses
pub fn over_keyword(input: &str) -> nom::IResult<&str, &str> {
    tag_no_case("over")(input)
}

pub fn where_keyword(input: &str) -> nom::IResult<&str, &str> {
    tag_no_case("where")(input)
}

pub fn group_by_keyword(input: &str) -> nom::IResult<&str, &str> {
    tag_no_case("group by")(input)
}

pub fn having_keyword(input: &str) -> nom::IResult<&str, &str> {
    tag_no_case("having")(input)
}

pub fn select_keyword(input: &str) -> nom::IResult<&str, &str> {
    tag_no_case("select")(input)
}

pub fn wildcard(input: &str) -> nom::IResult<&str, &str> {
    char('*')(input)
}

pub fn comma(input: &str) -> nom::IResult<&str, &str> {
    char(',')(input)
}

// Function Rules
pub fn funcarg(input: &str) -> nom::IResult<&str, Expr> {
    alt((
        binary_expr,
        literal,
        delimited(char('('), aggfunc, char(')')),
        aggfunc,
        func1,
        func2,
        func3,
        obs_dt,
        event_type,
        event_time,
        attr,
        wildcard,
    ))(input)
}

pub fn aggfunc0(input: &str) -> nom::IResult<&str, Expr> {
    let parser = tuple((
        funcname,
        char('('),
        funcarg,
        char(')'),
        over_keyword,
        interval,
        opt(tuple((where_keyword, where_expr))),
        opt(tuple((group_by_keyword, groupby_expr))),
        opt(tuple((having_keyword, having_expr))),
    ));

    value(Expr::AggrFunc0, parser)(input)
}

pub fn aggfunc1(input: &str) -> nom::IResult<&str, Expr> {
    let parser = tuple((
        funcname,
        char('('),
        funcarg,
        char(','),
        funcarg,
        char(')'),
        over_keyword,
        interval,
        opt(tuple((where_keyword, where_expr))),
        opt(tuple((group_by_keyword, groupby_expr))),
        opt(tuple((having_keyword, having_expr))),
    ));

    value(Expr::AggrFunc1, parser)(input)
}

pub fn aggfunc(input: &str) -> nom::IResult<&str, Expr> {
    alt((aggfunc0, aggfunc1))(input)
}

pub fn func0(input: &str) -> nom::IResult<&str, Expr> {
    let parser = tuple((funcname, char('('), char(')')));
    value(Expr::Func0, parser)(input)
}

pub fn func1(input: &str) -> nom::IResult<&str, Expr> {
    let parser = tuple((funcname, char('('), funcarg, char(')')));
    value(Expr::Func1, parser)(input)
}

pub fn func2(input: &str) -> nom::IResult<&str, Expr> {
    let parser = tuple((funcname, char('('), funcarg, char(','), funcarg, char(')')));
    value(Expr::Func2, parser)(input)
}

pub fn func3(input: &str) -> nom::IResult<&str, Expr> {
    let parser = tuple((funcname, char('('), funcarg, char(','), funcarg, char(','), funcarg, char(')')));
    value(Expr::Func3, parser)(input)
}
