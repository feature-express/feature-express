use nom::{
    branch::alt,
    bytes::complete::{tag, tag_no_case, take_while, take_while1},
    character::complete::{digit1, multispace1, one_of, char},
    combinator::{map, opt, recognize, value},
    multi::{separated_list1},
    sequence::{delimited, pair, preceded, tuple},
    IResult,
};
use crate::ast::Expr;
use crate::types::{FLOAT, INT};

pub fn whitespace(input: &str) -> IResult<&str, &str> {
    take_while(|c: char| c.is_whitespace() || c == '\n')(input)
}

pub fn wh(input: &str) -> IResult<&str, &str> {
    multispace1(input)
}

pub fn integer(input: &str) -> nom::IResult<&str, &str> {
    digit1(input)
}

pub fn lit_int(input: &str) -> IResult<&str, &str> {
    recognize(tuple((opt(one_of("+-")), digit1)))(input)
}

pub fn lit_float(input: &str) -> IResult<&str, &str> {
    recognize(tuple((lit_int, tag("."), digit1, opt(tuple((tag("e"), lit_int))))))(input)
}

pub fn lit_bool_true(input: &str) -> IResult<&str, &str> {
    tag("true")(input)
}

pub fn lit_bool_false(input: &str) -> IResult<&str, &str> {
    tag("false")(input)
}

pub fn lit_bool(input: &str) -> IResult<&str, &str> {
    alt((lit_bool_true, lit_bool_false))(input)
}

pub fn lit_string_chars_single_quote(input: &str) -> IResult<&str, &str> {
    take_while1(|c: char| c != '\'' && c != '\\')(input)
}

pub fn lit_string_chars_double_quote(input: &str) -> IResult<&str, &str> {
    take_while1(|c: char| c != '"' && c != '\\')(input)
}

pub fn lit_string(input: &str) -> IResult<&str, &str> {
    alt((
        delimited(tag("'"), lit_string_chars_single_quote, tag("'")),
        delimited(tag("\""), lit_string_chars_double_quote, tag("\"")),
    ))(input)
}

pub fn lit_null(input: &str) -> IResult<&str, &str> {
    nom::bytes::complete::tag_no_case("null")(input)
}

pub fn lit_tuple_int(input: &str) -> IResult<&str, Vec<i32>> {
    delimited(char('('), separated_list1(tag(","), lit_int), char(')'))(input)
        .map(|(rest, values)| (rest, values.into_iter().collect()))
}

pub fn lit_tuple_float(input: &str) -> IResult<&str, Vec<f64>> {
    delimited(char('('), separated_list1(tag(","), lit_float), char(')'))(input)
        .map(|(rest, values)| (rest, values.into_iter().collect()))
}

pub fn lit_tuple_string(input: &str) -> IResult<&str, Vec<&str>> {
    delimited(char('('), separated_list1(tag(","), lit_string), char(')'))(input)
}

pub fn lit_tuple_bool(input: &str) -> IResult<&str, Vec<bool>> {
    delimited(char('('), separated_list1(tag(","), lit_bool), char(')'))(input)
        .map(|(rest, values)| (rest, values.into_iter().collect()))
}



pub fn literal(input: &str) -> IResult<&str, Expr> {
    alt((
        map(lit_float, |x| Expr::LitNum(x.parse().unwrap())),
        map(lit_int, |x| Expr::LitInt(x.parse().unwrap())),
        map(lit_string, |x| Expr::LitStr(x.to_string())),
        map(lit_bool, |x| Expr::LitBool(x == "true")),
        map(lit_null, |_| Expr::None),
    ))(input)
}

pub fn lit_tuple(input: &str) -> IResult<&str, Expr> {
    alt((
        map(lit_tuple_int, Expr::TupleLitInt),
        map(lit_tuple_float, Expr::TupleLitNum),
        map(lit_tuple_string, Expr::TupleLitStr),
        map(lit_tuple_bool, Expr::TupleLitBool),
    ))(input)
}
