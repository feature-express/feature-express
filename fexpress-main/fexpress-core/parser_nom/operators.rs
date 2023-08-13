use nom::{
    branch::alt,
    bytes::complete::tag,
    combinator::{map, value},
};

pub fn math_op(input: &str) -> nom::IResult<&str, &str> {
    alt((add, sub, div, mul, pow))(input)
}

pub fn add(input: &str) -> nom::IResult<&str, &str> {
    value("+", tag("+"))(input)
}

pub fn sub(input: &str) -> nom::IResult<&str, &str> {
    value("-", tag("-"))(input)
}

pub fn div(input: &str) -> nom::IResult<&str, &str> {
    value("/", tag("/"))(input)
}

pub fn mul(input: &str) -> nom::IResult<&str, &str> {
    value("*", tag("*"))(input)
}

pub fn pow(input: &str) -> nom::IResult<&str, &str> {
    value("^", tag("^"))(input)
}

pub fn comparison_op(input: &str) -> nom::IResult<&str, &str> {
    alt((gte, lte, lt, gt, neq, eq))(input)
}

pub fn gte(input: &str) -> nom::IResult<&str, &str> {
    value(">=", tag(">="))(input)
}

pub fn lte(input: &str) -> nom::IResult<&str, &str> {
    value("<=", tag("<="))(input)
}

pub fn lt(input: &str) -> nom::IResult<&str, &str> {
    value("<", tag("<"))(input)
}

pub fn gt(input: &str) -> nom::IResult<&str, &str> {
    value(">", tag(">"))(input)
}

pub fn eq(input: &str) -> nom::IResult<&str, &str> {
    alt((tag("=="), tag("=")))(input)
}

pub fn neq(input: &str) -> nom::IResult<&str, &str> {
    alt((tag("!="), tag("<>")))(input)
}

pub fn logical_op(input: &str) -> nom::IResult<&str, &str> {
    alt((and, or))(input)
}

pub fn and(input: &str) -> nom::IResult<&str, &str> {
    value("and", tag("and"))(input)
}

pub fn or(input: &str) -> nom::IResult<&str, &str> {
    value("or", tag("or"))(input)
}

pub fn all_in_op(input: &str) -> nom::IResult<&str, &str> {
    alt((in_op, not_in_op))(input)
}

pub fn in_op(input: &str) -> nom::IResult<&str, &str> {
    value("in", tag("in"))(input)
}

pub fn not_in_op(input: &str) -> nom::IResult<&str, &str> {
    value("not in", tag("not in"))(input)
}

pub fn neg(input: &str) -> nom::IResult<&str, &str> {
    value("-", tag("-"))(input)
}

pub fn binary_op(input: &str) -> nom::IResult<&str, &str> {
    alt((comparison_op, math_op, logical_op))(input)
}
