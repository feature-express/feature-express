use nom::{
    bytes::complete::tag,
    character::complete::{alphanumeric1, char},
    combinator::{map, value},
};

pub fn symbol(input: &str) -> nom::IResult<&str, &str> {
    alphanumeric1(input)
}

pub fn attr_name(input: &str) -> nom::IResult<&str, &str> {
    nom::character::complete::one_of("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_.")(input)
}

pub fn special_symbol(input: &str) -> nom::IResult<&str, &str> {
    map(tag("@"), |_| "@")(input)
}

pub fn current_attr_name(input: &str) -> nom::IResult<&str, &str> {
    special_symbol(input)
}

pub fn variable_name(input: &str) -> nom::IResult<&str, &str> {
    special_symbol(input)
}

pub fn funcname(input: &str) -> nom::IResult<&str, &str> {
    alphanumeric1(input)
}

pub fn integer(input: &str) -> nom::IResult<&str, &str> {
    nom::character::complete::digit1(input)
}

pub fn id(input: &str) -> nom::IResult<&str, &str> {
    nom::character::complete::one_of("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_ ")(input)
}

pub fn symbol_untyped_attr(input: &str) -> nom::IResult<&str, &str> {
    nom::branch::alt((current_attr_name, attr_name))(input)
}

pub fn alias_symbol(input: &str) -> nom::IResult<&str, &str> {
    symbol(input)
}

pub fn any_symbol(input: &str) -> nom::IResult<&str, &str> {
    nom::branch::alt((obs_dt, event_type, event_time, attr))(input)
}

pub fn obs_dt(input: &str) -> nom::IResult<&str, &str> {
    value("obs_dt", tag("obs_dt"))(input)
}

pub fn event_type(input: &str) -> nom::IResult<&str, &str> {
    value("event_type", tag("event_type"))(input)
}

pub fn event_time(input: &str) -> nom::IResult<&str, &str> {
    value("event_time", tag("event_time"))(input)
}

pub fn attr(input: &str) -> nom::IResult<&str, &str> {
    symbol_untyped_attr(input)
}
