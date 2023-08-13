use nom::{
    bytes::complete::tag_no_case,
    combinator::{map, value},
    multi::many0,
    sequence::{pair, preceded},
};
use nom::branch::alt;
use crate::parser::expr_parser::Rule::integer;

pub fn direction(input: &str) -> nom::IResult<&str, &str> {
    alt((
        tag_no_case("next"),
        tag_no_case("last"),
        tag_no_case("previous"),
        tag_no_case("future"),
        tag_no_case("past"),
    ))(input)
}

pub fn direction_only(input: &str) -> nom::IResult<&str, &str> {
    alt((tag_no_case("future"), tag_no_case("past")))(input)
}

pub fn unit(input: &str) -> nom::IResult<&str, &str> {
    pair(
        alt((
            tag_no_case("millisecond"),
            tag_no_case("second"),
            tag_no_case("minute"),
            tag_no_case("hour"),
            tag_no_case("day"),
            tag_no_case("week"),
        )),
        tag_no_case("s").opt(),
    )(input)
        .map(|(rest, (unit, _))| (rest, unit))
}

pub fn fixed_interval(input: &str) -> nom::IResult<&str, &str> {
    map(
        pair(pair(direction, integer), unit),
        |((direction, integer), unit)| direction,
    )(input)
}

pub fn keyword_interval(input: &str) -> nom::IResult<&str, &str> {
    alt((
        tag_no_case("YTD"),
        tag_no_case("MTD"),
        tag_no_case("WTD"),
        tag_no_case("Yesterday"),
        tag_no_case("LastWeek"),
        tag_no_case("LastMonth"),
        tag_no_case("LastQuarter"),
        tag_no_case("LastYear"),
        tag_no_case("SameDayLastWeek"),
        tag_no_case("SameDayLastMonth"),
        tag_no_case("SameDayLastYear"),
        tag_no_case("Tomorrow"),
        tag_no_case("NextWeek"),
        tag_no_case("NextMonth"),
        tag_no_case("NextQuarter"),
        tag_no_case("NextYear"),
        tag_no_case("SameDayNextWeek"),
        tag_no_case("SameDayNextMonth"),
        tag_no_case("SameDayNextYear"),
        tag_no_case("NextWorkDay"),
        tag_no_case("PreviousWorkDay"),
    ))(input)
}

pub fn interval(input: &str) -> nom::IResult<&str, &str> {
    alt((
        fixed_interval,
        direction_only,
        keyword_interval,
    ))(input)
}
