use std::fmt::{Display, Formatter};
use std::ops::{Add, Sub};
use std::str::FromStr;

use crate::ast::core::Expr;
use crate::eval::{eval_simple_expr, EvalContext};
use crate::map::HashMap;
use crate::obs_dates::ObservationTime;
use crate::value::Value;
use chrono::{Datelike, Duration, NaiveDate, NaiveDateTime, Weekday};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use strum::{AsRefStr, EnumString};

#[derive(Clone, Debug, EnumString, Serialize, Deserialize, Eq, PartialEq, Hash)]
#[strum(serialize_all = "snake_case")]
pub enum Direction {
    Next,
    Last,
    Previous,
}

impl Display for Direction {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Direction::Next => write!(f, "next"),
            Direction::Last => write!(f, "last"),
            Direction::Previous => write!(f, "previous"),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq, Hash)]
pub enum Unit {
    Millisecond,
    Second,
    Minute,
    Hour,
    Day,
    Week,
}

impl Display for Unit {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Unit::Millisecond => write!(f, " millisecond "),
            Unit::Second => write!(f, " second "),
            Unit::Minute => write!(f, " minute "),
            Unit::Hour => write!(f, " hour "),
            Unit::Day => write!(f, " day "),
            Unit::Week => write!(f, " week "),
        }
    }
}

impl FromStr for Unit {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let s_lower = s.to_ascii_lowercase();
        let s_clean = s_lower.trim_end_matches('s');
        let time_resolution = match s_clean {
            "millisecond" => Some(Unit::Millisecond),
            "second" => Some(Unit::Second),
            "minute" => Some(Unit::Minute),
            "hour" => Some(Unit::Hour),
            "day" => Some(Unit::Day),
            "week" => Some(Unit::Week),
            _ => None,
        };
        time_resolution.ok_or(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct FixedInterval {
    /*
    Implements intervals like:
    - last 10 days
    - next 5 weeks
     */
    pub direction: Direction,
    pub int: usize,
    pub unit: Unit,
}

impl Display for FixedInterval {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, " {} {} {} ", self.direction, self.int, self.unit)
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct OffsetInterval {
    /*
    Implements intervals like:
    - between "2020-10-10" to "2022-10-12"
     */
    pub start: NaiveDate, // The type could be different based on your actual need
    pub end: NaiveDate,   // The type could be different based on your actual need
}

impl Display for OffsetInterval {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "between \"{}\" to \"{}\"", self.start, self.end)
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct SessionBasedMax {
    pub direction: Direction,
    pub session_count: Option<usize>,
    pub max_inactive_time: usize,
}

impl Display for SessionBasedMax {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "Direction: {:?}", self.direction)?;
        if let Some(count) = self.session_count {
            write!(f, ", Session Count: {}", count)?;
        }
        write!(f, ", Max Inactive Time: {}", self.max_inactive_time)
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct SessionBasedAttr {
    pub direction: Direction,
    pub attr_name: String,
}

impl Display for SessionBasedAttr {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(
            f,
            "Direction: {:?}, Attribute Name: {}",
            self.direction, self.attr_name
        )
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct BetweenDatesExpressions {
    pub from_date: Box<Expr>,
    pub to_date: Box<Expr>,
}

impl Display for BetweenDatesExpressions {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "between {:?} to {:?}", self.from_date, self.to_date)
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct SinceEvent {
    pub direction: Direction,
    pub event_condition: String, // You may need a different type or structure to represent the condition
}

impl Display for SinceEvent {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(
            f,
            "Direction: {:?}, Event Condition: {}",
            self.direction, self.event_condition
        )
    }
}

#[derive(Clone, Debug, AsRefStr, EnumString, Eq, PartialEq, Hash)]
pub enum KeywordInterval {
    #[strum(serialize = "ytd", serialize = "Ytd", serialize = "YTD")]
    YTD,
    #[strum(serialize = "mtd", serialize = "Mtd", serialize = "MTD")]
    MTD,
    #[strum(serialize = "wtd", serialize = "Wtd", serialize = "WTD")]
    WTD,
    #[strum(
        serialize = "yesterday",
        serialize = "Yesterday",
        serialize = "YESTERDAY"
    )]
    Yesterday,
    #[strum(serialize = "lastweek", serialize = "LastWeek", serialize = "LASTWEEK")]
    LastWeek,
    #[strum(
        serialize = "lastmonth",
        serialize = "LastMonth",
        serialize = "LASTMONTH"
    )]
    LastMonth,
    #[strum(
        serialize = "lastquarter",
        serialize = "LastQuarter",
        serialize = "LASTQUARTER"
    )]
    LastQuarter,
    #[strum(serialize = "lastyear", serialize = "LastYear", serialize = "LASTYEAR")]
    LastYear,
    #[strum(
        serialize = "samedaylastweek",
        serialize = "SameDayLastWeek",
        serialize = "SAMEDAYLASTWEEK"
    )]
    SameDayLastWeek,
    #[strum(
        serialize = "samedaylastmonth",
        serialize = "SameDayLastMonth",
        serialize = "SAMEDAYLASTMONTH"
    )]
    SameDayLastMonth,
    #[strum(
        serialize = "samedaylastyear",
        serialize = "SameDayLastYear",
        serialize = "SAMEDAYLASTYEAR"
    )]
    SameDayLastYear,
    #[strum(serialize = "tomorrow", serialize = "Tomorrow", serialize = "TOMORROW")]
    Tomorrow,
    #[strum(serialize = "nextweek", serialize = "NextWeek", serialize = "NEXTWEEK")]
    NextWeek,
    #[strum(
        serialize = "nextmonth",
        serialize = "NextMonth",
        serialize = "NEXTMONTH"
    )]
    NextMonth,
    #[strum(
        serialize = "nextquarter",
        serialize = "NextQuarter",
        serialize = "NEXTQUARTER"
    )]
    NextQuarter,
    #[strum(serialize = "nextyear", serialize = "NextYear", serialize = "NEXTYEAR")]
    NextYear,
    #[strum(
        serialize = "samedaynextweek",
        serialize = "SameDayNextWeek",
        serialize = "SAMEDAYNEXTWEEK"
    )]
    SameDayNextWeek,
    #[strum(
        serialize = "samedaynextmonth",
        serialize = "SameDayNextMonth",
        serialize = "SAMEDAYNEXTMONTH"
    )]
    SameDayNextMonth,
    #[strum(
        serialize = "samedaynextyear",
        serialize = "SameDayNextYear",
        serialize = "SAMEDAYNEXTYEAR"
    )]
    SameDayNextYear,
    #[strum(
        serialize = "nextbusinessday",
        serialize = "NextBusinessDay",
        serialize = "NEXTBUSINESSDAY"
    )]
    NextWorkDay,
    #[strum(
        serialize = "previousbusinessday",
        serialize = "PreviousBusinessDay",
        serialize = "PREVIOUSBUSINESSDAY"
    )]
    PreviousWorkDay,
}

impl Display for KeywordInterval {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "{}", self.as_ref())
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct EventBased {
    pub direction: Direction,
    pub event_count: usize,
}

impl Display for EventBased {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(
            f,
            "Direction: {:?}, Event Count: {}",
            self.direction, self.event_count
        )
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct EventCountBased {
    pub event_count: usize,
    pub condition: String, // You may need a different type or structure to represent the condition
}

impl Display for EventCountBased {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(
            f,
            "Event Count: {}, Condition: {}",
            self.event_count, self.condition
        )
    }
}

#[derive(Debug, Clone, EnumString, Eq, PartialEq, Hash)]
#[strum(serialize_all = "snake_case")]
pub enum DirectionOnly {
    Past,
    Future,
}

impl Display for DirectionOnly {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            DirectionOnly::Past => write!(f, "past"),
            DirectionOnly::Future => write!(f, "future"),
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub enum NewInterval {
    FixedInterval(FixedInterval),
    DirectionOnly(DirectionOnly),
    OffsetInterval(OffsetInterval),
    BetweenDatesExpressions(BetweenDatesExpressions),
    SessionBasedMax(SessionBasedMax),
    SessionBasedAttr(SessionBasedAttr),
    SinceEvent(SinceEvent),
    KeywordDate(KeywordInterval),
    EventBased(EventBased),
    EventCountBased(EventCountBased),
}

impl Display for NewInterval {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            NewInterval::FixedInterval(v) => write!(f, " {} ", v),
            NewInterval::DirectionOnly(v) => write!(f, " {} ", v),
            NewInterval::OffsetInterval(v) => write!(f, " {} ", v),
            NewInterval::SessionBasedMax(v) => write!(f, " {} ", v),
            NewInterval::SessionBasedAttr(v) => write!(f, " {} ", v),
            NewInterval::SinceEvent(v) => write!(f, " {} ", v),
            NewInterval::KeywordDate(v) => write!(f, " {} ", v),
            NewInterval::EventBased(v) => write!(f, " {} ", v),
            NewInterval::EventCountBased(v) => write!(f, " {} ", v),
            NewInterval::BetweenDatesExpressions(v) => write!(f, " {} ", v),
        }
    }
}

impl NewInterval {
    pub fn materialize_interval(&self, dt: &NaiveDateTime) -> Option<NaiveDateTimeInterval> {
        match self {
            NewInterval::FixedInterval(interval) => match interval.direction {
                Direction::Next => Some(NaiveDateTimeInterval {
                    start_dt: Some(*dt),
                    end_dt: match interval.unit {
                        Unit::Millisecond => {
                            Some(dt.add(Duration::milliseconds(interval.int as i64)))
                        }
                        Unit::Second => Some(dt.add(Duration::seconds(interval.int as i64))),
                        Unit::Minute => Some(dt.add(Duration::minutes(interval.int as i64))),
                        Unit::Hour => Some(dt.add(Duration::hours(interval.int as i64))),
                        Unit::Day => Some(dt.add(Duration::days(interval.int as i64))),
                        Unit::Week => Some(dt.add(Duration::weeks(interval.int as i64))),
                    },
                }),
                Direction::Last | Direction::Previous => Some(NaiveDateTimeInterval {
                    start_dt: match interval.unit {
                        Unit::Millisecond => {
                            Some(dt.sub(Duration::milliseconds(interval.int as i64)))
                        }
                        Unit::Second => Some(dt.sub(Duration::seconds(interval.int as i64))),
                        Unit::Minute => Some(dt.sub(Duration::minutes(interval.int as i64))),
                        Unit::Hour => Some(dt.sub(Duration::hours(interval.int as i64))),
                        Unit::Day => Some(dt.sub(Duration::days(interval.int as i64))),
                        Unit::Week => Some(dt.sub(Duration::weeks(interval.int as i64))),
                    },
                    end_dt: Some(*dt),
                }),
            },
            NewInterval::DirectionOnly(direction) => match direction {
                DirectionOnly::Past => Some(NaiveDateTimeInterval {
                    start_dt: None,
                    end_dt: Some(*dt),
                }),
                DirectionOnly::Future => Some(NaiveDateTimeInterval {
                    start_dt: Some(*dt),
                    end_dt: None,
                }),
            },
            NewInterval::OffsetInterval(interval) => Some(NaiveDateTimeInterval {
                start_dt: Some(
                    interval
                        .start
                        .and_hms_opt(0, 0, 0)
                        .expect("Cannot set 00:00::00 to Interval"),
                ),
                end_dt: Some(
                    interval
                        .end
                        .and_hms_opt(23, 59, 59)
                        .expect("Cannot set 23:59:50 to Interval"),
                ),
            }),
            NewInterval::KeywordDate(interval) => match interval {
                KeywordInterval::YTD => Some(NaiveDateTimeInterval {
                    start_dt: Some(
                        NaiveDate::from_ymd_opt(dt.year(), 1, 1)
                            .expect("Cannot construct date")
                            .and_hms_opt(0, 0, 0),
                    )
                    .expect("Cannot add hms"),
                    end_dt: Some(*dt),
                }),
                KeywordInterval::MTD => Some(NaiveDateTimeInterval {
                    start_dt: Some(
                        NaiveDate::from_ymd_opt(dt.year(), dt.month(), 1)
                            .expect("Cannot construct date")
                            .and_hms_opt(0, 0, 0),
                    )
                    .expect("Cannot add hms"),
                    end_dt: Some(*dt),
                }),
                KeywordInterval::WTD => {
                    let days_since_monday = dt.date().weekday().num_days_from_monday();
                    let start_date = dt.date() - Duration::days(days_since_monday as i64);
                    Some(NaiveDateTimeInterval {
                        start_dt: Some(start_date.and_hms_opt(0, 0, 0)).expect("Cannot add hms"),
                        end_dt: Some(*dt),
                    })
                }
                KeywordInterval::Yesterday => Some(NaiveDateTimeInterval {
                    start_dt: Some((dt.date() - Duration::days(1)).and_hms_opt(0, 0, 0))
                        .expect("Cannot add hms"),
                    end_dt: Some((dt.date() - Duration::days(1)).and_hms_opt(23, 59, 59))
                        .expect("Cannot add hms"),
                }),
                KeywordInterval::LastWeek => {
                    let days_since_last_monday =
                        dt.date().weekday().num_days_from_monday() as i64 + 1;
                    let start_date = dt.date() - Duration::days(days_since_last_monday + 6);
                    let end_date = start_date + Duration::days(6);
                    Some(NaiveDateTimeInterval {
                        start_dt: Some(start_date.and_hms_opt(0, 0, 0)).expect("Cannot add hms"),
                        end_dt: Some(end_date.and_hms_opt(23, 59, 59)).expect("Cannot add hms"),
                    })
                }
                KeywordInterval::LastMonth => {
                    let start_date = NaiveDate::from_ymd_opt(dt.year(), dt.month() - 1, 1)
                        .expect("Cannot build a date");
                    let end_date = start_date
                        + Duration::days(
                            month_last_day(start_date.year(), start_date.month()) as i64 - 1,
                        );
                    Some(NaiveDateTimeInterval {
                        start_dt: Some(start_date.and_hms_opt(0, 0, 0)).expect("Cannot add hms"),
                        end_dt: Some(end_date.and_hms_opt(23, 59, 59)).expect("Cannot add hms"),
                    })
                }
                KeywordInterval::LastQuarter => {
                    let quarter_month_start = ((dt.date().month() - 1) / 3) * 3 + 1;
                    let start_date_last_quarter = if quarter_month_start < 4 {
                        NaiveDate::from_ymd(dt.year() - 1, 12 - (3 - quarter_month_start), 1)
                    } else {
                        NaiveDate::from_ymd(dt.year(), quarter_month_start - 3, 1)
                    };
                    let end_date_last_quarter = NaiveDate::from_ymd(
                        start_date_last_quarter.year(),
                        start_date_last_quarter.month() + 2,
                        month_last_day(
                            start_date_last_quarter.year(),
                            start_date_last_quarter.month() + 2,
                        ),
                    );
                    Some(NaiveDateTimeInterval {
                        start_dt: Some(start_date_last_quarter.and_hms_opt(0, 0, 0))
                            .expect("Cannot add hms"),
                        end_dt: Some(end_date_last_quarter.and_hms_opt(23, 59, 59))
                            .expect("Cannot add hms"),
                    })
                }
                KeywordInterval::LastYear => Some(NaiveDateTimeInterval {
                    start_dt: Some(
                        NaiveDate::from_ymd_opt(dt.year() - 1, 1, 1)
                            .expect("Cannot construct date")
                            .and_hms_opt(0, 0, 0),
                    )
                    .expect("Cannot add hms"),
                    end_dt: Some(
                        NaiveDate::from_ymd_opt(dt.year() - 1, 12, 31)
                            .expect("Cannot construct date")
                            .and_hms_opt(23, 59, 59),
                    )
                    .expect("Cannot add hms"),
                }),
                KeywordInterval::SameDayLastWeek => Some(NaiveDateTimeInterval {
                    start_dt: Some((dt.date() - Duration::weeks(1)).and_hms_opt(0, 0, 0))
                        .expect("Cannot add hms"),
                    end_dt: Some((dt.date() - Duration::weeks(1)).and_hms_opt(23, 59, 59))
                        .expect("Cannot add hms"),
                }),
                KeywordInterval::SameDayLastMonth => Some(NaiveDateTimeInterval {
                    start_dt: Some((dt.date() - Duration::days(30)).and_hms_opt(0, 0, 0))
                        .expect("Cannot add hms"),
                    end_dt: Some((dt.date() - Duration::days(30)).and_hms_opt(23, 59, 59))
                        .expect("Cannot add hms"),
                }),
                KeywordInterval::SameDayLastYear => Some(NaiveDateTimeInterval {
                    start_dt: Some(
                        NaiveDate::from_ymd_opt(dt.year() - 1, dt.month(), dt.day())
                            .expect("Cannot construct date")
                            .and_hms(0, 0, 0),
                    ),
                    end_dt: Some(
                        NaiveDate::from_ymd(dt.year() - 1, dt.month(), dt.day())
                            .and_hms(23, 59, 59),
                    ),
                }),
                KeywordInterval::Tomorrow => Some(NaiveDateTimeInterval {
                    start_dt: Some((dt.date() + Duration::days(1)).and_hms_opt(0, 0, 0))
                        .expect("Cannot add hms"),
                    end_dt: Some((dt.date() + Duration::days(1)).and_hms_opt(23, 59, 59))
                        .expect("Cannot add hms"),
                }),
                KeywordInterval::NextWeek => {
                    let days_until_next_monday =
                        7 - dt.date().weekday().num_days_from_monday() as i64;
                    let start_date = dt.date() + Duration::days(days_until_next_monday);
                    let end_date = start_date + Duration::days(6);
                    Some(NaiveDateTimeInterval {
                        start_dt: Some(start_date.and_hms_opt(0, 0, 0)).expect("Cannot add hms"),
                        end_dt: Some(end_date.and_hms_opt(23, 59, 59)).expect("Cannot add hms"),
                    })
                }
                KeywordInterval::NextMonth => {
                    let start_date = NaiveDate::from_ymd_opt(dt.year(), dt.month() + 1, 1)
                        .expect("Cannot build a date");
                    let end_date = start_date
                        + Duration::days(
                            month_last_day(start_date.year(), start_date.month()) as i64 - 1,
                        );
                    Some(NaiveDateTimeInterval {
                        start_dt: Some(start_date.and_hms_opt(0, 0, 0)).expect("Cannot add hms"),
                        end_dt: Some(end_date.and_hms_opt(23, 59, 59)).expect("Cannot add hms"),
                    })
                }
                KeywordInterval::NextQuarter => {
                    let quarter_month_start = ((dt.date().month() - 1) / 3) * 3 + 1;
                    let start_date_next_quarter = if quarter_month_start >= 10 {
                        NaiveDate::from_ymd(dt.year() + 1, quarter_month_start - 9, 1)
                    } else {
                        NaiveDate::from_ymd(dt.year(), quarter_month_start + 3, 1)
                    };
                    let end_date_next_quarter = NaiveDate::from_ymd(
                        start_date_next_quarter.year(),
                        start_date_next_quarter.month() + 2,
                        month_last_day(
                            start_date_next_quarter.year(),
                            start_date_next_quarter.month() + 2,
                        ),
                    );
                    Some(NaiveDateTimeInterval {
                        start_dt: Some(start_date_next_quarter.and_hms_opt(0, 0, 0))
                            .expect("Cannot add hms"),
                        end_dt: Some(end_date_next_quarter.and_hms_opt(23, 59, 59))
                            .expect("Cannot add hms"),
                    })
                }
                KeywordInterval::NextYear => Some(NaiveDateTimeInterval {
                    start_dt: Some(
                        NaiveDate::from_ymd_opt(dt.year() + 1, 1, 1)
                            .expect("Cannot construct date")
                            .and_hms_opt(0, 0, 0),
                    )
                    .expect("Cannot add hms"),
                    end_dt: Some(
                        NaiveDate::from_ymd_opt(dt.year() + 1, 12, 31)
                            .expect("Cannot construct date")
                            .and_hms_opt(23, 59, 59),
                    )
                    .expect("Cannot add hms"),
                }),
                KeywordInterval::SameDayNextWeek => Some(NaiveDateTimeInterval {
                    start_dt: Some((dt.date() + Duration::weeks(1)).and_hms_opt(0, 0, 0))
                        .expect("Cannot add hms"),
                    end_dt: Some((dt.date() + Duration::weeks(1)).and_hms_opt(23, 59, 59))
                        .expect("Cannot add hms"),
                }),
                KeywordInterval::SameDayNextMonth => Some(NaiveDateTimeInterval {
                    start_dt: Some((dt.date() + Duration::days(30)).and_hms_opt(0, 0, 0))
                        .expect("Cannot add hms"),
                    end_dt: Some((dt.date() + Duration::days(30)).and_hms_opt(23, 59, 59))
                        .expect("Cannot add hms"),
                }),
                KeywordInterval::SameDayNextYear => Some(NaiveDateTimeInterval {
                    start_dt: Some(
                        NaiveDate::from_ymd_opt(dt.year() + 1, dt.month(), dt.day())
                            .expect("Cannot construct date")
                            .and_hms(0, 0, 0),
                    ),
                    end_dt: Some(
                        NaiveDate::from_ymd_opt(dt.year() + 1, dt.month(), dt.day())
                            .expect("Cannot construct date")
                            .and_hms(23, 59, 59),
                    ),
                }),
                KeywordInterval::NextWorkDay => {
                    let mut next_day = dt.date() + Duration::days(1);
                    while next_day.weekday() == Weekday::Sat || next_day.weekday() == Weekday::Sun {
                        next_day += Duration::days(1);
                    }
                    Some(NaiveDateTimeInterval {
                        start_dt: Some(next_day.and_hms_opt(0, 0, 0)).expect("Cannot add hms"),
                        end_dt: Some(next_day.and_hms_opt(23, 59, 59)).expect("Cannot add hms"),
                    })
                }
                KeywordInterval::PreviousWorkDay => {
                    let mut previous_day = dt.date() - Duration::days(1);
                    while previous_day.weekday() == Weekday::Sat
                        || previous_day.weekday() == Weekday::Sun
                    {
                        previous_day -= Duration::days(1);
                    }
                    Some(NaiveDateTimeInterval {
                        start_dt: Some(previous_day.and_hms_opt(0, 0, 0)).expect("Cannot add hms"),
                        end_dt: Some(previous_day.and_hms_opt(23, 59, 59)).expect("Cannot add hms"),
                    })
                }
            },
            NewInterval::SessionBasedMax(_) => unimplemented!(),
            NewInterval::SessionBasedAttr(_) => unimplemented!(),
            NewInterval::SinceEvent(_) => unimplemented!(),
            NewInterval::EventBased(_) => unimplemented!(),
            NewInterval::EventCountBased(_) => unimplemented!(),
            NewInterval::BetweenDatesExpressions(between) => {
                let expr = Expr::from_str("date_add(date(obs_dt), 1)").unwrap();
                let stored_variables = HashMap::new();
                let context = EvalContext {
                    obs_time: Some(ObservationTime {
                        datetime: dt.clone(),
                        event_id: None,
                    }),
                    ..Default::default()
                };
                let from_date_value =
                    eval_simple_expr(&between.from_date, None, Some(&context), &stored_variables)
                        .ok()?;
                let from_date = match from_date_value {
                    Value::None => None,
                    Value::Wildcard => None,
                    Value::Date(date) => Some(date.and_hms_opt(0, 0, 0)?),
                    Value::DateTime(datetime) => Some(datetime),
                    _ => None,
                };
                let to_date_value =
                    eval_simple_expr(&between.to_date, None, Some(&context), &stored_variables)
                        .ok()?;
                let to_date = match to_date_value {
                    Value::None => None,
                    Value::Wildcard => None,
                    Value::Date(date) => Some(date.and_hms_opt(23, 59, 59)?),
                    Value::DateTime(datetime) => Some(datetime),
                    _ => None,
                };
                Some(NaiveDateTimeInterval {
                    start_dt: from_date,
                    end_dt: to_date,
                })
            }
        }
    }
}

// Helper function to get the last day of a month
fn month_last_day(year: i32, month: u32) -> u32 {
    match month {
        4 | 6 | 9 | 11 => 30,
        2 => {
            if year % 4 == 0 && (year % 100 != 0 || year % 400 == 0) {
                29
            } else {
                28
            }
        }
        _ => 31,
    }
}

// Other date operations
// https://github.com/joshuaclayton/date-calculations/blob/main/src/lib.rs
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
pub enum DatePart {
    Millisecond,
    Second,
    Minute,
    Hour,
    Day,
    Week,
    All,
}

impl FromStr for DatePart {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let s_lower = s.to_ascii_lowercase();
        let s_clean = s_lower.trim_end_matches('s');
        let time_resolution = match s_clean {
            "millisecond" => Some(DatePart::Millisecond),
            "second" => Some(DatePart::Second),
            "minute" => Some(DatePart::Minute),
            "hour" => Some(DatePart::Hour),
            "day" => Some(DatePart::Day),
            "week" => Some(DatePart::Week),
            "alltime" => Some(DatePart::All),
            _ => None,
        };
        time_resolution.ok_or(())
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct NaiveDateTimeInterval {
    pub start_dt: Option<NaiveDateTime>,
    pub end_dt: Option<NaiveDateTime>,
}

impl NaiveDateTimeInterval {
    pub fn start_dt_safe(&self) -> NaiveDateTime {
        self.start_dt
            .unwrap_or_else(|| NaiveDateTime::from_timestamp(0, 0))
    }

    pub fn end_dt_safe(&self) -> NaiveDateTime {
        self.end_dt.unwrap_or(NaiveDateTime::MAX)
    }

    pub fn start_dt_exclusive_safe(&self) -> NaiveDateTime {
        self.start_dt
            .map(|dt| dt.add(Duration::milliseconds(1)))
            .unwrap_or_else(|| NaiveDateTime::from_timestamp(0, 0))
    }

    pub fn end_dt_exclusive_safe(&self) -> NaiveDateTime {
        self.end_dt
            .map(|dt| dt.sub(Duration::milliseconds(1)))
            .unwrap_or(NaiveDateTime::MAX)
    }

    pub fn contains(&self, dt: &NaiveDateTime, inclusive: bool) -> bool {
        if inclusive {
            *dt >= self.start_dt_safe() && *dt <= self.end_dt_safe()
        } else {
            *dt > self.start_dt_safe() && *dt < self.end_dt_safe()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_fixed_interval() {
        let dt = NaiveDateTime::from_timestamp(1_000_000, 0);

        let interval = NewInterval::FixedInterval(FixedInterval {
            direction: Direction::Next,
            int: 1,
            unit: Unit::Day,
        });
        let expected_end_dt = Some(dt + Duration::days(1));
        assert_eq!(
            interval.materialize_interval(&dt),
            Some(NaiveDateTimeInterval {
                start_dt: Some(dt),
                end_dt: expected_end_dt
            })
        );

        let interval = NewInterval::FixedInterval(FixedInterval {
            direction: Direction::Previous,
            int: 1,
            unit: Unit::Day,
        });
        let expected_start_dt = Some(dt - Duration::days(1));
        assert_eq!(
            interval.materialize_interval(&dt),
            Some(NaiveDateTimeInterval {
                start_dt: expected_start_dt,
                end_dt: Some(dt)
            })
        );
    }

    #[test]
    fn test_direction_only() {
        let dt = NaiveDateTime::from_timestamp(1_000_000, 0);

        let interval = NewInterval::DirectionOnly(DirectionOnly::Past);
        assert_eq!(
            interval.materialize_interval(&dt),
            Some(NaiveDateTimeInterval {
                start_dt: None,
                end_dt: Some(dt)
            })
        );

        let interval = NewInterval::DirectionOnly(DirectionOnly::Future);
        assert_eq!(
            interval.materialize_interval(&dt),
            Some(NaiveDateTimeInterval {
                start_dt: Some(dt),
                end_dt: None
            })
        );
    }

    #[test]
    fn test_offset_interval() {
        let start_date = NaiveDate::from_ymd_opt(2020, 1, 1).expect("Cannot build a date");
        let end_date = NaiveDate::from_ymd_opt(2020, 1, 31).expect("Cannot build a date");
        let interval = NewInterval::OffsetInterval(OffsetInterval {
            start: start_date,
            end: end_date,
        });
        assert_eq!(
            interval.materialize_interval(&NaiveDateTime::from_timestamp(1_000_000, 0)),
            Some(NaiveDateTimeInterval {
                start_dt: Some(start_date.and_hms_opt(0, 0, 0)).expect("Cannot add hms"),
                end_dt: Some(end_date.and_hms_opt(23, 59, 59)).expect("Cannot add hms")
            })
        );
    }

    #[test]
    fn test_keyword_date() {
        let dt = NaiveDate::from_ymd_opt(2023, 5, 17)
            .expect("Cannot build date using from_ymd")
            .and_hms_opt(12, 0, 0)
            .expect("Cannot build a date");

        let interval = NewInterval::KeywordDate(KeywordInterval::YTD);
        assert_eq!(
            interval.materialize_interval(&dt),
            Some(NaiveDateTimeInterval {
                start_dt: Some(
                    NaiveDate::from_ymd_opt(dt.year(), 1, 1)
                        .expect("Cannot construct date")
                        .and_hms_opt(0, 0, 0)
                )
                .expect("Cannot add hms"),
                end_dt: Some(dt)
            })
        );

        let interval = NewInterval::KeywordDate(KeywordInterval::MTD);
        assert_eq!(
            interval.materialize_interval(&dt),
            Some(NaiveDateTimeInterval {
                start_dt: Some(
                    NaiveDate::from_ymd_opt(dt.year(), dt.month(), 1)
                        .expect("Cannot construct date")
                        .and_hms_opt(0, 0, 0)
                )
                .expect("Cannot add hms"),
                end_dt: Some(dt)
            })
        );

        let interval = NewInterval::KeywordDate(KeywordInterval::WTD);
        let days_since_monday = dt.date().weekday().num_days_from_monday();
        let start_date = dt.date() - Duration::days(days_since_monday as i64);
        assert_eq!(
            interval.materialize_interval(&dt),
            Some(NaiveDateTimeInterval {
                start_dt: Some(start_date.and_hms_opt(0, 0, 0)).expect("Cannot add hms"),
                end_dt: Some(dt)
            })
        );
    }

    #[test]
    fn test_yesterday() {
        let dt = NaiveDate::from_ymd_opt(2023, 5, 17)
            .expect("Cannot build date using from_ymd")
            .and_hms_opt(12, 0, 0)
            .expect("Cannot build a date");
        let interval = NewInterval::KeywordDate(KeywordInterval::Yesterday);
        assert_eq!(
            interval.materialize_interval(&dt),
            Some(NaiveDateTimeInterval {
                start_dt: Some(
                    NaiveDate::from_ymd_opt(2023, 5, 16)
                        .expect("Cannot construct date")
                        .and_hms_opt(0, 0, 0)
                )
                .expect("Cannot add hms"),
                end_dt: Some(
                    NaiveDate::from_ymd_opt(2023, 5, 16)
                        .expect("Cannot construct date")
                        .and_hms_opt(23, 59, 59)
                )
                .expect("Cannot add hms"),
            })
        );
    }

    #[test]
    fn test_lastweek() {
        let dt = NaiveDate::from_ymd_opt(2023, 5, 17)
            .expect("Cannot build date using from_ymd")
            .and_hms_opt(12, 0, 0)
            .expect("Cannot build a date");
        let interval = NewInterval::KeywordDate(KeywordInterval::LastWeek);
        assert_eq!(
            interval.materialize_interval(&dt),
            Some(NaiveDateTimeInterval {
                start_dt: Some(
                    NaiveDate::from_ymd_opt(2023, 5, 8)
                        .expect("Cannot construct date")
                        .and_hms_opt(0, 0, 0)
                )
                .expect("Cannot add hms"),
                end_dt: Some(
                    NaiveDate::from_ymd_opt(2023, 5, 14)
                        .expect("Cannot construct date")
                        .and_hms_opt(23, 59, 59)
                )
                .expect("Cannot add hms"),
            })
        );
    }

    #[test]
    fn test_lastmonth() {
        let dt = NaiveDate::from_ymd_opt(2023, 5, 17)
            .expect("Cannot build date using from_ymd")
            .and_hms_opt(12, 0, 0)
            .expect("Cannot build a date");
        let interval = NewInterval::KeywordDate(KeywordInterval::LastMonth);
        assert_eq!(
            interval.materialize_interval(&dt),
            Some(NaiveDateTimeInterval {
                start_dt: Some(
                    NaiveDate::from_ymd_opt(2023, 4, 1)
                        .expect("Cannot construct date")
                        .and_hms_opt(0, 0, 0)
                )
                .expect("Cannot add hms"),
                end_dt: Some(
                    NaiveDate::from_ymd_opt(2023, 4, 30)
                        .expect("Cannot construct date")
                        .and_hms_opt(23, 59, 59)
                )
                .expect("Cannot add hms"),
            })
        );
    }

    #[test]
    fn test_lastquarter() {
        let dt = NaiveDate::from_ymd_opt(2023, 5, 17)
            .expect("Cannot build date using from_ymd")
            .and_hms_opt(12, 0, 0)
            .expect("Cannot build a date");
        let interval = NewInterval::KeywordDate(KeywordInterval::LastQuarter);
        assert_eq!(
            interval.materialize_interval(&dt),
            Some(NaiveDateTimeInterval {
                start_dt: Some(
                    NaiveDate::from_ymd_opt(2023, 1, 1)
                        .expect("Cannot construct date")
                        .and_hms_opt(0, 0, 0)
                )
                .expect("Cannot add hms"),
                end_dt: Some(
                    NaiveDate::from_ymd_opt(2023, 3, 31)
                        .expect("Cannot construct date")
                        .and_hms_opt(23, 59, 59)
                )
                .expect("Cannot add hms"),
            })
        );
    }

    #[test]
    fn test_lastyear() {
        let dt = NaiveDate::from_ymd_opt(2023, 5, 17)
            .expect("Cannot build date using from_ymd")
            .and_hms_opt(12, 0, 0)
            .expect("Cannot build a date");
        let interval = NewInterval::KeywordDate(KeywordInterval::LastYear);
        assert_eq!(
            interval.materialize_interval(&dt),
            Some(NaiveDateTimeInterval {
                start_dt: Some(
                    NaiveDate::from_ymd_opt(2022, 1, 1)
                        .expect("Cannot construct date")
                        .and_hms_opt(0, 0, 0)
                )
                .expect("Cannot add hms"),
                end_dt: Some(
                    NaiveDate::from_ymd_opt(2022, 12, 31)
                        .expect("Cannot construct date")
                        .and_hms_opt(23, 59, 59)
                )
                .expect("Cannot add hms"),
            })
        );
    }

    // Tests for the future keywords...

    #[test]
    fn test_tomorrow() {
        let dt = NaiveDate::from_ymd_opt(2023, 5, 17)
            .expect("Cannot build date using from_ymd")
            .and_hms_opt(12, 0, 0)
            .expect("Cannot build a date");
        let interval = NewInterval::KeywordDate(KeywordInterval::Tomorrow);
        assert_eq!(
            interval.materialize_interval(&dt),
            Some(NaiveDateTimeInterval {
                start_dt: Some(
                    NaiveDate::from_ymd_opt(2023, 5, 18)
                        .expect("Cannot construct date")
                        .and_hms_opt(0, 0, 0)
                )
                .expect("Cannot add hms"),
                end_dt: Some(
                    NaiveDate::from_ymd_opt(2023, 5, 18)
                        .expect("Cannot construct date")
                        .and_hms_opt(23, 59, 59)
                )
                .expect("Cannot add hms"),
            })
        );
    }

    #[test]
    fn test_nextweek() {
        let dt = NaiveDate::from_ymd_opt(2023, 5, 17)
            .expect("Cannot build date using from_ymd")
            .and_hms_opt(12, 0, 0)
            .expect("Cannot build a date");
        let interval = NewInterval::KeywordDate(KeywordInterval::NextWeek);
        assert_eq!(
            interval.materialize_interval(&dt),
            Some(NaiveDateTimeInterval {
                start_dt: Some(
                    NaiveDate::from_ymd_opt(2023, 5, 22)
                        .expect("Cannot construct date")
                        .and_hms_opt(0, 0, 0)
                )
                .expect("Cannot add hms"),
                end_dt: Some(
                    NaiveDate::from_ymd_opt(2023, 5, 28)
                        .expect("Cannot construct date")
                        .and_hms_opt(23, 59, 59)
                )
                .expect("Cannot add hms"),
            })
        );
    }

    #[test]
    fn test_nextmonth() {
        let dt = NaiveDate::from_ymd_opt(2023, 5, 17)
            .expect("Cannot build date using from_ymd")
            .and_hms_opt(12, 0, 0)
            .expect("Cannot build a date");
        let interval = NewInterval::KeywordDate(KeywordInterval::NextMonth);
        assert_eq!(
            interval.materialize_interval(&dt),
            Some(NaiveDateTimeInterval {
                start_dt: Some(
                    NaiveDate::from_ymd_opt(2023, 6, 1)
                        .expect("Cannot construct date")
                        .and_hms_opt(0, 0, 0)
                )
                .expect("Cannot add hms"),
                end_dt: Some(
                    NaiveDate::from_ymd_opt(2023, 6, 30)
                        .expect("Cannot construct date")
                        .and_hms_opt(23, 59, 59)
                )
                .expect("Cannot add hms"),
            })
        );
    }

    #[test]
    fn test_nextquarter() {
        let dt = NaiveDate::from_ymd_opt(2023, 5, 17)
            .expect("Cannot build date using from_ymd")
            .and_hms_opt(12, 0, 0)
            .expect("Cannot build a date");
        let interval = NewInterval::KeywordDate(KeywordInterval::NextQuarter);
        assert_eq!(
            interval.materialize_interval(&dt),
            Some(NaiveDateTimeInterval {
                start_dt: Some(
                    NaiveDate::from_ymd_opt(2023, 7, 1)
                        .expect("Cannot construct date")
                        .and_hms_opt(0, 0, 0)
                )
                .expect("Cannot add hms"),
                end_dt: Some(
                    NaiveDate::from_ymd_opt(2023, 9, 30)
                        .expect("Cannot construct date")
                        .and_hms_opt(23, 59, 59)
                )
                .expect("Cannot add hms"),
            })
        );
    }

    #[test]
    fn test_nextyear() {
        let dt = NaiveDate::from_ymd_opt(2023, 5, 17)
            .expect("Cannot build date using from_ymd")
            .and_hms_opt(12, 0, 0)
            .expect("Cannot build a date");
        let interval = NewInterval::KeywordDate(KeywordInterval::NextYear);
        assert_eq!(
            interval.materialize_interval(&dt),
            Some(NaiveDateTimeInterval {
                start_dt: Some(
                    NaiveDate::from_ymd_opt(2024, 1, 1)
                        .expect("Cannot construct date")
                        .and_hms_opt(0, 0, 0)
                )
                .expect("Cannot add hms"),
                end_dt: Some(
                    NaiveDate::from_ymd_opt(2024, 12, 31)
                        .expect("Cannot construct date")
                        .and_hms_opt(23, 59, 59)
                )
                .expect("Cannot add hms"),
            })
        );
    }

    #[test]
    fn test_nextbusinessday() {
        let dt = NaiveDate::from_ymd_opt(2023, 5, 19)
            .expect("Cannot build date using from_ymd")
            .and_hms_opt(12, 0, 0)
            .expect("Cannot build a date"); // a Friday
        let interval = NewInterval::KeywordDate(KeywordInterval::NextWorkDay);
        assert_eq!(
            interval.materialize_interval(&dt),
            Some(NaiveDateTimeInterval {
                start_dt: Some(
                    NaiveDate::from_ymd_opt(2023, 5, 22)
                        .expect("Cannot construct date")
                        .and_hms_opt(0, 0, 0)
                )
                .expect("Cannot add hms"), // next business day is Monday
                end_dt: Some(
                    NaiveDate::from_ymd_opt(2023, 5, 22)
                        .expect("Cannot construct date")
                        .and_hms_opt(23, 59, 59)
                )
                .expect("Cannot add hms"),
            })
        );
    }

    #[test]
    fn test_previousbusinessday() {
        let dt = NaiveDate::from_ymd_opt(2023, 5, 22)
            .expect("Cannot build date using from_ymd")
            .and_hms_opt(12, 0, 0)
            .expect("Cannot build a date"); // a Monday
        let interval = NewInterval::KeywordDate(KeywordInterval::PreviousWorkDay);
        assert_eq!(
            interval.materialize_interval(&dt),
            Some(NaiveDateTimeInterval {
                start_dt: Some(
                    NaiveDate::from_ymd_opt(2023, 5, 19)
                        .expect("Cannot construct date")
                        .and_hms_opt(0, 0, 0)
                )
                .expect("Cannot add hms"), // previous business day is Friday
                end_dt: Some(
                    NaiveDate::from_ymd_opt(2023, 5, 19)
                        .expect("Cannot construct date")
                        .and_hms_opt(23, 59, 59)
                )
                .expect("Cannot add hms"),
            })
        );
    }
}
