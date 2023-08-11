use crate::sstring::SmallString;
use crate::types::INT;
use anyhow::{anyhow, Context};
use chrono::{Datelike, NaiveDate, NaiveDateTime, Timelike, Weekday};

pub fn date_add_common(date: &NaiveDate, add: &INT) -> NaiveDate {
    *date + chrono::Duration::days(*add as i64)
}

pub fn date_sub_common(date: &NaiveDate, sub: &INT) -> NaiveDate {
    *date - chrono::Duration::days(*sub as i64)
}

pub fn datetime_hour_common(datetime: NaiveDateTime) -> INT {
    datetime.hour() as INT
}

pub fn datetime_minute_common(datetime: NaiveDateTime) -> INT {
    datetime.minute() as INT
}

pub fn datetime_second_common(datetime: NaiveDateTime) -> INT {
    datetime.second() as INT
}

pub fn datetime_microsecond_common(datetime: NaiveDateTime) -> INT {
    datetime.timestamp_subsec_micros() as INT
}

pub fn eval_date_part_common(
    date_part: &SmallString,
    datetime: &NaiveDateTime,
) -> anyhow::Result<INT> {
    match date_part.as_str() {
        "year" => Ok(datetime.year() as INT),
        "month" => Ok(datetime.month() as INT),
        "day" => Ok(datetime.day() as INT),
        _ => Err(anyhow!("Invalid date part: {}", date_part)),
    }
}

pub fn eval_now_common() -> NaiveDateTime {
    chrono::Local::now().naive_local()
}

pub fn eval_current_date_common() -> NaiveDate {
    chrono::Local::today().naive_local()
}

pub fn eval_current_time_common() -> String {
    chrono::Local::now().time().format("%H:%M:%S").to_string()
}

// Concrete functions
pub fn day_of_date_common(date: NaiveDate) -> INT {
    date.day() as INT
}

pub fn month_of_date_common(date: NaiveDate) -> INT {
    date.month() as INT
}

pub fn year_of_date_common(date: NaiveDate) -> INT {
    date.year() as INT
}

pub fn weekday_of_date_common(date: NaiveDate) -> INT {
    date.weekday().number_from_monday() as INT
}

pub fn day_of_year_of_date_common(date: NaiveDate) -> INT {
    date.ordinal() as INT
}

pub fn quarter_of_date_common(date: NaiveDate) -> INT {
    ((date.month() - 1) / 3 + 1) as INT
}

pub fn is_start_of_month_common(date: NaiveDate) -> bool {
    date.day() == 1
}

pub fn is_end_of_month_common(date: NaiveDate) -> bool {
    date.succ_opt().unwrap().month() != date.month()
}

pub fn is_weekend_common(date: NaiveDate) -> bool {
    matches!(date.weekday(), Weekday::Sat | Weekday::Sun)
}

pub fn week_of_date_common(date: NaiveDate) -> INT {
    date.iso_week().week() as INT
}

pub fn eval_date_diff_common(d1: NaiveDate, d2: NaiveDate) -> INT {
    (d1 - d2).num_days() as INT
}

pub fn eval_to_date_common(v: &SmallString) -> anyhow::Result<NaiveDate> {
    NaiveDate::parse_from_str(&v, "%Y-%m-%d").context("Cannot parse date")
}
