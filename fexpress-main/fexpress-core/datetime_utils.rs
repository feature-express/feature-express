use std::ops::{Add, Sub};

use chrono::{DateTime, Datelike, Duration, NaiveDate, NaiveDateTime};
use serde::{Deserialize, Deserializer};

use crate::types::Timestamp;

pub fn parse_utc_from_str(date_str: &str) -> NaiveDateTime {
    let datetime = DateTime::parse_from_rfc3339(date_str).expect("Cannot parse the date");
    datetime.naive_utc()
}

#[allow(dead_code)]
pub fn parse_naivedate(date_str: &str) -> Option<NaiveDate> {
    let dt_rfc = DateTime::parse_from_rfc3339(date_str).ok();
    if let Some(dt) = dt_rfc {
        return NaiveDate::from_ymd_opt(dt.year(), dt.month(), dt.day());
    }
    let dt_naive = NaiveDate::parse_from_str(date_str, "%Y-%m-%d").ok();
    if let Some(dt) = dt_naive {
        return Some(dt);
    }
    None
}

// prevents overflow when adding 1 millisecond
#[allow(dead_code)]
pub fn add_ms(ts: Timestamp) -> Timestamp {
    if ts == NaiveDateTime::MAX {
        NaiveDateTime::MAX
    } else {
        ts.add(Duration::milliseconds(1))
    }
}

// prevents under flow when subtracting 1 millisecond
#[allow(dead_code)]
pub fn sub_ms(ts: Timestamp) -> Timestamp {
    if ts == NaiveDateTime::MIN {
        NaiveDateTime::MIN
    } else {
        ts.sub(Duration::milliseconds(1))
    }
}

pub fn deserialize_naive_date_time<'de, D>(deserializer: D) -> Result<NaiveDateTime, D::Error>
where
    D: Deserializer<'de>,
{
    let s: String = Deserialize::deserialize(deserializer)?;

    // Check for characters that may indicate timezone information
    if s.contains('Z') || s.contains('+') || s.contains('-') && s.chars().nth(4) != Some('-') {
        return Err(serde::de::Error::custom(
            "The library only supports date times without timezone information. \
            Please convert them to a timezone-agnostic format, such as UTC, and strip the timezone information from the string.",
        ));
    }

    // List of supported formats, ordered from most specific to least specific
    let date_time_formats = [
        "%Y-%m-%d %H:%M:%S%.f", // e.g. "2023-06-10 15:30:00.123456"
        "%Y-%m-%d %H:%M:%S",    // e.g. "2023-06-10 15:30:00"
        "%Y-%m-%dT%H:%M:%S",    // e.g. "2023-06-10T15:30:00"
        "%Y-%m-%d %H:%M",       // e.g. "2023-06-10 15:30"
        "%Y-%m-%dT%H:%M",       // e.g. "2023-06-10T15:30"
    ];

    // Try parsing the string using each format
    for format in &date_time_formats {
        if let Ok(dt) = NaiveDateTime::parse_from_str(&s, format) {
            return Ok(dt);
        }
    }

    // If none of the date-time formats worked, try parsing as a date
    let date_format = "%Y-%m-%d"; // e.g. "2023-06-10"
    if let Ok(date) = NaiveDate::parse_from_str(&s, date_format) {
        // If the string is a valid date, assume a time of midnight
        return Ok(date.and_hms(0, 0, 0));
    }

    // If all parsing attempts fail, return an error
    Err(serde::de::Error::custom("Failed to parse date time"))
}

pub fn middle_datetime(datetime1: NaiveDateTime, datetime2: NaiveDateTime) -> NaiveDateTime {
    let timestamp1 = datetime1.timestamp();
    let timestamp2 = datetime2.timestamp();

    // Calculate the middle timestamp
    let middle_timestamp = (timestamp1 + timestamp2) / 2;

    // Convert the middle timestamp back to NaiveDateTime
    NaiveDateTime::from_timestamp(middle_timestamp, 0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_naivedate() {
        let result = parse_naivedate("1996-12-19T16:39:57-08:00").unwrap();
        assert_eq!(result, NaiveDate::from_ymd(1996, 12, 19));

        let result = parse_naivedate("1996-12-20").unwrap();
        assert_eq!(result, NaiveDate::from_ymd(1996, 12, 20));
    }
}
