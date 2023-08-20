extern crate chrono;

use chrono::{Duration, NaiveDateTime};
use crate::types::FLOAT;

// AverageTimeBetween implementation
pub struct AverageTimeBetween {
    start_time: Option<NaiveDateTime>,
    end_time: Option<NaiveDateTime>,
    sum_time_diff: Duration,
    count: usize,
}

impl PartialAggregate for AverageTimeBetween {
    type State = (Option<NaiveDateTime>, Option<NaiveDateTime>, Duration, usize);
    type Input = (NaiveDateTime, FLOAT);
    type Output = Option<Duration>;

    fn new() -> Self {
        AverageTimeBetween {
            start_time: None,
            end_time: None,
            sum_time_diff: Duration::zero(),
            count: 0,
        }
    }

    fn update(&mut self, input: Self::Input) {
        let (time, _) = input;

        if let Some(end_time) = self.end_time {
            let time_diff = time - end_time;
            self.sum_time_diff = self.sum_time_diff + time_diff;
        } else {
            self.start_time = Some(time);
        }

        self.end_time = Some(time);
        self.count += 1;
    }

    fn merge(&self, other: &Self) -> Self {
        let mut merged = self.clone();
        if let (Some(merged_end_time), Some(other_start_time)) = (merged.end_time, other.start_time) {
            let time_diff = other_start_time - merged_end_time;
            merged.sum_time_diff = merged.sum_time_diff + time_diff;
            merged.end_time = other.end_time;
            merged.count += other.count - 1;
        }

        merged
    }

    fn evaluate(&self) -> Self::Output {
        if self.count < 2 {
            None
        } else {
            Some(self.sum_time_diff / (self.count as i32 - 1))
        }
    }
}
