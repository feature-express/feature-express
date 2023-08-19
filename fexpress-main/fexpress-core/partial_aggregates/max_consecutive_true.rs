use crate::partial_agg::PartialAggregate;
use crate::partial_agg::SubtractPartialAggregate;
use chrono::NaiveDateTime;
use std::collections::VecDeque;

#[derive(Debug, Clone)]
pub struct Interval {
    start: NaiveDateTime,
    end: NaiveDateTime,
}

impl Interval {
    fn duration(&self) -> i64 {
        (self.end - self.start).num_seconds() + 1
    }
}

#[derive(Debug, Clone)]
pub struct MaxConsecutiveTrue {
    count: i64,
    intervals: VecDeque<Interval>,
    max_count: i64,
}

impl MaxConsecutiveTrue {
    fn recalculate_max(&mut self) {
        self.max_count = 0;
        for interval in &self.intervals {
            self.max_count = self.max_count.max(interval.duration());
        }
    }
}

impl PartialAggregate for MaxConsecutiveTrue {
    type State = (i64, VecDeque<Interval>, i64);
    type Input = (bool, NaiveDateTime);
    type Output = Option<i64>;

    fn new() -> Self {
        MaxConsecutiveTrue {
            count: 0,
            intervals: VecDeque::new(),
            max_count: 0,
        }
    }

    fn update(&mut self, input: Self::Input) {
        let (val, timestamp) = input;
        self.count += 1;

        if val {
            if let Some(last_interval) = self.intervals.back_mut() {
                if last_interval.end + chrono::Duration::seconds(1) == timestamp {
                    last_interval.end = timestamp;
                } else {
                    self.intervals.push_back(Interval {
                        start: timestamp,
                        end: timestamp,
                    });
                }
            } else {
                self.intervals.push_back(Interval {
                    start: timestamp,
                    end: timestamp,
                });
            }

            if let Some(last_interval) = self.intervals.back() {
                self.max_count = self.max_count.max(last_interval.duration());
            }
        }
    }

    fn merge(&self, other: &Self) -> Self {
        let mut merged_intervals = self.intervals.clone();

        if let Some(last_interval) = merged_intervals.back_mut() {
            if let Some(first_interval_other) = other.intervals.front() {
                // If the last interval of the merged_intervals and the first interval of other.intervals
                // are consecutive, merge them into one interval.
                if last_interval.end + chrono::Duration::seconds(1) == first_interval_other.start {
                    last_interval.end = first_interval_other.end;
                    // Now extend the rest, skipping the first interval from other as we have already merged it
                    merged_intervals.extend(other.intervals.iter().skip(1).cloned());
                } else {
                    merged_intervals.extend(other.intervals.iter().cloned());
                }
            }
        } else {
            merged_intervals.extend(other.intervals.iter().cloned());
        }

        let mut merged = MaxConsecutiveTrue {
            count: self.count + other.count,
            intervals: merged_intervals,
            max_count: 0, // This will be recalculated
        };
        merged.recalculate_max();

        merged
    }

    fn evaluate(&self) -> Self::Output {
        if self.max_count > 0 {
            Some(self.max_count)
        } else {
            None
        }
    }
}

impl SubtractPartialAggregate for MaxConsecutiveTrue {
    fn subtract_inplace(&mut self, other: &Self) {
        for other_interval in &other.intervals {
            // Here, we need logic to subtract other_interval from the intervals in self.
            // This is a complex operation and might involve splitting or removing intervals in self.

            let mut to_remove = Vec::new();
            for (idx, interval) in self.intervals.iter_mut().enumerate() {
                // If the other interval is entirely inside the current interval,
                // split the current interval.
                if interval.start <= other_interval.start && interval.end >= other_interval.end {
                    let new_interval = Interval {
                        start: other_interval.end + chrono::Duration::seconds(1),
                        end: interval.end,
                    };
                    interval.end = other_interval.start - chrono::Duration::seconds(1);
                    self.intervals.insert(idx + 1, new_interval);
                    break;
                }
                // If the other interval overlaps the start of the current interval,
                // adjust the start of the current interval.
                else if interval.start <= other_interval.start
                    && interval.end >= other_interval.start
                {
                    interval.end = other_interval.start - chrono::Duration::seconds(1);
                    break;
                }
                // If the other interval overlaps the end of the current interval,
                // adjust the end of the current interval.
                else if interval.start <= other_interval.end && interval.end >= other_interval.end
                {
                    interval.start = other_interval.end + chrono::Duration::seconds(1);
                    break;
                }
                // If the other interval is entirely outside the current interval,
                // remove the current interval.
                else if interval.start >= other_interval.start
                    && interval.end <= other_interval.end
                {
                    to_remove.push(idx);
                }
            }

            for idx in to_remove.iter().rev() {
                self.intervals.remove(*idx);
            }
        }

        self.recalculate_max();
    }

    fn subtract(&mut self, other: &Self) -> Self {
        let mut result = self.clone();
        result.subtract_inplace(other);
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::NaiveDate;

    #[test]
    fn test_basic_consecutive() {
        let data = vec![
            (true, NaiveDate::from_ymd(2023, 8, 19).and_hms(0, 0, 1)),
            (true, NaiveDate::from_ymd(2023, 8, 19).and_hms(0, 0, 2)),
            (true, NaiveDate::from_ymd(2023, 8, 19).and_hms(0, 0, 3)),
        ];

        let mut aggregate = MaxConsecutiveTrue::new();
        for (value, timestamp) in data {
            aggregate.update((value, timestamp));
        }

        assert_eq!(aggregate.evaluate(), Some(3));
    }

    #[test]
    fn test_non_consecutive() {
        let data = vec![
            (true, NaiveDate::from_ymd(2023, 8, 19).and_hms(0, 0, 1)),
            (false, NaiveDate::from_ymd(2023, 8, 19).and_hms(0, 0, 2)),
            (true, NaiveDate::from_ymd(2023, 8, 19).and_hms(0, 0, 4)),
            (true, NaiveDate::from_ymd(2023, 8, 19).and_hms(0, 0, 5)),
        ];

        let mut aggregate = MaxConsecutiveTrue::new();
        for (value, timestamp) in data {
            aggregate.update((value, timestamp));
        }

        assert_eq!(aggregate.evaluate(), Some(2));
    }

    #[test]
    fn test_merge() {
        let mut aggregate1 = MaxConsecutiveTrue::new();
        aggregate1.update((true, NaiveDate::from_ymd(2023, 8, 19).and_hms(0, 0, 1)));
        aggregate1.update((true, NaiveDate::from_ymd(2023, 8, 19).and_hms(0, 0, 2)));

        let mut aggregate2 = MaxConsecutiveTrue::new();
        aggregate2.update((true, NaiveDate::from_ymd(2023, 8, 19).and_hms(0, 0, 3)));
        aggregate2.update((false, NaiveDate::from_ymd(2023, 8, 19).and_hms(0, 0, 4)));

        let merged = aggregate1.merge(&aggregate2);
        assert_eq!(merged.evaluate(), Some(3));
    }

    #[test]
    fn test_subtract_middle() {
        let mut aggregate = MaxConsecutiveTrue::new();
        aggregate.update((true, NaiveDate::from_ymd(2023, 8, 19).and_hms(0, 0, 1)));
        aggregate.update((true, NaiveDate::from_ymd(2023, 8, 19).and_hms(0, 0, 2)));
        aggregate.update((true, NaiveDate::from_ymd(2023, 8, 19).and_hms(0, 0, 3)));

        let mut subtract_aggregate = MaxConsecutiveTrue::new();
        subtract_aggregate.update((true, NaiveDate::from_ymd(2023, 8, 19).and_hms(0, 0, 2)));

        aggregate.subtract_inplace(&subtract_aggregate);
        assert_eq!(aggregate.evaluate(), Some(1));
    }

    #[test]
    fn test_subtract_edge() {
        let mut aggregate = MaxConsecutiveTrue::new();
        aggregate.update((true, NaiveDate::from_ymd(2023, 8, 19).and_hms(0, 0, 1)));
        aggregate.update((true, NaiveDate::from_ymd(2023, 8, 19).and_hms(0, 0, 2)));
        aggregate.update((true, NaiveDate::from_ymd(2023, 8, 19).and_hms(0, 0, 3)));

        let mut subtract_aggregate = MaxConsecutiveTrue::new();
        subtract_aggregate.update((true, NaiveDate::from_ymd(2023, 8, 19).and_hms(0, 0, 1)));

        aggregate.subtract_inplace(&subtract_aggregate);
        assert_eq!(aggregate.evaluate(), Some(2));
    }

    // And many more edge cases...
}
