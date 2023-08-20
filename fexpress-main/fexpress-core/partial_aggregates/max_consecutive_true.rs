use crate::partial_agg::PartialAggregate;
use crate::partial_agg::SubtractPartialAggregate;
use chrono::NaiveDateTime;
use std::collections::VecDeque;

#[derive(Debug, Clone, PartialEq)]
pub struct StateSegment {
    start_timestamp: NaiveDateTime,
    end_timestamp: NaiveDateTime,
    value: bool,
    length: usize,
}

#[derive(Debug, Clone)]
pub struct MaxConsecutiveTrue {
    count: usize,
    state: Vec<StateSegment>,
    max_consecutive: usize,
}

impl PartialAggregate for MaxConsecutiveTrue {
    type State = (usize, Vec<StateSegment>, usize); // (count, segments, max_consecutive)
    type Input = (bool, NaiveDateTime);
    type Output = usize;

    fn new() -> Self {
        MaxConsecutiveTrue {
            count: 0,
            state: Vec::new(),
            max_consecutive: 0,
        }
    }

    fn update(&mut self, input: Self::Input) {
        let (value, timestamp) = input;
        self.count += 1;

        match self.state.last_mut() {
            Some(last_segment) if last_segment.value == value => {
                last_segment.end_timestamp = timestamp;
                last_segment.length += 1;
            }
            _ => {
                self.state.push(StateSegment {
                    start_timestamp: timestamp,
                    end_timestamp: timestamp,
                    value,
                    length: 1,
                });
            }
        }

        if value {
            self.max_consecutive = self.max_consecutive.max(self.state.last().unwrap().length);
        }
    }

    fn merge(&self, other: &Self) -> Self {
        let mut merged_segments = self.state.clone();
        let mut merged_max_consecutive = self.max_consecutive;

        for segment in other.state.iter() {
            match merged_segments.last_mut() {
                Some(last_segment) if last_segment.value == segment.value => {
                    last_segment.end_timestamp = segment.end_timestamp;
                    last_segment.length += segment.length;
                }
                _ => {
                    merged_segments.push(segment.clone());
                }
            }

            if segment.value {
                merged_max_consecutive =
                    merged_max_consecutive.max(merged_segments.last().unwrap().length);
            }
        }

        if merged_segments.len() > 1 {
            let segment_size = merged_segments.len();
            let last_two = &mut merged_segments[segment_size - 2..];
            if last_two[0].value && last_two[1].value {
                let new_len = last_two[0].length + last_two[1].length;
                last_two[0].length = new_len;
                last_two[0].end_timestamp = last_two[1].end_timestamp;
                merged_segments.pop();
                merged_max_consecutive = merged_max_consecutive.max(new_len);
            }
        }

        MaxConsecutiveTrue {
            count: self.count + other.count,
            state: merged_segments,
            max_consecutive: merged_max_consecutive,
        }
    }

    fn evaluate(&self) -> Self::Output {
        self.max_consecutive
    }
}

impl SubtractPartialAggregate for MaxConsecutiveTrue {
    fn subtract_inplace(&mut self, other: &Self) {
        if self.state.is_empty() || other.state.is_empty() {
            return;
        }

        // Check if subtracting from the beginning
        if self.state[0].start_timestamp == other.state[0].start_timestamp {
            for other_segment in &other.state {
                if self.state[0].value == other_segment.value
                    && self.state[0].end_timestamp >= other_segment.end_timestamp
                {
                    let diff = (other_segment.end_timestamp - self.state[0].start_timestamp)
                        .num_seconds() as usize
                        + 1;
                    if diff < self.state[0].length {
                        self.state[0].length -= diff;
                        self.state[0].start_timestamp =
                            other_segment.end_timestamp + chrono::Duration::seconds(1);
                    } else {
                        self.state.remove(0);
                    }
                } else {
                    break;
                }
            }
        }
        // Check if subtracting from the end
        else if self.state.last().unwrap().end_timestamp
            == other.state.last().unwrap().end_timestamp
        {
            for other_segment in other.state.iter().rev() {
                if let Some(last_segment) = self.state.last_mut() {
                    if last_segment.value == other_segment.value
                        && last_segment.start_timestamp <= other_segment.start_timestamp
                    {
                        let diff = (last_segment.end_timestamp - other_segment.start_timestamp)
                            .num_seconds() as usize
                            + 1;
                        if diff < last_segment.length {
                            last_segment.length -= diff;
                            last_segment.end_timestamp =
                                other_segment.start_timestamp - chrono::Duration::seconds(1);
                        } else {
                            self.state.pop();
                        }
                    } else {
                        break;
                    }
                } else {
                    break;
                }
            }
        }

        // After subtraction, recalculate max consecutive true count
        self.max_consecutive = self
            .state
            .iter()
            .filter(|&s| s.value)
            .map(|s| s.length)
            .max()
            .unwrap_or(0);
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

        assert_eq!(aggregate.evaluate(), 3);
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

        assert_eq!(aggregate.evaluate(), 2);
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
        assert_eq!(merged.evaluate(), 3);
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
        assert_eq!(aggregate.evaluate(), 2);
    }
}
