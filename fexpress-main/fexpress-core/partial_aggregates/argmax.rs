use crate::partial_agg::{PartialAggregate, SubtractPartialAggregate};
use crate::types::FLOAT;
use ordered_float::OrderedFloat;
use std::collections::BTreeMap;

/// Represents a structure to find the argmax.
/// ArgMax is determined by the maximum FLOAT value and its earliest timestamp.
#[derive(Clone, Debug)]
pub struct ArgMax<T>
where
    T: Ord + Clone,
{
    count: usize,
    state: BTreeMap<OrderedFloat<FLOAT>, BTreeMap<T, usize>>, // value -> {timestamp -> count}
}

impl<T> PartialAggregate for ArgMax<T>
where
    T: Ord + Clone,
{
    type State = (usize, BTreeMap<OrderedFloat<FLOAT>, BTreeMap<T, usize>>);
    type Input = (T, FLOAT);
    type Output = Option<T>;

    fn new() -> Self {
        ArgMax {
            count: 0,
            state: BTreeMap::new(),
        }
    }

    fn update(&mut self, input: Self::Input) {
        let (timestamp, value) = input;
        self.count += 1;
        let ts_map = self
            .state
            .entry(OrderedFloat(value))
            .or_insert_with(BTreeMap::new);
        *ts_map.entry(timestamp).or_insert(0) += 1;
    }

    fn merge(&self, other: &Self) -> Self {
        let mut merged_state = self.state.clone();
        for (key, inner_map) in other.state.iter() {
            let merged_inner_map = merged_state.entry(*key).or_insert_with(BTreeMap::new);
            for (ts, &count) in inner_map.iter() {
                *merged_inner_map.entry(ts.clone()).or_insert(0) += count;
            }
        }

        ArgMax {
            count: self.count + other.count,
            state: merged_state,
        }
    }

    fn evaluate(&self) -> Self::Output {
        self.state
            .keys()
            .last()
            .and_then(|k| self.state[k].keys().next().cloned())
    }
}

impl<T> SubtractPartialAggregate for ArgMax<T>
where
    T: Ord,
    T: Clone,
{
    fn subtract_inplace(&mut self, other: &Self) {
        for (&key, inner_map) in other.state.iter() {
            if let Some(merged_inner_map) = self.state.get_mut(&key) {
                for (ts, &count) in inner_map.iter() {
                    if let Some(merged_count) = merged_inner_map.get_mut(ts) {
                        *merged_count -= count;
                        if *merged_count == 0 {
                            merged_inner_map.remove(ts);
                        }
                    }
                }
                if merged_inner_map.is_empty() {
                    self.state.remove(&key);
                }
            }
        }
        self.count -= other.count;
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
    fn test_argmax() {
        let data = vec![(3, 3.0), (2, 2.0), (5, 5.0), (4, 4.0), (1, 1.5)];
        let mut argmax = ArgMax::new();

        for (timestamp, value) in data {
            argmax.update((timestamp, value));
        }

        let expected_result = Some(5);
        let result = argmax.evaluate();
        assert_eq!(result, expected_result);
    }

    #[test]
    fn test_merge() {
        let mut aggregate1 = ArgMax::new();
        aggregate1.update((3, 2.0));
        aggregate1.update((4, 3.0));

        let mut aggregate2 = ArgMax::new();
        aggregate2.update((5, 4.0));
        aggregate2.update((6, 1.0));

        let merged = aggregate1.merge(&aggregate2);
        assert_eq!(merged.evaluate(), Some(5));

        let mut aggregate3 = ArgMax::new();
        aggregate3.update((5, 5.0));
        aggregate3.update((6, 6.0));

        let merged2 = merged.merge(&aggregate3);
        assert_eq!(merged2.evaluate(), Some(6));
    }

    #[test]
    fn test_subtract() {
        let mut aggregate1 = ArgMax::new();
        aggregate1.update((3, 3.0));
        aggregate1.update((4, 4.0));
        aggregate1.update((5, 5.0));

        let mut subtractor = ArgMax::new();
        subtractor.update((5, 5.0));
        subtractor.update((6, 6.0));

        aggregate1.subtract_inplace(&subtractor);
        assert_eq!(aggregate1.evaluate(), Some(4));

        let mut subtractor2 = ArgMax::new();
        subtractor2.update((4, 4.0));

        aggregate1.subtract_inplace(&subtractor2);
        assert_eq!(aggregate1.evaluate(), Some(3));
    }

    #[test]
    fn test_duplicate_values() {
        let mut aggregate = ArgMax::new();
        aggregate.update((3, 3.0));
        aggregate.update((4, 3.0));

        let result = aggregate.evaluate();
        assert_eq!(result, Some(3));

        let mut subtractor = ArgMax::new();
        subtractor.update((3, 3.0));

        aggregate.subtract_inplace(&subtractor);
        assert_eq!(aggregate.evaluate(), Some(4));
    }
}
