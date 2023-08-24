use crate::partial_agg::{PartialAggregate, SubtractPartialAggregate};
use crate::types::FLOAT;
use ordered_float::OrderedFloat;
use std::collections::BTreeMap;

#[derive(Clone, Debug)]
pub struct ArgMin<T>
where
    T: Ord + Clone,
{
    count: usize,
    state: BTreeMap<OrderedFloat<FLOAT>, Vec<T>>, // value -> timestamps
}

impl<T> PartialAggregate for ArgMin<T>
where
    T: Ord + Clone,
{
    type State = (usize, BTreeMap<OrderedFloat<FLOAT>, Vec<T>>);
    type Input = (T, FLOAT);
    type Output = Option<T>;

    fn new() -> Self {
        ArgMin {
            count: 0,
            state: BTreeMap::new(),
        }
    }

    fn update(&mut self, input: Self::Input) {
        let (timestamp, value) = input;
        self.count += 1;
        self.state
            .entry(OrderedFloat(value))
            .or_insert_with(Vec::new)
            .push(timestamp);
    }

    fn merge(&self, other: &Self) -> Self {
        let mut merged_state = self.state.clone();
        for (key, val) in other.state.iter() {
            merged_state
                .entry(*key)
                .or_insert_with(Vec::new)
                .extend(val.clone());
        }

        ArgMin {
            count: self.count + other.count,
            state: merged_state,
        }
    }

    fn merge_inplace(&mut self, other: &Self) {
        for (key, val) in other.state.iter() {
            self.state
                .entry(*key)
                .or_insert_with(Vec::new)
                .extend(val.clone());
        }

        self.count += other.count;
    }

    fn evaluate(&self) -> Self::Output {
        self.state.values().next().and_then(|v| v.first().cloned()) // Only change from ArgMax is here
    }
}

impl<T> SubtractPartialAggregate for ArgMin<T>
where
    T: Ord,
    T: Clone,
{
    fn subtract_inplace(&mut self, other: &Self) {
        for (key, val) in other.state.iter() {
            if let Some(current_vals) = self.state.get_mut(key) {
                for v in val.iter() {
                    if let Some(pos) = current_vals.iter().position(|x| *x == *v) {
                        current_vals.remove(pos);
                    }
                }
                if current_vals.is_empty() {
                    self.state.remove(key);
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
mod argmin_tests {
    use super::*;

    #[test]
    fn test_argmin() {
        let data = vec![(3, 3.0), (2, 2.0), (5, 5.0), (4, 4.0), (1, 1.5)];
        let mut argmin = ArgMin::new();

        for (timestamp, value) in data {
            argmin.update((timestamp, value));
        }

        let expected_result = Some(1); // because 2.0 is the smallest value and its timestamp is 2
        let result = argmin.evaluate();

        assert_eq!(result, expected_result);
    }
}
