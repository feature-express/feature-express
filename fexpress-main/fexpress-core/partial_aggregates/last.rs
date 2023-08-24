use crate::partial_agg::{PartialAggregate, SubtractPartialAggregate};
use std::collections::BTreeMap;
use std::iter::Product;

#[derive(Debug, Clone)]
pub struct Last<T, V>
where
    T: Ord + Clone,
    V: Clone,
{
    count: usize,
    state: BTreeMap<T, Vec<V>>, // timestamp -> values
}

impl<T, V> PartialAggregate for Last<T, V>
where
    T: Ord + Clone,
    V: Clone,
{
    type State = (usize, BTreeMap<T, Vec<V>>);
    type Input = (T, V);
    type Output = Option<V>;

    fn new() -> Self {
        Last {
            count: 0,
            state: BTreeMap::new(),
        }
    }

    fn update(&mut self, input: Self::Input) {
        let (timestamp, value) = input;
        self.count += 1;
        self.state
            .entry(timestamp)
            .or_insert_with(Vec::new)
            .push(value);
    }

    fn merge(&self, other: &Self) -> Self {
        let mut merged_state = self.state.clone();
        for (key, val) in other.state.iter() {
            merged_state
                .entry(key.clone())
                .or_insert_with(Vec::new)
                .extend(val.clone());
        }

        Last {
            count: self.count + other.count,
            state: merged_state,
        }
    }

    fn merge_inplace(&mut self, other: &Self) {
        for (key, val) in other.state.iter() {
            self.state
                .entry(key.clone())
                .or_insert_with(Vec::new)
                .extend(val.clone());
        }

        self.count += other.count;
    }

    fn evaluate(&self) -> Self::Output {
        self.state.values().last().and_then(|v| v.last().cloned())
    }
}

impl<T, V> SubtractPartialAggregate for Last<T, V>
where
    T: Ord + Clone,
    V: Clone + PartialEq,
{
    fn subtract_inplace(&mut self, other: &Self) {
        for (key, val) in other.state.iter() {
            if let Some(current_vals) = self.state.get_mut(key) {
                for v in val.iter() {
                    if let Some(pos) = current_vals.iter().position(|x| x == v) {
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
mod tests {
    use super::*;

    #[test]
    fn test_last() {
        let data = vec![(3, 1.0), (2, 2.0), (5, 5.0), (4, 4.0), (1, 1.5)];
        let mut last = Last::new();

        for (timestamp, value) in data {
            last.update((timestamp, value));
        }

        let expected_result = Some(5.0);
        let result = last.evaluate();

        assert_eq!(result, expected_result);
    }
}
