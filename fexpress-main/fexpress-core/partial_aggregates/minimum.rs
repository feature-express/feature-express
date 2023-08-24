use crate::partial_agg::{PartialAggregate, SubtractPartialAggregate};
use crate::types::FLOAT;
use ordered_float::OrderedFloat;
use std::collections::BTreeMap;

#[derive(Debug, Clone)]
pub struct Minimum {
    count: usize,
    state: BTreeMap<OrderedFloat<FLOAT>, usize>, // (value, occurrences)
}

impl PartialAggregate for Minimum {
    type State = (usize, BTreeMap<OrderedFloat<FLOAT>, usize>);
    type Input = FLOAT;
    type Output = Option<FLOAT>;

    fn new() -> Self {
        Minimum {
            count: 0,
            state: BTreeMap::new(),
        }
    }

    fn update(&mut self, input: Self::Input) {
        let ordered_input = OrderedFloat(input);
        self.count += 1;
        *self.state.entry(ordered_input).or_insert(0) += 1;
    }

    fn merge(&self, other: &Self) -> Self {
        let mut merged_state = self.state.clone();
        for (key, val) in other.state.iter() {
            *merged_state.entry(*key).or_insert(0) += val;
        }

        Minimum {
            count: self.count + other.count,
            state: merged_state,
        }
    }

    fn merge_inplace(&mut self, other: &Self) {
        for (key, val) in other.state.iter() {
            *self.state.entry(*key).or_insert(0) += val;
        }

        self.count += other.count;
    }

    fn evaluate(&self) -> Self::Output {
        self.state.keys().next().map(|&k| k.into_inner())
    }
}

impl SubtractPartialAggregate for Minimum {
    fn subtract_inplace(&mut self, other: &Self) {
        for (key, val) in other.state.iter() {
            if let Some(current_val) = self.state.get_mut(key) {
                *current_val -= val;
                if *current_val == 0 {
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
    fn test_minimum() {
        let data = vec![3.0, 2.0, 5.0, 4.0, 1.0];
        let mut minimum = Minimum::new();

        for value in data {
            minimum.update(value);
        }

        let expected_result = Some(1.0);
        let result = minimum.evaluate();

        assert_eq!(result, expected_result);
    }
}
