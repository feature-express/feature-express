use crate::partial_agg::{PartialAggregate, SubtractPartialAggregate};
use std::collections::HashMap;
use std::hash::Hash;

#[derive(Debug, Clone)]
pub struct NUnique<T>
where
    T: Ord + Clone,
{
    state: HashMap<T, usize>, // Element -> Count
}

impl<T> PartialAggregate for NUnique<T>
where
    T: Hash + Ord + Clone,
{
    type State = HashMap<T, usize>;
    type Input = T;
    type Output = usize; // number of unique elements

    fn new() -> Self {
        NUnique {
            state: HashMap::new(),
        }
    }

    fn update(&mut self, input: Self::Input) {
        *self.state.entry(input).or_insert(0) += 1;
    }

    fn merge(&self, other: &Self) -> Self {
        let mut merged_state = self.state.clone();
        for (key, val) in other.state.iter() {
            *merged_state.entry(key.clone()).or_insert(0) += val;
        }

        NUnique {
            state: merged_state,
        }
    }

    fn merge_inplace(&mut self, other: &Self) {
        for (key, val) in other.state.iter() {
            *self.state.entry(key.clone()).or_insert(0) += val;
        }
    }

    fn evaluate(&self) -> Self::Output {
        self.state.keys().count()
    }
}

impl<T> SubtractPartialAggregate for NUnique<T>
where
    T: Ord + Clone + Hash,
{
    fn subtract_inplace(&mut self, other: &Self) {
        for (key, val) in other.state.iter() {
            if let Some(count) = self.state.get_mut(key) {
                if *count <= *val {
                    self.state.remove(key);
                } else {
                    *count -= val;
                }
            }
        }
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
    fn test_n_unique_basic() {
        let data = vec![1, 2, 2, 3, 3, 3];
        let mut n_unique_agg = NUnique::new();

        for value in data {
            n_unique_agg.update(value);
        }

        let expected_result = 3;
        let result = n_unique_agg.evaluate();

        assert_eq!(result, expected_result);
    }

    #[test]
    fn test_n_unique_merge() {
        let mut agg1 = NUnique::new();
        let mut agg2 = NUnique::new();

        agg1.update(1);
        agg1.update(2);

        agg2.update(2);
        agg2.update(3);

        let merged = agg1.merge(&agg2);

        assert_eq!(merged.evaluate(), 3);
    }

    #[test]
    fn test_n_unique_subtract() {
        let mut agg1 = NUnique::new();
        let mut agg2 = NUnique::new();

        agg1.update(1);
        agg1.update(2);
        agg1.update(3);

        agg2.update(2);

        agg1.subtract_inplace(&agg2);

        assert_eq!(agg1.evaluate(), 2); // Only 1 and 3 remain as unique numbers.
    }
}
