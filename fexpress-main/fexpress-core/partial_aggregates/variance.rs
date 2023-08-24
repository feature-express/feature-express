use crate::map::HashMap;
use crate::partial_agg::{PartialAggregate, SubtractPartialAggregate};
use crate::types::FLOAT;
use ordered_float::OrderedFloat;

#[derive(Clone, Debug)]
pub struct Variance {
    freq_map: HashMap<OrderedFloat<FLOAT>, usize>,
    total_values: usize,
}

impl PartialAggregate for Variance {
    type State = HashMap<OrderedFloat<FLOAT>, usize>;
    type Input = FLOAT;
    type Output = Option<FLOAT>;

    fn new() -> Self {
        Variance {
            freq_map: HashMap::new(),
            total_values: 0,
        }
    }

    fn update(&mut self, x: Self::Input) {
        *self.freq_map.entry(OrderedFloat(x)).or_insert(0) += 1;
        self.total_values += 1;
    }

    fn merge(&self, other: &Self) -> Self {
        let mut merged = self.freq_map.clone();

        for (key, value) in other.freq_map.iter() {
            *merged.entry(*key).or_insert(0) += *value;
        }

        Variance {
            freq_map: merged,
            total_values: self.total_values + other.total_values,
        }
    }

    fn merge_inplace(&mut self, other: &Self) {
        for (key, value) in other.freq_map.iter() {
            *self.freq_map.entry(*key).or_insert(0) += *value;
        }

        self.total_values += other.total_values;
    }

    fn evaluate(&self) -> Self::Output {
        if self.total_values < 2 {
            return None;
        }
        let mean: FLOAT = self
            .freq_map
            .iter()
            .map(|(value, freq)| value.into_inner() * (*freq as FLOAT))
            .sum::<FLOAT>()
            / self.total_values as FLOAT;

        let variance: FLOAT = self
            .freq_map
            .iter()
            .map(|(value, freq)| ((value.into_inner() - mean).powi(2) * (*freq as FLOAT)))
            .sum::<FLOAT>()
            / (self.total_values - 1) as FLOAT; // NOTE: We use N-1 here

        Some(variance)
    }
}

impl SubtractPartialAggregate for Variance {
    fn subtract_inplace(&mut self, other: &Self) {
        for (key, value) in other.freq_map.iter() {
            if let Some(count) = self.freq_map.get_mut(key) {
                if *count <= *value {
                    self.freq_map.remove(key);
                } else {
                    *count -= value;
                }
            }
        }
        self.total_values -= other.total_values;
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
    fn test_variance() {
        let data = vec![1.0, 2.0, 3.0, 4.0, 5.0];
        let mut standard_deviation = Variance::new();

        for value in data {
            standard_deviation.update(value);
        }

        let expected_result = 2.5 as FLOAT; // Precomputed standard deviation
        let result = standard_deviation.evaluate().unwrap();

        assert_eq!(result, expected_result);
    }

    #[test]
    fn test_variance_merge() {
        let mut variance1 = Variance::new();
        for i in 1..=5 {
            variance1.update(i as FLOAT);
        }

        let mut variance2 = Variance::new();
        for i in 6..=10 {
            variance2.update(i as FLOAT);
        }

        let variance_merged = variance1.merge(&variance2);

        assert_eq!(variance_merged.evaluate(), Some(9.166667)); // The variance for values 1 to 10 is 8.25
    }

    #[test]
    fn test_variance_subtract() {
        let mut variance1 = Variance::new();
        for i in 1..=10 {
            variance1.update(i as FLOAT);
        }

        let mut variance2 = Variance::new();
        for i in 1..=5 {
            variance2.update(i as FLOAT);
        }

        let subtracted_deviation = variance1.subtract(&variance2);

        // This should now only represent the variance for values 6 to 10
        assert_eq!(subtracted_deviation.evaluate(), Some(2.5));
    }
}
