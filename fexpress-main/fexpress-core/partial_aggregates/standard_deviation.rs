use crate::map::HashMap;
use crate::partial_agg::{PartialAggregate, SubtractPartialAggregate};
use crate::types::FLOAT;
use ordered_float::OrderedFloat;

#[derive(Clone, Debug)]
pub struct StandardDeviation {
    freq_map: HashMap<OrderedFloat<FLOAT>, usize>,
    total_values: usize,
}

impl PartialAggregate for StandardDeviation {
    type State = HashMap<OrderedFloat<FLOAT>, usize>;
    type Input = FLOAT;
    type Output = Option<FLOAT>;

    fn new() -> Self {
        StandardDeviation {
            freq_map: HashMap::new(),
            total_values: 0,
        }
    }

    fn update(&mut self, x: Self::Input) {
        *self.freq_map.entry(OrderedFloat(x)).or_insert(0) += 1;
        self.total_values += 1;
    }

    fn merge_inplace(&mut self, other: &Self) {
        for (key, value) in other.freq_map.iter() {
            *self.freq_map.entry(*key).or_insert(0) += *value;
        }
        self.total_values += other.total_values;
    }

    fn merge(&self, other: &Self) -> Self {
        let mut merged = self.freq_map.clone();

        for (key, value) in other.freq_map.iter() {
            *merged.entry(*key).or_insert(0) += *value;
        }

        StandardDeviation {
            freq_map: merged,
            total_values: self.total_values + other.total_values,
        }
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

        Some(variance.sqrt())
    }
}

impl SubtractPartialAggregate for StandardDeviation {
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
    fn test_standard_deviation() {
        let data = vec![1.0, 2.0, 3.0, 4.0, 5.0];
        let mut standard_deviation = StandardDeviation::new();

        for value in data {
            standard_deviation.update(value);
        }

        let expected_result = 1.5811388300841898 as FLOAT; // Precomputed standard deviation
        let result = standard_deviation.evaluate().unwrap();

        assert_eq!(result, expected_result);
    }

    #[test]
    fn test_stddev_merge() {
        let mut stddev1 = StandardDeviation::new();
        for i in 1..=5 {
            stddev1.update(i as FLOAT);
        }

        let mut stddev2 = StandardDeviation::new();
        for i in 6..=10 {
            stddev2.update(i as FLOAT);
        }

        let stddev_merged = stddev1.merge(&stddev2);

        assert_eq!(stddev_merged.evaluate(), Some(3.0276504)); // The variance for values 1 to 10 is 8.25
    }

    #[test]
    fn test_stddev_subtract() {
        let mut stddev1 = StandardDeviation::new();
        for i in 1..=10 {
            stddev1.update(i as FLOAT);
        }

        let mut variance2 = StandardDeviation::new();
        for i in 1..=5 {
            variance2.update(i as FLOAT);
        }

        let subtracted_deviation = stddev1.subtract(&variance2);

        // This should now only represent the variance for values 6 to 10
        assert_eq!(subtracted_deviation.evaluate(), Some(1.5811388));
    }
}
