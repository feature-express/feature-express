use std::collections::HashMap;
use crate::types::FLOAT;

use crate::partial_agg::PartialAggregate;

pub struct HasDuplicate {
    value_counts: HashMap<String, usize>,
    any_duplicate: bool,
}

impl PartialAggregate for HasDuplicate {
    type State = HashMap<String, usize>;
    type Input = FLOAT;
    type Output = bool;

    fn new() -> Self {
        HasDuplicate {
            value_counts: HashMap::new(),
            any_duplicate: false,
        }
    }

    fn update(&mut self, input: Self::Input) {
        let input_str = format!("{:.10}", input);
        let count = self.value_counts.entry(input_str).or_insert(0);
        *count += 1;
        if *count > 1 && !self.any_duplicate {
            self.any_duplicate = true;
        }
    }

    fn merge(&self, other: &Self) -> Self {
        let mut merged_counts = self.value_counts.clone();

        let mut any_duplicate = false;
        for (value, count) in other.value_counts.iter() {
            let entry = merged_counts.entry(value.to_string()).or_insert(0);
            *entry += count;
            if *entry > 1 {
                any_duplicate = true;
            }
        }

        HasDuplicate {
            value_counts: merged_counts,
            any_duplicate: any_duplicate || self.any_duplicate || other.any_duplicate,
        }
    }

    fn merge_inplace(&mut self, other: &Self) {
        let mut any_duplicate = false;
        for (value, count) in other.value_counts.iter() {
            let entry = self.value_counts.entry(value.to_string()).or_insert(0);
            *entry += count;
            if *entry > 1 {
                any_duplicate = true;
            }
        }

        self.any_duplicate = any_duplicate || self.any_duplicate || other.any_duplicate;
    }

    fn evaluate(&self) -> Self::Output {
        self.any_duplicate
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_has_duplicate() {
        let data = vec![1.0, 2.0, 3.0, 2.0, 4.0];
        let mut has_duplicate = HasDuplicate::new();

        for value in data {
            has_duplicate.update(value);
        }

        let expected_result = true;
        let result = has_duplicate.evaluate();

        assert_eq!(result, expected_result);
    }
}
