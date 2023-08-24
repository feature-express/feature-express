use crate::partial_agg::{PartialAggregate, SubtractPartialAggregate};

#[derive(Debug, Clone)]
pub struct All {
    true_count: usize,
    false_count: usize,
}

impl PartialAggregate for All {
    type State = (usize, usize); // (true_count, false_count)
    type Input = bool;
    type Output = bool;

    fn new() -> Self {
        All {
            true_count: 0,
            false_count: 0,
        }
    }

    fn update(&mut self, input: Self::Input) {
        if input {
            self.true_count += 1;
        } else {
            self.false_count += 1;
        }
    }

    fn merge(&self, other: &Self) -> Self {
        All {
            true_count: self.true_count + other.true_count,
            false_count: self.false_count + other.false_count,
        }
    }

    fn merge_inplace(&mut self, other: &Self) {
        self.true_count += other.true_count;
        self.false_count += other.false_count;
    }

    fn evaluate(&self) -> Self::Output {
        // Return true only if there are no false values
        self.false_count == 0
    }
}

impl SubtractPartialAggregate for All {
    fn subtract_inplace(&mut self, other: &Self) {
        self.true_count = self.true_count.saturating_sub(other.true_count);
        self.false_count = self.false_count.saturating_sub(other.false_count);
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
    fn test_all() {
        let data = vec![true, true, false, true];
        let mut all_agg = All::new();

        for value in data {
            all_agg.update(value);
        }

        let expected_result = false;
        let result = all_agg.evaluate();

        assert_eq!(result, expected_result);
    }
}
