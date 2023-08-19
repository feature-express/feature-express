use crate::partial_agg::{PartialAggregate, SubtractPartialAggregate};

#[derive(Debug, Clone)]
pub struct Any {
    true_count: usize,
    false_count: usize,
}

impl PartialAggregate for Any {
    type State = (usize, usize); // (true_count, false_count)
    type Input = bool;
    type Output = bool;

    fn new() -> Self {
        Any {
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
        Any {
            true_count: self.true_count + other.true_count,
            false_count: self.false_count + other.false_count,
        }
    }

    fn evaluate(&self) -> Self::Output {
        // Return true if there is at least one true value
        self.true_count > 0
    }
}

impl SubtractPartialAggregate for Any {
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
    fn test_any_basic() {
        let data = vec![false, false, true, false];
        let mut any_agg = Any::new();

        for value in data {
            any_agg.update(value);
        }

        let expected_result = true;
        let result = any_agg.evaluate();

        assert_eq!(result, expected_result);
    }

    #[test]
    fn test_any_merge() {
        let mut agg1 = Any::new();
        let mut agg2 = Any::new();

        agg1.update(false);
        agg1.update(false);

        agg2.update(true);
        agg2.update(false);

        let merged = agg1.merge(&agg2);

        assert_eq!(merged.evaluate(), true);
    }

    #[test]
    fn test_any_subtract() {
        let mut agg1 = Any::new();
        let mut agg2 = Any::new();

        // Aggregate 1 gets three values: false, false, true
        agg1.update(false);
        agg1.update(false);
        agg1.update(true);

        // Aggregate 2 gets two of the same values: false, true
        agg2.update(false);
        agg2.update(true);

        agg1.subtract_inplace(&agg2);

        // After subtracting agg2 from agg1, agg1 should have a single false left.
        assert_eq!(agg1.evaluate(), false);
    }
}
