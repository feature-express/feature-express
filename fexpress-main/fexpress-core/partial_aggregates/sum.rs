use crate::partial_agg::{PartialAggregate, SubtractPartialAggregate};
use crate::types::FLOAT;

#[derive(Clone, Debug)]
pub struct Sum {
    count: usize,
    total: FLOAT,
}

impl PartialAggregate for Sum {
    type State = (usize, FLOAT);
    type Input = FLOAT;
    type Output = Option<FLOAT>;

    fn new() -> Self {
        Sum {
            count: 0,
            total: 0.0,
        }
    }

    fn update(&mut self, input: Self::Input) {
        self.count += 1;
        self.total += input;
    }

    fn merge(&self, other: &Self) -> Self {
        Sum {
            count: self.count + other.count,
            total: self.total + other.total,
        }
    }

    fn merge_inplace(&mut self, other: &Self) {
        self.count += other.count;
        self.total += other.total;
    }

    fn evaluate(&self) -> Self::Output {
        if self.count == 0 {
            None
        } else {
            Some(self.total)
        }
    }
}

impl SubtractPartialAggregate for Sum {
    fn subtract_inplace(&mut self, other: &Self) {
        self.count -= other.count;
        self.total -= other.total;
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
    fn test_sum() {
        let data = vec![1.0, 2.0, 3.0, 4.0, 5.0];
        let mut sum = Sum::new();

        for value in data {
            sum.update(value);
        }

        let expected_result = Some(15.0);
        let result = sum.evaluate();

        assert_eq!(result, expected_result);
    }

    #[test]
    fn test_sum_merge() {
        let data1 = vec![1.0, 2.0, 3.0];
        let data2 = vec![4.0, 5.0];
        let mut sum1 = Sum::new();
        let mut sum2 = Sum::new();

        for value in data1 {
            sum1.update(value);
        }
        for value in data2 {
            sum2.update(value);
        }

        let sum_merged = sum1.merge(&sum2);
        let expected_result = Some(15.0);
        let result = sum_merged.evaluate();

        assert_eq!(result, expected_result);
    }

    #[test]
    fn test_sum_empty() {
        let sum = Sum::new();

        let expected_result: Option<FLOAT> = None;
        let result = sum.evaluate();

        assert_eq!(result, expected_result);
    }
}
