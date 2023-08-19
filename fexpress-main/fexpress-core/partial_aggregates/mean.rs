use crate::partial_agg::{PartialAggregate, SubtractPartialAggregate};
use crate::types::FLOAT;

pub struct Mean {
    count: usize,
    sum: FLOAT,
}

impl PartialAggregate for Mean {
    type State = (usize, FLOAT);
    type Input = FLOAT;
    type Output = Option<FLOAT>;

    fn new() -> Self {
        Mean { count: 0, sum: 0.0 }
    }

    fn update(&mut self, input: Self::Input) {
        self.count += 1;
        self.sum += input;
    }

    fn merge(&self, other: &Self) -> Self {
        let total_count = self.count + other.count;
        let total_sum = self.sum + other.sum;

        Mean {
            count: total_count,
            sum: total_sum,
        }
    }

    fn evaluate(&self) -> Self::Output {
        if self.count == 0 {
            None
        } else {
            Some(self.sum / self.count as FLOAT)
        }
    }
}

impl SubtractPartialAggregate for Mean {
    fn subtract_inplace(&mut self, other: &Self) {
        if other.count > self.count {
            panic!("Cannot subtract partial aggregates with different state");
        }

        if other.count == 0 {
            return;
        }

        let total_count = self.count - other.count;
        let total_sum = self.sum - other.sum;

        if total_count == 0 {
            self.count = 0;
            self.sum = 0.0;
        } else {
            self.count = total_count;
            self.sum = total_sum;
        }
    }

    fn subtract(&mut self, other: &Self) -> Self {
        if other.count > self.count {
            panic!("Cannot subtract partial aggregates with different state");
        }

        let mut result = Mean {
            count: self.count,
            sum: self.sum,
        };
        result.subtract_inplace(other);
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mean() {
        let data = vec![1.0, 2.0, 3.0, 4.0, 5.0];
        let mut mean = Mean::new();

        for value in data {
            mean.update(value);
        }

        let expected_result = Some(3.0);
        let result = mean.evaluate();

        assert_eq!(result, expected_result);
    }

    #[test]
    fn test_mean_empty() {
        let mean = Mean::new();

        let expected_result: Option<FLOAT> = None;
        let result = mean.evaluate();

        assert_eq!(result, expected_result);
    }
}
