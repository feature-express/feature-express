use crate::partial_agg::{PartialAggregate, SubtractPartialAggregate};
use crate::types::FLOAT;
use crate::value::Value;

#[derive(Debug, Clone)]
pub struct Count {
    count: usize,
}

impl PartialAggregate for Count {
    type State = usize;
    type Input = Value;
    type Output = usize;

    fn new() -> Self {
        Count { count: 0 }
    }

    fn update(&mut self, _input: Self::Input) {
        self.count += 1;
    }

    fn merge(&self, other: &Self) -> Self {
        Count {
            count: self.count + other.count,
        }
    }

    fn merge_inplace(&mut self, other: &Self) {
        self.count += other.count;
    }

    fn evaluate(&self) -> Self::Output {
        self.count
    }
}

impl SubtractPartialAggregate for Count {
    fn subtract_inplace(&mut self, other: &Self) {
        self.count = self.count.saturating_sub(other.count);
    }

    fn subtract(&mut self, other: &Self) -> Self {
        Count {
            count: self.count.saturating_sub(other.count),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::value::Value;

    use super::*;

    #[test]
    fn test_count() {
        let data = vec![Value::Num(1.0), Value::Num(2.0), Value::Num(3.0)];
        let mut count = Count::new();

        for value in data {
            count.update(value);
        }

        let expected_result = 3 as usize;
        let result = count.evaluate();

        assert_eq!(result, expected_result);
    }

    #[test]
    fn test_count_empty() {
        let count = Count::new();

        let expected_result: usize = 0;
        let result = count.evaluate();

        assert_eq!(result, expected_result);
    }
}
