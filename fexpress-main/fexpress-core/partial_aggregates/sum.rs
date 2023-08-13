use crate::partial_agg::PartialAggregate;

pub struct Sum {
    sum: f64,
    count: usize,
}

impl PartialAggregate for Sum {
    type State = (f64, usize);
    type Input = f64;
    type Output = Option<f64>;

    fn new() -> Self {
        Sum { sum: 0.0, count: 0 }
    }

    fn update(&mut self, input: Self::Input) {
        self.sum += input;
        self.count += 1;
    }

    fn merge(&self, other: &Self) -> Self {
        Sum {
            sum: self.sum + other.sum,
            count: self.count + other.count,
        }
    }

    fn evaluate(&self) -> Self::Output {
        if self.count == 0 {
            None
        } else {
            Some(self.sum)
        }
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

        let expected_result: Option<f64> = None;
        let result = sum.evaluate();

        assert_eq!(result, expected_result);
    }
}
