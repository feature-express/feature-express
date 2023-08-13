use crate::partial_agg::PartialAggregate;

pub struct RootMeanSquare {
    count: usize,
    sum_of_squares: f64,
}

impl PartialAggregate for RootMeanSquare {
    type State = (usize, f64);
    type Input = f64;
    type Output = Option<f64>;

    fn new() -> Self {
        RootMeanSquare {
            count: 0,
            sum_of_squares: 0.0,
        }
    }

    fn update(&mut self, input: Self::Input) {
        self.count += 1;
        self.sum_of_squares += input * input;
    }

    fn merge(&self, other: &Self) -> Self {
        RootMeanSquare {
            count: self.count + other.count,
            sum_of_squares: self.sum_of_squares + other.sum_of_squares,
        }
    }

    fn evaluate(&self) -> Self::Output {
        if self.count == 0 {
            None
        } else {
            Some((self.sum_of_squares / self.count as f64).sqrt())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_root_mean_square() {
        let data = vec![1.0, 2.0, 3.0, 4.0, 5.0];
        let mut rms = RootMeanSquare::new();

        for value in data {
            rms.update(value);
        }

        let expected_result = Some(((55.0 / 5.0) as f64).sqrt());
        let result = rms.evaluate();

        assert_eq!(result, expected_result);
    }

    #[test]
    fn test_root_mean_square_merge() {
        let data1 = vec![1.0, 2.0, 3.0];
        let data2 = vec![4.0, 5.0];
        let mut rms1 = RootMeanSquare::new();
        let mut rms2 = RootMeanSquare::new();

        for value in data1 {
            rms1.update(value);
        }
        for value in data2 {
            rms2.update(value);
        }

        let rms_merged = rms1.merge(&rms2);
        let expected_result = Some(((55.0 / 5.0) as f64).sqrt());
        let result = rms_merged.evaluate();

        assert_eq!(result, expected_result);
    }

    #[test]
    fn test_root_mean_square_empty() {
        let rms = RootMeanSquare::new();

        let expected_result: Option<f64> = None;
        let result = rms.evaluate();

        assert_eq!(result, expected_result);
    }
}
