use crate::partial_agg::PartialAggregate;
use crate::types::FLOAT;

pub struct Kurtosis {
    count: usize,
    sum: FLOAT,
    sum_sq: FLOAT,
    sum_cub: FLOAT,
    sum_quart: FLOAT,
}

impl PartialAggregate for Kurtosis {
    type State = (usize, FLOAT, FLOAT, FLOAT, FLOAT);
    type Input = FLOAT;
    type Output = Option<FLOAT>;

    fn new() -> Self {
        Kurtosis {
            count: 0,
            sum: 0.0,
            sum_sq: 0.0,
            sum_cub: 0.0,
            sum_quart: 0.0,
        }
    }

    fn update(&mut self, input: Self::Input) {
        self.count += 1;
        self.sum += input;
        self.sum_sq += input * input;
        self.sum_cub += input * input * input;
        self.sum_quart += input * input * input * input;
    }

    fn merge(&self, other: &Self) -> Self {
        Kurtosis {
            count: self.count + other.count,
            sum: self.sum + other.sum,
            sum_sq: self.sum_sq + other.sum_sq,
            sum_cub: self.sum_cub + other.sum_cub,
            sum_quart: self.sum_quart + other.sum_quart,
        }
    }

    fn evaluate(&self) -> Self::Output {
        if self.count < 4 {
            None
        } else {
            let n = self.count as FLOAT;
            let mean = self.sum / n;
            let variance = (self.sum_sq - n * mean * mean) / (n - 1.0);
            let m3 = (self.sum_cub - 3.0 * mean * self.sum_sq + 3.0 * mean * mean * self.sum) / n;
            let m4 = (self.sum_quart - 4.0 * mean * self.sum_cub + 6.0 * mean * mean * self.sum_sq
                - 4.0 * mean * mean * mean * self.sum)
                / n;
            let kurtosis = (n * (n + 1.0) * m4 - 3.0 * m3 * m3 * (n - 1.0))
                / ((n - 1.0) * (n - 2.0) * (n - 3.0) * variance * variance);
            Some(kurtosis)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_kurtosis_evaluate() {
        let data = vec![1.0, 2.0, 3.0, 4.0, 5.0];
        let mut kurtosis = Kurtosis::new();

        for value in data {
            kurtosis.update(value);
        }

        let expected_result = Some(-1.3);
        let result = kurtosis.evaluate().map(|x| (x * 10.0).round() / 10.0); // Round to one decimal place

        assert_eq!(result, expected_result);
    }

    #[test]
    fn test_kurtosis_merge() {
        let data1 = vec![1.0, 2.0, 3.0];
        let data2 = vec![4.0, 5.0];
        let mut kurtosis1 = Kurtosis::new();
        let mut kurtosis2 = Kurtosis::new();

        for value in data1 {
            kurtosis1.update(value);
        }
        for value in data2 {
            kurtosis2.update(value);
        }

        let kurtosis_merged = kurtosis1.merge(&kurtosis2);
        let expected_result = Some(-1.3);
        let result = kurtosis_merged.evaluate().map(|x| (x * 10.0).round() / 10.0); // Round to one decimal place

        assert_eq!(result, expected_result);
    }

    #[test]
    fn test_kurtosis_empty() {
        let mut kurtosis = Kurtosis::new();

        let expected_result: Option<FLOAT> = None;
        let result = kurtosis.evaluate();

        assert_eq!(result, expected_result);
    }
}
