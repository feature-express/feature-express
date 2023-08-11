use crate::partial_agg::PartialAggregate;

pub struct StandardDeviation {
    count: usize,
    mean: f64,
    m2: f64,
}

impl PartialAggregate for StandardDeviation {
    type State = (usize, f64, f64);
    type Input = f64;
    type Output = f64;

    fn new() -> Self {
        StandardDeviation {
            count: 0,
            mean: 0.0,
            m2: 0.0,
        }
    }

    fn update(&mut self, input: Self::Input) {
        self.count += 1;
        let delta = input - self.mean;
        self.mean += delta / self.count as f64;
        let delta2 = input - self.mean;
        self.m2 += delta * delta2;
    }

    fn merge(&self, other: &Self) -> Self {
        let total_count = self.count + other.count;
        let delta = other.mean - self.mean;
        let m2 = self.m2
            + other.m2
            + delta * delta * (self.count * other.count) as f64 / total_count as f64;
        let mean =
            (self.mean * self.count as f64 + other.mean * other.count as f64) / total_count as f64;

        StandardDeviation {
            count: total_count,
            mean,
            m2,
        }
    }

    fn evaluate(&self) -> Self::Output {
        if self.count < 2 {
            0.0
        } else {
            (self.m2 / (self.count - 1) as f64).sqrt()
        }
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

        let expected_result = 1.5811388300841898; // Precomputed standard deviation
        let result = standard_deviation.evaluate();

        assert!((result - expected_result).abs() < 1e-9); // Comparing with a tolerance
    }
}
