use crate::partial_agg::PartialAggregate;

pub struct Skewness {
    count: usize,
    mean: f64,
    m2: f64,
    m3: f64,
}

impl PartialAggregate for Skewness {
    type State = (usize, f64, f64, f64);
    type Input = f64;
    type Output = f64;

    fn new() -> Self {
        Skewness {
            count: 0,
            mean: 0.0,
            m2: 0.0,
            m3: 0.0,
        }
    }

    fn update(&mut self, input: Self::Input) {
        let count_f64 = self.count as f64;
        let delta = input - self.mean;
        let delta_n = delta / (self.count + 1) as f64;
        let term1 = delta * delta_n * count_f64;
        self.mean += delta_n;
        self.m3 += term1 * delta_n * (count_f64 - 1.0) - 3.0 * delta_n * self.m2;
        self.m2 += term1;
        self.count += 1;
    }

    fn merge(&self, other: &Self) -> Self {
        let total_count = self.count + other.count;
        let delta = other.mean - self.mean;
        let delta2 = delta * delta;
        let delta3 = delta2 * delta;
        let total_count_f64 = total_count as f64;
        let mean =
            (self.mean * self.count as f64 + other.mean * other.count as f64) / total_count_f64;
        let m2 =
            self.m2 + other.m2 + delta2 * self.count as f64 * other.count as f64 / total_count_f64;
        let m3 = self.m3
            + other.m3
            + delta3 * self.count as f64 * (self.count - 1) as f64 * other.count as f64
                / (total_count_f64 * (total_count - 1) as f64)
            - 3.0 * delta * (self.count as f64 * other.m2 - other.count as f64 * self.m2)
                / total_count_f64;

        Skewness {
            count: total_count,
            mean,
            m2,
            m3,
        }
    }

    fn evaluate(&self) -> Self::Output {
        if self.count < 3 {
            0.0
        } else {
            let m2_sqrt = (self.m2 / self.count as f64).sqrt();
            if m2_sqrt > 1e-9 {
                self.m3 / (self.count as f64 * m2_sqrt.powi(3))
            } else {
                0.0
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_skewness() {
        let data = vec![1.0, 2.0, 3.0, 4.0, 5.0];
        let mut skewness = Skewness::new();

        for value in data {
            skewness.update(value);
        }

        let expected_result = 0.0; // Precomputed skewness for the input data
        let result = skewness.evaluate();

        assert!((result - expected_result).abs() < 1e-9); // Comparing with a
    }
}
