use crate::partial_agg::PartialAggregate;
use crate::types::FLOAT;

pub struct Skewness {
    count: usize,
    mean: FLOAT,
    m2: FLOAT,
    m3: FLOAT,
}

impl PartialAggregate for Skewness {
    type State = (usize, FLOAT, FLOAT, FLOAT);
    type Input = FLOAT;
    type Output = FLOAT;

    fn new() -> Self {
        Skewness {
            count: 0,
            mean: 0.0,
            m2: 0.0,
            m3: 0.0,
        }
    }

    fn update(&mut self, input: Self::Input) {
        let count_FLOAT = self.count as FLOAT;
        let delta = input - self.mean;
        let delta_n = delta / (self.count + 1) as FLOAT;
        let term1 = delta * delta_n * count_FLOAT;
        self.mean += delta_n;
        self.m3 += term1 * delta_n * (count_FLOAT - 1.0) - 3.0 * delta_n * self.m2;
        self.m2 += term1;
        self.count += 1;
    }

    fn merge(&self, other: &Self) -> Self {
        let total_count = self.count + other.count;
        let delta = other.mean - self.mean;
        let delta2 = delta * delta;
        let delta3 = delta2 * delta;
        let total_count_FLOAT = total_count as FLOAT;
        let mean =
            (self.mean * self.count as FLOAT + other.mean * other.count as FLOAT) / total_count_FLOAT;
        let m2 =
            self.m2 + other.m2 + delta2 * self.count as FLOAT * other.count as FLOAT / total_count_FLOAT;
        let m3 = self.m3
            + other.m3
            + delta3 * self.count as FLOAT * (self.count - 1) as FLOAT * other.count as FLOAT
                / (total_count_FLOAT * (total_count - 1) as FLOAT)
            - 3.0 * delta * (self.count as FLOAT * other.m2 - other.count as FLOAT * self.m2)
                / total_count_FLOAT;

        Skewness {
            count: total_count,
            mean,
            m2,
            m3,
        }
    }

    fn merge_inplace(&mut self, other: &Self) {
        let total_count = self.count + other.count;
        let delta = other.mean - self.mean;
        let delta2 = delta * delta;
        let delta3 = delta2 * delta;
        let total_count_FLOAT = total_count as FLOAT;
        let mean =
            (self.mean * self.count as FLOAT + other.mean * other.count as FLOAT) / total_count_FLOAT;
        let m2 =
            self.m2 + other.m2 + delta2 * self.count as FLOAT * other.count as FLOAT / total_count_FLOAT;
        let m3 = self.m3
            + other.m3
            + delta3 * self.count as FLOAT * (self.count - 1) as FLOAT * other.count as FLOAT
            / (total_count_FLOAT * (total_count - 1) as FLOAT)
            - 3.0 * delta * (self.count as FLOAT * other.m2 - other.count as FLOAT * self.m2)
            / total_count_FLOAT;

        self.count = total_count;
        self.mean = mean;
        self.m2 = m2;
        self.m3 = m3;
    }

    fn evaluate(&self) -> Self::Output {
        if self.count < 3 {
            0.0
        } else {
            let m2_sqrt = (self.m2 / self.count as FLOAT).sqrt();
            if m2_sqrt > 1e-9 {
                self.m3 / (self.count as FLOAT * m2_sqrt.powi(3))
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
