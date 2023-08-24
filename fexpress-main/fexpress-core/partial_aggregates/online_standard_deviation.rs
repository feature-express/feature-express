use crate::partial_agg::{PartialAggregate, SubtractPartialAggregate};
use crate::types::FLOAT;

#[derive(Clone, Debug)]
pub struct OnlineStandardDeviation {
    count: usize,
    mean: FLOAT,
    sum_squared_diffs: FLOAT,
}

impl PartialAggregate for OnlineStandardDeviation {
    type State = (usize, FLOAT, FLOAT);
    type Input = FLOAT;
    type Output = Option<FLOAT>;

    fn new() -> Self {
        OnlineStandardDeviation {
            count: 0,
            mean: 0.0,
            sum_squared_diffs: 0.0,
        }
    }

    fn update(&mut self, input: Self::Input) {
        self.count += 1;
        let delta = input - self.mean;
        self.mean += delta / self.count as FLOAT;
        let delta2 = input - self.mean;
        self.sum_squared_diffs += delta * delta2;
    }

    fn merge(&self, other: &Self) -> Self {
        let total_count = self.count + other.count;
        let delta_mean = other.mean - self.mean;
        let new_mean = self.mean + delta_mean * (other.count as FLOAT / total_count as FLOAT);
        let new_sum_squared_diffs = self.sum_squared_diffs
            + other.sum_squared_diffs
            + delta_mean * delta_mean * (self.count * other.count) as FLOAT / total_count as FLOAT;

        OnlineStandardDeviation {
            count: total_count,
            mean: new_mean,
            sum_squared_diffs: new_sum_squared_diffs,
        }
    }

    fn merge_inplace(&mut self, other: &Self) {
        let total_count = self.count + other.count;
        let delta_mean = other.mean - self.mean;
        self.mean += delta_mean * (other.count as FLOAT / total_count as FLOAT);
        self.sum_squared_diffs += other.sum_squared_diffs
            + delta_mean * delta_mean * (self.count * other.count) as FLOAT / total_count as FLOAT;
        self.count = total_count;
    }

    fn evaluate(&self) -> Self::Output {
        if self.count > 1 {
            Some((self.sum_squared_diffs / (self.count as FLOAT - 1.0)).sqrt())
        } else {
            None
        }
    }
}
