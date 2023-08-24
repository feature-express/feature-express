use crate::partial_agg::{PartialAggregate, SubtractPartialAggregate};
use crate::types::FLOAT;

pub struct AbsEnergy {
    count: usize,
    state: FLOAT,
}

impl PartialAggregate for AbsEnergy {
    type State = (usize, FLOAT);
    type Input = FLOAT;
    type Output = Option<FLOAT>;

    fn new() -> Self {
        AbsEnergy {
            count: 0,
            state: 0.0,
        }
    }

    fn update(&mut self, input: Self::Input) {
        self.count += 1;
        self.state += input * input;
    }

    fn merge(&self, other: &Self) -> Self {
        AbsEnergy {
            count: self.count + other.count,
            state: self.state + other.state,
        }
    }

    fn merge_inplace(&mut self, other: &Self) {
        self.count += other.count;
        self.state += other.state;
    }

    fn evaluate(&self) -> Self::Output {
        if self.count == 0 {
            None
        } else {
            Some(self.state)
        }
    }
}

impl SubtractPartialAggregate for AbsEnergy {
    fn subtract_inplace(&mut self, other: &Self) {
        self.count -= other.count;
        self.state -= other.state;
    }

    fn subtract(&mut self, other: &Self) -> Self {
        AbsEnergy {
            count: self.count - other.count,
            state: self.state - other.state,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_abs_energy() {
        let data = vec![1.0, 2.0, 3.0, 4.0, 5.0];
        let mut abs_energy = AbsEnergy::new();

        for value in data {
            abs_energy.update(value);
        }

        let expected_result = Some(55.0); // 1^2 + 2^2 + 3^2 + 4^2 + 5^2
        let result = abs_energy.evaluate();

        assert_eq!(result, expected_result);
    }
}
