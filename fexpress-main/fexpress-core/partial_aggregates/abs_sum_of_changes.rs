use crate::partial_agg::PartialAggregate;
use crate::types::FLOAT;

pub struct AbsoluteSumOfChanges {
    state: FLOAT,
    prev_value: Option<FLOAT>,
}

impl PartialAggregate for AbsoluteSumOfChanges {
    type State = FLOAT;
    type Input = FLOAT;
    type Output = FLOAT;

    fn new() -> Self {
        AbsoluteSumOfChanges {
            state: 0.0,
            prev_value: None,
        }
    }

    fn update(&mut self, input: Self::Input) {
        if let Some(prev_value) = self.prev_value {
            self.state += (input - prev_value).abs();
        }
        self.prev_value = Some(input);
    }

    fn merge(&self, _other: &Self) -> Self {
        panic!("Merging is not supported for AbsoluteSumOfChanges");
    }

    fn evaluate(&self) -> Self::Output {
        self.state
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_absolute_sum_of_changes() {
        let data = vec![1.0, 2.0, 3.0, 4.0, 5.0];
        let mut absolute_sum_of_changes = AbsoluteSumOfChanges::new();

        for value in data {
            absolute_sum_of_changes.update(value);
        }

        let expected_result = 4.0; // |2 - 1| + |3 - 2| + |4 - 3| + |5 - 4|
        let result = absolute_sum_of_changes.evaluate();

        assert_eq!(result, expected_result);
    }
}
