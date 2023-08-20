use crate::partial_agg::PartialAggregate;
use crate::types::FLOAT;

#[derive(Clone)]
pub struct HasDuplicateMax {
    max_value: Option<FLOAT>,
    max_count: usize,
}

impl PartialAggregate for HasDuplicateMax {
    type State = (Option<FLOAT>, usize);
    type Input = FLOAT;
    type Output = bool;

    fn new() -> Self {
        HasDuplicateMax {
            max_value: None,
            max_count: 0,
        }
    }

    fn update(&mut self, input: Self::Input) {
        match self.max_value {
            Some(max_value) => {
                if input > max_value {
                    self.max_value = Some(input);
                    self.max_count = 1;
                } else if input == max_value {
                    self.max_count += 1;
                }
            }
            None => {
                self.max_value = Some(input);
                self.max_count = 1;
            }
        }
    }

    fn merge(&self, other: &Self) -> Self {
        match (self.max_value, other.max_value) {
            (Some(self_max), Some(other_max)) => {
                if self_max > other_max {
                    HasDuplicateMax {
                        max_value: self.max_value,
                        max_count: self.max_count,
                    }
                } else if other_max > self_max {
                    HasDuplicateMax {
                        max_value: other.max_value,
                        max_count: other.max_count,
                    }
                } else {
                    HasDuplicateMax {
                        max_value: self.max_value,
                        max_count: self.max_count + other.max_count,
                    }
                }
            }
            (Some(_), None) => self.clone(),
            (None, Some(_)) => other.clone(),
            (None, None) => HasDuplicateMax::new(),
        }
    }

    fn evaluate(&self) -> Self::Output {
        self.max_count >= 2
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_has_duplicate_max() {
        let data = vec![1.0, 5.0, 3.0, 5.0, 2.0];
        let mut has_duplicate_max = HasDuplicateMax::new();

        for value in data {
            has_duplicate_max.update(value);
        }

        let expected_result = true;
        let result = has_duplicate_max.evaluate();

        assert_eq!(result, expected_result);
    }
}
