use crate::partial_agg::PartialAggregate;
use crate::types::FLOAT;

#[derive(Clone)]
pub struct HasDuplicateMin {
    min_value: Option<FLOAT>,
    min_count: usize,
}

impl PartialAggregate for HasDuplicateMin {
    type State = (Option<FLOAT>, usize);
    type Input = FLOAT;
    type Output = bool;

    fn new() -> Self {
        HasDuplicateMin {
            min_value: None,
            min_count: 0,
        }
    }

    fn update(&mut self, input: Self::Input) {
        match self.min_value {
            Some(min_value) => {
                if input < min_value {
                    self.min_value = Some(input);
                    self.min_count = 1;
                } else if input == min_value {
                    self.min_count += 1;
                }
            }
            None => {
                self.min_value = Some(input);
                self.min_count = 1;
            }
        }
    }

    fn merge(&self, other: &Self) -> Self {
        match (self.min_value, other.min_value) {
            (Some(self_min), Some(other_min)) => {
                if self_min < other_min {
                    HasDuplicateMin {
                        min_value: self.min_value,
                        min_count: self.min_count,
                    }
                } else if other_min < self_min {
                    HasDuplicateMin {
                        min_value: other.min_value,
                        min_count: other.min_count,
                    }
                } else {
                    HasDuplicateMin {
                        min_value: self.min_value,
                        min_count: self.min_count + other.min_count,
                    }
                }
            }
            (Some(_), None) => self.clone(),
            (None, Some(_)) => other.clone(),
            (None, None) => HasDuplicateMin::new(),
        }
    }

    fn merge_inplace(&mut self, other: &Self) {
        match (self.min_value, other.min_value) {
            (Some(self_min), Some(other_min)) => {
                if self_min > other_min {
                    self.min_value = other.min_value;
                    self.min_count = other.min_count;
                } else if other_min == self_min {
                    self.min_count += other.min_count;
                }
            }
            (None, Some(_)) => {
                self.min_value = other.min_value;
                self.min_count = other.min_count;
            }
            _ => {}
        }
    }

    fn evaluate(&self) -> Self::Output {
        self.min_count >= 2
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_has_duplicate_min() {
        let data = vec![1.0, 5.0, 3.0, 1.0, 2.0];
        let mut has_duplicate_min = HasDuplicateMin::new();

        for value in data {
            has_duplicate_min.update(value);
        }

        let expected_result = true;
        let result = has_duplicate_min.evaluate();

        assert_eq!(result, expected_result);
    }
}
