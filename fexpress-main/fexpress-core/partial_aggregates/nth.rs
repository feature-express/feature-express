extern crate chrono;

use std::collections::VecDeque;
use crate::types::FLOAT;

use chrono::NaiveDateTime;

// Nth implementation with a more efficient state
pub struct Nth {
    k_oldest_tuples: VecDeque<(NaiveDateTime, FLOAT)>,
    k: usize,
}

impl PartialAggregate for Nth {
    type State = VecDeque<(NaiveDateTime, FLOAT)>;
    type Input = (NaiveDateTime, FLOAT);
    type Output = Option<FLOAT>;

    fn new() -> Self {
        Nth {
            k_oldest_tuples: VecDeque::new(),
            k: 0,
        }
    }

    fn with_k(k: usize) -> Self {
        Nth {
            k_oldest_tuples: VecDeque::with_capacity(k + 1),
            k,
        }
    }

    fn update(&mut self, input: Self::Input) {
        if let Some(back) = self.k_oldest_tuples.back() {
            if back.0 > input.0 {
                return;
            }
        }

        self.k_oldest_tuples.push_back(input);
        if self.k_oldest_tuples.len() > self.k + 1 {
            self.k_oldest_tuples.pop_front();
        }
    }

    fn merge(&self, other: &Self) -> Self {
        let mut merged_tuples = self.k_oldest_tuples.clone();

        for other_tuple in &other.k_oldest_tuples {
            if let Some(back) = merged_tuples.back() {
                if back.0 > other_tuple.0 {
                    continue;
                }
            }

            merged_tuples.push_back(*other_tuple);
            if merged_tuples.len() > self.k + 1 {
                merged_tuples.pop_front();
            }
        }

        Nth {
            k_oldest_tuples: merged_tuples,
            k: self.k,
        }
    }

    fn evaluate(&self) -> Self::Output {
        if self.k_oldest_tuples.len() <= self.k {
            None
        } else {
            Some(self.k_oldest_tuples[self.k].1)
        }
    }
}
