extern crate chrono;

use rand::prelude::*;

// Approximate Median implementation using reservoir sampling
pub struct ApproxMedian {
    reservoir: Vec<f64>,
    num_samples: usize,
    rng: ThreadRng,
}

const RESERVOIR_SIZE: usize = 1000;

impl PartialAggregate for ApproxMedian {
    type State = (Vec<f64>, usize);
    type Input = f64;
    type Output = Option<f64>;

    fn new() -> Self {
        ApproxMedian {
            reservoir: Vec::with_capacity(RESERVOIR_SIZE),
            num_samples: 0,
            rng: thread_rng(),
        }
    }

    fn update(&mut self, input: Self::Input) {
        let (_, value) = input;
        self.num_samples += 1;

        if self.reservoir.len() < RESERVOIR_SIZE {
            self.reservoir.push(value);
        } else {
            let random_index = self.rng.gen_range(0..self.num_samples);
            if random_index < RESERVOIR_SIZE {
                self.reservoir[random_index] = value;
            }
        }
    }

    fn merge(&self, other: &Self) -> Self {
        let mut merged_reservoir = self.reservoir.clone();
        for value in &other.reservoir {
            merged_reservoir.push(*value);
        }

        ApproxMedian {
            reservoir: merged_reservoir,
            num_samples: self.num_samples + other.num_samples,
            rng: thread_rng(),
        }
    }

    fn evaluate(&self) -> Self::Output {
        if self.reservoir.is_empty() {
            return None;
        }

        let mut sorted_reservoir = self.reservoir.clone();
        sorted_reservoir.sort_unstable_by(|a, b| a.partial_cmp(b).unwrap_or(Ordering::Equal));

        let approximate_median = sorted_reservoir[RESERVOIR_SIZE / 2];
        Some(approximate_median)
    }
}