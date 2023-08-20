use crate::partial_agg::{PartialAggregate, SubtractPartialAggregate};
use std::cmp::Ordering;
use std::collections::HashMap;

#[derive(Clone, Debug)]
pub struct Mode<T>
where
    T: Eq + std::hash::Hash + Clone,
{
    values: HashMap<T, usize>,
    mode: Option<T>,
    max_count: usize,
}

impl<T> PartialAggregate for Mode<T>
where
    T: Eq + std::hash::Hash + Clone,
{
    type State = HashMap<T, usize>;
    type Input = T;
    type Output = Option<T>;

    fn new() -> Self {
        Mode {
            values: HashMap::new(),
            mode: None,
            max_count: 0,
        }
    }

    fn update(&mut self, input: Self::Input) {
        let count = self.values.entry(input.clone()).or_insert(0);
        *count += 1;

        // Update mode if needed
        if *count > self.max_count {
            self.max_count = *count;
            self.mode = Some(input);
        }
    }

    fn merge(&self, other: &Self) -> Self {
        let mut merged = self.clone();
        for (value, count) in &other.values {
            let merged_count = merged.values.entry(value.clone()).or_insert(0);
            *merged_count += count;

            if *merged_count > merged.max_count {
                merged.max_count = *merged_count;
                merged.mode = Some(value.clone());
            }
        }

        merged
    }

    fn evaluate(&self) -> Self::Output {
        self.mode.clone()
    }
}

impl<T> SubtractPartialAggregate for Mode<T>
where
    T: Eq + std::hash::Hash + Clone,
{
    fn subtract_inplace(&mut self, other: &Self) {
        for (value, count) in &other.values {
            if let Some(existing_count) = self.values.get_mut(value) {
                if *existing_count <= *count {
                    self.values.remove(value);
                } else {
                    *existing_count -= count;
                }
            }
        }

        // Recalculate mode
        let mut max_val = 0;
        let mut mode = None;
        for (val, &count) in &self.values {
            if count > max_val {
                max_val = count;
                mode = Some(val.clone());
            }
        }
        self.mode = mode;
        self.max_count = max_val;
    }

    fn subtract(&mut self, other: &Self) -> Self {
        let mut result = self.clone();
        result.subtract_inplace(other);
        result
    }
}
