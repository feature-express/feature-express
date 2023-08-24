use crate::partial_agg::{PartialAggregate, SubtractPartialAggregate};
use crate::types::FLOAT;

#[derive(Clone, Debug)]
pub struct Product {
    count: usize,
    product: FLOAT,
}

impl PartialAggregate for Product {
    type State = (usize, FLOAT);
    type Input = FLOAT;
    type Output = Option<FLOAT>;

    fn new() -> Self {
        Product {
            count: 0,
            product: 1.0, // Initialize to 1 for multiplication.
        }
    }

    fn update(&mut self, input: Self::Input) {
        self.count += 1;
        self.product *= input;
    }

    fn merge(&self, other: &Self) -> Self {
        Product {
            count: self.count + other.count,
            product: self.product * other.product,
        }
    }

    fn merge_inplace(&mut self, other: &Self) {
        self.count += other.count;
        self.product *= other.product;
    }

    fn evaluate(&self) -> Self::Output {
        if self.count == 0 {
            None
        } else {
            Some(self.product)
        }
    }
}

impl SubtractPartialAggregate for Product {
    fn subtract_inplace(&mut self, other: &Self) {
        self.count -= other.count;
        if other.product != 0.0 {
            self.product /= other.product;
        } else {
            // Handle the case when other.product is 0
            self.product = 0.0;
        }
    }

    fn subtract(&mut self, other: &Self) -> Self {
        let mut result = self.clone();
        result.subtract_inplace(other);
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_product() {
        let data = vec![1.0, 2.0, 3.0, 4.0, 5.0];
        let mut product = Product::new();

        for value in data {
            product.update(value);
        }

        let expected_result = Some(120.0);
        let result = product.evaluate();

        assert_eq!(result, expected_result);
    }

    #[test]
    fn test_product_merge() {
        let data1 = vec![1.0, 2.0, 3.0];
        let data2 = vec![4.0, 5.0];
        let mut product1 = Product::new();
        let mut product2 = Product::new();

        for value in data1 {
            product1.update(value);
        }
        for value in data2 {
            product2.update(value);
        }

        let product_merged = product1.merge(&product2);
        let expected_result = Some(120.0);
        let result = product_merged.evaluate();

        assert_eq!(result, expected_result);
    }

    #[test]
    fn test_product_empty() {
        let product = Product::new();

        let expected_result: Option<FLOAT> = None;
        let result = product.evaluate();

        assert_eq!(result, expected_result);
    }
}
