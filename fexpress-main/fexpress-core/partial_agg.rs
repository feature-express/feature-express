use crate::ast::core::AggregateFunction;
use crate::types::{FLOAT, INT};
use crate::value::{nan_to_none, Value};
use crate::partial_aggregates::minimum::Minimum;

pub trait PartialAggregate {
    type State;
    type Input;
    type Output;

    fn new() -> Self;
    fn update(&mut self, input: Self::Input);
    fn merge(&self, other: &Self) -> Self;
    fn evaluate(&self) -> Self::Output;
}

pub trait SubtractPartialAggregate {
    fn subtract_inplace(&mut self, other: &Self);
    fn subtract(&mut self, other: &Self) -> Self;
}

// TODO: probably unitialized aggregates should be returning Value::None
macro_rules! partial_agg_1 {
    ($name:ident, $input:ty, $output:ty, $state:ty, $init:expr, $statemap1:expr, $op1add:tt, $op1sub:tt, $eval:expr) => {
        #[derive(Debug)]
        pub struct $name { state: $state }

        impl PartialAggregate for $name {
            type State = $state;
            type Input = $input;
            type Output = $output;

            fn new() -> Self {
                Self {
                    state: $init
                }
            }

            fn update(&mut self, input: Self::Input) {
                self.state.0 = self.state.0 $op1add $statemap1(input);
            }

            fn merge(&self, other: &Self) -> Self {
                Self {
                    state: (self.state.0 $op1add other.state.0, )
                }
            }

            fn evaluate(&self) -> Self::Output {
                $eval(self)
            }
        }

        impl SubtractPartialAggregate for $name {
            fn subtract_inplace(&mut self, other: &Self) {
                self.state.0 = self.state.0 $op1sub other.state.0;
            }

            fn subtract(&mut self, other: &Self) -> Self {
                Self {
                    state: (self.state.0 $op1sub other.state.0, )
                }
            }
        }
    }
}

macro_rules! partial_agg_2 {
    ($name:ident, $input:ty, $output:ty, $state:ty, $init:expr, $statemap1:expr, $statemap2:expr, $op1add:tt, $op2add:tt, $op1sub:tt, $op2sub:tt, $eval:expr) => {
        #[derive(Debug)]
        pub struct $name { state: $state }

        impl PartialAggregate for $name {
            type State = $state;
            type Input = $input;
            type Output = $output;

            fn new() -> Self {
                Self {
                    state: $init
                }
            }

            fn update(&mut self, input: Self::Input) {
                self.state.0 = self.state.0 $op1add $statemap1(input);
                self.state.1 = self.state.1 $op2add $statemap2(input);
            }

            fn merge(&self, other: &Self) -> Self {
                Self {
                    state: (self.state.0 $op1add other.state.0, self.state.1 $op2add other.state.1)
                }
            }

            fn evaluate(&self) -> Self::Output {
                $eval(self)
            }
        }

        impl SubtractPartialAggregate for $name {
            fn subtract_inplace(&mut self, other: &Self) {
                self.state.0 = self.state.0 $op1sub other.state.0;
                self.state.1 = self.state.1 $op2sub other.state.1;
            }

            fn subtract(&mut self, other: &Self) -> Self {
                Self {
                    state: (
                        self.state.0 $op1sub other.state.0,
                        self.state.1 $op2sub other.state.1
                    )
                }
            }
        }
    }
}

macro_rules! partial_agg_3 {
    ($name:ident, $input:ty, $output:ty, $state:ty, $init:expr, $statemap1:expr, $statemap2:expr, $statemap3:expr, $op1add:tt, $op2add:tt, $op3add:tt, $op1sub:tt, $op2sub:tt, $op3sub:tt, $eval:expr) => {
        #[derive(Debug)]
        pub struct $name { state: $state }

        impl PartialAggregate for $name {
            type State = $state;
            type Input = $input;
            type Output = $output;

            fn new() -> Self {
                Self {
                    state: $init
                }
            }

            fn update(&mut self, input: Self::Input) {
                self.state.0 = self.state.0 $op1add $statemap1(input);
                self.state.1 = self.state.1 $op2add $statemap2(input);
                self.state.2 = self.state.2 $op3add $statemap3(input);
            }

            fn merge(&self, other: &Self) -> Self {
                Self {
                    state: (
                        self.state.0 $op1add other.state.0,
                        self.state.1 $op2add other.state.1,
                        self.state.2 $op3add other.state.2
                    )
                }
            }

            fn evaluate(&self) -> Self::Output {
                $eval(self)
            }
        }

        impl SubtractPartialAggregate for $name {
            fn subtract_inplace(&mut self, other: &Self) {
                self.state.0 = self.state.0 $op1sub other.state.0;
                self.state.1 = self.state.1 $op2sub other.state.1;
                self.state.2 = self.state.2 $op3sub other.state.2;
            }

            fn subtract(&mut self, other: &Self) -> Self {
                Self {
                    state: (
                        self.state.0 $op1sub other.state.0,
                        self.state.1 $op2sub other.state.1,
                        self.state.2 $op3sub other.state.2
                    )
                }
            }
        }
    }
}

partial_agg_1!(PartialSum, FLOAT, FLOAT, (FLOAT, ), (0.0, ), |x: FLOAT| { x }, +, -, |s: &PartialSum| (s.state.0));
partial_agg_1!(PartialProduct, FLOAT, FLOAT, (FLOAT, ), (1.0, ), |x: FLOAT| { x }, *, /, |s: &PartialProduct| (s.state.0));
partial_agg_1!(PartialCount, FLOAT, usize, (usize, ), (0, ), |_: FLOAT| { 1 }, +, -, |s: &PartialCount| (s.state.0));
partial_agg_2!(PartialAvg, FLOAT, FLOAT, (FLOAT, usize), (0.0, 0), |x: FLOAT| { x }, |_: FLOAT| { 1 }, +, +, -, -, |s: &PartialAvg| (s.state.0 / (s.state.1 as FLOAT)));
partial_agg_3!(PartialVar, FLOAT, FLOAT, (FLOAT, FLOAT, usize), (0.0, 0.0, 0), |x: FLOAT| { x }, |x: FLOAT| { x*x }, |_: FLOAT| 1, +, +, +, -, -, -, |s: &PartialVar| ((s.state.1 / (s.state.2 as FLOAT)) - (s.state.0 / (s.state.2 as FLOAT)).powi(2))*((s.state.2 as FLOAT) / ((s.state.2 as FLOAT)- 1.0)));
partial_agg_3!(PartialStdDev, FLOAT, FLOAT, (FLOAT, FLOAT, usize), (0.0, 0.0, 0), |x: FLOAT| { x }, |x: FLOAT| { x*x }, |_: FLOAT| 1, +, +, +, -, -, -, |s: &PartialStdDev| (((s.state.1 / (s.state.2 as FLOAT)) - (s.state.0 / (s.state.2 as FLOAT)).powi(2))*((s.state.2 as FLOAT) / ((s.state.2 as FLOAT)- 1.0))).sqrt());

#[derive(Debug)]
#[allow(dead_code)]
pub enum PartialAggregateWrapper {
    Sum(PartialSum),
    Product(PartialProduct),
    Count(PartialCount),
    Avg(PartialAvg),
    Var(PartialVar),
    StdDev(PartialStdDev),
    Minimum(Minimum)
}

#[rustfmt::skip]
#[allow(dead_code)]
impl PartialAggregateWrapper {
    pub fn new(agg_func: AggregateFunction) -> Self {
        match agg_func {
            AggregateFunction::Count => PartialAggregateWrapper::Count(PartialCount::new()),
            AggregateFunction::Sum => PartialAggregateWrapper::Sum(PartialSum::new()),
            AggregateFunction::Min => PartialAggregateWrapper::Minimum(Minimum::new()),
            AggregateFunction::Max => unimplemented!(),
            AggregateFunction::Avg => PartialAggregateWrapper::Avg(PartialAvg::new()),
            AggregateFunction::Median => unimplemented!(),
            AggregateFunction::Var => PartialAggregateWrapper::Var(PartialVar::new()),
            AggregateFunction::StDev => PartialAggregateWrapper::StdDev(PartialStdDev::new()),
            AggregateFunction::Last => unimplemented!(),
            AggregateFunction::Nth(_) => unimplemented!(),
            AggregateFunction::First => unimplemented!(),
            AggregateFunction::TimeOfLast => unimplemented!(),
            AggregateFunction::TimeOfFirst => unimplemented!(),
            AggregateFunction::TimeOfNext => unimplemented!(),
            AggregateFunction::AvgDaysBetween => unimplemented!(),
            AggregateFunction::Values => unimplemented!()
        }
    }

    pub fn update(&mut self, value: Value) {
        match self {
            PartialAggregateWrapper::Sum(s) => s.update(value.into()),
            PartialAggregateWrapper::Product(s) => s.update(value.into()),
            PartialAggregateWrapper::Count(s) => s.update(value.into()),
            PartialAggregateWrapper::Avg(s) => s.update(value.into()),
            PartialAggregateWrapper::Var(s) => s.update(value.into()),
            PartialAggregateWrapper::StdDev(s) => s.update(value.into()),
            PartialAggregateWrapper::Minimum(s) => s.update(value.into()),
        }
    }

    pub fn merge(&mut self, other: &Self) -> Self {
        match (self, other) {
            (PartialAggregateWrapper::Sum(a), PartialAggregateWrapper::Sum(b)) => PartialAggregateWrapper::Sum(a.merge(b)),
            (PartialAggregateWrapper::Product(a), PartialAggregateWrapper::Product(b)) => PartialAggregateWrapper::Product(a.merge(b)),
            (PartialAggregateWrapper::Count(a), PartialAggregateWrapper::Count(b)) => PartialAggregateWrapper::Count(a.merge(b)),
            (PartialAggregateWrapper::Avg(a), PartialAggregateWrapper::Avg(b)) => PartialAggregateWrapper::Avg(a.merge(b)),
            (PartialAggregateWrapper::Var(a), PartialAggregateWrapper::Var(b)) => PartialAggregateWrapper::Var(a.merge(b)),
            (PartialAggregateWrapper::StdDev(a), PartialAggregateWrapper::StdDev(b)) => PartialAggregateWrapper::StdDev(a.merge(b)),
            _ => panic!("Cannot merge Partial aggregates of different types")
        }
    }

    pub fn evaluate(&mut self) -> Value {
        let val = match self {
            PartialAggregateWrapper::Sum(s) => Value::Num(s.evaluate()),
            PartialAggregateWrapper::Product(s) => Value::Num(s.evaluate()),
            PartialAggregateWrapper::Count(s) => Value::Int(s.evaluate() as INT),
            PartialAggregateWrapper::Avg(s) => Value::Num(s.evaluate()),
            PartialAggregateWrapper::Var(s) => if s.state.2 < 2 { Value::Num(0.0) } else { Value::Num(s.evaluate()) },
            PartialAggregateWrapper::StdDev(s) => if s.state.2 < 2 { Value::Num(0.0) } else { Value::Num(s.evaluate()) },
            PartialAggregateWrapper::Minimum(s) => s.evaluate().map_or(Value::None, Value::Num),
        };
        nan_to_none(val)
    }

    pub fn subtract(&mut self, other: &Self) -> Self {
        match (self, other) {
            (PartialAggregateWrapper::Sum(a), PartialAggregateWrapper::Sum(b)) => PartialAggregateWrapper::Sum(a.subtract(b)),
            (PartialAggregateWrapper::Product(a), PartialAggregateWrapper::Product(b)) => PartialAggregateWrapper::Product(a.subtract(b)),
            (PartialAggregateWrapper::Count(a), PartialAggregateWrapper::Count(b)) => PartialAggregateWrapper::Count(a.subtract(b)),
            (PartialAggregateWrapper::Avg(a), PartialAggregateWrapper::Avg(b)) => PartialAggregateWrapper::Avg(a.subtract(b)),
            (PartialAggregateWrapper::Var(a), PartialAggregateWrapper::Var(b)) => PartialAggregateWrapper::Var(a.subtract(b)),
            (PartialAggregateWrapper::StdDev(a), PartialAggregateWrapper::StdDev(b)) => PartialAggregateWrapper::StdDev(a.subtract(b)),
            (PartialAggregateWrapper::Minimum(a), PartialAggregateWrapper::Minimum(b)) => PartialAggregateWrapper::Minimum(a.subtract(b)),
            _ => panic!("Cannot merge Partial aggregates of different types")
        }
    }

    pub fn subtract_inplace(&mut self, other: &Self) {
        match (self, other) {
            (PartialAggregateWrapper::Sum(a), PartialAggregateWrapper::Sum(b)) => a.subtract_inplace(b),
            (PartialAggregateWrapper::Product(a), PartialAggregateWrapper::Product(b)) => a.subtract_inplace(b),
            (PartialAggregateWrapper::Count(a), PartialAggregateWrapper::Count(b)) => a.subtract_inplace(b),
            (PartialAggregateWrapper::Avg(a), PartialAggregateWrapper::Avg(b)) => a.subtract_inplace(b),
            (PartialAggregateWrapper::Var(a), PartialAggregateWrapper::Var(b)) => a.subtract_inplace(b),
            (PartialAggregateWrapper::StdDev(a), PartialAggregateWrapper::StdDev(b)) => a.subtract_inplace(b),
            (PartialAggregateWrapper::Minimum(a), PartialAggregateWrapper::Minimum(b)) => a.subtract_inplace(b),
            _ => ()
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::partial_agg::PartialSum;
    use crate::stats::Stats;

    use super::*;

    #[test]
    fn test_count() {
        let v1 = vec![1.0, 2.0];
        let v2 = vec![4.0, 5.0, 6.0];
        let mut p1 = PartialCount::new();
        for e in v1 {
            p1.update(e)
        }
        assert_eq!(p1.evaluate(), 2);

        let mut p2 = PartialCount::new();
        for e in v2 {
            p2.update(e)
        }
        assert_eq!(p2.evaluate(), 3);

        let mut p3 = p1.merge(&p2);
        assert_eq!(p3.evaluate(), 5);

        p3.subtract_inplace(&p2);
        assert_eq!(p3.evaluate(), 2);
    }

    #[test]
    fn test_partial_sum() {
        let v1 = vec![1.0, 2.0, 3.0];
        let v2 = vec![4.0, 5.0, 6.0];
        let mut p1 = PartialSum::new();
        for e in v1 {
            p1.update(e)
        }
        assert!((p1.evaluate() - 6.0) < 1e-04);

        let mut p2 = PartialSum::new();
        for e in v2 {
            p2.update(e)
        }
        assert!((p2.evaluate() - 15.0) < 1e-04);

        let mut p3 = p1.merge(&p2);
        assert!((p3.evaluate() - 21.0) < 1e-04);

        p3.subtract_inplace(&p2);
        assert!((p3.evaluate() - 6.0) < 1e-04);
    }

    #[test]
    fn test_partial_avg() {
        let v1 = vec![1.0, 2.0, 3.0];
        let v2 = vec![4.0, 5.0, 6.0];
        let mut p1 = PartialAvg::new();
        for e in v1 {
            p1.update(e)
        }
        assert!((p1.evaluate() - 2.0) < 1e-04);

        let mut p2 = PartialAvg::new();
        for e in v2 {
            p2.update(e)
        }
        assert!((p2.evaluate() - 5.0) < 1e-04);

        let mut p3 = p1.merge(&p2);
        assert!((p3.evaluate() - 3.5) < 1e-04);

        p3.subtract_inplace(&p2);
        assert!((p3.evaluate() - 2.0) < 1e-04);
    }

    #[test]
    fn test_partial_stddev() {
        let v1 = vec![1.0, 20.0, 50.0];
        let v2 = vec![20.0, 60.0, 70.0];
        let v3 = vec![1.0, 20.0, 50.0, 20.0, 60.0, 70.0];
        let mut p1 = PartialStdDev::new();
        for e in v1.clone() {
            p1.update(e)
        }
        assert!((p1.evaluate() - v1.std_dev()).abs() < 1e-4);

        let mut p2 = PartialStdDev::new();
        for e in v2.clone() {
            p2.update(e)
        }
        assert!((p2.evaluate() - v2.std_dev()).abs() < 1e-4);

        let p3 = p1.merge(&p2);
        assert!((p3.evaluate() - v3.std_dev()) < 1e-4);
    }

    #[test]
    fn test_partial_var() {
        let v1 = vec![1.0, 20.0, 50.0];
        let v2 = vec![20.0, 60.0, 70.0];
        let v3 = vec![1.0, 20.0, 50.0, 20.0, 60.0, 70.0];
        let mut p1 = PartialVar::new();
        for e in v1.clone() {
            p1.update(e)
        }
        assert!((p1.evaluate() - v1.var()).abs() < 1e-3);

        let mut p2 = PartialVar::new();
        for e in v2.clone() {
            p2.update(e)
        }
        assert!((p2.evaluate() - v2.var()).abs() < 1e-3);

        let p3 = p1.merge(&p2);
        assert!((p3.evaluate() - v3.var()) < 1e-3);
    }
}
