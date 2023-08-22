use crate::ast::core::AggregateFunction;
use crate::partial_aggregates::all::All;
use crate::partial_aggregates::any::Any;
use crate::partial_aggregates::argmax::ArgMax;
use crate::partial_aggregates::argmin::ArgMin;
use crate::partial_aggregates::count::Count;
use crate::partial_aggregates::first::First;
use crate::partial_aggregates::last::Last;
use crate::partial_aggregates::max_consecutive_true::MaxConsecutiveTrue;
use crate::partial_aggregates::maximum::Maximum;
use crate::partial_aggregates::mean::Mean;
use crate::partial_aggregates::minimum::Minimum;
use crate::partial_aggregates::mode::Mode;
use crate::partial_aggregates::product::Product;
use crate::partial_aggregates::standard_deviation::StandardDeviation;
use crate::partial_aggregates::sum::Sum;
use crate::partial_aggregates::variance::Variance;
use crate::types::{FLOAT, INT};
use crate::value::{nan_to_none, Value};
use chrono::NaiveDateTime;
use std::convert::identity;

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

#[derive(Debug)]
#[allow(dead_code)]
pub enum PartialAggregateWrapper {
    Sum(Sum),
    Product(Product),
    Count(Count),
    Avg(Mean),
    Var(Variance),
    StdDev(StandardDeviation),
    Minimum(Minimum),
    Maximum(Maximum),
    First(First<NaiveDateTime, Value>),
    Last(Last<NaiveDateTime, Value>),
    ArgMax(ArgMax<NaiveDateTime>),
    ArgMin(ArgMin<NaiveDateTime>),
    Mode(Mode<Value>),
    Any(Any),
    All(All),
    MaxConsecutiveTrue(MaxConsecutiveTrue),
}

impl Into<AggregateFunction> for PartialAggregateWrapper {
    fn into(self) -> AggregateFunction {
        match self {
            PartialAggregateWrapper::Sum(_) => AggregateFunction::Sum,
            PartialAggregateWrapper::Product(_) => AggregateFunction::Product,
            PartialAggregateWrapper::Count(_) => AggregateFunction::Count,
            PartialAggregateWrapper::Avg(_) => AggregateFunction::Avg,
            PartialAggregateWrapper::Var(_) => AggregateFunction::Var,
            PartialAggregateWrapper::StdDev(_) => AggregateFunction::Stdev,
            PartialAggregateWrapper::Minimum(_) => AggregateFunction::Min,
            PartialAggregateWrapper::Maximum(_) => AggregateFunction::Max,
            PartialAggregateWrapper::First(_) => AggregateFunction::First,
            PartialAggregateWrapper::Last(_) => AggregateFunction::Last,
            PartialAggregateWrapper::ArgMax(_) => AggregateFunction::Argmax,
            PartialAggregateWrapper::ArgMin(_) => AggregateFunction::Argmin,
            PartialAggregateWrapper::Mode(_) => AggregateFunction::Mode,
            PartialAggregateWrapper::Any(_) => AggregateFunction::Any,
            PartialAggregateWrapper::All(_) => AggregateFunction::All,
            PartialAggregateWrapper::MaxConsecutiveTrue(_) => AggregateFunction::MaxConsecutiveTrue
        }
    }
}

// implements match for pairs of variants of the same type and applies method to them
macro_rules! gen_match_arms {
    ($enum_name:ident, $variant:ident, $method:ident) => {
        ($enum_name::$variant(a), $enum_name::$variant(b)) => $enum_name::$variant(a.$method(&b)),
        ($enum_name::$variant(_), _) => panic!("Mismatched or unhandled variants in the left operand"),
        (_, $enum_name::$variant(_)) => panic!("Mismatched or unhandled variants in the right operand"),
    };
}

#[rustfmt::skip]
#[allow(dead_code)]
impl PartialAggregateWrapper {
    pub fn new(agg_func: AggregateFunction) -> Self {
        match agg_func {
            AggregateFunction::Count => PartialAggregateWrapper::Count(Count::new()),
            AggregateFunction::Sum => PartialAggregateWrapper::Sum(Sum::new()),
            AggregateFunction::Product => PartialAggregateWrapper::Product(Product::new()),
            AggregateFunction::Min => PartialAggregateWrapper::Minimum(Minimum::new()),
            AggregateFunction::Max => PartialAggregateWrapper::Maximum(Maximum::new()),
            AggregateFunction::Avg => PartialAggregateWrapper::Avg(Mean::new()),
            AggregateFunction::Median => unimplemented!(),
            AggregateFunction::Var => PartialAggregateWrapper::Var(Variance::new()),
            AggregateFunction::Stdev => PartialAggregateWrapper::StdDev(StandardDeviation::new()),
            AggregateFunction::Last => PartialAggregateWrapper::Last(Last::new()),
            AggregateFunction::Nth(_) => unimplemented!(),
            AggregateFunction::First => PartialAggregateWrapper::First(First::new()),
            AggregateFunction::TimeOfLast => unimplemented!(),
            AggregateFunction::TimeOfFirst => unimplemented!(),
            AggregateFunction::TimeOfNext => unimplemented!(),
            AggregateFunction::AvgDaysBetween => unimplemented!(),
            AggregateFunction::Values => unimplemented!(),
            AggregateFunction::Argmax => PartialAggregateWrapper::ArgMax(ArgMax::new()),
            AggregateFunction::Argmin => PartialAggregateWrapper::ArgMin(ArgMin::new()),
            AggregateFunction::Mode => PartialAggregateWrapper::Mode(Mode::new()),
            AggregateFunction::Any => PartialAggregateWrapper::Any(Any::new()),
            AggregateFunction::All => PartialAggregateWrapper::All(All::new()),
            AggregateFunction::MaxConsecutiveTrue => PartialAggregateWrapper::MaxConsecutiveTrue(MaxConsecutiveTrue::new()),
        }
    }

    pub fn update(&mut self, value: Value, ts: NaiveDateTime) {
        match self {
            PartialAggregateWrapper::Sum(s) => s.update(value.into()),
            PartialAggregateWrapper::Product(s) => s.update(value.into()),
            PartialAggregateWrapper::Count(s) => s.update(value.into()),
            PartialAggregateWrapper::Avg(s) => s.update(value.into()),
            PartialAggregateWrapper::Var(s) => s.update(value.into()),
            PartialAggregateWrapper::StdDev(s) => s.update(value.into()),
            PartialAggregateWrapper::Minimum(s) => s.update(value.into()),
            PartialAggregateWrapper::Maximum(s) => s.update(value.into()),
            PartialAggregateWrapper::First(s) => s.update((ts, value.into())),
            PartialAggregateWrapper::Last(s) => s.update((ts, value.into())),
            PartialAggregateWrapper::ArgMax(s) => s.update((ts, value.into())),
            PartialAggregateWrapper::ArgMin(s) => s.update((ts, value.into())),
            PartialAggregateWrapper::Mode(s) => s.update(value.into()),
            PartialAggregateWrapper::Any(s) => s.update(value.into()),
            PartialAggregateWrapper::All(s) => s.update(value.into()),
            PartialAggregateWrapper::MaxConsecutiveTrue(s) => s.update((value.into(), ts))
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
            (PartialAggregateWrapper::Minimum(a), PartialAggregateWrapper::Minimum(b)) => PartialAggregateWrapper::Minimum(a.merge(b)),
            (PartialAggregateWrapper::Maximum(a), PartialAggregateWrapper::Maximum(b)) => PartialAggregateWrapper::Maximum(a.merge(b)),
            (PartialAggregateWrapper::First(a), PartialAggregateWrapper::First(b)) => PartialAggregateWrapper::First(a.merge(b)),
            (PartialAggregateWrapper::Last(a), PartialAggregateWrapper::Last(b)) => PartialAggregateWrapper::Last(a.merge(b)),
            (PartialAggregateWrapper::ArgMin(a), PartialAggregateWrapper::ArgMin(b)) => PartialAggregateWrapper::ArgMin(a.merge(b)),
            (PartialAggregateWrapper::ArgMax(a), PartialAggregateWrapper::ArgMax(b)) => PartialAggregateWrapper::ArgMax(a.merge(b)),
            (PartialAggregateWrapper::Mode(a), PartialAggregateWrapper::Mode(b)) => PartialAggregateWrapper::Mode(a.merge(b)),
            (PartialAggregateWrapper::Any(a), PartialAggregateWrapper::Any(b)) => PartialAggregateWrapper::Any(a.merge(b)),
            (PartialAggregateWrapper::All(a), PartialAggregateWrapper::All(b)) => PartialAggregateWrapper::All(a.merge(b)),
            (PartialAggregateWrapper::MaxConsecutiveTrue(a), PartialAggregateWrapper::MaxConsecutiveTrue(b)) => PartialAggregateWrapper::MaxConsecutiveTrue(a.merge(b)),
            _ => panic!("Cannot merge Partial aggregates of different types")
        }
    }

    pub fn evaluate(&mut self) -> Value {
        let val = match self {
            PartialAggregateWrapper::Sum(s) => s.evaluate().map_or(Value::None, Value::Num),
            PartialAggregateWrapper::Product(s) => s.evaluate().map_or(Value::None, Value::Num),
            PartialAggregateWrapper::Count(s) => Value::Int(s.evaluate() as INT),
            PartialAggregateWrapper::Avg(s) => s.evaluate().map_or(Value::None, Value::Num),
            PartialAggregateWrapper::Var(s) => s.evaluate().map_or(Value::None, Value::Num),
            PartialAggregateWrapper::StdDev(s) => s.evaluate().map_or(Value::None, Value::Num),
            PartialAggregateWrapper::Minimum(s) => s.evaluate().map_or(Value::None, Value::Num),
            PartialAggregateWrapper::Maximum(s) => s.evaluate().map_or(Value::None, Value::Num),
            PartialAggregateWrapper::First(s) => s.evaluate().map_or(Value::None, identity),
            PartialAggregateWrapper::Last(s) => s.evaluate().map_or(Value::None, identity),
            PartialAggregateWrapper::ArgMax(s) => s.evaluate().map_or(Value::None, Value::DateTime),
            PartialAggregateWrapper::ArgMin(s) => s.evaluate().map_or(Value::None, Value::DateTime),
            PartialAggregateWrapper::Mode(s) => s.evaluate().map_or(Value::None, identity),
            PartialAggregateWrapper::Any(s) => Value::Bool(s.evaluate()),
            PartialAggregateWrapper::All(s) => Value::Bool(s.evaluate()),
            PartialAggregateWrapper::MaxConsecutiveTrue(s) => Value::Int(s.evaluate() as INT),
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
            (PartialAggregateWrapper::Maximum(a), PartialAggregateWrapper::Maximum(b)) => PartialAggregateWrapper::Maximum(a.subtract(b)),
            (PartialAggregateWrapper::First(a), PartialAggregateWrapper::First(b)) => PartialAggregateWrapper::First(a.subtract(b)),
            (PartialAggregateWrapper::Last(a), PartialAggregateWrapper::Last(b)) => PartialAggregateWrapper::Last(a.subtract(b)),
            (PartialAggregateWrapper::ArgMin(a), PartialAggregateWrapper::ArgMin(b)) => PartialAggregateWrapper::ArgMin(a.subtract(b)),
            (PartialAggregateWrapper::ArgMax(a), PartialAggregateWrapper::ArgMax(b)) => PartialAggregateWrapper::ArgMax(a.subtract(b)),
            (PartialAggregateWrapper::Mode(a), PartialAggregateWrapper::Mode(b)) => PartialAggregateWrapper::Mode(a.subtract(b)),
            (PartialAggregateWrapper::Any(a), PartialAggregateWrapper::Any(b)) => PartialAggregateWrapper::Any(a.subtract(b)),
            (PartialAggregateWrapper::All(a), PartialAggregateWrapper::All(b)) => PartialAggregateWrapper::All(a.subtract(b)),
            (PartialAggregateWrapper::MaxConsecutiveTrue(a), PartialAggregateWrapper::MaxConsecutiveTrue(b)) => PartialAggregateWrapper::MaxConsecutiveTrue(a.subtract(b)),
            _ => panic!("Cannot subtract Partial aggregates of different types")
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
            (PartialAggregateWrapper::Maximum(a), PartialAggregateWrapper::Maximum(b)) => a.subtract_inplace(b),
            (PartialAggregateWrapper::First(a), PartialAggregateWrapper::First(b)) => a.subtract_inplace(b),
            (PartialAggregateWrapper::Last(a), PartialAggregateWrapper::Last(b)) => a.subtract_inplace(b),
            (PartialAggregateWrapper::ArgMin(a), PartialAggregateWrapper::ArgMin(b)) => a.subtract_inplace(b),
            (PartialAggregateWrapper::ArgMax(a), PartialAggregateWrapper::ArgMax(b)) => a.subtract_inplace(b),
            (PartialAggregateWrapper::Mode(a), PartialAggregateWrapper::Mode(b)) => a.subtract_inplace(b),
            (PartialAggregateWrapper::Any(a), PartialAggregateWrapper::Any(b)) => a.subtract_inplace(b),
            (PartialAggregateWrapper::All(a), PartialAggregateWrapper::All(b)) => a.subtract_inplace(b),
            (PartialAggregateWrapper::MaxConsecutiveTrue(a), PartialAggregateWrapper::MaxConsecutiveTrue(b)) => a.subtract_inplace(b),
            _ => panic!("Cannot subtract_inplace Partial aggregates of different types")
        }
    }
}
