use crate::event_store::column_event_store::encoding::{NonNullableDecoding, NullableDecoding};
use crate::event_store::column_event_store::raw_column::{RawColumnVec, RawColumnVecGen};
use crate::types::FLOAT;
use crate::types::INT;
use chrono::{NaiveDate, NaiveDateTime};
use enum_dispatch::enum_dispatch;
use ordered_float::OrderedFloat;
use std::collections::HashMap;
use std::fmt::Debug;
use std::hash::Hash;
use std::mem;

#[derive(Debug, Clone, PartialEq)]
pub struct RunLengthEncodedVecGen<T> {
    pub values: Vec<T>,
    pub lengths: Vec<usize>,
}

impl<T: PartialEq> RunLengthEncodedVecGen<T> {
    pub(crate) fn encode(mut vec: Vec<T>) -> Self {
        if vec.is_empty() {
            return Self {
                values: Vec::new(),
                lengths: Vec::new(),
            };
        }

        let mut values = Vec::new();
        let mut lengths = Vec::new();
        let mut prev = vec.remove(0);
        let mut count = 1;

        for elem in vec {
            if elem == prev {
                count += 1;
            } else {
                values.push(prev);
                lengths.push(count);
                prev = elem;
                count = 1;
            }
        }
        values.push(prev);
        lengths.push(count);

        Self { values, lengths }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct RunLengthEncodedVecOptionGen<T: PartialEq> {
    pub values: Vec<Option<T>>,
    pub lengths: Vec<usize>,
}

impl<T: Clone + PartialEq + Debug> NonNullableDecoding<T> for RunLengthEncodedVecGen<T> {
    fn decode(self) -> Vec<T> {
        let mut vec = Vec::new();
        for (value, length) in self.values.into_iter().zip(self.lengths.into_iter()) {
            vec.extend(std::iter::repeat(value).take(length));
        }
        vec
    }

    fn size(&self) -> usize {
        let values_size = self.values.len() * mem::size_of::<T>();
        let lengths_size = self.lengths.len() * mem::size_of::<usize>();
        values_size + lengths_size
    }
}

#[derive(Debug, Clone, PartialEq)]
#[enum_dispatch(NonNullableDecoding)]
pub enum RunLengthEncodedVec {
    Bool(RunLengthEncodedVecGen<bool>),
    Int(RunLengthEncodedVecGen<INT>),
    Str(RunLengthEncodedVecGen<String>),
    VecBool(RunLengthEncodedVecGen<Vec<bool>>),
    VecInt(RunLengthEncodedVecGen<Vec<INT>>),
    VecStr(RunLengthEncodedVecGen<Vec<String>>),
    Date(RunLengthEncodedVecGen<NaiveDate>),
    DateTime(RunLengthEncodedVecGen<NaiveDateTime>),
}

macro_rules! implement_from_trait {
    ($variant:ident, $type:ty) => {
        impl From<RunLengthEncodedVecGen<$type>> for RunLengthEncodedVec {
            fn from(item: RunLengthEncodedVecGen<$type>) -> Self {
                RunLengthEncodedVec::$variant(item)
            }
        }
    };
}

implement_from_trait!(Bool, bool);
implement_from_trait!(Int, INT);
implement_from_trait!(Str, String);
implement_from_trait!(VecBool, Vec<bool>);
implement_from_trait!(VecInt, Vec<INT>);
implement_from_trait!(VecStr, Vec<String>);
implement_from_trait!(Date, NaiveDate);
implement_from_trait!(DateTime, NaiveDateTime);

impl RunLengthEncodedVec {
    pub fn decode(&self) -> RawColumnVec {
        match self {
            RunLengthEncodedVec::Bool(v) => {
                RawColumnVec::Bool(RawColumnVecGen::NonNullable(v.clone().decode()))
            }
            RunLengthEncodedVec::Int(v) => {
                RawColumnVec::Int(RawColumnVecGen::NonNullable(v.clone().decode()))
            }
            RunLengthEncodedVec::Str(v) => {
                RawColumnVec::Str(RawColumnVecGen::NonNullable(v.clone().decode()))
            }
            RunLengthEncodedVec::VecBool(v) => {
                RawColumnVec::VecBool(RawColumnVecGen::NonNullable(v.clone().decode()))
            }
            RunLengthEncodedVec::VecInt(v) => {
                RawColumnVec::VecInt(RawColumnVecGen::NonNullable(v.clone().decode()))
            }
            RunLengthEncodedVec::VecStr(v) => {
                RawColumnVec::VecStr(RawColumnVecGen::NonNullable(v.clone().decode()))
            }
            RunLengthEncodedVec::Date(v) => {
                RawColumnVec::Date(RawColumnVecGen::NonNullable(v.clone().decode()))
            }
            RunLengthEncodedVec::DateTime(v) => {
                RawColumnVec::DateTime(RawColumnVecGen::NonNullable(v.clone().decode()))
            }
        }
    }
}

impl<T: PartialEq> RunLengthEncodedVecOptionGen<T> {
    pub(crate) fn encode(mut vec: Vec<Option<T>>) -> Self {
        if vec.is_empty() {
            return Self {
                values: Vec::new(),
                lengths: Vec::new(),
            };
        }

        let mut values = Vec::new();
        let mut lengths = Vec::new();
        let mut prev = vec.remove(0);
        let mut count = 1;

        for elem in vec {
            if elem == prev {
                count += 1;
            } else {
                values.push(prev);
                lengths.push(count);
                prev = elem;
                count = 1;
            }
        }
        values.push(prev);
        lengths.push(count);

        Self { values, lengths }
    }
}

impl<T: Clone + PartialEq + Debug> NullableDecoding<T> for RunLengthEncodedVecOptionGen<T> {
    fn decode(self) -> Vec<Option<T>> {
        let mut vec = Vec::new();
        for (value, length) in self.values.into_iter().zip(self.lengths.into_iter()) {
            vec.extend(std::iter::repeat(value).take(length));
        }
        vec
    }

    fn size(&self) -> usize {
        // The size of an Option<T> is generally the same as size of T plus the size of bool flag (1 byte).
        let values_size = self.values.len() * (mem::size_of::<T>() + mem::size_of::<bool>());
        let lengths_size = self.lengths.len() * mem::size_of::<usize>();
        values_size + lengths_size
    }
}

#[derive(Debug, Clone, PartialEq)]
#[enum_dispatch(NullableDecoding)]
pub enum RunLengthEncodedVecOption {
    Bool(RunLengthEncodedVecOptionGen<bool>),
    Int(RunLengthEncodedVecOptionGen<INT>),
    Str(RunLengthEncodedVecOptionGen<String>),
    VecBool(RunLengthEncodedVecOptionGen<Vec<bool>>),
    VecInt(RunLengthEncodedVecOptionGen<Vec<INT>>),
    VecStr(RunLengthEncodedVecOptionGen<Vec<String>>),
    Date(RunLengthEncodedVecOptionGen<NaiveDate>),
    DateTime(RunLengthEncodedVecOptionGen<NaiveDateTime>),
}

macro_rules! implement_from_trait_option {
    ($variant:ident, $type:ty) => {
        impl From<RunLengthEncodedVecOptionGen<$type>> for RunLengthEncodedVecOption {
            fn from(item: RunLengthEncodedVecOptionGen<$type>) -> Self {
                RunLengthEncodedVecOption::$variant(item)
            }
        }
    };
}

implement_from_trait_option!(Bool, bool);
implement_from_trait_option!(Int, INT);
implement_from_trait_option!(Str, String);
implement_from_trait_option!(VecBool, Vec<bool>);
implement_from_trait_option!(VecInt, Vec<INT>);
implement_from_trait_option!(VecStr, Vec<String>);
implement_from_trait_option!(Date, NaiveDate);
implement_from_trait_option!(DateTime, NaiveDateTime);

impl RunLengthEncodedVecOption {
    pub fn decode(&self) -> RawColumnVec {
        match self {
            RunLengthEncodedVecOption::Bool(v) => {
                RawColumnVec::Bool(RawColumnVecGen::Nullable(v.clone().decode()))
            }
            RunLengthEncodedVecOption::Int(v) => {
                RawColumnVec::Int(RawColumnVecGen::Nullable(v.clone().decode()))
            }
            RunLengthEncodedVecOption::Str(v) => {
                RawColumnVec::Str(RawColumnVecGen::Nullable(v.clone().decode()))
            }
            RunLengthEncodedVecOption::VecBool(v) => {
                RawColumnVec::VecBool(RawColumnVecGen::Nullable(v.clone().decode()))
            }
            RunLengthEncodedVecOption::VecInt(v) => {
                RawColumnVec::VecInt(RawColumnVecGen::Nullable(v.clone().decode()))
            }
            RunLengthEncodedVecOption::VecStr(v) => {
                RawColumnVec::VecStr(RawColumnVecGen::Nullable(v.clone().decode()))
            }
            RunLengthEncodedVecOption::Date(v) => {
                RawColumnVec::Date(RawColumnVecGen::Nullable(v.clone().decode()))
            }
            RunLengthEncodedVecOption::DateTime(v) => {
                RawColumnVec::DateTime(RawColumnVecGen::Nullable(v.clone().decode()))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_run_length_encoded_vec_encode() {
        let input = vec![1, 1, 2, 2, 2, 3, 4, 4, 4, 4];
        let encoded = RunLengthEncodedVecGen::encode(input);
        assert_eq!(encoded.values, vec![1, 2, 3, 4]);
        assert_eq!(encoded.lengths, vec![2, 3, 1, 4]);
    }

    #[test]
    fn test_run_length_encoded_vec_decode() {
        let encoded = RunLengthEncodedVecGen {
            values: vec![1, 2, 3, 4],
            lengths: vec![2, 3, 1, 4],
        };
        let decoded = encoded.decode();
        assert_eq!(decoded, vec![1, 1, 2, 2, 2, 3, 4, 4, 4, 4]);
    }

    #[test]
    fn test_run_length_encoded_vec_option_encode() {
        let input = vec![Some(1), Some(1), None, None, Some(2), Some(2)];
        let encoded = RunLengthEncodedVecOptionGen::encode(input);
        assert_eq!(encoded.values, vec![Some(1), None, Some(2)]);
        assert_eq!(encoded.lengths, vec![2, 2, 2]);
    }

    #[test]
    fn test_run_length_encoded_vec_option_decode() {
        let encoded = RunLengthEncodedVecOptionGen {
            values: vec![Some(1), None, Some(2)],
            lengths: vec![2, 2, 2],
        };
        let decoded = encoded.decode();
        assert_eq!(
            decoded,
            vec![Some(1), Some(1), None, None, Some(2), Some(2)]
        );
    }
}
