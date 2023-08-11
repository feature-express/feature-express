use crate::event_store::column_event_store::raw_column::{RawColumnVec, RawColumnVecGen};
use crate::types::{FLOAT, INT};
use bincode;
use chrono::{NaiveDate, NaiveDateTime};
use lz4_flex::{compress_prepend_size, decompress_size_prepended};
use ordered_float::OrderedFloat;
use serde::{Deserialize, Serialize};
use std::io::prelude::*;
use std::io::Cursor;

#[derive(Debug, Clone, PartialEq)]
pub struct LZ4CompressedVecGen<T: PartialEq> {
    compressed_data: Vec<u8>,
    _marker: std::marker::PhantomData<T>,
}

impl<T: PartialEq + Serialize + for<'de> Deserialize<'de>> LZ4CompressedVecGen<T> {
    pub fn encode(input: Vec<T>) -> Result<Self, Box<bincode::ErrorKind>> {
        let serialized_data = bincode::serialize(&input)?;
        let compressed_data = compress_prepend_size(&serialized_data);
        Ok(Self {
            compressed_data,
            _marker: std::marker::PhantomData,
        })
    }

    pub fn decode(self) -> Vec<T> {
        let mut decompressed_data =
            decompress_size_prepended(&self.compressed_data).expect("Cannot decompress data");
        bincode::deserialize(&decompressed_data).expect("Cannot deserialize data")
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum LZ4CompressedVec {
    Bool(LZ4CompressedVecGen<bool>),
    Int(LZ4CompressedVecGen<INT>),
    Num(LZ4CompressedVecGen<OrderedFloat<FLOAT>>),
    Str(LZ4CompressedVecGen<String>),
    VecBool(LZ4CompressedVecGen<Vec<bool>>),
    VecInt(LZ4CompressedVecGen<Vec<INT>>),
    VecStr(LZ4CompressedVecGen<Vec<String>>),
    VecNum(LZ4CompressedVecGen<Vec<OrderedFloat<FLOAT>>>),
    Date(LZ4CompressedVecGen<NaiveDate>),
    DateTime(LZ4CompressedVecGen<NaiveDateTime>),
}

macro_rules! implement_from_trait {
    ($variant:ident, $type:ty) => {
        impl From<LZ4CompressedVecGen<$type>> for LZ4CompressedVec {
            fn from(item: LZ4CompressedVecGen<$type>) -> Self {
                LZ4CompressedVec::$variant(item)
            }
        }
    };
}

impl LZ4CompressedVec {
    pub fn decode(&self) -> RawColumnVec {
        match self {
            LZ4CompressedVec::Bool(v) => {
                RawColumnVec::Bool(RawColumnVecGen::NonNullable(v.clone().decode()))
            }
            LZ4CompressedVec::Int(v) => {
                RawColumnVec::Int(RawColumnVecGen::NonNullable(v.clone().decode()))
            }
            LZ4CompressedVec::Num(v) => {
                RawColumnVec::Num(RawColumnVecGen::NonNullable(v.clone().decode()))
            }
            LZ4CompressedVec::Str(v) => {
                RawColumnVec::Str(RawColumnVecGen::NonNullable(v.clone().decode()))
            }
            LZ4CompressedVec::VecBool(v) => {
                RawColumnVec::VecBool(RawColumnVecGen::NonNullable(v.clone().decode()))
            }
            LZ4CompressedVec::VecInt(v) => {
                RawColumnVec::VecInt(RawColumnVecGen::NonNullable(v.clone().decode()))
            }
            LZ4CompressedVec::VecStr(v) => {
                RawColumnVec::VecStr(RawColumnVecGen::NonNullable(v.clone().decode()))
            }
            LZ4CompressedVec::VecNum(v) => {
                RawColumnVec::VecNum(RawColumnVecGen::NonNullable(v.clone().decode()))
            }
            LZ4CompressedVec::Date(v) => {
                RawColumnVec::Date(RawColumnVecGen::NonNullable(v.clone().decode()))
            }
            LZ4CompressedVec::DateTime(v) => {
                RawColumnVec::DateTime(RawColumnVecGen::NonNullable(v.clone().decode()))
            }
        }
    }
}

implement_from_trait!(Bool, bool);
implement_from_trait!(Int, INT);
implement_from_trait!(Num, OrderedFloat<FLOAT>);
implement_from_trait!(Str, String);
implement_from_trait!(VecBool, Vec<bool>);
implement_from_trait!(VecInt, Vec<INT>);
implement_from_trait!(VecStr, Vec<String>);
implement_from_trait!(VecNum, Vec<OrderedFloat<FLOAT>>);
implement_from_trait!(Date, NaiveDate);
implement_from_trait!(DateTime, NaiveDateTime);

#[derive(Debug, Clone, PartialEq)]
pub struct LZ4CompressedVecOptionGen<T> {
    compressed_data: Vec<u8>,
    _marker: std::marker::PhantomData<T>,
}

impl<T: Serialize + for<'de> Deserialize<'de>> LZ4CompressedVecOptionGen<T> {
    pub fn encode(input: Vec<Option<T>>) -> Result<Self, Box<bincode::ErrorKind>> {
        let serialized_data = bincode::serialize(&input)?;
        let compressed_data = compress_prepend_size(&serialized_data);
        Ok(Self {
            compressed_data,
            _marker: std::marker::PhantomData,
        })
    }

    pub fn decode(self) -> Vec<Option<T>> {
        let mut decompressed_data =
            decompress_size_prepended(&self.compressed_data).expect("Cannot decompress data");
        bincode::deserialize(&decompressed_data).expect("Cannot deserialize data")
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum LZ4CompressedVecOption {
    Bool(LZ4CompressedVecOptionGen<bool>),
    Int(LZ4CompressedVecOptionGen<INT>),
    Num(LZ4CompressedVecOptionGen<OrderedFloat<FLOAT>>),
    Str(LZ4CompressedVecOptionGen<String>),
    VecBool(LZ4CompressedVecOptionGen<Vec<bool>>),
    VecInt(LZ4CompressedVecOptionGen<Vec<INT>>),
    VecStr(LZ4CompressedVecOptionGen<Vec<String>>),
    VecNum(LZ4CompressedVecOptionGen<Vec<OrderedFloat<FLOAT>>>),
    Date(LZ4CompressedVecOptionGen<NaiveDate>),
    DateTime(LZ4CompressedVecOptionGen<NaiveDateTime>),
}

impl LZ4CompressedVecOption {
    pub fn decode(&self) -> RawColumnVec {
        match self {
            LZ4CompressedVecOption::Bool(v) => {
                RawColumnVec::Bool(RawColumnVecGen::Nullable(v.clone().decode()))
            }
            LZ4CompressedVecOption::Int(v) => {
                RawColumnVec::Int(RawColumnVecGen::Nullable(v.clone().decode()))
            }
            LZ4CompressedVecOption::Num(v) => {
                RawColumnVec::Num(RawColumnVecGen::Nullable(v.clone().decode()))
            }
            LZ4CompressedVecOption::Str(v) => {
                RawColumnVec::Str(RawColumnVecGen::Nullable(v.clone().decode()))
            }
            LZ4CompressedVecOption::VecBool(v) => {
                RawColumnVec::VecBool(RawColumnVecGen::Nullable(v.clone().decode()))
            }
            LZ4CompressedVecOption::VecInt(v) => {
                RawColumnVec::VecInt(RawColumnVecGen::Nullable(v.clone().decode()))
            }
            LZ4CompressedVecOption::VecStr(v) => {
                RawColumnVec::VecStr(RawColumnVecGen::Nullable(v.clone().decode()))
            }
            LZ4CompressedVecOption::VecNum(v) => {
                RawColumnVec::VecNum(RawColumnVecGen::Nullable(v.clone().decode()))
            }
            LZ4CompressedVecOption::Date(v) => {
                RawColumnVec::Date(RawColumnVecGen::Nullable(v.clone().decode()))
            }
            LZ4CompressedVecOption::DateTime(v) => {
                RawColumnVec::DateTime(RawColumnVecGen::Nullable(v.clone().decode()))
            }
        }
    }
}

macro_rules! implement_from_trait {
    ($variant:ident, $type:ty) => {
        impl From<LZ4CompressedVecOptionGen<$type>> for LZ4CompressedVecOption {
            fn from(item: LZ4CompressedVecOptionGen<$type>) -> Self {
                LZ4CompressedVecOption::$variant(item)
            }
        }
    };
}

implement_from_trait!(Bool, bool);
implement_from_trait!(Int, INT);
implement_from_trait!(Num, OrderedFloat<FLOAT>);
implement_from_trait!(Str, String);
implement_from_trait!(VecBool, Vec<bool>);
implement_from_trait!(VecInt, Vec<INT>);
implement_from_trait!(VecStr, Vec<String>);
implement_from_trait!(VecNum, Vec<OrderedFloat<FLOAT>>);
implement_from_trait!(Date, NaiveDate);
implement_from_trait!(DateTime, NaiveDateTime);
