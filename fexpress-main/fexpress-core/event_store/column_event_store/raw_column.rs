use crate::event_store::column_event_store::encoded_column::EncodedColumnVec;
use anyhow::{bail, Context, Result};
use chrono::{NaiveDate, NaiveDateTime};

use ordered_float::OrderedFloat;

use crate::event_store::column_event_store::encoded_column::{
    NonNullableEncodedColumnVec, NullableEncodedColumnVec,
};
use crate::event_store::column_event_store::encoding::dictionary::{
    DictionaryEncodedVecGen, DictionaryEncodedVecOptionGen,
};
use crate::event_store::column_event_store::encoding::lz4::{
    LZ4CompressedVecGen, LZ4CompressedVecOptionGen,
};
use crate::event_store::column_event_store::encoding::rle::{
    RunLengthEncodedVecGen, RunLengthEncodedVecOptionGen,
};

use crate::types::{FLOAT, INT};
use crate::value::Value;

#[derive(Debug, Clone, PartialEq)]
pub enum RawColumnVecGen<T> {
    Nullable(Vec<Option<T>>),
    NonNullable(Vec<T>),
}

impl<T> Default for RawColumnVecGen<T> {
    fn default() -> Self {
        Self::NonNullable(vec![])
    }
}

impl<T: Clone> RawColumnVecGen<T> {
    pub fn len(&self) -> usize {
        match self {
            RawColumnVecGen::Nullable(v) => v.len(),
            RawColumnVecGen::NonNullable(v) => v.len(),
        }
    }

    pub fn push_value(&mut self, value: T) {
        match self {
            RawColumnVecGen::Nullable(v) => v.push(Some(value.clone())),
            RawColumnVecGen::NonNullable(v) => v.push(value.clone()),
        }
    }

    pub fn push_none(&mut self) {
        if let RawColumnVecGen::Nullable(nullable) = self {
            nullable.push(None);
        } else {
            self.convert_to_nullable_generic();
            self.push_none();
        }
    }

    pub fn convert_to_nullable_generic(&mut self) {
        *self = match std::mem::take(self) {
            RawColumnVecGen::Nullable(vec) => RawColumnVecGen::Nullable(vec),
            RawColumnVecGen::NonNullable(vec) => {
                let converted_vec: Vec<Option<T>> = vec.into_iter().map(Some).collect();
                RawColumnVecGen::Nullable(converted_vec)
            }
        };
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum RawColumnVec {
    Bool(RawColumnVecGen<bool>),
    Num(RawColumnVecGen<OrderedFloat<FLOAT>>),
    Int(RawColumnVecGen<INT>),
    Str(RawColumnVecGen<String>),
    VecBool(RawColumnVecGen<Vec<bool>>),
    VecNum(RawColumnVecGen<Vec<OrderedFloat<FLOAT>>>),
    VecInt(RawColumnVecGen<Vec<INT>>),
    VecStr(RawColumnVecGen<Vec<String>>),
    Date(RawColumnVecGen<NaiveDate>),
    DateTime(RawColumnVecGen<NaiveDateTime>),
}

macro_rules! encode_column {
    ($data:expr, Nullable) => {{
        let rle_encoding = RunLengthEncodedVecOptionGen::encode($data.to_vec());
        let dict_encoding = DictionaryEncodedVecOptionGen::encode($data.to_vec());
        let rle_size = rle_encoding.values.len() + rle_encoding.lengths.len();
        let dict_size = dict_encoding.values.len() + dict_encoding.dictionary.len();
        if rle_size <= dict_size {
            EncodedColumnVec::Nullable(NullableEncodedColumnVec::RunLengthEncodedVec(
                rle_encoding.into(),
            ))
        } else {
            EncodedColumnVec::Nullable(NullableEncodedColumnVec::DictionaryEncodedVec(
                dict_encoding.into(),
            ))
        }
    }};
    ($data:expr, NonNullable) => {{
        let rle_encoding = RunLengthEncodedVecGen::encode($data.to_vec());
        let dict_encoding = DictionaryEncodedVecGen::encode($data.to_vec());
        let rle_size = rle_encoding.values.len() + rle_encoding.lengths.len();
        let dict_size = dict_encoding.values.len() + dict_encoding.dictionary.len();
        if rle_size <= dict_size {
            EncodedColumnVec::NonNullable(NonNullableEncodedColumnVec::RunLengthEncodedVec(
                rle_encoding.into(),
            ))
        } else {
            EncodedColumnVec::NonNullable(NonNullableEncodedColumnVec::DictionaryEncodedVec(
                dict_encoding.into(),
            ))
        }
    }};
}

impl RawColumnVec {
    pub fn len(&self) -> usize {
        match self {
            RawColumnVec::Bool(v) => v.len(),
            RawColumnVec::Num(v) => v.len(),
            RawColumnVec::Int(v) => v.len(),
            RawColumnVec::Str(v) => v.len(),
            RawColumnVec::VecBool(v) => v.len(),
            RawColumnVec::VecNum(v) => v.len(),
            RawColumnVec::VecInt(v) => v.len(),
            RawColumnVec::VecStr(v) => v.len(),
            RawColumnVec::Date(v) => v.len(),
            RawColumnVec::DateTime(v) => v.len(),
        }
    }

    pub fn push_value(&mut self, value: Value) -> Result<()> {
        match (self, value) {
            (RawColumnVec::Bool(col), Value::Bool(value)) => col.push_value(value),
            (RawColumnVec::Num(col), Value::Num(value)) => col.push_value(OrderedFloat(value)),
            (RawColumnVec::Int(col), Value::Int(value)) => col.push_value(value),
            (RawColumnVec::Str(col), Value::Str(value)) => col.push_value(value),
            (RawColumnVec::VecBool(col), Value::VecBool(value)) => col.push_value(value),
            (RawColumnVec::VecNum(col), Value::VecNum(value)) => {
                let new_vec: Vec<_> = value.iter().map(|v| OrderedFloat(*v)).collect();
                col.push_value(new_vec);
            }
            (RawColumnVec::VecInt(col), Value::VecInt(value)) => col.push_value(value),
            (RawColumnVec::VecStr(col), Value::VecStr(value)) => col.push_value(value),
            (RawColumnVec::Date(col), Value::Date(value)) => col.push_value(value),
            (RawColumnVec::DateTime(col), Value::DateTime(value)) => col.push_value(value),
            (vec, value) => {
                if matches!(value, Value::None) {
                    vec.push_none()?;
                } else {
                    bail!("Trying to push value type {:?} raw vector {:?}", value, vec)
                }
            }
        }
        Ok(())
    }

    pub fn push_none(&mut self) -> Result<()> {
        match self {
            RawColumnVec::Bool(column_data) => column_data.push_none(),
            RawColumnVec::Num(column_data) => column_data.push_none(),
            RawColumnVec::Int(column_data) => column_data.push_none(),
            RawColumnVec::Str(column_data) => column_data.push_none(),
            RawColumnVec::VecBool(column_data) => column_data.push_none(),
            RawColumnVec::VecNum(column_data) => column_data.push_none(),
            RawColumnVec::VecInt(column_data) => column_data.push_none(),
            RawColumnVec::VecStr(column_data) => column_data.push_none(),
            RawColumnVec::Date(column_data) => column_data.push_none(),
            RawColumnVec::DateTime(column_data) => column_data.push_none(),
        }
        Ok(())
    }

    pub fn encode(&self) -> Result<EncodedColumnVec> {
        match self {
            RawColumnVec::Bool(v) => match v {
                RawColumnVecGen::Nullable(nullable) => Ok(encode_column!(nullable, Nullable)),
                RawColumnVecGen::NonNullable(nonnullable) => {
                    Ok(encode_column!(nonnullable, NonNullable))
                }
            },
            RawColumnVec::Num(_v) => todo!(),
            RawColumnVec::Int(v) => match v {
                RawColumnVecGen::Nullable(nullable) => Ok(encode_column!(nullable, Nullable)),
                RawColumnVecGen::NonNullable(nonnullable) => {
                    Ok(encode_column!(nonnullable, NonNullable))
                }
            },
            RawColumnVec::Str(v) => match v {
                RawColumnVecGen::Nullable(nullable) => Ok(encode_column!(nullable, Nullable)),
                RawColumnVecGen::NonNullable(nonnullable) => {
                    Ok(encode_column!(nonnullable, NonNullable))
                }
            },
            RawColumnVec::VecBool(v) => match v {
                RawColumnVecGen::Nullable(nullable) => Ok(encode_column!(nullable, Nullable)),
                RawColumnVecGen::NonNullable(nonnullable) => {
                    Ok(encode_column!(nonnullable, NonNullable))
                }
            },
            RawColumnVec::VecNum(v) => match v {
                RawColumnVecGen::Nullable(nullable) => {
                    let compressed = LZ4CompressedVecOptionGen::encode(nullable.clone())
                        .context("Cannot compress vector")?;
                    Ok(EncodedColumnVec::Nullable(
                        NullableEncodedColumnVec::LZ4EncodedVec(compressed.into()),
                    ))
                }
                RawColumnVecGen::NonNullable(nonnullable) => {
                    let compressed = LZ4CompressedVecGen::encode(nonnullable.clone())
                        .context("Cannot compress vector")?;
                    Ok(EncodedColumnVec::NonNullable(
                        NonNullableEncodedColumnVec::LZ4EncodedVec(compressed.into()),
                    ))
                }
            },
            RawColumnVec::VecInt(v) => match v {
                RawColumnVecGen::Nullable(nullable) => Ok(encode_column!(nullable, Nullable)),
                RawColumnVecGen::NonNullable(nonnullable) => {
                    Ok(encode_column!(nonnullable, NonNullable))
                }
            },
            RawColumnVec::VecStr(v) => match v {
                RawColumnVecGen::Nullable(nullable) => Ok(encode_column!(nullable, Nullable)),
                RawColumnVecGen::NonNullable(nonnullable) => {
                    Ok(encode_column!(nonnullable, NonNullable))
                }
            },
            RawColumnVec::Date(v) => match v {
                RawColumnVecGen::Nullable(nullable) => Ok(encode_column!(nullable, Nullable)),
                RawColumnVecGen::NonNullable(nonnullable) => {
                    Ok(encode_column!(nonnullable, NonNullable))
                }
            },
            RawColumnVec::DateTime(v) => match v {
                RawColumnVecGen::Nullable(nullable) => Ok(encode_column!(nullable, Nullable)),
                RawColumnVecGen::NonNullable(nonnullable) => {
                    Ok(encode_column!(nonnullable, NonNullable))
                }
            },
        }
    }
}

impl RawColumnVec {
    pub fn convert_to_nullable(&mut self) {
        match self {
            RawColumnVec::Bool(v) => v.convert_to_nullable_generic(),
            RawColumnVec::Num(v) => v.convert_to_nullable_generic(),
            RawColumnVec::Int(v) => v.convert_to_nullable_generic(),
            RawColumnVec::Str(v) => v.convert_to_nullable_generic(),
            RawColumnVec::VecBool(v) => v.convert_to_nullable_generic(),
            RawColumnVec::VecNum(v) => v.convert_to_nullable_generic(),
            RawColumnVec::VecInt(v) => v.convert_to_nullable_generic(),
            RawColumnVec::VecStr(v) => v.convert_to_nullable_generic(),
            RawColumnVec::Date(v) => v.convert_to_nullable_generic(),
            RawColumnVec::DateTime(v) => v.convert_to_nullable_generic(),
        }
    }
}
