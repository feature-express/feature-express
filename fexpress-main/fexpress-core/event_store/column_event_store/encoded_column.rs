use crate::event_store::column_event_store::encoding::dictionary::{
    DictionaryEncodedVec, DictionaryEncodedVecOption,
};
use crate::event_store::column_event_store::encoding::lz4::{
    LZ4CompressedVec, LZ4CompressedVecOption,
};
use crate::event_store::column_event_store::encoding::rle::{
    RunLengthEncodedVec, RunLengthEncodedVecOption,
};
use crate::event_store::column_event_store::raw_column::RawColumnVec;

#[derive(Debug, Clone, PartialEq)]
pub enum NullableEncodedColumnVec {
    RunLengthEncodedVec(RunLengthEncodedVecOption),
    DictionaryEncodedVec(DictionaryEncodedVecOption),
    LZ4EncodedVec(LZ4CompressedVecOption),
}

impl NullableEncodedColumnVec {
    pub fn decode(&self) -> RawColumnVec {
        match self {
            NullableEncodedColumnVec::RunLengthEncodedVec(rle) => rle.decode(),
            NullableEncodedColumnVec::DictionaryEncodedVec(dict) => dict.decode(),
            NullableEncodedColumnVec::LZ4EncodedVec(lz4) => lz4.decode(),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum NonNullableEncodedColumnVec {
    RunLengthEncodedVec(RunLengthEncodedVec),
    DictionaryEncodedVec(DictionaryEncodedVec),
    LZ4EncodedVec(LZ4CompressedVec),
}

impl NonNullableEncodedColumnVec {
    pub fn decode(&self) -> RawColumnVec {
        match self {
            NonNullableEncodedColumnVec::RunLengthEncodedVec(rle) => rle.decode(),
            NonNullableEncodedColumnVec::DictionaryEncodedVec(dict) => dict.decode(),
            NonNullableEncodedColumnVec::LZ4EncodedVec(lz4) => lz4.decode(),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum EncodedColumnVec {
    Nullable(NullableEncodedColumnVec),
    NonNullable(NonNullableEncodedColumnVec),
}

impl EncodedColumnVec {
    pub fn decode(&self) -> RawColumnVec {
        match self {
            EncodedColumnVec::Nullable(nullable) => nullable.decode(),
            EncodedColumnVec::NonNullable(nonnullable) => nonnullable.decode(),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum ColumnVecType {
    NullableType,
    NonNullableType,
}
