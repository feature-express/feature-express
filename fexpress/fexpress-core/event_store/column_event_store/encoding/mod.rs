pub mod bitpacking_float;
pub mod bitpacking_int;
pub mod dictionary;
pub mod lz4;
pub mod rle;

use enum_dispatch::enum_dispatch;

use std::fmt::Debug;
// pub use bitpacking_float::{BitPackedFloatVec, BitPackedFloatVecOption};
// pub use bitpacking_int::{BitPackedIntVec, BitPackedIntVecOption};
// pub use dictionary::{DictionaryEncodedVec, DictionaryEncodedVecOption};
// pub use rle::{RunLengthEncodedVec, RunLengthEncodedVecOption};
// use crate::column_event_store::encoding::rle::RunLengthEncodedVecOptionGen;
// use crate::column_event_store::encoding::rle::RunLengthEncodedVecGen;
use crate::types::INT;
use chrono::{NaiveDate, NaiveDateTime};

#[enum_dispatch]
pub trait NonNullableDecoding<T: Debug + Clone + PartialEq>: Debug + Clone {
    /// Encodes a vector of T into a bit-packed representation
    // fn encode(vec: Vec<T>) -> Self;

    /// Decodes the bit-packed representation back into a vector of T
    fn decode(self) -> Vec<T>;

    /// Returns the size in bytes of the bit-packed representation
    fn size(&self) -> usize;
}

#[enum_dispatch]
pub trait NullableDecoding<T: Debug + Clone + PartialEq>: Debug + Clone {
    /// Encodes a vector of T into a bit-packed representation
    // fn encode(vec: Vec<Option<T>>) -> Self;

    /// Decodes the bit-packed representation back into a vector of T
    fn decode(self) -> Vec<Option<T>>;

    /// Returns the size in bytes of the bit-packed representation
    fn size(&self) -> usize;
}
