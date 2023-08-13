use crate::event_store::column_event_store::encoding::{NonNullableDecoding, NullableDecoding};
use crate::event_store::column_event_store::raw_column::{RawColumnVec, RawColumnVecGen};
use crate::types::{FLOAT, INT};
use chrono::{NaiveDate, NaiveDateTime};
use enum_dispatch::enum_dispatch;
use ordered_float::OrderedFloat;
use std::collections::HashMap;
use std::fmt::Debug;
use std::hash::Hash;
use std::mem;

#[derive(Debug, Clone, PartialEq)]
pub struct DictionaryEncodedVecGen<T: PartialEq + Eq + Hash> {
    pub values: Vec<u32>,
    pub dictionary: Vec<T>,
    pub string_to_index: HashMap<T, u32>,
}

impl<T: PartialEq + Eq + Hash + Clone> DictionaryEncodedVecGen<T> {
    pub(crate) fn encode(vec: Vec<T>) -> Self {
        let mut dictionary = Vec::new();
        let mut string_to_index = HashMap::new();
        let mut values = Vec::new();
        let mut next_index = 0;
        for elem in vec {
            if let Some(&index) = string_to_index.get(&elem) {
                values.push(index);
            } else {
                dictionary.push(elem.clone());
                values.push(next_index);
                string_to_index.insert(elem, next_index);
                next_index += 1;
            }
        }
        Self {
            values,
            dictionary,
            string_to_index,
        }
    }
}

impl<T: Clone + Eq + Hash + Debug> NonNullableDecoding<T> for DictionaryEncodedVecGen<T> {
    fn decode(self) -> Vec<T> {
        self.values
            .clone()
            .into_iter()
            .map(|index| self.dictionary[index as usize].clone())
            .collect()
    }

    fn size(&self) -> usize {
        let values_size = self.values.len() * mem::size_of::<u32>();
        let dictionary_size = self.dictionary.len() * mem::size_of::<T>();
        let hashmap_keys_size =
            self.string_to_index.len() * (mem::size_of::<T>() + mem::size_of::<u32>());
        let hashmap_overhead =
            self.string_to_index.capacity() * (mem::size_of::<T>() + mem::size_of::<u32>());

        values_size + dictionary_size + hashmap_keys_size + hashmap_overhead
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum DictionaryEncodedVec {
    Bool(DictionaryEncodedVecGen<bool>),
    Int(DictionaryEncodedVecGen<INT>),
    Str(DictionaryEncodedVecGen<String>),
    VecBool(DictionaryEncodedVecGen<Vec<bool>>),
    VecInt(DictionaryEncodedVecGen<Vec<INT>>),
    VecStr(DictionaryEncodedVecGen<Vec<String>>),
    Date(DictionaryEncodedVecGen<NaiveDate>),
    DateTime(DictionaryEncodedVecGen<NaiveDateTime>),
}

macro_rules! implement_from_trait {
    ($variant:ident, $type:ty) => {
        impl From<DictionaryEncodedVecGen<$type>> for DictionaryEncodedVec {
            fn from(item: DictionaryEncodedVecGen<$type>) -> Self {
                DictionaryEncodedVec::$variant(item)
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

impl DictionaryEncodedVec {
    pub fn decode(&self) -> RawColumnVec {
        match self {
            DictionaryEncodedVec::Bool(v) => {
                RawColumnVec::Bool(RawColumnVecGen::NonNullable(v.clone().decode()))
            }
            DictionaryEncodedVec::Int(v) => {
                RawColumnVec::Int(RawColumnVecGen::NonNullable(v.clone().decode()))
            }
            DictionaryEncodedVec::Str(v) => {
                RawColumnVec::Str(RawColumnVecGen::NonNullable(v.clone().decode()))
            }
            DictionaryEncodedVec::VecBool(v) => {
                RawColumnVec::VecBool(RawColumnVecGen::NonNullable(v.clone().decode()))
            }
            DictionaryEncodedVec::VecInt(v) => {
                RawColumnVec::VecInt(RawColumnVecGen::NonNullable(v.clone().decode()))
            }
            DictionaryEncodedVec::VecStr(v) => {
                RawColumnVec::VecStr(RawColumnVecGen::NonNullable(v.clone().decode()))
            }
            DictionaryEncodedVec::Date(v) => {
                RawColumnVec::Date(RawColumnVecGen::NonNullable(v.clone().decode()))
            }
            DictionaryEncodedVec::DateTime(v) => {
                RawColumnVec::DateTime(RawColumnVecGen::NonNullable(v.clone().decode()))
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct DictionaryEncodedVecOptionGen<T: Eq + Hash> {
    pub values: Vec<u32>,
    pub dictionary: Vec<Option<T>>,
    pub string_to_index: HashMap<Option<T>, u32>,
}

impl<T: Eq + Hash + Clone> DictionaryEncodedVecOptionGen<T> {
    pub(crate) fn encode(vec: Vec<Option<T>>) -> Self {
        let mut dictionary = Vec::new();
        let mut string_to_index = HashMap::new();
        let mut values = Vec::new();
        let mut next_index = 0;
        for elem in vec {
            if let Some(&index) = string_to_index.get(&elem) {
                values.push(index);
            } else {
                dictionary.push(elem.clone());
                values.push(next_index);
                string_to_index.insert(elem, next_index);
                next_index += 1;
            }
        }
        Self {
            values,
            dictionary,
            string_to_index,
        }
    }
}

impl<T: Clone + Eq + Hash + Debug> NullableDecoding<T> for DictionaryEncodedVecOptionGen<T> {
    fn decode(self) -> Vec<Option<T>> {
        self.values
            .clone()
            .into_iter()
            .map(|index| self.dictionary[index as usize].clone())
            .collect()
    }

    fn size(&self) -> usize {
        let values_size = self.values.len() * mem::size_of::<u32>();
        let dictionary_size =
            self.dictionary.len() * (mem::size_of::<T>() + mem::size_of::<bool>());
        let hashmap_keys_size =
            self.string_to_index.len() * (mem::size_of::<Option<T>>() + mem::size_of::<u32>());
        let hashmap_overhead =
            self.string_to_index.capacity() * (mem::size_of::<Option<T>>() + mem::size_of::<u32>());

        values_size + dictionary_size + hashmap_keys_size + hashmap_overhead
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum DictionaryEncodedVecOption {
    Bool(DictionaryEncodedVecOptionGen<bool>),
    Int(DictionaryEncodedVecOptionGen<INT>),
    Str(DictionaryEncodedVecOptionGen<String>),
    VecBool(DictionaryEncodedVecOptionGen<Vec<bool>>),
    VecInt(DictionaryEncodedVecOptionGen<Vec<INT>>),
    VecStr(DictionaryEncodedVecOptionGen<Vec<String>>),
    Date(DictionaryEncodedVecOptionGen<NaiveDate>),
    DateTime(DictionaryEncodedVecOptionGen<NaiveDateTime>),
}

macro_rules! implement_from_trait_option {
    ($variant:ident, $type:ty) => {
        impl From<DictionaryEncodedVecOptionGen<$type>> for DictionaryEncodedVecOption {
            fn from(item: DictionaryEncodedVecOptionGen<$type>) -> Self {
                DictionaryEncodedVecOption::$variant(item)
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

impl DictionaryEncodedVecOption {
    pub fn decode(&self) -> RawColumnVec {
        match self {
            DictionaryEncodedVecOption::Bool(v) => {
                RawColumnVec::Bool(RawColumnVecGen::Nullable(v.clone().decode()))
            }
            DictionaryEncodedVecOption::Int(v) => {
                RawColumnVec::Int(RawColumnVecGen::Nullable(v.clone().decode()))
            }
            DictionaryEncodedVecOption::Str(v) => {
                RawColumnVec::Str(RawColumnVecGen::Nullable(v.clone().decode()))
            }
            DictionaryEncodedVecOption::VecBool(v) => {
                RawColumnVec::VecBool(RawColumnVecGen::Nullable(v.clone().decode()))
            }
            DictionaryEncodedVecOption::VecInt(v) => {
                RawColumnVec::VecInt(RawColumnVecGen::Nullable(v.clone().decode()))
            }
            DictionaryEncodedVecOption::VecStr(v) => {
                RawColumnVec::VecStr(RawColumnVecGen::Nullable(v.clone().decode()))
            }
            DictionaryEncodedVecOption::Date(v) => {
                RawColumnVec::Date(RawColumnVecGen::Nullable(v.clone().decode()))
            }
            DictionaryEncodedVecOption::DateTime(v) => {
                RawColumnVec::DateTime(RawColumnVecGen::Nullable(v.clone().decode()))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_dictionary_encoded_vec_encode() {
        let input = vec!["apple", "banana", "apple", "orange"];
        let encoded = DictionaryEncodedVecGen::encode(input);
        assert_eq!(encoded.values, vec![0, 1, 0, 2]);
        assert_eq!(encoded.dictionary, vec!["apple", "banana", "orange"]);
    }

    #[test]
    fn test_dictionary_encoded_vec_decode() {
        let encoded = DictionaryEncodedVecGen {
            values: vec![0, 1, 0, 2],
            dictionary: vec!["apple", "banana", "orange"].iter().cloned().collect(),
            string_to_index: HashMap::new(), // not used in decoding
        };
        let decoded = encoded.decode();
        assert_eq!(decoded, vec!["apple", "banana", "apple", "orange"]);
    }

    #[test]
    fn test_dictionary_encoded_vec_option_encode() {
        let input = vec![Some("apple"), None, Some("banana"), None, Some("apple")];
        let encoded = DictionaryEncodedVecOptionGen::encode(input);
        assert_eq!(encoded.values, vec![0, 1, 2, 1, 0]);
        assert_eq!(
            encoded.dictionary,
            vec![Some("apple"), None, Some("banana")]
        );
    }

    #[test]
    fn test_dictionary_encoded_vec_option_decode() {
        let encoded = DictionaryEncodedVecOptionGen {
            values: vec![0, 1, 2, 1, 0],
            dictionary: vec![Some("apple"), None, Some("banana")],
            string_to_index: HashMap::new(), // not used in decoding
        };
        let decoded = encoded.decode();
        assert_eq!(
            decoded,
            vec![Some("apple"), None, Some("banana"), None, Some("apple")]
        );
    }

    #[test]
    fn check_dispatch() {
        let encoded = DictionaryEncodedVecOptionGen {
            values: vec![0, 1, 2, 1, 0],
            dictionary: vec![Some("apple".to_string()), None, Some("banana".to_string())],
            string_to_index: HashMap::new(), // not used in decoding
        };
        let wrapped = DictionaryEncodedVecOption::Str(encoded);
        println!("{:?}", wrapped.decode());
    }
}
