use std::cmp::Ordering;
use std::fmt;
use std::hash::{Hash, Hasher};

use crate::map::HashMap;
use crate::sstring::SmallString;
use chrono::{Datelike, NaiveDate, NaiveDateTime};
use itertools::Itertools;
use pyo3::exceptions as pyexceptions;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyFloat, PyInt, PyList, PyLong, PyString};
use serde::{Deserialize, Serialize};
use strum::EnumDiscriminants;

use crate::event::AttributeName;
use crate::types::{Timestamp, FLOAT, INT};
use schemars::JsonSchema;

// Can look into this code in the future
// https://github.com/kaimast/schema/blob/0b1cbe7a1aa56c0320a9a041b78bc8142bed2207/src/value.rs

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, EnumDiscriminants, JsonSchema)]
#[serde(untagged)]
pub enum Value {
    None,
    Wildcard,
    Bool(bool),
    Num(FLOAT),
    Int(INT),
    Str(SmallString),
    MapNum(HashMap<AttributeName, FLOAT>),
    MapStr(HashMap<AttributeName, SmallString>),
    VecBool(Vec<bool>),
    VecNum(Vec<FLOAT>),
    VecInt(Vec<INT>),
    VecStr(Vec<SmallString>),
    Date(NaiveDate),
    DateTime(NaiveDateTime),
    Map(HashMap<AttributeName, Box<Value>>),
    ValueWithAlias(Box<ValueWithAlias>),
    NotCalculatedYet,
}

impl Hash for Value {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.to_string().hash(state);
    }
}

impl Value {
    pub fn is_null(&self) -> bool {
        matches!(self, Value::None)
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct ValueWithAlias {
    pub alias: Option<SmallString>,
    pub value: Value,
}

impl AsRef<Value> for ValueWithAlias {
    fn as_ref(&self) -> &Value {
        &self.value
    }
}

impl From<ValueWithAlias> for Value {
    fn from(v: ValueWithAlias) -> Self {
        v.value
    }
}

///Conversions
///
impl Into<Value> for i32 {
    fn into(self) -> Value {
        Value::Int(self as INT)
    }
}

impl Into<Value> for u32 {
    fn into(self) -> Value {
        Value::Int(self as INT)
    }
}

impl Into<Value> for i64 {
    fn into(self) -> Value {
        Value::Int(self as INT)
    }
}

impl Into<Value> for u64 {
    fn into(self) -> Value {
        Value::Int(self as INT)
    }
}

impl Into<Value> for bool {
    fn into(self) -> Value {
        Value::Bool(self)
    }
}

impl Into<Value> for f64 {
    fn into(self) -> Value {
        Value::Num(self as FLOAT)
    }
}

impl Into<Value> for String {
    fn into(self) -> Value {
        Value::Str(SmallString::from(self))
    }
}

impl Into<Value> for Vec<String> {
    fn into(self) -> Value {
        Value::VecStr(self.into_iter().map(SmallString::from).collect())
    }
}

impl Into<Value> for Vec<INT> {
    fn into(self) -> Value {
        Value::VecInt(self.into_iter().map(|x| x as INT).collect())
    }
}

impl Into<Value> for Vec<FLOAT> {
    fn into(self) -> Value {
        Value::VecNum(self)
    }
}

impl Into<Value> for HashMap<AttributeName, FLOAT> {
    fn into(self) -> Value {
        let mut new_v = HashMap::new();
        for (k, v) in self {
            new_v.insert(k, v);
        }
        Value::MapNum(new_v)
    }
}

impl Into<Value> for HashMap<String, String> {
    fn into(self) -> Value {
        let mut new_v = HashMap::new();
        for (k, v) in self {
            new_v.insert(a!(k), SmallString::from(v));
        }
        Value::MapStr(new_v)
    }
}

impl Into<Value> for HashMap<String, FLOAT> {
    fn into(self) -> Value {
        let mut new_v = HashMap::new();
        for (k, v) in self {
            new_v.insert(a!(k), v);
        }
        Value::MapNum(new_v)
    }
}

impl Into<Value> for HashMap<AttributeName, String> {
    fn into(self) -> Value {
        let mut new_v = HashMap::new();
        for (k, v) in self {
            new_v.insert(k, SmallString::from(v));
        }
        Value::MapStr(new_v)
    }
}

impl FromPyObject<'_> for Value {
    fn extract(obj: &PyAny) -> PyResult<Self> {
        if obj.is_none() {
            Ok(Value::None)
        } else if let Ok(v) = PyAny::downcast::<PyString>(obj) {
            let rs_v: String = v.extract()?;
            Ok(rs_v.into())
        } else if let Ok(v) = PyAny::downcast::<PyInt>(obj) {
            let rs_v: i64 = v.extract()?;
            Ok(rs_v.into())
        } else if let Ok(v) = PyAny::downcast::<PyLong>(obj) {
            let rs_v: i64 = v.extract()?;
            Ok(rs_v.into())
        } else if let Ok(v) = PyAny::downcast::<PyFloat>(obj) {
            let rs_v: f64 = v.extract()?;
            Ok(rs_v.into())
        } else if let Ok(v) = PyAny::downcast::<PyList>(obj) {
            if let Ok(vec) = v.extract::<Vec<INT>>() {
                Ok(vec.into())
            } else if let Ok(vec) = v.extract::<Vec<String>>() {
                Ok(vec.into())
            } else {
                Err(PyErr::new::<pyexceptions::PyTypeError, _>("Failed to convert List to Value. Must be one of List[int], List[float], List[str]"))
            }
        } else if let Ok(v) = PyAny::downcast::<PyDict>(obj) {
            if let Ok(hm) = v.extract::<HashMap<String, FLOAT>>() {
                Ok(hm.into())
            } else if let Ok(hm) = v.extract::<HashMap<String, String>>() {
                Ok(hm.into())
            } else if let Ok(dict) = obj.extract::<&PyDict>() {
                let mut hashmap = HashMap::new();
                for (key, value) in dict {
                    let key: String = key.extract()?;
                    let value: Box<Value> = Box::new(value.extract()?);
                    hashmap.insert(a!(key), value);
                }
                Ok(Value::Map(hashmap))
            } else {
                Err(PyErr::new::<pyexceptions::PyTypeError, _>(
                    "Failed to convert Dictionary to Value. Must be Dict[str, float]",
                ))
            }
        } else {
            Err(PyErr::new::<pyexceptions::PyTypeError, _>(
                "Failed to convert PyObject to Value",
            ))
        }
    }
}

impl Eq for Value {}

macro_rules! value_cmp {
    ($self:expr, $other:expr, $cmp:ident, $default:expr) => {
        match ($self, $other) {
            (Value::Bool(a), Value::Bool(b)) => a.$cmp(b),
            (Value::Int(a), Value::Int(b)) => a.$cmp(b),
            (Value::Num(a), Value::Num(b)) => a.$cmp(b),
            (Value::Int(a), Value::Num(b)) => (*a as FLOAT).$cmp(b),
            (Value::Num(a), Value::Int(b)) => a.$cmp(&(*b as FLOAT)),
            (Value::Str(a), Value::Str(b)) => a.$cmp(b),
            (Value::Date(a), Value::Date(b)) => a.$cmp(b),
            (Value::DateTime(a), Value::DateTime(b)) => a.$cmp(b),
            _ => $default,
        }
    };
}

impl PartialOrd for Value {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        value_cmp!(self, other, partial_cmp, None)
    }

    fn lt(&self, other: &Self) -> bool {
        value_cmp!(self, other, lt, false)
    }

    fn le(&self, other: &Self) -> bool {
        value_cmp!(self, other, le, false)
    }

    fn gt(&self, other: &Self) -> bool {
        value_cmp!(self, other, gt, false)
    }

    fn ge(&self, other: &Self) -> bool {
        value_cmp!(self, other, ge, false)
    }
}

impl Ord for Value {
    fn cmp(&self, other: &Self) -> Ordering {
        match (self, other) {
            (Value::Bool(a), Value::Bool(b)) => a.cmp(b),
            (Value::Int(a), Value::Int(b)) => a.cmp(b),
            (Value::Num(a), Value::Num(b)) => {
                if (a - b).abs() < 1e-06 {
                    Ordering::Equal
                } else if a > b {
                    Ordering::Greater
                } else {
                    Ordering::Less
                }
            }
            (Value::Int(a), Value::Num(b)) => {
                let a_float = *a as FLOAT;
                if (a_float - b).abs() < 1e-06 {
                    Ordering::Equal
                } else if a_float > *b {
                    Ordering::Greater
                } else {
                    Ordering::Less
                }
            }
            (Value::Num(a), Value::Int(b)) => {
                let b_float = *b as FLOAT;
                if (a - b_float).abs() < 1e-06 {
                    Ordering::Equal
                } else if *a > b_float {
                    Ordering::Greater
                } else {
                    Ordering::Less
                }
            }
            (Value::Str(a), Value::Str(b)) => a.cmp(b),
            (Value::Date(a), Value::Date(b)) => a.cmp(b),
            (Value::DateTime(a), Value::DateTime(b)) => a.cmp(b),
            (Value::None, Value::None) => Ordering::Equal,
            _ => unreachable!(),
        }
    }

    fn max(self, other: Self) -> Self
    where
        Self: Sized,
    {
        if self > other {
            self
        } else {
            other
        }
    }

    fn min(self, other: Self) -> Self
    where
        Self: Sized,
    {
        if self < other {
            self
        } else {
            other
        }
    }

    fn clamp(self, min: Self, max: Self) -> Self
    where
        Self: Sized,
    {
        if self < min {
            min
        } else if self > max {
            max
        } else {
            self
        }
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub enum ValueType {
    Bool,
    Num,
    Int,
    MapNum,
    MapStr,
    Str,
    Date,
    DateTime,
    VecCat,
    VecNum,
    VecInt,
    VecBool,
    Map,
    None,
    Wildcard,
    NotCalculatedYet,
}

impl Into<ValueType> for Value {
    fn into(self) -> ValueType {
        match self {
            Value::Bool(_) => ValueType::Bool,
            Value::Num(_) => ValueType::Num,
            Value::Int(_) => ValueType::Int,
            Value::Str(_) => ValueType::Str,
            Value::Map(_) => ValueType::Map,
            Value::Date(_) => ValueType::Date,
            Value::DateTime(_) => ValueType::DateTime,
            Value::VecStr(_) => ValueType::VecCat,
            Value::VecInt(_) => ValueType::VecInt,
            Value::VecNum(_) => ValueType::VecNum,
            Value::VecBool(_) => ValueType::VecBool,
            Value::None => ValueType::None,
            Value::MapNum(_) => ValueType::MapNum,
            Value::ValueWithAlias(v) => v.value.into(),
            Value::MapStr(_) => ValueType::MapStr,
            Value::Wildcard => ValueType::Wildcard,
            Value::NotCalculatedYet => ValueType::NotCalculatedYet,
        }
    }
}

impl fmt::Display for ValueType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ValueType::Bool => write!(f, "Bool"),
            ValueType::Num => write!(f, "Num"),
            ValueType::Int => write!(f, "Int"),
            ValueType::MapNum => write!(f, "MapNum"),
            ValueType::MapStr => write!(f, "MapStr"),
            ValueType::Str => write!(f, "Str"),
            ValueType::Date => write!(f, "Date"),
            ValueType::DateTime => write!(f, "DateTime"),
            ValueType::VecCat => write!(f, "VecCat"),
            ValueType::VecNum => write!(f, "VecNum"),
            ValueType::VecInt => write!(f, "VecInt"),
            ValueType::VecBool => write!(f, "VecBool"),
            ValueType::Map => write!(f, "Map"),
            ValueType::None => write!(f, "None"),
            ValueType::Wildcard => write!(f, "Wildcard"),
            ValueType::NotCalculatedYet => write!(f, "?"),
        }
    }
}

impl From<Value> for Option<FLOAT> {
    fn from(e: Value) -> Self {
        match e {
            Value::Bool(v) => Some((v as i64) as FLOAT),
            Value::Num(v) => Some(v),
            Value::Int(v) => Some(v as FLOAT),
            _ => None,
        }
    }
}

impl Into<Option<NaiveDate>> for Value {
    fn into(self) -> Option<NaiveDate> {
        match self {
            Value::Date(v) => Some(v),
            Value::DateTime(v) => Some(NaiveDate::from_ymd(v.year(), v.month(), v.day())),
            Value::Str(v) => {
                let formats = [
                    "%Y-%m-%d", "%d/%m/%Y", "%m/%d/%Y", "%d-%m-%Y", "%d %b %Y", "%d %B %Y",
                ];

                formats
                    .iter()
                    .filter_map(|&fmt| NaiveDate::parse_from_str(&v, fmt).ok())
                    .next()
            }
            _ => None,
        }
    }
}

impl Into<Option<INT>> for Value {
    fn into(self) -> Option<INT> {
        match self {
            Value::Num(v) => Some(v as INT),
            Value::Int(v) => Some(v),
            _ => None,
        }
    }
}

impl Into<FLOAT> for Value {
    fn into(self) -> FLOAT {
        match self {
            Value::Num(v) => v,
            Value::Int(v) => v as FLOAT,
            Value::Bool(v) => {
                if v {
                    1.0
                } else {
                    0.0
                }
            }
            Value::Wildcard => 1.0,
            _ => panic!("Cannot convert {:?} to FLOAT TYPE", self),
        }
    }
}

impl Into<bool> for Value {
    fn into(self) -> bool {
        match self {
            Value::Bool(v) => v,
            _ => false,
        }
    }
}

impl ToString for Value {
    fn to_string(&self) -> String {
        match self {
            Value::Bool(v) => v.to_string(),
            Value::Num(v) => v.to_string(),
            Value::Int(v) => v.to_string(),
            Value::Str(v) => v.clone().to_string(),
            Value::Map(map) => {
                let map_entries: Vec<String> = map
                    .iter()
                    .map(|(k, v)| format!("{}: {}", k, v.to_string()))
                    .collect();
                format!("{{{}}}", map_entries.join(", "))
            }
            Value::None => "".into(),
            Value::Date(v) => v.to_string(),
            Value::DateTime(v) => v.to_string(),
            Value::MapNum(v) => {
                let map_entries: Vec<String> =
                    v.iter().map(|(k, v)| format!("{}: {}", k, v)).collect();
                format!("{{{}}}", map_entries.join(", "))
            }
            Value::MapStr(v) => {
                let map_entries: Vec<String> =
                    v.iter().map(|(k, v)| format!("{}: {}", k, v)).collect();
                format!("{{{}}}", map_entries.join(", "))
            }
            Value::VecStr(v) => v.join(","),
            Value::VecInt(v) => v.iter().map(|v| v.to_string()).collect_vec().join(","),
            Value::VecNum(v) => v.iter().map(|v| v.to_string()).collect_vec().join(","),
            Value::VecBool(v) => v.iter().map(|v| v.to_string()).collect_vec().join(","),
            Value::ValueWithAlias(v) => v.value.to_string(),
            Value::Wildcard => "*".into(),
            Value::NotCalculatedYet => "?".into(),
        }
    }
}

impl IntoPy<Py<PyAny>> for Value {
    fn into_py(self, py: Python<'_>) -> Py<PyAny> {
        match self {
            Value::None => py.None(),
            Value::Wildcard => py.None(),
            Value::DateTime(v) => v.to_string().into_py(py),
            Value::Bool(v) => v.into_py(py),
            Value::Int(v) => v.into_py(py),
            Value::Str(v) => v.into_py(py),
            Value::Num(v) => v.into_py(py),
            Value::MapNum(v) => {
                let mut hm = HashMap::new();
                for (k, v) in v {
                    hm.insert(k.to_string(), v);
                }
                hm.into_py(py)
            }
            Value::MapStr(v) => {
                let mut hm = HashMap::new();
                for (k, v) in v {
                    hm.insert(k.to_string(), v.to_string());
                }
                hm.into_py(py)
            }
            Value::VecStr(v) => {
                let v = v.iter().map(|x| x.to_string()).collect_vec();
                v.into_py(py)
            }
            Value::VecNum(v) => v.into_py(py),
            Value::VecInt(v) => v.into_py(py),
            Value::VecBool(v) => v.into_py(py),
            Value::Date(v) => v.to_string().into_py(py),
            Value::Map(v) => {
                let mut hm: HashMap<String, PyObject> = HashMap::new();
                for (k, v) in v {
                    hm.insert(k.to_string(), (*v).into_py(py));
                }
                hm.into_py(py)
            }
            Value::ValueWithAlias(v) => {
                (v.alias.unwrap_or("".into()).to_string(), v.value).into_py(py)
            }
            Value::NotCalculatedYet => py.None(),
        }
    }
}

pub fn nan_to_none(v: Value) -> Value {
    match v {
        Value::Num(v) if v.is_nan() => Value::None,
        _ => v,
    }
}

#[derive(Clone, Debug)]
pub struct ValueWithTimestamp {
    pub value: Value,
    pub ts: Timestamp,
}

#[derive(Clone, Debug)]
pub struct ValueWithDates {
    pub value: Value,
    pub ts: Timestamp,
    pub obs_dt: Timestamp,
}
