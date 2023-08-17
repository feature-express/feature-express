use std::collections::BTreeMap;
use std::mem::size_of;

use crate::sstring::SmallString;
use chrono::NaiveDateTime;

use crate::event::{EntityID, EntityType};

pub type Timestamp = NaiveDateTime;
pub type EventID = SmallString;

pub type Entities = BTreeMap<EntityType, EntityID>;

// make this a feature flag
pub type FLOAT = f32;
pub const BITS_PER_FLOAT: usize = size_of::<FLOAT>() * 8;
pub const BITS_PER_INT: usize = size_of::<INT>() * 8;

pub type INT = i32;
pub type UINT = u32;
