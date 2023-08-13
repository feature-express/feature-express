use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::fmt;
use std::fmt::{Display, Formatter};
use std::str::FromStr;

use crate::map::HashMap;
use crate::sstring::SmallString;
use anyhow::{anyhow, Error, Result};
use chrono::Utc;
use derivative::Derivative;
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use serde_json::to_string;
use vec1::Vec1;

use crate::datetime_utils::deserialize_naive_date_time;
use crate::types::{Entities, EventID, Timestamp};
use crate::value::{Value, ValueType};
use schemars::gen::SchemaGenerator;
use schemars::schema::{InstanceType, Schema, SchemaObject};
use schemars::{schema_for, JsonSchema};

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub enum AttributeKey {
    Single(SmallString),
    Nested(Vec1<SmallString>),
}

impl AttributeKey {
    pub fn to_kstring(&self) -> SmallString {
        match self {
            AttributeKey::Single(s) => s.clone(),
            AttributeKey::Nested(v) => v.iter().join(".").into(),
        }
    }
}

impl Display for AttributeKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            AttributeKey::Single(value) => write!(f, "{}", value),
            AttributeKey::Nested(values) => write!(f, "{}", values.join(".")),
        }
    }
}

impl FromStr for AttributeKey {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let parts = s.split('.').map(|x| x.to_string()).collect_vec();
        if parts.len() == 1 {
            Ok(AttributeKey::Single(SmallString::from(
                parts[0].to_string(),
            )))
        } else if parts.len() > 1 {
            let mut parts_iter = parts.into_iter();
            let first = parts_iter
                .next()
                .ok_or(anyhow!("Cannot extract first part of the attribute key"))?;
            let mut vec = Vec1::new(first.into());
            for part in parts_iter {
                vec.push(part.into());
            }
            Ok(AttributeKey::Nested(vec))
        } else {
            Err(anyhow!("Could not parse the string into an Attribute."))
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct AttributeName(pub SmallString);

impl AttributeName {
    pub fn new<S: AsRef<str>>(name: S) -> Self {
        let smallname = SmallString::from(name.as_ref());
        AttributeName(smallname)
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl Display for AttributeName {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<&str> for AttributeName {
    fn from(name: &str) -> Self {
        AttributeName::new(name)
    }
}

/// This is the original format of the event
#[derive(Derivative)]
#[derivative(Hash)]
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct Event {
    pub event_type: EventType,
    #[serde(deserialize_with = "deserialize_naive_date_time")]
    pub event_time: Timestamp,
    pub entities: Entities,
    pub event_id: Option<EventID>,
    pub experiment_id: Option<SmallString>,
    #[derivative(Hash = "ignore")]
    pub attrs: Option<HashMap<AttributeName, Value>>,
}

impl PartialOrd for Event {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Eq for Event {}

impl Ord for Event {
    fn cmp(&self, other: &Self) -> Ordering {
        self.event_time.cmp(&other.event_time)
    }
}

impl Event {
    pub fn entities(&self) -> Vec<Entity> {
        self.entities
            .iter()
            .map(|(entity_type, entity_id)| Entity {
                typ: entity_type.clone(),
                id: entity_id.0.clone(),
            })
            .collect()
    }
}

impl Default for Event {
    fn default() -> Self {
        Self {
            event_type: EventType("".into()),
            event_time: Utc::now().naive_utc(),
            entities: BTreeMap::new(),
            event_id: None,
            experiment_id: None,
            attrs: None,
        }
    }
}

impl Event {
    pub fn extract_attribute(&self, attribute: &AttributeKey) -> Option<Value> {
        let attrs = self.attrs.as_ref()?;
        match attribute {
            AttributeKey::Single(k) => attrs.get(&(a!(k))).cloned(),
            AttributeKey::Nested(keys) => {
                let attribute_name = a!(&keys[0]);
                let mut current_value = attrs.get(&attribute_name).cloned();
                for key in keys[1..].iter() {
                    let key = a!(key);
                    current_value = match current_value {
                        Some(Value::Map(map)) => {
                            let val = map.get(&key).map(|v| (**v).clone());
                            val
                        }
                        Some(Value::MapStr(map)) => {
                            let val = map.get(&key).cloned().map(Value::Str);
                            val
                        }
                        Some(Value::MapNum(map)) => {
                            let val = map.get(&key).cloned().map(Value::Num);
                            val
                        }
                        _ => return None,
                    };
                }
                current_value
            }
        }
    }

    pub fn extract_attributes_value_types(&self) -> HashMap<AttributeName, ValueType> {
        let mut types = HashMap::new();
        if let Some(attrs) = &self.attrs {
            for (key, value) in attrs {
                self.populate_value_types(key.clone(), value, &mut types);
            }
        }
        types
    }

    fn populate_value_types(
        &self,
        prefix: AttributeName,
        value: &Value,
        types: &mut HashMap<AttributeName, ValueType>,
    ) {
        match value {
            Value::Map(map) => {
                for (key, value) in map {
                    let new_prefix = a!(SmallString::from(format!("{}.{}", prefix, key)));
                    self.populate_value_types(new_prefix, value, types);
                }
            }
            _ => {
                types.insert(prefix, value.clone().into());
            }
        }
    }

    pub fn extract_attributes_values(&self) -> HashMap<AttributeName, Value> {
        let mut values = HashMap::new();
        if let Some(attrs) = &self.attrs {
            for (key, value) in attrs {
                self.populate_values(key.clone(), value, &mut values);
            }
        }
        values
    }

    fn populate_values(
        &self,
        prefix: AttributeName,
        value: &Value,
        values: &mut HashMap<AttributeName, Value>,
    ) {
        match value {
            Value::Map(map) => {
                values.insert(prefix.clone(), value.clone());
                for (key, value) in map {
                    let new_prefix = a!(SmallString::from(format!("{}.{}", prefix, key)));
                    self.populate_values(new_prefix, value, values);
                }
            }
            Value::MapNum(map) => {
                values.insert(prefix.clone(), value.clone());
                for (key, value) in map {
                    let new_prefix = a!(SmallString::from(format!("{}.{}", prefix, key)));
                    values.insert(new_prefix, Value::Num(*value));
                }
            }
            Value::MapStr(map) => {
                values.insert(prefix.clone(), value.clone());
                for (key, value) in map {
                    let new_prefix = a!(SmallString::from(format!("{}.{}", prefix, key)));
                    values.insert(new_prefix, Value::Str(value.clone()));
                }
            }
            _ => {
                values.insert(prefix, value.clone());
            }
        }
    }
}

#[allow(dead_code)]
impl Event {
    pub fn json_serialize_attributes(&self) -> Result<String> {
        to_string(&self.attrs).map_err(|e| anyhow!(format!("Parsing error {:#}", e)))
    }

    pub fn json_deserialize_attributes(&self, s: &str) -> Result<HashMap<String, Value>> {
        serde_json::from_str(s).map_err(|e| anyhow!(format!("Parsing error {:#}", e)))
    }
}

#[derive(Clone, Debug, Hash, Eq, Ord, PartialOrd, PartialEq, Serialize, Deserialize)]
pub struct EntityType(pub SmallString);

impl JsonSchema for EntityType {
    fn is_referenceable() -> bool {
        false
    }

    fn schema_name() -> String {
        "EntityType".to_owned()
    }

    fn json_schema(_: &mut SchemaGenerator) -> Schema {
        SchemaObject {
            instance_type: Some(InstanceType::String.into()),
            ..Default::default()
        }
        .into()
    }
}

impl From<&str> for EntityType {
    fn from(value: &str) -> Self {
        Self(value.to_owned().into())
    }
}

impl From<SmallString> for EntityType {
    fn from(value: SmallString) -> Self {
        Self(value)
    }
}

#[derive(Clone, Debug, Hash, Eq, PartialOrd, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct EntityID(pub SmallString);

impl From<&str> for EntityID {
    fn from(value: &str) -> Self {
        Self(value.to_owned().into())
    }
}

impl From<SmallString> for EntityID {
    fn from(value: SmallString) -> Self {
        Self(value)
    }
}

#[derive(Clone, Debug, Hash, PartialOrd, Eq, PartialEq, Serialize, Deserialize)]
pub struct Entity {
    pub typ: EntityType,
    pub id: SmallString,
}

impl Ord for Entity {
    fn cmp(&self, other: &Self) -> Ordering {
        match self.typ.cmp(&other.typ) {
            Ordering::Equal => self.id.cmp(&other.id),
            non_eq => non_eq,
        }
    }
}

impl From<&str> for Entity {
    fn from(value: &str) -> Self {
        Self {
            typ: "".into(),
            id: value.to_owned().into(),
        }
    }
}

impl From<(SmallString, SmallString)> for Entity {
    fn from(value: (SmallString, SmallString)) -> Self {
        Self {
            typ: value.0.into(),
            id: value.1,
        }
    }
}

impl Into<Entities> for Entity {
    fn into(self) -> Entities {
        btreemap![self.typ.clone() => EntityID(self.id)]
    }
}

impl Into<Entities> for &Entity {
    fn into(self) -> Entities {
        btreemap![self.typ.clone() => EntityID(self.id.clone())]
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Hash, Serialize, Deserialize, JsonSchema)]
pub struct EventType(pub SmallString);

impl Display for EventType {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct EventWithoutEntity {
    pub event_id: Option<SmallString>,
    pub event_type: EventType,
    pub event_time: Timestamp,
    pub experiment_id: Option<SmallString>,
    pub attrs: Option<SmallString>,
}

impl Into<EventWithoutEntity> for Event {
    fn into(self) -> EventWithoutEntity {
        let attrs = self
            .attrs
            .map(|attrs| serde_json::to_string(&attrs).unwrap().into());
        EventWithoutEntity {
            event_type: self.event_type,
            event_time: self.event_time,
            event_id: self.event_id,
            experiment_id: self.experiment_id,
            attrs,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct EventEntity {
    pub event_id: SmallString,
    pub entity: Entity,
}

impl Into<Vec<EventEntity>> for Event {
    fn into(self) -> Vec<EventEntity> {
        self.entities()
            .iter()
            .map(|entity| EventEntity {
                event_id: self.event_id.as_ref().unwrap().clone(),
                entity: entity.clone(),
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use crate::map::HashMap;
    use crate::sstring::SmallString;
    use chrono::{NaiveDate, NaiveDateTime, Utc};

    use super::*;

    #[test]
    pub fn test_serialization() {
        let event = Event {
            event_type: EventType("".into()),
            event_time: Utc::now().naive_utc(),
            entities: btreemap!["location".into() => "a".into()],
            event_id: None,
            experiment_id: None,
            attrs: Some(hashmap!(
                a!("numeric_attribute") => Value::Num(1.0),
                a!("none") => Value::None
            )),
        };

        let json_attrs = event.json_serialize_attributes().unwrap();
        let attrs = event
            .json_deserialize_attributes(json_attrs.as_str())
            .unwrap();
        assert_eq!(
            attrs.get("numeric_attribute").unwrap().clone(),
            Value::Num(1.0)
        );
        assert_eq!(attrs.get("none").unwrap().clone(), Value::None);
    }

    #[test]
    fn test_extract_attribute_single() {
        let mut attrs = HashMap::new();
        attrs.insert(a!("key1"), Value::Str("value1".into()));

        let event = Event {
            event_type: EventType("test".into()),
            event_time: Utc::now().naive_utc(),
            entities: btreemap!["location".into() => "a".into()],
            event_id: None,
            experiment_id: None,
            attrs: Some(attrs),
        };

        let attribute = AttributeKey::from_str("key1").unwrap();
        let extracted = event.extract_attribute(&attribute).unwrap();

        match extracted {
            Value::Str(s) => assert_eq!(s, "value1"),
            _ => panic!("Unexpected Value type"),
        }
    }

    #[test]
    fn test_extract_attribute_nested() {
        let mut inner_map = HashMap::new();
        inner_map.insert(a!("key2"), Box::new(Value::Str("value2".into())));

        let mut outer_map = HashMap::new();
        outer_map.insert(a!("key1"), Value::Map(inner_map));

        let event = Event {
            event_type: EventType("test".into()),
            event_time: Utc::now().naive_utc(),
            entities: BTreeMap::new(),
            event_id: None,
            experiment_id: None,
            attrs: Some(outer_map),
        };

        let attribute = AttributeKey::from_str("key1.key2").unwrap();
        let extracted = event.extract_attribute(&attribute).unwrap();

        match extracted {
            Value::Str(s) => assert_eq!(s, "value2"),
            _ => panic!("Unexpected Value type"),
        }
    }

    #[test]
    fn test_extract_attribute_triple_nested() {
        let mut inner_inner_map = HashMap::new();
        inner_inner_map.insert(a!("key3"), Box::new(Value::Str("value3".into())));

        let mut inner_map = HashMap::new();
        inner_map.insert(a!("key2"), Box::new(Value::Map(inner_inner_map)));

        let mut outer_map = HashMap::new();
        outer_map.insert(a!("key1"), Value::Map(inner_map));

        let event = Event {
            event_type: EventType("test".into()),
            event_time: Utc::now().naive_utc(),
            entities: BTreeMap::new(),
            event_id: None,
            experiment_id: None,
            attrs: Some(outer_map),
        };

        let attribute = AttributeKey::from_str("key1.key2.key3").unwrap();
        let extracted = event.extract_attribute(&attribute).unwrap();

        match extracted {
            Value::Str(s) => assert_eq!(s, "value3"),
            _ => panic!("Unexpected Value type"),
        }
    }

    #[test]
    fn test_value_type_extraction_bool() {
        let attrs_bool = Some(hashmap! {
            a!("bool_attr") => Value::Bool(true)
        });
        let event_bool = Event {
            attrs: attrs_bool,
            ..Default::default()
        };
        assert_eq!(
            event_bool.extract_attributes_value_types(),
            hashmap! { a!("bool_attr") => ValueType::Bool }
        );
    }

    #[test]
    fn test_value_type_extraction_num() {
        let attrs_num = Some(hashmap! {
            a!("num_attr") => Value::Num(1.0)
        });
        let event_num = Event {
            attrs: attrs_num,
            ..Default::default()
        };
        assert_eq!(
            event_num.extract_attributes_value_types(),
            hashmap! { a!("num_attr") => ValueType::Num }
        );
    }

    #[test]
    fn test_value_type_extraction_map() {
        let mut inner_map = HashMap::new();
        inner_map.insert(
            a!("nested_attr"),
            Box::new(Value::Str(from_string!("nested".to_string()))),
        );
        let attrs_map = Some(hashmap! {
            a!("map_attr") => Value::Map(inner_map)
        });
        let event_map = Event {
            attrs: attrs_map,
            ..Default::default()
        };
        assert_eq!(
            event_map.extract_attributes_value_types(),
            hashmap! { a!("map_attr.nested_attr") => ValueType::Str }
        );
    }

    #[test]
    fn test_event_parse() {
        let json = r#"{"entities": {"user": "a"}, "event_time": "2020-01-01T16:39:57", "event_type": "pressure", "attrs": {"pressure": 10.0}, "event_id": "1"}"#;
        let event: Event = serde_json::from_str(json).unwrap();
        let json = r#"{"entities": {"user": "a"}, "event_time": "2020-01-01 16:39:57", "event_type": "pressure", "attrs": {"pressure": 10.0}, "event_id": "1"}"#;
        let event: Event = serde_json::from_str(json).unwrap();
        let json = r#"{"entities": {"user": "a"}, "event_time": "2020-01-01 16:39", "event_type": "pressure", "attrs": {"pressure": 10.0}, "event_id": "1"}"#;
        let event: Event = serde_json::from_str(json).unwrap();
        let json = r#"{"entities": {"user": "a"}, "event_time": "2020-01-01", "event_type": "pressure", "attrs": {"pressure": 10.0}, "event_id": "1"}"#;
        let event: Event = serde_json::from_str(json).unwrap();
    }
}
