pub mod encoded_column;
pub mod encoding;
pub mod evaluation;
mod logical_plan;
pub mod raw_column;

use paste::paste;
use std::collections::BTreeMap;
use std::collections::Bound::{Included, Unbounded};
use std::fmt::Debug;
use std::hash::Hash;

use anyhow::{anyhow, bail, Result};
use chrono::{DateTime, NaiveDate, NaiveDateTime, Utc};
use itertools::Itertools;
use ordered_float::OrderedFloat;
use vec1::{vec1, Vec1};

use crate::event::{AttributeName, Event, EventType};
use crate::event_store::column_event_store::encoded_column::{ColumnVecType, EncodedColumnVec};
use crate::event_store::column_event_store::raw_column::{RawColumnVec, RawColumnVecGen};
use crate::map::HashMap;
use crate::sstring::SmallString;
use crate::types::{Timestamp, FLOAT, INT};
use crate::value::{Value, ValueType};

// This represents the type including nullability (stored in the block)
#[derive(Debug, Clone)]
pub enum AnyColumnDataTypeWithNull {
    Bool(ColumnVecType),
    Num(ColumnVecType),
    Int(ColumnVecType),
    Str(ColumnVecType),
    VecBool(ColumnVecType),
    VecNum(ColumnVecType),
    VecInt(ColumnVecType),
    VecStr(ColumnVecType),
    Date(ColumnVecType),
    DateTime(ColumnVecType),
}

// This represents the type excluding nullability (stored in the table)
#[derive(Debug, Clone, PartialEq)]
pub enum AnyColumnDataType {
    Bool,
    Num,
    Int,
    Str,
    VecBool,
    VecNum,
    VecInt,
    VecStr,
    Date,
    DateTime,
    NotSupported,
}

impl From<ValueType> for AnyColumnDataType {
    fn from(value: ValueType) -> Self {
        match value {
            ValueType::Bool => AnyColumnDataType::Bool,
            ValueType::Num => AnyColumnDataType::Num,
            ValueType::Int => AnyColumnDataType::Int,
            ValueType::MapNum => AnyColumnDataType::NotSupported,
            ValueType::MapStr => AnyColumnDataType::NotSupported,
            ValueType::Str => AnyColumnDataType::Str,
            ValueType::Date => AnyColumnDataType::Date,
            ValueType::DateTime => AnyColumnDataType::DateTime,
            ValueType::VecCat => AnyColumnDataType::VecStr,
            ValueType::VecNum => AnyColumnDataType::VecNum,
            ValueType::VecInt => AnyColumnDataType::VecInt,
            ValueType::VecBool => AnyColumnDataType::VecBool,
            ValueType::Map => AnyColumnDataType::NotSupported,
            ValueType::None => AnyColumnDataType::NotSupported,
            ValueType::Wildcard => AnyColumnDataType::NotSupported,
            ValueType::NotCalculatedYet => AnyColumnDataType::NotSupported,
        }
    }
}

fn is_value_supported(value_type: &ValueType) -> bool {
    match value_type {
        ValueType::MapNum => false,
        ValueType::MapStr => false,
        ValueType::Map => false,
        ValueType::None => false,
        ValueType::Wildcard => false,
        ValueType::NotCalculatedYet => false,
        _ => true,
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum ColumnData {
    Raw(RawColumnVec),
    Encoded(EncodedColumnVec),
}

pub fn option_vec_prepend_empty<T: Clone>(n_empty: usize, value: T) -> Vec<Option<T>> {
    let mut vec = vec![None; n_empty];
    vec.push(Some(value));
    vec
}

impl ColumnData {
    pub fn new_from_value(value: Value, n_empty: usize) -> Result<ColumnData> {
        match value {
            Value::None => bail!("Cannot create column from {:?}", value),
            Value::Wildcard => bail!("Cannot create column from {:?}", value),
            Value::Bool(v) => Ok(ColumnData::Raw(RawColumnVec::Bool(
                RawColumnVecGen::Nullable(option_vec_prepend_empty(n_empty, v)),
            ))),
            Value::Num(v) => Ok(ColumnData::Raw(RawColumnVec::Num(
                RawColumnVecGen::Nullable(option_vec_prepend_empty(n_empty, v.into())),
            ))),
            Value::Int(v) => Ok(ColumnData::Raw(RawColumnVec::Int(
                RawColumnVecGen::Nullable(option_vec_prepend_empty(n_empty, v)),
            ))),
            Value::Str(v) => Ok(ColumnData::Raw(RawColumnVec::Str(
                RawColumnVecGen::Nullable(option_vec_prepend_empty(n_empty, v)),
            ))),
            Value::MapNum(v) => bail!("Cannot create column from {:?}", v),
            Value::MapStr(v) => bail!("Cannot create column from {:?}", v),
            Value::VecBool(v) => Ok(ColumnData::Raw(RawColumnVec::VecBool(
                RawColumnVecGen::Nullable(option_vec_prepend_empty(n_empty, v)),
            ))),
            Value::VecNum(v) => {
                let vec = v.iter().map(|v| (*v).into()).collect_vec();
                Ok(ColumnData::Raw(RawColumnVec::VecNum(
                    RawColumnVecGen::Nullable(option_vec_prepend_empty(n_empty, vec)),
                )))
            }
            Value::VecInt(v) => Ok(ColumnData::Raw(RawColumnVec::VecInt(
                RawColumnVecGen::Nullable(option_vec_prepend_empty(n_empty, v)),
            ))),
            Value::VecStr(v) => Ok(ColumnData::Raw(RawColumnVec::VecStr(
                RawColumnVecGen::Nullable(option_vec_prepend_empty(n_empty, v)),
            ))),
            Value::Date(v) => Ok(ColumnData::Raw(RawColumnVec::Date(
                RawColumnVecGen::Nullable(option_vec_prepend_empty(n_empty, v)),
            ))),
            Value::DateTime(v) => Ok(ColumnData::Raw(RawColumnVec::DateTime(
                RawColumnVecGen::Nullable(option_vec_prepend_empty(n_empty, v)),
            ))),
            Value::Map(v) => bail!("Cannot create column from {:?}", v),
            Value::ValueWithAlias(v) => bail!("Cannot create column from {:?}", v),
            Value::NotCalculatedYet => {
                bail!("Cannot create column from {:?}", Value::NotCalculatedYet)
            }
        }
    }

    pub fn new_from_type(typ: &AnyColumnDataType, n: usize) -> Result<ColumnData> {
        if n == 0 {
            match typ {
                AnyColumnDataType::Bool => Ok(ColumnData::Raw(RawColumnVec::Bool(
                    RawColumnVecGen::NonNullable(vec![]),
                ))),
                AnyColumnDataType::Num => Ok(ColumnData::Raw(RawColumnVec::Num(
                    RawColumnVecGen::NonNullable(vec![]),
                ))),
                AnyColumnDataType::Int => Ok(ColumnData::Raw(RawColumnVec::Int(
                    RawColumnVecGen::NonNullable(vec![]),
                ))),
                AnyColumnDataType::Str => Ok(ColumnData::Raw(RawColumnVec::Str(
                    RawColumnVecGen::NonNullable(vec![]),
                ))),
                AnyColumnDataType::VecBool => Ok(ColumnData::Raw(RawColumnVec::VecBool(
                    RawColumnVecGen::NonNullable(vec![]),
                ))),
                AnyColumnDataType::VecNum => Ok(ColumnData::Raw(RawColumnVec::VecNum(
                    RawColumnVecGen::NonNullable(vec![]),
                ))),
                AnyColumnDataType::VecInt => Ok(ColumnData::Raw(RawColumnVec::VecInt(
                    RawColumnVecGen::NonNullable(vec![]),
                ))),
                AnyColumnDataType::VecStr => Ok(ColumnData::Raw(RawColumnVec::VecStr(
                    RawColumnVecGen::NonNullable(vec![]),
                ))),
                AnyColumnDataType::Date => Ok(ColumnData::Raw(RawColumnVec::Date(
                    RawColumnVecGen::NonNullable(vec![]),
                ))),
                AnyColumnDataType::DateTime => Ok(ColumnData::Raw(RawColumnVec::DateTime(
                    RawColumnVecGen::NonNullable(vec![]),
                ))),
                AnyColumnDataType::NotSupported => {
                    bail!("ColumnData type cannot be created as a new column")
                }
            }
        } else {
            match typ {
                AnyColumnDataType::Bool => Ok(ColumnData::Raw(RawColumnVec::Bool(
                    RawColumnVecGen::Nullable(vec![None; n]),
                ))),
                AnyColumnDataType::Num => Ok(ColumnData::Raw(RawColumnVec::Num(
                    RawColumnVecGen::Nullable(vec![None; n]),
                ))),
                AnyColumnDataType::Int => Ok(ColumnData::Raw(RawColumnVec::Int(
                    RawColumnVecGen::Nullable(vec![None; n]),
                ))),
                AnyColumnDataType::Str => Ok(ColumnData::Raw(RawColumnVec::Str(
                    RawColumnVecGen::Nullable(vec![None; n]),
                ))),
                AnyColumnDataType::VecBool => Ok(ColumnData::Raw(RawColumnVec::VecBool(
                    RawColumnVecGen::Nullable(vec![None; n]),
                ))),
                AnyColumnDataType::VecNum => Ok(ColumnData::Raw(RawColumnVec::VecNum(
                    RawColumnVecGen::Nullable(vec![None; n]),
                ))),
                AnyColumnDataType::VecInt => Ok(ColumnData::Raw(RawColumnVec::VecInt(
                    RawColumnVecGen::Nullable(vec![None; n]),
                ))),
                AnyColumnDataType::VecStr => Ok(ColumnData::Raw(RawColumnVec::VecStr(
                    RawColumnVecGen::Nullable(vec![None; n]),
                ))),
                AnyColumnDataType::Date => Ok(ColumnData::Raw(RawColumnVec::Date(
                    RawColumnVecGen::Nullable(vec![None; n]),
                ))),
                AnyColumnDataType::DateTime => Ok(ColumnData::Raw(RawColumnVec::DateTime(
                    RawColumnVecGen::Nullable(vec![None; n]),
                ))),
                AnyColumnDataType::NotSupported => {
                    bail!("ColumnData type cannot be created as a new column")
                }
            }
        }
    }

    pub fn push_none(&mut self) -> Result<()> {
        match self {
            ColumnData::Raw(raw) => raw.push_none()?,
            ColumnData::Encoded(enc) => {
                self.decode();
                self.push_none()?;
            }
        }
        Ok(())
    }

    pub fn push_value(&mut self, value: Value) -> Result<()> {
        match self {
            ColumnData::Raw(raw) => {
                raw.push_value(value)?;
            }
            ColumnData::Encoded(enc) => {
                self.decode();
                self.push_value(value)?;
            }
        }
        Ok(())
    }

    pub fn encode(&mut self) -> bool {
        match self {
            ColumnData::Raw(raw) => {
                if let Some(encoded) = raw.encode().ok() {
                    *self = ColumnData::Encoded(encoded);
                    true
                } else {
                    false
                }
            }
            ColumnData::Encoded(enc) => true,
        }
    }

    pub fn decode(&mut self) {
        match self {
            ColumnData::Raw(_) => {}
            ColumnData::Encoded(enc) => *self = ColumnData::Raw(enc.decode()),
        }
    }
}

#[derive(Debug)]
pub struct Block {
    start_time: Timestamp,
    end_time: Timestamp,
    columns: HashMap<String, ColumnData>,
    n_rows: usize,
    index_by_event_time: BTreeMap<Timestamp, usize>,
    // I think that if some passes from the last insertion
    // the block can be compressed
    last_insertion_time: DateTime<Utc>,
}

impl Block {
    /*
    The algorithm for inserting a new event:
    1. Iterate through attributes. Nested attributes create column names
    separated by a dot. Check if the value type is proper for the
    AnyColumnData type. If it is not return a Result with an appropriate
    anyhow context. One exception is Value::None. Value::None
    maps to None when the ColumnData is nullable. When it is not
    the type of the columndata must change to accomodate null value.
    Basically map Vec<T> to Vec<Option<T>>.
    2. Check if the columns for standard event data exist:
    - event_time
    - entities - each entity maps to a column entity.[entity_type]
    - event_id
    3. Note that blocks must inherit the same schema. They may not have
    all the columns that exist in other blocks but their common schema
    is the same with the exception of nullable or not type.
    */
    fn insert_new_event_incremental(
        &mut self,
        event: &Event,
        settings: &Settings,
        table_schema: &mut HashMap<String, AnyColumnDataType>,
    ) -> Result<()> {
        self.end_time = event.event_time;
        self.last_insertion_time = Utc::now();
        let attribute_values = event.extract_attributes_values();

        Self::check_schema(table_schema, &attribute_values)?;
        self.fill_common_event_attributes(&event)?;
        self.insert_event_attributes(event, table_schema, attribute_values)?;

        // whatever happens if we are here it means that the event was added
        self.index_by_event_time
            .insert(event.event_time.clone(), self.n_rows);
        self.n_rows += 1;

        Ok(())
    }

    fn insert_event_attributes(
        &mut self,
        event: &Event,
        table_schema: &mut HashMap<String, AnyColumnDataType>,
        attribute_values: HashMap<AttributeName, Value>,
    ) -> Result<()> {
        for (attr_name, attr_value) in attribute_values.into_iter() {
            let attr_value_type: ValueType = attr_value.clone().into();
            let any_column_data_type: AnyColumnDataType = attr_value_type.clone().into();

            // add the value type to the table_schema only if it doesn't exist and it is not None
            if !table_schema.contains_key(attr_name.as_str()) && attr_value_type != ValueType::None
            {
                table_schema.insert(attr_name.0.clone(), any_column_data_type);
            }

            // exists in the schema + exists as an attribute
            if is_value_supported(&attr_value_type) {
                if let Some(column_data_type) = table_schema.get(&attr_name.0) {
                    // column exists
                    if let Some(column) = self.columns.get_mut(attr_name.as_str()) {
                        if attr_value_type == ValueType::None {
                            column.push_none()?;
                        } else {
                            column.push_value(attr_value)?;
                        }
                    } else {
                        // create n-1 rows for nullable type
                        if self.n_rows > 0 {
                            self.columns.insert(
                                attr_name.0.clone(),
                                ColumnData::new_from_type(column_data_type, self.n_rows)?,
                            );
                        } else {
                            self.columns.insert(
                                attr_name.0.clone(),
                                ColumnData::new_from_type(column_data_type, 0)?,
                            );
                        }
                        let column = self
                            .columns
                            .get_mut(&attr_name.0)
                            .ok_or(anyhow!("Cannot extract newly created column"))?;
                        if attr_value_type == ValueType::None {
                            column.push_none()?;
                        } else {
                            column.push_value(attr_value)?;
                        }
                    }
                }
            }
        }

        Ok(())
    }

    fn fill_common_event_attributes(&mut self, event: &Event) -> Result<()> {
        if self.n_rows == 0 {
            self.columns.insert(
                "event_time".into(),
                ColumnData::Raw(RawColumnVec::DateTime(RawColumnVecGen::NonNullable(vec![
                    event.event_time,
                ]))),
            );
            self.columns.insert(
                "event_id".into(),
                ColumnData::Raw(RawColumnVec::Str(RawColumnVecGen::Nullable(vec![event
                    .event_id
                    .clone()]))),
            );
            for (entity_type, entity_id) in &event.entities {
                let column_name = format!("entity.{}", entity_type.0);
                self.columns.insert(
                    column_name,
                    ColumnData::Raw(RawColumnVec::Str(RawColumnVecGen::Nullable(vec![Some(
                        entity_id.0.clone(),
                    )]))),
                );
            }
        } else {
            let event_time_column = self
                .columns
                .get_mut("event_time")
                .ok_or(anyhow!("Cannot extract the column which should be there"))?;
            let event_time_value = Value::DateTime(event.event_time);
            event_time_column.push_value(event_time_value)?;

            let event_id_column = self
                .columns
                .get_mut("event_id")
                .ok_or(anyhow!("Cannot extract the column which should be there"))?;
            let event_id_value = match &event.event_id {
                Some(id) => Value::Str(id.clone()),
                None => Value::None,
            };
            event_id_column.push_value(event_id_value)?;

            for (entity_type, entity_id) in &event.entities {
                let column_name = format!("entity.{}", entity_type.0);
                let entity_column = self
                    .columns
                    .get_mut(&column_name)
                    .ok_or(anyhow!("Cannot extract the column which should be there"))?;
                let entity_id_value = Value::Str(entity_id.0.clone());
                entity_column.push_value(entity_id_value)?;
            }
        }
        Ok(())
    }

    fn check_schema(
        table_schema: &mut HashMap<String, AnyColumnDataType>,
        attribute_values: &HashMap<AttributeName, Value>,
    ) -> Result<()> {
        for (attr_name, attr_value) in attribute_values {
            let attr_value_type: ValueType = (*attr_value).clone().into();
            if is_value_supported(&attr_value_type) {
                let attr_value_column_data_type: AnyColumnDataType = attr_value_type.clone().into();
                if let Some(existing_column_data_type) = table_schema.get(attr_name.as_str()) {
                    if *existing_column_data_type != attr_value_column_data_type
                        && attr_value_type != ValueType::None
                    {
                        bail!(
                            "The type for {:?} ({:?}) is not matching the previous type {:?}",
                            attr_name,
                            attr_value_column_data_type,
                            existing_column_data_type
                        );
                    }
                }
            }
        }
        Ok(())
    }

    pub fn encode(&mut self) -> Result<()> {
        for (_, column) in self.columns.iter_mut() {
            column.encode();
        }
        Ok(())
    }

    pub fn decode(&mut self) -> Result<()> {
        for (_, column) in self.columns.iter_mut() {
            column.decode()
        }
        Ok(())
    }

    pub fn make_projection(&self, columns: Vec<String>) -> Result<Block> {
        let mut selected_columns = HashMap::new();
        for column in &columns {
            let mut new_column = self
                .columns
                .get(column)
                .ok_or(anyhow!("Cannot extract column {} from the block", column))?
                .clone();
            new_column.decode();
            selected_columns.insert(column.clone(), new_column);
        }
        Ok(Block {
            start_time: self.start_time,
            end_time: self.end_time,
            columns: selected_columns,
            n_rows: self.n_rows,
            index_by_event_time: self.index_by_event_time.clone(),
            last_insertion_time: self.last_insertion_time,
        })
    }
}

#[derive(Debug)]
pub struct Table {
    name: String,
    schema: HashMap<String, AnyColumnDataType>,
    blocks: BTreeMap<Timestamp, Vec1<Block>>, // there can be more than 1 block with exact the same timestamp
}

impl Table {
    /*
    1. Find a block to insert the new event. If the found block exceeds
    settings.block_size create a new block. If the block has the same timestamp
    as the event add the new block to the vec1<block> with the same timestamp.
    If the timestamp is different find the next block. If the next block
    also exceeds settings.block_size create a new block between the 2 blocks with
    event timestamp as the key.
    2. Delegate inserting new event to the block
     */
    fn insert_new_event_incremental(&mut self, event: &Event, settings: &Settings) -> Result<()> {
        // Find the block where the event should be inserted.
        // If necessary, create a new block.
        // Delegate the insertion to the block.
        let event_timestamp = event.event_time;

        /*
        Blocks with the same timestamp as the event and holding exactly the same timestamp is
        an easy situation. I just squeeze in the event in the set of blocks.
         */

        if let Some((_, inner_blocks)) = self.blocks.range_mut(..).next_back() {
            if inner_blocks.last().n_rows < settings.block_size {
                inner_blocks.last_mut().insert_new_event_incremental(
                    event,
                    settings,
                    &mut self.schema,
                )
            } else {
                // TODO: compress previous block
                let mut last_block = inner_blocks.last_mut();
                last_block.encode();
                self.create_new_block_with_event(event, settings, event_timestamp)
            }
        } else {
            self.create_new_block_with_event(event, settings, event_timestamp)
        }
    }

    fn create_new_block_with_event(
        &mut self,
        event: &Event,
        settings: &Settings,
        event_timestamp: Timestamp,
    ) -> Result<()> {
        let mut new_block = Block {
            start_time: event_timestamp.clone(),
            end_time: event_timestamp.clone(),
            columns: Default::default(),
            n_rows: 0,
            index_by_event_time: Default::default(),
            last_insertion_time: Utc::now(),
        };
        new_block.insert_new_event_incremental(event, settings, &mut self.schema)?;
        self.blocks
            .insert(event_timestamp.clone(), vec1![new_block]);
        Ok(())
    }

    pub fn make_projection(&self, columns: Vec<String>) -> Result<Table> {
        let mut new_schema = HashMap::new();
        for column in columns {
            new_schema.insert(
                column.clone(),
                self.schema
                    .get(&column)
                    .ok_or(anyhow!("Cannot extract column {} from schema", column))?
                    .clone(),
            );
        }
        Ok(Table {
            name: self.name.clone(),
            schema: new_schema,
            blocks: Default::default(),
        })
    }
}

#[derive(Clone, Debug)]
pub struct Settings {
    pub block_size: usize,
    pub enable_compression: bool,
}

#[derive(Debug)]
pub struct ColumnStore {
    pub tables: HashMap<String, Table>,
    pub settings: Settings,
    pub last_timestamp: Timestamp,
}

impl ColumnStore {
    pub fn new(settings: Settings) -> Self {
        ColumnStore {
            tables: HashMap::new(),
            settings,
            last_timestamp: Timestamp::MIN,
        }
    }

    /*
    The algorithm for inserting a new event:
    1. event.event_type maps to a table name
    2. If a table does not exist create it
    3. Delegate inserting the event to the table

    For now only accepts timestamps that are greater or equal than the last ingested
    timestamp. This is because ingesting timestamps in the middle of the blocks would require
    a lot of book keeping.
     */
    pub fn insert_new_event_incremental_incremental(&mut self, event: &Event) -> Result<()> {
        if event.event_time < self.last_timestamp {
            bail!("Can only ingest timestamps that are greater or equal than the last timestamp");
        }

        // Map event type to table name.
        // Create the table if it does not exist
        let table_name = event.event_type.0.clone();
        if !self.tables.contains_key(&table_name) {
            self.tables.insert(
                table_name.clone(),
                Table {
                    name: table_name.clone(),
                    schema: Default::default(),
                    blocks: Default::default(),
                },
            );
        }

        // Get or create the table for the event type.
        let table = match self.tables.get_mut(&table_name) {
            Some(table) => table,
            None => {
                return Err(anyhow!(
                    "Table not found for event type '{}'",
                    event.event_type
                ));
            }
        };

        // Delegate the insertion to the table.
        table.insert_new_event_incremental(event, &self.settings)?;
        self.last_timestamp = event.event_time.clone();
        Ok(())
    }

    /*
    Creating the projection of a column store is just selecting the right tables
    and columns and uncompressing them. Alternatively it could be done by uncompressing
    the columns of the original column store.
    */
    pub fn make_projection(
        &self,
        tables_columns: HashMap<String, Vec<String>>,
    ) -> Result<ColumnStore> {
        let mut new_tables = HashMap::new();
        for (table, columns) in tables_columns.iter() {
            let old_table = self
                .tables
                .get(table)
                .ok_or(anyhow!("cannot find table {}", table))?;
            let new_table = old_table.make_projection(columns.clone())?;
            new_tables.insert(table.clone(), new_table);
        }
        let mut new_settings = self.settings.clone();
        new_settings.enable_compression = false;
        Ok(ColumnStore {
            tables: new_tables,
            settings: new_settings,
            last_timestamp: self.last_timestamp,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event_store::column_event_store::encoded_column::NullableEncodedColumnVec;
    use crate::event_store::column_event_store::encoding::rle::RunLengthEncodedVecOptionGen;
    use crate::tests::fake_nba::generate_nba_game_events;
    use chrono::Duration;
    use std::ops::Add;

    #[test]
    fn test() {
        let rle_vec = NullableEncodedColumnVec::RunLengthEncodedVec(
            RunLengthEncodedVecOptionGen {
                values: vec![Some(true), None, Some(false)],
                lengths: vec![3, 2, 1],
            }
            .into(),
        );

        let decoded_bool = rle_vec.decode();

        println!("{:?}", decoded_bool);
    }

    #[test]
    fn test_nba() {
        let mut events = generate_nba_game_events(100);
        let mut column_store = ColumnStore {
            tables: Default::default(),
            settings: Settings {
                block_size: 10,
                enable_compression: true,
            },
            last_timestamp: NaiveDateTime::MIN,
        };
        events.sort_by_key(|e| e.event_time);
        for event in &events {
            column_store
                .insert_new_event_incremental_incremental(event)
                .unwrap();
        }
        println!("{:#?}", column_store);
    }

    /*
    What we want here is to assure that if the value is first None, and then is not None
    All the previous values will be filled
     */
    #[test]
    fn test_none_backfilling() {
        let dt1 =
            NaiveDateTime::parse_from_str("2023-01-01 00:00:00", "%Y-%m-%d %H:%M:%S").unwrap();
        let dt2 =
            NaiveDateTime::parse_from_str("2023-01-02 00:00:00", "%Y-%m-%d %H:%M:%S").unwrap();
        let mut events = vec![
            Event {
                event_type: EventType("game".into()),
                event_time: dt1.clone(),
                entities: Default::default(),
                event_id: None,
                experiment_id: None,
                attrs: Some(hashmap![a!("test") => Value::None]),
            },
            Event {
                event_type: EventType("game".into()),
                event_time: dt2.clone(),
                entities: Default::default(),
                event_id: None,
                experiment_id: None,
                attrs: Some(hashmap![a!("test") => Value::Int(1)]),
            },
        ];
        let mut column_store = ColumnStore {
            tables: Default::default(),
            settings: Settings {
                block_size: 10,
                enable_compression: true,
            },
            last_timestamp: NaiveDateTime::MIN,
        };
        events.sort_by_key(|e| e.event_time);
        for event in &events {
            column_store
                .insert_new_event_incremental_incremental(event)
                .unwrap();
        }
        assert_eq!(
            column_store
                .tables
                .get("game")
                .unwrap()
                .blocks
                .first_key_value()
                .unwrap()
                .1
                .last()
                .n_rows,
            2
        );
        let column_data = ColumnData::Raw(RawColumnVec::Int(RawColumnVecGen::Nullable(vec![
            None,
            Some(1),
        ])));
        assert_eq!(
            column_store
                .tables
                .get("game")
                .unwrap()
                .blocks
                .first_key_value()
                .unwrap()
                .1
                .last()
                .columns
                .get("test")
                .unwrap()
                .clone(),
            column_data
        );
    }

    /*
    Similar to the previous test. The result should be the same even if the attribute is not provided.
     */
    #[test]
    fn test_none_backfilling_2() {
        let dt1 =
            NaiveDateTime::parse_from_str("2023-01-01 00:00:00", "%Y-%m-%d %H:%M:%S").unwrap();
        let dt2 =
            NaiveDateTime::parse_from_str("2023-01-02 00:00:00", "%Y-%m-%d %H:%M:%S").unwrap();
        let mut events = vec![
            Event {
                event_type: EventType("game".into()),
                event_time: dt1.clone(),
                entities: Default::default(),
                event_id: None,
                experiment_id: None,
                attrs: None,
            },
            Event {
                event_type: EventType("game".into()),
                event_time: dt2.clone(),
                entities: Default::default(),
                event_id: None,
                experiment_id: None,
                attrs: Some(hashmap![a!("test") => Value::Int(1)]),
            },
        ];
        let mut column_store = ColumnStore {
            tables: Default::default(),
            settings: Settings {
                block_size: 10,
                enable_compression: true,
            },
            last_timestamp: NaiveDateTime::MIN,
        };
        events.sort_by_key(|e| e.event_time);
        for event in &events {
            column_store
                .insert_new_event_incremental_incremental(event)
                .unwrap();
        }
        assert_eq!(
            column_store
                .tables
                .get("game")
                .unwrap()
                .blocks
                .first_key_value()
                .unwrap()
                .1
                .last()
                .n_rows,
            2
        );
        let column_data = ColumnData::Raw(RawColumnVec::Int(RawColumnVecGen::Nullable(vec![
            None,
            Some(1),
        ])));
        assert_eq!(
            column_store
                .tables
                .get("game")
                .unwrap()
                .blocks
                .first_key_value()
                .unwrap()
                .1
                .last()
                .columns
                .get("test")
                .unwrap()
                .clone(),
            column_data
        );
    }

    /*
    When all the values are provided then the type of column data should be nonnu
    */
    #[test]
    fn test_nonnullable_columns() {
        let dt1 =
            NaiveDateTime::parse_from_str("2023-01-01 00:00:00", "%Y-%m-%d %H:%M:%S").unwrap();
        let dt2 =
            NaiveDateTime::parse_from_str("2023-01-02 00:00:00", "%Y-%m-%d %H:%M:%S").unwrap();
        let mut events = vec![
            Event {
                event_type: EventType("game".into()),
                event_time: dt1.clone(),
                entities: Default::default(),
                event_id: None,
                experiment_id: None,
                attrs: Some(hashmap![a!("test") => Value::Int(0)]),
            },
            Event {
                event_type: EventType("game".into()),
                event_time: dt2.clone(),
                entities: Default::default(),
                event_id: None,
                experiment_id: None,
                attrs: Some(hashmap![a!("test") => Value::Int(1)]),
            },
        ];
        let mut column_store = ColumnStore {
            tables: Default::default(),
            settings: Settings {
                block_size: 10,
                enable_compression: true,
            },
            last_timestamp: NaiveDateTime::MIN,
        };
        events.sort_by_key(|e| e.event_time);
        for event in &events {
            column_store
                .insert_new_event_incremental_incremental(event)
                .unwrap();
        }
        assert_eq!(
            column_store
                .tables
                .get("game")
                .unwrap()
                .blocks
                .first_key_value()
                .unwrap()
                .1
                .last()
                .n_rows,
            2
        );
        let column_data =
            ColumnData::Raw(RawColumnVec::Int(RawColumnVecGen::NonNullable(vec![0, 1])));
        assert_eq!(
            column_store
                .tables
                .get("game")
                .unwrap()
                .blocks
                .first_key_value()
                .unwrap()
                .1
                .last()
                .columns
                .get("test")
                .unwrap()
                .clone(),
            column_data
        );
    }

    #[test]
    fn test_invalid_timestamp() {
        let dt1 =
            NaiveDateTime::parse_from_str("2023-01-02 00:00:00", "%Y-%m-%d %H:%M:%S").unwrap();
        let dt2 =
            NaiveDateTime::parse_from_str("2023-01-01 00:00:00", "%Y-%m-%d %H:%M:%S").unwrap();
        let mut events = vec![
            Event {
                event_type: EventType("game".into()),
                event_time: dt1.clone(),
                entities: Default::default(),
                event_id: None,
                experiment_id: None,
                attrs: Some(hashmap![a!("test") => Value::Int(0)]),
            },
            Event {
                event_type: EventType("game".into()),
                event_time: dt2.clone(),
                entities: Default::default(),
                event_id: None,
                experiment_id: None,
                attrs: Some(hashmap![a!("test") => Value::Int(1)]),
            },
        ];

        let mut column_store = ColumnStore {
            tables: Default::default(),
            settings: Settings {
                block_size: 10,
                enable_compression: true,
            },
            last_timestamp: NaiveDateTime::MIN,
        };

        // The first event should be inserted without any issues.
        column_store
            .insert_new_event_incremental_incremental(&events[0])
            .unwrap();

        // The second event has an earlier timestamp, so it should throw an error.
        let result = column_store.insert_new_event_incremental_incremental(&events[1]);
        assert!(result.is_err());
    }

    #[test]
    fn test_mixed_types_should_fail() {
        let dt1 =
            NaiveDateTime::parse_from_str("2023-01-01 00:00:00", "%Y-%m-%d %H:%M:%S").unwrap();
        let dt2 =
            NaiveDateTime::parse_from_str("2023-01-02 00:00:00", "%Y-%m-%d %H:%M:%S").unwrap();
        let event_1 = Event {
            event_type: EventType("game".into()),
            event_time: dt1.clone(),
            entities: Default::default(),
            event_id: None,
            experiment_id: None,
            attrs: Some(hashmap![a!("test") => Value::Int(0)]),
        };
        let event_2 = Event {
            event_type: EventType("game".into()),
            event_time: dt2.clone(),
            entities: Default::default(),
            event_id: None,
            experiment_id: None,
            attrs: Some(hashmap![a!("test") => Value::Str("1".into())]),
        };
        let mut column_store = ColumnStore {
            tables: Default::default(),
            settings: Settings {
                block_size: 10,
                enable_compression: true,
            },
            last_timestamp: NaiveDateTime::MIN,
        };
        column_store
            .insert_new_event_incremental_incremental(&event_1)
            .unwrap();
        assert!(column_store
            .insert_new_event_incremental_incremental(&event_2)
            .is_err());
    }

    #[test]
    fn test_large_number_of_attributes() {
        let dt = NaiveDateTime::parse_from_str("2023-01-01 00:00:00", "%Y-%m-%d %H:%M:%S").unwrap();

        let mut attrs = HashMap::new();
        for i in 0..1000 {
            attrs.insert(AttributeName(format!("attr_{}", i)), Value::Int(i));
        }

        let event = Event {
            event_type: EventType("game".into()),
            event_time: dt.clone(),
            entities: Default::default(),
            event_id: None,
            experiment_id: None,
            attrs: Some(attrs),
        };

        let mut column_store = ColumnStore {
            tables: Default::default(),
            settings: Settings {
                block_size: 10,
                enable_compression: true,
            },
            last_timestamp: NaiveDateTime::MIN,
        };

        column_store
            .insert_new_event_incremental_incremental(&event)
            .unwrap();

        let table = column_store.tables.get("game").unwrap();
        let block = table.blocks.first_key_value().unwrap().1.last();

        for i in 0..1000 {
            let attr_name = format!("attr_{}", i);
            match block.columns.get(&attr_name).unwrap() {
                ColumnData::Raw(RawColumnVec::Int(data)) => {
                    assert_eq!(*data, RawColumnVecGen::NonNullable(vec![i]))
                }
                _ => panic!("Unexpected data"),
            }
        }
    }

    #[test]
    fn test_single_event_per_block() {
        let mut column_store = ColumnStore {
            tables: Default::default(),
            settings: Settings {
                block_size: 1,
                enable_compression: false,
            },
            last_timestamp: NaiveDateTime::MIN,
        };

        let dt1 =
            NaiveDateTime::parse_from_str("2023-01-01 00:00:00", "%Y-%m-%d %H:%M:%S").unwrap();
        let dt2 =
            NaiveDateTime::parse_from_str("2023-01-02 00:00:00", "%Y-%m-%d %H:%M:%S").unwrap();

        let events = vec![
            Event {
                event_type: EventType("game".into()),
                event_time: dt1,
                entities: Default::default(),
                event_id: None,
                experiment_id: None,
                attrs: Some(hashmap![a!("score") => Value::Int(10)]),
            },
            Event {
                event_type: EventType("game".into()),
                event_time: dt2,
                entities: Default::default(),
                event_id: None,
                experiment_id: None,
                attrs: Some(hashmap![a!("score") => Value::Int(20)]),
            },
        ];

        for event in &events {
            column_store
                .insert_new_event_incremental_incremental(event)
                .unwrap();
        }

        let game_table = column_store.tables.get("game").unwrap();

        // Assert that the number of blocks is equal to the number of events
        assert_eq!(game_table.blocks.len(), events.len());

        // Assert that each block contains exactly one event
        for (_, block) in game_table.blocks.iter() {
            assert_eq!(block.last().n_rows, 1);
        }
    }

    // tests around None handling
    fn create_column_store_with_events(events: Vec<Event>, block_size: usize) -> ColumnStore {
        let mut column_store = ColumnStore {
            tables: Default::default(),
            settings: Settings {
                block_size,
                enable_compression: true,
            },
            last_timestamp: NaiveDateTime::MIN,
        };
        for event in &events {
            column_store
                .insert_new_event_incremental_incremental(event)
                .unwrap();
        }
        column_store
    }

    #[test]
    fn test_nullable_column_with_backfilling() {
        let dt = NaiveDateTime::parse_from_str("2023-01-01 00:00:00", "%Y-%m-%d %H:%M:%S").unwrap();
        let mut events = vec![];

        // Add 3 events with None value
        for _ in 0..3 {
            events.push(Event {
                event_type: EventType("game".into()),
                event_time: dt.clone(),
                entities: Default::default(),
                event_id: None,
                experiment_id: None,
                attrs: Some(hashmap![a!("score") => Value::None]),
            });
        }

        // Add an event with a non-None value
        events.push(Event {
            event_type: EventType("game".into()),
            event_time: dt.clone(),
            entities: Default::default(),
            event_id: None,
            experiment_id: None,
            attrs: Some(hashmap![a!("score") => Value::Int(10)]),
        });

        let column_store = create_column_store_with_events(events, 10);

        let column_data = ColumnData::Raw(RawColumnVec::Int(RawColumnVecGen::Nullable(vec![
            None,
            None,
            None,
            Some(10),
        ])));
        assert_eq!(
            column_store
                .tables
                .get("game")
                .unwrap()
                .blocks
                .first_key_value()
                .unwrap()
                .1
                .last()
                .columns
                .get("score")
                .unwrap()
                .clone(),
            column_data
        );
    }

    #[test]
    fn test_all_none_values() {
        let dt = NaiveDateTime::parse_from_str("2023-01-01 00:00:00", "%Y-%m-%d %H:%M:%S").unwrap();
        let events = vec![
            Event {
                event_type: EventType("game".into()),
                event_time: dt.clone(),
                entities: Default::default(),
                event_id: None,
                experiment_id: None,
                attrs: Some(hashmap![a!("score") => Value::None]),
            },
            Event {
                event_type: EventType("game".into()),
                event_time: dt.clone(),
                entities: Default::default(),
                event_id: None,
                experiment_id: None,
                attrs: Some(hashmap![a!("score") => Value::None]),
            },
        ];

        let column_store = create_column_store_with_events(events, 10);

        assert!(column_store
            .tables
            .get("game")
            .unwrap()
            .blocks
            .first_key_value()
            .unwrap()
            .1
            .last()
            .columns
            .get("score")
            .is_none());
    }

    #[test]
    fn test_compress_previous_block() {
        let dt = NaiveDateTime::parse_from_str("2023-01-01 00:00:00", "%Y-%m-%d %H:%M:%S").unwrap();
        let events = vec![
            Event {
                event_type: EventType("game".into()),
                event_time: dt.clone(),
                entities: Default::default(),
                event_id: None,
                experiment_id: None,
                attrs: Some(hashmap![a!("score") => Value::Int(1)]),
            },
            Event {
                event_type: EventType("game".into()),
                event_time: dt.clone().add(Duration::days(1)),
                entities: Default::default(),
                event_id: None,
                experiment_id: None,
                attrs: Some(hashmap![a!("score") => Value::Int(1)]),
            },
        ];
        let column_store = create_column_store_with_events(events, 1);
        let first_block = column_store
            .tables
            .get("game")
            .unwrap()
            .blocks
            .first_key_value()
            .unwrap()
            .1
            .first();
        let column = first_block.columns.get("score").unwrap();
        assert!(matches!(ColumnData::Encoded, column));
    }

    #[test]
    fn test_no_backfilling_for_first_non_none_value() {
        let dt = NaiveDateTime::parse_from_str("2023-01-01 00:00:00", "%Y-%m-%d %H:%M:%S").unwrap();
        let events = vec![Event {
            event_type: EventType("game".into()),
            event_time: dt.clone(),
            entities: Default::default(),
            event_id: None,
            experiment_id: None,
            attrs: Some(hashmap![a!("score") => Value::Int(10)]),
        }];

        let column_store = create_column_store_with_events(events, 10);

        let column_data =
            ColumnData::Raw(RawColumnVec::Int(RawColumnVecGen::NonNullable(vec![10])));
        assert_eq!(
            column_store
                .tables
                .get("game")
                .unwrap()
                .blocks
                .first_key_value()
                .unwrap()
                .1
                .last()
                .columns
                .get("score")
                .unwrap()
                .clone(),
            column_data
        );
    }
}
