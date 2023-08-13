#![allow(clippy::unwrap_used)]

use std::fmt;
use std::fmt::{Debug, Formatter};
use std::sync::{Arc, RwLock};

use crate::ast::core::Expr;
use crate::map::{HashMap, HashSet};
use crate::sstring::SmallString;
use anyhow::Result;
use chrono::NaiveDateTime;
use postgres::{Client, NoTls, Row};

use crate::datetime_utils::parse_utc_from_str;
use crate::event::{AttributeName, Entity, Event, EventType};
use crate::event_index::{EventScopeConfig, QueryConfig};
use crate::event_store::EventStore;
use crate::interval::NaiveDateTimeInterval;
use crate::types::{Entities, EventID, Timestamp};
use crate::value::{Value, ValueType};

type AttributeMap = HashMap<SmallString, HashMap<AttributeName, ValueType>>;

#[derive(Clone, Debug)]
pub struct PostgresEventStoreConfig {
    pub host: String,
    pub port: String,
    pub username: String,
    pub password: String,
    pub db_name: String,
}

#[derive(Clone)]
pub struct PostgresEventStore {
    pub config: PostgresEventStoreConfig,
    pub client: Arc<RwLock<Client>>,
    pub attribute_map: Arc<RwLock<AttributeMap>>,
}

unsafe impl Send for PostgresEventStore {}

unsafe impl Sync for PostgresEventStore {}

impl Debug for PostgresEventStore {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("postgres_event_store")
            .field("config", &self.config)
            .finish()
    }
}

impl PostgresEventStore {
    pub fn new(config: PostgresEventStoreConfig) -> Self {
        let conn = format!(
            "host={:} user={:} password={:} dbname={:}",
            config.host, config.username, config.password, config.db_name
        );
        let client = Arc::new(RwLock::new(
            Client::connect(conn.as_str(), NoTls).expect("Cannot connect to database"),
        ));
        let store = Self {
            config,
            client,
            attribute_map: Default::default(),
        };
        store.create_tables();
        store.download_attribute_map();
        store
    }

    pub fn drop_tables(&self) {
        let mut client = self.client.write().unwrap();
        let mut transaction = client.transaction().expect("Error getting transaction");
        transaction
            .execute(r#"DROP table events_entities"#, &[])
            .expect("Cannot drop table events_entities");

        transaction
            .execute(r#"DROP table events"#, &[])
            .expect("Cannot drop table events");

        transaction
            .execute(r#"DROP table attribute_value_types"#, &[])
            .expect("Cannot create table attribute_value_types");
        transaction
            .commit()
            .expect("Cannot commit transaction with drop tables");
    }

    pub fn create_tables(&self) {
        let mut client = self.client.write().unwrap();
        let mut transaction = client.transaction().expect("Error getting transaction");
        transaction
            .execute(
                r#"
        CREATE table if not exists events (
            event_id text not null primary key,
            event_type text not null,
            event_time timestamp(3) not null,
            attrs json,
            experiment_id text null
        )"#,
                &[],
            )
            .expect("Cannot create table events");

        transaction
            .execute(
                r#"create index if not exists events_event_time on events (event_time);"#,
                &[],
            )
            .expect("Cannot create index events_event_time");

        transaction
            .execute(
                r#"create table if not exists events_entities (
            event_id text not null,
            entity_id text not null
        );"#,
                &[],
            )
            .expect("Cannot create table events_entities");

        transaction
            .execute(
                r#"ALTER TABLE events_entities ADD FOREIGN KEY (event_id)
                     REFERENCES events(event_id) ON DELETE CASCADE;"#,
                &[],
            )
            .expect("Cannot add foreign key");

        transaction.execute(r#"create index if not exists events_entities_entity_idx on events_entities (entity_id);"#,
                            &[]).expect("Cannot create index events_entities_entitiy_idx");
        transaction.execute(r#"create unique index if not exists events_entities_unique on events_entities (entity_id, event_id);"#,
                            &[]).expect("Cannot create index events_entities_unique");

        transaction
            .execute(
                r#"create table if not exists attribute_value_types (
            event_type text not null,
            attr_name text not null,
            attr_type text not null
        );"#,
                &[],
            )
            .expect("Cannot create table attribute_value_types");
        transaction.execute(r#"create unique index if not exists attribute_value_types_idx on attribute_value_types (event_type, attr_name);"#,
                            &[]).expect("Cannot create index attribute_value_types_idx");
        transaction
            .commit()
            .expect("Cannot commit transaction with the schema update");
    }

    fn row_to_event(row: Row) -> Event {
        unimplemented!()
        // let event_id: String = row.get(0);
        // let entity_id: String = row.get(1);
        // let event_type: String = row.get(2);
        // let event_time: NaiveDateTime = row.get(3);
        // let attrs: Option<HashMap<AttributeName, Value>> =
        //     serde_json::from_str(row.get(4)).expect("Cannot parse json");
        // let experiment_id: Option<String> = row.get(5);
        // Event {
        //     event_type: event_type.into(),
        //     event_time,
        //     entities: Some(vec![entity_id.into()]),
        //     event_id: Some(event_id.into()),
        //     experiment_id: experiment_id.map(|s| s.into()),
        //     attrs,
        // }
    }

    pub fn download_attribute_map(&self) {
        unimplemented!()
        // let mut attribute_map = self.attribute_map.write().unwrap();
        // let mut client = self.client.write().unwrap();
        // for row in client
        //     .query(
        //         r#"
        //     SELECT
        //         event_type,
        //         attr_name,
        //         attr_type
        //     FROM
        //         attribute_value_types"#,
        //         &[],
        //     )
        //     .expect("Cannot update schema")
        // {
        //     let event_type: String = row.get(0);
        //     let attr_name: String = row.get(1);
        //     let attr_type_str: String = row.get(2);
        //     let attr_type: ValueType =
        //         serde_json::from_str(&attr_type_str).expect("Cannot convert value type");
        //
        //     let attr_map = attribute_map.entry(event_type.into()).or_default();
        //     attr_map.insert(attr_name.as_str().into(), attr_type);
        // }
    }

    fn minimum_date() -> NaiveDateTime {
        parse_utc_from_str("1970-01-01T00:00:00+00:00")
    }

    fn maximum_date() -> NaiveDateTime {
        parse_utc_from_str("2050-12-31T00:00:00+00:00")
    }
}

pub fn kstring_opt_to_string(v: Option<SmallString>) -> String {
    match v {
        None => "##RESERVED_STRING##".to_string(),
        Some(s) => s.to_string(),
    }
}

impl EventStore for PostgresEventStore {
    fn insert(&self, event: Event) -> Result<()> {
        unimplemented!();
        // self.update_schema(&event);
        //
        // let mut client = self.client.write().unwrap();
        //
        // let mut transaction = client.transaction().expect("Error getting transaction");
        //
        // let attrs = serde_json::to_value(event.attrs).unwrap();
        // let timestamp = event.event_time.timestamp_millis() as f64 / 1000.0;
        // transaction.execute(
        //     "INSERT INTO events (event_id, event_type, event_time, attrs, experiment_id) VALUES ($1, $2, to_timestamp($3), $4, $5)",
        //     &[
        //         &event.event_id.as_ref().map(|s| s.as_str()),
        //         &event.event_type.as_str(),
        //         &timestamp,
        //         &attrs,
        //         &event.experiment_id.map(|v| v.to_string())
        //     ],
        // ).expect("Error inserting data");
        //
        // for entity_id in event.entities.iter().flatten() {
        //     transaction
        //         .execute(
        //             "INSERT INTO events_entities (entity_id, event_id) VALUES ($1, $2)",
        //             &[
        //                 &entity_id.as_str(),
        //                 &event.event_id.as_ref().map(|s| s.as_str()),
        //             ],
        //         )
        //         .expect("Error inserting data");
        // }
        //
        // transaction.commit().expect("Error committing transaction");
    }

    fn insert_batch(&self, events: Vec<Event>) -> Result<()> {
        unimplemented!()
        // let events_without_entity_id: Vec<EventWithoutEntity> =
        //     events.iter().map(|event| event.clone().into()).collect();
        // let batch_insert_events = BatchInsert::new(self.client.clone(), "events".into(), 10);
        // batch_insert_events
        //     .insert(events_without_entity_id)
        //     .expect("Problems inserting events");
        //
        // let batch_insert_events =
        //     BatchInsert::new(self.client.clone(), "events_entities".into(), 10);
        // let events_entities: Vec<_> = events
        //     .iter()
        //     .flat_map(|event| {
        //         let event_ent: Vec<EventEntity> = event.clone().into();
        //         event_ent
        //     })
        //     .collect();
        // batch_insert_events
        //     .insert(events_entities)
        //     .expect("Problems inserting events");
        //
        // for event in events {
        //     self.update_schema(&event);
        // }
    }

    fn get_entities(&self, experiment_id: &Option<SmallString>) -> Vec<Entity> {
        unimplemented!()
        // let mut client = self.client.write().unwrap();
        // let mut entities = vec![];
        // let query: String = match experiment_id {
        //     None => {
        //         "select distinct entity_id from events_entities where experiment_id is null".into()
        //     }
        //     Some(experiment_id) => {
        //         format!("select distinct entity_id from events_entities where experiment_id is null or experiment_id = {}", experiment_id)
        //     }
        // };
        // for _ in client.query(&*query, &[]).expect("Cannot query entities") {
        //     for row in client
        //         .query("select distinct entity_id from events_entities", &[])
        //         .expect("Cannot query entities")
        //     {
        //         let entity_id: String = row.get(0);
        //         entities.push(entity_id.into());
        //     }
        // }
        // entities
    }

    fn update_schema(&self, event: &Event) {
        unimplemented!()
        // let mut client = self.client.write().unwrap();
        // if let Some(attrs) = &event.attrs {
        //     let mut commit_transaction = false;
        //     let mut transaction = client.transaction().expect("Error getting transaction");
        //     for (new_attr_name, new_attr_value) in attrs.iter() {
        //         let new_value_type: ValueType = (*new_attr_value).clone().into();
        //
        //         let mut attribute_map = self.attribute_map.write().unwrap();
        //         if let Some(old_value_type) = attribute_map
        //             .get(&event.event_type)
        //             .and_then(|hm| hm.get(new_attr_name))
        //         {
        //             if new_value_type != *old_value_type {
        //                 panic!("New attribute value {:?}={:?} ({:?}) type doesn't match existing schema value type {:?}", new_attr_name, new_attr_value, new_value_type, old_value_type);
        //             }
        //         } else {
        //             // update local copy of the attribute map
        //             let attr_map = attribute_map.entry(event.event_type.clone()).or_default();
        //             attr_map.insert(new_attr_name.clone(), new_value_type.clone());
        //
        //             // update db
        //             transaction.execute("INSERT INTO attribute_value_types (event_type, attr_name, attr_type) VALUES ($1, $2, $3)", &[
        //                 &event.event_type.as_str(),
        //                 &new_attr_name.as_str(),
        //                 &serde_json::to_string(&new_value_type).expect("Cannot convert value type")
        //             ]).expect("Failed insert into attributes");
        //
        //             commit_transaction = true;
        //         }
        //     }
        //     if commit_transaction {
        //         transaction
        //             .commit()
        //             .expect("Failed transaction commit in update schema");
        //     }
        // }
    }

    fn query_entity_event_type(
        &self,
        entities: &Entities,
        event_type: &EventType,
        interval: &NaiveDateTimeInterval,
        query_config: &QueryConfig,
        experiment_id: &Option<SmallString>,
    ) -> Option<Vec<(Timestamp, Vec<Arc<Event>>)>> {
        unimplemented!();
        // let experiment_id = experiment_id.as_ref().map(|v| v.to_string());
        // let mut events_treemap: BTreeMap<Timestamp, Vec<Arc<Event>>> = BTreeMap::new();
        // let mut client = self.client.write().unwrap();
        // for row in client
        //     .query(
        //         r#"
        // select
        //     ev.event_id,
        //     ent.entity_id,
        //     ev.event_type,
        //     ev.event_time,
        //     ev.attrs::TEXT as attrs,
        //     ev.experiment_id
        // from events as ev
        //      inner join events_entities as ent
        //         on ev.event_id  = ent.event_id
        // where ent.entity_id = $1 and ev.event_type = $2 and ev.event_time between $3 and $4 and
        //       (ev.experiment_id is null or ev.experiment_id = $5)
        // order by ev.event_time
        // "#,
        //         &[
        //             &entity.as_str(),
        //             &event_type.as_str(),
        //             &interval.start_dt_safe().max(Self::minimum_date()),
        //             &interval.end_dt_safe().min(Self::maximum_date()),
        //             &experiment_id,
        //         ],
        //     )
        //     .expect("Cannot query entity event type")
        // {
        //     let event = PostgresEventStore::row_to_event(row);
        //     events_treemap
        //         .entry(event.event_time)
        //         .or_default()
        //         .push(Arc::new(event));
        // }
        //
        // let mut output = vec![];
        // for (k, v) in events_treemap.into_iter() {
        //     output.push((k, v));
        // }
        //
        // Some(output)
    }

    fn query_entity_interval(
        &self,
        entities: &Entities,
        interval: &NaiveDateTimeInterval,
        query_config: &QueryConfig,
        experiment_id: &Option<SmallString>,
    ) -> Option<Vec<(Timestamp, Vec<Arc<Event>>)>> {
        unimplemented!()
        // let experiment_id = kstring_opt_to_string(experiment_id.clone());
        // let mut events_treemap: BTreeMap<Timestamp, Vec<Arc<Event>>> = BTreeMap::new();
        // let mut client = self.client.write().unwrap();
        // for row in client
        //     .query(
        //         r#"
        // select
        //     ev.event_id,
        //     ent.entity_id,
        //     ev.event_type,
        //     ev.event_time,
        //     ev.attrs::TEXT as attrs,
        //     ev.experiment_id
        // from events as ev
        //      inner join events_entities as ent
        //         on ev.event_id  = ent.event_id
        // where ent.entity_id = $1 and ev.event_time between $2 and $3 and
        //       (ev.experiment_id is null or ev.experiment_id = $4)
        // order by ev.event_time
        // "#,
        //         &[
        //             &entity.as_str(),
        //             &interval.start_dt,
        //             &interval.end_dt,
        //             &experiment_id,
        //         ],
        //     )
        //     .expect("Cannot query entity event type")
        // {
        //     let event = PostgresEventStore::row_to_event(row);
        //
        //     events_treemap
        //         .entry(event.event_time)
        //         .or_default()
        //         .push(Arc::new(event));
        // }
        //
        // let mut output = vec![];
        // for (k, v) in events_treemap.into_iter() {
        //     output.push((k, v));
        // }
        //
        // Some(output)
    }

    fn query_entity(
        &self,
        entities: &Entities,
        query_config: &QueryConfig,
        experiment_id: Option<SmallString>,
    ) -> Option<Vec<(Timestamp, Vec<Arc<Event>>)>> {
        unimplemented!()
        // let experiment_id = kstring_opt_to_string(experiment_id);
        // let mut events_treemap: BTreeMap<Timestamp, Vec<Arc<Event>>> = BTreeMap::new();
        // let mut client = self.client.write().unwrap();
        // for row in client
        //     .query(
        //         r#"
        // select
        //     ev.event_id,
        //     ent.entity_id,
        //     ev.event_type,
        //     ev.event_time,
        //     ev.attrs::TEXT as attrs,
        //     ev.experiment_id
        // from events as ev
        //      inner join events_entities as ent
        //         on ev.event_id  = ent.event_id
        // where ent.entity_id = $1 and (ev.experiment_id is null or ev.experiment_id = $2)
        // order by ev.event_time
        // "#,
        //         &[&entity.as_str(), &experiment_id.as_str()],
        //     )
        //     .expect("Cannot query entity event type")
        // {
        //     let event = PostgresEventStore::row_to_event(row);
        //
        //     events_treemap
        //         .entry(event.event_time)
        //         .or_default()
        //         .push(Arc::new(event));
        // }
        //
        // let mut output = vec![];
        // for (k, v) in events_treemap.into_iter() {
        //     output.push((k, v));
        // }
        //
        // Some(output)
    }

    fn query_event_type(
        &self,
        event_type: &EventType,
        query_config: &QueryConfig,
        interval: Option<&NaiveDateTimeInterval>,
    ) -> Option<Vec<(Timestamp, Vec<Arc<Event>>)>> {
        todo!()
    }

    fn query_interval(
        &self,
        interval: &NaiveDateTimeInterval,
        query_config: &QueryConfig,
    ) -> Option<Vec<(Timestamp, Vec<Arc<Event>>)>> {
        todo!()
    }

    fn filter_events(
        &self,
        condition: &Expr,
        query_config: &QueryConfig,
        stored_variables: &HashMap<SmallString, HashMap<Timestamp, Value>>,
    ) -> Result<Vec<Arc<Event>>> {
        todo!()
    }

    fn get_event_by_id(&self, event_id: &EventID) -> Option<Arc<Event>> {
        todo!()
    }

    fn all_events(&self) -> Result<Vec<Arc<Event>>> {
        todo!()
    }

    fn all_events_sorted(&self) -> Result<Vec<Arc<Event>>> {
        todo!()
    }

    fn get_attribute_value_type(&self, name: &AttributeName) -> Option<HashSet<ValueType>> {
        unimplemented!()
        // let mut value_types: HashSet<ValueType> = HashSet::new();
        // let mut client = self.client.write().unwrap();
        // for row in client
        //     .query(
        //         r#"
        // select
        //     attr_type
        // from attribute_value_types
        // where attr_name = $1
        // "#,
        //         &[&name.as_str()],
        //     )
        //     .expect("Cannot query attribute type")
        // {
        //     let value: String = row.get(0);
        //     let value_type: ValueType =
        //         serde_json::from_str(value.as_str()).expect("Cannot parse value type");
        //     value_types.insert(value_type);
        // }
        //
        // Some(value_types)
    }

    fn n_entities(&self) -> usize {
        unimplemented!()
        // let mut client = self.client.write().unwrap();
        // let result: i64 = client
        //     .query_one(
        //         r#"select COUNT(distinct entity_id) as cnt from events_entities"#,
        //         &[],
        //     )
        //     .expect("Cannot query entity event type")
        //     .get(0);
        // result as usize
    }

    fn flush(&self) {
        unimplemented!();
        // self.drop_tables();
        // let mut client = self.client.write().unwrap();
        // client
        //     .execute("DELETE FROM events", &[])
        //     .expect("Error deleting data");
        // client
        //     .execute("DELETE FROM events_entities", &[])
        //     .expect("Error deleting data");
        // client
        //     .execute("DELETE FROM attribute_value_types", &[])
        //     .expect("Error deleting data");
        // let mut attr_map = self.attribute_map.write().unwrap();
        // attr_map.clear();
    }

    fn flush_experiments(&self) {
        unimplemented!();
        // let mut client = self.client.write().unwrap();
        // let mut transaction = client.transaction().expect("Error getting transaction");
        // transaction
        //     .execute("DELETE FROM events WHERE experiment_id is not NULL", &[])
        //     .expect("Error deleting experiments");
    }

    fn flush_experiment(&self, experiment_id: SmallString) {
        unimplemented!();
        // let mut client = self.client.write().unwrap();
        // let mut transaction = client.transaction().expect("Error getting transaction");
        // transaction
        //     .execute(
        //         "DELETE FROM events WHERE experiment_id = $1",
        //         &[&experiment_id.as_str()],
        //     )
        //     .expect("Error deleting experiments");
    }

    fn get_schema(&self) -> HashMap<SmallString, HashMap<AttributeName, ValueType>> {
        unimplemented!();
        // let attr_map = self.attribute_map.read().unwrap().clone();
        // attr_map
    }

    fn get_n_events(&self) -> usize {
        todo!()
    }
}

// #[cfg(test)]
// mod tests {
//     use chrono::Utc;
//     use serial_test::serial;
//
//     use crate::datetime_utils::parse_utc_from_str;
//     use crate::types::FLOAT;
//     use crate::value::Value;
//
//     use super::*;
//
//     fn get_config() -> PostgresEventStoreConfig {
//         PostgresEventStoreConfig {
//             host: "localhost".into(),
//             port: "5432".into(),
//             username: "postgres".into(),
//             password: "postgres".into(),
//             db_name: "python".into(),
//         }
//     }
//
//     #[test]
//     #[serial]
//     #[ignore]
//     pub fn test_connection_local_pg() {
//         let config = get_config();
//         let _store = PostgresEventStore::new(config);
//     }
//
//     #[test]
//     #[serial]
//     #[ignore]
//     pub fn test_connection_insert_ok() {
//         let config = get_config();
//         let store = PostgresEventStore::new(config);
//         store.drop_tables();
//         store.create_tables();
//
//         let events: Vec<_> = (0..1000)
//             .map(|i| Event {
//                 event_type: "test".into(),
//                 event_time: Utc::now().naive_utc(),
//                 entities: Some(vec!["a".into()]),
//                 event_id: Some(i.to_string().into()),
//                 experiment_id: None,
//                 attrs: Some(hashmap!["pressure".into() => Value::Num(i as FLOAT)]),
//             })
//             .collect();
//
//         store.insert_batch(events);
//     }
//
//     #[test]
//     #[serial]
//     #[should_panic]
//     #[ignore]
//     pub fn test_connection_insert_fail() {
//         let config = get_config();
//         let store = PostgresEventStore::new(config);
//         store.drop_tables();
//         store.create_tables();
//         let event = Event {
//             event_type: "test".into(),
//             event_time: Utc::now().naive_utc(),
//             entities: Some(vec!["a".into()]),
//             event_id: Some("1".into()),
//             experiment_id: None,
//             attrs: Some(hashmap!["pressure".into() => Value::Num(100.0)]),
//         };
//         store.insert(event);
//
//         let event = Event {
//             event_type: "test".into(),
//             event_time: Utc::now().naive_utc(),
//             entities: Some(vec!["a".into()]),
//             event_id: Some("2".into()),
//             experiment_id: None,
//             attrs: Some(hashmap!["pressure".into() => Value::Int(100)]),
//         };
//         store.insert(event);
//     }
//
//     #[test]
//     #[serial]
//     #[ignore]
//     pub fn test_query_event_type_entity() {
//         let config = get_config();
//         let store = PostgresEventStore::new(config);
//         store.drop_tables();
//         store.create_tables();
//
//         let event = Event {
//             event_type: "test".into(),
//             event_time: parse_utc_from_str("2020-01-01T00:00:00+00:00"),
//             entities: Some(vec!["a".into()]),
//             event_id: Some("1".into()),
//             experiment_id: None,
//             attrs: Some(hashmap!["pressure".into() => Value::Num(100.0)]),
//         };
//         store.insert(event);
//
//         let event = Event {
//             event_type: "test2".into(),
//             event_time: parse_utc_from_str("2020-01-02T00:00:00+00:00"),
//             entities: Some(vec!["a".into()]),
//             event_id: Some("2".into()),
//             experiment_id: None,
//             attrs: Some(hashmap!["pressure".into() => Value::Num(200.0)]),
//         };
//         store.insert(event);
//
//         let event = Event {
//             event_type: "test".into(),
//             event_time: parse_utc_from_str("2020-01-03T00:00:00+00:00"),
//             entities: Some(vec!["b".into()]),
//             event_id: Some("3".into()),
//             experiment_id: None,
//             attrs: Some(hashmap!["pressure".into() => Value::Num(300.0)]),
//         };
//         store.insert(event);
//
//         let interval = NaiveDateTimeInterval {
//             start_dt: Some(parse_utc_from_str("2020-01-01T00:00:00+00:00")),
//             end_dt: Some(parse_utc_from_str("2020-01-04T00:00:00+00:00")),
//         };
//
//         let experiment_id: Option<SmallString> = None;
//         let result = store.query_entity_event_type(
//             &SmallString::from_ref("a"),
//             &SmallString::from_ref("test"),
//             &interval.clone(),
//             &experiment_id,
//         );
//         assert!(result.is_some());
//         if let Some(v) = result {
//             assert!(v.len() == 1);
//         }
//
//         let result = store.query_entity_interval(&SmallString::from_ref("a"), &interval, &None);
//         assert!(result.is_some());
//         if let Some(v) = result {
//             assert!(v.len() == 2);
//         }
//
//         let result = store.n_entities();
//         assert_eq!(result, 2);
//     }
//
//     #[test]
//     #[serial]
//     #[ignore]
//     pub fn test() {
//         let config = get_config();
//         let store = PostgresEventStore::new(config);
//         store.drop_tables();
//         store.create_tables();
//         let batch_insert = BatchInsert::new(store.client, "events".into(), 10);
//         let event: EventWithoutEntity = Event {
//             event_type: "test".into(),
//             event_time: parse_utc_from_str("2020-01-03T00:00:00+00:00"),
//             entities: Some(vec!["b".into()]),
//             event_id: Some("4".into()),
//             experiment_id: None,
//             attrs: Some(hashmap!["pressure".into() => Value::Num(300.0)]),
//         }
//         .into();
//         batch_insert.insert(vec![event]).unwrap();
//     }
// }
