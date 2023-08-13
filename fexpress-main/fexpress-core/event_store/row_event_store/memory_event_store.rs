#![allow(clippy::unwrap_used)]

use std::collections::BTreeMap;
use std::sync::{Arc, RwLock};

use crate::map::{HashMap, HashSet};
use crate::sstring::SmallString;
use anyhow::{anyhow, bail, Result};
use itertools::Itertools;
use slotmap::{DefaultKey, SlotMap};

use crate::algo::intersect::intersect;
use crate::ast::core::Expr;
use crate::eval::{eval_simple_expr, EvalContext};
use crate::event::{AttributeName, Entity, Event, EventType};
use crate::event_index::{EventContext, EventScopeConfig, Query};
use crate::event_store::{EventStore, EventStoreImpl, QueryConfig};
use crate::interval::NaiveDateTimeInterval;
use crate::types::{Entities, EventID, Timestamp};
use crate::value::{Value, ValueType};

//https://users.rust-lang.org/t/data-structure-with-views-indexes-into-itself/9803

pub type TimeBTree = BTreeMap<Timestamp, Vec<DefaultKey>>;

/*

Change this data structure to remove additional atributes related to experiments and store every
slotmap

pub enum Experiment {
     NoExperiment,
     ExperimentId(usize)
}

#[derive(Debug)]
pub struct Event {
    // your other Event fields here...
    experiment: Experiment,
}

#[derive(Debug, Default)]
pub struct MemoryEventStore {
    // main collection of events
    pub sm: Arc<RwLock<SlotMap<DefaultKey, Arc<Event>>>>,
    // indices
    pub global_index_ts: Arc<RwLock<HashMap<Experiment, TimeBTree>>>,
    pub global_index_event_type_ts: Arc<RwLock<HashMap<Experiment, HashMap<SmallString, TimeBTree>>>>,
    // global events not tied to any entity
    pub index_by_entity_ts: Arc<RwLock<HashMap<Experiment, HashMap<SmallString, TimeBTree>>>>,
    pub index_by_entity_event_type_ts: Arc<RwLock<HashMap<Experiment, HashMap<(SmallString, SmallString), TimeBTree>>>>,
    /// schema keeps value types for event_type -> attribute_name
    pub schema: Arc<RwLock<HashMap<SmallString, HashMap<SmallString, ValueType>>>>,
    /// attr_name -> value type
    /// We are basically creating a structure that disambiguates valuetype
    /// just by looking at the attribute name.
    pub attr_value_types: Arc<RwLock<HashMap<SmallString, HashSet<ValueType>>>>,
}

 */
#[derive(Clone, Debug, Default)]
pub struct MemoryEventStore {
    // main collection of events
    pub sm: Arc<RwLock<SlotMap<DefaultKey, Arc<Event>>>>,
    // indices
    pub index_by_event_id: Arc<RwLock<HashMap<EventID, DefaultKey>>>,
    pub global_index_ts: Arc<RwLock<TimeBTree>>,
    pub global_index_event_type_ts: Arc<RwLock<HashMap<EventType, TimeBTree>>>,
    // global events not tied to any entity
    // (entity_type, entity_id) -> index
    pub index_by_entity_ts: Arc<RwLock<HashMap<Entity, TimeBTree>>>,
    // (event_type, entity_type, entity_id) -> index
    pub index_by_event_type_entity_ts: Arc<RwLock<HashMap<EventType, HashMap<Entity, TimeBTree>>>>,
    /// keep separate indices for experiments
    /// I think they need to be separate because in most of the cases if the experiment_id is null
    /// we only are interested in the indices without experiments. If we are extracting features
    /// from experiments then we can gather the data from both indices and then combine TimeBTree
    pub experiment_index_by_ts: Arc<RwLock<HashMap<SmallString, TimeBTree>>>,
    // (experiment_id, entity_type, entity_id) -> index
    pub experiment_index_by_entity_ts:
        Arc<RwLock<HashMap<SmallString, HashMap<Entity, TimeBTree>>>>,
    pub experiment_index_by_entity_event_type_ts:
        Arc<RwLock<HashMap<SmallString, HashMap<Entity, TimeBTree>>>>,
    /// schema keeps value types for event_type -> attribute_name
    pub schema: Arc<RwLock<HashMap<SmallString, HashMap<AttributeName, ValueType>>>>,
    /// attr_name -> value type
    /// We are basically creating a structure that disambiguates valuetype
    /// just by looking at the attribute name.
    pub attr_value_types: Arc<RwLock<HashMap<AttributeName, HashSet<ValueType>>>>,
}

fn merge_event_vectors(
    vec_a: Option<Vec<(Timestamp, Vec<Arc<Event>>)>>,
    vec_b: Option<Vec<(Timestamp, Vec<Arc<Event>>)>>,
) -> Option<Vec<(Timestamp, Vec<Arc<Event>>)>> {
    match (vec_a, vec_b) {
        (None, Some(v)) => Some(v),
        (Some(v), None) => Some(v),
        (None, None) => None,
        (Some(a), Some(b)) => {
            let mut hm: BTreeMap<Timestamp, Vec<Arc<Event>>> = BTreeMap::new();
            for (k, vs) in a {
                let entry = hm.entry(k).or_default();
                for v in vs {
                    entry.push(v.clone());
                }
            }

            for (k, vs) in b {
                let entry = hm.entry(k).or_default();
                for v in vs {
                    entry.push(v.clone());
                }
            }

            let mut out = Vec::new();
            for (k, vs) in hm.iter() {
                let mut vs = vs.clone();
                vs.sort_by_key(|event| event.event_time);
                out.push((*k, vs))
            }

            Some(out)
        }
    }
}

#[allow(dead_code)]
impl MemoryEventStore {
    pub fn new() -> Self {
        Self {
            sm: Default::default(),
            index_by_event_id: Arc::new(Default::default()),
            global_index_ts: Arc::new(Default::default()),
            global_index_event_type_ts: Arc::new(Default::default()),
            index_by_entity_ts: Arc::new(Default::default()),
            index_by_event_type_entity_ts: Arc::new(Default::default()),
            experiment_index_by_ts: Arc::new(Default::default()),
            experiment_index_by_entity_ts: Arc::new(Default::default()),
            experiment_index_by_entity_event_type_ts: Arc::new(Default::default()),
            schema: Default::default(),
            attr_value_types: Default::default(),
        }
    }

    pub fn convert_vec_key_to_events(&self, key_vec: Vec<DefaultKey>) -> Result<Vec<Arc<Event>>> {
        key_vec
            .iter()
            .map(|key| self.convert_key_to_event(key))
            .collect()
    }

    pub fn convert_key_to_event(&self, key: &DefaultKey) -> Result<Arc<Event>> {
        let sm = self.sm.read().unwrap();
        sm.get(*key).ok_or(anyhow!("Cannot find Event")).cloned()
    }

    pub fn get_entity(&self, entity: &Entity) -> Option<TimeBTree> {
        let index_by_entity_ts = self.index_by_entity_ts.read().unwrap();
        index_by_entity_ts.get(entity).cloned()
    }

    fn insert_with_experiment_id(&self, event: &Event, experiment_id: &SmallString) -> Result<()> {
        // Check if an event with the same ID already exists
        if let Some(event_id) = &event.event_id {
            let index_by_event_id = self.index_by_event_id.read().unwrap();
            if index_by_event_id.contains_key(event_id) {
                return Err(anyhow!("An event with this ID already exists."));
            }
        }

        let mut sm = self.sm.write().unwrap();
        let key = sm.insert(Arc::new(event.clone()));

        let mut index_by_ts = self.experiment_index_by_ts.write().unwrap();
        let mut index_by_event_id = self.index_by_event_id.write().unwrap();
        let mut experiment_index_by_entity_ts = self.experiment_index_by_entity_ts.write().unwrap();
        let mut index_by_entity_event_type_ts = self
            .experiment_index_by_entity_event_type_ts
            .write()
            .unwrap();

        let event = sm.get(key).ok_or(anyhow!("Cannot read Event"))?;
        if let Some(event_id) = &event.event_id {
            index_by_event_id.insert(event_id.clone(), key.clone());
        }

        index_by_ts
            .entry(experiment_id.clone())
            .or_default()
            .entry(event.event_time)
            .or_default()
            .push(key);

        for entity in event.entities() {
            experiment_index_by_entity_ts
                .entry(experiment_id.clone())
                .or_default()
                .entry(entity.clone())
                .or_default()
                .entry(event.event_time)
                .or_default()
                .push(key);

            index_by_entity_event_type_ts
                .entry(experiment_id.clone())
                .or_default()
                .entry(entity.clone())
                .or_default()
                .entry(event.event_time)
                .or_default()
                .push(key);
        }

        Ok(())
    }

    fn insert_without_experiment_id(&self, event: &Event) -> Result<()> {
        // Check if an event with the same ID already exists
        if let Some(event_id) = &event.event_id {
            let index_by_event_id = self.index_by_event_id.read().unwrap();
            if index_by_event_id.contains_key(event_id) {
                return Err(anyhow!("An event with this ID already exists."));
            }
        }

        let mut sm = self.sm.write().unwrap();
        let key = sm.insert(Arc::new(event.clone()));
        let mut index_by_entity_ts = self.index_by_entity_ts.write().unwrap();
        let mut index_by_event_id = self.index_by_event_id.write().unwrap();
        let mut index_by_entity_event_type_ts = self.index_by_event_type_entity_ts.write().unwrap();

        let event = sm.get(key).ok_or(anyhow!("Cannot find Event"))?;

        if let Some(event_id) = &event.event_id {
            index_by_event_id.insert(event_id.clone(), key.clone());
        }

        if event.entities.len() > 0 {
            for entity in event.entities().iter() {
                index_by_entity_ts
                    .entry(entity.clone())
                    .or_default()
                    .entry(event.event_time)
                    .or_default()
                    .push(key);

                index_by_entity_event_type_ts
                    .entry(event.event_type.clone())
                    .or_default()
                    .entry(entity.clone().into())
                    .or_default()
                    .entry(event.event_time)
                    .or_default()
                    .push(key);
            }
        } else {
            let mut global_index_by_ts = self.global_index_ts.write().unwrap();
            let mut global_index_by_event_type_ts =
                self.global_index_event_type_ts.write().unwrap();

            global_index_by_ts
                .entry(event.event_time)
                .or_default()
                .push(key);

            global_index_by_event_type_ts
                .entry(event.event_type.clone())
                .or_default()
                .entry(event.event_time)
                .or_default()
                .push(key);
        }

        Ok(())
    }

    fn get_event_by_id(&self, event_id: &EventID) -> Option<Arc<Event>> {
        let index_by_event_id = self.index_by_event_id.read().unwrap();
        let sm = self.sm.read().unwrap();
        index_by_event_id
            .get(event_id)
            .and_then(|key| sm.get(*key).cloned())
    }

    fn extract_events_from_treemap(
        &self,
        interval: Option<&NaiveDateTimeInterval>,
        treemap: &TimeBTree,
        query_config: &QueryConfig,
    ) -> Result<Vec<(Timestamp, Vec<Arc<Event>>)>> {
        let range = interval.map(|interval| {
            let (start_dt, end_dt) = if query_config.include_events_on_obs_date {
                (interval.start_dt_exclusive_safe(), interval.end_dt_safe())
            } else {
                (interval.start_dt_safe(), interval.end_dt_exclusive_safe())
            };
            start_dt..=end_dt
        });

        let entries = match range {
            Some(range) => treemap.range(range).collect_vec(),
            None => treemap.iter().collect_vec(),
        };

        entries
            .iter()
            .map(|(&ts, keys)| {
                let events = self.convert_vec_key_to_events(keys.to_vec());
                match events {
                    Ok(events) => Ok((ts, events)),
                    Err(e) => Err(e),
                }
            })
            .collect()
    }

    pub fn all_events_memory_store(&self) -> Result<Vec<Arc<Event>>> {
        let sm = self.sm.read().unwrap();
        Ok(sm.values().cloned().collect())
    }

    pub fn all_events_sorted_memory_store(&self) -> Result<Vec<Arc<Event>>> {
        let mut events = self.all_events()?;
        events.sort_unstable_by(|a, b| a.event_time.cmp(&b.event_time));
        Ok(events)
    }

    fn get_events_by_keys(&self, keys: Vec<DefaultKey>) -> Vec<Arc<Event>> {
        let sm = self.sm.read().unwrap();
        keys.iter()
            .filter_map(|&key| sm.get(key).cloned())
            .collect()
    }

    pub fn merge_sorted_events(
        &self,
        mut a: Vec<Arc<Event>>,
        mut b: Vec<Arc<Event>>,
    ) -> Vec<Arc<Event>> {
        let mut result = Vec::with_capacity(a.len() + b.len());
        let mut i = 0;
        let mut j = 0;

        while i < a.len() && j < b.len() {
            if a[i].event_time <= b[j].event_time {
                result.push(a[i].clone());
                i += 1;
            } else {
                result.push(b[j].clone());
                j += 1;
            }
        }

        while i < a.len() {
            result.push(a[i].clone());
            i += 1;
        }

        while j < b.len() {
            result.push(b[j].clone());
            j += 1;
        }

        result
    }
}

impl EventStore for MemoryEventStore {
    fn insert(&self, event: Event) -> Result<()> {
        self.update_schema(&event);
        if let Some(ref experiment_id) = event.experiment_id {
            self.insert_with_experiment_id(&event, experiment_id)?;
        } else {
            self.insert_without_experiment_id(&event)?;
        }
        Ok(())
    }

    fn insert_batch(&self, events: Vec<Event>) -> Result<()> {
        for event in events.into_iter() {
            self.insert(event)?;
        }
        Ok(())
    }

    fn get_entities(&self, experiment_id: &Option<SmallString>) -> Vec<Entity> {
        let index_by_entity_ts = self.index_by_entity_ts.read().unwrap();
        let common_entities: Vec<_> = index_by_entity_ts.keys().cloned().collect();

        match experiment_id {
            None => common_entities,
            Some(experiment_id) => {
                let experiment_index_by_entity_ts =
                    self.experiment_index_by_entity_ts.read().unwrap();

                let experiment_entities: Vec<_> = experiment_index_by_entity_ts
                    .get(experiment_id)
                    .map(|v| v.keys().cloned().collect())
                    .unwrap_or_default();

                common_entities
                    .into_iter()
                    .chain(experiment_entities.into_iter())
                    .sorted()
                    .dedup()
                    .collect()
            }
        }
    }

    fn update_schema(&self, event: &Event) {
        let mut schema = self.schema.write().unwrap();

        let event_type = &event.event_type;
        if let Some(event_type_schema) = schema.get(&event_type.0) {
            for (attr_name, new_attr_value) in event.extract_attributes_values() {
                let new_attr_value_type: ValueType = new_attr_value.clone().into();
                if let Some(value_type) = event_type_schema.get(&attr_name) {
                    if new_attr_value_type != *value_type {
                        panic!("New attribute value {:?} ({:?}) type doesn't match existing schema value type {:?}", new_attr_value, new_attr_value_type, value_type);
                    }
                }
            }
        }

        // update the schema
        let event_type_entry = schema.entry(event_type.0.clone()).or_default();
        let mut attr_value_types = self.attr_value_types.write().unwrap();

        for (attr_name, value_new) in event.extract_attributes_values() {
            let value_type_new: ValueType = value_new.into();
            event_type_entry.insert(attr_name.clone(), value_type_new.clone());

            attr_value_types
                .entry(attr_name.clone())
                .or_default()
                .insert(value_type_new);
        }
    }

    fn query_entity_event_type(
        &self,
        entities: &Entities,
        event_type: &EventType,
        interval: &NaiveDateTimeInterval,
        query_config: &QueryConfig,
        experiment_id: &Option<SmallString>,
    ) -> Option<Vec<(Timestamp, Vec<Arc<Event>>)>> {
        // extract without experiment
        let index_by_entity_event_type_ts = self.index_by_event_type_entity_ts.read().unwrap();

        // here we are querying the entity index one entity by one entity
        let events_without_experiment = entities
            .iter()
            .map(|(entity_type, entity_id)| {
                index_by_entity_event_type_ts
                    .get(&event_type)
                    .and_then(|map| {
                        map.get(&Entity {
                            typ: entity_type.clone(),
                            id: entity_id.0.clone(),
                        })
                    })
                    .map(|treemap| {
                        self.extract_events_from_treemap(Some(interval), treemap, query_config)
                            .ok()
                    })
                    .flatten()
            })
            .collect_vec();

        let events_without_experiment = intersect(events_without_experiment);

        // extract global events
        let global_index_event_type_ts = self.global_index_event_type_ts.read().unwrap();
        let global_events = global_index_event_type_ts
            .get(event_type)
            .map(|treemap| {
                self.extract_events_from_treemap(Some(interval), treemap, query_config)
                    .ok()
            })
            .flatten();

        // events with experiment
        let events_with_experiment = if let Some(ref experiment_id) = experiment_id {
            let index_by_entity_event_type_ts = self
                .experiment_index_by_entity_event_type_ts
                .read()
                .unwrap();
            let events_with_experiment = entities
                .iter()
                .map(|(entity_type, entity_id)| {
                    index_by_entity_event_type_ts
                        .get(experiment_id)
                        .and_then(|hm| {
                            hm.get(&Entity {
                                typ: entity_type.clone(),
                                id: entity_id.0.clone(),
                            })
                            .map(|treemap| {
                                self.extract_events_from_treemap(
                                    Some(interval),
                                    treemap,
                                    query_config,
                                )
                                .ok()
                            })
                        })
                        .flatten()
                })
                .collect_vec();
            let events_with_experiment = intersect(events_with_experiment);
            events_with_experiment
        } else {
            None
        };
        merge_event_vectors(
            global_events,
            merge_event_vectors(events_without_experiment, events_with_experiment),
        )
    }

    fn query_entity_interval(
        &self,
        entities: &Entities,
        interval: &NaiveDateTimeInterval,
        query_config: &QueryConfig,
        experiment_id: &Option<SmallString>,
    ) -> Option<Vec<(Timestamp, Vec<Arc<Event>>)>> {
        // events without experiment
        let index_by_entity_ts = self.index_by_entity_ts.read().unwrap();
        let events_without_experiment = entities
            .iter()
            .map(|(entity_type, entity_id)| {
                index_by_entity_ts
                    .get(&Entity {
                        typ: entity_type.clone(),
                        id: entity_id.0.clone(),
                    })
                    .map(|treemap| {
                        self.extract_events_from_treemap(Some(interval), treemap, query_config)
                            .ok()
                    })
                    .flatten()
            })
            .collect_vec();

        let events_without_experiment = intersect(events_without_experiment);

        // extract global events
        let global_index_ts = self.global_index_ts.read().unwrap();
        let global_events = self
            .extract_events_from_treemap(Some(interval), &global_index_ts, query_config)
            .ok();

        // events with experiment
        let events_with_experiment = if let Some(ref experiment_id) = experiment_id {
            let index_by_entity_ts = self.experiment_index_by_entity_ts.read().unwrap();
            let events_with_experiment = entities
                .iter()
                .map(|(entity_type, entity_id)| {
                    index_by_entity_ts.get(experiment_id).and_then(|hm| {
                        hm.get(&Entity {
                            typ: entity_type.clone(),
                            id: entity_id.0.clone(),
                        })
                        .map(|treemap| {
                            self.extract_events_from_treemap(Some(interval), treemap, query_config)
                                .ok()
                        })
                        .flatten()
                    })
                })
                .collect_vec();
            intersect(events_with_experiment)
        } else {
            None
        };

        merge_event_vectors(
            global_events,
            merge_event_vectors(events_without_experiment, events_with_experiment),
        )
    }

    fn query_entity(
        &self,
        entities: &Entities,
        query_config: &QueryConfig,
        experiment_id: Option<SmallString>,
    ) -> Option<Vec<(Timestamp, Vec<Arc<Event>>)>> {
        let index_by_entity_ts = self.index_by_entity_ts.read().unwrap();
        let events_without_experiment = entities
            .iter()
            .map(|(entity_type, entity_id)| {
                index_by_entity_ts
                    .get(&Entity {
                        typ: entity_type.clone(),
                        id: entity_id.0.clone(),
                    })
                    .map(|treemap| {
                        self.extract_events_from_treemap(None, treemap, query_config)
                            .ok()
                    })
                    .flatten()
            })
            .collect_vec();
        let events_without_experiment = intersect(events_without_experiment);

        let global_index_by_ts = self.global_index_ts.read().unwrap();
        let events_global = self
            .extract_events_from_treemap(None, &global_index_by_ts, query_config)
            .ok();

        let events_with_experiment = if let Some(ref experiment_id) = experiment_id {
            let index_by_entity_ts = self.experiment_index_by_entity_ts.read().unwrap();
            let events_with_experiment = index_by_entity_ts.get(experiment_id).and_then(|hm| {
                let events = entities
                    .iter()
                    .map(|(entity_type, entity_id)| {
                        hm.get(&Entity {
                            typ: entity_type.clone(),
                            id: entity_id.0.clone(),
                        })
                        .map(|treemap| {
                            self.extract_events_from_treemap(None, treemap, query_config)
                                .ok()
                        })
                        .flatten()
                    })
                    .collect_vec();
                intersect(events)
            });
            events_with_experiment
        } else {
            None
        };

        merge_event_vectors(
            events_global,
            merge_event_vectors(events_with_experiment, events_without_experiment),
        )
    }

    fn query_event_type(
        &self,
        event_type: &EventType,
        query_config: &QueryConfig,
        interval: Option<&NaiveDateTimeInterval>,
    ) -> Option<Vec<(Timestamp, Vec<Arc<Event>>)>> {
        let index_by_event_type_entity_ts = self.index_by_event_type_entity_ts.read().unwrap();

        let mut timestamp_event_map = BTreeMap::new();

        if let Some(entity_map) = index_by_event_type_entity_ts.get(event_type) {
            for treemap in entity_map.values() {
                let mut keys = Vec::new();
                if let Some(interval) = interval {
                    for (timestamp, key_set) in
                        treemap.range(interval.start_dt_safe()..=interval.end_dt_safe())
                    {
                        keys.extend(key_set.iter().cloned());
                    }
                } else {
                    keys.extend(treemap.values().flat_map(|key_set| key_set.iter().cloned()));
                }

                let events = self.get_events_by_keys(keys);
                for event in events {
                    timestamp_event_map
                        .entry(event.event_time)
                        .or_insert_with(Vec::new)
                        .push(event);
                }
            }
        }

        if timestamp_event_map.is_empty() {
            None
        } else {
            Some(timestamp_event_map.into_iter().collect())
        }
    }

    fn query_interval(
        &self,
        interval: &NaiveDateTimeInterval,
        query_config: &QueryConfig,
    ) -> Option<Vec<(Timestamp, Vec<Arc<Event>>)>> {
        let index_by_event_type_entity_ts = self.index_by_event_type_entity_ts.read().unwrap();

        let mut timestamp_event_map = BTreeMap::new();

        for entity_map in index_by_event_type_entity_ts.values() {
            for treemap in entity_map.values() {
                let mut keys = Vec::new();
                for (timestamp, key_set) in
                    treemap.range(interval.start_dt_safe()..=interval.end_dt_safe())
                {
                    keys.extend(key_set.iter().cloned());
                }
                let events = self.get_events_by_keys(keys);
                for event in events {
                    timestamp_event_map
                        .entry(event.event_time)
                        .or_insert_with(Vec::new)
                        .push(event);
                }
            }
        }

        if timestamp_event_map.is_empty() {
            None
        } else {
            Some(timestamp_event_map.into_iter().collect())
        }
    }

    fn filter_events(
        &self,
        condition: &Expr,
        query_config: &QueryConfig,
        stored_variables: &HashMap<SmallString, HashMap<Timestamp, Value>>,
    ) -> Result<Vec<Arc<Event>>> {
        let context = EvalContext {
            event_index: &EventContext {
                event_store: EventStoreImpl::MemoryEventStore(self.clone()),
            },
            query_config,
            event_query_config: Default::default(),
            entities: Default::default(),
            experiment_id: None,
            obs_date: None,
            obs_time: None,
            event_types: vec![],
            event: None,
            event_on_obs_date: None,
        };
        let sm = self.sm.read().unwrap();
        let mut results = Vec::new();
        for (_, event) in sm.iter() {
            let eval_result = eval_simple_expr(
                condition,
                Some(&(**event)),
                Some(&context),
                stored_variables,
            );
            match eval_result {
                Ok(Value::Bool(true)) => results.push(event.clone()),
                Err(e) => bail!(e),
                _ => {}
            }
        }
        Ok(results)
    }

    fn get_event_by_id(&self, event_id: &EventID) -> Option<Arc<Event>> {
        self.get_event_by_id(event_id)
    }

    fn all_events(&self) -> Result<Vec<Arc<Event>>> {
        self.all_events_memory_store()
    }

    fn all_events_sorted(&self) -> Result<Vec<Arc<Event>>> {
        self.all_events_sorted_memory_store()
    }

    fn get_attribute_value_type(&self, name: &AttributeName) -> Option<HashSet<ValueType>> {
        let attr_value_types = self.attr_value_types.read().unwrap();
        attr_value_types.get(name).cloned()
    }

    fn n_entities(&self) -> usize {
        let sm = self.sm.read().unwrap();
        sm.len()
    }

    fn flush(&self) {
        let mut sm = self.sm.write().unwrap();
        sm.clear();

        let mut index_by_entity_ts = self.index_by_entity_ts.write().unwrap();
        index_by_entity_ts.clear();

        let mut index_by_entity_event_type_ts = self.index_by_event_type_entity_ts.write().unwrap();
        index_by_entity_event_type_ts.clear();

        let mut schema = self.schema.write().unwrap();
        schema.clear();

        let mut attr_value_types = self.attr_value_types.write().unwrap();
        attr_value_types.clear();

        let mut experiment_index_by_ts = self.experiment_index_by_ts.write().unwrap();
        experiment_index_by_ts.clear();

        let mut experiment_index_by_entity_ts = self.experiment_index_by_entity_ts.write().unwrap();
        experiment_index_by_entity_ts.clear();

        let mut experiment_index_by_entity_event_type_ts = self
            .experiment_index_by_entity_event_type_ts
            .write()
            .unwrap();
        experiment_index_by_entity_event_type_ts.clear();
    }

    fn flush_experiments(&self) {
        let mut sm = self.sm.write().unwrap();

        let mut experiment_index_by_ts = self.experiment_index_by_ts.write().unwrap();

        // clean the slotmap
        for (_, hm) in experiment_index_by_ts.iter() {
            for keys in hm.values() {
                for key in keys {
                    sm.remove(*key);
                }
            }
        }

        experiment_index_by_ts.clear();

        let mut experiment_index_by_entity_ts = self.experiment_index_by_entity_ts.write().unwrap();
        experiment_index_by_entity_ts.clear();

        let mut experiment_index_by_entity_event_type_ts = self
            .experiment_index_by_entity_event_type_ts
            .write()
            .unwrap();
        experiment_index_by_entity_event_type_ts.clear();
    }

    fn flush_experiment(&self, experiment_id: SmallString) {
        let mut sm = self.sm.write().unwrap();

        let mut experiment_index_by_ts = self.experiment_index_by_ts.write().unwrap();

        // clean the slotmap
        let hm = experiment_index_by_ts.get(&experiment_id);
        if let Some(hm) = hm {
            for keys in hm.values() {
                for key in keys {
                    sm.remove(*key);
                }
            }
        }

        experiment_index_by_ts.remove(experiment_id.clone().as_str());

        let mut experiment_index_by_entity_ts = self.experiment_index_by_entity_ts.write().unwrap();
        experiment_index_by_entity_ts.remove(experiment_id.clone().as_str());

        let mut experiment_index_by_entity_event_type_ts = self
            .experiment_index_by_entity_event_type_ts
            .write()
            .unwrap();
        experiment_index_by_entity_event_type_ts.remove(experiment_id.clone().as_str());
    }

    fn get_schema(&self) -> HashMap<SmallString, HashMap<AttributeName, ValueType>> {
        let attr = self.schema.read().unwrap().clone();
        attr
    }

    fn get_n_events(&self) -> usize {
        let sm = self.sm.read().unwrap();
        sm.len()
    }
}

#[cfg(test)]
mod test {
    use std::str::FromStr;

    use chrono::{NaiveDateTime, Utc};

    use crate::datetime_utils::parse_utc_from_str;
    use crate::event::EntityType;
    use crate::event_index::{EventContext, EventScopeConfig, RawQuery};
    use crate::features::Features;
    use crate::obs_dates::{Fixed, ObservationDatesConfig};
    use crate::value::Value;

    use super::*;

    #[test]
    fn test_event_db() {
        let event_db = MemoryEventStore::default();

        let event = Event {
            event_type: EventType("test".into()),
            event_time: Utc::now().naive_utc(),
            entities: btreemap!["".into() => "1".into()],
            attrs: Some(hashmap![a!("a") => Value::Int(1)]),
            ..Default::default()
        };

        event_db.insert(event);
    }

    #[test]
    fn test_event_filter() {
        let event_db = MemoryEventStore::default();
        let query_config = QueryConfig::default();

        for i in 1..=100 {
            let event = Event {
                event_type: EventType("test".into()),
                event_time: Utc::now().naive_utc(),
                entities: btreemap!["".into() => "1".into()],
                attrs: Some(hashmap![a!("a") => Value::Int(i)]),
                ..Default::default()
            };
            event_db.insert(event);
        }

        let hm = HashMap::new();
        let expr = Expr::from_str("a <= 50").unwrap();
        let events = event_db.filter_events(&expr, &query_config, &hm).unwrap();
    }

    #[test]
    #[should_panic]
    fn test_schema_conflict() {
        let event_db = MemoryEventStore::default();

        let event = Event {
            event_type: EventType("test".into()),
            event_time: Utc::now().naive_utc(),
            entities: btreemap!["".into() => "1".into()],
            attrs: Some(hashmap![a!("a") => Value::Int(1)]),
            ..Default::default()
        };

        event_db.insert(event);

        // creating an event with a diffent attribute data type
        let event = Event {
            event_type: EventType("test".into()),
            event_time: Utc::now().naive_utc(),
            entities: btreemap!["".into() => "1".into()],
            attrs: Some(hashmap![a!("a") => Value::Num(1.0)]),
            ..Default::default()
        };

        event_db.insert(event);
    }

    #[test]
    fn test_schema_no_conflict() {
        let event_db = MemoryEventStore::default();

        let event = Event {
            event_type: EventType("test".into()),
            event_time: Utc::now().naive_utc(),
            entities: btreemap!["".into() => "1".into()],
            attrs: Some(hashmap![a!("a") => Value::Int(1)]),
            ..Default::default()
        };

        event_db.insert(event);

        let event = Event {
            event_type: EventType("test".into()),
            event_time: Utc::now().naive_utc(),
            entities: btreemap!["".into() => "1".into()],
            attrs: Some(hashmap![a!("a") => Value::Int(2)]),
            ..Default::default()
        };

        event_db.insert(event);
    }

    #[test]
    pub fn test_query_event_type_entity() {
        let mut store = EventContext::default();

        let event = Event {
            event_type: EventType("test".into()),
            event_time: parse_utc_from_str("2020-01-01T00:00:00+00:00"),
            entities: btreemap!["".into() => "a".into()],
            event_id: Some("1".into()),
            experiment_id: None,
            attrs: Some(hashmap![a!("pressure") => Value::Num(100.0)]),
        };
        store.new_event(event);

        let event = Event {
            event_type: EventType("test2".into()),
            event_time: parse_utc_from_str("2020-01-02T00:00:00+00:00"),
            entities: btreemap!["".into() => "a".into()],
            event_id: Some("2".into()),
            experiment_id: None,
            attrs: Some(hashmap![a!("pressure") => Value::Num(200.0)]),
        };
        store.new_event(event);

        let event = Event {
            event_type: EventType("test".into()),
            event_time: parse_utc_from_str("2020-01-03T00:00:00+00:00"),
            entities: btreemap!["".into() => "a".into()],
            event_id: Some("3".into()),
            experiment_id: Some(from_string!("experiment_1".to_string())),
            attrs: Some(hashmap![a!("pressure") => Value::Num(300.0)]),
        };
        store.new_event(event);

        let event = Event {
            event_type: EventType("test".into()),
            event_time: parse_utc_from_str("2020-01-04T00:00:00+00:00"),
            entities: btreemap!["".into() => "b".into()],
            event_id: Some("4".into()),
            experiment_id: Some(from_string!("experiment_3".to_string())),
            attrs: Some(hashmap![a!("pressure") => Value::Num(400.0)]),
        };
        store.new_event(event);

        let event = Event {
            event_type: EventType("test".into()),
            event_time: parse_utc_from_str("2020-01-05T00:00:00+00:00"),
            entities: btreemap!["".into() => "b".into()],
            event_id: Some("5".into()),
            experiment_id: Some(from_string!("experiment_3".to_string())),
            attrs: Some(hashmap![a!("pressure") => Value::Num(500.0)]),
        };
        store.new_event(event);
        let mut entity_types = HashSet::new();
        entity_types.insert(EntityType(SmallString::from("")));
        let obs_dates = ObservationDatesConfig::Fixed(Fixed::new_from_str_vec(
            entity_types,
            vec!["2020-01-04T00:00:01".into()],
        ));
        let features_def =
            vec!["SUM(pressure) OVER past WHERE event_type = 'test' as f".to_string()];
        let entity_query =
            EventScopeConfig::RelatedEntitiesEvents(vec![EntityType("".to_string())]);
        let query_config = QueryConfig::default();
        let features = store
            .extract_features_from_expr(
                &obs_dates,
                entity_query,
                RawQuery::VecExpr(features_def),
                &query_config,
                None,
                None,
            )
            .unwrap();
        assert_eq!(features.get("f").unwrap().clone(), vec![Value::Num(100.0)]);

        let mut entity_types = HashSet::new();
        entity_types.insert(EntityType(SmallString::from("")));
        let obs_dates = ObservationDatesConfig::Fixed(Fixed::new_from_str_vec(
            entity_types.clone(),
            vec!["2020-01-04T00:00:01".into()],
        ));
        let features_def =
            vec!["SUM(pressure) OVER past WHERE event_type = 'test' as f".to_string()];
        let entity_query = EventScopeConfig::RelatedEntitiesEvents(vec![EntityType("".into())]);
        let query_config = QueryConfig::default();
        let features = store
            .extract_features_from_expr(
                &obs_dates,
                entity_query,
                RawQuery::VecExpr(features_def),
                &query_config,
                Some("experiment_1".into()),
                None,
            )
            .unwrap();
        assert_eq!(features.get("f").unwrap().clone(), vec![Value::Num(400.0)]);

        let obs_dates = ObservationDatesConfig::Fixed(Fixed::new_from_str_vec(
            entity_types.clone(),
            vec!["2020-01-04T00:00:00".into()],
        ));
        let features_def =
            vec!["SUM(pressure) OVER past WHERE event_type = 'test' as f".to_string()];
        let entity_query = EventScopeConfig::RelatedEntitiesEvents(vec![EntityType("".into())]);
        let query_config = QueryConfig::default();
        let features = store
            .extract_features_from_expr(
                &obs_dates,
                entity_query,
                RawQuery::VecExpr(features_def),
                &query_config,
                Some("experiment_2".into()),
                None,
            )
            .unwrap();
        assert_eq!(features.get("f").unwrap().clone(), vec![Value::Num(100.0)]);

        let obs_dates = ObservationDatesConfig::Fixed(Fixed::new_from_str_vec(
            entity_types.clone(),
            vec!["2020-01-06T00:00:00".into()],
        ));
        let features_def =
            vec!["SUM(pressure) OVER past WHERE event_type = 'test' as f".to_string()];
        let entity_query = EventScopeConfig::RelatedEntitiesEvents(vec![EntityType("".into())]);
        let query_config = QueryConfig::default();
        let features = store
            .extract_features_from_expr(
                &obs_dates,
                entity_query,
                RawQuery::VecExpr(features_def),
                &query_config,
                Some("experiment_3".into()),
                None,
            )
            .unwrap();
        assert_eq!(features.get("f").unwrap().clone(), vec![Value::Num(100.0)]);

        // test chunks
        let obs_dates = ObservationDatesConfig::Fixed(Fixed::new_from_str_vec(
            entity_types.clone(),
            vec!["2020-01-06T00:00:00".into()],
        ));
        let features_def =
            vec!["AVG(pressure) OVER past WHERE event_type = 'test' as f".to_string()];
        let entity_query = EventScopeConfig::RelatedEntitiesEvents(vec![EntityType("".into())]);
        let query_config = QueryConfig::default();
        let features = store
            .extract_features_from_expr(
                &obs_dates,
                entity_query,
                RawQuery::VecExpr(features_def),
                &query_config,
                Some("experiment_3".into()),
                Some(2),
            )
            .unwrap();
        assert_eq!(features.get("f").unwrap().clone(), vec![Value::Num(100.0)]);
    }

    #[test]
    pub fn test_global_events() {
        let mut store = EventContext::default();
        let event = Event {
            event_type: EventType("test".into()),
            event_time: parse_utc_from_str("2020-01-01T00:00:00+00:00"),
            entities: btreemap![],
            event_id: Some("1".into()),
            experiment_id: None,
            attrs: Some(hashmap![a!("pressure") => Value::Num(1000.0)]),
        };
        store.new_event(event);

        let event = Event {
            event_type: EventType("test".into()),
            event_time: parse_utc_from_str("2020-01-02T00:00:00+00:00"),
            entities: btreemap!["entity_type_a".into() => "ent1".into()],
            event_id: Some("2".into()),
            experiment_id: None,
            attrs: Some(hashmap![a!("pressure") => Value::Num(200.0)]),
        };
        store.new_event(event);

        let event = Event {
            event_type: EventType("test".into()),
            event_time: parse_utc_from_str("2020-01-03T00:00:00+00:00"),
            entities: btreemap!["entity_type_a".into() => "ent2".into()],
            event_id: Some("3".into()),
            experiment_id: None,
            attrs: Some(hashmap!["pressure".into() => Value::Num(300.0)]),
        };
        store.new_event(event);
        let mut entity_types = HashSet::new();
        entity_types.insert(EntityType(SmallString::from("entity_type_a")));
        let obs_dates = ObservationDatesConfig::Fixed(Fixed::new_from_str_vec(
            entity_types,
            vec!["2020-01-04T00:00:00".into()],
        ));
        let features_def =
            vec!["SUM(pressure) OVER past WHERE event_type = 'test' as f".to_string()];
        println!("obs dates {:?}", obs_dates);
        let entity_query = EventScopeConfig::RelatedEntitiesEvents(vec![EntityType("".into())]);
        let query_config = QueryConfig::default();
        let features = store
            .extract_features_from_expr(
                &obs_dates,
                entity_query,
                RawQuery::VecExpr(features_def),
                &query_config,
                None,
                None,
            )
            .unwrap();
        println!("{:?}", features);
        let mut values = features.get("f").unwrap().clone();
        values.sort();
        assert_eq!(values, vec![Value::Num(1200.0), Value::Num(1300.0)]);
    }

    fn create_store_with_sample_events() -> EventContext {
        let mut store = EventContext::default();

        // Event 1
        let event1 = Event {
            event_type: EventType("test".into()),
            event_time: parse_utc_from_str("2020-01-01T00:00:00+00:00"),
            entities: btreemap!["entity_type_a".into() => "ent1".into()],
            event_id: Some("1".into()),
            experiment_id: None,
            attrs: Some(hashmap![a!("pressure") => Value::Num(1000.0)]),
        };
        store.new_event(event1);

        // Event 2
        let event2 = Event {
            event_type: EventType("test".into()),
            event_time: parse_utc_from_str("2020-01-02T00:00:00+00:00"),
            entities: btreemap!["entity_type_a".into() => "ent1".into()],
            event_id: Some("2".into()),
            experiment_id: None,
            attrs: Some(hashmap![a!("pressure") => Value::Num(200.0)]),
        };
        store.new_event(event2);

        // Event 3
        let event3 = Event {
            event_type: EventType("test".into()),
            event_time: parse_utc_from_str("2020-01-03T00:00:00+00:00"),
            entities: btreemap!["entity_type_a".into() => "ent2".into()],
            event_id: Some("3".into()),
            experiment_id: None,
            attrs: Some(hashmap!["pressure".into() => Value::Num(300.0)]),
        };
        store.new_event(event3);

        // Return the populated store
        store
    }

    #[test]
    fn test_query_event_type() {
        let store = create_store_with_sample_events();
        let query_config = QueryConfig {
            parallel: false,
            include_events_on_obs_date: true,
        };

        // Define the parameters for the query
        let event_type = EventType("test".into());
        let interval = NaiveDateTimeInterval {
            start_dt: Some(NaiveDateTime::from_str("2020-01-01T00:00:00").unwrap()),
            end_dt: Some(NaiveDateTime::from_str("2020-01-05T00:00:00").unwrap()),
        };

        // Query the events
        let result = store
            .event_store
            .query_event_type(&event_type, &query_config, Some(&interval))
            .expect("Should return events.");

        // Write assertions to check if the result is as expected.
        assert_eq!(result.len(), 3); // Expecting 3 groups of events

        for (timestamp, events) in result {
            println!("{:?} => {:?}", timestamp, events);
            assert!(events.iter().all(|e| e.event_type == event_type)); // All events should have the event_type "test"
        }
    }

    #[test]
    fn test_query_interval() {
        let store = create_store_with_sample_events();

        let query_config = QueryConfig {
            parallel: false,
            include_events_on_obs_date: true,
        };
        // Define the parameters for the query
        let interval = NaiveDateTimeInterval {
            start_dt: Some(NaiveDateTime::from_str("2020-01-01T00:00:00").unwrap()),
            end_dt: Some(NaiveDateTime::from_str("2020-01-05T00:00:00").unwrap()),
        };

        // Query the events
        let result = store
            .event_store
            .query_interval(&interval, &query_config)
            .expect("Should return events.");

        // Write assertions to check if the result is as expected.
        assert_eq!(result.len(), 3); // Expecting 3 groups of events

        for (timestamp, events) in result {
            println!("{:?} => {:?}", timestamp, events);
            assert!(interval.contains(&timestamp, true)); // Timestamps should be within the interval
        }
    }
}
