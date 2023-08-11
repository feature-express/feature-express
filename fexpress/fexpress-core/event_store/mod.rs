use std::sync::Arc;

use crate::map::{HashMap, HashSet};
use crate::sstring::SmallString;
use anyhow::Result;
use enum_dispatch::enum_dispatch;
use serde::{Deserialize, Serialize};

use crate::event::{AttributeName, Entity, Event, EventType};
use crate::interval::NaiveDateTimeInterval;
use crate::types::{Entities, EventID, Timestamp};
use crate::value::{Value, ValueType};

use crate::ast::core::Expr;
use crate::event_index::EventQueryConfig;
use crate::event_store::postgres::postgres_event_store::PostgresEventStore;
use crate::event_store::row_event_store::memory_event_store::MemoryEventStore;
use schemars::JsonSchema;

pub mod column_event_store;
pub mod postgres;
pub mod row_event_store;
mod test_implementations;

#[enum_dispatch]
pub trait EventStore {
    /// Insert new event
    fn insert(&self, event: Event) -> Result<()>;

    /// Insert new event
    fn insert_batch(&self, events: Vec<Event>) -> Result<()>;

    /// Get list of entities
    fn get_entities(&self, experiment_id: &Option<SmallString>) -> Vec<Entity>;

    /// Update schema of the events
    fn update_schema(&self, event: &Event);

    /// Extract events for entity and event_type
    fn query_entity_event_type(
        &self,
        entities: &Entities,
        event_type: &EventType,
        interval: &NaiveDateTimeInterval,
        experiment_id: &Option<SmallString>,
    ) -> Option<Vec<(Timestamp, Vec<Arc<Event>>)>>;

    /// Extract events for entity, interval
    fn query_entity_interval(
        &self,
        entities: &Entities,
        interval: &NaiveDateTimeInterval,
        experiment_id: &Option<SmallString>,
    ) -> Option<Vec<(Timestamp, Vec<Arc<Event>>)>>;

    /// Extract events for entity
    fn query_entity(
        &self,
        entities: &Entities,
        experiment_id: Option<SmallString>,
    ) -> Option<Vec<(Timestamp, Vec<Arc<Event>>)>>;

    /// Query event type for all entities
    fn query_event_type(
        &self,
        event_type: &EventType,
        interval: Option<&NaiveDateTimeInterval>,
    ) -> Option<Vec<(Timestamp, Vec<Arc<Event>>)>>;

    /// Query interval
    fn query_interval(
        &self,
        interval: &NaiveDateTimeInterval,
    ) -> Option<Vec<(Timestamp, Vec<Arc<Event>>)>>;

    /// Filter events based on an expression
    fn filter_events(
        &self,
        condition: &Expr,
        stored_variables: &HashMap<SmallString, HashMap<Timestamp, Value>>,
    ) -> Result<Vec<Arc<Event>>>;

    /// Gets the event by ID
    fn get_event_by_id(&self, event_id: &EventID) -> Option<Arc<Event>>;

    /// Returns all of the events
    fn all_events(&self) -> Result<Vec<Arc<Event>>>;

    /// Returns all of the events in a sorted order
    fn all_events_sorted(&self) -> Result<Vec<Arc<Event>>>;

    /// Get attribute value types across all event types
    fn get_attribute_value_type(&self, name: &AttributeName) -> Option<HashSet<ValueType>>;

    /// Number of entities
    fn n_entities(&self) -> usize;

    /// Removes all entries
    fn flush(&self);

    /// Removes all experiments
    fn flush_experiments(&self);

    /// Removes a single experiments
    fn flush_experiment(&self, experiment_id: SmallString);

    /// Gets schema
    fn get_schema(&self) -> HashMap<SmallString, HashMap<AttributeName, ValueType>>;

    /// Gets number of events
    fn get_n_events(&self) -> usize;
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
pub struct EventStoreSettings {
    pub parallel: bool,
    // This setting controls whether events occurring at the exact time of an observation date
    // should be included in the historical data used for feature generation.
    pub include_events_on_obs_date: bool,
}

impl Default for EventStoreSettings {
    fn default() -> Self {
        Self {
            parallel: false,
            include_events_on_obs_date: true,
        }
    }
}

#[enum_dispatch(EventStore)]
#[derive(Clone, Debug)]
pub enum EventStoreImpl {
    MemoryEventStore(MemoryEventStore),
    PostgresEventStore(PostgresEventStore),
}

impl Default for EventStoreImpl {
    fn default() -> Self {
        EventStoreImpl::MemoryEventStore(MemoryEventStore::new(EventStoreSettings::default()))
    }
}
