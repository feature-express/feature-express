use pyo3::exceptions;
use pyo3::prelude::*;
use pyo3::types::PyDict;

use fexpress::event::Event;
use fexpress::event_index::EventQueryConfig;
use fexpress::event_index::{EventContext as EventContextR, Query};
use fexpress::event_store::{EventStore, EventStoreSettings};
use fexpress::map::HashMap;
use fexpress::obs_dates::ObservationDatesConfig;
use fexpress::sstring::SmallString;
use fexpress::value::Value;

#[pyclass(unsendable)]
pub struct EventContext {
    event_context: EventContextR,
}

#[pymethods]
impl EventContext {
    #[new]
    pub fn new(settings_json: String) -> PyResult<Self> {
        let settings: EventStoreSettings = serde_json::from_str(&settings_json).map_err(|err| {
            PyErr::new::<exceptions::PyValueError, String>(format!("{}", err).into())
        })?;
        Ok(Self {
            event_context: EventContextR::new_memory(settings),
        })
    }

    pub fn new_json_event(&mut self, event: String) -> PyResult<()> {
        let event: Event = serde_json::from_str(&event).map_err(|err| {
            PyErr::new::<exceptions::PyValueError, String>(format!("{}", err).into())
        })?;
        self.event_context.new_event(event);
        Ok(())
    }

    pub fn query(
        &mut self,
        obs_dates_config_json: String,
        event_query_config_json: String,
        query: String,
        experiment_id: Option<String>,
        chunk_size: Option<usize>,
    ) -> PyResult<(Vec<SmallString>, Vec<Vec<Value>>)> {
        let obs_dates_config: ObservationDatesConfig = serde_json::from_str(&obs_dates_config_json)
            .map_err(|err| {
                PyErr::new::<exceptions::PyValueError, String>(format!("{}", err).into())
            })?;
        let entity_query_config: EventQueryConfig = serde_json::from_str(&event_query_config_json)
            .map_err(|err| {
                PyErr::new::<exceptions::PyValueError, String>(format!("{}", err).into())
            })?;
        Ok(self
            .event_context
            .extract_records_from_expr(
                obs_dates_config,
                entity_query_config,
                Query::SelectExpr(query),
                experiment_id.map(|v| v.into()),
                chunk_size,
            )
            .map_err(|err| {
                PyErr::new::<exceptions::PyValueError, String>(format!("{}", err).into())
            })?)
    }

    pub fn extract_features(
        &mut self,
        obs_dates_config_json: String,
        entity_query_config: String,
        exprs_str: Vec<String>,
        experiment_id: Option<String>,
        chunk_size: Option<usize>,
    ) -> PyResult<(Vec<SmallString>, Vec<Vec<Value>>)> {
        let obs_dates_config: ObservationDatesConfig = serde_json::from_str(&obs_dates_config_json)
            .map_err(|err| {
                PyErr::new::<exceptions::PyValueError, String>(format!("{}", err).into())
            })?;
        let entity_query_config: EventQueryConfig = serde_json::from_str(&entity_query_config)
            .map_err(|err| {
                PyErr::new::<exceptions::PyValueError, String>(format!("{}", err).into())
            })?;
        Ok(self
            .event_context
            .extract_records_from_expr(
                obs_dates_config,
                entity_query_config,
                Query::VecExpr(exprs_str),
                experiment_id.map(|v| v.into()),
                chunk_size,
            )
            .map_err(|err| {
                PyErr::new::<exceptions::PyValueError, String>(format!("{}", err).into())
            })?)
    }

    pub fn flush(&self) {
        self.event_context.event_store.flush();
    }

    pub fn flush_experiments(&self) {
        self.event_context.event_store.flush_experiments();
    }

    pub fn flush_experiment(&self, experiment_id: String) {
        self.event_context
            .event_store
            .flush_experiment(experiment_id.into());
    }
}

/// A Python module implemented in Rust.
#[pymodule]
fn fexpress_python(_: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<EventContext>()?;
    Ok(())
}
