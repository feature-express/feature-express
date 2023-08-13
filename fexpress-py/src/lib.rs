use pyo3::exceptions;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList, PyString};

use fexpress_core::event::{AttributeName, Event};
use fexpress_core::event_index::{
    EventContext as EventContextR, EventScopeConfig, QueryConfig, RawQuery,
};
use fexpress_core::event_store::EventStore;
use fexpress_core::map::HashMap;
use fexpress_core::obs_dates::ObservationDatesConfig;
use fexpress_core::sstring::SmallString;
use fexpress_core::value::{Value, ValueType};

#[pyclass(unsendable)]
pub struct EventContext {
    event_context: EventContextR,
}

#[pymethods]
impl EventContext {
    #[new]
    pub fn new() -> PyResult<Self> {
        Ok(Self {
            event_context: EventContextR::new_memory(),
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
        event_scope_config_json: String,
        query: PyObject,
        query_config_json: String,
        experiment_id: Option<String>,
        chunk_size: Option<usize>,
    ) -> PyResult<(Vec<SmallString>, Vec<Vec<Value>>)> {
        let raw_query = Python::with_gil(|py| {
            let query = query.as_ref(py);
            if let Ok(query_str) = query.downcast::<PyString>() {
                Ok(RawQuery::SelectExpr(query_str.to_string()))
            } else if let Ok(exprs_list) = query.downcast::<PyList>() {
                let mut vec_exprs = Vec::new();
                for item in exprs_list.iter() {
                    if let Ok(py_str) = item.downcast::<PyString>() {
                        vec_exprs.push(py_str.to_string());
                    } else {
                        return Err(PyErr::new::<exceptions::PyTypeError, _>(
                            "List items must all be strings",
                        ));
                    }
                }
                Ok(RawQuery::VecExpr(vec_exprs))
            } else {
                Err(PyErr::new::<exceptions::PyTypeError, _>(
                    "Expected a string or a list of strings",
                ))
            }
        })?;

        let obs_dates_config: ObservationDatesConfig = serde_json::from_str(&obs_dates_config_json)
            .map_err(|err| {
                PyErr::new::<exceptions::PyValueError, String>(format!("{:#}", err).into())
            })?;
        let event_scope_config: EventScopeConfig = serde_json::from_str(&event_scope_config_json)
            .map_err(|err| {
            PyErr::new::<exceptions::PyValueError, String>(format!("{:#}", err).into())
        })?;
        let query_config: QueryConfig =
            serde_json::from_str(&query_config_json).map_err(|err| {
                PyErr::new::<exceptions::PyValueError, String>(format!("{:#}", err).into())
            })?;

        Ok(self
            .event_context
            .extract_records_from_expr(
                obs_dates_config,
                event_scope_config,
                raw_query,
                &query_config,
                experiment_id.map(|v| v.into()),
                chunk_size,
            )
            .map_err(|err| {
                PyErr::new::<exceptions::PyValueError, String>(format!("{:#}", err).into())
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

    pub fn schema(&self) -> String {
        serde_json::to_string_pretty(&self.event_context.event_store.get_schema())
            .expect("Cannot extract schema")
    }

    pub fn events(&self, py: Python) -> PyResult<Vec<PyObject>> {
        self.event_context
            .event_store
            .all_events()
            .map(|events| {
                events
                    .iter()
                    .map(|event| {
                        serde_json::to_string(&(**event))
                            .map(|json_str| json_str.into_py(py))
                            .map_err(|err| {
                                PyErr::new::<exceptions::PyValueError, String>(
                                    format!("{:#}", err).into(),
                                )
                            })
                    })
                    .collect::<Result<Vec<PyObject>, _>>() // Collect results, propagating errors
            })
            .map_err(|err| {
                PyErr::new::<exceptions::PyValueError, String>(format!("{:#}", err).into())
            })?
    }
}

/// A Python module implemented in Rust.
#[pymodule]
fn fexpress(_: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<EventContext>()?;
    Ok(())
}
