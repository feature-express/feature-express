mod chunk_benchmark;

extern crate fexpress_core;

use fexpress_core::event::Event;
use fexpress_core::event_index::{EventScopeConfig, QueryConfig};
use fexpress_core::obs_dates::ObservationDatesConfig;
use schemars::schema_for;

use std::fs::File;
use std::io::Write;

pub fn main() {
    macro_rules! write_schema_to_file {
        ($schema_type:ty, $file_path:expr) => {
            let schema = schema_for!($schema_type);
            let mut output = File::create($file_path).expect("Cannot open file");
            output
                .write_all(serde_json::to_string_pretty(&schema).unwrap().as_bytes())
                .expect("Cannot write to file");
        };
    }

    write_schema_to_file!(
        ObservationDatesConfig,
        "fexpress-py/fexpress/sdk/observation_dates_config.json"
    );
    write_schema_to_file!(Event, "fexpress-py/fexpress/sdk/event.json");
    write_schema_to_file!(QueryConfig, "fexpress-py/fexpress/sdk/query_config.json");
    write_schema_to_file!(
        EventScopeConfig,
        "fexpress-py/fexpress/sdk/event_scope_config.json"
    );
}
