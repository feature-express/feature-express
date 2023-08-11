mod nba_column_store;

extern crate fexpress;

use fexpress::event::Event;
use fexpress::event_index::EventQueryConfig;
use fexpress::event_store::EventStoreSettings;
use fexpress::obs_dates::ObservationDatesConfig;
use schemars::schema_for;

use std::fs::File;
use std::io::Write;

pub fn main() {
    let schema = schema_for!(ObservationDatesConfig);
    let mut output = File::create("fexpress-py/fexpress_python/sdk/observation_dates_config.json")
        .expect("Cannot open file");
    output.write_all(serde_json::to_string_pretty(&schema).unwrap().as_bytes());

    let schema = schema_for!(Event);
    let mut output =
        File::create("fexpress-py/fexpress_python/sdk/event.json").expect("Cannot open file");
    output.write_all(serde_json::to_string_pretty(&schema).unwrap().as_bytes());

    let schema = schema_for!(EventStoreSettings);
    let mut output = File::create("fexpress-py/fexpress_python/sdk/event_store_settings.json")
        .expect("Cannot open file");
    output.write_all(serde_json::to_string_pretty(&schema).unwrap().as_bytes());

    let schema = schema_for!(EventQueryConfig);
    let mut output = File::create("fexpress-py/fexpress_python/sdk/event_query_config.json")
        .expect("Cannot open file");
    output.write_all(serde_json::to_string_pretty(&schema).unwrap().as_bytes());
}
