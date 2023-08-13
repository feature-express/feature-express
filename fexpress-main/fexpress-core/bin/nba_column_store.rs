// extern crate fexpress;
//
// use chrono::NaiveDateTime;
// use fexpress::column_event_store::ColumnStore;
// use fexpress::column_event_store::Settings;
// use fexpress::event::{Event, EventType};
// use fexpress::tests::fake_nba::generate_nba_game_events;
//
// pub fn main() {
//     let mut events = generate_nba_game_events(10);
//     let mut column_store = ColumnStore {
//         tables: Default::default(),
//         settings: Settings { block_size: 10 },
//         last_timestamp: NaiveDateTime::MIN,
//     };
//     events.sort_by_key(|e| e.event_time);
//     for event in &events {
//         column_store
//             .insert_new_event_incremental_incremental(event)
//             .unwrap();
//     }
//     println!("{:#?}", column_store);
// }
