use std::error::Error;
use std::io::Write;
use std::sync::{Arc, RwLock};

use csv::WriterBuilder;
use postgres::Client;
use serde::Serialize;

trait ToCsvRow<T> {
    fn convert(val: T) -> String;
}

pub struct BatchInsert {
    pub client: Arc<RwLock<Client>>,
    pub table_name: String,
    pub batch_size: usize,
    pub buffer: Vec<u8>,
}

impl BatchInsert {
    pub fn new(client: Arc<RwLock<Client>>, table_name: String, batch_size: usize) -> Self {
        Self {
            client,
            table_name,
            batch_size,
            buffer: vec![],
        }
    }

    pub fn insert<T: Serialize>(&self, vals: Vec<T>) -> Result<(), Box<dyn Error>> {
        let mut wtr = WriterBuilder::new().has_headers(false).from_writer(vec![]);
        for val in vals.iter() {
            wtr.serialize(val).unwrap();
        }
        let data: Vec<u8> = String::from_utf8(wtr.into_inner().unwrap())
            .unwrap()
            .bytes()
            .collect();
        self.postgres_insert(&data[..]).unwrap();
        Ok(())
    }

    fn postgres_insert(&self, data: &[u8]) -> Result<u64, Box<dyn Error>> {
        let mut client = self.client.write().unwrap();
        let mut writer = client
            .copy_in(format!("COPY {} FROM STDIN WITH (FORMAT CSV)", self.table_name).as_str())
            .unwrap();
        writer.write_all(data).unwrap();
        let rows = writer.finish().unwrap();
        Ok(rows)
    }
}
