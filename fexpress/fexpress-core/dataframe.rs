use std::collections::HashSet;
use std::iter::FromIterator;

use prettytable::format::consts::FORMAT_NO_BORDER_LINE_SEPARATOR;
use prettytable::{Row as PTRow, Table};

use crate::types::FLOAT;
use crate::value::Value;

pub struct DataFrame {
    columns: Vec<String>,
    data: Vec<Vec<Value>>,
}

#[derive(PartialEq, Eq, Hash, Clone)]
struct Row(Vec<Value>);

impl DataFrame {
    // Create new DataFrame
    pub fn new(columns: Vec<String>, data: Vec<Vec<Value>>) -> DataFrame {
        DataFrame { columns, data }
    }

    // Select the first n rows
    pub fn head(&self, n: usize) -> DataFrame {
        DataFrame::new(
            self.columns.clone(),
            self.data.iter().take(n).cloned().collect(),
        )
    }

    // Select the last n rows
    pub fn tail(&self, n: usize) -> DataFrame {
        let len = self.data.len();
        DataFrame::new(
            self.columns.clone(),
            self.data.iter().skip(len - n).cloned().collect(),
        )
    }

    // Select columns by names
    pub fn select_columns(&self, col_names: Vec<&str>) -> Option<DataFrame> {
        let indices: Vec<usize> = col_names
            .iter()
            .map(|&name| self.columns.iter().position(|c| c == name))
            .collect::<Option<Vec<usize>>>()?;

        let selected_columns = indices.iter().map(|&i| self.columns[i].clone()).collect();
        let selected_data = self
            .data
            .iter()
            .map(|row| indices.iter().map(|&i| row[i].clone()).collect())
            .collect();

        Some(DataFrame::new(selected_columns, selected_data))
    }

    // Display the DataFrame
    pub fn display(&self) {
        let mut table = Table::new();
        table.set_format(*FORMAT_NO_BORDER_LINE_SEPARATOR);

        // Add headers
        let headers: Vec<String> = self.columns.clone();
        table.add_row(PTRow::from_iter(headers));

        // Add rows
        for row in &self.data {
            let stringified_row: Vec<String> = row.iter().map(|value| value.to_string()).collect();
            table.add_row(PTRow::from_iter(stringified_row));
        }

        table.printstd();
    }

    pub fn col(&self, col_name: &str) -> Option<Series> {
        let index = self.columns.iter().position(|c| c == col_name)?;

        let selected_data = self.data.iter().map(|row| row[index].clone()).collect();

        Some(Series::new(col_name.to_string(), selected_data))
    }

    // Drop duplicates based on keys
    pub fn drop_duplicates(&self, keys: Vec<&str>) -> Option<DataFrame> {
        // Determine the column indices for the keys
        let key_indices: Vec<usize> = keys
            .iter()
            .map(|&key| self.columns.iter().position(|c| c == key))
            .collect::<Option<Vec<usize>>>()?;

        // Use a HashSet to detect and ignore duplicates
        let mut unique_rows: HashSet<Row> = HashSet::new();
        let mut new_data: Vec<Vec<Value>> = vec![];

        for row in &self.data {
            let key_row: Vec<Value> = key_indices.iter().map(|&i| row[i].clone()).collect();

            // Only add the row to the new_data if it's not already present
            if unique_rows.insert(Row(key_row)) {
                new_data.push(row.clone());
            }
        }

        Some(DataFrame::new(self.columns.clone(), new_data))
    }
}

pub struct Series {
    name: String,
    data: Vec<Value>,
}

impl Series {
    pub fn new(name: String, data: Vec<Value>) -> Series {
        Series { name, data }
    }

    // Compute the sum of the series
    pub fn sum(&self) -> FLOAT {
        self.data
            .iter()
            .map(|value| Into::<FLOAT>::into(value.clone()))
            .sum()
    }
}
