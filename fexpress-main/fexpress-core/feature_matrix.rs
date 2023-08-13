use crate::obs_dates::ObservationTime;
use crate::sstring::SmallString;
use crate::types::Entities;
use crate::value::Value;
use anyhow::Result;
use csv::Writer;
use std::collections::HashMap;

type Feature = SmallString;
type Index = (Entities, ObservationTime);

pub struct FeatureMatrix {
    matrix: HashMap<Index, HashMap<Feature, Value>>,
    not_calculated_in_row: HashMap<Index, usize>,
    not_calculated_in_col: HashMap<Feature, usize>,
}

impl FeatureMatrix {
    pub fn new() -> Self {
        Self {
            matrix: HashMap::new(),
            not_calculated_in_row: HashMap::new(),
            not_calculated_in_col: HashMap::new(),
        }
    }

    pub fn is_calculated(&self, index: &Index, feature: &Feature) -> Result<bool> {
        match self.matrix.get(index) {
            Some(row) => match row.get(feature) {
                Some(value) => Ok(*value != Value::NotCalculatedYet),
                None => Err(anyhow::anyhow!("Feature not found")),
            },
            None => Err(anyhow::anyhow!("Index not found")),
        }
    }

    pub fn is_row_calculated(&self, index: &Index) -> Result<bool> {
        match self.not_calculated_in_row.get(index) {
            Some(count) => Ok(*count == 0),
            None => Err(anyhow::anyhow!("Index not found")),
        }
    }

    pub fn get_row(&self, index: &Index) -> Result<HashMap<Feature, Value>> {
        match self.matrix.get(index) {
            Some(row) => Ok(row.clone()),
            None => Err(anyhow::anyhow!("Index not found")),
        }
    }

    pub fn get_column(&self, feature: &Feature) -> Result<HashMap<Index, Value>> {
        let mut column = HashMap::new();

        for (idx, row) in &self.matrix {
            if let Some(value) = row.get(feature) {
                column.insert(idx.clone(), value.clone());
            }
        }

        if column.is_empty() {
            Err(anyhow::anyhow!("Feature not found"))
        } else {
            Ok(column)
        }
    }

    pub fn get_non_calculated_cells(&self) -> Vec<(Index, Feature)> {
        let mut not_calculated = Vec::new();

        for (idx, row) in &self.matrix {
            for (feature, value) in row {
                if *value == Value::NotCalculatedYet {
                    not_calculated.push((idx.clone(), feature.clone()));
                }
            }
        }

        not_calculated
    }

    pub fn get_calculation_progress(&self) -> f64 {
        let total_cells = self.matrix.iter().map(|(_, row)| row.len()).sum::<usize>();
        let calculated_cells = total_cells - self.not_calculated_in_row.values().sum::<usize>();
        calculated_cells as f64 / total_cells as f64
    }

    pub fn get(&self, index: &Index, feature: &Feature) -> Result<Value> {
        match self.matrix.get(index) {
            Some(row) => match row.get(feature) {
                Some(value) => Ok(value.clone()),
                None => Err(anyhow::anyhow!("Feature not found")),
            },
            None => Err(anyhow::anyhow!("Index not found")),
        }
    }

    pub fn insert(&mut self, index: Index, feature: Feature, value: Value) -> Result<()> {
        let row = self.matrix.entry(index.clone()).or_insert(HashMap::new());

        let old_value = row.insert(feature.clone(), value.clone());

        if let Some(old_value) = old_value {
            if old_value == Value::NotCalculatedYet && value != Value::NotCalculatedYet {
                let count = self.not_calculated_in_row.entry(index).or_insert(0);
                *count -= 1;

                let count = self.not_calculated_in_col.entry(feature).or_insert(0);
                *count -= 1;
            }
        }

        Ok(())
    }

    pub fn batch_get(
        &self,
        cells: &[(Index, Feature)],
    ) -> Result<HashMap<(Index, Feature), Value>> {
        let mut values = HashMap::new();

        for (idx, feature) in cells {
            match self.get(idx, feature) {
                Ok(value) => {
                    values.insert((idx.clone(), feature.clone()), value);
                }
                Err(err) => return Err(err),
            }
        }

        Ok(values)
    }

    pub fn batch_insert(&mut self, values: &HashMap<(Index, Feature), Value>) -> Result<()> {
        for ((idx, feature), value) in values {
            self.insert(idx.clone(), feature.clone(), value.clone())?;
        }

        Ok(())
    }

    pub fn feature_mapping(&self) -> HashMap<usize, Feature> {
        let features: Vec<Feature> = if let Some(first_row) = self.matrix.values().next() {
            first_row.keys().cloned().collect()
        } else {
            Vec::new()
        };

        features
            .into_iter()
            .enumerate()
            .map(|(i, feature)| (i, feature))
            .collect()
    }

    pub fn index_mapping(&self) -> HashMap<usize, Index> {
        self.matrix
            .keys()
            .cloned()
            .enumerate()
            .map(|(i, index)| (i, index))
            .collect()
    }

    pub fn write_to_csv(&self, path: &str) -> Result<()> {
        let mut writer = Writer::from_path(path)?;

        let feature_mapping = self.feature_mapping();
        let index_mapping = self.index_mapping();

        // Write the header
        let header: Vec<String> = (0..feature_mapping.len())
            .map(|i| feature_mapping[&i].clone().to_string())
            .collect();
        writer.write_record(&header)?;

        // Write each record
        // for (row_i, row) in self.matrix.iter().enumerate() {
        //     let mut record: Vec<String> = vec![index_mapping[&row_i].clone()];
        //     record.extend(row.iter().map(|v| format!("{}", v)));  // You would need to implement Display or ToString for Value
        //     writer.write_record(&record)?;
        // }

        writer.flush()?;

        Ok(())
    }
}

pub struct DenseFeatureMatrix {
    pub matrix: Vec<Vec<Value>>,
    pub index_mapping: HashMap<usize, Index>,
    pub feature_mapping: HashMap<usize, Feature>,
}

impl From<FeatureMatrix> for DenseFeatureMatrix {
    fn from(matrix: FeatureMatrix) -> Self {
        let indices: Vec<Index> = matrix.matrix.keys().cloned().collect();
        let features: Vec<Feature> = matrix.feature_mapping().values().cloned().collect();

        let dense_matrix: Vec<Vec<Value>> = indices
            .iter()
            .map(|index| {
                features
                    .iter()
                    .map(|feature| {
                        matrix
                            .matrix
                            .get(index)
                            .and_then(|row| row.get(feature))
                            .cloned()
                            .unwrap_or(Value::NotCalculatedYet)
                    })
                    .collect()
            })
            .collect();

        let index_mapping: HashMap<usize, Index> = indices
            .into_iter()
            .enumerate()
            .map(|(i, index)| (i, index))
            .collect();
        let feature_mapping = matrix.feature_mapping();

        Self {
            matrix: dense_matrix,
            index_mapping,
            feature_mapping,
        }
    }
}
