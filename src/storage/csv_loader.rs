use std::fs::File;
use std::path::Path;
use csv::ReaderBuilder;
use crate::types::{RowDisk, Value};
use crate::config::Config;
use crate::errors::StorageError;

pub struct CsvLoader;

impl CsvLoader {
    pub fn load_csv<P: AsRef<Path>>(path: P, config: &Config) -> Result<Vec<RowDisk>, StorageError> {
        let file = File::open(path).map_err(StorageError::ReadError)?;
        let mut rdr = ReaderBuilder::new()
            .has_headers(true)
            .from_reader(file);

        let mut data = Vec::new();
        let headers = rdr.headers().map_err(|e| StorageError::InvalidFormat(e.to_string()))?.clone();
        
        // Map header names to indices in the schema
        let mut col_map = Vec::new();
        for header in headers.iter() {
            let idx = config.schema.columns.iter().position(|c| c.name == header)
                .ok_or_else(|| StorageError::InvalidFormat(format!("Column {} not in schema", header)))?;
            col_map.push(idx);
        }

        for result in rdr.records() {
            let record = result.map_err(|e| StorageError::InvalidFormat(e.to_string()))?;
            let mut values = vec![Value::Int(0); config.schema.columns.len()];
            
            for (i, field) in record.iter().enumerate() {
                let schema_idx = col_map[i];
                let col_type = &config.schema.columns[schema_idx].r#type;
                
                let val = match col_type.as_str() {
                    "u64" | "i64" | "u32" | "u8" | "i8" => {
                        let v = field.parse::<i64>().map_err(|e| StorageError::InvalidFormat(e.to_string()))?;
                        Value::Int(v)
                    }
                    "f64" | "f32" => {
                        let v = field.parse::<f64>().map_err(|e| StorageError::InvalidFormat(e.to_string()))?;
                        Value::Float(v)
                    }
                    _ => Value::String(field.to_string()),
                };
                values[schema_idx] = val;
            }

            data.push(RowDisk {
                version: 1,
                crc: 0,
                seq: 0,
                values,
            });
        }

        Ok(data)
    }
}
