use std::fs;
use std::path::{Path, PathBuf};
use crate::types::{SSTableMetadata, Value};
use crate::storage::columnar::encoding::rle::{RleEncoder, RleRun};
use crate::storage::columnar::encoding::delta::{DeltaEncoder, DeltaEncoded};
use crate::errors::StorageError;

pub struct ColumnarReader {
    pub segment_path: PathBuf,
    pub metadata: SSTableMetadata,
}

impl ColumnarReader {
    pub fn new(segment_path: PathBuf) -> Result<Self, StorageError> {
        let meta_path = segment_path.join("meta.toml");
        let content = fs::read_to_string(meta_path)?;
        let metadata: SSTableMetadata = toml::from_str(&content).map_err(|e| {
            StorageError::ReadError(std::io::Error::new(std::io::ErrorKind::Other, e))
        })?;
        
        Ok(Self { segment_path, metadata })
    }

    pub fn read_column_i64(&self, col_name: &str) -> Result<Vec<i64>, StorageError> {
        let file_path = self.segment_path.join(format!("{}.col", col_name));
        let bytes = fs::read(file_path)?;
        
        let encoding = self.metadata.column_encodings.get(col_name)
            .map(|s| s.as_str())
            .unwrap_or("bincode");

        match encoding {
            "rle" => {
                let runs: Vec<RleRun> = bincode::deserialize(&bytes).map_err(|e| {
                    StorageError::ReadError(std::io::Error::new(std::io::ErrorKind::Other, e))
                })?;
                Ok(RleEncoder::decode(&runs).into_iter().map(|v| v as i64).collect())
            }
            "delta" => {
                let encoded: DeltaEncoded = bincode::deserialize(&bytes).map_err(|e| {
                    StorageError::ReadError(std::io::Error::new(std::io::ErrorKind::Other, e))
                })?;
                Ok(DeltaEncoder::decode(&encoded).into_iter().map(|v| v as i64).collect())
            }
            _ => {
                let values: Vec<Value> = bincode::deserialize(&bytes).map_err(|e| {
                    StorageError::ReadError(std::io::Error::new(std::io::ErrorKind::Other, e))
                })?;
                Ok(values.into_iter().map(|v| match v {
                    Value::Int(i) => i,
                    Value::Float(f) => f as i64,
                    _ => 0,
                }).collect())
            }
        }
    }

    pub fn read_column_f64(&self, col_name: &str) -> Result<Vec<f64>, StorageError> {
        // Similar to i64 but for floats. For now, since our RLE/Delta only handles i64,
        // we fallback to bincode for floats.
        let file_path = self.segment_path.join(format!("{}.col", col_name));
        let bytes = fs::read(file_path)?;
        let values: Vec<Value> = bincode::deserialize(&bytes).map_err(|e| {
            StorageError::ReadError(std::io::Error::new(std::io::ErrorKind::Other, e))
        })?;
        Ok(values.into_iter().map(|v| match v {
            Value::Float(f) => f,
            Value::Int(i) => i as f64,
            _ => 0.0,
        }).collect())
    }

    pub fn read_column_string(&self, col_name: &str) -> Result<Vec<String>, StorageError> {
        let file_path = self.segment_path.join(format!("{}.col", col_name));
        let bytes = fs::read(file_path)?;
        let values: Vec<Value> = bincode::deserialize(&bytes).map_err(|e| {
            StorageError::ReadError(std::io::Error::new(std::io::ErrorKind::Other, e))
        })?;
        Ok(values.into_iter().map(|v| match v {
            Value::String(s) => s,
            _ => "".to_string(),
        }).collect())
    }

    pub fn read_raw_values(&self, col_name: &str) -> Result<Vec<Value>, StorageError> {
        let _file_path = self.segment_path.join(format!("{}.col", col_name));
        // ... existing read_column code renamed
        // let bytes = fs::read(file_path)?;
        // Handle decoding and convert to Value... (not used in Hot Path)
        Ok(vec![]) // Placeholder
    }
}
