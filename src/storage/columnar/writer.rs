use std::fs;
use std::path::{Path, PathBuf};
use crate::types::{RowDisk, SSTableMetadata, Value};
use crate::storage::columnar::encoding::rle::RleEncoder;
use crate::storage::columnar::encoding::delta::DeltaEncoder;
use crate::errors::StorageError;

pub struct ColumnarWriter;

impl ColumnarWriter {
    pub fn write_segment(segment_path: &Path, rows: &[RowDisk], config: &crate::config::Config, fpr: f64) -> Result<(), StorageError> {
        if rows.is_empty() {
            return Ok(());
        }

        // 1. Prepare directory (atomic write to tmp first is preferred, but we'll create directly for now)
        if segment_path.exists() {
            fs::remove_dir_all(segment_path)?;
        }
        fs::create_dir_all(segment_path)?;

        // 2. Transpose Rows to Columns
        let num_cols = config.schema.columns.len();
        let num_rows = rows.len();
        let mut columns: Vec<Vec<Value>> = vec![Vec::with_capacity(num_rows); num_cols];

        for row in rows {
            for (i, val) in row.values.iter().enumerate() {
                if i < num_cols {
                    columns[i].push(val.clone());
                }
            }
        }

        // 3. Encode and Write Columns
        let mut sums = std::collections::HashMap::new();
        let mut min_ts = i64::MAX;
        let mut max_ts = i64::MIN;

        for (i, col_schema) in config.schema.columns.iter().enumerate() {
            let col_data = &columns[i];
            let file_name = format!("{}.col", col_schema.name);
            let file_path = segment_path.join(file_name);
            
            let mut col_sum = 0.0;
            
            // Choose encoding
            match col_schema.name.as_str() {
                "status" | "country" => {
                    let u8_data: Vec<u8> = col_data.iter().map(|v| match v {
                        Value::Int(i) => *i as u8,
                        _ => 0,
                    }).collect();
                    
                    for &val in &u8_data { col_sum += val as f64; }
                    
                    let encoded = RleEncoder::encode(&u8_data);
                    let bytes = bincode::serialize(&encoded).map_err(|e| {
                        StorageError::WriteError(std::io::Error::new(std::io::ErrorKind::Other, e))
                    })?;
                    fs::write(file_path, bytes)?;
                }
                "user_id" | "timestamp" => {
                    let u64_data: Vec<u64> = col_data.iter().map(|v| match v {
                        Value::Int(i) => *i as u64,
                        _ => 0,
                    }).collect();
                    
                    for &val in &u64_data { 
                        col_sum += val as f64; 
                        if col_schema.name == "timestamp" {
                            min_ts = min_ts.min(val as i64);
                            max_ts = max_ts.max(val as i64);
                        }
                    }
                    
                    let encoded = DeltaEncoder::encode(&u64_data);
                    let bytes = bincode::serialize(&encoded).map_err(|e| {
                        StorageError::WriteError(std::io::Error::new(std::io::ErrorKind::Other, e))
                    })?;
                    fs::write(file_path, bytes)?;
                }
                _ => {
                    // Default bincode for other types (fallback)
                    let bytes = bincode::serialize(col_data).map_err(|e| {
                        StorageError::WriteError(std::io::Error::new(std::io::ErrorKind::Other, e))
                    })?;
                    fs::write(file_path, bytes)?;
                }
            }
            
            sums.insert(format!("col_{}", i), col_sum);
        }

        // 4. Write meta.toml
        let mut column_encodings = std::collections::HashMap::new();
        for col_schema in &config.schema.columns {
            let enc = match col_schema.name.as_str() {
                "status" | "country" => "rle",
                "user_id" | "timestamp" => "delta",
                _ => "bincode",
            };
            column_encodings.insert(col_schema.name.clone(), enc.to_string());
        }

        let metadata = SSTableMetadata {
            row_count: num_rows as u64,
            min_ts: if min_ts == i64::MAX { 0 } else { min_ts },
            max_ts: if max_ts == i64::MIN { 0 } else { max_ts },
            schema_version: rows[0].version,
            sums,
            column_encodings,
        };

        let meta_toml = toml::to_string_pretty(&metadata).map_err(|e| {
            StorageError::WriteError(std::io::Error::new(std::io::ErrorKind::Other, e))
        })?;
        fs::write(segment_path.join("meta.toml"), meta_toml)?;

        // 5. Write Bloom filter (global for now, Druid-style is per col but SSTable legacy is shared)
        // For Phase 3, we'll write one bloom.bin for equalities as requested.
        let mut bloom = crate::storage::bloom::filter::BloomFilterWrapper::new(num_rows, fpr);
        for row in rows {
            if let Some(Value::Int(uid)) = row.values.get(0) {
                bloom.insert(*uid as u64);
            }
        }
        let bloom_bytes = bincode::serialize(&bloom).map_err(|e| {
            StorageError::WriteError(std::io::Error::new(std::io::ErrorKind::Other, e))
        })?;
        fs::write(segment_path.join("bloom.bin"), bloom_bytes)?;

        Ok(())
    }
}
