use std::fs::File;
use std::io::{Read, Write, Seek, SeekFrom, BufWriter};
use crate::types::{RowDisk, Value};
use crate::errors::StorageError;
use serde::{Deserialize, Serialize};

pub const COLUMNAR_MAGIC: &[u8; 4] = b"COL1";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnMetadata {
    pub name: String,
    pub data_type: String, // "i64", "f64", "string"
    pub offset: u64,
    pub length: u64,
    pub sum: f64, // Pre-calculated sum for fast-path
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnarMetadata {
    pub row_count: u64,
    pub columns: Vec<ColumnMetadata>,
    pub sums: std::collections::HashMap<String, f64>,
}

pub struct ColumnarWriter {
    writer: BufWriter<File>,
}

impl ColumnarWriter {
    pub fn new(path: &str) -> Result<Self, StorageError> {
        let file = File::create(path)?;
        Ok(Self {
            writer: BufWriter::new(file),
        })
    }

    pub fn write_sstable(&mut self, rows: &[RowDisk], config: &crate::config::Config) -> Result<crate::types::InsertionProfile, StorageError> {
        let mut profile = crate::types::InsertionProfile::default();
        if rows.is_empty() {
            return Ok(profile);
        }
        
        // 1. Magic
        self.writer.write_all(COLUMNAR_MAGIC)?;

        let row_count = rows.len() as u64;
        let mut column_metadata = Vec::new();
        let mut global_sums = std::collections::HashMap::new();

        let start_pos = 4 + 8; // magic + metadata_offset
        self.writer.write_all(&0u64.to_le_bytes())?;

        let mut current_offset = start_pos;

        // Transpose and write columns
        for (col_idx, col_def) in config.schema.columns.iter().enumerate() {
            let start_transpose = std::time::Instant::now();
            let data_type = match col_def.r#type.as_str() {
                "Int" | "int" | "i64" | "u64" | "u32" | "i32" | "u8" | "i8" => "i64",
                "Float" | "float" | "f64" | "f32" => "f64",
                _ => "string",
            };

            let start_col_offset = current_offset;
            let mut col_sum = 0.0;
            
            match data_type {
                "i64" => {
                    for row in rows {
                        let val = row.values.get(col_idx).unwrap_or(&Value::Int(0));
                        let i = match val {
                            Value::Int(v) => *v,
                            _ => 0,
                        };
                        col_sum += i as f64;
                        self.writer.write_all(&i.to_le_bytes())?;
                        current_offset += 8;
                    }
                }
                "f64" => {
                    for row in rows {
                        let val = row.values.get(col_idx).unwrap_or(&Value::Float(0.0));
                        let f = match val {
                            Value::Float(v) => *v,
                            _ => 0.0,
                        };
                        col_sum += f;
                        self.writer.write_all(&f.to_le_bytes())?;
                        current_offset += 8;
                    }
                }
                _ => {
                    let mut data_buf = Vec::new();
                    let mut lengths = Vec::new();
                    for row in rows {
                        let s = match row.values.get(col_idx) {
                            Some(Value::String(s)) => s.as_str(),
                            _ => "",
                        };
                        lengths.push(s.len() as u32);
                        data_buf.extend_from_slice(s.as_bytes());
                    }
                    
                    for len in lengths {
                        self.writer.write_all(&len.to_le_bytes())?;
                        current_offset += 4;
                    }
                    self.writer.write_all(&data_buf)?;
                    current_offset += data_buf.len() as u64;
                }
            }

            // We attribute the transpose and buffered write to transposition_ms
            // Proper I/O write will be measured during flush.
            profile.transposition_ms += start_transpose.elapsed().as_secs_f64() * 1000.0;

            column_metadata.push(ColumnMetadata {
                name: col_def.name.clone(),
                data_type: data_type.to_string(),
                offset: start_col_offset,
                length: current_offset - start_col_offset,
                sum: col_sum,
            });
            global_sums.insert(col_def.name.clone(), col_sum);
        }

        // Write metadata at the end
        let start_meta = std::time::Instant::now();
        let metadata = ColumnarMetadata {
            row_count,
            columns: column_metadata,
            sums: global_sums,
        };
        let metadata_bytes = serde_json::to_vec(&metadata).map_err(|e| {
            StorageError::WriteError(std::io::Error::new(std::io::ErrorKind::Other, e))
        })?;
        
        let meta_offset = current_offset;
        self.writer.write_all(&metadata_bytes)?;
        profile.metadata_serialize_ms = start_meta.elapsed().as_secs_f64() * 1000.0;
        
        // Finalize by writing the metadata offset back at the start
        let start_io = std::time::Instant::now();
        self.writer.flush()?;
        let mut file = self.writer.get_ref().try_clone()?;
        file.seek(SeekFrom::Start(4))?;
        file.write_all(&meta_offset.to_le_bytes())?;
        file.flush()?;
        profile.io_write_ms = start_io.elapsed().as_secs_f64() * 1000.0;

        Ok(profile)
    }
}

pub struct ColumnarReader {
    path: String,
}

impl ColumnarReader {
    pub fn new(path: &str) -> Self {
        Self { path: path.to_string() }
    }

    pub fn get_metadata(path: &str) -> Result<ColumnarMetadata, StorageError> {
        let mut file = File::open(path)?;
        let mut magic = [0u8; 4];
        file.read_exact(&mut magic)?;
        if &magic != COLUMNAR_MAGIC {
            return Err(StorageError::InvalidFormat("Invalid columnar magic".to_string()));
        }

        let mut meta_offset_bytes = [0u8; 8];
        file.read_exact(&mut meta_offset_bytes)?;
        let meta_offset = u64::from_le_bytes(meta_offset_bytes);

        file.seek(SeekFrom::Start(meta_offset))?;
        let mut meta_bytes = Vec::new();
        file.read_to_end(&mut meta_bytes)?;

        let metadata: ColumnarMetadata = serde_json::from_slice(&meta_bytes).map_err(|e| {
            StorageError::InvalidFormat(format!("Failed to parse JSON metadata: {:?}", e))
        })?;

        Ok(metadata)
    }

    pub fn read_column_i64(&self, col_name: &str) -> Result<Vec<i64>, StorageError> {
        let meta = Self::get_metadata(&self.path)?;
        let col = meta.columns.iter().find(|c| c.name == col_name)
            .ok_or_else(|| StorageError::ReadError(std::io::Error::new(std::io::ErrorKind::NotFound, "Column not found")))?;
        
        if col.data_type != "i64" {
            return Err(StorageError::ReadError(std::io::Error::new(std::io::ErrorKind::InvalidData, "Column is not i64")));
        }

        let mut file = File::open(&self.path)?;
        file.seek(SeekFrom::Start(col.offset))?;
        
        let mut data = vec![0i64; meta.row_count as usize];
        let bytes = unsafe {
            std::slice::from_raw_parts_mut(data.as_mut_ptr() as *mut u8, data.len() * 8)
        };
        file.read_exact(bytes)?;
        
        Ok(data)
    }

    pub fn read_column_f64(&self, col_name: &str) -> Result<Vec<f64>, StorageError> {
        let meta = Self::get_metadata(&self.path)?;
        let col = meta.columns.iter().find(|c| c.name == col_name)
            .ok_or_else(|| StorageError::ReadError(std::io::Error::new(std::io::ErrorKind::NotFound, "Column not found")))?;
        
        if col.data_type != "f64" {
            return Err(StorageError::ReadError(std::io::Error::new(std::io::ErrorKind::InvalidData, "Column is not f64")));
        }

        let mut file = File::open(&self.path)?;
        file.seek(SeekFrom::Start(col.offset))?;
        
        let mut data = vec![0.0f64; meta.row_count as usize];
        let bytes = unsafe {
            std::slice::from_raw_parts_mut(data.as_mut_ptr() as *mut u8, data.len() * 8)
        };
        file.read_exact(bytes)?;
        
        Ok(data)
    }
}
