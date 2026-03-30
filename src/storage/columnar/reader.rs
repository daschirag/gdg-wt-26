use crate::storage::columnar::encoding::rle::{RleEncoder, RleRun};
use crate::storage::columnar::encoding::delta::{DeltaEncoder, DeltaEncoded};
use crate::errors::StorageError;
use memmap2::Mmap;
use std::fs::{self, File};
use std::path::PathBuf;
use crate::types::Value;
use crate::utils::aligned_vec::AlignedVec;

pub struct ColumnarReader {
    pub segment_path: PathBuf,
    pub metadata: crate::types::SSTableMetadata,
}

impl ColumnarReader {
    pub fn new(segment_path: PathBuf) -> Result<Self, StorageError> {
        let meta_path = segment_path.join("meta.toml");
        let meta_str = fs::read_to_string(meta_path)?;
        let metadata: crate::types::SSTableMetadata = toml::from_str(&meta_str).map_err(|e| {
            StorageError::ReadError(std::io::Error::new(std::io::ErrorKind::Other, e))
        })?;

        Ok(Self {
            segment_path,
            metadata,
        })
    }

    pub fn read_column_i64(&self, col_name: &str) -> Result<AlignedVec<i64>, StorageError> {
        let file_path = self.segment_path.join(format!("{}.col", col_name));
        let file = File::open(file_path)?;
        let mmap = unsafe { Mmap::map(&file)? };
        let bytes = &mmap[..];
        
        // Verify Column CRC
        if let Some(col_meta) = self.metadata.columns.get(col_name) {
            let actual_crc = crc32fast::hash(&bytes);
            if actual_crc != col_meta.crc32 {
                return Err(StorageError::ReadError(std::io::Error::new(std::io::ErrorKind::InvalidData, format!("Column {} checksum mismatch", col_name))));
            }
        }

        let encoding = self.metadata.columns.get(col_name)
            .map(|c| c.encoding.as_str())
            .unwrap_or("bincode");

        match encoding {
            "rle" => {
                let runs: Vec<RleRun<i64>> = bincode::deserialize(&bytes).map_err(|e| {
                    StorageError::ReadError(std::io::Error::new(std::io::ErrorKind::Other, e))
                })?;
                Ok(RleEncoder::decode::<i64>(&runs))
            }
            "delta" => {
                let encoded: DeltaEncoded<i64> = bincode::deserialize(&bytes).map_err(|e| {
                    StorageError::ReadError(std::io::Error::new(std::io::ErrorKind::Other, e))
                })?;
                Ok(DeltaEncoder::decode_i64(&encoded))
            }
            _ => {
                let aligned: AlignedVec<i64> = bincode::deserialize(&bytes).map_err(|e| {
                    StorageError::ReadError(std::io::Error::new(std::io::ErrorKind::Other, e))
                })?;
                Ok(aligned)
            }
        }
    }

    pub fn read_column_i64_subset(&self, col_name: &str, indices: &[usize]) -> Result<AlignedVec<i64>, StorageError> {
        let full = self.read_column_i64(col_name)?;
        let mut subset = AlignedVec::with_capacity(indices.len());
        for &idx in indices {
            if idx < full.len() {
                subset.push(full[idx]);
            }
        }
        Ok(subset)
    }

    pub fn read_column_f64(&self, col_name: &str) -> Result<AlignedVec<f64>, StorageError> {
        let file_path = self.segment_path.join(format!("{}.col", col_name));
        let file = File::open(file_path)?;
        let mmap = unsafe { Mmap::map(&file)? };
        let bytes = &mmap[..];

        // Verify Column CRC
        if let Some(col_meta) = self.metadata.columns.get(col_name) {
            let actual_crc = crc32fast::hash(&bytes);
            if actual_crc != col_meta.crc32 {
                return Err(StorageError::ReadError(std::io::Error::new(std::io::ErrorKind::InvalidData, format!("Column {} checksum mismatch", col_name))));
            }
        }

        let encoding = self.metadata.columns.get(col_name)
            .map(|c| c.encoding.as_str())
            .unwrap_or("bincode");

        match encoding {
            "rle" => {
                let runs: Vec<RleRun<f64>> = bincode::deserialize(&bytes).map_err(|e| {
                    StorageError::ReadError(std::io::Error::new(std::io::ErrorKind::Other, e))
                })?;
                Ok(RleEncoder::decode::<f64>(&runs))
            }
            _ => {
                let aligned: AlignedVec<f64> = bincode::deserialize(&bytes).map_err(|e| {
                    StorageError::ReadError(std::io::Error::new(std::io::ErrorKind::Other, e))
                })?;
                Ok(aligned)
            }
        }
    }

    pub fn read_column_f64_subset(&self, col_name: &str, indices: &[usize]) -> Result<AlignedVec<f64>, StorageError> {
        let full = self.read_column_f64(col_name)?;
        let mut subset = AlignedVec::with_capacity(indices.len());
        for &idx in indices {
            if idx < full.len() {
                subset.push(full[idx]);
            }
        }
        Ok(subset)
    }

    pub fn read_column_string(&self, col_name: &str) -> Result<Vec<String>, StorageError> {
        let file_path = self.segment_path.join(format!("{}.col", col_name));
        let bytes = fs::read(file_path)?;

        // Verify Column CRC
        if let Some(col_meta) = self.metadata.columns.get(col_name) {
            let actual_crc = crc32fast::hash(&bytes);
            if actual_crc != col_meta.crc32 {
                return Err(StorageError::ReadError(std::io::Error::new(std::io::ErrorKind::InvalidData, format!("Column {} checksum mismatch", col_name))));
            }
        }

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
        Ok(vec![]) // Placeholder
    }
}
