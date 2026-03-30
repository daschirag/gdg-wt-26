use crate::errors::StorageError;
use crate::storage::columnar::encoding::delta::{DeltaEncoded, DeltaEncoder};
use crate::storage::columnar::encoding::rle::{RleEncoder, RleRun, RleRunI64};
use crate::types::{ColumnMetadata, SSTableMetadata};
use crate::utils::aligned_vec::AlignedVec;
use memmap2::Mmap;
use serde::{Deserialize, Serialize};
use std::fs::{self, File};
use std::path::PathBuf;

const RLE_I64_MAGIC: [u8; 4] = *b"RLI4";
const RLE_I64_VERSION: u32 = 1;
const RLE_I64_HEADER_LEN: usize = 16;

fn parse_rle_i64_slice(bytes: &[u8]) -> Result<&[RleRunI64], StorageError> {
    if bytes.len() < RLE_I64_HEADER_LEN {
        return Err(StorageError::ReadError(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "RLE i64 file too small for header",
        )));
    }
    if bytes[..4] != RLE_I64_MAGIC {
        return Err(StorageError::ReadError(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "RLE i64 file missing RLI4 header",
        )));
    }

    let version = u32::from_le_bytes(bytes[4..8].try_into().unwrap());
    if version != RLE_I64_VERSION {
        return Err(StorageError::ReadError(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("Unsupported RLE i64 version: {}", version),
        )));
    }

    let run_count = u64::from_le_bytes(bytes[8..16].try_into().unwrap()) as usize;
    let body = &bytes[RLE_I64_HEADER_LEN..];
    let expected_len = run_count
        .checked_mul(std::mem::size_of::<RleRunI64>())
        .ok_or_else(|| {
            StorageError::ReadError(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "RLE i64 run count overflow",
            ))
        })?;
    if body.len() != expected_len {
        return Err(StorageError::ReadError(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!(
                "RLE i64 payload size mismatch: expected {} bytes, got {}",
                expected_len,
                body.len()
            ),
        )));
    }

    bytemuck::try_cast_slice(body).map_err(|e| {
        StorageError::ReadError(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            e.to_string(),
        ))
    })
}

fn decode_rle_i64_owned(bytes: &[u8]) -> Result<Vec<RleRunI64>, StorageError> {
    Ok(parse_rle_i64_slice(bytes)?.to_vec())
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct ColumnarReader {
    pub path: PathBuf,
    pub metadata: SSTableMetadata,
    pub seg_idx: u64,
}

impl ColumnarReader {
    pub fn new(path: PathBuf, seg_idx: u64) -> Result<Self, StorageError> {
        let json_path = path.join("metadata.json");
        let toml_path = path.join("meta.toml");

        let metadata = if json_path.exists() {
            let s = fs::read_to_string(json_path).map_err(|e| StorageError::ReadError(e))?;
            serde_json::from_str(&s).map_err(|e| {
                StorageError::ReadError(std::io::Error::new(std::io::ErrorKind::Other, e))
            })?
        } else if toml_path.exists() {
            let s = fs::read_to_string(toml_path).map_err(|e| StorageError::ReadError(e))?;
            toml::from_str(&s).map_err(|e| {
                StorageError::ReadError(std::io::Error::new(std::io::ErrorKind::Other, e))
            })?
        } else {
            return Err(StorageError::ReadError(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "No metadata file found",
            )));
        };

        Ok(Self {
            path,
            metadata,
            seg_idx,
        })
    }

    pub fn read_column_i64(&self, name: &str) -> Result<AlignedVec<i64>, StorageError> {
        let col_meta = self.metadata.columns.get(name).ok_or_else(|| {
            StorageError::ReadError(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "Column not found",
            ))
        })?;

        let file_path = self.path.join(format!("{}.col", name));
        let bytes = fs::read(file_path)?;

        match col_meta.encoding.as_str() {
            "rle" => {
                let runs = decode_rle_i64_owned(&bytes)?;
                Ok(AlignedVec::from_slice(&RleEncoder::decode_i64(&runs)))
            }
            "delta" => {
                let encoded: DeltaEncoded<i64> = bincode::deserialize(&bytes).map_err(|e| {
                    StorageError::ReadError(std::io::Error::new(std::io::ErrorKind::Other, e))
                })?;
                Ok(AlignedVec::from_slice(&DeltaEncoder::decode_i64(&encoded)))
            }
            _ => Err(StorageError::ReadError(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Invalid encoding",
            ))),
        }
    }

    pub fn read_column_f64(&self, name: &str) -> Result<AlignedVec<f64>, StorageError> {
        let col_meta = self.metadata.columns.get(name).ok_or_else(|| {
            StorageError::ReadError(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "Column not found",
            ))
        })?;

        let file_path = self.path.join(format!("{}.col", name));
        let bytes = fs::read(file_path)?;

        match col_meta.encoding.as_str() {
            "rle" => {
                let runs: Vec<RleRun<f64>> = bincode::deserialize(&bytes).map_err(|e| {
                    StorageError::ReadError(std::io::Error::new(std::io::ErrorKind::Other, e))
                })?;
                Ok(AlignedVec::from_slice(&RleEncoder::decode::<f64>(&runs)))
            }
            _ => {
                let data: Vec<f64> = bincode::deserialize(&bytes).map_err(|e| {
                    StorageError::ReadError(std::io::Error::new(std::io::ErrorKind::Other, e))
                })?;
                Ok(AlignedVec::from_slice(&data))
            }
        }
    }

    pub fn read_column_bytes(&self, name: &str) -> Result<Vec<u8>, StorageError> {
        let file_path = self.path.join(format!("{}.col", name));
        fs::read(file_path).map_err(|e| StorageError::ReadError(e))
    }

    pub fn read_column_runs_i64(&self, name: &str) -> Result<Vec<RleRunI64>, StorageError> {
        let bytes = self.read_column_bytes(name)?;
        decode_rle_i64_owned(&bytes)
    }

    pub fn open_column(&self, col_name: &str) -> Result<ColumnFileHandle, StorageError> {
        let col_meta = self
            .metadata
            .columns
            .get(col_name)
            .cloned()
            .ok_or_else(|| {
                StorageError::ReadError(std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    "Column not found",
                ))
            })?;
        let file_path = self.path.join(format!("{}.col", col_name));
        let file = File::open(&file_path)?;
        let mmap = unsafe { Mmap::map(&file).map_err(StorageError::ReadError)? };
        Ok(ColumnFileHandle {
            bytes: ColumnBytes::Mapped(mmap),
            metadata: col_meta,
            cached_runs_i64: None,
            cached_runs_u8: None,
            seg_idx: self.seg_idx,
        })
    }

    pub fn read_all_rows(
        &self,
        config: &crate::config::Config,
    ) -> Result<Vec<crate::types::RowDisk>, StorageError> {
        let row_count = self.metadata.row_count as usize;
        let mut columns = Vec::new();

        for col_schema in &config.schema.columns {
            let _col_meta = self.metadata.columns.get(&col_schema.name).ok_or_else(|| {
                StorageError::ReadError(std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    format!("Column {} not found in metadata", col_schema.name),
                ))
            })?;

            if col_schema.r#type.starts_with('f') {
                let data = self.read_column_f64(&col_schema.name)?;
                columns.push(
                    data.iter()
                        .map(|&v| crate::types::Value::Float(v))
                        .collect::<Vec<_>>(),
                );
            } else {
                let data = self.read_column_i64(&col_schema.name)?;
                columns.push(
                    data.iter()
                        .map(|&v| crate::types::Value::Int(v))
                        .collect::<Vec<_>>(),
                );
            }
        }

        let mut rows = Vec::with_capacity(row_count);
        for r in 0..row_count {
            let mut values = Vec::with_capacity(columns.len());
            for c in 0..columns.len() {
                values.push(columns[c][r].clone());
            }
            rows.push(crate::types::RowDisk {
                version: self.metadata.schema_version,
                crc: 0,
                seq: 0,
                values,
            });
        }

        Ok(rows)
    }
}

pub struct ColumnFileHandle {
    pub bytes: ColumnBytes,
    pub metadata: ColumnMetadata,
    pub cached_runs_i64: Option<Vec<RleRunI64>>,
    pub cached_runs_u8: Option<Vec<RleRun<u8>>>,
    pub seg_idx: u64,
}

pub enum ColumnBytes {
    Owned(Vec<u8>),
    Mapped(Mmap),
}

impl ColumnBytes {
    fn as_slice(&self) -> &[u8] {
        match self {
            Self::Owned(bytes) => bytes.as_slice(),
            Self::Mapped(mmap) => mmap.as_ref(),
        }
    }
}

impl ColumnFileHandle {
    pub fn get_runs_i64(&mut self) -> &[RleRunI64] {
        if let Ok(runs) = parse_rle_i64_slice(self.bytes.as_slice()) {
            return runs;
        }
        if self.cached_runs_i64.is_none() {
            if let Ok(runs) = decode_rle_i64_owned(self.bytes.as_slice()) {
                self.cached_runs_i64 = Some(runs);
            }
        }
        self.cached_runs_i64
            .as_ref()
            .map(|v| v.as_slice())
            .unwrap_or(&[])
    }

    pub fn get_runs_u8(&mut self) -> &[RleRun<u8>] {
        if self.cached_runs_u8.is_none() {
            if let Ok(runs) = bincode::deserialize::<Vec<RleRun<u8>>>(self.bytes.as_slice()) {
                self.cached_runs_u8 = Some(runs);
            }
        }
        self.cached_runs_u8
            .as_ref()
            .map(|v| v.as_slice())
            .unwrap_or(&[])
    }

    pub fn read_all_i64(&mut self) -> Result<AlignedVec<i64>, StorageError> {
        let runs = self.get_runs_i64();
        if runs.is_empty() {
            return Ok(AlignedVec::new());
        }
        Ok(AlignedVec::from_slice(&RleEncoder::decode_i64(runs)))
    }

    pub fn verify_crc(&self) -> Result<(), StorageError> {
        let actual_crc = crc32fast::hash(self.bytes.as_slice());
        if actual_crc != self.metadata.crc32 {
            return Err(StorageError::ReadError(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "CRC mismatch",
            )));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::{
        ColumnBytes, RLE_I64_HEADER_LEN, RLE_I64_MAGIC, RLE_I64_VERSION, decode_rle_i64_owned,
        parse_rle_i64_slice,
    };
    use crate::storage::columnar::encoding::rle::{RleRun, RleRunI64};

    fn encode_new_format(runs: &[RleRunI64]) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(RLE_I64_HEADER_LEN + std::mem::size_of_val(runs));
        bytes.extend_from_slice(&RLE_I64_MAGIC);
        bytes.extend_from_slice(&RLE_I64_VERSION.to_le_bytes());
        bytes.extend_from_slice(&(runs.len() as u64).to_le_bytes());
        bytes.extend_from_slice(bytemuck::cast_slice(runs));
        bytes
    }

    #[test]
    fn column_bytes_owned_exposes_slice() {
        let bytes = ColumnBytes::Owned(vec![1, 2, 3]);
        assert_eq!(bytes.as_slice(), &[1, 2, 3]);
    }

    #[test]
    fn parses_new_rle_i64_format_zero_copy() {
        let runs = [RleRunI64::new(7, 3), RleRunI64::new(9, 2)];
        let bytes = encode_new_format(&runs);

        let parsed = parse_rle_i64_slice(&bytes).unwrap();

        assert_eq!(parsed, runs);
    }

    #[test]
    fn rejects_legacy_bincode_rle_i64_format() {
        let legacy = vec![
            RleRun {
                value: 5_i64,
                length: 4,
            },
            RleRun {
                value: 8_i64,
                length: 1,
            },
        ];
        let bytes = bincode::serialize(&legacy).unwrap();

        let err = decode_rle_i64_owned(&bytes).unwrap_err();

        assert!(err.to_string().contains("RLI4"));
    }
}
