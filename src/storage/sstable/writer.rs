use crate::errors::StorageError;
use crate::storage::bloom::filter::BloomFilterWrapper;
use crate::types::{RowDisk, Value};
use std::fs::File;
use std::io::{BufWriter, Write};

pub const MAGIC_BYTES: &[u8; 4] = b"SQTE";

pub struct SSTableWriter {
    writer: BufWriter<File>,
}

impl SSTableWriter {
    pub fn new(path: &str) -> Result<Self, StorageError> {
        let file = File::create(path)?;
        Ok(Self {
            writer: BufWriter::new(file),
        })
    }

    pub fn write_sstable_to_file(
        path: &str,
        rows: &[RowDisk],
        config: &crate::config::Config,
    ) -> Result<(), StorageError> {
        let mut writer = Self::new(path)?;
        // Use 1% FPR and check config for crc
        writer.write_sstable(rows, 0.01, config.verify_crc)
    }

    pub fn write_sstable(
        &mut self,
        rows: &[RowDisk],
        fpr: f64,
        generate_crc: bool,
    ) -> Result<(), StorageError> {
        // 1. Magic bytes
        self.writer.write_all(MAGIC_BYTES)?;

        let row_count = rows.len() as u64;
        let mut min_ts = i64::MAX;
        let mut max_ts = i64::MIN;
        let mut sums: std::collections::HashMap<String, f64> = std::collections::HashMap::new();

        // We'll need the column names to store in metadata.
        // Assuming rows are not empty, we map by index strings if we don't have schema.
        // But we actually have the schema in config. Let's pass it.

        for row in rows {
            for (idx, val) in row.values.iter().enumerate() {
                let col_name = format!("col_{}", idx); // Fallback
                match val {
                    crate::types::Value::Int(i) => {
                        let sum = sums.entry(col_name.clone()).or_insert(0.0);
                        *sum += *i as f64;

                        if *i > 1_000_000_000_000 && *i < 2_000_000_000_000 {
                            min_ts = min_ts.min(*i);
                            max_ts = max_ts.max(*i);
                        }
                    }
                    crate::types::Value::Float(f) => {
                        let sum = sums.entry(col_name).or_insert(0.0);
                        *sum += *f;
                    }
                    _ => {}
                }
            }
        }

        let mut column_metadata = std::collections::BTreeMap::new();
        for (col_name, sum) in sums {
            column_metadata.insert(
                col_name,
                crate::types::ColumnMetadata {
                    encoding: "bincode".to_string(),
                    sum,
                    ..Default::default()
                },
            );
        }

        let mut metadata = crate::types::SSTableMetadata {
            magic: "AQEM".to_string(),
            row_count,
            min_ts: if min_ts == i64::MAX { 0 } else { min_ts },
            max_ts: if max_ts == i64::MIN { 0 } else { max_ts },
            schema_version: rows.first().map(|r| r.version).unwrap_or(1),
            columns: column_metadata,
            checksum: 0,
        };

        // Compute metadata checksum
        let metadata_pre = bincode::serialize(&metadata).map_err(|e| {
            StorageError::WriteError(std::io::Error::new(std::io::ErrorKind::Other, e))
        })?;
        metadata.checksum = crc32fast::hash(&metadata_pre);

        let metadata_bytes = bincode::serialize(&metadata).map_err(|e| {
            StorageError::WriteError(std::io::Error::new(std::io::ErrorKind::Other, e))
        })?;

        self.writer
            .write_all(&(metadata_bytes.len() as u32).to_le_bytes())?;
        self.writer.write_all(&metadata_bytes)?;

        // 3. Bloom filter
        let mut bloom = BloomFilterWrapper::new(rows.len(), fpr);
        for row in rows {
            if let Some(Value::Int(uid)) = row.values.get(0) {
                bloom.insert(*uid as u64);
            }
        }

        let bloom_bytes = bincode::serialize(&bloom).map_err(|e| {
            StorageError::WriteError(std::io::Error::new(std::io::ErrorKind::Other, e))
        })?;

        self.writer.write_all(&bloom_bytes)?;

        // 4. Rows
        for row in rows {
            let mut final_row = row.clone();

            if generate_crc {
                let mut row_to_crc = row.clone();
                row_to_crc.crc = 0;
                let row_bytes_for_crc = bincode::serialize(&row_to_crc).map_err(|e| {
                    StorageError::WriteError(std::io::Error::new(std::io::ErrorKind::Other, e))
                })?;

                let mut hasher = crc32fast::Hasher::new();
                hasher.update(&row_bytes_for_crc);
                final_row.crc = hasher.finalize();
            } else {
                final_row.crc = 0;
            }

            let row_bytes = bincode::serialize(&final_row).map_err(|e| {
                StorageError::WriteError(std::io::Error::new(std::io::ErrorKind::Other, e))
            })?;
            self.writer.write_all(&row_bytes)?;
        }

        self.writer.flush()?;
        Ok(())
    }
}
