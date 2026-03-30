use std::fs::File;
use std::io::{BufReader, Read, Seek, SeekFrom};
use crate::types::RowDisk;
use crate::storage::bloom::filter::BloomFilterWrapper;
use crate::errors::StorageError;
use crate::storage::sstable::writer::MAGIC_BYTES;

pub struct SSTableReader {
    path: String,
}

impl SSTableReader {
    pub fn new(path: &str) -> Self {
        Self {
            path: path.to_string(),
        }
    }

    pub fn read_rows(&self) -> Result<Vec<RowDisk>, StorageError> {
        let mut file = File::open(&self.path)?;
        let mut reader = BufReader::new(&mut file);

        // 1. Magic bytes
        let mut magic = [0u8; 4];
        reader.read_exact(&mut magic)?;
        if &magic != MAGIC_BYTES {
            return Err(StorageError::MagicMismatch);
        }

        // 2. Row count
        let mut row_count_bytes = [0u8; 8];
        reader.read_exact(&mut row_count_bytes)?;
        let expected_row_count = u64::from_le_bytes(row_count_bytes);

        // 3. Bloom filter (skip for now to read rows, or deserialize)
        // Since BloomFilterWrapper is serialized with bincode, we need its size or use bincode::deserialize_from
        let _bloom: BloomFilterWrapper = bincode::deserialize_from(&mut reader).map_err(|e| {
            StorageError::ReadError(std::io::Error::new(std::io::ErrorKind::Other, e))
        })?;

        // 4. Rows
        let mut rows = Vec::with_capacity(expected_row_count as usize);
        for _ in 0..expected_row_count {
            let row: RowDisk = bincode::deserialize_from(&mut reader).map_err(|e| {
                StorageError::ReadError(std::io::Error::new(std::io::ErrorKind::Other, e))
            })?;
            rows.push(row);
        }

        if rows.len() as u64 != expected_row_count {
             // Spec says warn if mismatch, never hard error (though read_exact/deserialize_from would error anyway)
             println!("Warning: Row count mismatch: expected {}, actual {}", expected_row_count, rows.len());
        }

        Ok(rows)
    }

    pub fn get_bloom_filter(&self) -> Result<BloomFilterWrapper, StorageError> {
        let mut file = File::open(&self.path)?;
        let mut reader = BufReader::new(&mut file);

        // Skip magic and row count
        reader.seek(SeekFrom::Start(4 + 8))?;

        let bloom: BloomFilterWrapper = bincode::deserialize_from(&mut reader).map_err(|e| {
            StorageError::ReadError(std::io::Error::new(std::io::ErrorKind::Other, e))
        })?;

        Ok(bloom)
    }
}
