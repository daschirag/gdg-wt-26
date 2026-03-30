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

    pub fn read_rows_profiled(&self, verify_crc: bool) -> Result<(Vec<RowDisk>, crate::types::QueryProfile), StorageError> {
        let mut profile = crate::types::QueryProfile::default();
        let start_io = std::time::Instant::now();
        
        let mut file = File::open(&self.path)?;
        let mut reader = BufReader::new(&mut file);

        // 1. Magic bytes
        let mut magic = [0u8; 4];
        reader.read_exact(&mut magic)?;
        if &magic != MAGIC_BYTES {
            return Err(StorageError::MagicMismatch);
        }

        // 2. Metadata
        let mut meta_len_bytes = [0u8; 4];
        reader.read_exact(&mut meta_len_bytes)?;
        let meta_len = u32::from_le_bytes(meta_len_bytes);
        
        let mut meta_bytes = vec![0u8; meta_len as usize];
        reader.read_exact(&mut meta_bytes)?;
        let metadata: crate::types::SSTableMetadata = bincode::deserialize(&meta_bytes).map_err(|e| {
            StorageError::ReadError(std::io::Error::new(std::io::ErrorKind::Other, e))
        })?;
        
        let expected_row_count = metadata.row_count;
        profile.io_read_ms += start_io.elapsed().as_secs_f64() * 1000.0;

        // 3. Bloom filter
        let start_bloom = std::time::Instant::now();
        let _bloom: BloomFilterWrapper = bincode::deserialize_from(&mut reader).map_err(|e| {
            StorageError::ReadError(std::io::Error::new(std::io::ErrorKind::Other, e))
        })?;
        profile.deserialization_ms += start_bloom.elapsed().as_secs_f64() * 1000.0;

        // 4. Rows
        let mut rows = Vec::with_capacity(expected_row_count as usize);
        for _ in 0..expected_row_count {
            let start_row_deser = std::time::Instant::now();
            let row: RowDisk = bincode::deserialize_from(&mut reader).map_err(|e| {
                StorageError::ReadError(std::io::Error::new(std::io::ErrorKind::Other, e))
            })?;
            profile.deserialization_ms += start_row_deser.elapsed().as_secs_f64() * 1000.0;
            
            if verify_crc {
                let start_crc = std::time::Instant::now();
                // Verify CRC
                let mut row_to_check = row.clone();
                let stored_crc = row.crc;
                row_to_check.crc = 0;
                
                let row_bytes = bincode::serialize(&row_to_check).map_err(|e| {
                    StorageError::ReadError(std::io::Error::new(std::io::ErrorKind::Other, e))
                })?;
                
                let mut hasher = crc32fast::Hasher::new();
                hasher.update(&row_bytes);
                let computed_crc = hasher.finalize();
                
                if computed_crc != stored_crc {
                    return Err(StorageError::ReadError(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        format!("CRC mismatch for row! Stored: {}, Computed: {}", stored_crc, computed_crc)
                    )));
                }
                profile.crc_verify_ms += start_crc.elapsed().as_secs_f64() * 1000.0;
            }
            
            rows.push(row);
        }

        Ok((rows, profile))
    }

    pub fn get_metadata(&self) -> Result<crate::types::SSTableMetadata, StorageError> {
        let mut file = File::open(&self.path)?;
        let mut reader = BufReader::new(&mut file);

        let mut magic = [0u8; 4];
        reader.read_exact(&mut magic)?;
        if &magic != MAGIC_BYTES {
            return Err(StorageError::MagicMismatch);
        }

        let mut meta_len_bytes = [0u8; 4];
        reader.read_exact(&mut meta_len_bytes)?;
        let meta_len = u32::from_le_bytes(meta_len_bytes);
        
        let mut meta_bytes = vec![0u8; meta_len as usize];
        reader.read_exact(&mut meta_bytes)?;
        let metadata: crate::types::SSTableMetadata = bincode::deserialize(&meta_bytes).map_err(|e| {
            StorageError::ReadError(std::io::Error::new(std::io::ErrorKind::Other, e))
        })?;
        
        Ok(metadata)
    }

    pub fn read_rows(&self) -> Result<Vec<RowDisk>, StorageError> {
        Ok(self.read_rows_profiled(true)?.0)
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
