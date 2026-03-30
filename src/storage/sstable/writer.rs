use std::fs::File;
use std::io::{BufWriter, Write};
use crate::types::RowDisk;
use crate::storage::bloom::filter::BloomFilterWrapper;
use crate::errors::StorageError;

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

    pub fn write_sstable(&mut self, rows: &[RowDisk], fpr: f64) -> Result<(), StorageError> {
        // 1. Magic bytes
        self.writer.write_all(MAGIC_BYTES)?;

        // 2. Row count
        let row_count = rows.len() as u64;
        self.writer.write_all(&row_count.to_le_bytes())?;

        // 3. Bloom filter
        let mut bloom = BloomFilterWrapper::new(rows.len(), fpr);
        for row in rows {
            bloom.insert(row.user_id);
        }

        // Serialize bloom wrapper using bincode
        let bloom_bytes = bincode::serialize(&bloom).map_err(|e| {
            StorageError::WriteError(std::io::Error::new(std::io::ErrorKind::Other, e))
        })?;
        
        // We write FPR, BitVecLen, then Bytes as per spec (though bincode handles this for us in the struct)
        // SSTable binary format spec:
        // [BLOOM_FILTER]
        //   fpr: f64
        //   len: u32
        //   bytes: [u8; len]
        // Since BloomFilterWrapper has these fields, bincode::serialize(&bloom) is sufficient.
        
        self.writer.write_all(&bloom_bytes)?;

        // 4. Rows
        for row in rows {
            let row_bytes = bincode::serialize(row).map_err(|e| {
                StorageError::WriteError(std::io::Error::new(std::io::ErrorKind::Other, e))
            })?;
            self.writer.write_all(&row_bytes)?;
        }

        self.writer.flush()?;
        Ok(())
    }
}
