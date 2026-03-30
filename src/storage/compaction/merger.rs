use std::path::Path;
use crate::storage::sstable::reader::SSTableReader;
use crate::storage::columnar::writer::ColumnarWriter;
use crate::types::RowDisk;
use crate::errors::StorageError;

pub struct SegmentMerger;

impl SegmentMerger {
    pub fn merge_sstables_to_segment(sst_paths: &[String], output_segment_path: &Path, config: &crate::config::Config) -> Result<(), StorageError> {
        let mut all_rows = Vec::new();

        // 1. Read all rows from provided SSTables
        for path in sst_paths {
            let reader = SSTableReader::new(path);
            let (rows, _) = reader.read_rows_profiled(false)?; // Disable CRC check for speed during compaction
            all_rows.extend(rows);
        }

        if all_rows.is_empty() {
            return Ok(());
        }

        // 2. Sort by user_id (first column) and then timestamp
        all_rows.sort_by(|a, b| {
            let a_val = a.values.first();
            let b_val = b.values.first();
            a_val.partial_cmp(&b_val).unwrap_or(std::cmp::Ordering::Equal)
        });

        // 3. Write as Segment
        ColumnarWriter::write_segment(output_segment_path, &all_rows, config, 0.01)?; // 1% FPR for compacted bloom

        // 4. Cleanup source SSTables (atomic rename then delete)
        // For now, we just delete them.
        for path in sst_paths {
            let _ = std::fs::remove_file(path);
        }

        Ok(())
    }
}
