use std::path::Path;
use crate::storage::sstable::reader::SSTableReader;
use crate::storage::columnar::writer::ColumnarWriter;
use crate::errors::StorageError;

pub struct SegmentMerger;

impl SegmentMerger {
    pub fn merge_sstables_to_segment(sst_paths: &[String], output_segment_path: &Path, config: &crate::config::Config) -> Result<(), StorageError> {
        use std::collections::BinaryHeap;

        struct MergeEntry {
            row: crate::types::RowDisk,
            reader_idx: usize,
            remaining_rows: std::vec::IntoIter<crate::types::RowDisk>,
        }

        impl PartialEq for MergeEntry {
            fn eq(&self, other: &Self) -> bool {
                self.row.values.first() == other.row.values.first() && self.row.seq == other.row.seq
            }
        }
        impl Eq for MergeEntry {}
        impl PartialOrd for MergeEntry {
            fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
                Some(self.cmp(other))
            }
        }
        impl Ord for MergeEntry {
            fn cmp(&self, other: &Self) -> std::cmp::Ordering {
                let a_uid = self.row.values.first().and_then(|v| match v { crate::types::Value::Int(i) => Some(i), _ => None });
                let b_uid = other.row.values.first().and_then(|v| match v { crate::types::Value::Int(i) => Some(i), _ => None });
                
                // BinaryHeap is a Max-Heap, so we want the "smallest" (earliest uid) to be at the top.
                // Reverse the primary ordering, but keep seq descending (higher seq first).
                match b_uid.cmp(&a_uid) {
                    std::cmp::Ordering::Equal => self.row.seq.cmp(&other.row.seq), // Higher seq first
                    other => other,
                }
            }
        }

        let mut heap = BinaryHeap::new();
        for (idx, path) in sst_paths.iter().enumerate() {
            let reader = SSTableReader::new(path);
            let (rows, _) = reader.read_rows_profiled(false)?;
            let mut iter = rows.into_iter();
            if let Some(row) = iter.next() {
                heap.push(MergeEntry { row, reader_idx: idx, remaining_rows: iter });
            }
        }

        let mut last_uid = None;
        let deduped_iter = std::iter::from_fn(move || {
            while let Some(mut top) = heap.pop() {
                let current_row = top.row.clone();
                let current_uid = current_row.values.first().and_then(|v| match v { crate::types::Value::Int(i) => Some(*i), _ => None });
                
                // Push next row from the same reader
                if let Some(next_row) = top.remaining_rows.next() {
                    top.row = next_row;
                    heap.push(top);
                }

                if current_uid != last_uid || current_uid.is_none() {
                    last_uid = current_uid;
                    return Some(current_row);
                }
            }
            None
        });

        // 4. Write as Segment using the streaming iterator
        ColumnarWriter::write_segment_from_iter(output_segment_path, deduped_iter, config, 0.01)?;
 // 1% FPR for compacted bloom

        // 4. Cleanup source SSTables (atomic rename then delete)
        // For now, we just delete them.
        for path in sst_paths {
            let _ = std::fs::remove_file(path);
        }

        Ok(())
    }
}
