use crate::errors::StorageError;
use crate::storage::columnar::writer::ColumnarWriter;
use crate::storage::sstable::reader::SSTableReader;
use std::path::Path;

pub struct SegmentMerger;

impl SegmentMerger {
    pub fn merge_sstables_to_segment(
        sst_paths: &[String],
        output_base_path: &Path,
        config: &crate::config::Config,
    ) -> Result<Vec<String>, StorageError> {
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
                let a_uid = self.row.values.first().and_then(|v| match v {
                    crate::types::Value::Int(i) => Some(i),
                    _ => None,
                });
                let b_uid = other.row.values.first().and_then(|v| match v {
                    crate::types::Value::Int(i) => Some(i),
                    _ => None,
                });

                match b_uid.cmp(&a_uid) {
                    std::cmp::Ordering::Equal => self.row.seq.cmp(&other.row.seq),
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
                heap.push(MergeEntry {
                    row,
                    reader_idx: idx,
                    remaining_rows: iter,
                });
            }
        }

        let mut last_uid = None;
        let mut deduped_iter = std::iter::from_fn(move || {
            while let Some(mut top) = heap.pop() {
                let current_row = top.row.clone();
                let current_uid = current_row.values.first().and_then(|v| match v {
                    crate::types::Value::Int(i) => Some(*i),
                    _ => None,
                });

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

        // Split writing into multiple segments if row count exceeds max_segment_rows
        let mut created_segments = Vec::new();
        let mut segment_idx = 0;
        let mut done = false;

        while !done {
            let mut chunk = Vec::with_capacity(config.max_segment_rows as usize);
            for _ in 0..config.max_segment_rows {
                if let Some(row) = deduped_iter.next() {
                    chunk.push(row);
                } else {
                    done = true;
                    break;
                }
            }

            if !chunk.is_empty() {
                let seg_path = if segment_idx == 0 {
                    output_base_path.to_path_buf()
                } else {
                    let parent = output_base_path.parent().unwrap();
                    let file_name = output_base_path.file_name().unwrap().to_str().unwrap();
                    parent.join(format!("{}_{:03}", file_name, segment_idx))
                };

                ColumnarWriter::write_segment(&seg_path, &chunk, config, 0.01)?;
                created_segments.push(seg_path.to_str().unwrap().to_string());
                segment_idx += 1;
            }
        }

        // Cleanup source SSTables
        for path in sst_paths {
            let _ = std::fs::remove_file(path);
        }

        Ok(created_segments)
    }
}
