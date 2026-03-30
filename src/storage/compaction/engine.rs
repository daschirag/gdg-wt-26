use crate::config::Config;
use crate::errors::StorageError;
use crate::storage::compaction::lcs::LcsCompactor;
use crate::storage::compaction::merger::SegmentMerger;
use crate::storage::compaction::stcs::StcsCompactor;
use std::path::Path;

pub struct CompactionEngine;

impl CompactionEngine {
    pub fn run_compaction(
        storage: &crate::storage::manager::StorageManager,
    ) -> Result<usize, StorageError> {
        let config = &storage.config;
        let sst_dir = Path::new(&storage.sst_dir);
        let segments_dir = sst_dir.join("segments");

        // Startup Validation (FIX 2)
        Self::verify_storage_metadata(&segments_dir, config)?;

        if !segments_dir.exists() {
            std::fs::create_dir_all(&segments_dir)?;
        }

        // 1. Get Compaction Plan
        let candidates = match config.compaction_strategy.as_str() {
            "lcs" => LcsCompactor::plan_compaction(sst_dir, config.lcs_l1_max_bytes)?,
            _ => StcsCompactor::plan_compaction(sst_dir, config.min_compaction_files as usize)?,
        };

        let num_tasks = candidates.len();
        if num_tasks == 0 {
            return Ok(0);
        }

        // 2. Execute Merges
        for bucket in candidates {
            let seg_id = Self::next_segment_id(&segments_dir)?;
            let seg_name = format!("seg_{:03}", seg_id);
            let seg_path = segments_dir.join(seg_name);

            let created_segments =
                SegmentMerger::merge_sstables_to_segment(&bucket, &seg_path, config)?;

            // FIX 17: Update thread-safe registry for all created segments
            for path in created_segments {
                storage.register_segment(path);
            }
            storage.unregister_sstables(&bucket);

            // Clean up old SSTables
            for sst in &bucket {
                let _ = std::fs::remove_file(sst);
            }
        }

        Ok(num_tasks)
    }

    pub fn verify_storage_metadata(
        segments_dir: &Path,
        config: &Config,
    ) -> Result<(), StorageError> {
        if !segments_dir.exists() {
            return Ok(());
        }

        let entries = std::fs::read_dir(segments_dir)?;
        for entry in entries {
            let entry = entry?;
            if entry.file_type()?.is_dir() {
                let meta_path = entry.path().join("meta.toml");
                if meta_path.exists() {
                    let content = std::fs::read_to_string(meta_path)?;
                    let metadata: crate::types::SSTableMetadata = toml::from_str(&content)
                        .map_err(|e| {
                            StorageError::ReadError(std::io::Error::new(
                                std::io::ErrorKind::Other,
                                e,
                            ))
                        })?;

                    for col in &config.schema.columns {
                        if !metadata.columns.contains_key(&col.name) {
                            return Err(StorageError::MetadataSchemaMismatch);
                        }
                    }
                }
            }
        }
        Ok(())
    }

    fn next_segment_id(segments_dir: &Path) -> Result<u32, StorageError> {
        let mut max_id = 0;
        let entries = std::fs::read_dir(segments_dir)?;
        for entry in entries {
            let entry = entry?;
            let name = entry.file_name().into_string().unwrap_or_default();
            if name.starts_with("seg_") {
                if let Ok(id) = name[4..].parse::<u32>() {
                    max_id = max_id.max(id + 1);
                }
            }
        }
        Ok(max_id)
    }
}
