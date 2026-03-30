use std::path::{Path, PathBuf};
use crate::storage::compaction::stcs::StcsCompactor;
use crate::storage::compaction::lcs::LcsCompactor;
use crate::storage::compaction::merger::SegmentMerger;
use crate::config::Config;
use crate::errors::StorageError;

pub struct CompactionEngine;

impl CompactionEngine {
    pub fn run_compaction(config: &Config) -> Result<usize, StorageError> {
        let sst_dir = Path::new("data");
        let segments_dir = sst_dir.join("segments");
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
            
            SegmentMerger::merge_sstables_to_segment(&bucket, &seg_path, config)?;
        }

        Ok(num_tasks)
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
