use std::path::Path;
use crate::errors::StorageError;

pub struct LcsCompactor;

impl LcsCompactor {
    pub fn plan_compaction(sst_dir: &Path, l1_max_bytes: u64) -> Result<Vec<Vec<String>>, StorageError> {
        // Simplified LCS for our prototype:
        // Group all L0 files (those not in segments) if their total size > threshold
        let entries = std::fs::read_dir(sst_dir)?;
        let mut l0_files = Vec::new();
        let mut total_size = 0u64;

        for entry in entries {
            let entry = entry?;
            let path = entry.path();
            if path.extension().map_or(false, |ext| ext == "aqe") {
                let size = entry.metadata()?.len();
                l0_files.push(path.to_str().unwrap().to_string());
                total_size += size;
            }
        }

        if l0_files.len() >= 4 || total_size > l1_max_bytes {
            Ok(vec![l0_files])
        } else {
            Ok(Vec::new())
        }
    }
}
