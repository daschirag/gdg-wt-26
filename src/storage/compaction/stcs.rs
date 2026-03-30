use crate::errors::StorageError;
use std::path::Path;

pub struct StcsCompactor;

impl StcsCompactor {
    pub fn plan_compaction(
        sst_dir: &Path,
        min_files: usize,
    ) -> Result<Vec<Vec<String>>, StorageError> {
        let entries = std::fs::read_dir(sst_dir)?;
        let mut files = Vec::new();

        println!("DEBUG: StcsCompactor scanning {:?}", sst_dir);
        for entry in entries {
            let entry = entry?;
            let path = entry.path();
            if path.extension().map_or(false, |ext| ext == "aqe") {
                let size = entry.metadata()?.len();
                let path_str = path.to_str().unwrap().to_string();
                println!("DEBUG: Found .aqe candidate: {} ({} bytes)", path_str, size);
                files.push((path_str, size));
            }
        }

        if files.is_empty() {
            return Ok(Vec::new());
        }

        // 1. Sort by size
        files.sort_by_key(|f| f.1);

        // 2. Simple bucketing: if size[i] and size[i+1] are within 2x, they are in same bucket
        let mut buckets: Vec<Vec<String>> = Vec::new();
        let mut current_bucket = vec![files[0].0.clone()];
        let mut last_size = files[0].1;

        for (path, size) in files.iter().skip(1) {
            if *size < last_size * 2 {
                current_bucket.push(path.clone());
            } else {
                buckets.push(current_bucket);
                current_bucket = vec![path.clone()];
                last_size = *size;
            }
        }
        buckets.push(current_bucket);

        // 3. Keep only buckets that meet the minimum file count
        let candidates: Vec<Vec<String>> = buckets
            .into_iter()
            .filter(|b| b.len() >= min_files)
            .collect();

        Ok(candidates)
    }
}
