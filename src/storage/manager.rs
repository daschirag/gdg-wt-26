use crate::config::Config;
use crate::errors::StorageError;
use crate::query::ast::QueryPlan;
use crate::storage::memtable::Memtable;
use crate::types::RowDisk;
use std::path::Path;
use std::sync::{Arc, RwLock};

pub struct StorageManager {
    pub memtable: RwLock<Memtable>,
    pub config: Arc<Config>,
    pub sst_dir: String,
    pub cached_segments: Arc<RwLock<Vec<String>>>,
    pub cached_sstables: Arc<RwLock<Vec<String>>>,
}

impl StorageManager {
    pub fn new(config: Arc<Config>, sst_dir: String) -> Self {
        let mut segments = Vec::new();
        let seg_dir = Path::new(&sst_dir).join("segments");
        if let Ok(entries) = std::fs::read_dir(&seg_dir) {
            for entry in entries.flatten() {
                let path = entry.path();
                if path.is_dir() {
                    segments.push(path.to_str().unwrap().to_string());
                }
            }
        }

        let mut sstables = Vec::new();
        if let Ok(entries) = std::fs::read_dir(&sst_dir) {
            for entry in entries.flatten() {
                let path = entry.path();
                if path.extension().map_or(false, |ext| ext == "aqe") {
                    sstables.push(path.to_str().unwrap().to_string());
                }
            }
        }

        Self {
            memtable: RwLock::new(Memtable::new()),
            config,
            sst_dir,
            cached_segments: Arc::new(RwLock::new(segments)),
            cached_sstables: Arc::new(RwLock::new(sstables)),
        }
    }

    pub fn register_segment(&self, path: String) {
        if let Ok(mut segs) = self.cached_segments.write() {
            if !segs.contains(&path) {
                segs.push(path);
            }
        }
    }

    pub fn unregister_segments(&self, paths: &[String]) {
        if let Ok(mut segs) = self.cached_segments.write() {
            segs.retain(|s| !paths.contains(s));
        }
    }

    pub fn register_sstable(&self, path: String) {
        if let Ok(mut ssts) = self.cached_sstables.write() {
            if !ssts.contains(&path) {
                ssts.push(path);
            }
        }
    }

    pub fn unregister_sstables(&self, paths: &[String]) {
        if let Ok(mut ssts) = self.cached_sstables.write() {
            ssts.retain(|s| !paths.contains(s));
        }
    }

    pub fn put(&self, row: RowDisk) -> Result<(), StorageError> {
        let mut mem = self.memtable.write().map_err(|_| {
            StorageError::WriteError(std::io::Error::new(
                std::io::ErrorKind::Other,
                "Lock poisoned",
            ))
        })?;

        mem.insert(row);
        Ok(())
    }

    pub fn get_all_sstables(&self) -> Vec<String> {
        self.cached_sstables
            .read()
            .unwrap_or_else(|_| panic!("SSTable cache lock poisoned"))
            .clone()
    }

    pub fn get_all_segments(&self) -> Vec<String> {
        self.cached_segments
            .read()
            .unwrap_or_else(|_| panic!("Segment cache lock poisoned"))
            .clone()
    }

    pub fn get_memtable_keys(&self) -> std::collections::HashSet<u64> {
        let mem = self.memtable.read().unwrap();
        mem.data.keys().cloned().collect()
    }

    pub fn scan_memtable(&self, plan: &QueryPlan) -> Vec<RowDisk> {
        let mem = self.memtable.read().unwrap();
        let mut results = Vec::new();

        for row in mem.data.values() {
            // Apply filter
            let include = plan.filter.as_ref().map_or(true, |expr| {
                expr.eval_value(&|column| crate::types::get_value(row, column, &self.config))
            });

            if include {
                results.push(row.clone());
            }
        }
        results
    }

    pub fn all_sources_compacted(&self) -> bool {
        self.cached_sstables.read().unwrap().is_empty()
    }
}
