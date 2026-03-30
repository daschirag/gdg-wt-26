use std::thread;
use std::time::Duration;
use std::sync::Arc;
use crate::storage::compaction::engine::CompactionEngine;

use std::sync::atomic::{AtomicBool, Ordering};

use crate::storage::manager::StorageManager;

pub struct CompactionScheduler;

static IS_COMPACTING: AtomicBool = AtomicBool::new(false);

impl CompactionScheduler {
    pub fn start_background_thread(storage: Arc<StorageManager>) {
        let config = storage.config.clone();
        thread::spawn(move || {
            loop {
                thread::sleep(Duration::from_secs(config.compaction_interval_secs));
                
                if !IS_COMPACTING.swap(true, Ordering::SeqCst) {
                    let _ = CompactionEngine::run_compaction(&storage);
                    IS_COMPACTING.store(false, Ordering::SeqCst);
                }
            }
        });
    }

    pub fn compact_now(storage: &StorageManager) -> Result<usize, crate::errors::StorageError> {
        if IS_COMPACTING.swap(true, Ordering::SeqCst) {
            return Ok(0); // Already compacting
        }
        
        let res = CompactionEngine::run_compaction(storage);
        IS_COMPACTING.store(false, Ordering::SeqCst);
        res
    }
}
