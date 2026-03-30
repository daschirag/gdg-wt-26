use std::thread;
use std::time::Duration;
use std::sync::Arc;
use crate::config::Config;
use crate::storage::compaction::engine::CompactionEngine;

use std::sync::atomic::{AtomicBool, Ordering};

pub struct CompactionScheduler;

static IS_COMPACTING: AtomicBool = AtomicBool::new(false);

impl CompactionScheduler {
    pub fn start_background_thread(config: Arc<Config>) {
        thread::spawn(move || {
            loop {
                thread::sleep(Duration::from_secs(config.compaction_interval_secs));
                
                if !IS_COMPACTING.swap(true, Ordering::SeqCst) {
                    let _ = CompactionEngine::run_compaction(&config);
                    IS_COMPACTING.store(false, Ordering::SeqCst);
                }
            }
        });
    }

    pub fn compact_now(config: &Config) -> Result<usize, crate::errors::StorageError> {
        if IS_COMPACTING.swap(true, Ordering::SeqCst) {
            return Ok(0); // Already compacting
        }
        
        let res = CompactionEngine::run_compaction(config);
        IS_COMPACTING.store(false, Ordering::SeqCst);
        res
    }
}
