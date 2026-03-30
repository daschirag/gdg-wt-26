use crate::types::{RowDisk, Value};
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};

static GLOBAL_SEQ: AtomicU64 = AtomicU64::new(0);

pub struct Memtable {
    pub data: BTreeMap<u64, RowDisk>,
}

impl Memtable {
    pub fn new() -> Self {
        Self {
            data: BTreeMap::new(),
        }
    }

    pub fn insert(&mut self, mut row: RowDisk) {
        if let Some(Value::Int(uid)) = row.values.get(0) {
            row.seq = GLOBAL_SEQ.fetch_add(1, Ordering::SeqCst);
            self.data.insert(*uid as u64, row);
        }
    }

    pub fn get(&self, user_id: u64) -> Option<&RowDisk> {
        self.data.get(&user_id)
    }

    pub fn clear(&mut self) {
        self.data.clear();
    }

    pub fn row_count(&self) -> usize {
        self.data.len()
    }
}
