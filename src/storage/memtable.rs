use std::collections::BTreeMap;
use crate::types::RowDisk;

pub struct Memtable {
    pub data: BTreeMap<u64, RowDisk>,
}

impl Memtable {
    pub fn new() -> Self {
        Self {
            data: BTreeMap::new(),
        }
    }

    pub fn insert(&mut self, row: RowDisk) {
        self.data.insert(row.user_id, row);
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
