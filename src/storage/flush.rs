use crate::storage::memtable::Memtable;
use crate::types::RowDisk;
use std::mem::size_of;

pub struct FlushTrigger {
    pub memtable_row_limit: u64,
    pub memtable_size_limit: u64,
}

impl FlushTrigger {
    pub fn should_flush(&self, memtable: &Memtable) -> bool {
        let row_count = memtable.row_count() as u64;
        let estimated_size = row_count * (size_of::<RowDisk>() as u64);

        row_count >= self.memtable_row_limit || estimated_size >= self.memtable_size_limit
    }
}
