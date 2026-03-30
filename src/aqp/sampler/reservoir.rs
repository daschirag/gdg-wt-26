use crate::types::RowDisk;
use rand::Rng;

/// Reservoir sampler using Vitter's Algorithm R.
/// Maintains a fixed-size uniform random sample of a stream without knowing its length.
pub struct ReservoirSampler {
    reservoir: Vec<RowDisk>,
    capacity: usize,
    count: u64,
}

impl ReservoirSampler {
    pub fn new(capacity: usize) -> Self {
        Self {
            reservoir: Vec::with_capacity(capacity),
            capacity,
            count: 0,
        }
    }

    /// Feed a row into the sampler. Algorithm R:
    /// - While reservoir isn't full: always include the row.
    /// - After full: include with probability capacity/count, replacing a random existing entry.
    pub fn insert(&mut self, row: RowDisk) {
        if self.count < self.capacity as u64 {
            self.reservoir.push(row);
        } else {
            let j = rand::thread_rng().gen_range(0..=self.count);
            if (j as usize) < self.capacity {
                self.reservoir[j as usize] = row;
            }
        }
        self.count += 1;
    }

    /// Returns the current reservoir contents (may be < capacity if fewer rows seen).
    pub fn sample(&self) -> &[RowDisk] {
        &self.reservoir
    }

    /// Total rows seen (not just sampled).
    pub fn total_count(&self) -> u64 {
        self.count
    }
}
