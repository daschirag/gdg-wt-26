use crate::errors::StorageError;
use serde::{Deserialize, Serialize};
use std::collections::hash_map::DefaultHasher;
use std::fs;
use std::hash::{Hash, Hasher};
use std::path::Path;

const HLL_PRECISION: u8 = 10;
const HLL_REGISTERS: usize = 1 << HLL_PRECISION;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct HllSketch {
    precision: u8,
    registers: Vec<u8>,
}

impl Default for HllSketch {
    fn default() -> Self {
        Self {
            precision: HLL_PRECISION,
            registers: vec![0; HLL_REGISTERS],
        }
    }
}

impl HllSketch {
    pub fn add_i64(&mut self, value: i64) {
        let mut hasher = DefaultHasher::new();
        value.hash(&mut hasher);
        let hash = hasher.finish();
        let idx = (hash as usize) & ((1usize << self.precision) - 1);
        let w = hash >> self.precision;
        let rank = (w.leading_zeros() + 1) as u8;
        self.registers[idx] = self.registers[idx].max(rank);
    }

    pub fn estimate(&self) -> f64 {
        let m = self.registers.len() as f64;
        let alpha = match self.registers.len() {
            16 => 0.673,
            32 => 0.697,
            64 => 0.709,
            _ => 0.7213 / (1.0 + 1.079 / m),
        };
        let z = self
            .registers
            .iter()
            .map(|reg| 2f64.powi(-(*reg as i32)))
            .sum::<f64>();
        let raw = alpha * m * m / z.max(f64::MIN_POSITIVE);
        let zeros = self.registers.iter().filter(|reg| **reg == 0).count() as f64;
        if raw <= 2.5 * m && zeros > 0.0 {
            m * (m / zeros).ln()
        } else {
            raw
        }
    }
}

pub fn write_hll(path: &Path, sketch: &HllSketch) -> Result<(), StorageError> {
    let bytes = bincode::serialize(sketch).map_err(|e| {
        StorageError::WriteError(std::io::Error::new(std::io::ErrorKind::Other, e))
    })?;
    fs::write(path, bytes)?;
    Ok(())
}

pub fn read_hll(path: &Path) -> Result<HllSketch, StorageError> {
    let bytes = fs::read(path)?;
    bincode::deserialize(&bytes).map_err(|e| {
        StorageError::ReadError(std::io::Error::new(std::io::ErrorKind::Other, e))
    })
}
