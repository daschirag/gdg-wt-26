use crate::errors::StorageError;
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::Path;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq)]
pub struct Centroid {
    pub mean: f64,
    pub weight: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct TDigest {
    pub compression: usize,
    pub total_weight: f64,
    pub centroids: Vec<Centroid>,
}

impl TDigest {
    pub fn new(compression: usize) -> Self {
        Self {
            compression: compression.max(1),
            total_weight: 0.0,
            centroids: Vec::new(),
        }
    }

    pub fn from_values(values: &[f64], compression: usize) -> Self {
        if values.is_empty() {
            return Self::new(compression);
        }

        let mut sorted = values.to_vec();
        sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));

        let mut digest = Self::new(compression);
        let target = compression.max(1);
        let chunk_size = ((sorted.len() as f64) / target as f64).ceil() as usize;
        for chunk in sorted.chunks(chunk_size.max(1)) {
            let sum: f64 = chunk.iter().sum();
            let weight = chunk.len() as f64;
            digest.centroids.push(Centroid {
                mean: sum / weight,
                weight,
            });
            digest.total_weight += weight;
        }
        digest
    }

    pub fn merge(&mut self, other: &TDigest) {
        if other.centroids.is_empty() {
            return;
        }
        self.centroids.extend(other.centroids.iter().copied());
        self.total_weight += other.total_weight;
        self.compress();
    }

    pub fn quantile(&self, q: f64) -> Option<f64> {
        if self.centroids.is_empty() {
            return None;
        }
        let q = q.clamp(0.0, 1.0);
        if self.centroids.len() == 1 {
            return Some(self.centroids[0].mean);
        }
        let target = self.total_weight * q;
        let mut running = 0.0;
        for (idx, centroid) in self.centroids.iter().enumerate() {
            let next_running = running + centroid.weight;
            if target <= next_running || idx + 1 == self.centroids.len() {
                if idx + 1 == self.centroids.len() {
                    return Some(centroid.mean);
                }
                let next = self.centroids[idx + 1];
                let span = (next_running - running).max(1.0);
                let frac = ((target - running) / span).clamp(0.0, 1.0);
                return Some(centroid.mean + (next.mean - centroid.mean) * frac);
            }
            running = next_running;
        }
        self.centroids.last().map(|c| c.mean)
    }

    fn compress(&mut self) {
        if self.centroids.len() <= self.compression.max(1) {
            self.centroids
                .sort_by(|a, b| a.mean.partial_cmp(&b.mean).unwrap_or(std::cmp::Ordering::Equal));
            return;
        }

        self.centroids
            .sort_by(|a, b| a.mean.partial_cmp(&b.mean).unwrap_or(std::cmp::Ordering::Equal));
        let max_weight = (self.total_weight / self.compression.max(1) as f64).ceil().max(1.0);
        let mut merged: Vec<Centroid> = Vec::with_capacity(self.compression.max(1));

        for centroid in self.centroids.drain(..) {
            if let Some(last) = merged.last_mut() {
                if last.weight + centroid.weight <= max_weight {
                    let combined = last.weight + centroid.weight;
                    last.mean =
                        (last.mean * last.weight + centroid.mean * centroid.weight) / combined;
                    last.weight = combined;
                    continue;
                }
            }
            merged.push(centroid);
        }

        self.centroids = merged;
    }
}

pub fn write_tdigest(path: &Path, digest: &TDigest) -> Result<(), StorageError> {
    let bytes = bincode::serialize(digest).map_err(|e| {
        StorageError::WriteError(std::io::Error::new(std::io::ErrorKind::Other, e))
    })?;
    fs::write(path, bytes)?;
    Ok(())
}

pub fn read_tdigest(path: &Path) -> Result<TDigest, StorageError> {
    let bytes = fs::read(path)?;
    bincode::deserialize(&bytes)
        .map_err(|e| StorageError::ReadError(std::io::Error::new(std::io::ErrorKind::Other, e)))
}

#[cfg(test)]
mod tests {
    use super::TDigest;

    #[test]
    fn approximates_quantiles_monotonic_data() {
        let values: Vec<f64> = (0..1000).map(|v| v as f64).collect();
        let digest = TDigest::from_values(&values, 100);

        let median = digest.quantile(0.5).unwrap();
        let p95 = digest.quantile(0.95).unwrap();

        assert!((median - 500.0).abs() < 25.0);
        assert!((p95 - 950.0).abs() < 35.0);
    }
}
