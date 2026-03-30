use rand::distributions::{Distribution, WeightedIndex};
use rand::prelude::*;
use rand_chacha::ChaCha8Rng;

pub struct SSTSampler;

impl SSTSampler {
    pub fn sample_sstables(
        sstables: &[(String, u64)], // (path, row_count)
        sst_rate: f64,
        seed: u64,
    ) -> Vec<String> {
        let total = sstables.len();
        if total == 0 {
            return Vec::new();
        }
        let n = ((sst_rate * total as f64).floor() as usize)
            .max(1)
            .min(total);

        if total == 1 && sst_rate < 1.0 {
            eprintln!(
                "[WARN] Only 1 segment available. SST-level sampling disabled. Run COMPACT or RESPLIT to improve sampling granularity."
            );
        }

        if n == total {
            return sstables.iter().map(|(p, _)| p.clone()).collect();
        }

        let weights: Vec<u64> = sstables.iter().map(|(_, c)| (*c).max(1)).collect();
        let dist = WeightedIndex::new(&weights).expect("Failed to create weighted index");
        let mut rng = ChaCha8Rng::seed_from_u64(seed);
        let mut selected_indices = std::collections::HashSet::new();

        // Sample distinct indices
        while selected_indices.len() < n {
            selected_indices.insert(dist.sample(&mut rng));
        }

        selected_indices
            .into_iter()
            .map(|i| sstables[i].0.clone())
            .collect()
    }
}
