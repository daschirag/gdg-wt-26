use rand::prelude::*;
use rand_chacha::ChaCha8Rng;

pub struct SSTSampler;

impl SSTSampler {
    pub fn sample_sstables(
        sstables: &[(String, u64)], // (path, row_count)
        sst_rate: f64,
        seed: u64
    ) -> Vec<String> {
        let mut rng = ChaCha8Rng::seed_from_u64(seed);
        let total_sstables = sstables.len();
        let num_to_select = (sst_rate * total_sstables as f64).floor() as usize;
        let num_to_select = num_to_select.max(1).min(total_sstables);

        if num_to_select == total_sstables {
            return sstables.iter().map(|(p, _)| p.clone()).collect();
        }

        let total_rows: u64 = sstables.iter().map(|(_, c)| *c).sum();
        if total_rows == 0 {
             return sstables.iter().take(num_to_select).map(|(p, _)| p.clone()).collect();
        }

        // Weighted random selection by row_count
        let mut weights = Vec::with_capacity(total_sstables);
        for (_, count) in sstables {
            weights.push(*count as f64 / total_rows as f64);
        }

        let mut indices: Vec<usize> = (0..total_sstables).collect();
        indices.sort_by(|&a, &b| {
            let wa = weights[a];
            let wb = weights[b];
            wb.partial_cmp(&wa).unwrap() // Simple heuristic: pick largest first, or use a proper weighted sampler
        });
        
        // Actually, the spec says "Weighted random selection by row_count"
        // Let's use a proper weighted sampler
        let mut selected = Vec::new();
        let mut remaining = sstables.to_vec();
        
        while selected.len() < num_to_select && !remaining.is_empty() {
             let current_total: u64 = remaining.iter().map(|(_, c)| *c).sum();
             if current_total == 0 { break; }
             
             let roll = rng.gen_range(0..current_total);
             let mut cumulative = 0;
             let mut found_idx = 0;
             for (idx, (_, count)) in remaining.iter().enumerate() {
                 cumulative += *count;
                 if roll < cumulative {
                     found_idx = idx;
                     break;
                 }
             }
             let (path, _) = remaining.remove(found_idx);
             selected.push(path);
        }

        selected
    }
}
