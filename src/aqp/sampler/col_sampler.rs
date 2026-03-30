use crate::aqp::sampler::row_sampler::RowSampler;
use rand::prelude::*;

pub struct ColumnSampler;

impl ColumnSampler {
    pub fn sample_segments(segments: &[String], sst_rate: f64, seed: u64) -> Vec<String> {
        let n = ((sst_rate * segments.len() as f64).floor() as usize).max(1).min(segments.len());
        if n == segments.len() {
            return segments.to_vec();
        }
        
        let mut rng = rand::prelude::StdRng::seed_from_u64(seed);
        let mut indices: Vec<usize> = (0..segments.len()).collect();
        indices.shuffle(&mut rng);
        
        indices.into_iter().take(n).map(|i| segments[i].clone()).collect()
    }

    pub fn should_sample_row_group(_seg_idx: usize, group_idx: usize, row_rate: f64, seed: u64) -> bool {
        RowSampler::is_row_sampled(group_idx as u64, row_rate, seed)
    }
}
