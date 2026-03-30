use crate::types::{RowDisk, get_value, Confidence};
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

pub struct RowSampler;

impl RowSampler {
    pub fn sample_rows(
        rows: Vec<RowDisk>,
        row_rate: f64,
        seed: u64,
        _min_group_rows: u64,
        group_by: &Option<String>,
    ) -> (Vec<RowDisk>, Vec<Confidence>) {
        let mut sampled_rows = Vec::new();
        let mut group_counts = std::collections::HashMap::new();

        for (offset, row) in rows.into_iter().enumerate() {
            let mut hasher = DefaultHasher::new();
            seed.hash(&mut hasher);
            offset.hash(&mut hasher);
            let h = hasher.finish();
            
            let include = (h % 1000) < (row_rate * 1000.0) as u64;

            if include {
                if let Some(group_col) = group_by {
                    if let Some(val) = get_value(&row, group_col) {
                        let key = format!("{:?}", val);
                        let count = group_counts.entry(key).or_insert(0);
                        *count += 1;
                    }
                }
                sampled_rows.push(row);
            }
        }

        let confidences: Vec<Confidence> = vec![Confidence(1.0)];
        (sampled_rows, confidences)
    }
    
    pub fn is_row_sampled(offset: u64, row_rate: f64, seed: u64) -> bool {
        let mut hasher = DefaultHasher::new();
        seed.hash(&mut hasher);
        offset.hash(&mut hasher);
        let h = hasher.finish();
        (h % 1000) < (row_rate * 1000.0) as u64
    }
}
