use rayon::prelude::*;
use crate::types::{RowDisk, QueryResult, AggregateValue, Confidence, get_value, Value};
use crate::query::ast::{QueryPlan, FilterOp};
use crate::query::executor::aggregator::Aggregator;
use crate::storage::sstable::reader::SSTableReader;
// Removed unused BloomFilterWrapper import
use crate::errors::{QueryError, StorageError};

pub struct Pipeline {
    pub sstables: Vec<String>,
}

impl Pipeline {
    pub fn new(sstables: Vec<String>) -> Self {
        Self { sstables }
    }

    pub fn execute(&self, plan: &QueryPlan, config: &crate::config::Config) -> Result<QueryResult, QueryError> {
        // 1. Calculate sampling rates
        let sampling_rate = crate::aqp::accuracy::AccuracyCalculator::calculate_sampling_rate(
            config.accuracy_target,
            config.k
        );
        let (sst_rate, row_rate) = crate::aqp::accuracy::AccuracyCalculator::split_sampling_rate(sampling_rate);

        let mut warnings = Vec::new();
        if sampling_rate < 0.05 {
            warnings.push(format!("Sampling rate clamped to 0.05 (calculated: {})", (1.0 - config.accuracy_target).powf(config.k)));
        }

        // 2. Bloom filter check (equality only)
        let filtered_sstables = self.apply_bloom_filter(plan);

        // 3. SSTable-level sampling
        // We need row counts for weighted sampling
        let sst_with_counts: Vec<(String, u64)> = filtered_sstables.iter()
            .map(|path| {
                let _reader = SSTableReader::new(path);
                // Simple version: we'd ideally store row counts in a manifest or header we can read quickly
                // For MVP, we'll just read enough to get the count
                let count = if let Ok(mut file) = std::fs::File::open(path) {
                    let mut buf = [0u8; 8];
                    use std::io::{Read, Seek, SeekFrom};
                    if file.seek(SeekFrom::Start(4)).is_ok() && file.read_exact(&mut buf).is_ok() {
                        u64::from_le_bytes(buf)
                    } else { 0 }
                } else { 0 };
                (path.clone(), count)
            })
            .collect();

        let sampled_sst_paths = crate::aqp::sampler::sst_sampler::SSTSampler::sample_sstables(
            &sst_with_counts,
            sst_rate,
            config.seed
        );

        let sampled_sst_paths_with_counts: Vec<(String, u64)> = sampled_sst_paths.into_iter().filter_map(|p| {
            sst_with_counts.iter().find(|(path, _)| *path == p).cloned()
        }).collect();

        // 4. Row-level sampling and Scanning
        let all_sampled_rows = self.scan_and_sample(&sampled_sst_paths_with_counts, plan, row_rate, config.seed, config.min_group_rows)?;

        if all_sampled_rows.is_empty() {
             return Ok(QueryResult {
                 value: AggregateValue::Empty,
                 confidence: Confidence(0.0),
                 warnings: vec!["Empty sample obtained".to_string()],
             });
        }

        // 5. Aggregation
        let aggregator = Aggregator::new(plan.aggregation.clone(), plan.group_by.clone(), config.min_group_rows);
        let agg_value = aggregator.aggregate(&all_sampled_rows);

        // 6. Scale result
        let scaled_value = crate::aqp::estimator::Estimator::scale_result(
            agg_value,
            &plan.aggregation,
            sampling_rate
        );

        // 7. Confidence check
        let total_sampled = all_sampled_rows.len();
        let score = if total_sampled == 0 {
            0.0
        } else {
            (1.0 - (1.96 / (total_sampled as f64).sqrt())).max(0.0)
        };
        
        let confidence = Confidence(score);
        if total_sampled < config.low_confidence_threshold as usize {
            warnings.push(format!("Low sample count: {} (Target: {})", total_sampled, config.low_confidence_threshold));
        }

        Ok(QueryResult {
            value: scaled_value,
            confidence,
            warnings,
        })
    }

    fn apply_bloom_filter(&self, plan: &QueryPlan) -> Vec<String> {
        if let Some(filter) = &plan.filter {
            if filter.op == FilterOp::Eq && filter.column == "user_id" {
                if let Ok(user_id) = filter.value.parse::<u64>() {
                    return self.sstables.iter()
                        .filter(|path| {
                            let reader = SSTableReader::new(path);
                            if let Ok(bloom) = reader.get_bloom_filter() {
                                bloom.contains(user_id)
                            } else {
                                true // Fallback to scan if bloom missing/error
                            }
                        })
                        .cloned()
                        .collect();
                }
            }
        }
        self.sstables.clone()
    }

    fn scan_and_sample(&self, sstables: &[(String, u64)], plan: &QueryPlan, row_rate: f64, seed: u64, min_group_rows: u64) -> Result<Vec<RowDisk>, QueryError> {
        let mut starts = Vec::with_capacity(sstables.len());
        let mut current_offset = 0;
        for (_, count) in sstables {
            starts.push(current_offset);
            current_offset += count;
        }

        let results: Result<Vec<Vec<RowDisk>>, StorageError> = sstables.par_iter().zip(starts.par_iter())
            .map(|((path, _), start_offset)| {
                let reader = SSTableReader::new(path);
                let rows = reader.read_rows()?;
                
                let mut group_counts = std::collections::HashMap::new();
                let mut sampled = Vec::new();
                
                for (i, r) in rows.into_iter().enumerate() {
                    let global_offset = *start_offset + i as u64;
                    let is_sampled = crate::aqp::sampler::row_sampler::RowSampler::is_row_sampled(global_offset, row_rate, seed);
                    
                    if !self.apply_filter(&r, plan) {
                        continue;
                    }

                    let mut force_include = false;
                    if let Some(group_col) = &plan.group_by {
                        if let Some(val) = get_value(&r, group_col) {
                            let key = format!("{:?}", val);
                            let count = group_counts.entry(key).or_insert(0);
                            if *count < min_group_rows {
                                force_include = true;
                            }
                            if is_sampled || force_include {
                                *count += 1;
                            }
                        }
                    }

                    if is_sampled || force_include {
                        sampled.push(r);
                    }
                }
                
                Ok(sampled)
            })
            .collect();

        let rows_vec = results.map_err(|e| QueryError::UnsupportedOperation(e.to_string()))?;
        Ok(rows_vec.into_iter().flatten().collect())
    }

    fn apply_filter(&self, row: &RowDisk, plan: &QueryPlan) -> bool {
        if let Some(filter) = &plan.filter {
            if let Some(val) = get_value(row, &filter.column) {
                match val {
                    Value::Int(row_val) => {
                        if let Ok(filter_val) = filter.value.parse::<i64>() {
                            return match filter.op {
                                FilterOp::Eq => row_val == filter_val,
                                FilterOp::Gt => row_val > filter_val,
                                FilterOp::Lt => row_val < filter_val,
                                FilterOp::Ge => row_val >= filter_val,
                                FilterOp::Le => row_val <= filter_val,
                            };
                        }
                    }
                    _ => return false,
                }
            }
            return false;
        }
        true
    }
}
