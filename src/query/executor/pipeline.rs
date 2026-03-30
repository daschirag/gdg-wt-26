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
        let mut overall_profile = crate::types::QueryProfile::default();
        
        // 1. Metadata-Only Fast Path (for global COUNT/SUM/AVG without filters)
        if plan.filter.is_none() && plan.group_by.is_none() {
            let mut total_count = 0u64;
            let mut total_sum = 0.0;
            let mut possible = true;
            let mut found_any = false;

            for path in &self.sstables {
                if let Ok(meta) = SSTableReader::new(path).get_metadata() {
                    found_any = true;
                    total_count += meta.row_count;
                    match &plan.aggregation {
                        crate::query::ast::Aggregation::Count => {}
                        crate::query::ast::Aggregation::Sum(col) | crate::query::ast::Aggregation::Avg(col) => {
                            // Find column index from schema to match metadata col_N
                            if let Some(idx) = config.schema.columns.iter().position(|c| &c.name == col) {
                                let key = format!("col_{}", idx);
                                if let Some(s) = meta.sums.get(&key) {
                                    total_sum += *s;
                                } else { possible = false; break; }
                            } else { possible = false; break; }
                        }
                    }
                } else { possible = false; break; }
            }

            if possible && found_any {
                let value = match &plan.aggregation {
                    crate::query::ast::Aggregation::Count => AggregateValue::Scalar(total_count as f64),
                    crate::query::ast::Aggregation::Sum(_) => AggregateValue::Scalar(total_sum),
                    crate::query::ast::Aggregation::Avg(_) => {
                        if total_count > 0 { AggregateValue::Scalar(total_sum / total_count as f64) } 
                        else { AggregateValue::Empty }
                    }
                };
                
                return Ok(QueryResult {
                    value,
                    confidence: Confidence(1.0), // Exact
                    rows_read: 0, // No rows actually read
                    warnings: vec!["Metadata Fast-Path: Result is exact".to_string()],
                    profile: overall_profile,
                });
            }
        }

        // 2. Calculate sampling rates
        let start_sampling = std::time::Instant::now();
        let sampling_rate = crate::aqp::accuracy::AccuracyCalculator::calculate_sampling_rate(
            config.accuracy_target,
            config.k
        );
        let (sst_rate, row_rate) = crate::aqp::accuracy::AccuracyCalculator::split_sampling_rate(sampling_rate);
        overall_profile.sst_sampling_ms = start_sampling.elapsed().as_secs_f64() * 1000.0;

        let mut warnings = Vec::new();
        if sampling_rate < 0.05 {
            warnings.push(format!("Sampling rate clamped to 0.05 (calculated: {})", (1.0 - config.accuracy_target).powf(config.k)));
        }

        // 2. Bloom filter check (equality only)
        let start_bloom = std::time::Instant::now();
        let filtered_sstables = self.apply_bloom_filter(plan);
        overall_profile.bloom_filter_ms = start_bloom.elapsed().as_secs_f64() * 1000.0;

        // 3. SSTable-level sampling
        let start_sst_sample = std::time::Instant::now();
        let sst_with_counts: Vec<(String, u64)> = filtered_sstables.iter()
            .map(|path| {
                let count = SSTableReader::new(path).get_metadata().map(|m| m.row_count).unwrap_or(0);
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
        overall_profile.sst_sampling_ms += start_sst_sample.elapsed().as_secs_f64() * 1000.0;

        // 4. Row-level sampling and Scanning
        let (all_sampled_rows, scan_profile) = self.scan_and_sample(&sampled_sst_paths_with_counts, plan, row_rate, config.seed, config.min_group_rows, config)?;
        
        // Merge scan profile into overall profile
        overall_profile.io_read_ms += scan_profile.io_read_ms;
        overall_profile.deserialization_ms += scan_profile.deserialization_ms;
        overall_profile.crc_verify_ms += scan_profile.crc_verify_ms;
        overall_profile.filtering_ms += scan_profile.filtering_ms;

        if all_sampled_rows.is_empty() {
             return Ok(QueryResult {
                 value: AggregateValue::Empty,
                 confidence: Confidence(0.0),
                 warnings: vec!["Empty sample obtained".to_string()],
                 rows_read: 0,
                 profile: overall_profile,
             });
        }

        // 5. Aggregation
        let start_agg = std::time::Instant::now();
        let aggregator = Aggregator::new(plan.aggregation.clone(), plan.group_by.clone(), config.min_group_rows, config);
        let agg_value = aggregator.aggregate(&all_sampled_rows);
        overall_profile.aggregation_ms = start_agg.elapsed().as_secs_f64() * 1000.0;

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
            rows_read: total_sampled,
            profile: overall_profile,
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

    fn scan_and_sample(&self, sstables: &[(String, u64)], plan: &QueryPlan, row_rate: f64, seed: u64, _min_group_rows: u64, config: &crate::config::Config) -> Result<(Vec<RowDisk>, crate::types::QueryProfile), QueryError> {
        let mut starts = Vec::with_capacity(sstables.len());
        let mut current_offset = 0;
        for (_, count) in sstables {
            starts.push(current_offset);
            current_offset += count;
        }

        let results: Result<Vec<(Vec<RowDisk>, crate::types::QueryProfile)>, StorageError> = sstables.par_iter().zip(starts.par_iter())
            .map(|((path, _), start_offset)| {
                let reader = SSTableReader::new(path);
                let (rows, mut file_profile) = reader.read_rows_profiled(config.verify_crc)?;
                
                let start_filter = std::time::Instant::now();
                let mut group_counts = std::collections::HashMap::new();
                let mut sampled = Vec::new();
                
                for (i, r) in rows.into_iter().enumerate() {
                    let global_offset = *start_offset + i as u64;
                    let is_sampled = crate::aqp::sampler::row_sampler::RowSampler::is_row_sampled(global_offset, row_rate, seed);
                    
                    if !self.apply_filter(&r, plan, config) {
                        continue;
                    }

                    let mut force_include = false;
                    if let Some(group_col) = &plan.group_by {
                        if let Some(val) = get_value(&r, group_col, config) {
                            let key = format!("{:?}", val);
                            let count = group_counts.entry(key).or_insert(0);
                            if *count < config.min_group_rows {
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
                file_profile.filtering_ms = start_filter.elapsed().as_secs_f64() * 1000.0;
                
                Ok((sampled, file_profile))
            })
            .collect();

        let intermediate = results.map_err(|e| QueryError::UnsupportedOperation(e.to_string()))?;
        
        let mut all_rows = Vec::new();
        let mut merged_profile = crate::types::QueryProfile::default();
        
        for (rows, profile) in intermediate {
            all_rows.extend(rows);
            merged_profile.io_read_ms += profile.io_read_ms;
            merged_profile.deserialization_ms += profile.deserialization_ms;
            merged_profile.crc_verify_ms += profile.crc_verify_ms;
            merged_profile.filtering_ms += profile.filtering_ms;
        }
        
        Ok((all_rows, merged_profile))
    }

    fn apply_filter(&self, row: &RowDisk, plan: &QueryPlan, config: &crate::config::Config) -> bool {
        if let Some(filter) = &plan.filter {
            if let Some(val) = get_value(row, &filter.column, config) {
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
