use crate::types::{QueryResult, AggregateValue, Confidence};
use crate::storage::sstable::reader::SSTableReader;
use crate::storage::sstable::columnar::ColumnarReader;
use crate::query::ast::{QueryPlan, FilterOp};
use crate::errors::QueryError;
use crate::query::executor::aggregator::Aggregator;

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
                // Try Columnar first
                if let Ok(meta) = ColumnarReader::get_metadata(path) {
                    found_any = true;
                    total_count += meta.row_count;
                    match &plan.aggregation {
                        crate::query::ast::Aggregation::Count => {}
                        crate::query::ast::Aggregation::Sum(col) | crate::query::ast::Aggregation::Avg(col) => {
                            if let Some(s) = meta.sums.get(col) {
                                total_sum += *s;
                            } else { possible = false; break; }
                        }
                    }
                    continue;
                }

                // Fallback to old format
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
                let count = self.get_row_count(path);
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
        let required_cols = plan.required_columns();
        
        let start_scan = std::time::Instant::now();
        let mut i64_cols_all = std::collections::HashMap::new();
        let mut f64_cols_all = std::collections::HashMap::new();
        
        // For simplicity, we'll assume all SSTables are columnar for this path.
        // In a real system, we'd have a way to handle both.
        for (path, _) in &sampled_sst_paths_with_counts {
            let reader = ColumnarReader::new(path);
            for col in &required_cols {
                if let Ok(data) = reader.read_column_i64(col) {
                    i64_cols_all.entry(col.clone()).or_insert(Vec::new()).extend(data);
                } else if let Ok(data) = reader.read_column_f64(col) {
                    f64_cols_all.entry(col.clone()).or_insert(Vec::new()).extend(data);
                }
            }
        }
        overall_profile.io_read_ms = start_scan.elapsed().as_secs_f64() * 1000.0;
        overall_profile.deserialization_ms = 0.0; // Minimal in columnar

        // 5. Filtering and Sampling (Columnar)
        let start_filter = std::time::Instant::now();
        let filtered_indices = self.apply_filter_columnar(&i64_cols_all, &f64_cols_all, plan);
        
        let mut final_i64 = std::collections::HashMap::new();
        let mut final_f64 = std::collections::HashMap::new();
        
        let mut sampled_count = 0;
        for idx in filtered_indices {
            // Apply row-level sampling
            if crate::aqp::sampler::row_sampler::RowSampler::is_row_sampled(idx as u64, row_rate, config.seed) {
                sampled_count += 1;
                for (col, data) in &i64_cols_all {
                    final_i64.entry(col.clone()).or_insert(Vec::new()).push(data[idx]);
                }
                for (col, data) in &f64_cols_all {
                    final_f64.entry(col.clone()).or_insert(Vec::new()).push(data[idx]);
                }
            }
        }
        overall_profile.filtering_ms = start_filter.elapsed().as_secs_f64() * 1000.0;

        if sampled_count == 0 {
             return Ok(QueryResult {
                 value: AggregateValue::Empty,
                 confidence: Confidence(0.0),
                 warnings: vec!["Empty sample obtained".to_string()],
                 rows_read: 0,
                 profile: overall_profile,
             });
        }

        // 6. Aggregation
        let start_agg = std::time::Instant::now();
        let aggregator = Aggregator::new(plan.aggregation.clone(), plan.group_by.clone(), config.min_group_rows, config);
        let agg_value = aggregator.aggregate_columnar(&final_i64, &final_f64);
        overall_profile.aggregation_ms = start_agg.elapsed().as_secs_f64() * 1000.0;

        // 7. Scale result
        let scaled_value = crate::aqp::estimator::Estimator::scale_result(
            agg_value,
            &plan.aggregation,
            sampling_rate
        );

        // 8. Confidence check
        let total_sampled = sampled_count;
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
                            // Only works for old format. Columnar has no bloom yet.
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

    fn get_row_count(&self, path: &str) -> u64 {
        // Try columnar first
        if let Ok(meta) = ColumnarReader::get_metadata(path) {
            return meta.row_count;
        }
        
        // Fallback to old format
        let reader = SSTableReader::new(path);
        reader.get_metadata().map(|m| m.row_count).unwrap_or(0)
    }

    fn apply_filter_columnar(&self, i64_cols: &std::collections::HashMap<String, Vec<i64>>, f64_cols: &std::collections::HashMap<String, Vec<f64>>, plan: &QueryPlan) -> Vec<usize> {
        let row_count = i64_cols.values().next().map(|v| v.len())
            .or_else(|| f64_cols.values().next().map(|v| v.len()))
            .unwrap_or(0);

        if let Some(filter) = &plan.filter {
            let mut indices = Vec::new();
            if let Some(vals) = i64_cols.get(&filter.column) {
                if let Ok(filter_val) = filter.value.parse::<i64>() {
                    match filter.op {
                        FilterOp::Eq => for (i, &v) in vals.iter().enumerate() { if v == filter_val { indices.push(i); } },
                        FilterOp::Gt => for (i, &v) in vals.iter().enumerate() { if v > filter_val { indices.push(i); } },
                        FilterOp::Lt => for (i, &v) in vals.iter().enumerate() { if v < filter_val { indices.push(i); } },
                        FilterOp::Ge => for (i, &v) in vals.iter().enumerate() { if v >= filter_val { indices.push(i); } },
                        FilterOp::Le => for (i, &v) in vals.iter().enumerate() { if v <= filter_val { indices.push(i); } },
                    }
                }
            } else if let Some(vals) = f64_cols.get(&filter.column) {
                if let Ok(filter_val) = filter.value.parse::<f64>() {
                    match filter.op {
                        FilterOp::Eq => for (i, &v) in vals.iter().enumerate() { if (v - filter_val).abs() < f64::EPSILON { indices.push(i); } },
                        FilterOp::Gt => for (i, &v) in vals.iter().enumerate() { if v > filter_val { indices.push(i); } },
                        FilterOp::Lt => for (i, &v) in vals.iter().enumerate() { if v < filter_val { indices.push(i); } },
                        FilterOp::Ge => for (i, &v) in vals.iter().enumerate() { if v >= filter_val { indices.push(i); } },
                        FilterOp::Le => for (i, &v) in vals.iter().enumerate() { if v <= filter_val { indices.push(i); } },
                    }
                }
            }
            indices
        } else {
            (0..row_count).collect()
        }
    }
}
