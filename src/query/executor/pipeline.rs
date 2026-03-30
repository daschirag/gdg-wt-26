use crate::types::{QueryResult, AggregateValue, ConfidenceFlag, StoragePath};
use crate::storage::sstable::reader::SSTableReader;
use crate::storage::columnar::reader::ColumnarReader;
use crate::query::ast::{QueryPlan, FilterOp, Aggregation};
use crate::errors::QueryError;
use crate::query::executor::aggregator::Aggregator;
use crate::utils::aligned_vec::AlignedVec;
use std::collections::HashSet;

pub struct Pipeline {
    pub sstables: Vec<String>,
}

impl Pipeline {
    pub fn new(sstables: Vec<String>) -> Self {
        Self { sstables }
    }

    pub fn execute(&self, plan: &QueryPlan, config: &crate::config::Config, mask: Option<&std::collections::HashSet<u64>>, skip_dedup: bool) -> Result<QueryResult, QueryError> {
        let mut overall_profile = crate::types::QueryProfile::default();
        
        // 1. Metadata-Only Fast Path (for global COUNT/SUM/AVG without filters)
        if plan.filter.is_none() && plan.group_by.is_none() {
            let mut total_count = 0u64;
            let mut total_sum = 0.0;
            let mut possible = true;
            let mut found_any = false;

            for path in &self.sstables {
                // Try Columnar first
                if let Ok(reader) = ColumnarReader::new(path.into()) {
                    let meta = &reader.metadata;
                    found_any = true;
                    total_count += meta.row_count;
                    match &plan.aggregation {
                        Aggregation::Count => {}
                        Aggregation::Sum(col) | Aggregation::Avg(col) => {
                            if let Some(c_meta) = meta.columns.get(col) {
                                total_sum += c_meta.sum;
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
                        Aggregation::Count => {}
                        Aggregation::Sum(col) | Aggregation::Avg(col) => {
                            if let Some(idx) = config.schema.col_index(col) {
                                let key = format!("col_{}", idx);
                                if let Some(c) = meta.columns.get(&key) {
                                    total_sum += c.sum;
                                } else { possible = false; break; }
                            } else { possible = false; break; }
                        }
                    }
                } else { possible = false; break; }
            }

            if possible && found_any {
                let value = match &plan.aggregation {
                    Aggregation::Count => AggregateValue::Scalar(total_count as f64),
                    Aggregation::Sum(_) => AggregateValue::Scalar(total_sum),
                    Aggregation::Avg(_) => {
                        if total_count > 0 { AggregateValue::Scalar(total_sum / total_count as f64) } 
                        else { AggregateValue::Empty }
                    }
                };
                
                return Ok(QueryResult {
                    value,
                    confidence: ConfidenceFlag::Exact,
                    rows_scanned: 0,
                    sampling_rate: 1.0,
                    estimated_variance: 0.0,
                    storage_path: StoragePath::Row,
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
        let needed_indices: Vec<usize> = required_cols.iter()
            .map(|name| config.schema.col_index(name).expect("Column not in schema"))
            .collect();

        let filter_col_idx = plan.filter.as_ref().and_then(|f| config.schema.col_index(&f.column));
        let group_col_idx = plan.group_by.as_ref().and_then(|g| config.schema.col_index(g));
        let agg_col_idx = match &plan.aggregation {
            Aggregation::Count => None,
            Aggregation::Sum(col) | Aggregation::Avg(col) => config.schema.col_index(col),
        };
        let user_id_idx = config.schema.col_index("user_id");

        let mut final_count = 0;
        let mut final_i64: std::collections::HashMap<usize, AlignedVec<i64>> = std::collections::HashMap::new();
        let mut final_f64: std::collections::HashMap<usize, AlignedVec<f64>> = std::collections::HashMap::new();
        let mut seen = mask.cloned().unwrap_or_default();

        for (path, _) in &sampled_sst_paths_with_counts {
            if let Ok(reader) = ColumnarReader::new(path.into()) {
                let start_seg_scan = std::time::Instant::now();
                
                // LOAD ALL NEEDED COLUMNS ONCE PER SEGMENT
                let mut seg_i64: std::collections::HashMap<usize, AlignedVec<i64>> = std::collections::HashMap::new();
                let mut seg_f64: std::collections::HashMap<usize, AlignedVec<f64>> = std::collections::HashMap::new();

                for &idx in &needed_indices {
                    let col_name = &config.schema.columns[idx].name;
                    let col_type = &config.schema.columns[idx].r#type;
                    if col_type == "i64" {
                        if let Ok(data) = reader.read_column_i64(col_name) {
                            seg_i64.insert(idx, data);
                        }
                    } else if col_type == "f64" {
                        if let Ok(data) = reader.read_column_f64(col_name) {
                            seg_f64.insert(idx, data);
                        }
                    }
                }
                overall_profile.io_read_ms += start_seg_scan.elapsed().as_secs_f64() * 1000.0;

                // 2. Filter this segment
                let start_seg_filter = std::time::Instant::now();
                let seg_filtered_indices = self.apply_filter_columnar_indexed(&seg_i64, &seg_f64, plan, filter_col_idx);
                
                if seg_filtered_indices.is_empty() {
                    continue;
                }

                // 4. Sample and Deduplicate
                for &idx in &seg_filtered_indices {
                    // Deduplication
                    if !skip_dedup {
                        if let Some(u_idx) = user_id_idx {
                            if let Some(user_ids) = seg_i64.get(&u_idx) {
                                let uid = user_ids[idx] as u64;
                                if seen.contains(&uid) { continue; }
                                seen.insert(uid);
                            }
                        }
                    }

                    if crate::aqp::sampler::row_sampler::RowSampler::is_row_sampled(idx as u64, row_rate, config.seed) {
                        final_count += 1;
                        for &col_idx in &needed_indices {
                            if let Some(data) = seg_i64.get(&col_idx) {
                                final_i64.entry(col_idx).or_insert_with(AlignedVec::new).push(data[idx]);
                            } else if let Some(data) = seg_f64.get(&col_idx) {
                                final_f64.entry(col_idx).or_insert_with(AlignedVec::new).push(data[idx]);
                            }
                        }
                    }
                }
                overall_profile.filtering_ms += start_seg_filter.elapsed().as_secs_f64() * 1000.0;
            }
        }
        overall_profile.deserialization_ms = 0.0;

        // 6. Final Aggregation
        let aggregator = Aggregator::new(plan.aggregation.clone(), plan.group_by.clone(), config.min_group_rows, config);
        let (agg_value, variance) = aggregator.aggregate_columnar(&final_i64, &final_f64, agg_col_idx, group_col_idx);

        Ok(QueryResult {
            value: agg_value,
            confidence: ConfidenceFlag::High,
            rows_scanned: final_count,
            sampling_rate: 1.0,
            estimated_variance: variance,
            storage_path: StoragePath::Columnar,
            warnings: Vec::new(),
            profile: overall_profile,
        })
    }

    fn apply_bloom_filter(&self, plan: &QueryPlan) -> Vec<String> {
        if let Some(filter) = &plan.filter {
            if filter.column == "user_id" && filter.op == FilterOp::Eq {
                if let Ok(uid) = filter.value.parse::<u64>() {
                    return self.sstables.iter()
                        .filter(|path| {
                            // Try columnar bloom first
                            let bloom_path = std::path::Path::new(path).join("bloom.bin");
                            if bloom_path.exists() {
                                if let Ok(bytes) = std::fs::read(bloom_path) {
                                    if let Ok(bloom) = bincode::deserialize::<crate::storage::bloom::filter::BloomFilterWrapper>(&bytes) {
                                        return bloom.contains(uid);
                                    }
                                }
                            }
                            true
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
        if let Ok(reader) = ColumnarReader::new(path.into()) {
            return reader.metadata.row_count;
        }
        
        // Fallback to old format
        let reader = SSTableReader::new(path);
        reader.get_metadata().map(|m| m.row_count).unwrap_or(0)
    }

    fn apply_filter_columnar_indexed(&self, i64_cols: &std::collections::HashMap<usize, AlignedVec<i64>>, f64_cols: &std::collections::HashMap<usize, AlignedVec<f64>>, plan: &QueryPlan, filter_col_idx: Option<usize>) -> Vec<usize> {
        let row_count = i64_cols.values().next().map(|v| v.len())
            .or_else(|| f64_cols.values().next().map(|v| v.len()))
            .unwrap_or(0);

        let mut indices = Vec::with_capacity(row_count);
        for i in 0..row_count {
            let matches = if let Some(f_idx) = filter_col_idx {
                if let Some(filter) = &plan.filter {
                    if let Some(vals) = i64_cols.get(&f_idx) {
                        if let Ok(f_val) = filter.value.parse::<i64>() {
                            match filter.op {
                                FilterOp::Eq => vals[i] == f_val,
                                FilterOp::Gt => vals[i] > f_val,
                                FilterOp::Lt => vals[i] < f_val,
                                FilterOp::Ge => vals[i] >= f_val,
                                FilterOp::Le => vals[i] <= f_val,
                            }
                        } else { true }
                    } else if let Some(vals) = f64_cols.get(&f_idx) {
                        if let Ok(f_val) = filter.value.parse::<f64>() {
                            match filter.op {
                                FilterOp::Eq => (vals[i] - f_val).abs() < 1e-6,
                                FilterOp::Gt => vals[i] > f_val,
                                FilterOp::Lt => vals[i] < f_val,
                                FilterOp::Ge => vals[i] >= f_val,
                                FilterOp::Le => vals[i] <= f_val,
                            }
                        } else { true }
                    } else { true }
                } else { true }
            } else { true };

            if matches {
                indices.push(i);
            }
        }
        indices
    }
}


