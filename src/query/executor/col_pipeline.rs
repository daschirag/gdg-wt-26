use crate::types::{QueryResult, AggregateValue, Confidence};
use crate::query::ast::{QueryPlan, Aggregation};
use crate::storage::columnar::reader::ColumnarReader;
use crate::aqp::sampler::col_sampler::ColumnSampler;
use crate::errors::QueryError;
use std::collections::HashMap;
use std::time::Instant;

pub struct ColumnarPipeline {
    pub segments: Vec<String>,
}

impl ColumnarPipeline {
    pub fn new(segments: Vec<String>) -> Self {
        Self { segments }
    }

    pub fn execute(&self, plan: &QueryPlan, columns_needed: &[String], config: &crate::config::Config) -> Result<QueryResult, QueryError> {
        let mut overall_profile = crate::types::QueryProfile::default();
        let start_total = Instant::now();

        // 0. Metadata-Only Fast Path
        if plan.filter.is_none() && plan.group_by.is_none() {
            let mut total_count = 0u64;
            let mut total_sum = 0.0;
            let mut possible = true;

            for path in &self.segments {
                let reader = ColumnarReader::new(path.clone().into()).map_err(|e| QueryError::StorageError(e))?;
                total_count += reader.metadata.row_count;
                match &plan.aggregation {
                    Aggregation::Count => {}
                    Aggregation::Sum(col) | Aggregation::Avg(col) => {
                        if let Some(s) = reader.metadata.sums.get(col) {
                            total_sum += *s;
                        } else {
                            // Fallback to col_N if name missing in sums but exist in schema
                            if let Some(pos) = config.schema.columns.iter().position(|c| &c.name == col) {
                                let alt_key = format!("col_{}", pos);
                                if let Some(s) = reader.metadata.sums.get(&alt_key) {
                                    total_sum += *s;
                                } else { possible = false; break; }
                            } else { possible = false; break; }
                        }
                    }
                }
            }

            if possible {
                let value = match &plan.aggregation {
                    Aggregation::Count => AggregateValue::Scalar(total_count as f64),
                    Aggregation::Sum(_) => AggregateValue::Scalar(total_sum),
                    Aggregation::Avg(_) => if total_count > 0 { AggregateValue::Scalar(total_sum / total_count as f64) } else { AggregateValue::Empty },
                };
                return Ok(QueryResult {
                    value,
                    confidence: Confidence(1.0),
                    rows_read: 0,
                    warnings: vec!["Metadata Fast-Path: Result is exact".to_string()],
                    profile: overall_profile,
                });
            }
        }

        // 1. Calculate sampling rates
        let sampling_rate = crate::aqp::accuracy::AccuracyCalculator::calculate_sampling_rate(
            config.accuracy_target,
            config.k
        );
        let sst_rate = sampling_rate.sqrt();
        let row_rate = sampling_rate.sqrt();

        // 2. Segment-level sampling
        let start_sst_sample = Instant::now();
        let sampled_segments = ColumnSampler::sample_segments(&self.segments, sst_rate, config.seed);
        overall_profile.sst_sampling_ms = start_sst_sample.elapsed().as_secs_f64() * 1000.0;

        let mut total_rows_read = 0;
        let mut group_map: HashMap<String, (f64, u64)> = HashMap::new();
        let mut scalar_sum = 0.0;
        let mut scalar_count = 0u64;

        // 3. Process each sampled segment
        for (seg_idx, seg_path) in sampled_segments.iter().enumerate() {
            let start_seg = Instant::now();
            let reader = ColumnarReader::new(seg_path.into()).map_err(|e| QueryError::StorageError(e))?;
            
            // Pre-parse filters for performance
            let filter_i64 = plan.filter.as_ref().and_then(|f| f.value.parse::<i64>().ok());
            let filter_f64 = plan.filter.as_ref().and_then(|f| f.value.parse::<f64>().ok());

            // Fix B: Column-at-a-time loading (Segment scan)
            // Load all columns indexed by their position in the schema to avoid string lookups
            let schema_len = config.schema.columns.len();
            let mut all_i64: Vec<Option<Vec<i64>>> = vec![None; schema_len];
            let mut all_f64: Vec<Option<Vec<f64>>> = vec![None; schema_len];

            for (idx, col_def) in config.schema.columns.iter().enumerate() {
                if columns_needed.contains(&col_def.name) {
                    if col_def.name == "timestamp" || col_def.name == "user_id" || col_def.name == "status" || col_def.name == "country" || col_def.name == "level" {
                        if let Ok(data) = reader.read_column_i64(&col_def.name) {
                            all_i64[idx] = Some(data);
                        }
                    } else {
                        if let Ok(data) = reader.read_column_f64(&col_def.name) {
                            all_f64[idx] = Some(data);
                        }
                    }
                }
            }
            overall_profile.io_read_ms += start_seg.elapsed().as_secs_f64() * 1000.0;

            // Fix C: Index-based Column Access (No hash lookups in loop)
            let filter_col_idx = plan.filter.as_ref().and_then(|f| config.schema.columns.iter().position(|c| c.name == f.column));
            let group_col_idx = plan.group_by.as_ref().and_then(|g| config.schema.columns.iter().position(|c| c.name == *g));
            let agg_col_idx = match &plan.aggregation {
                Aggregation::Count => None,
                Aggregation::Sum(c) | Aggregation::Avg(c) => config.schema.columns.iter().position(|col| col.name == *c),
            };
            let user_id_idx = config.schema.columns.iter().position(|c| c.name == "user_id");

            let num_rows = reader.metadata.row_count as usize;
            let group_size = config.row_group_size as usize;
            let num_groups = (num_rows + group_size - 1) / group_size;

            // Fix D: Compacted Deduplication Bypass
            let skip_dedup = reader.metadata.is_compacted;
            let mut seen = std::collections::HashSet::new();

            for group_idx in 0..num_groups {
                if ColumnSampler::should_sample_row_group(seg_idx, group_idx, row_rate, config.seed) {
                    let start_idx = group_idx * group_size;
                    let end_idx = (start_idx + group_size).min(num_rows);
                    let actual_group_rows = end_idx - start_idx;
                    total_rows_read += actual_group_rows;

                    // Aggregate within the row group
                    for i in start_idx..end_idx {
                        // Check filter first
                        let mut passes_filter = true;
                        if let Some(f_idx) = filter_col_idx {
                            if let Some(f) = &plan.filter {
                                // Try i64 first
                                if let Some(data) = &all_i64[f_idx] {
                                    if let Some(target) = filter_i64 {
                                        passes_filter = data[i] == target;
                                    } else {
                                        passes_filter = data[i].to_string() == f.value;
                                    }
                                } else if let Some(data) = &all_f64[f_idx] {
                                    if let Some(target) = filter_f64 {
                                        passes_filter = (data[i] - target).abs() < 1e-6;
                                    } else {
                                        passes_filter = data[i].to_string() == f.value;
                                    }
                                }
                            }
                        }
                        if !passes_filter { continue; }

                        // Fix D: Compacted Deduplication Bypass
                        if !skip_dedup {
                            if let Some(u_idx) = user_id_idx {
                                if let Some(user_ids) = &all_i64[u_idx] {
                                    let uid = user_ids[i] as u64;
                                    if seen.contains(&uid) { continue; }
                                    seen.insert(uid);
                                }
                            }
                        }

                        total_rows_read += 1;

                        // Get group key
                        let group_key = if let Some(g_idx) = group_col_idx {
                            if let Some(data) = &all_i64[g_idx] {
                                data[i].to_string()
                            } else if let Some(data) = &all_f64[g_idx] {
                                data[i].to_string()
                            } else {
                                "null".to_string()
                            }
                        } else {
                            "scalar".to_string()
                        };

                        // Aggregate value
                        let agg_val = match &plan.aggregation {
                            Aggregation::Count => 1.0,
                            Aggregation::Sum(_) | Aggregation::Avg(_) => {
                                if let Some(idx) = agg_col_idx {
                                    if let Some(data) = &all_i64[idx] {
                                        data[i] as f64
                                    } else if let Some(data) = &all_f64[idx] {
                                        data[i]
                                    } else { 0.0 }
                                } else { 0.0 }
                            }
                        };

                        if plan.group_by.is_some() {
                            let entry = group_map.entry(group_key).or_insert((0.0, 0));
                            entry.0 += agg_val;
                            entry.1 += 1;
                        } else {
                            scalar_sum += agg_val;
                            scalar_count += 1;
                        }
                    }
                }
            }
        }

        // 4. Scale results (Robust Scaling)
        let sst_scale = if !sampled_segments.is_empty() {
            self.segments.len() as f64 / sampled_segments.len() as f64
        } else {
            1.0 / sst_rate
        };
        let row_scale = 1.0 / row_rate;
        let scale_factor = sst_scale * row_scale;

        let value = if plan.group_by.is_some() {
            let mut group_results = Vec::new();
            for (k, (sum, count)) in group_map {
                let final_val = match &plan.aggregation {
                    Aggregation::Avg(_) => if count > 0 { sum / count as f64 } else { 0.0 },
                    _ => sum * scale_factor,
                };
                group_results.push((k, final_val, Confidence(0.95)));
            }
            AggregateValue::Groups(group_results)
        } else {
            let final_val = match &plan.aggregation {
                Aggregation::Avg(_) => if scalar_count > 0 { scalar_sum / scalar_count as f64 } else { 0.0 },
                _ => scalar_sum * scale_factor,
            };
            AggregateValue::Scalar(final_val)
        };

        overall_profile.aggregation_ms = start_total.elapsed().as_secs_f64() * 1000.0 - overall_profile.io_read_ms;

        Ok(QueryResult {
            value,
            confidence: Confidence(0.95), // Placeholder
            warnings: vec!["Columnar Fast-Path: AQP enabled".to_string()],
            rows_read: total_rows_read,
            profile: overall_profile,
        })
    }
}
