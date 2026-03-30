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

            // Type-specific loading
            let mut i64_cols: HashMap<String, Vec<i64>> = HashMap::new();
            let mut f64_cols: HashMap<String, Vec<f64>> = HashMap::new();
            let str_cols: HashMap<String, Vec<String>> = HashMap::new();

            for col in columns_needed {
                // Determine type from metadata or convention
                // For this prototype, we'll try to guess or use i64 as default
                if col == "timestamp" || col == "user_id" || col == "status" || col == "country" || col == "level" {
                    let data = reader.read_column_i64(col).map_err(|e| QueryError::StorageError(e))?;
                    i64_cols.insert(col.clone(), data);
                } else {
                    let data = reader.read_column_f64(col).map_err(|e| QueryError::StorageError(e))?;
                    f64_cols.insert(col.clone(), data);
                }
            }
            overall_profile.io_read_ms += start_seg.elapsed().as_secs_f64() * 1000.0;

            let num_rows = reader.metadata.row_count as usize;
            let group_size = config.row_group_size as usize;
            let num_groups = (num_rows + group_size - 1) / group_size;

            for group_idx in 0..num_groups {
                if ColumnSampler::should_sample_row_group(seg_idx, group_idx, row_rate, config.seed) {
                    let start_idx = group_idx * group_size;
                    let end_idx = (start_idx + group_size).min(num_rows);
                    let actual_group_rows = end_idx - start_idx;
                    total_rows_read += actual_group_rows;

                    // Aggregate within the row group
                    for i in start_idx..end_idx {
                        // Check filter first
                        if let Some(filter) = &plan.filter {
                            let match_found = if let Some(d) = i64_cols.get(&filter.column) {
                                if let Some(target) = filter_i64 {
                                    d[i] == target
                                } else {
                                    d[i].to_string() == filter.value
                                }
                            } else if let Some(d) = f64_cols.get(&filter.column) {
                                if let Some(target) = filter_f64 {
                                    (d[i] - target).abs() < 1e-6
                                } else {
                                    d[i].to_string() == filter.value
                                }
                            } else if let Some(d) = str_cols.get(&filter.column) {
                                d[i] == filter.value
                            } else {
                                false
                            };
                            if !match_found { continue; }
                        }

                        // Get group key
                        let group_key = if let Some(group_col) = &plan.group_by {
                            if let Some(d) = i64_cols.get(group_col) {
                                d[i].to_string()
                            } else if let Some(d) = f64_cols.get(group_col) {
                                d[i].to_string()
                            } else if let Some(d) = str_cols.get(group_col) {
                                d[i].clone()
                            } else {
                                "null".to_string()
                            }
                        } else {
                            "scalar".to_string()
                        };

                        // Aggregate value
                        let agg_val = match &plan.aggregation {
                            Aggregation::Count => 1.0,
                            Aggregation::Sum(col) | Aggregation::Avg(col) => {
                                if let Some(d) = i64_cols.get(col) {
                                    let v = d[i] as f64;
                                    if v < 0.0 && col == "timestamp" {
                                         // This should never happen for timestamps
                                    }
                                    v
                                } else if let Some(d) = f64_cols.get(col) {
                                    d[i]
                                } else {
                                    0.0
                                }
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

        if scalar_sum < 0.0 && format!("{:?}", plan.aggregation).contains("timestamp") {
            // eprintln!("DEBUG: Negative sum detected: {}", scalar_sum);
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
