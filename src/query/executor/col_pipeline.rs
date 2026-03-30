use crate::types::{QueryResult, AggregateValue, ConfidenceFlag, StoragePath};
use crate::query::ast::{QueryPlan, Aggregation, FilterOp};
use crate::storage::columnar::reader::ColumnarReader;
use crate::aqp::sampler::col_sampler::ColumnSampler;
use crate::aqp::accuracy::AccuracyCalculator;
use crate::errors::QueryError;
use crate::utils::aligned_vec::AlignedVec;
use crate::query::executor::aggregator::GroupKey;
use std::collections::HashMap;
use std::time::Instant;
use rand::prelude::*;

pub struct ColumnarPipeline {
    pub segments: Vec<String>,
}

impl ColumnarPipeline {
    pub fn new(segments: Vec<String>) -> Self {
        Self { segments }
    }

    pub fn execute(&self, plan: &QueryPlan, columns_needed: &[String], config: &crate::config::Config, mask: Option<&std::collections::HashSet<u64>>, skip_dedup: bool) -> Result<QueryResult, QueryError> {
        let mut overall_profile = crate::types::QueryProfile::default();
        let start_total = Instant::now();

        // 1. Map column names to schema indices once
        let needed_indices: Vec<usize> = columns_needed.iter()
            .map(|name| config.schema.col_index(name).ok_or_else(|| QueryError::UnknownColumn(name.clone())))
            .collect::<Result<Vec<_>, _>>()?;

        let filter_col_idx = plan.filter.as_ref()
            .and_then(|f| config.schema.col_index(&f.column));
        
        let group_col_idx = plan.group_by.as_ref()
            .and_then(|g| config.schema.col_index(g));

        let agg_col_idx = match &plan.aggregation {
            Aggregation::Count => None,
            Aggregation::Sum(col) | Aggregation::Avg(col) => config.schema.col_index(col),
        };

        let user_id_idx = config.schema.col_index("user_id");

        // 2. Metadata-Only Fast Path
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
                        if let Some(c) = reader.metadata.columns.get(col) {
                            total_sum += c.sum;
                        } else if let Some(pos) = agg_col_idx {
                            let alt_key = format!("col_{}", pos);
                            if let Some(c) = reader.metadata.columns.get(&alt_key) {
                                total_sum += c.sum;
                            } else { possible = false; break; }
                        } else { possible = false; break; }
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
                    confidence: ConfidenceFlag::High,
                    storage_path: StoragePath::Columnar,
                    rows_scanned: 0,
                    sampling_rate: 1.0,
                    estimated_variance: 0.0,
                    warnings: vec!["Metadata Fast-Path: Result is exact".to_string()],
                    profile: overall_profile,
                });
            }
        }

        // 3. Calculate sampling rates
        let sampling_rate = crate::aqp::accuracy::AccuracyCalculator::calculate_sampling_rate(
            config.accuracy_target,
            config.k
        );

        // 4. Segment-level sampling
        let start_sst_sample = Instant::now();
        let sst_rate = sampling_rate.sqrt();
        let sampled_segments = if sst_rate >= 1.0 {
            self.segments.clone()
        } else {
            let mut rng = rand::prelude::StdRng::seed_from_u64(config.seed);
            let n = ((sst_rate * self.segments.len() as f64).floor() as usize).max(1).min(self.segments.len());
            let mut indices: Vec<usize> = (0..self.segments.len()).collect();
            indices.shuffle(&mut rng);
            indices.into_iter().take(n).map(|i| self.segments[i].clone()).collect()
        };
        overall_profile.sst_sampling_ms = start_sst_sample.elapsed().as_secs_f64() * 1000.0;

        let mut group_map: HashMap<GroupKey, (f64, u64)> = HashMap::with_capacity(1024);
        let mut total_rows_read = 0;
        let mut total_rows_possible = 0;
        let mut scalar_sum = 0.0;
        let mut scalar_count = 0u64;
        let mut scalar_sum_sq = 0.0;
        let row_rate = sampling_rate.sqrt();
        let mut current_row_rate = row_rate;
        let mut pilot_done = false;
        let mut seen = mask.cloned().unwrap_or_default();

        // 5. Row-level scanning
        let schema_len = config.schema.columns.len();
        for (seg_idx, seg_path) in sampled_segments.iter().enumerate() {
            let start_seg = Instant::now();
            let reader = ColumnarReader::new(seg_path.into()).map_err(|e| QueryError::StorageError(e))?;
            
            // USE INDEXED VECS INSTEAD OF HASHMAP - FIX C
            let mut all_i64: Vec<Option<AlignedVec<i64>>> = (0..schema_len).map(|_| None).collect();
            let mut all_f64: Vec<Option<AlignedVec<f64>>> = (0..schema_len).map(|_| None).collect();

            for &col_idx in &needed_indices {
                let col_name = &config.schema.columns[col_idx].name;
                let col_type = &config.schema.columns[col_idx].r#type;
                
                if matches!(col_type.as_str(), "i64" | "u64" | "i32" | "u32" | "i16" | "u16" | "i8" | "u8") {
                    if let Ok(data) = reader.read_column_i64(col_name) {
                        all_i64[col_idx] = Some(data);
                    }
                } else if col_type == "f64" || col_type == "f32" {
                    if let Ok(data) = reader.read_column_f64(col_name) {
                        all_f64[col_idx] = Some(data);
                    }
                }
            }
            overall_profile.io_read_ms += start_seg.elapsed().as_secs_f64() * 1000.0;

            let num_rows = reader.metadata.row_count as usize;
            total_rows_possible += num_rows;
            let group_size = config.row_group_size as usize;
            let num_groups = (num_rows + group_size - 1) / group_size;

            let effective_rate = if !pilot_done { 1.0 } else { current_row_rate };

            // Column-Based Sampling
            let mut any_group_sampled = false;
            for g_idx in 0..num_groups {
                if ColumnSampler::should_sample_row_group(seg_idx, g_idx, effective_rate, config.seed) {
                    any_group_sampled = true;
                    break;
                }
            }
            if !any_group_sampled { continue; }

            // Pre-parse filters for performance
            let filter_i64_val = plan.filter.as_ref().and_then(|f| f.value.parse::<i64>().ok());
            let filter_f64_val = plan.filter.as_ref().and_then(|f| f.value.parse::<f64>().ok());

            for g_idx in 0..num_groups {
                if ColumnSampler::should_sample_row_group(seg_idx, g_idx, effective_rate, config.seed) {
                    let start_idx = g_idx * group_size;
                    let end_idx = (start_idx + group_size).min(num_rows);
                    
                    // Specialized fast-path loop for simple filtering + count
                    let is_simple_count = plan.group_by.is_none() && filter_col_idx.is_some() && matches!(plan.aggregation, Aggregation::Count);
                    
                    if is_simple_count {
                        let f_idx = filter_col_idx.unwrap();
                        let f = plan.filter.as_ref().unwrap();
                        if let (Some(data), Some(target)) = (&all_i64[f_idx], filter_i64_val) {
                            if skip_dedup {
                                for i in start_idx..end_idx {
                                    let v = data[i];
                                    let passes = match f.op {
                                        FilterOp::Eq => v == target, FilterOp::Gt => v > target, FilterOp::Lt => v < target,
                                        FilterOp::Ge => v >= target, FilterOp::Le => v <= target,
                                    };
                                    if passes { total_rows_read += 1; scalar_count += 1; }
                                }
                            } else if let (Some(u_idx), Some(user_ids)) = (user_id_idx, &all_i64[user_id_idx.unwrap_or(0)]) {
                                for i in start_idx..end_idx {
                                    let v = data[i];
                                    let passes = match f.op {
                                        FilterOp::Eq => v == target, FilterOp::Gt => v > target, FilterOp::Lt => v < target,
                                        FilterOp::Ge => v >= target, FilterOp::Le => v <= target,
                                    };
                                    if passes {
                                        let uid = user_ids[i] as u64;
                                        if !seen.contains(&uid) {
                                            seen.insert(uid);
                                            total_rows_read += 1;
                                            scalar_count += 1;
                                        }
                                    }
                                }
                            }
                            continue;
                        }
                    }

                    // Generic row-by-row loop with lifted dedup
                    // Generic row-by-row loop with fully lifted dedup
                    if skip_dedup {
                        for i in start_idx..end_idx {
                            // Filtering
                            let mut passes_filter = true;
                            if let Some(f_idx) = filter_col_idx {
                                if let Some(f) = &plan.filter {
                                    if let Some(data) = &all_i64[f_idx] {
                                        if let Some(target) = filter_i64_val {
                                            let v = data[i];
                                            passes_filter = match f.op {
                                                FilterOp::Eq => v == target, FilterOp::Gt => v > target, FilterOp::Lt => v < target,
                                                FilterOp::Ge => v >= target, FilterOp::Le => v <= target,
                                            };
                                        } else { passes_filter = false; }
                                    } else if let Some(data) = &all_f64[f_idx] {
                                        if let Some(target) = filter_f64_val {
                                            let v = data[i];
                                            passes_filter = match f.op {
                                                FilterOp::Eq => (v - target).abs() < 1e-6, FilterOp::Gt => v > target, FilterOp::Lt => v < target,
                                                FilterOp::Ge => v >= target, FilterOp::Le => v <= target,
                                            };
                                        } else { passes_filter = false; }
                                    }
                                }
                            }
                            if !passes_filter { continue; }

                            total_rows_read += 1;

                            // Grouping and Aggregation
                            let group_key = if let Some(g_idx) = group_col_idx {
                                if let Some(data) = &all_i64[g_idx] {
                                    GroupKey::Int(data[i])
                                } else if let Some(data) = &all_f64[g_idx] {
                                    GroupKey::Float(data[i].to_bits())
                                } else {
                                    GroupKey::String("null".to_string())
                                }
                            } else {
                                GroupKey::String("scalar".to_string())
                            };

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
                                scalar_sum_sq += agg_val * agg_val;
                                scalar_count += 1;
                            }
                        }
                    } else {
                        // Version with Dedup
                        for i in start_idx..end_idx {
                            // Filtering
                            let mut passes_filter = true;
                            if let Some(f_idx) = filter_col_idx {
                                if let Some(f) = &plan.filter {
                                    if let Some(data) = &all_i64[f_idx] {
                                        if let Some(target) = filter_i64_val {
                                            let v = data[i];
                                            passes_filter = match f.op {
                                                FilterOp::Eq => v == target, FilterOp::Gt => v > target, FilterOp::Lt => v < target,
                                                FilterOp::Ge => v >= target, FilterOp::Le => v <= target,
                                            };
                                        } else { passes_filter = false; }
                                    } else if let Some(data) = &all_f64[f_idx] {
                                        if let Some(target) = filter_f64_val {
                                            let v = data[i];
                                            passes_filter = match f.op {
                                                FilterOp::Eq => (v - target).abs() < 1e-6, FilterOp::Gt => v > target, FilterOp::Lt => v < target,
                                                FilterOp::Ge => v >= target, FilterOp::Le => v <= target,
                                            };
                                        } else { passes_filter = false; }
                                    }
                                }
                            }
                            if !passes_filter { continue; }

                            if let Some(u_idx) = user_id_idx {
                                if let Some(user_ids) = &all_i64[u_idx] {
                                    let uid = user_ids[i] as u64;
                                    if seen.contains(&uid) { continue; }
                                    seen.insert(uid);
                                }
                            }

                            total_rows_read += 1;

                            // Grouping and Aggregation
                            let group_key = if let Some(g_idx) = group_col_idx {
                                if let Some(data) = &all_i64[g_idx] {
                                    GroupKey::Int(data[i])
                                } else if let Some(data) = &all_f64[g_idx] {
                                    GroupKey::Float(data[i].to_bits())
                                } else {
                                    GroupKey::String("null".to_string())
                                }
                            } else {
                                GroupKey::String("scalar".to_string())
                            };

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
                                scalar_sum_sq += agg_val * agg_val;
                                scalar_count += 1;
                            }
                        }
                    }
                }
            }

            // Pilot Phase Completion
            if !pilot_done && scalar_count > 100 {
                let mean = scalar_sum / scalar_count as f64;
                let variance = (scalar_sum_sq / scalar_count as f64) - (mean * mean);
                let required_n = AccuracyCalculator::calculate_required_sample_size(variance, config.accuracy_target, config.k);
                let remaining_segments = sampled_segments.len() - (seg_idx + 1);
                if remaining_segments > 0 && required_n > scalar_count as f64 {
                    let needed_per_segment = (required_n - scalar_count as f64) / remaining_segments as f64;
                    current_row_rate = (needed_per_segment / num_rows as f64).max(0.01).min(1.0);
                }
                pilot_done = true;
            }
        }

        // 6. Final Result
        let mut results = Vec::new();
        let mut total_scalar = 0.0;
        let mut final_scalar_count = 0u64;

        if plan.group_by.is_some() {
            for (key, (sum, count)) in group_map {
                let final_val = match &plan.aggregation {
                    Aggregation::Avg(_) => if count > 0 { sum / count as f64 } else { 0.0 },
                    _ => sum,
                };
                results.push((key.to_string(), final_val, ConfidenceFlag::High));
                final_scalar_count += count;
            }
        } else {
            total_scalar = match &plan.aggregation {
                Aggregation::Avg(_) => if scalar_count > 0 { scalar_sum / scalar_count as f64 } else { 0.0 },
                _ => scalar_sum,
            };
            final_scalar_count = scalar_count;
        }

        let value = if plan.group_by.is_some() {
            AggregateValue::Groups(results)
        } else if final_scalar_count > 0 || matches!(plan.aggregation, Aggregation::Count) {
            AggregateValue::Scalar(total_scalar)
        } else {
            AggregateValue::Empty
        };

        let scaled_value = crate::aqp::estimator::Estimator::scale_result(value, &plan.aggregation, sampling_rate);

        overall_profile.aggregation_ms = start_total.elapsed().as_secs_f64() * 1000.0 - overall_profile.io_read_ms;

        Ok(QueryResult {
            value: scaled_value,
            confidence: ConfidenceFlag::High,
            storage_path: StoragePath::Columnar,
            rows_scanned: total_rows_read as u64,
            sampling_rate: if total_rows_possible > 0 { total_rows_read as f64 / total_rows_possible as f64 } else { 1.0 },
            estimated_variance: if scalar_count > 0 { ((scalar_sum_sq / scalar_count as f64) - (scalar_sum / scalar_count as f64).powi(2)).max(0.0) } else { 0.0 },
            warnings: vec!["Columnar Path: Dynamic Sampling enabled".to_string()],
            profile: overall_profile,
        })
    }
}
