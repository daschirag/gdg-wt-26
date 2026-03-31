use crate::aqp::accuracy::AccuracyCalculator;
use crate::aqp::estimator::Estimator;
use crate::aqp::tdigest::TDigest;
use crate::errors::QueryError;
use crate::query::ast::{Aggregation, PredicateExpr, QueryPlan};
use crate::query::executor::aggregator::Aggregator;
use crate::query::executor::col_pipeline::ColumnarPipeline;
use crate::storage::columnar::reader::ColumnarReader;
use crate::types::{
    AggregateValue, AqpDiagnostics, ConfidenceFlag, QueryProfile, QueryResult, RowDisk,
    StoragePath, Value, get_value,
};

pub struct AqpPipeline {
    pub segments: Vec<String>,
}

impl AqpPipeline {
    pub fn new(segments: Vec<String>) -> Self {
        Self { segments }
    }

    pub fn supports_plan(plan: &QueryPlan) -> bool {
        match &plan.aggregation {
            Aggregation::Count | Aggregation::Sum(_) | Aggregation::Avg(_) => true,
            Aggregation::ApproxPercentile(_, _) => plan.group_by.is_none(),
        }
    }

    pub fn execute(
        &self,
        plan: &QueryPlan,
        config: &crate::config::Config,
        mask: Option<&std::collections::HashSet<u64>>,
        skip_dedup: bool,
    ) -> Result<QueryResult, QueryError> {
        if !Self::supports_plan(plan) {
            return self.exact_fallback(plan, config, mask, skip_dedup);
        }

        let mode = AccuracyCalculator::parse_mode(&config.aqp_mode);
        let settings = AccuracyCalculator::mode_settings(mode);

        if let Aggregation::ApproxPercentile(column, quantile) = &plan.aggregation {
            if plan.filter.is_none() {
                let mut merged = TDigest::new(settings.tdigest_compression);
                let mut total_rows = 0u64;
                for path in &self.segments {
                    let reader = ColumnarReader::new(path.into(), 0)?;
                    if let Ok(digest) = reader.read_tdigest(column) {
                        merged.merge(&digest);
                        total_rows += reader.metadata.row_count;
                    }
                }

                if let Some(value) = merged.quantile(*quantile) {
                    return Ok(QueryResult {
                        value: AggregateValue::Scalar(value),
                        confidence: ConfidenceFlag::High,
                        warnings: Vec::new(),
                        storage_path: StoragePath::Columnar,
                        rows_scanned: total_rows,
                        sampling_rate: 1.0,
                        estimated_variance: 0.0,
                        profile: QueryProfile::default(),
                        aqp: Some(AqpDiagnostics {
                            mode: config.aqp_mode.clone(),
                            source: "tdigest".to_string(),
                            sample_rate: 1.0,
                            sample_rows: 0,
                            estimated_error: 1.0 / settings.tdigest_compression as f64,
                            ci_lower: None,
                            ci_upper: None,
                        }),
                        next_offset: None,
                    });
                }
            }
        }

        let mut sampled_rows = Vec::new();
        let mut matched_sample_rows = 0usize;
        let mut total_sample_rows = 0usize;
        let mut estimated_population_rows = 0u64;
        let mut total_weighted_rate = 0.0f64;
        let mut segment_count = 0usize;
        // Per-group sample counts for per-group confidence (only populated when group_by is set).
        let mut group_sample_counts: std::collections::HashMap<String, usize> =
            std::collections::HashMap::new();

        for path in &self.segments {
            let reader = ColumnarReader::new(path.into(), 0)?;
            let sample = match reader.read_aqp_sample() {
                Ok(sample) => sample,
                Err(_) => return self.exact_fallback(plan, config, mask, skip_dedup),
            };
            total_weighted_rate += sample.sample_rate.max(0.0001);
            segment_count += 1;
            estimated_population_rows += sample.row_count;
            total_sample_rows += sample.rows.len();

            for row in sample.rows {
                if predicate_matches(plan.filter.as_ref(), &row, config) {
                    matched_sample_rows += 1;
                    if let Some(group_col) = &plan.group_by {
                        let key = match get_value(&row, group_col, config) {
                            Some(Value::Int(v)) => v.to_string(),
                            Some(Value::Float(v)) => format!("{:.4}", v),
                            Some(Value::String(s)) => s,
                            None => "__null__".to_string(),
                        };
                        *group_sample_counts.entry(key).or_insert(0) += 1;
                    }
                    sampled_rows.push(row);
                }
            }
        }

        let sample_rate = if segment_count > 0 {
            total_weighted_rate / segment_count as f64
        } else {
            settings.sample_rate
        };

        if matched_sample_rows < settings.min_filtered_rows {
            return self.exact_fallback(plan, config, mask, skip_dedup);
        }

        let estimated_match_rows = ((matched_sample_rows as f64) / sample_rate).round() as u64;
        let mut warnings = Vec::new();
        if matched_sample_rows < settings.min_filtered_rows * 2 {
            warnings.push(format!(
                "AQP sample support is modest ({} matched rows); estimates may be noisy",
                matched_sample_rows
            ));
        }

        let (value, estimated_variance, estimated_error, ci_lower, ci_upper) =
            match &plan.aggregation {
                Aggregation::ApproxPercentile(column, quantile) => {
                    let mut values = Vec::new();
                    for row in &sampled_rows {
                        if let Some(value) = get_value(row, column, config) {
                            match value {
                                Value::Int(v) => values.push(v as f64),
                                Value::Float(v) => values.push(v),
                                Value::String(_) => {}
                            }
                        }
                    }
                    if values.is_empty() {
                        return self.exact_fallback(plan, config, mask, skip_dedup);
                    }
                    let digest = TDigest::from_values(&values, settings.tdigest_compression);
                    let percentile = digest.quantile(*quantile).unwrap_or(0.0);
                    let stderr = 1.0 / (values.len() as f64).sqrt();
                    (
                        AggregateValue::Scalar(percentile),
                        stderr * stderr,
                        stderr,
                        None,
                        None,
                    )
                }
                _ => {
                    let aggregator = Aggregator::new(
                        plan.aggregation.clone(),
                        plan.group_by.clone(),
                        config.min_group_rows,
                        config,
                    );
                    let sampled_value = aggregator.aggregate(&sampled_rows);
                    let mut estimated =
                        Estimator::scale_result(sampled_value, &plan.aggregation, sample_rate);
                    // For grouped results, replace Aggregator's row-count-based confidence
                    // with per-group error estimates derived from sample size.
                    if let AggregateValue::Groups(ref mut groups) = estimated {
                        for (key, _val, conf) in groups.iter_mut() {
                            let group_n =
                                *group_sample_counts.get(key.as_str()).unwrap_or(&0) as f64;
                            let per_group_error = if group_n > 0.0 {
                                (1.0 / group_n.sqrt()).min(1.0)
                            } else {
                                1.0
                            };
                            *conf = if per_group_error <= settings.max_relative_error {
                                ConfidenceFlag::High
                            } else {
                                ConfidenceFlag::Low
                            };
                        }
                    }
                    let (variance, error, lower, upper) = estimate_uncertainty(
                        &sampled_rows,
                        plan,
                        config,
                        sample_rate,
                        total_sample_rows,
                        estimated_population_rows,
                    );
                    (estimated, variance, error, lower, upper)
                }
            };

        let confidence = if estimated_error <= settings.max_relative_error {
            ConfidenceFlag::High
        } else {
            warnings.push(format!(
                "Estimated relative error {:.3} exceeds {:?} mode target {:.3}",
                estimated_error, mode, settings.max_relative_error
            ));
            ConfidenceFlag::Low
        };

        Ok(QueryResult {
            value,
            confidence,
            warnings,
            storage_path: StoragePath::Columnar,
            rows_scanned: estimated_match_rows,
            sampling_rate: sample_rate,
            estimated_variance,
            profile: QueryProfile::default(),
            aqp: Some(AqpDiagnostics {
                mode: config.aqp_mode.clone(),
                source: if matches!(plan.aggregation, Aggregation::ApproxPercentile(_, _)) {
                    "sample_fallback".to_string()
                } else {
                    "segment_sample".to_string()
                },
                sample_rate,
                sample_rows: matched_sample_rows as u64,
                estimated_error,
                ci_lower,
                ci_upper,
            }),
            next_offset: plan.limit.map(|lim| plan.offset.unwrap_or(0) + lim),
        })
    }

    fn exact_fallback(
        &self,
        plan: &QueryPlan,
        config: &crate::config::Config,
        mask: Option<&std::collections::HashSet<u64>>,
        skip_dedup: bool,
    ) -> Result<QueryResult, QueryError> {
        if let Aggregation::ApproxPercentile(column, quantile) = &plan.aggregation {
            let mut values = Vec::new();
            for path in &self.segments {
                let reader = ColumnarReader::new(path.into(), 0)?;
                let rows = reader.read_all_rows(config)?;
                for row in rows {
                    if predicate_matches(plan.filter.as_ref(), &row, config) {
                        if let Some(value) = get_value(&row, column, config) {
                            match value {
                                Value::Int(v) => values.push(v as f64),
                                Value::Float(v) => values.push(v),
                                Value::String(_) => {}
                            }
                        }
                    }
                }
            }
            if values.is_empty() {
                return Ok(QueryResult {
                    value: AggregateValue::Empty,
                    confidence: ConfidenceFlag::Exact,
                    warnings: vec!["AQP fallback: exact percentile scan".to_string()],
                    storage_path: StoragePath::Columnar,
                    rows_scanned: 0,
                    sampling_rate: 1.0,
                    estimated_variance: 0.0,
                    profile: QueryProfile::default(),
                    aqp: None,
                    next_offset: None,
                });
            }
            values.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
            let idx = ((*quantile).clamp(0.0, 1.0) * (values.len() - 1) as f64).round() as usize;
            return Ok(QueryResult {
                value: AggregateValue::Scalar(values[idx]),
                confidence: ConfidenceFlag::Exact,
                warnings: vec!["AQP fallback: exact percentile scan".to_string()],
                storage_path: StoragePath::Columnar,
                rows_scanned: values.len() as u64,
                sampling_rate: 1.0,
                estimated_variance: 0.0,
                profile: QueryProfile::default(),
                aqp: None,
                next_offset: None,
            });
        }
        let pipeline = ColumnarPipeline::new(self.segments.clone())?;
        let columns_needed = plan.required_columns();
        let mut result = pipeline.execute(plan, &columns_needed, config, mask, skip_dedup)?;
        result.warnings.push("AQP fallback: exact columnar execution".to_string());
        Ok(result)
    }
}

fn predicate_matches(
    filter: Option<&PredicateExpr>,
    row: &RowDisk,
    config: &crate::config::Config,
) -> bool {
    filter.map_or(true, |expr| expr.eval_value(&|column| get_value(row, column, config)))
}

fn estimate_uncertainty(
    sampled_rows: &[RowDisk],
    plan: &QueryPlan,
    config: &crate::config::Config,
    sample_rate: f64,
    _total_sample_rows: usize,
    _estimated_population_rows: u64,
) -> (f64, f64, Option<f64>, Option<f64>) {
    match &plan.aggregation {
        Aggregation::Count => {
            let matched = sampled_rows.len() as f64;
            let variance = ((1.0 - sample_rate) / (sample_rate * sample_rate)) * matched.max(1.0);
            let estimate = matched / sample_rate;
            let stderr = variance.sqrt();
            let rel_error = if estimate > 0.0 { (1.96 * stderr) / estimate } else { 0.0 };
            (
                variance,
                rel_error,
                Some((estimate - 1.96 * stderr).max(0.0)),
                Some(estimate + 1.96 * stderr),
            )
        }
        Aggregation::Sum(column) => {
            let values: Vec<f64> = sampled_rows
                .iter()
                .filter_map(|row| match get_value(row, column, config) {
                    Some(Value::Int(v)) => Some(v as f64),
                    Some(Value::Float(v)) => Some(v),
                    _ => None,
                })
                .collect();
            if values.is_empty() {
                return (0.0, 0.0, None, None);
            }
            let sum_sq: f64 = values.iter().map(|v| v * v).sum();
            let estimate: f64 = values.iter().sum::<f64>() / sample_rate;
            let variance = ((1.0 - sample_rate) / (sample_rate * sample_rate)) * sum_sq.max(1.0);
            let stderr = variance.sqrt();
            let rel_error = if estimate.abs() > f64::EPSILON {
                (1.96 * stderr) / estimate.abs()
            } else {
                0.0
            };
            (
                variance,
                rel_error,
                Some(estimate - 1.96 * stderr),
                Some(estimate + 1.96 * stderr),
            )
        }
        Aggregation::Avg(column) => {
            let values: Vec<f64> = sampled_rows
                .iter()
                .filter_map(|row| match get_value(row, column, config) {
                    Some(Value::Int(v)) => Some(v as f64),
                    Some(Value::Float(v)) => Some(v),
                    _ => None,
                })
                .collect();
            if values.len() < 2 {
                return (0.0, 0.0, None, None);
            }
            let mean = values.iter().sum::<f64>() / values.len() as f64;
            let variance = values
                .iter()
                .map(|value| {
                    let delta = value - mean;
                    delta * delta
                })
                .sum::<f64>()
                / (values.len() - 1) as f64;
            let stderr = (variance / values.len() as f64).sqrt();
            let rel_error = if mean.abs() > f64::EPSILON {
                (1.96 * stderr) / mean.abs()
            } else {
                0.0
            };
            (
                variance,
                rel_error,
                Some(mean - 1.96 * stderr),
                Some(mean + 1.96 * stderr),
            )
        }
        Aggregation::ApproxPercentile(_, _) => {
            let sample_rows = sampled_rows.len().max(1) as f64;
            let rel_error = (1.0 / sample_rows.sqrt()).min(1.0);
            (rel_error * rel_error, rel_error, None, None)
        }
    }
}

pub fn group_by_is_low_cardinality(
    segments: &[String],
    plan: &QueryPlan,
) -> Result<bool, QueryError> {
    let Some(group_by) = &plan.group_by else {
        return Ok(true);
    };
    for path in segments {
        let reader = ColumnarReader::new(path.into(), 0)?;
        let Some(meta) = reader.metadata.columns.get(group_by) else {
            return Ok(false);
        };
        if meta.distinct_count > 256 {
            return Ok(false);
        }
    }
    Ok(true)
}

