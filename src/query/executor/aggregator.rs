use crate::config::Config;
use crate::query::ast::Aggregation;
use crate::types::{AggregateValue, ConfidenceFlag, RowDisk, Value, get_value};
use crate::utils::aligned_vec::AlignedVec;
use std::collections::HashMap;

#[derive(Hash, PartialEq, Eq, Clone, Debug)]
pub enum GroupKey {
    Int(i64),
    Float(u64), // Store f64 bits for Eq/Hash
    String(String),
}

impl GroupKey {
    pub fn to_string(&self) -> String {
        match self {
            GroupKey::Int(i) => i.to_string(),
            GroupKey::Float(f_bits) => f64::from_bits(*f_bits).to_string(),
            GroupKey::String(s) => s.clone(),
        }
    }
}

pub struct Aggregator<'a> {
    pub aggregation: Aggregation,
    pub group_by: Option<String>,
    pub min_group_rows: u64,
    pub config: &'a Config,
}

impl<'a> Aggregator<'a> {
    pub fn new(
        aggregation: Aggregation,
        group_by: Option<String>,
        min_group_rows: u64,
        config: &'a Config,
    ) -> Self {
        Self {
            aggregation,
            group_by,
            min_group_rows,
            config,
        }
    }

    pub fn aggregate(&self, rows: &[RowDisk]) -> AggregateValue {
        if rows.is_empty() {
            return AggregateValue::Empty;
        }

        match &self.group_by {
            Some(group_col) => self.aggregate_groups(rows, group_col),
            None => self.aggregate_scalar(rows),
        }
    }

    fn aggregate_scalar(&self, rows: &[RowDisk]) -> AggregateValue {
        match &self.aggregation {
            Aggregation::Count => AggregateValue::Scalar(rows.len() as f64),
            Aggregation::Sum(col) => {
                let sum: f64 = rows
                    .iter()
                    .filter_map(|r| get_value(r, col, self.config))
                    .filter_map(|v| match v {
                        Value::Int(i) => Some(i as f64),
                        Value::Float(f) => Some(f),
                        _ => None,
                    })
                    .sum();
                AggregateValue::Scalar(sum)
            }
            Aggregation::Avg(col) => {
                let mut sum = 0.0;
                let mut count = 0;
                for row in rows {
                    if let Some(v) = get_value(row, col, self.config) {
                        match v {
                            Value::Int(i) => {
                                sum += i as f64;
                                count += 1;
                            }
                            Value::Float(f) => {
                                sum += f;
                                count += 1;
                            }
                            _ => {}
                        }
                    }
                }
                if count == 0 {
                    AggregateValue::Empty
                } else {
                    AggregateValue::Scalar(sum / count as f64)
                }
            }
            Aggregation::ApproxPercentile(col, quantile) => {
                let mut values: Vec<f64> = rows
                    .iter()
                    .filter_map(|r| get_value(r, col, self.config))
                    .filter_map(|v| match v {
                        Value::Int(i) => Some(i as f64),
                        Value::Float(f) => Some(f),
                        _ => None,
                    })
                    .collect();
                if values.is_empty() {
                    AggregateValue::Empty
                } else {
                    values.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
                    let idx = ((*quantile).clamp(0.0, 1.0) * (values.len() - 1) as f64).round()
                        as usize;
                    AggregateValue::Scalar(values[idx])
                }
            }
        }
    }

    fn aggregate_groups(&self, rows: &[RowDisk], group_col: &str) -> AggregateValue {
        let mut group_map: HashMap<GroupKey, (f64, u64)> = HashMap::with_capacity(128);

        for row in rows {
            if let Some(group_val) = get_value(row, group_col, self.config) {
                let key = match group_val {
                    Value::Int(i) => GroupKey::Int(i),
                    Value::Float(f) => GroupKey::Float(f.to_bits()),
                    Value::String(s) => GroupKey::String(s),
                };

                let entry = group_map.entry(key).or_insert((0.0, 0));

                match &self.aggregation {
                    Aggregation::Count => {
                        entry.0 += 1.0;
                        entry.1 += 1;
                    }
                    Aggregation::Sum(col) => {
                        if let Some(v) = get_value(row, col, self.config) {
                            match v {
                                Value::Int(i) => {
                                    entry.0 += i as f64;
                                    entry.1 += 1;
                                }
                                Value::Float(f) => {
                                    entry.0 += f;
                                    entry.1 += 1;
                                }
                                _ => {}
                            }
                        }
                    }
                    Aggregation::Avg(col) => {
                        if let Some(v) = get_value(row, col, self.config) {
                            match v {
                                Value::Int(i) => {
                                    entry.0 += i as f64;
                                    entry.1 += 1;
                                }
                                Value::Float(f) => {
                                    entry.0 += f;
                                    entry.1 += 1;
                                }
                                _ => {}
                            }
                        }
                    }
                    Aggregation::ApproxPercentile(_, _) => {
                        entry.0 += 1.0;
                        entry.1 += 1;
                    }
                }
            }
        }

        let mut results = Vec::new();
        for (key, (val, count)) in group_map {
            let final_val = match &self.aggregation {
                Aggregation::Avg(_) => {
                    if count > 0 {
                        val / count as f64
                    } else {
                        0.0
                    }
                }
                Aggregation::Count | Aggregation::Sum(_) | Aggregation::ApproxPercentile(_, _) => {
                    val
                }
            };
            let confidence = if count < self.config.low_confidence_threshold {
                ConfidenceFlag::Low
            } else {
                ConfidenceFlag::High
            };
            results.push((key.to_string(), final_val, confidence));
        }

        AggregateValue::Groups(results)
    }

    pub fn aggregate_columnar(
        &self,
        i64_cols: &HashMap<usize, AlignedVec<i64>>,
        f64_cols: &HashMap<usize, AlignedVec<f64>>,
        agg_col_idx: Option<usize>,
        group_col_idx: Option<usize>,
    ) -> (AggregateValue, f64) {
        match group_col_idx {
            Some(idx) => (
                self.aggregate_groups_columnar(i64_cols, f64_cols, idx, agg_col_idx),
                0.0,
            ),
            None => self.aggregate_scalar_columnar(i64_cols, f64_cols, agg_col_idx),
        }
    }

    fn aggregate_scalar_columnar(
        &self,
        i64_cols: &HashMap<usize, AlignedVec<i64>>,
        f64_cols: &HashMap<usize, AlignedVec<f64>>,
        agg_col_idx: Option<usize>,
    ) -> (AggregateValue, f64) {
        let mut sum = 0.0;
        let mut sum_sq = 0.0;
        let mut count = 0;

        match &self.aggregation {
            Aggregation::Count => {
                let cnt = i64_cols
                    .values()
                    .next()
                    .map(|v| v.len())
                    .or_else(|| f64_cols.values().next().map(|v| v.len()))
                    .unwrap_or(0);
                return (AggregateValue::Scalar(cnt as f64), 0.0);
            }
            Aggregation::Sum(_) | Aggregation::Avg(_) => {
                if let Some(idx) = agg_col_idx {
                    if let Some(d) = i64_cols.get(&idx) {
                        for &v in d.as_slice() {
                            let f = v as f64;
                            sum += f;
                            sum_sq += f * f;
                            count += 1;
                        }
                    } else if let Some(d) = f64_cols.get(&idx) {
                        for &v in d.as_slice() {
                            sum += v;
                            sum_sq += v * v;
                            count += 1;
                        }
                    }
                }
            }
            Aggregation::ApproxPercentile(_, quantile) => {
                if let Some(idx) = agg_col_idx {
                    let mut values = Vec::new();
                    if let Some(d) = i64_cols.get(&idx) {
                        values.extend(d.as_slice().iter().map(|&v| v as f64));
                    } else if let Some(d) = f64_cols.get(&idx) {
                        values.extend(d.as_slice().iter().copied());
                    }
                    if values.is_empty() {
                        return (AggregateValue::Empty, 0.0);
                    }
                    values.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
                    let idx =
                        ((*quantile).clamp(0.0, 1.0) * (values.len() - 1) as f64).round() as usize;
                    return (AggregateValue::Scalar(values[idx]), 0.0);
                }
            }
        }

        if count == 0 {
            (AggregateValue::Empty, 0.0)
        } else {
            let mean = sum / count as f64;
            let variance = (sum_sq / count as f64) - (mean * mean);
            let val = if matches!(self.aggregation, Aggregation::Sum(_)) {
                AggregateValue::Scalar(sum)
            } else {
                AggregateValue::Scalar(mean)
            };
            (val, variance.max(0.0))
        }
    }

    fn aggregate_groups_columnar(
        &self,
        i64_cols: &HashMap<usize, AlignedVec<i64>>,
        f64_cols: &HashMap<usize, AlignedVec<f64>>,
        group_col_idx: usize,
        agg_col_idx: Option<usize>,
    ) -> AggregateValue {
        let mut group_map: HashMap<GroupKey, (f64, u64)> = HashMap::with_capacity(1024);

        let row_count = i64_cols
            .values()
            .next()
            .map(|v| v.len())
            .or_else(|| f64_cols.values().next().map(|v| v.len()))
            .unwrap_or(0);

        for i in 0..row_count {
            let key = if let Some(vals) = i64_cols.get(&group_col_idx) {
                GroupKey::Int(vals[i])
            } else if let Some(vals) = f64_cols.get(&group_col_idx) {
                GroupKey::Float(vals[i].to_bits())
            } else {
                continue;
            };

            let entry = group_map.entry(key).or_insert((0.0, 0));

            match &self.aggregation {
                Aggregation::Count => {
                    entry.0 += 1.0;
                    entry.1 += 1;
                }
                Aggregation::Sum(_) | Aggregation::Avg(_) => {
                    if let Some(idx) = agg_col_idx {
                        if let Some(vals) = i64_cols.get(&idx) {
                            entry.0 += vals[i] as f64;
                            entry.1 += 1;
                        } else if let Some(vals) = f64_cols.get(&idx) {
                            entry.0 += vals[i];
                            entry.1 += 1;
                        }
                    }
                }
                Aggregation::ApproxPercentile(_, _) => {
                    entry.0 += 1.0;
                    entry.1 += 1;
                }
            }
        }

        let mut results = Vec::new();
        for (key, (val, count)) in group_map {
            let final_val = match &self.aggregation {
                Aggregation::Avg(_) => {
                    if count > 0 {
                        val / count as f64
                    } else {
                        0.0
                    }
                }
                Aggregation::Count | Aggregation::Sum(_) | Aggregation::ApproxPercentile(_, _) => {
                    val
                }
            };
            let confidence = if count < self.config.low_confidence_threshold {
                ConfidenceFlag::Low
            } else {
                ConfidenceFlag::High
            };
            results.push((key.to_string(), final_val, confidence));
        }

        AggregateValue::Groups(results)
    }
}
