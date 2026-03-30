use std::collections::HashMap;
use crate::types::{RowDisk, AggregateValue, Confidence, get_value, Value};
use crate::query::ast::Aggregation;

pub struct Aggregator {
    pub aggregation: Aggregation,
    pub group_by: Option<String>,
    pub min_group_rows: u64,
}

impl Aggregator {
    pub fn new(aggregation: Aggregation, group_by: Option<String>, min_group_rows: u64) -> Self {
        Self { aggregation, group_by, min_group_rows }
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
                let sum: f64 = rows.iter()
                    .filter_map(|r| get_value(r, col))
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
                    if let Some(v) = get_value(row, col) {
                        match v {
                            Value::Int(i) => { sum += i as f64; count += 1; }
                            Value::Float(f) => { sum += f; count += 1; }
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
        }
    }

    fn aggregate_groups(&self, rows: &[RowDisk], group_col: &str) -> AggregateValue {
        let mut group_map: HashMap<String, (f64, u64)> = HashMap::new();

        for row in rows {
            if let Some(group_val) = get_value(row, group_col) {
                let key = match group_val {
                    Value::Int(i) => i.to_string(),
                    Value::Float(f) => f.to_string(),
                    Value::String(s) => s,
                };

                let entry = group_map.entry(key).or_insert((0.0, 0));
                
                match &self.aggregation {
                    Aggregation::Count => {
                        entry.0 += 1.0;
                        entry.1 += 1;
                    }
                    Aggregation::Sum(col) => {
                        if let Some(v) = get_value(row, col) {
                            match v {
                                Value::Int(i) => { entry.0 += i as f64; entry.1 += 1; }
                                Value::Float(f) => { entry.0 += f; entry.1 += 1; }
                                _ => {}
                            }
                        }
                    }
                    Aggregation::Avg(col) => {
                        if let Some(v) = get_value(row, col) {
                            match v {
                                Value::Int(i) => { entry.0 += i as f64; entry.1 += 1; }
                                Value::Float(f) => { entry.0 += f; entry.1 += 1; }
                                _ => {}
                            }
                        }
                    }
                }
            }
        }

        let mut results = Vec::new();
        for (key, (val, count)) in group_map {
            let final_val = match &self.aggregation {
                Aggregation::Avg(_) => if count > 0 { val / count as f64 } else { 0.0 },
                _ => val,
            };
            let score = if count == 0 {
                0.0
            } else {
                // Statistical Margin of Error (95% CI): 1.96 / sqrt(n)
                // We map this to a [0, 1] confidence score
                (1.0 - (1.96 / (count as f64).sqrt())).max(0.0)
            };
            results.push((key, final_val, Confidence(score)));
        }

        AggregateValue::Groups(results)
    }
}
