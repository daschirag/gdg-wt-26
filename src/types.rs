use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct RowDisk {
    pub user_id: u64,
    pub status: u8,    // 0=active 1=inactive 2=banned
    pub country: u8,   // 0=IN 1=US 2=DE 3=JP 4=BR
    pub timestamp: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Value {
    Int(i64),
    Float(f64),
    String(String),
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct Confidence(pub f64);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum AggregateValue {
    Scalar(f64),
    Groups(Vec<(String, f64, Confidence)>),
    Empty,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryResult {
    pub value: AggregateValue,
    pub confidence: Confidence,
    pub warnings: Vec<String>,
}

pub fn get_value(row: &RowDisk, col: &str) -> Option<Value> {
    match col {
        "user_id" => Some(Value::Int(row.user_id as i64)),
        "status" => Some(Value::Int(row.status as i64)),
        "country" => Some(Value::Int(row.country as i64)),
        "timestamp" => Some(Value::Int(row.timestamp as i64)),
        _ => None,
    }
}
