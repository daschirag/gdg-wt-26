use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct RowDisk {
    pub version: u32,
    pub crc: u32,
    pub seq: u64,
    pub values: Vec<Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ColumnMetadata {
    pub encoding: String,
    pub sum: f64,
    pub min: f64,
    pub max: f64,
    pub distinct_count: u64,
    #[serde(default)]
    pub run_count: u64,
    pub crc32: u32,
    #[serde(default)]
    pub indexes: Vec<ColumnIndexMetadata>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ColumnIndexMetadata {
    pub kind: String,
    pub file: String,
    #[serde(default)]
    pub values: Vec<i64>,
    pub row_count: u64,
    #[serde(default)]
    pub supports: Vec<String>,
}

impl ColumnMetadata {
    pub fn index(&self, kind: &str) -> Option<&ColumnIndexMetadata> {
        self.indexes.iter().find(|idx| idx.kind == kind)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct GroupStatsData {
    pub values: Vec<i64>,
    pub counts: Vec<u64>,
    #[serde(default)]
    pub measure_sums: BTreeMap<String, Vec<f64>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct RangeBlock {
    pub row_start: u64,
    pub row_count: u32,
    pub start_value: i64,
    pub end_value: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct RangeBlocksData {
    pub blocks: Vec<RangeBlock>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SSTableMetadata {
    pub magic: String,
    pub row_count: u64,
    pub min_ts: i64,
    pub max_ts: i64,
    pub schema_version: u32,
    pub columns: BTreeMap<String, ColumnMetadata>,
    pub checksum: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, PartialOrd)]
pub enum Value {
    Int(i64),
    Float(f64),
    String(String),
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq)]
pub enum ConfidenceFlag {
    High,
    Low,
    Exact,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq)]
pub enum StoragePath {
    Row,
    Columnar,
    Mixed,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum AggregateValue {
    Scalar(f64),
    Groups(Vec<(String, f64, ConfidenceFlag)>),
    Empty,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct QueryProfile {
    pub bloom_filter_ms: f64,
    pub sst_sampling_ms: f64,
    pub io_read_ms: f64,
    pub deserialization_ms: f64,
    pub crc_verify_ms: f64,
    pub filtering_ms: f64,
    pub aggregation_ms: f64,
    pub rayon_parallel: bool,
    pub rayon_threads_used: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryResult {
    pub value: AggregateValue,
    pub confidence: ConfidenceFlag,
    pub warnings: Vec<String>,
    pub storage_path: StoragePath,
    pub rows_scanned: u64,
    pub sampling_rate: f64,
    pub estimated_variance: f64,
    pub profile: QueryProfile,
}

pub fn get_value(row: &RowDisk, col: &str, config: &crate::config::Config) -> Option<Value> {
    let idx = config.schema.columns.iter().position(|c| c.name == col)?;
    row.values.get(idx).cloned()
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct InsertionProfile {
    pub transposition_ms: f64,
    pub aggregation_ms: f64,
    pub io_write_ms: f64,
    pub metadata_serialize_ms: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct GenerationProfile {
    pub random_gen_ms: f64,
    pub allocation_ms: f64,
}
