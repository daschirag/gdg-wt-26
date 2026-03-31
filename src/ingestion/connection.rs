use crate::aqp::sampler::reservoir::ReservoirSampler;
use crate::config::Config;
use crate::errors::{IngestionError, StorageError};
use crate::storage::flush::FlushTrigger;
use crate::storage::manager::StorageManager;
use crate::types::{RowDisk, Value};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

pub struct Connection {
    pub(crate) storage: Arc<StorageManager>,
    pub(crate) config: Arc<Config>,
    flush_trigger: FlushTrigger,
    pub(crate) sampler: Arc<Mutex<ReservoirSampler>>,
    sst_counter: AtomicU64,
}

impl Connection {
    /// Open a storage directory. Looks for `config.toml` inside `data_dir`.
    pub fn open(data_dir: &str) -> Result<Self, IngestionError> {
        let config_path = format!("{}/config.toml", data_dir.trim_end_matches('/'));
        let config = Config::load_from_file(&config_path)
            .map_err(|e| IngestionError::Config(e.to_string()))?;
        let config = Arc::new(config);
        let storage = Arc::new(StorageManager::new(config.clone(), data_dir.to_string()));

        // Start above any existing ingestion_*.aqe to avoid overwriting on restart
        let counter_start = scan_ingestion_counter(data_dir);

        Ok(Self {
            flush_trigger: FlushTrigger {
                memtable_row_limit: config.memtable_row_limit,
                memtable_size_limit: config.memtable_size_limit,
            },
            sampler: Arc::new(Mutex::new(ReservoirSampler::new(1000))),
            sst_counter: AtomicU64::new(counter_start),
            storage,
            config,
        })
    }

    /// Build a Connection from an already-initialised StorageManager (e.g. from REPL).
    pub fn from_storage(config: Arc<Config>, storage: Arc<StorageManager>) -> Self {
        let counter_start = scan_ingestion_counter(&storage.sst_dir);
        Self {
            flush_trigger: FlushTrigger {
                memtable_row_limit: config.memtable_row_limit,
                memtable_size_limit: config.memtable_size_limit,
            },
            sampler: Arc::new(Mutex::new(ReservoirSampler::new(1000))),
            sst_counter: AtomicU64::new(counter_start),
            storage,
            config,
        }
    }

    /// Insert a row. Values must match schema column order.
    pub fn insert(&self, values: Vec<Value>) -> Result<(), IngestionError> {
        let expected = self.config.schema.columns.len();
        if expected > 0 && values.len() != expected {
            return Err(IngestionError::SchemaMismatch {
                expected,
                got: values.len(),
            });
        }
        let row = RowDisk { version: 1, crc: 0, seq: 0, values };
        self.storage.put(row.clone())?;
        self.sampler.lock().unwrap().insert(row);
        self.maybe_flush()?;
        Ok(())
    }

    /// Execute a SQL query against all storage layers, returning the full result.
    pub fn query(&self, sql: &str) -> Result<crate::types::QueryResult, IngestionError> {
        let result = execute_query(sql, &self.config, &self.storage)?;
        Ok(result)
    }

    /// Return up to `n` rows from the current reservoir sample.
    pub fn reservoir_sample(&self, n: usize) -> Vec<RowDisk> {
        let sampler = self.sampler.lock().unwrap();
        let s = sampler.sample();
        s[..n.min(s.len())].to_vec()
    }

    pub fn close(self) {}

    /// Flush memtable to SSTable if threshold is exceeded.
    /// Double-checked: read lock to test, write lock to drain (no lock held during write).
    pub(crate) fn maybe_flush(&self) -> Result<(), StorageError> {
        let should = {
            let mem = self.storage.memtable.read().map_err(|_| {
                StorageError::WriteError(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    "Lock poisoned",
                ))
            })?;
            self.flush_trigger.should_flush(&mem)
        };

        if !should {
            return Ok(());
        }

        let rows: Vec<RowDisk> = {
            let mut mem = self.storage.memtable.write().map_err(|_| {
                StorageError::WriteError(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    "Lock poisoned",
                ))
            })?;
            // Re-check after acquiring write lock (another thread may have already flushed)
            if !self.flush_trigger.should_flush(&mem) {
                return Ok(());
            }
            let rows: Vec<RowDisk> = mem.data.values().cloned().collect();
            mem.clear();
            rows
        };

        if rows.is_empty() {
            return Ok(());
        }

        let n = self.sst_counter.fetch_add(1, Ordering::SeqCst);
        let path = format!("{}/ingestion_{:06}.aqe", self.storage.sst_dir, n);
        crate::storage::sstable::writer::SSTableWriter::write_sstable_to_file(
            &path,
            &rows,
            &self.config,
        )?;
        self.storage.register_sstable(path);
        Ok(())
    }
}

/// Scan `dir` for `ingestion_NNNNNN.aqe` files and return max index + 1.
fn scan_ingestion_counter(dir: &str) -> u64 {
    let mut max = 0u64;
    if let Ok(entries) = std::fs::read_dir(dir) {
        for entry in entries.flatten() {
            let name = entry.file_name();
            let name = name.to_string_lossy();
            if let Some(rest) = name.strip_prefix("ingestion_") {
                if let Some(idx_str) = rest.strip_suffix(".aqe") {
                    if let Ok(idx) = idx_str.parse::<u64>() {
                        if idx >= max {
                            max = idx + 1;
                        }
                    }
                }
            }
        }
    }
    max
}

// ---------------------------------------------------------------------------
// Query execution (adapted from repl.rs::handle_query, returns QueryResult)
// ---------------------------------------------------------------------------

fn execute_query(
    sql: &str,
    config: &Config,
    storage: &StorageManager,
) -> Result<crate::types::QueryResult, IngestionError> {
    use crate::query::ast::Aggregation;
    use crate::query::executor::col_pipeline::ColumnarPipeline;
    use crate::query::executor::pipeline::Pipeline;
    use crate::query::parser::Parser;
    use crate::query::planner::{ExecutionPath, QueryPlanner};
    use crate::storage::columnar::reader::ColumnarReader;
    use crate::types::{AggregateValue, ConfidenceFlag, QueryResult, StoragePath};

    let plan = Parser::parse(sql).map_err(|e| IngestionError::Query(e))?;
    let execution_path = QueryPlanner::plan(&plan, storage);

    let aqp_enabled = matches!(plan.aggregation, Aggregation::ApproxPercentile(_, _));

    // 1. Scan MemTable (exact)
    let mem_rows = storage.scan_memtable(&plan);
    let mem_count = mem_rows.len() as u64;
    let mut mem_sum = 0.0f64;
    for row in &mem_rows {
        match &plan.aggregation {
            Aggregation::Sum(col) | Aggregation::Avg(col) => {
                if let Some(Value::Int(i)) = crate::types::get_value(row, col, config) {
                    mem_sum += i as f64;
                }
            }
            _ => {}
        }
    }

    // 2. Scan disk layers
    let mem_keys = storage.get_memtable_keys();
    let mask = Some(&mem_keys);
    let skip_dedup = storage.all_sources_compacted();

    let disc_result = match execution_path {
        ExecutionPath::FastPath { segments, column, aggregation } => {
            let mut total_count = 0u64;
            let mut total_sum = 0.0f64;
            for path in segments {
                if let Ok(reader) = ColumnarReader::new(path.into(), 0) {
                    total_count += reader.metadata.row_count;
                    if let Some(col_name) = &column {
                        if let Some(col_meta) = reader.metadata.columns.get(col_name) {
                            total_sum += col_meta.sum;
                        }
                    }
                }
            }
            let value = match aggregation {
                Aggregation::Count => AggregateValue::Scalar(total_count as f64),
                Aggregation::Sum(_) => AggregateValue::Scalar(total_sum),
                Aggregation::Avg(_) | Aggregation::ApproxPercentile(_, _) => {
                    if total_count > 0 {
                        AggregateValue::Scalar(total_sum / total_count as f64)
                    } else {
                        AggregateValue::Empty
                    }
                }
            };
            Ok(QueryResult {
                value,
                confidence: ConfidenceFlag::Exact,
                warnings: vec!["Fast-path: Using pre-computed metadata".to_string()],
                storage_path: StoragePath::Columnar,
                rows_scanned: 0,
                sampling_rate: 1.0,
                estimated_variance: 0.0,
                profile: crate::types::QueryProfile::default(),
                aqp: None,
                next_offset: None,
            })
        }
        ExecutionPath::Row { sstables } => {
            Pipeline::new(sstables).execute(&plan, config, mask, skip_dedup)
        }
        ExecutionPath::Columnar { segments, columns_needed } => {
            if aqp_enabled
                && crate::query::executor::aqp_pipeline::AqpPipeline::supports_plan(&plan)
                && crate::query::executor::aqp_pipeline::group_by_is_low_cardinality(
                    &segments, &plan,
                )
                .map_err(IngestionError::Query)?
            {
                crate::query::executor::aqp_pipeline::AqpPipeline::new(segments)
                    .execute(&plan, config, mask, skip_dedup)
            } else {
                ColumnarPipeline::new(segments)
                    .map_err(IngestionError::Storage)?
                    .execute(&plan, &columns_needed, config, mask, skip_dedup)
            }
        }
        ExecutionPath::Mixed { sstables, segments, columns_needed } => {
            let res_row = Pipeline::new(sstables).execute(&plan, config, mask, skip_dedup)?;
            let res_col = if aqp_enabled
                && crate::query::executor::aqp_pipeline::AqpPipeline::supports_plan(&plan)
                && crate::query::executor::aqp_pipeline::group_by_is_low_cardinality(
                    &segments, &plan,
                )
                .map_err(IngestionError::Query)?
            {
                crate::query::executor::aqp_pipeline::AqpPipeline::new(segments)
                    .execute(&plan, config, mask, skip_dedup)?
            } else {
                ColumnarPipeline::new(segments)
                    .map_err(IngestionError::Storage)?
                    .execute(&plan, &columns_needed, config, mask, skip_dedup)?
            };

            let merged_value = merge_aggregate_values(
                &res_row.value,
                &res_col.value,
                &plan.aggregation,
                res_row.rows_scanned,
                res_col.rows_scanned,
            );
            let merged_confidence = merge_confidence(res_row.confidence, res_col.confidence);
            Ok(QueryResult {
                value: merged_value,
                confidence: merged_confidence,
                warnings: [res_row.warnings, res_col.warnings].concat(),
                storage_path: StoragePath::Mixed,
                rows_scanned: res_row.rows_scanned + res_col.rows_scanned,
                sampling_rate: (res_row.sampling_rate + res_col.sampling_rate) / 2.0,
                estimated_variance: 0.0,
                profile: res_row.profile,
                aqp: res_col.aqp,
                next_offset: res_col.next_offset.or(res_row.next_offset),
            })
        }
    }
    .map_err(IngestionError::Query)?;

    // 3. Merge memtable contribution
    let result = if mem_count > 0 {
        let final_value = match (&disc_result.value, &plan.aggregation) {
            (AggregateValue::Scalar(s), Aggregation::Avg(_)) => {
                let total = disc_result.rows_scanned + mem_count;
                AggregateValue::Scalar(
                    (s * disc_result.rows_scanned as f64 + mem_sum) / total as f64,
                )
            }
            (AggregateValue::Scalar(s), _) => AggregateValue::Scalar(
                s + if matches!(plan.aggregation, Aggregation::Count) {
                    mem_count as f64
                } else {
                    mem_sum
                },
            ),
            (AggregateValue::Empty, Aggregation::Avg(_)) => {
                AggregateValue::Scalar(mem_sum / mem_count as f64)
            }
            (AggregateValue::Empty, _) => AggregateValue::Scalar(
                if matches!(plan.aggregation, Aggregation::Count) {
                    mem_count as f64
                } else {
                    mem_sum
                },
            ),
            _ => disc_result.value.clone(),
        };
        QueryResult {
            value: final_value,
            rows_scanned: disc_result.rows_scanned + mem_count,
            warnings: [
                disc_result.warnings,
                vec!["MemTable data included".to_string()],
            ]
            .concat(),
            ..disc_result
        }
    } else {
        disc_result
    };

    Ok(result)
}

fn merge_aggregate_values(
    a: &crate::types::AggregateValue,
    b: &crate::types::AggregateValue,
    aggregation: &crate::query::ast::Aggregation,
    rows_a: u64,
    rows_b: u64,
) -> crate::types::AggregateValue {
    use crate::types::AggregateValue;
    use std::collections::HashMap;
    match (a, b) {
        (AggregateValue::Scalar(s1), AggregateValue::Scalar(s2)) => match aggregation {
            crate::query::ast::Aggregation::Avg(_) => {
                let total = rows_a + rows_b;
                if total > 0 {
                    AggregateValue::Scalar(
                        (s1 * rows_a as f64 + s2 * rows_b as f64) / total as f64,
                    )
                } else {
                    AggregateValue::Empty
                }
            }
            _ => AggregateValue::Scalar(s1 + s2),
        },
        (AggregateValue::Scalar(s), AggregateValue::Empty)
        | (AggregateValue::Empty, AggregateValue::Scalar(s)) => AggregateValue::Scalar(*s),
        (AggregateValue::Empty, AggregateValue::Empty) => AggregateValue::Empty,
        (AggregateValue::Groups(ga), AggregateValue::Groups(gb)) => {
            let mut merged: HashMap<String, (f64, u64, crate::types::ConfidenceFlag)> =
                HashMap::new();
            for (k, v, c) in ga {
                merged.insert(k.clone(), (*v, rows_a, *c));
            }
            for (k, v, c) in gb {
                merged
                    .entry(k.clone())
                    .and_modify(|(ev, er, ec)| {
                        *ev = match aggregation {
                            crate::query::ast::Aggregation::Avg(_) => {
                                let total = *er + rows_b;
                                if total > 0 {
                                    (*ev * *er as f64 + v * rows_b as f64) / total as f64
                                } else {
                                    *ev
                                }
                            }
                            _ => *ev + v,
                        };
                        *er += rows_b;
                        *ec = merge_confidence(*ec, *c);
                    })
                    .or_insert((*v, rows_b, *c));
            }
            let mut groups: Vec<(String, f64, crate::types::ConfidenceFlag)> =
                merged.into_iter().map(|(k, (v, _, c))| (k, v, c)).collect();
            groups.sort_by(|a, b| a.0.cmp(&b.0));
            AggregateValue::Groups(groups)
        }
        (AggregateValue::Groups(g), AggregateValue::Empty)
        | (AggregateValue::Empty, AggregateValue::Groups(g)) => {
            AggregateValue::Groups(g.clone())
        }
        (AggregateValue::Groups(g), _) | (_, AggregateValue::Groups(g)) => {
            AggregateValue::Groups(g.clone())
        }
    }
}

fn merge_confidence(
    a: crate::types::ConfidenceFlag,
    b: crate::types::ConfidenceFlag,
) -> crate::types::ConfidenceFlag {
    use crate::types::ConfidenceFlag;
    match (a, b) {
        (ConfidenceFlag::Low, _) | (_, ConfidenceFlag::Low) => ConfidenceFlag::Low,
        (ConfidenceFlag::High, _) | (_, ConfidenceFlag::High) => ConfidenceFlag::High,
        _ => ConfidenceFlag::Exact,
    }
}
