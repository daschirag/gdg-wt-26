use crate::errors::StorageError;
use crate::storage::bitmap::{build_equality_bitmaps, write_bitmap_index};
use crate::storage::columnar::encoding::delta::DeltaEncoder;
use crate::storage::columnar::encoding::rle::{RleEncoder, RleRunI64};
use crate::types::{ColumnIndexMetadata, RowDisk, SSTableMetadata, Value};
use std::fs;
use std::path::Path;

pub struct ColumnarWriter;

const RLE_I64_MAGIC: [u8; 4] = *b"RLI4";
const RLE_I64_VERSION: u32 = 1;
const BITMAP_MAX_DISTINCT: usize = 256;

fn write_group_counts(path: &Path, values: &[i64], counts: &[u64]) -> Result<(), StorageError> {
    let bytes = bincode::serialize(&(values, counts)).map_err(|e| {
        StorageError::WriteError(std::io::Error::new(std::io::ErrorKind::Other, e))
    })?;
    fs::write(path, bytes)?;
    Ok(())
}

fn serialize_rle_i64(runs: &[RleRunI64]) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(16 + std::mem::size_of_val(runs));
    bytes.extend_from_slice(&RLE_I64_MAGIC);
    bytes.extend_from_slice(&RLE_I64_VERSION.to_le_bytes());
    bytes.extend_from_slice(&(runs.len() as u64).to_le_bytes());
    bytes.extend_from_slice(bytemuck::cast_slice(runs));
    bytes
}

impl ColumnarWriter {
    pub fn write_segment(
        segment_path: &Path,
        rows: &[RowDisk],
        config: &crate::config::Config,
        fpr: f64,
    ) -> Result<(), StorageError> {
        Self::write_segment_from_iter(segment_path, rows.iter().cloned(), config, fpr)
    }

    pub fn write_segment_from_iter<I>(
        segment_path: &Path,
        row_iter: I,
        config: &crate::config::Config,
        fpr: f64,
    ) -> Result<(), StorageError>
    where
        I: Iterator<Item = RowDisk>,
    {
        let rows: Vec<RowDisk> = row_iter.collect(); // Still collecting for now to handle transposition easily
        if rows.is_empty() {
            return Ok(());
        }

        // 1. Prepare directory
        if segment_path.exists() {
            fs::remove_dir_all(segment_path)?;
        }
        fs::create_dir_all(segment_path)?;

        // 2. Transpose Rows to Columns
        let num_cols = config.schema.columns.len();
        let num_rows = rows.len();
        let mut columns: Vec<Vec<Value>> = vec![Vec::with_capacity(num_rows); num_cols];

        for row in &rows {
            for i in 0..num_cols {
                let val = row.values.get(i).cloned().unwrap_or(Value::Int(0));
                columns[i].push(val);
            }
        }

        // 3. Encode and Write Columns
        let mut column_metadata = std::collections::BTreeMap::new();
        let mut min_ts = i64::MAX;
        let mut max_ts = i64::MIN;
        for (i, col_schema) in config.schema.columns.iter().enumerate() {
            let col_data = &columns[i];
            let file_name = format!("{}.col", col_schema.name);
            let file_path = segment_path.join(file_name);
            let is_numeric_col = matches!(
                col_schema.r#type.as_str(),
                "i64" | "u64" | "u32" | "i32" | "u16" | "i16" | "u8" | "i8" | "f64" | "f32"
            );

            let mut col_sum = 0.0;
            let mut col_min = f64::MAX;
            let mut col_max = f64::MIN;
            let mut distinct_values = std::collections::HashSet::new();

            // Collect stats
            for val in col_data {
                match val {
                    Value::Int(iv) => {
                        let f_val = *iv as f64;
                        col_sum += f_val;
                        col_min = col_min.min(f_val);
                        col_max = col_max.max(f_val);
                        distinct_values.insert(*iv);
                    }
                    Value::Float(fv) => {
                        col_sum += *fv;
                        col_min = col_min.min(*fv);
                        col_max = col_max.max(*fv);
                    }
                    _ => {}
                }
            }

            if col_schema.name == "timestamp" {
                min_ts = min_ts.min(col_min as i64);
                max_ts = max_ts.max(col_max as i64);
            }

            let distinct_count = distinct_values.len();
            let row_count = col_data.len();
            let i64_data: Option<Vec<i64>> = if is_numeric_col {
                Some(
                    col_data
                        .iter()
                        .map(|v| match v {
                            Value::Int(iv) => *iv,
                            Value::Float(fv) => *fv as i64,
                            _ => 0,
                        })
                        .collect(),
                )
            } else {
                None
            };

            let monotonic_i64 = i64_data
                .as_ref()
                .map(|vals| vals.windows(2).all(|w| w[0] <= w[1]))
                .unwrap_or(false);

            // Heuristic Selection
            let (encoding, bytes) = if distinct_count < row_count / 10 && row_count > 0 {
                // Low cardinality -> RLE
                let i64_data = i64_data.clone().unwrap_or_default();
                let encoded = RleEncoder::encode_i64(&i64_data);
                ("rle".to_string(), serialize_rle_i64(&encoded))
            } else if monotonic_i64 {
                // Monotonic-like -> Delta
                let i64_data = i64_data.clone().unwrap_or_default();
                let encoded = DeltaEncoder::encode_i64(&i64_data);
                (
                    "delta".to_string(),
                    bincode::serialize(&encoded).map_err(|e| {
                        StorageError::WriteError(std::io::Error::new(std::io::ErrorKind::Other, e))
                    })?,
                )
            } else {
                // High cardinality / Random -> Bincode
                if is_numeric_col {
                    let mut aligned =
                        crate::utils::aligned_vec::AlignedVec::with_capacity(col_data.len());
                    for v in col_data {
                        aligned.push(match v {
                            Value::Int(iv) => *iv,
                            Value::Float(fv) => *fv as i64,
                            _ => 0,
                        });
                    }
                    (
                        "bincode".to_string(),
                        bincode::serialize(&aligned).map_err(|e| {
                            StorageError::WriteError(std::io::Error::new(
                                std::io::ErrorKind::Other,
                                e,
                            ))
                        })?,
                    )
                } else {
                    let mut aligned =
                        crate::utils::aligned_vec::AlignedVec::with_capacity(col_data.len());
                    for v in col_data {
                        aligned.push(match v {
                            Value::Float(fv) => *fv,
                            Value::Int(iv) => *iv as f64,
                            _ => 0.0,
                        });
                    }
                    (
                        "bincode".to_string(),
                        bincode::serialize(&aligned).map_err(|e| {
                            StorageError::WriteError(std::io::Error::new(
                                std::io::ErrorKind::Other,
                                e,
                            ))
                        })?,
                    )
                }
            };

            let crc32 = crc32fast::hash(&bytes);
            fs::write(file_path, bytes)?;

            let indexes = if let Some(i64_data) = i64_data.as_ref() {
                let mut indexes = Vec::new();
                if distinct_count > 0 && distinct_count <= BITMAP_MAX_DISTINCT {
                    let mut values: Vec<i64> = distinct_values.iter().copied().collect();
                    values.sort_unstable();
                    let bitmap_file = format!("{}.bitmap", col_schema.name);
                    let bitmap_path = segment_path.join(&bitmap_file);
                    let bitmaps = build_equality_bitmaps(i64_data);
                    write_bitmap_index(&bitmap_path, i64_data.len(), &values, &bitmaps)?;
                    indexes.push(ColumnIndexMetadata {
                        kind: "bitmap_eq".to_string(),
                        file: bitmap_file,
                        values,
                        row_count: i64_data.len() as u64,
                        supports: vec![
                            "=".to_string(),
                            ">".to_string(),
                            "<".to_string(),
                            ">=".to_string(),
                            "<=".to_string(),
                            "and".to_string(),
                            "or".to_string(),
                            "not".to_string(),
                        ],
                    });

                    let mut ordered_values: Vec<i64> = distinct_values.iter().copied().collect();
                    ordered_values.sort_unstable();
                    let counts: Vec<u64> = ordered_values
                        .iter()
                        .map(|value| bitmaps.get(value).map(|bm| bm.count_ones()).unwrap_or(0))
                        .collect();
                    let group_file = format!("{}.group", col_schema.name);
                    let group_path = segment_path.join(&group_file);
                    write_group_counts(&group_path, &ordered_values, &counts)?;
                    indexes.push(ColumnIndexMetadata {
                        kind: "group_counts".to_string(),
                        file: group_file,
                        values: ordered_values,
                        row_count: i64_data.len() as u64,
                        supports: vec!["count".to_string(), "group_by".to_string()],
                    });
                }
                indexes
            } else {
                Vec::new()
            };

            column_metadata.insert(
                col_schema.name.clone(),
                crate::types::ColumnMetadata {
                    encoding,
                    sum: col_sum,
                    min: if col_min == f64::MAX { 0.0 } else { col_min },
                    max: if col_max == f64::MIN { 0.0 } else { col_max },
                    distinct_count: distinct_count as u64,
                    crc32,
                    indexes,
                },
            );
        }

        let mut metadata = SSTableMetadata {
            magic: "AQEM".to_string(),
            row_count: num_rows as u64,
            min_ts: if min_ts == i64::MAX { 0 } else { min_ts },
            max_ts: if max_ts == i64::MIN { 0 } else { max_ts },
            schema_version: rows[0].version,
            columns: column_metadata,
            checksum: 0,
        };

        // Compute metadata checksum
        let meta_toml_pre = toml::to_string(&metadata).map_err(|e| {
            StorageError::WriteError(std::io::Error::new(std::io::ErrorKind::Other, e))
        })?;
        metadata.checksum = crc32fast::hash(meta_toml_pre.as_bytes());

        let meta_toml = toml::to_string_pretty(&metadata).map_err(|e| {
            StorageError::WriteError(std::io::Error::new(std::io::ErrorKind::Other, e))
        })?;
        fs::write(segment_path.join("meta.toml"), meta_toml)?;

        let _ = fpr;

        Ok(())
    }
}
