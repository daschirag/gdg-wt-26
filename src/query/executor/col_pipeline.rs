use crate::errors::{QueryError, StorageError};
use crate::query::ast::{Aggregation, Filter, FilterOp, PredicateExpr, QueryPlan};
use crate::storage::bitmap::Bitmap;
use crate::storage::columnar::encoding::rle::RleRunI64;
use crate::storage::columnar::reader::ColumnarReader;
use crate::types::{AggregateValue, ConfidenceFlag, QueryResult, SSTableMetadata, StoragePath};
use std::collections::{BTreeMap, HashMap};
use std::path::PathBuf;
use std::time::Instant;

pub struct ColumnarPipeline {
    pub segments: Vec<SegmentEntry>,
}

#[derive(Clone)]
pub struct SegmentEntry {
    pub path: PathBuf,
    pub metadata: SSTableMetadata,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ColumnStrategy {
    MetadataFastPath,
    GroupCountFastPath,
    BitmapScalar,
    BitmapGroup,
    RleScan,
    // COUNT with OR filter: one arm has bitmap_eq index, other is RLE-only.
    // Uses inclusion-exclusion: count(A) + count(B) - count(A AND B).
    InclusionExclusionCount,
    // COUNT with Or(bitmap_leaf, And(bitmap_leaf, rle_leaf)) shape.
    // Uses inclusion-exclusion without ever materializing the rle column bitmap.
    InclusionExclusionCount3,
    ScanFallback,
}

impl ColumnarPipeline {
    pub fn new(segments: Vec<String>) -> Result<Self, StorageError> {
        let segments = segments
            .into_iter()
            .enumerate()
            .map(|(seg_idx, path)| {
                let reader = ColumnarReader::new(path.clone().into(), seg_idx as u64)?;
                Ok(SegmentEntry {
                    path: PathBuf::from(path),
                    metadata: reader.metadata,
                })
            })
            .collect::<Result<Vec<_>, StorageError>>()?;
        Ok(Self { segments })
    }

    pub fn execute(
        &self,
        plan: &QueryPlan,
        _columns_needed: &[String],
        config: &crate::config::Config,
        _mask: Option<&std::collections::HashSet<u64>>,
        _skip_dedup: bool,
    ) -> Result<QueryResult, QueryError> {
        let start_total = Instant::now();
        let strategy = self.choose_strategy(plan);
        let mut profile = crate::types::QueryProfile::default();

        if strategy == ColumnStrategy::MetadataFastPath {
            let mut total_count = 0u64;
            let mut total_sum = 0.0;
            for segment in &self.segments {
                total_count += segment.metadata.row_count;
                if let Aggregation::Sum(col) | Aggregation::Avg(col) = &plan.aggregation {
                    total_sum += segment.metadata.columns.get(col).map(|c| c.sum).unwrap_or(0.0);
                }
            }
            let value = match &plan.aggregation {
                Aggregation::Count => AggregateValue::Scalar(total_count as f64),
                Aggregation::Sum(_) => AggregateValue::Scalar(total_sum),
                Aggregation::Avg(_) => {
                    if total_count > 0 {
                        AggregateValue::Scalar(total_sum / total_count as f64)
                    } else {
                        AggregateValue::Empty
                    }
                }
            };
            self.print_profile(plan, start_total, 0.0, 0.0, 0.0, self.segments.len() as u64, 0, total_count, 0, 0, 1.0, "metadata");
            return Ok(QueryResult {
                value,
                confidence: ConfidenceFlag::Exact,
                warnings: vec!["Metadata Fast-Path: Result is exact".to_string()],
                storage_path: StoragePath::Columnar,
                rows_scanned: 0,
                sampling_rate: 1.0,
                estimated_variance: 0.0,
                profile,
            });
        }

        let mut stage_open_ms = 0.0;
        let mut stage_filter_ms = 0.0;
        let mut stage_agg_ms = 0.0;
        let mut io_col = 0u64;
        let mut rows_total = 0u64;
        let mut rows_matched = 0u64;
        let mut rows_scanned = 0u64;

        let mut scalar_sum = 0.0;
        let mut scalar_count = 0u64;
        let mut grouped: BTreeMap<String, (f64, u64)> = BTreeMap::new();

        for (seg_idx, segment) in self.segments.iter().enumerate() {
            let reader = ColumnarReader {
                path: segment.path.clone(),
                metadata: segment.metadata.clone(),
                seg_idx: seg_idx as u64,
            };
            rows_total += reader.metadata.row_count;

            match strategy {
                ColumnStrategy::GroupCountFastPath => {
                    let open_t = Instant::now();
                    let group_col = plan
                        .group_by
                        .as_ref()
                        .expect("group count fast path requires group_by");
                    let group_counts = reader.read_group_counts(group_col)?;
                    stage_open_ms += open_t.elapsed().as_secs_f64() * 1000.0;
                    io_col += 1;

                    let agg_t = Instant::now();
                    for (value, count) in group_counts {
                        if count == 0 {
                            continue;
                        }
                        let entry = grouped.entry(value.to_string()).or_insert((0.0, 0));
                        entry.0 += count as f64;
                        entry.1 += count;
                        rows_matched += count;
                        rows_scanned += count;
                    }
                    stage_agg_ms += agg_t.elapsed().as_secs_f64() * 1000.0;
                }
                ColumnStrategy::BitmapScalar | ColumnStrategy::BitmapGroup => {
                    let open_t = Instant::now();
                    let mut bitmap_cache: HashMap<String, HashMap<i64, Bitmap>> = HashMap::new();
                    let filter_bitmap = self.eval_bitmap_expr(plan.filter.as_ref(), &reader, &mut bitmap_cache)?;
                    stage_open_ms += open_t.elapsed().as_secs_f64() * 1000.0;
                    io_col += bitmap_cache.len() as u64;
                    let effective_bitmap = filter_bitmap.unwrap_or_else(|| Bitmap::full(reader.metadata.row_count as usize));
                    rows_matched += effective_bitmap.count_ones();

                    let agg_t = Instant::now();
                    if strategy == ColumnStrategy::BitmapGroup {
                        let group_col = plan.group_by.as_ref().expect("bitmap group strategy needs group_by");
                        let group_bitmaps = if let Some(existing) = bitmap_cache.get(group_col) {
                            existing.clone()
                        } else {
                            let loaded = reader.read_bitmap_index(group_col)?;
                            io_col += 1;
                            bitmap_cache.insert(group_col.clone(), loaded.clone());
                            loaded
                        };
                        let agg_i64 = match &plan.aggregation {
                            Aggregation::Sum(col) | Aggregation::Avg(col) => {
                                io_col += 1;
                                Some(reader.read_column_i64(col)?)
                            }
                            _ => None,
                        };

                        for (value, group_bitmap) in group_bitmaps {
                            let selected = group_bitmap.and(&effective_bitmap);
                            let count = selected.count_ones();
                            if count == 0 {
                                continue;
                            }
                            let entry = grouped.entry(value.to_string()).or_insert((0.0, 0));
                            match &plan.aggregation {
                                Aggregation::Count => {
                                    entry.0 += count as f64;
                                    entry.1 += count;
                                }
                                Aggregation::Sum(_) | Aggregation::Avg(_) => {
                                    let mut seg_sum = 0.0;
                                    if let Some(values) = &agg_i64 {
                                        for idx in selected.iter_ones() {
                                            seg_sum += values[idx] as f64;
                                        }
                                    }
                                    entry.0 += seg_sum;
                                    entry.1 += count;
                                }
                            }
                            rows_scanned += count;
                        }
                    } else {
                        match &plan.aggregation {
                            Aggregation::Count => {
                                scalar_count += effective_bitmap.count_ones();
                                rows_scanned += effective_bitmap.count_ones();
                            }
                            Aggregation::Sum(col) | Aggregation::Avg(col) => {
                                io_col += 1;
                                let seg_count = effective_bitmap.count_ones();
                                let values = reader.read_column_i64(col)?;
                                for idx in effective_bitmap.iter_ones() {
                                    scalar_sum += values[idx] as f64;
                                }
                                scalar_count += seg_count;
                                rows_scanned += seg_count;
                            }
                        }
                    }
                    stage_agg_ms += agg_t.elapsed().as_secs_f64() * 1000.0;
                }
                ColumnStrategy::InclusionExclusionCount => {
                    // COUNT(A OR B) = COUNT(A) + COUNT(B) - COUNT(A AND B)
                    // A: bitmap-indexed column, B: RLE column (no bitmap index)
                    let (bitmap_filter, rle_filter) = self
                        .split_or_for_inclusion_exclusion(plan.filter.as_ref().unwrap())
                        .expect("strategy guarantees split");
                    let bmp_threshold = bitmap_filter.value.parse::<i64>().unwrap_or(0);
                    let rle_threshold = rle_filter.value.parse::<i64>().unwrap_or(0);

                    let open_t = Instant::now();
                    let bitmaps = reader.read_bitmap_index(&bitmap_filter.column)?;
                    io_col += 1;
                    let runs = reader.read_column_runs_i64(&rle_filter.column)?;
                    io_col += 1;
                    stage_open_ms += open_t.elapsed().as_secs_f64() * 1000.0;

                    let agg_t = Instant::now();
                    // count(A): sum bitmaps for values satisfying bitmap_filter.op
                    let mut count_a = 0u64;
                    let mut matching_bitmaps: Vec<&Bitmap> = Vec::new();
                    for (val, bmap) in &bitmaps {
                        let include = match &bitmap_filter.op {
                            FilterOp::Eq => *val == bmp_threshold,
                            FilterOp::Gt => *val > bmp_threshold,
                            FilterOp::Lt => *val < bmp_threshold,
                            FilterOp::Ge => *val >= bmp_threshold,
                            FilterOp::Le => *val <= bmp_threshold,
                        };
                        if include {
                            count_a += bmap.count_ones();
                            matching_bitmaps.push(bmap);
                        }
                    }
                    // count(B): RLE COUNT — O(runs), no bitmap materialization
                    let mut count_b = 0u64;
                    for run in &runs {
                        let matches = match &rle_filter.op {
                            FilterOp::Eq => run.value == rle_threshold,
                            FilterOp::Gt => run.value > rle_threshold,
                            FilterOp::Lt => run.value < rle_threshold,
                            FilterOp::Ge => run.value >= rle_threshold,
                            FilterOp::Le => run.value <= rle_threshold,
                        };
                        if matches { count_b += run.length as u64; }
                    }
                    // count(A AND B): lockstep walk of RLE runs against each matching bitmap
                    let mut count_ab = 0u64;
                    for bmap in &matching_bitmaps {
                        count_ab += rle_count_matching_in_bitmap(&runs, bmap, &rle_filter.op, rle_threshold);
                    }
                    let seg_count = count_a + count_b - count_ab;
                    scalar_count += seg_count;
                    rows_matched += seg_count;
                    rows_scanned += reader.metadata.row_count;
                    stage_agg_ms += agg_t.elapsed().as_secs_f64() * 1000.0;
                }
                ColumnStrategy::InclusionExclusionCount3 => {
                    // COUNT(A OR (B AND C)) = COUNT(A) + COUNT(B AND C) - COUNT(A AND B AND C)
                    // A, B: bitmap-indexed; C: RLE-only (never materialized as a bitmap)
                    let (fa, fb, fc) = self
                        .split_ie3(plan.filter.as_ref().unwrap())
                        .expect("strategy guarantees split");
                    let ta = fa.value.parse::<i64>().unwrap_or(0);
                    let tb = fb.value.parse::<i64>().unwrap_or(0);
                    let tc = fc.value.parse::<i64>().unwrap_or(0);

                    let open_t = Instant::now();
                    let bitmaps_a = reader.read_bitmap_index(&fa.column)?;
                    io_col += 1;
                    let bitmaps_b = reader.read_bitmap_index(&fb.column)?;
                    io_col += 1;
                    let runs_c = reader.read_column_runs_i64(&fc.column)?;
                    io_col += 1;
                    stage_open_ms += open_t.elapsed().as_secs_f64() * 1000.0;

                    let agg_t = Instant::now();
                    // Build bitmap for A
                    let row_count = reader.metadata.row_count as usize;
                    let mut bmp_a = Bitmap::new(row_count);
                    for (val, bmap) in &bitmaps_a {
                        let include = match &fa.op {
                            FilterOp::Eq => *val == ta, FilterOp::Gt => *val > ta,
                            FilterOp::Lt => *val < ta, FilterOp::Ge => *val >= ta,
                            FilterOp::Le => *val <= ta,
                        };
                        if include { bmp_a = bmp_a.or(bmap); }
                    }
                    // count(A)
                    let count_a = bmp_a.count_ones();
                    // Build bitmap for B
                    let mut bmp_b = Bitmap::new(row_count);
                    for (val, bmap) in &bitmaps_b {
                        let include = match &fb.op {
                            FilterOp::Eq => *val == tb, FilterOp::Gt => *val > tb,
                            FilterOp::Lt => *val < tb, FilterOp::Ge => *val >= tb,
                            FilterOp::Le => *val <= tb,
                        };
                        if include { bmp_b = bmp_b.or(bmap); }
                    }
                    // count(B AND C): lockstep RLE walk against bmp_b
                    let count_bc = rle_count_matching_in_bitmap(&runs_c, &bmp_b, &fc.op, tc);
                    // count(A AND B AND C): lockstep RLE walk against bmp_a AND bmp_b
                    let bmp_ab = bmp_a.and(&bmp_b);
                    let count_abc = rle_count_matching_in_bitmap(&runs_c, &bmp_ab, &fc.op, tc);

                    let seg_count = count_a + count_bc - count_abc;
                    scalar_count += seg_count;
                    rows_matched += seg_count;
                    rows_scanned += reader.metadata.row_count;
                    stage_agg_ms += agg_t.elapsed().as_secs_f64() * 1000.0;
                }
                ColumnStrategy::RleScan => {
                    let filter = match plan.filter.as_ref() {
                        Some(PredicateExpr::Comparison(f)) => f,
                        _ => unreachable!("rle_scan_ready guarantees single comparison"),
                    };
                    let threshold = filter.value.parse::<i64>().unwrap_or(0);
                    let op = &filter.op;

                    let col_encoding = reader
                        .metadata
                        .columns
                        .get(&filter.column)
                        .map(|c| c.encoding.as_str())
                        .unwrap_or("rle");

                    let open_t = Instant::now();
                    io_col += 1;

                    if col_encoding == "bincode" {
                        // Fast path for bincode (raw i64 array): read payload bytes,
                        // cast directly to &[i64], count with a tight scalar loop.
                        let raw = reader.read_column_i64_raw_slice(&filter.column)?;
                        stage_open_ms += open_t.elapsed().as_secs_f64() * 1000.0;

                        let filter_t = Instant::now();
                        let mut count = 0u64;
                        let n = raw.len() / 8;
                        for chunk in raw.chunks_exact(8) {
                            let v = i64::from_le_bytes(chunk.try_into().unwrap());
                            let matches = match op {
                                FilterOp::Eq => v == threshold,
                                FilterOp::Gt => v > threshold,
                                FilterOp::Lt => v < threshold,
                                FilterOp::Ge => v >= threshold,
                                FilterOp::Le => v <= threshold,
                            };
                            count += matches as u64;
                        }
                        rows_scanned += n as u64;
                        rows_matched += count;
                        scalar_count += count;
                        stage_filter_ms += filter_t.elapsed().as_secs_f64() * 1000.0;
                    } else {
                        // RLE path: O(runs) for COUNT, avoids full materialization.
                        let runs = reader.read_column_runs_i64(&filter.column)?;

                        // For SUM/AVG we also need the agg column values.
                        let agg_col_data = match &plan.aggregation {
                            Aggregation::Sum(col) | Aggregation::Avg(col)
                                if col != &filter.column =>
                            {
                                io_col += 1;
                                Some(reader.read_column_i64(col)?)
                            }
                            _ => None,
                        };
                        stage_open_ms += open_t.elapsed().as_secs_f64() * 1000.0;

                        let filter_t = Instant::now();
                        let mut row_offset = 0usize;
                        for run in &runs {
                            let len = run.length as u64;
                            let matches = match op {
                                FilterOp::Eq => run.value == threshold,
                                FilterOp::Gt => run.value > threshold,
                                FilterOp::Lt => run.value < threshold,
                                FilterOp::Ge => run.value >= threshold,
                                FilterOp::Le => run.value <= threshold,
                            };
                            rows_scanned += len;
                            if matches {
                                rows_matched += len;
                                match &plan.aggregation {
                                    Aggregation::Count => scalar_count += len,
                                    Aggregation::Sum(_) | Aggregation::Avg(_) => {
                                        if let Some(agg_data) = &agg_col_data {
                                            for r in row_offset..row_offset + len as usize {
                                                scalar_sum += agg_data[r] as f64;
                                            }
                                            scalar_count += len;
                                        } else {
                                            scalar_sum += run.value as f64 * len as f64;
                                            scalar_count += len;
                                        }
                                    }
                                }
                            }
                            row_offset += len as usize;
                        }
                        stage_filter_ms += filter_t.elapsed().as_secs_f64() * 1000.0;
                    }
                }
                ColumnStrategy::ScanFallback => {
                    let open_t = Instant::now();
                    let required = plan.required_columns();
                    let mut i64_cols: HashMap<String, crate::utils::aligned_vec::AlignedVec<i64>> = HashMap::new();
                    for column in required.iter().filter(|name| config.schema.col_index(name).is_some()) {
                        if let Ok(values) = reader.read_column_i64(column) {
                            io_col += 1;
                            i64_cols.insert(column.clone(), values);
                        }
                    }
                    stage_open_ms += open_t.elapsed().as_secs_f64() * 1000.0;

                    // Pre-parse filter literals so we don't re-parse per row.
                    let parsed_filter = plan.filter.as_ref().map(|expr| preparse_filter(expr));

                    let filter_t = Instant::now();
                    let row_count = i64_cols.values().next().map(|v| v.len()).unwrap_or(reader.metadata.row_count as usize);
                    let agg_values = match &plan.aggregation {
                        Aggregation::Sum(col) | Aggregation::Avg(col) => i64_cols.get(col),
                        Aggregation::Count => None,
                    };
                    for row_idx in 0..row_count {
                        let matched = parsed_filter.as_ref().map_or(true, |expr| {
                            eval_preparsed(expr, &|column| {
                                i64_cols.get(column).map(|vals| vals[row_idx])
                            })
                        });
                        if !matched {
                            continue;
                        }
                        rows_matched += 1;
                        rows_scanned += 1;
                        match &plan.group_by {
                            Some(group_col) => {
                                let group_val = i64_cols
                                    .get(group_col)
                                    .map(|vals| vals[row_idx].to_string())
                                    .unwrap_or_else(|| "0".to_string());
                                let entry = grouped.entry(group_val).or_insert((0.0, 0));
                                match &plan.aggregation {
                                    Aggregation::Count => {
                                        entry.0 += 1.0;
                                        entry.1 += 1;
                                    }
                                    Aggregation::Sum(_) | Aggregation::Avg(_) => {
                                        let value = agg_values.map(|vals| vals[row_idx] as f64).unwrap_or(0.0);
                                        entry.0 += value;
                                        entry.1 += 1;
                                    }
                                }
                            }
                            None => match &plan.aggregation {
                                Aggregation::Count => scalar_count += 1,
                                Aggregation::Sum(_) | Aggregation::Avg(_) => {
                                    scalar_sum += agg_values.map(|vals| vals[row_idx] as f64).unwrap_or(0.0);
                                    scalar_count += 1;
                                }
                            },
                        }
                    }
                    stage_filter_ms += filter_t.elapsed().as_secs_f64() * 1000.0;
                }
                ColumnStrategy::MetadataFastPath => unreachable!(),
            }
        }

        profile.io_read_ms = stage_open_ms;
        profile.filtering_ms = stage_filter_ms;
        profile.aggregation_ms = stage_agg_ms;

        let value = if !grouped.is_empty() {
            AggregateValue::Groups(
                grouped
                    .into_iter()
                    .map(|(key, (sum, count))| {
                        let val = match &plan.aggregation {
                            Aggregation::Avg(_) => {
                                if count > 0 {
                                    sum / count as f64
                                } else {
                                    0.0
                                }
                            }
                            Aggregation::Count => count as f64,
                            Aggregation::Sum(_) => sum,
                        };
                        (key, val, ConfidenceFlag::Exact)
                    })
                    .collect(),
            )
        } else {
            let scalar = match &plan.aggregation {
                Aggregation::Count => scalar_count as f64,
                Aggregation::Sum(_) => scalar_sum,
                Aggregation::Avg(_) => {
                    if scalar_count > 0 {
                        scalar_sum / scalar_count as f64
                    } else {
                        0.0
                    }
                }
            };
            AggregateValue::Scalar(scalar)
        };

        self.print_profile(
            plan,
            start_total,
            stage_open_ms,
            stage_filter_ms,
            stage_agg_ms,
            self.segments.len() as u64,
            io_col,
            rows_total,
            rows_matched,
            rows_scanned,
            1.0,
            match strategy {
                ColumnStrategy::GroupCountFastPath => "group_counts",
                ColumnStrategy::BitmapScalar => "bitmap_scalar",
                ColumnStrategy::BitmapGroup => "bitmap_group",
                ColumnStrategy::RleScan => "rle_scan",
                ColumnStrategy::InclusionExclusionCount => "ie_count",
                ColumnStrategy::InclusionExclusionCount3 => "ie_count3",
                ColumnStrategy::ScanFallback => "scan_fallback",
                ColumnStrategy::MetadataFastPath => "metadata",
            },
        );

        Ok(QueryResult {
            value,
            confidence: ConfidenceFlag::Exact,
            warnings: vec![],
            storage_path: StoragePath::Columnar,
            rows_scanned,
            sampling_rate: 1.0,
            estimated_variance: 0.0,
            profile,
        })
    }

    fn choose_strategy(&self, plan: &QueryPlan) -> ColumnStrategy {
        if plan.filter.is_none() && plan.group_by.is_none() {
            return ColumnStrategy::MetadataFastPath;
        }
        if plan.filter.is_none()
            && matches!(plan.aggregation, Aggregation::Count)
            && let Some(group_col) = &plan.group_by
            && self
                .segments
                .iter()
                .all(|segment| segment.metadata.columns.get(group_col).and_then(|c| c.index("group_counts")).is_some())
        {
            return ColumnStrategy::GroupCountFastPath;
        }
        if let Some(group_col) = &plan.group_by {
            if self
                .segments
                .iter()
                .all(|segment| segment.metadata.columns.get(group_col).and_then(|c| c.index("bitmap_eq")).is_some())
                && (plan.filter.is_none() || self.bitmap_ready_expr(plan.filter.as_ref()))
            {
                return ColumnStrategy::BitmapGroup;
            }
        }
        if plan.group_by.is_none()
            && matches!(plan.aggregation, Aggregation::Count)
            && self.inclusion_exclusion_ready(plan.filter.as_ref())
        {
            return ColumnStrategy::InclusionExclusionCount;
        }
        if plan.group_by.is_none() && self.bitmap_ready_expr(plan.filter.as_ref()) {
            return ColumnStrategy::BitmapScalar;
        }
        if plan.group_by.is_none() && self.rle_scan_ready(plan) {
            return ColumnStrategy::RleScan;
        }
        ColumnStrategy::ScanFallback
    }

    // Returns Some((bitmap_filter, rle_filter)) if the filter is a two-arm OR where one
    // arm has a bitmap_eq index and the other is an RLE-encoded Eq column (no bitmap index).
    // The RLE arm must use Eq (not a range op) because the IE intersection term
    // rle_count_matching_in_bitmap walks every matching RLE run against the bitmap —
    // cheap for Eq (few matching runs) but expensive for range ops on high-cardinality columns.
    // Range ops stay in BitmapScalar where a SIMD bitmap OR is faster.
    fn split_or_for_inclusion_exclusion<'a>(
        &self,
        expr: &'a PredicateExpr,
    ) -> Option<(&'a Filter, &'a Filter)> {
        let PredicateExpr::Or(left, right) = expr else { return None };
        let (PredicateExpr::Comparison(lf), PredicateExpr::Comparison(rf)) =
            (left.as_ref(), right.as_ref())
        else {
            return None;
        };
        let l_has_bitmap = self.segments.iter().all(|s| {
            s.metadata.columns.get(&lf.column).and_then(|c| c.index("bitmap_eq")).is_some()
        });
        let r_is_rle_eq = matches!(rf.op, FilterOp::Eq) && self.segments.iter().all(|s| {
            s.metadata.columns.get(&rf.column)
                .map(|c| c.encoding == "rle" && c.index("bitmap_eq").is_none())
                .unwrap_or(false)
        });
        if l_has_bitmap && r_is_rle_eq {
            return Some((lf, rf));
        }
        let r_has_bitmap = self.segments.iter().all(|s| {
            s.metadata.columns.get(&rf.column).and_then(|c| c.index("bitmap_eq")).is_some()
        });
        let l_is_rle_eq = matches!(lf.op, FilterOp::Eq) && self.segments.iter().all(|s| {
            s.metadata.columns.get(&lf.column)
                .map(|c| c.encoding == "rle" && c.index("bitmap_eq").is_none())
                .unwrap_or(false)
        });
        if r_has_bitmap && l_is_rle_eq {
            return Some((rf, lf));
        }
        None
    }

    fn inclusion_exclusion_ready(&self, filter: Option<&PredicateExpr>) -> bool {
        filter
            .map(|f| self.split_or_for_inclusion_exclusion(f).is_some())
            .unwrap_or(false)
    }

    // Detects Or(bitmap_leaf, And(bitmap_leaf, rle_leaf)) or the symmetric form.
    // Returns (bitmap_a, bitmap_b, rle_filter) where:
    //   count = count(A) + count(B AND C) - count(A AND B AND C)
    // and none of these require materializing a bitmap for the rle column.
    fn split_ie3<'a>(
        &self,
        expr: &'a PredicateExpr,
    ) -> Option<(&'a Filter, &'a Filter, &'a Filter)> {
        let PredicateExpr::Or(or_left, or_right) = expr else { return None };

        // Try Or(bitmap_leaf, And(bitmap_leaf, rle_leaf))
        let try_split = |a: &'a PredicateExpr, and_expr: &'a PredicateExpr| -> Option<(&'a Filter, &'a Filter, &'a Filter)> {
            let PredicateExpr::Comparison(fa) = a else { return None };
            let PredicateExpr::And(and_l, and_r) = and_expr else { return None };
            let (PredicateExpr::Comparison(fb), PredicateExpr::Comparison(fc)) =
                (and_l.as_ref(), and_r.as_ref()) else { return None };

            let a_has_bitmap = self.segments.iter().all(|s| {
                s.metadata.columns.get(&fa.column).and_then(|c| c.index("bitmap_eq")).is_some()
            });
            let b_has_bitmap = self.segments.iter().all(|s| {
                s.metadata.columns.get(&fb.column).and_then(|c| c.index("bitmap_eq")).is_some()
            });
            let c_is_rle = self.segments.iter().all(|s| {
                s.metadata.columns.get(&fc.column)
                    .map(|c| c.encoding == "rle" && c.index("bitmap_eq").is_none())
                    .unwrap_or(false)
            });
            if a_has_bitmap && b_has_bitmap && c_is_rle {
                return Some((fa, fb, fc));
            }
            // Also try And(rle_leaf, bitmap_leaf) order
            let b_is_rle = self.segments.iter().all(|s| {
                s.metadata.columns.get(&fb.column)
                    .map(|c| c.encoding == "rle" && c.index("bitmap_eq").is_none())
                    .unwrap_or(false)
            });
            let c_has_bitmap = self.segments.iter().all(|s| {
                s.metadata.columns.get(&fc.column).and_then(|c| c.index("bitmap_eq")).is_some()
            });
            if a_has_bitmap && c_has_bitmap && b_is_rle {
                return Some((fa, fc, fb));
            }
            None
        };

        try_split(or_left, or_right).or_else(|| try_split(or_right, or_left))
    }

    fn ie3_ready(&self, filter: Option<&PredicateExpr>) -> bool {
        filter.map(|f| self.split_ie3(f).is_some()).unwrap_or(false)
    }

    // Returns true when the plan has a simple COUNT with a single comparison filter
    // on an RLE or bincode i64 column with no group_by.
    fn rle_scan_ready(&self, plan: &QueryPlan) -> bool {
        if plan.group_by.is_some() {
            return false;
        }
        let filter = match plan.filter.as_ref() {
            Some(PredicateExpr::Comparison(f)) => f,
            _ => return false,
        };
        self.segments.iter().all(|seg| {
            seg.metadata
                .columns
                .get(&filter.column)
                .map(|c| c.encoding == "rle" || c.encoding == "bincode")
                .unwrap_or(false)
        })
    }

    fn bitmap_ready_expr(&self, expr: Option<&PredicateExpr>) -> bool {
        expr.map_or(false, |expr| self.bitmap_ready_node(expr))
    }

    fn bitmap_ready_node(&self, expr: &PredicateExpr) -> bool {
        match expr {
            PredicateExpr::Comparison(filter) => self.segments.iter().all(|segment| {
                let col = segment.metadata.columns.get(&filter.column);
                // bitmap_eq index: supports all ops via index lookup
                // rle encoding: supports all ops via on-the-fly run scan
                col.and_then(|c| c.index("bitmap_eq")).is_some()
                    || col.map(|c| c.encoding == "rle").unwrap_or(false)
            }),
            PredicateExpr::Not(inner) => self.bitmap_ready_node(inner),
            PredicateExpr::And(left, right) | PredicateExpr::Or(left, right) => {
                self.bitmap_ready_node(left) && self.bitmap_ready_node(right)
            }
        }
    }

    fn eval_bitmap_expr(
        &self,
        expr: Option<&PredicateExpr>,
        reader: &ColumnarReader,
        cache: &mut HashMap<String, HashMap<i64, Bitmap>>,
    ) -> Result<Option<Bitmap>, QueryError> {
        expr.map(|expr| self.eval_bitmap_node(expr, reader, cache))
            .transpose()
    }

    fn eval_bitmap_node(
        &self,
        expr: &PredicateExpr,
        reader: &ColumnarReader,
        cache: &mut HashMap<String, HashMap<i64, Bitmap>>,
    ) -> Result<Bitmap, QueryError> {
        match expr {
            PredicateExpr::Comparison(filter) => self.bitmap_for_filter(filter, reader, cache),
            PredicateExpr::Not(inner) => Ok(self.eval_bitmap_node(inner, reader, cache)?.not()),
            PredicateExpr::And(left, right) => Ok(self
                .eval_bitmap_node(left, reader, cache)?
                .and(&self.eval_bitmap_node(right, reader, cache)?)),
            PredicateExpr::Or(left, right) => {
                let left_bmp = self.eval_bitmap_node(left, reader, cache)?;
                // Short-circuit: if left already matches all rows, skip evaluating right.
                if left_bmp.count_ones() as u64 == reader.metadata.row_count {
                    return Ok(left_bmp);
                }
                Ok(left_bmp.or(&self.eval_bitmap_node(right, reader, cache)?))
            }
        }
    }

    fn bitmap_for_filter(
        &self,
        filter: &Filter,
        reader: &ColumnarReader,
        cache: &mut HashMap<String, HashMap<i64, Bitmap>>,
    ) -> Result<Bitmap, QueryError> {
        let col_meta = reader
            .metadata
            .columns
            .get(&filter.column)
            .ok_or_else(|| QueryError::UnknownColumn(filter.column.clone()))?;
        let target = filter
            .value
            .parse::<i64>()
            .map_err(|_| QueryError::ParseError(format!("invalid literal {}", filter.value)))?;
        let op = &filter.op;

        // Fast path: use pre-built bitmap_eq index when available.
        if let Some(bitmap_meta) = col_meta.index("bitmap_eq") {
            let index = if let Some(existing) = cache.get(&filter.column) {
                existing.clone()
            } else {
                let loaded = reader.read_bitmap_index(&filter.column).map_err(QueryError::StorageError)?;
                cache.insert(filter.column.clone(), loaded.clone());
                loaded
            };
            let mut bitmap = Bitmap::new(bitmap_meta.row_count as usize);
            for value in &bitmap_meta.values {
                let include = match op {
                    FilterOp::Eq => *value == target,
                    FilterOp::Gt => *value > target,
                    FilterOp::Lt => *value < target,
                    FilterOp::Ge => *value >= target,
                    FilterOp::Le => *value <= target,
                };
                if include {
                    if let Some(part) = index.get(value) {
                        bitmap = bitmap.or(part);
                    }
                }
            }
            return Ok(bitmap);
        }

        // Fallback: build bitmap from RLE runs (O(runs), no index required).
        let row_count = reader.metadata.row_count as usize;
        let runs = reader.read_column_runs_i64(&filter.column)?;
        let mut bitmap = Bitmap::new(row_count);
        let mut row = 0usize;
        for run in &runs {
            let len = run.length as usize;
            let include = match op {
                FilterOp::Eq => run.value == target,
                FilterOp::Gt => run.value > target,
                FilterOp::Lt => run.value < target,
                FilterOp::Ge => run.value >= target,
                FilterOp::Le => run.value <= target,
            };
            if include {
                bitmap.set_range(row, len);
            }
            row += len;
        }
        Ok(bitmap)
    }

    fn print_profile(
        &self,
        plan: &QueryPlan,
        start: Instant,
        open_ms: f64,
        filter_ms: f64,
        agg_ms: f64,
        segs: u64,
        cols: u64,
        rows_total: u64,
        rows_matched: u64,
        rows_scanned: u64,
        rate: f64,
        strategy: &str,
    ) {
        let q = match (&plan.aggregation, plan.group_by.is_some()) {
            (Aggregation::Count, true) => "Q1",
            (Aggregation::Sum(_), false) => "Q2",
            (Aggregation::Count, false) => "Q3",
            _ => "QX",
        };
        println!(
            "[PROFILE] query={q} total_ms={:.2} stage.col_open_ms={open_ms:.2} stage.decode_ms=0.00 stage.filter_agg_ms={:.2} io.seg={segs} io.col={cols} rows.tot={rows_total} rows.filt={rows_matched} rows.samp={rows_scanned} rate.sst={rate:.4} rate.row={rate:.4} mfp={} parallel=true strategy={strategy}",
            start.elapsed().as_secs_f64() * 1000.0,
            filter_ms + agg_ms,
            strategy == "metadata",
        );
    }
}

// Pre-parsed filter tree where literal values are already converted to i64.
// Used by ScanFallback to avoid re-parsing per row.
enum PreparsedExpr {
    Cmp { col: String, op: FilterOp, threshold: i64 },
    Not(Box<PreparsedExpr>),
    And(Box<PreparsedExpr>, Box<PreparsedExpr>),
    Or(Box<PreparsedExpr>, Box<PreparsedExpr>),
}

fn preparse_filter(expr: &PredicateExpr) -> PreparsedExpr {
    match expr {
        PredicateExpr::Comparison(f) => PreparsedExpr::Cmp {
            col: f.column.clone(),
            op: f.op.clone(),
            threshold: f.value.parse::<i64>().unwrap_or(0),
        },
        PredicateExpr::Not(inner) => PreparsedExpr::Not(Box::new(preparse_filter(inner))),
        PredicateExpr::And(l, r) => PreparsedExpr::And(
            Box::new(preparse_filter(l)),
            Box::new(preparse_filter(r)),
        ),
        PredicateExpr::Or(l, r) => PreparsedExpr::Or(
            Box::new(preparse_filter(l)),
            Box::new(preparse_filter(r)),
        ),
    }
}

fn eval_preparsed(expr: &PreparsedExpr, get: &impl Fn(&str) -> Option<i64>) -> bool {
    match expr {
        PreparsedExpr::Cmp { col, op, threshold } => {
            let Some(v) = get(col) else { return false };
            match op {
                FilterOp::Eq => v == *threshold,
                FilterOp::Gt => v > *threshold,
                FilterOp::Lt => v < *threshold,
                FilterOp::Ge => v >= *threshold,
                FilterOp::Le => v <= *threshold,
            }
        }
        PreparsedExpr::Not(inner) => !eval_preparsed(inner, get),
        PreparsedExpr::And(l, r) => eval_preparsed(l, get) && eval_preparsed(r, get),
        PreparsedExpr::Or(l, r) => eval_preparsed(l, get) || eval_preparsed(r, get),
    }
}

/// Sum values from `runs` at positions indicated by `bitmap`, without materializing the full
/// decoded array. Walks runs in order, uses word-level popcount per run: O(runs × words/run).
fn rle_sum_by_bitmap(runs: &[RleRunI64], bitmap: &Bitmap) -> f64 {
    let mut sum = 0.0f64;
    let mut row = 0usize;
    for run in runs {
        let len = run.length as usize;
        let end = row + len;
        let count = bitmap.count_ones_range(row, end);
        if count > 0 {
            sum += run.value as f64 * count as f64;
        }
        row = end;
    }
    sum
}

/// Count bits set in `bitmap` at positions where the RLE column satisfies `op threshold`.
/// Used for inclusion-exclusion on OR filters involving RLE columns.
fn rle_count_matching_in_bitmap(runs: &[RleRunI64], bitmap: &Bitmap, op: &FilterOp, threshold: i64) -> u64 {
    let mut count = 0u64;
    let mut row = 0usize;
    for run in runs {
        let len = run.length as usize;
        let end = row + len;
        let matches = match op {
            FilterOp::Eq => run.value == threshold,
            FilterOp::Gt => run.value > threshold,
            FilterOp::Lt => run.value < threshold,
            FilterOp::Ge => run.value >= threshold,
            FilterOp::Le => run.value <= threshold,
        };
        if matches {
            count += bitmap.count_ones_range(row, end);
        }
        row = end;
    }
    count
}
