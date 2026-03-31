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
    GroupStatsScalar,
    RangeBlocks,
    DecomposedOrCount,
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

#[derive(Debug, Clone)]
struct StrategyChoice {
    strategy: ColumnStrategy,
    reason: String,
    estimate: PredicateEstimate,
    path_cost: f64,
}

#[derive(Debug, Clone, Default)]
struct DetailedProfile {
    bitmap_load_ms: f64,
    bitmap_eval_ms: f64,
    bitmap_combine_ms: f64,
    bitmap_iter_ms: f64,
    bitmap_match_rows: u64,
    bitmap_values_touched: u64,
    bitmap_leaf_count: u64,
    group_index_ms: f64,
    group_emit_ms: f64,
    group_stats_ms: f64,
    range_index_ms: f64,
    range_search_ms: f64,
    rle_scan_ms: f64,
    agg_sum_ms: f64,
    agg_count_ms: f64,
    planner_reason: String,
    planner_est_match_ratio: f64,
    planner_est_values_touched: u64,
    planner_est_path_cost: f64,
    segments_pruned_by_stats: u64,
}

#[derive(Debug, Clone, Default)]
struct PredicateEstimate {
    match_ratio: f64,
    values_touched: u64,
    has_or: bool,
    has_range: bool,
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
        let choice = self.choose_strategy(plan);
        let strategy = choice.strategy;
        let mut profile = crate::types::QueryProfile::default();
        let mut detailed = DetailedProfile {
            planner_reason: choice.reason.clone(),
            planner_est_match_ratio: choice.estimate.match_ratio,
            planner_est_values_touched: choice.estimate.values_touched,
            planner_est_path_cost: choice.path_cost,
            bitmap_leaf_count: plan
                .filter
                .as_ref()
                .map(predicate_leaf_count)
                .unwrap_or(0),
            ..DetailedProfile::default()
        };

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
                Aggregation::ApproxPercentile(_, _) => unreachable!(),
            };
            self.print_profile(
                plan,
                start_total,
                0.0,
                0.0,
                0.0,
                self.segments.len() as u64,
                0,
                total_count,
                0,
                0,
                1.0,
                "metadata",
                &detailed,
            );
            return Ok(QueryResult {
                value,
                confidence: ConfidenceFlag::Exact,
                warnings: vec!["Metadata Fast-Path: Result is exact".to_string()],
                storage_path: StoragePath::Columnar,
                rows_scanned: 0,
                sampling_rate: 1.0,
                estimated_variance: 0.0,
                profile,
                aqp: None,
                next_offset: None,
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
            if let Some(filter_expr) = &plan.filter
                && self.segment_pruned_by_stats(segment, filter_expr)
            {
                eprintln!("[PRUNE] seg={} skipped by min/max stats", seg_idx);
                rows_total += segment.metadata.row_count;
                detailed.segments_pruned_by_stats += 1;
                continue;
            }

            let reader = ColumnarReader {
                path: segment.path.clone(),
                metadata: segment.metadata.clone(),
                seg_idx: seg_idx as u64,
            };
            rows_total += reader.metadata.row_count;

            match strategy {
                ColumnStrategy::GroupStatsScalar => {
                    let filter = match plan.filter.as_ref() {
                        Some(PredicateExpr::Comparison(f)) => f,
                        _ => unreachable!("group stats strategy requires simple comparison"),
                    };
                    let target = filter.value.parse::<i64>().unwrap_or(0);
                    let open_t = Instant::now();
                    let stats = reader.read_group_stats(&filter.column)?;
                    let open_ms = open_t.elapsed().as_secs_f64() * 1000.0;
                    stage_open_ms += open_ms;
                    detailed.group_stats_ms += open_ms;
                    io_col += 1;

                    let agg_t = Instant::now();
                    if let Some(idx) = stats.values.iter().position(|value| *value == target) {
                        let count = stats.counts.get(idx).copied().unwrap_or(0);
                        rows_matched += count;
                        rows_scanned += count;
                        match &plan.aggregation {
                            Aggregation::Count => {
                                scalar_count += count;
                            }
                            Aggregation::Sum(col) | Aggregation::Avg(col) => {
                                let sum = stats
                                    .measure_sums
                                    .get(col)
                                    .and_then(|sums| sums.get(idx))
                                    .copied()
                                    .unwrap_or(0.0);
                                scalar_sum += sum;
                                scalar_count += count;
                            }
                            Aggregation::ApproxPercentile(_, _) => unreachable!(),
                        }
                    }
                    let agg_ms = agg_t.elapsed().as_secs_f64() * 1000.0;
                    stage_agg_ms += agg_ms;
                    detailed.agg_sum_ms += agg_ms;
                }
                ColumnStrategy::RangeBlocks => {
                    let filter = match plan.filter.as_ref() {
                        Some(PredicateExpr::Comparison(f)) => f,
                        _ => unreachable!("range blocks strategy requires simple comparison"),
                    };
                    let target = filter.value.parse::<i64>().unwrap_or(0);
                    let open_t = Instant::now();
                    let blocks = reader.read_range_blocks(&filter.column)?;
                    let encoded = reader.read_delta_encoded_i64(&filter.column)?;
                    let open_ms = open_t.elapsed().as_secs_f64() * 1000.0;
                    stage_open_ms += open_ms;
                    detailed.range_index_ms += open_ms;
                    io_col += 1;

                    let search_t = Instant::now();
                    let count = delta_range_count(&encoded, &blocks, &filter.op, target);
                    let search_ms = search_t.elapsed().as_secs_f64() * 1000.0;
                    stage_agg_ms += search_ms;
                    detailed.range_search_ms += search_ms;
                    detailed.agg_count_ms += search_ms;
                    scalar_count += count;
                    rows_matched += count;
                    rows_scanned += count;
                }
                ColumnStrategy::DecomposedOrCount => {
                    let expr = plan
                        .filter
                        .as_ref()
                        .expect("decomposed or count requires filter");
                    let count = self.count_expr_exact(
                        expr,
                        &reader,
                        &mut io_col,
                        &mut detailed,
                    )?;
                    scalar_count += count;
                    rows_matched += count;
                    rows_scanned += count;
                }
                ColumnStrategy::BitmapScalar | ColumnStrategy::BitmapGroup => {
                    if strategy == ColumnStrategy::BitmapScalar
                        && plan.group_by.is_none()
                        && matches!(plan.aggregation, Aggregation::Count)
                    {
                        let expr = plan
                            .filter
                            .as_ref()
                            .expect("bitmap scalar count requires filter");
                        let count = self.count_expr_exact(
                            expr,
                            &reader,
                            &mut io_col,
                            &mut detailed,
                        )?;
                        scalar_count += count;
                        rows_matched += count;
                        rows_scanned += count;
                        detailed.bitmap_match_rows += count;
                        continue;
                    }

                    let open_t = Instant::now();
                    let mut bitmap_cache: HashMap<String, HashMap<i64, Bitmap>> = HashMap::new();
                    let filter_bitmap = self.eval_bitmap_expr(
                        plan.filter.as_ref(),
                        &reader,
                        &mut bitmap_cache,
                        &mut detailed,
                    )?;
                    stage_open_ms += open_t.elapsed().as_secs_f64() * 1000.0;
                    io_col += bitmap_cache.len() as u64;
                    let effective_bitmap = filter_bitmap.unwrap_or_else(|| Bitmap::full(reader.metadata.row_count as usize));
                    rows_matched += effective_bitmap.count_ones();
                    detailed.bitmap_match_rows += effective_bitmap.count_ones();

                    let agg_t = Instant::now();
                    if strategy == ColumnStrategy::BitmapGroup {
                        let group_col = plan.group_by.as_ref().expect("bitmap group strategy needs group_by");
                        enum AggData {
                            Runs(Vec<RleRunI64>),
                            Array(crate::utils::aligned_vec::AlignedVec<i64>),
                            None,
                        }
                        enum GroupData {
                            Runs(Vec<RleRunI64>),
                            Array(crate::utils::aligned_vec::AlignedVec<i64>),
                        }
                        let agg_data = match &plan.aggregation {
                            Aggregation::Sum(col) | Aggregation::Avg(col) => {
                                io_col += 1;
                                if self.col_rle_compressible(col) {
                                    AggData::Runs(reader.read_column_runs_i64(col)?)
                                } else {
                                    AggData::Array(reader.read_column_i64(col)?)
                                }
                            }
                            _ => AggData::None,
                        };
                        let group_data = match &plan.aggregation {
                            Aggregation::Sum(_) | Aggregation::Avg(_) => {
                                io_col += 1;
                                if self.col_rle_compressible(group_col) {
                                    GroupData::Runs(reader.read_column_runs_i64(group_col)?)
                                } else {
                                    GroupData::Array(reader.read_column_i64(group_col)?)
                                }
                            }
                            Aggregation::Count => {
                                let group_bitmaps = if let Some(existing) = bitmap_cache.get(group_col) {
                                    existing.clone()
                                } else {
                                    let loaded = reader.read_bitmap_index(group_col)?;
                                    io_col += 1;
                                    bitmap_cache.insert(group_col.clone(), loaded.clone());
                                    loaded
                                };
                                for (value, group_bitmap) in group_bitmaps {
                                    let selected = group_bitmap.and(&effective_bitmap);
                                    let count = selected.count_ones();
                                    if count == 0 {
                                        continue;
                                    }
                                    let iter_t = Instant::now();
                                    let entry = grouped.entry(value.to_string()).or_insert((0.0, 0));
                                    let count_t = Instant::now();
                                    entry.0 += count as f64;
                                    entry.1 += count;
                                    detailed.agg_count_ms += count_t.elapsed().as_secs_f64() * 1000.0;
                                    detailed.bitmap_iter_ms += iter_t.elapsed().as_secs_f64() * 1000.0;
                                    detailed.group_emit_ms += iter_t.elapsed().as_secs_f64() * 1000.0;
                                    rows_scanned += count;
                                }
                                continue;
                            }
                            Aggregation::ApproxPercentile(_, _) => unreachable!(),
                        };

                        let iter_t = Instant::now();
                        let sum_t = Instant::now();
                        match (&group_data, &agg_data) {
                            (GroupData::Runs(group_runs), AggData::Runs(value_runs)) => {
                                for (group_key, (sum, count)) in
                                    rle_sum_grouped(group_runs, value_runs, &effective_bitmap)
                                {
                                    let entry = grouped.entry(group_key).or_insert((0.0, 0));
                                    entry.0 += sum;
                                    entry.1 += count;
                                    rows_scanned += count;
                                }
                            }
                            (GroupData::Runs(group_runs), AggData::Array(values)) => {
                                accumulate_grouped_array_by_group_runs(
                                    group_runs,
                                    values,
                                    &effective_bitmap,
                                    &mut grouped,
                                    &mut rows_scanned,
                                );
                            }
                            (GroupData::Array(group_values), AggData::Runs(value_runs)) => {
                                accumulate_grouped_rle_by_group_values(
                                    group_values,
                                    value_runs,
                                    &effective_bitmap,
                                    &mut grouped,
                                    &mut rows_scanned,
                                );
                            }
                            (GroupData::Array(group_values), AggData::Array(values)) => {
                                accumulate_grouped_array_by_group_values(
                                    group_values,
                                    values,
                                    &effective_bitmap,
                                    &mut grouped,
                                    &mut rows_scanned,
                                );
                            }
                            (_, AggData::None) => {}
                        }
                        detailed.bitmap_iter_ms += iter_t.elapsed().as_secs_f64() * 1000.0;
                        detailed.group_emit_ms += iter_t.elapsed().as_secs_f64() * 1000.0;
                        detailed.agg_sum_ms += sum_t.elapsed().as_secs_f64() * 1000.0;
                    } else {
                        match &plan.aggregation {
                            Aggregation::Count => {
                                let count_t = Instant::now();
                                scalar_count += effective_bitmap.count_ones();
                                rows_scanned += effective_bitmap.count_ones();
                                detailed.agg_count_ms += count_t.elapsed().as_secs_f64() * 1000.0;
                            }
                            Aggregation::Sum(col) | Aggregation::Avg(col) => {
                                io_col += 1;
                                let seg_count = effective_bitmap.count_ones();
                                let iter_t = Instant::now();
                                let sum_t = Instant::now();
                                if self.col_rle_compressible(col) {
                                    let runs = reader.read_column_runs_i64(col)?;
                                    scalar_sum += rle_sum_by_bitmap(&runs, &effective_bitmap);
                                } else {
                                    let values = reader.read_column_i64(col)?;
                                    for idx in effective_bitmap.iter_ones() {
                                        scalar_sum += values[idx] as f64;
                                    }
                                }
                                detailed.bitmap_iter_ms += iter_t.elapsed().as_secs_f64() * 1000.0;
                                detailed.agg_sum_ms += sum_t.elapsed().as_secs_f64() * 1000.0;
                                scalar_count += seg_count;
                                rows_scanned += seg_count;
                            }
                            Aggregation::ApproxPercentile(_, _) => unreachable!(),
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
                    let open_ms = open_t.elapsed().as_secs_f64() * 1000.0;
                    stage_open_ms += open_ms;
                    detailed.bitmap_load_ms += open_ms;

                    let agg_t = Instant::now();
                    let eval_t = Instant::now();
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
                            detailed.bitmap_values_touched += 1;
                        }
                    }
                    detailed.bitmap_eval_ms += eval_t.elapsed().as_secs_f64() * 1000.0;
                    // count(B): RLE COUNT — O(runs), no bitmap materialization
                    let rle_t = Instant::now();
                    let mut count_b = 0u64;
                    for run in &runs {
                        let matches = match &rle_filter.op {
                            FilterOp::Eq => run.value == rle_threshold,
                            FilterOp::Gt => run.value > rle_threshold,
                            FilterOp::Lt => run.value < rle_threshold,
                            FilterOp::Ge => run.value >= rle_threshold,
                            FilterOp::Le => run.value <= rle_threshold,
                        };
                        if matches {
                            count_b += run.length as u64;
                        }
                    }
                    detailed.rle_scan_ms += rle_t.elapsed().as_secs_f64() * 1000.0;
                    // count(A AND B): lockstep walk of RLE runs against each matching bitmap
                    let mut count_ab = 0u64;
                    let combine_t = Instant::now();
                    for bmap in &matching_bitmaps {
                        count_ab += rle_count_matching_in_bitmap(&runs, bmap, &rle_filter.op, rle_threshold);
                    }
                    detailed.bitmap_combine_ms += combine_t.elapsed().as_secs_f64() * 1000.0;
                    let seg_count = count_a + count_b - count_ab;
                    let count_t = Instant::now();
                    scalar_count += seg_count;
                    rows_matched += seg_count;
                    detailed.bitmap_match_rows += seg_count;
                    rows_scanned += reader.metadata.row_count;
                    detailed.agg_count_ms += count_t.elapsed().as_secs_f64() * 1000.0;
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
                    let open_ms = open_t.elapsed().as_secs_f64() * 1000.0;
                    stage_open_ms += open_ms;
                    detailed.bitmap_load_ms += open_ms;

                    let agg_t = Instant::now();
                    let eval_t = Instant::now();
                    // Build bitmap for A
                    let row_count = reader.metadata.row_count as usize;
                    let mut bmp_a = Bitmap::new(row_count);
                    for (val, bmap) in &bitmaps_a {
                        let include = match &fa.op {
                            FilterOp::Eq => *val == ta, FilterOp::Gt => *val > ta,
                            FilterOp::Lt => *val < ta, FilterOp::Ge => *val >= ta,
                            FilterOp::Le => *val <= ta,
                        };
                        if include {
                            bmp_a = bmp_a.or(bmap);
                            detailed.bitmap_values_touched += 1;
                        }
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
                        if include {
                            bmp_b = bmp_b.or(bmap);
                            detailed.bitmap_values_touched += 1;
                        }
                    }
                    detailed.bitmap_eval_ms += eval_t.elapsed().as_secs_f64() * 1000.0;
                    // count(B AND C): lockstep RLE walk against bmp_b
                    let rle_t = Instant::now();
                    let count_bc = rle_count_matching_in_bitmap(&runs_c, &bmp_b, &fc.op, tc);
                    detailed.rle_scan_ms += rle_t.elapsed().as_secs_f64() * 1000.0;
                    // count(A AND B AND C): lockstep RLE walk against bmp_a AND bmp_b
                    let combine_t = Instant::now();
                    let bmp_ab = bmp_a.and(&bmp_b);
                    let count_abc = rle_count_matching_in_bitmap(&runs_c, &bmp_ab, &fc.op, tc);
                    detailed.bitmap_combine_ms += combine_t.elapsed().as_secs_f64() * 1000.0;

                    let seg_count = count_a + count_bc - count_abc;
                    let count_t = Instant::now();
                    scalar_count += seg_count;
                    rows_matched += seg_count;
                    detailed.bitmap_match_rows += seg_count;
                    rows_scanned += reader.metadata.row_count;
                    detailed.agg_count_ms += count_t.elapsed().as_secs_f64() * 1000.0;
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
                        let scan_ms = filter_t.elapsed().as_secs_f64() * 1000.0;
                        stage_filter_ms += scan_ms;
                        detailed.rle_scan_ms += scan_ms;
                        detailed.agg_count_ms += scan_ms;
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
                                    Aggregation::Count => {
                                        let count_t = Instant::now();
                                        scalar_count += len;
                                        detailed.agg_count_ms += count_t.elapsed().as_secs_f64() * 1000.0;
                                    }
                                    Aggregation::Sum(_) | Aggregation::Avg(_) => {
                                        let sum_t = Instant::now();
                                        if let Some(agg_data) = &agg_col_data {
                                            for r in row_offset..row_offset + len as usize {
                                                scalar_sum += agg_data[r] as f64;
                                            }
                                            scalar_count += len;
                                        } else {
                                            scalar_sum += run.value as f64 * len as f64;
                                            scalar_count += len;
                                        }
                                        detailed.agg_sum_ms += sum_t.elapsed().as_secs_f64() * 1000.0;
                                    }
                                    Aggregation::ApproxPercentile(_, _) => unreachable!(),
                                }
                            }
                            row_offset += len as usize;
                        }
                        let scan_ms = filter_t.elapsed().as_secs_f64() * 1000.0;
                        stage_filter_ms += scan_ms;
                        detailed.rle_scan_ms += scan_ms;
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
                        Aggregation::ApproxPercentile(_, _) => unreachable!(),
                    };
                    let sf_offset = plan.offset.unwrap_or(0);
                    let sf_limit = plan.limit;
                    let mut sf_matched: u64 = 0;
                    'scan_fallback: for row_idx in 0..row_count {
                        let matched = parsed_filter.as_ref().map_or(true, |expr| {
                            eval_preparsed(expr, &|column| {
                                i64_cols.get(column).map(|vals| vals[row_idx])
                            })
                        });
                        if !matched {
                            continue;
                        }
                        if sf_matched < sf_offset {
                            sf_matched += 1;
                            continue;
                        }
                        if let Some(lim) = sf_limit {
                            if sf_matched >= sf_offset + lim {
                                break 'scan_fallback;
                            }
                        }
                        sf_matched += 1;
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
                                        let count_t = Instant::now();
                                        entry.0 += 1.0;
                                        entry.1 += 1;
                                        detailed.agg_count_ms += count_t.elapsed().as_secs_f64() * 1000.0;
                                    }
                                    Aggregation::Sum(_) | Aggregation::Avg(_) => {
                                        let sum_t = Instant::now();
                                        let value = agg_values.map(|vals| vals[row_idx] as f64).unwrap_or(0.0);
                                        entry.0 += value;
                                        entry.1 += 1;
                                        detailed.agg_sum_ms += sum_t.elapsed().as_secs_f64() * 1000.0;
                                    }
                                    Aggregation::ApproxPercentile(_, _) => unreachable!(),
                                }
                            }
                            None => match &plan.aggregation {
                                Aggregation::Count => {
                                    let count_t = Instant::now();
                                    scalar_count += 1;
                                    detailed.agg_count_ms += count_t.elapsed().as_secs_f64() * 1000.0;
                                }
                                Aggregation::Sum(_) | Aggregation::Avg(_) => {
                                    let sum_t = Instant::now();
                                    scalar_sum += agg_values.map(|vals| vals[row_idx] as f64).unwrap_or(0.0);
                                    scalar_count += 1;
                                    detailed.agg_sum_ms += sum_t.elapsed().as_secs_f64() * 1000.0;
                                }
                                Aggregation::ApproxPercentile(_, _) => unreachable!(),
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
                            Aggregation::ApproxPercentile(_, _) => unreachable!(),
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
                Aggregation::ApproxPercentile(_, _) => unreachable!(),
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
                ColumnStrategy::GroupStatsScalar => "group_stats_scalar",
                ColumnStrategy::RangeBlocks => "range_blocks",
                ColumnStrategy::DecomposedOrCount => "decomposed_or_count",
                ColumnStrategy::BitmapScalar => "bitmap_scalar",
                ColumnStrategy::BitmapGroup => "bitmap_group",
                ColumnStrategy::RleScan => "rle_scan",
                ColumnStrategy::InclusionExclusionCount => "ie_count",
                ColumnStrategy::InclusionExclusionCount3 => "ie_count3",
                ColumnStrategy::ScanFallback => "scan_fallback",
                ColumnStrategy::MetadataFastPath => "metadata",
            },
            &detailed,
        );

        if detailed.segments_pruned_by_stats > 0 {
            eprintln!(
                "[PRUNE] segments_pruned_by_stats={}",
                detailed.segments_pruned_by_stats
            );
        }

        let next_offset = plan.limit.map(|lim| plan.offset.unwrap_or(0) + lim);
        Ok(QueryResult {
            value,
            confidence: ConfidenceFlag::Exact,
            warnings: vec![],
            storage_path: StoragePath::Columnar,
            rows_scanned,
            sampling_rate: 1.0,
            estimated_variance: 0.0,
            profile,
            aqp: None,
            next_offset,
        })
    }

    fn choose_strategy(&self, plan: &QueryPlan) -> StrategyChoice {
        if plan.filter.is_none() && plan.group_by.is_none() {
            return StrategyChoice {
                strategy: ColumnStrategy::MetadataFastPath,
                reason: "metadata_fastpath".to_string(),
                estimate: PredicateEstimate::default(),
                path_cost: 0.0,
            };
        }
        if plan.group_by.is_none()
            && self.group_stats_scalar_ready(plan)
        {
            return StrategyChoice {
                strategy: ColumnStrategy::GroupStatsScalar,
                reason: "group_stats_scalar".to_string(),
                estimate: PredicateEstimate::default(),
                path_cost: 0.0,
            };
        }

        let estimate = plan
            .filter
            .as_ref()
            .map(|expr| self.estimate_predicate(expr))
            .unwrap_or_default();
        let est_path_cost =
            estimate.match_ratio * self.total_rows() as f64 + estimate.values_touched as f64 * 1024.0;

        if let Some(group_col) = &plan.group_by {
            if self
                .segments
                .iter()
                .all(|segment| segment.metadata.columns.get(group_col).and_then(|c| c.index("bitmap_eq")).is_some())
                && (plan.filter.is_none() || self.bitmap_ready_expr(plan.filter.as_ref()))
            {
                return StrategyChoice {
                    strategy: ColumnStrategy::BitmapGroup,
                    reason: "bitmap_group_preferred".to_string(),
                    estimate,
                    path_cost: est_path_cost,
                };
            }
        }
        if plan.group_by.is_none()
            && matches!(plan.aggregation, Aggregation::Count)
            && self.inclusion_exclusion_ready(plan.filter.as_ref())
        {
            return StrategyChoice {
                strategy: ColumnStrategy::InclusionExclusionCount,
                reason: "or_inclusion_exclusion".to_string(),
                estimate,
                path_cost: est_path_cost,
            };
        }
        if plan.group_by.is_none()
            && matches!(plan.aggregation, Aggregation::Count)
            && self.ie3_ready(plan.filter.as_ref())
        {
            return StrategyChoice {
                strategy: ColumnStrategy::InclusionExclusionCount3,
                reason: "or_inclusion_exclusion3".to_string(),
                estimate,
                path_cost: est_path_cost,
            };
        }
        if plan.group_by.is_none()
            && matches!(plan.aggregation, Aggregation::Count)
            && self.range_blocks_ready(plan)
        {
            return StrategyChoice {
                strategy: ColumnStrategy::RangeBlocks,
                reason: "range_blocks".to_string(),
                estimate,
                path_cost: est_path_cost,
            };
        }
        let bitmap_ready = plan.group_by.is_none() && self.bitmap_ready_expr(plan.filter.as_ref());
        let broad_or = estimate.has_or && estimate.match_ratio >= 0.45;
        let high_match_ratio = estimate.match_ratio >= 0.60;
        let prefer_dense_non_bitmap = broad_or || high_match_ratio;
        let can_dense_rle = plan.group_by.is_none()
            && self.rle_scan_ready(plan)
            && (estimate.match_ratio >= 0.30 || estimate.has_range);

        if can_dense_rle && prefer_dense_non_bitmap {
            return StrategyChoice {
                strategy: ColumnStrategy::RleScan,
                reason: "planner_choose_rle_scan_dense".to_string(),
                estimate,
                path_cost: est_path_cost,
            };
        }
        if bitmap_ready && !prefer_dense_non_bitmap {
            let reason = if estimate.has_range {
                "bitmap_scalar_range_selective"
            } else {
                "bitmap_scalar_selective"
            };
            return StrategyChoice {
                strategy: ColumnStrategy::BitmapScalar,
                reason: reason.to_string(),
                estimate,
                path_cost: est_path_cost,
            };
        }
        if bitmap_ready {
            let reason = if broad_or {
                "bitmap_scalar_broad_or_fallback_unavailable"
            } else if high_match_ratio {
                "bitmap_scalar_dense_fallback_unavailable"
            } else if estimate.has_range {
                "bitmap_scalar_range_selective"
            } else {
                "bitmap_scalar_selective"
            };
            return StrategyChoice {
                strategy: ColumnStrategy::BitmapScalar,
                reason: reason.to_string(),
                estimate,
                path_cost: est_path_cost,
            };
        }
        if plan.group_by.is_none()
            && matches!(plan.aggregation, Aggregation::Count)
            && self.decomposed_or_ready(plan.filter.as_ref())
            && !self.bitmap_ready_expr(plan.filter.as_ref())
            && !self.inclusion_exclusion_ready(plan.filter.as_ref())
            && !self.ie3_ready(plan.filter.as_ref())
        {
            return StrategyChoice {
                strategy: ColumnStrategy::DecomposedOrCount,
                reason: "count_or_decompose_fallback".to_string(),
                estimate,
                path_cost: est_path_cost,
            };
        }
        if plan.group_by.is_none() && self.rle_scan_ready(plan) {
            return StrategyChoice {
                strategy: ColumnStrategy::RleScan,
                reason: if prefer_dense_non_bitmap {
                    "planner_choose_rle_scan_dense".to_string()
                } else {
                    "timestamp_rle_preferred".to_string()
                },
                estimate,
                path_cost: est_path_cost,
            };
        }
        let reason = if prefer_dense_non_bitmap && estimate.has_or {
            "planner_reject_broad_or"
        } else if prefer_dense_non_bitmap {
            "planner_reject_high_match_ratio"
        } else if plan.filter.is_some() {
            "scan_fallback_non_bitmap_leaf"
        } else {
            "scan_fallback_no_index"
        };
        StrategyChoice {
            strategy: ColumnStrategy::ScanFallback,
            reason: reason.to_string(),
            estimate,
            path_cost: est_path_cost,
        }
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
        let r_is_rle_range = self.col_rle_compressible(&rf.column) && self.segments.iter().all(|s| {
            s.metadata.columns.get(&rf.column)
                .map(|c| c.encoding == "rle" && c.index("bitmap_eq").is_none())
                .unwrap_or(false)
        });
        if l_has_bitmap && r_is_rle_range {
            return Some((lf, rf));
        }
        let r_has_bitmap = self.segments.iter().all(|s| {
            s.metadata.columns.get(&rf.column).and_then(|c| c.index("bitmap_eq")).is_some()
        });
        let l_is_rle_range = self.col_rle_compressible(&lf.column) && self.segments.iter().all(|s| {
            s.metadata.columns.get(&lf.column)
                .map(|c| c.encoding == "rle" && c.index("bitmap_eq").is_none())
                .unwrap_or(false)
        });
        if r_has_bitmap && l_is_rle_range {
            return Some((rf, lf));
        }
        None
    }

    fn inclusion_exclusion_ready(&self, filter: Option<&PredicateExpr>) -> bool {
        filter
            .map(|f| self.split_or_for_inclusion_exclusion(f).is_some())
            .unwrap_or(false)
    }

    fn simple_filter<'a>(&self, plan: &'a QueryPlan) -> Option<&'a Filter> {
        match plan.filter.as_ref() {
            Some(PredicateExpr::Comparison(filter)) => Some(filter),
            _ => None,
        }
    }

    fn group_stats_scalar_ready(&self, plan: &QueryPlan) -> bool {
        let Some(filter) = self.simple_filter(plan) else {
            return false;
        };
        matches!(filter.op, FilterOp::Eq)
            && matches!(plan.aggregation, Aggregation::Sum(_) | Aggregation::Avg(_))
            && self.segments.iter().all(|segment| {
                segment
                    .metadata
                    .columns
                    .get(&filter.column)
                    .and_then(|c| c.index("group_stats"))
                    .is_some()
            })
    }

    fn range_blocks_ready(&self, plan: &QueryPlan) -> bool {
        let Some(filter) = self.simple_filter(plan) else {
            return false;
        };
        self.segments.iter().all(|segment| {
            segment
                .metadata
                .columns
                .get(&filter.column)
                .and_then(|c| c.index("range_blocks"))
                .is_some()
        })
    }

    fn total_rows(&self) -> u64 {
        self.segments.iter().map(|s| s.metadata.row_count).sum()
    }

    fn estimate_predicate(&self, expr: &PredicateExpr) -> PredicateEstimate {
        match expr {
            PredicateExpr::Comparison(filter) => self.estimate_filter(filter),
            PredicateExpr::Not(inner) => {
                let inner_est = self.estimate_predicate(inner);
                PredicateEstimate {
                    match_ratio: (1.0 - inner_est.match_ratio).clamp(0.0, 1.0),
                    values_touched: inner_est.values_touched,
                    has_or: inner_est.has_or,
                    has_range: inner_est.has_range,
                }
            }
            PredicateExpr::And(left, right) => {
                let left_est = self.estimate_predicate(left);
                let right_est = self.estimate_predicate(right);
                PredicateEstimate {
                    match_ratio: (left_est.match_ratio * right_est.match_ratio).clamp(0.0, 1.0),
                    values_touched: left_est.values_touched + right_est.values_touched,
                    has_or: left_est.has_or || right_est.has_or,
                    has_range: left_est.has_range || right_est.has_range,
                }
            }
            PredicateExpr::Or(left, right) => {
                let left_est = self.estimate_predicate(left);
                let right_est = self.estimate_predicate(right);
                let union_ratio = (left_est.match_ratio + right_est.match_ratio
                    - left_est.match_ratio * right_est.match_ratio)
                    .clamp(0.0, 1.0);
                PredicateEstimate {
                    match_ratio: union_ratio,
                    values_touched: left_est.values_touched + right_est.values_touched,
                    has_or: true,
                    has_range: left_est.has_range || right_est.has_range,
                }
            }
        }
    }

    fn estimate_filter(&self, filter: &Filter) -> PredicateEstimate {
        let target = filter.value.parse::<i64>().unwrap_or(0);
        let mut weighted_ratio = 0.0;
        let mut total_rows = 0u64;
        let mut values_touched = 0u64;

        for segment in &self.segments {
            let Some(col_meta) = segment.metadata.columns.get(&filter.column) else {
                continue;
            };
            let seg_rows = segment.metadata.row_count;
            total_rows += seg_rows;

            let ratio = if let Some(bitmap_meta) = col_meta.index("bitmap_eq") {
                let values = &bitmap_meta.values;
                let touched = values
                    .iter()
                    .filter(|value| match filter.op {
                        FilterOp::Eq => **value == target,
                        FilterOp::Gt => **value > target,
                        FilterOp::Lt => **value < target,
                        FilterOp::Ge => **value >= target,
                        FilterOp::Le => **value <= target,
                    })
                    .count() as u64;
                values_touched += touched;
                match filter.op {
                    FilterOp::Eq => self.estimate_eq_ratio(segment, &filter.column, target),
                    FilterOp::Gt | FilterOp::Lt | FilterOp::Ge | FilterOp::Le => {
                        self.estimate_range_ratio(col_meta, &filter.op, target)
                    }
                }
            } else if col_meta.encoding == "rle" || col_meta.encoding == "bincode" {
                self.estimate_range_ratio(col_meta, &filter.op, target)
            } else {
                1.0
            };

            weighted_ratio += ratio * seg_rows as f64;
        }

        PredicateEstimate {
            match_ratio: if total_rows > 0 {
                (weighted_ratio / total_rows as f64).clamp(0.0, 1.0)
            } else {
                0.0
            },
            values_touched,
            has_or: false,
            has_range: !matches!(filter.op, FilterOp::Eq),
        }
    }

    fn estimate_eq_ratio(&self, segment: &SegmentEntry, column: &str, target: i64) -> f64 {
        let Some(col_meta) = segment.metadata.columns.get(column) else {
            return 1.0;
        };
        if let Some(bitmap_meta) = col_meta.index("bitmap_eq") {
            let total = bitmap_meta.values.len() as u64;
            if total > 0 {
                let matching = bitmap_meta.values.iter().filter(|&&v| v == target).count() as u64;
                return (matching as f64 / total as f64).clamp(0.0, 1.0);
            }
        }

        let hll_estimate = if col_meta.index("hll").is_some() {
            let reader = ColumnarReader {
                path: segment.path.clone(),
                metadata: segment.metadata.clone(),
                seg_idx: 0,
            };
            reader.read_hll(column).ok().map(|hll| hll.estimate().round() as u64)
        } else {
            None
        };
        let distinct = hll_estimate
            .unwrap_or(col_meta.distinct_count)
            .max(col_meta.index("bitmap_eq").map(|idx| idx.values.len() as u64).unwrap_or(0))
            .max(1);
        1.0 / distinct as f64
    }

    fn estimate_range_ratio(
        &self,
        col_meta: &crate::types::ColumnMetadata,
        op: &FilterOp,
        target: i64,
    ) -> f64 {
        if let Some(bitmap_meta) = col_meta.index("bitmap_eq") {
            let total = bitmap_meta.values.len();
            if total > 0 {
                let touched = bitmap_meta
                    .values
                    .iter()
                    .filter(|value| match op {
                        FilterOp::Eq => **value == target,
                        FilterOp::Gt => **value > target,
                        FilterOp::Lt => **value < target,
                        FilterOp::Ge => **value >= target,
                        FilterOp::Le => **value <= target,
                    })
                    .count();
                return touched as f64 / total as f64;
            }
        }

        let min = col_meta.min;
        let max = col_meta.max;
        if max <= min {
            return 1.0;
        }
        let target = target as f64;
        match op {
            FilterOp::Eq => (1.0 / col_meta.distinct_count.max(1) as f64).clamp(0.0, 1.0),
            FilterOp::Gt => ((max - target) / (max - min)).clamp(0.0, 1.0),
            FilterOp::Lt => ((target - min) / (max - min)).clamp(0.0, 1.0),
            FilterOp::Ge => ((max - target + 1.0) / (max - min + 1.0)).clamp(0.0, 1.0),
            FilterOp::Le => ((target - min + 1.0) / (max - min + 1.0)).clamp(0.0, 1.0),
        }
    }

    // Returns true when the RLE column has genuinely long runs (avg >= 16 rows/run),
    // meaning rle_sum_by_bitmap / lockstep walks beat scatter-reading a decoded array.
    // Requires run_count > 0 in metadata (written by new segments; old segments return false).
    fn col_rle_compressible(&self, col: &str) -> bool {
        self.segments.iter().all(|s| {
            s.metadata.columns.get(col).map(|c| {
                c.encoding == "rle"
                    && c.run_count > 0
                    && s.metadata.row_count / c.run_count >= 16
            }).unwrap_or(false)
        })
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
            // The rle arm must have genuinely long runs so the lockstep walk is cheap.
            let c_is_rle = self.col_rle_compressible(&fc.column)
                && self.segments.iter().all(|s| {
                    s.metadata.columns.get(&fc.column)
                        .map(|c| c.index("bitmap_eq").is_none())
                        .unwrap_or(false)
                });
            if a_has_bitmap && b_has_bitmap && c_is_rle {
                return Some((fa, fb, fc));
            }
            // Also try And(rle_leaf, bitmap_leaf) order
            let b_is_rle = self.col_rle_compressible(&fb.column)
                && self.segments.iter().all(|s| {
                    s.metadata.columns.get(&fb.column)
                        .map(|c| c.index("bitmap_eq").is_none())
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

    fn decomposed_or_ready(&self, filter: Option<&PredicateExpr>) -> bool {
        matches!(filter, Some(PredicateExpr::Or(_, _)))
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

    fn count_expr_exact(
        &self,
        expr: &PredicateExpr,
        reader: &ColumnarReader,
        io_col: &mut u64,
        detailed: &mut DetailedProfile,
    ) -> Result<u64, QueryError> {
        match expr {
            PredicateExpr::Comparison(filter) => {
                self.count_filter_exact(filter, reader, io_col, detailed)
            }
            PredicateExpr::And(left, right) => {
                let combined = PredicateExpr::And(left.clone(), right.clone());
                if self.bitmap_ready_expr(Some(&combined)) {
                    let mut cache = HashMap::new();
                    let bitmap = self
                        .eval_bitmap_expr(Some(&combined), reader, &mut cache, detailed)?
                        .unwrap_or_else(|| Bitmap::full(reader.metadata.row_count as usize));
                    *io_col += cache.len() as u64;
                    Ok(bitmap.count_ones())
                } else {
                    self.count_expr_scan(&combined, reader, io_col, detailed)
                }
            }
            PredicateExpr::Or(left, right) => {
                let left_count = self.count_expr_exact(left, reader, io_col, detailed)?;
                let right_count = self.count_expr_exact(right, reader, io_col, detailed)?;
                let intersection = PredicateExpr::And(left.clone(), right.clone());
                let both_count =
                    self.count_expr_exact(&intersection, reader, io_col, detailed)?;
                Ok(left_count + right_count - both_count)
            }
            PredicateExpr::Not(inner) => {
                let inner_count = self.count_expr_exact(inner, reader, io_col, detailed)?;
                Ok(reader.metadata.row_count.saturating_sub(inner_count))
            }
        }
    }

    fn count_filter_exact(
        &self,
        filter: &Filter,
        reader: &ColumnarReader,
        io_col: &mut u64,
        detailed: &mut DetailedProfile,
    ) -> Result<u64, QueryError> {
        let col_meta = reader
            .metadata
            .columns
            .get(&filter.column)
            .ok_or_else(|| QueryError::UnknownColumn(filter.column.clone()))?;
        let target = filter
            .value
            .parse::<i64>()
            .map_err(|_| QueryError::ParseError(format!("invalid literal {}", filter.value)))?;

        if let Some(bitmap_meta) = col_meta.index("bitmap_eq") {
            let load_t = Instant::now();
            let index = reader.read_bitmap_index(&filter.column)?;
            detailed.bitmap_load_ms += load_t.elapsed().as_secs_f64() * 1000.0;
            *io_col += 1;

            let eval_t = Instant::now();
            let mut count = 0u64;
            for value in &bitmap_meta.values {
                let include = match filter.op {
                    FilterOp::Eq => *value == target,
                    FilterOp::Gt => *value > target,
                    FilterOp::Lt => *value < target,
                    FilterOp::Ge => *value >= target,
                    FilterOp::Le => *value <= target,
                };
                if include && let Some(bitmap) = index.get(value) {
                    count += bitmap.count_ones();
                    detailed.bitmap_values_touched += 1;
                }
            }
            if matches!(filter.op, FilterOp::Eq) && (index.is_empty() || count == 0) {
                eprintln!(
                    "[BITMAP_DEBUG] col={} target={} bitmap_values={:?} index_keys={:?}",
                    filter.column,
                    target,
                    Some(&bitmap_meta.values),
                    index.keys().take(5).collect::<Vec<_>>()
                );
            }
            detailed.bitmap_eval_ms += eval_t.elapsed().as_secs_f64() * 1000.0;
            return Ok(count);
        }

        if col_meta.index("range_blocks").is_some() {
            let open_t = Instant::now();
            let blocks = reader.read_range_blocks(&filter.column)?;
            let encoded = reader.read_delta_encoded_i64(&filter.column)?;
            let open_ms = open_t.elapsed().as_secs_f64() * 1000.0;
            detailed.range_index_ms += open_ms;
            *io_col += 1;
            let search_t = Instant::now();
            let count = delta_range_count(&encoded, &blocks, &filter.op, target);
            detailed.range_search_ms += search_t.elapsed().as_secs_f64() * 1000.0;
            return Ok(count);
        }

        if col_meta.encoding == "rle" {
            let load_t = Instant::now();
            let runs = reader.read_column_runs_i64(&filter.column)?;
            detailed.bitmap_load_ms += load_t.elapsed().as_secs_f64() * 1000.0;
            *io_col += 1;

            let scan_t = Instant::now();
            let mut count = 0u64;
            for run in &runs {
                let include = match filter.op {
                    FilterOp::Eq => run.value == target,
                    FilterOp::Gt => run.value > target,
                    FilterOp::Lt => run.value < target,
                    FilterOp::Ge => run.value >= target,
                    FilterOp::Le => run.value <= target,
                };
                if include {
                    count += run.length as u64;
                }
            }
            let scan_ms = scan_t.elapsed().as_secs_f64() * 1000.0;
            detailed.rle_scan_ms += scan_ms;
            detailed.bitmap_eval_ms += scan_ms;
            return Ok(count);
        }

        if col_meta.encoding == "bincode" {
            let load_t = Instant::now();
            let raw = reader.read_column_i64_raw_slice(&filter.column)?;
            detailed.bitmap_load_ms += load_t.elapsed().as_secs_f64() * 1000.0;
            *io_col += 1;

            let scan_t = Instant::now();
            let mut count = 0u64;
            for chunk in raw.chunks_exact(8) {
                let value = i64::from_le_bytes(chunk.try_into().unwrap());
                let include = match filter.op {
                    FilterOp::Eq => value == target,
                    FilterOp::Gt => value > target,
                    FilterOp::Lt => value < target,
                    FilterOp::Ge => value >= target,
                    FilterOp::Le => value <= target,
                };
                count += include as u64;
            }
            let scan_ms = scan_t.elapsed().as_secs_f64() * 1000.0;
            detailed.rle_scan_ms += scan_ms;
            detailed.bitmap_eval_ms += scan_ms;
            return Ok(count);
        }

        self.count_expr_scan(&PredicateExpr::Comparison(filter.clone()), reader, io_col, detailed)
    }

    fn segment_pruned_by_stats(&self, segment: &SegmentEntry, expr: &PredicateExpr) -> bool {
        match expr {
            PredicateExpr::Comparison(filter) => {
                let Some(col_meta) = segment.metadata.columns.get(&filter.column) else {
                    return false;
                };
                let Ok(target) = filter.value.parse::<i64>() else {
                    return false;
                };
                let min = col_meta.min as i64;
                let max = col_meta.max as i64;
                match filter.op {
                    FilterOp::Gt | FilterOp::Ge if target >= max => true,
                    FilterOp::Lt | FilterOp::Le if target <= min => true,
                    FilterOp::Eq if target < min || target > max => true,
                    _ => false,
                }
            }
            PredicateExpr::And(left, right) => {
                self.segment_pruned_by_stats(segment, left)
                    || self.segment_pruned_by_stats(segment, right)
            }
            PredicateExpr::Or(..) | PredicateExpr::Not(_) => false,
        }
    }

    fn count_expr_scan(
        &self,
        expr: &PredicateExpr,
        reader: &ColumnarReader,
        io_col: &mut u64,
        detailed: &mut DetailedProfile,
    ) -> Result<u64, QueryError> {
        let mut columns = Vec::new();
        expr.collect_columns(&mut columns);
        let mut i64_cols = HashMap::new();
        for column in &columns {
            if let Ok(values) = reader.read_column_i64(column) {
                *io_col += 1;
                i64_cols.insert(column.clone(), values);
            }
        }
        let parsed = preparse_filter(expr);
        let filter_t = Instant::now();
        let row_count = i64_cols
            .values()
            .next()
            .map(|vals| vals.len())
            .unwrap_or(reader.metadata.row_count as usize);
        let mut count = 0u64;
        for row_idx in 0..row_count {
            if eval_preparsed(&parsed, &|column| i64_cols.get(column).map(|vals| vals[row_idx])) {
                count += 1;
            }
        }
        detailed.agg_count_ms += filter_t.elapsed().as_secs_f64() * 1000.0;
        Ok(count)
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
        detailed: &mut DetailedProfile,
    ) -> Result<Option<Bitmap>, QueryError> {
        expr.map(|expr| self.eval_bitmap_node(expr, reader, cache, detailed))
            .transpose()
    }

    fn eval_bitmap_node(
        &self,
        expr: &PredicateExpr,
        reader: &ColumnarReader,
        cache: &mut HashMap<String, HashMap<i64, Bitmap>>,
        detailed: &mut DetailedProfile,
    ) -> Result<Bitmap, QueryError> {
        match expr {
            PredicateExpr::Comparison(filter) => self.bitmap_for_filter(filter, reader, cache, detailed),
            PredicateExpr::Not(inner) => {
                let combine_t = Instant::now();
                let result = self.eval_bitmap_node(inner, reader, cache, detailed)?.not();
                detailed.bitmap_combine_ms += combine_t.elapsed().as_secs_f64() * 1000.0;
                Ok(result)
            }
            PredicateExpr::And(left, right) => {
                let mut left_bmp = self.eval_bitmap_node(left, reader, cache, detailed)?;
                let right_bmp = self.eval_bitmap_node(right, reader, cache, detailed)?;
                let combine_t = Instant::now();
                left_bmp.and_inplace(&right_bmp);
                detailed.bitmap_combine_ms += combine_t.elapsed().as_secs_f64() * 1000.0;
                Ok(left_bmp)
            }
            PredicateExpr::Or(left, right) => {
                let mut left_bmp = self.eval_bitmap_node(left, reader, cache, detailed)?;
                // Short-circuit: if left already matches all rows, skip evaluating right.
                if left_bmp.count_ones() as u64 == reader.metadata.row_count {
                    return Ok(left_bmp);
                }
                let right_bmp = self.eval_bitmap_node(right, reader, cache, detailed)?;
                let combine_t = Instant::now();
                left_bmp.or_inplace(&right_bmp);
                detailed.bitmap_combine_ms += combine_t.elapsed().as_secs_f64() * 1000.0;
                Ok(left_bmp)
            }
        }
    }

    fn bitmap_for_filter(
        &self,
        filter: &Filter,
        reader: &ColumnarReader,
        cache: &mut HashMap<String, HashMap<i64, Bitmap>>,
        detailed: &mut DetailedProfile,
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
            let load_t = Instant::now();
            let index = if let Some(existing) = cache.get(&filter.column) {
                existing.clone()
            } else {
                let loaded = reader.read_bitmap_index(&filter.column).map_err(QueryError::StorageError)?;
                cache.insert(filter.column.clone(), loaded.clone());
                loaded
            };
            detailed.bitmap_load_ms += load_t.elapsed().as_secs_f64() * 1000.0;

            let eval_t = Instant::now();
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
                        bitmap.or_inplace(part);
                        detailed.bitmap_values_touched += 1;
                    }
                }
            }
            detailed.bitmap_eval_ms += eval_t.elapsed().as_secs_f64() * 1000.0;
            return Ok(bitmap);
        }

        // Fallback: build bitmap from RLE runs (O(runs), no index required).
        let row_count = reader.metadata.row_count as usize;
        let load_t = Instant::now();
        let runs = reader.read_column_runs_i64(&filter.column)?;
        detailed.bitmap_load_ms += load_t.elapsed().as_secs_f64() * 1000.0;
        let eval_t = Instant::now();
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
        let eval_ms = eval_t.elapsed().as_secs_f64() * 1000.0;
        detailed.bitmap_eval_ms += eval_ms;
        detailed.rle_scan_ms += eval_ms;
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
        detailed: &DetailedProfile,
    ) {
        let q = match (&plan.aggregation, plan.group_by.is_some()) {
            (Aggregation::Count, true) => "Q1",
            (Aggregation::Sum(_), false) => "Q2",
            (Aggregation::Count, false) => "Q3",
            _ => "QX",
        };
        let match_ratio = if rows_total > 0 {
            detailed.bitmap_match_rows as f64 / rows_total as f64
        } else {
            0.0
        };
        println!(
            "[PROFILE] query={q} total_ms={:.2} stage.col_open_ms={open_ms:.2} stage.decode_ms=0.00 stage.filter_agg_ms={:.2} io.seg={segs} io.col={cols} rows.tot={rows_total} rows.filt={rows_matched} rows.samp={rows_scanned} rate.sst={rate:.4} rate.row={rate:.4} mfp={} parallel=true strategy={strategy} bitmap.load_ms={:.2} bitmap.eval_ms={:.2} bitmap.combine_ms={:.2} bitmap.iter_ms={:.2} bitmap.match_rows={} bitmap.match_ratio={:.4} bitmap.values_touched={} bitmap.leaf_count={} group.index_ms={:.2} group.emit_ms={:.2} group.stats_ms={:.2} range.index_ms={:.2} range.search_ms={:.2} rle.scan_ms={:.2} agg.sum_ms={:.2} agg.count_ms={:.2} planner.reason={} planner.est_match_ratio={:.4} planner.est_values_touched={} planner.est_path_cost={:.2} segments.pruned_by_stats={}",
            start.elapsed().as_secs_f64() * 1000.0,
            filter_ms + agg_ms,
            strategy == "metadata",
            detailed.bitmap_load_ms,
            detailed.bitmap_eval_ms,
            detailed.bitmap_combine_ms,
            detailed.bitmap_iter_ms,
            detailed.bitmap_match_rows,
            match_ratio,
            detailed.bitmap_values_touched,
            detailed.bitmap_leaf_count,
            detailed.group_index_ms,
            detailed.group_emit_ms,
            detailed.group_stats_ms,
            detailed.range_index_ms,
            detailed.range_search_ms,
            detailed.rle_scan_ms,
            detailed.agg_sum_ms,
            detailed.agg_count_ms,
            detailed.planner_reason,
            detailed.planner_est_match_ratio,
            detailed.planner_est_values_touched,
            detailed.planner_est_path_cost,
            detailed.segments_pruned_by_stats,
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

fn predicate_leaf_count(expr: &PredicateExpr) -> u64 {
    match expr {
        PredicateExpr::Comparison(_) => 1,
        PredicateExpr::Not(inner) => predicate_leaf_count(inner),
        PredicateExpr::And(left, right) | PredicateExpr::Or(left, right) => {
            predicate_leaf_count(left) + predicate_leaf_count(right)
        }
    }
}

fn delta_range_count(
    encoded: &crate::storage::columnar::encoding::delta::DeltaEncoded<i64>,
    blocks: &crate::types::RangeBlocksData,
    op: &FilterOp,
    target: i64,
) -> u64 {
    let total_rows = encoded.deltas.len() as u64 + if encoded.deltas.is_empty() { 0 } else { 1 };
    if total_rows == 0 {
        return 0;
    }
    match op {
        FilterOp::Eq => {
            let lo = monotonic_lower_bound(encoded, blocks, target);
            let hi = monotonic_upper_bound(encoded, blocks, target);
            hi.saturating_sub(lo) as u64
        }
        FilterOp::Gt => {
            let idx = monotonic_upper_bound(encoded, blocks, target);
            total_rows.saturating_sub(idx as u64)
        }
        FilterOp::Ge => {
            let idx = monotonic_lower_bound(encoded, blocks, target);
            total_rows.saturating_sub(idx as u64)
        }
        FilterOp::Lt => monotonic_lower_bound(encoded, blocks, target) as u64,
        FilterOp::Le => monotonic_upper_bound(encoded, blocks, target) as u64,
    }
}

fn monotonic_lower_bound(
    encoded: &crate::storage::columnar::encoding::delta::DeltaEncoded<i64>,
    blocks: &crate::types::RangeBlocksData,
    target: i64,
) -> usize {
    if blocks.blocks.is_empty() {
        return 0;
    }
    let block_idx = blocks
        .blocks
        .iter()
        .position(|block| block.end_value >= target)
        .unwrap_or(blocks.blocks.len());
    if block_idx >= blocks.blocks.len() {
        return encoded.deltas.len() + 1;
    }
    let block = &blocks.blocks[block_idx];
    let start = block.row_start as usize;
    let end = (start + block.row_count as usize).min(encoded.deltas.len() + 1);
    let mut current = block.start_value;
    if current >= target {
        return start;
    }
    for row in (start + 1)..end {
        current += encoded.deltas[row - 1];
        if current >= target {
            return row;
        }
    }
    end
}

fn monotonic_upper_bound(
    encoded: &crate::storage::columnar::encoding::delta::DeltaEncoded<i64>,
    blocks: &crate::types::RangeBlocksData,
    target: i64,
) -> usize {
    if blocks.blocks.is_empty() {
        return 0;
    }
    let block_idx = blocks
        .blocks
        .iter()
        .position(|block| block.end_value > target)
        .unwrap_or(blocks.blocks.len());
    if block_idx >= blocks.blocks.len() {
        return encoded.deltas.len() + 1;
    }
    let block = &blocks.blocks[block_idx];
    let start = block.row_start as usize;
    let end = (start + block.row_count as usize).min(encoded.deltas.len() + 1);
    let mut current = block.start_value;
    if current > target {
        return start;
    }
    for row in (start + 1)..end {
        current += encoded.deltas[row - 1];
        if current > target {
            return row;
        }
    }
    end
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

fn accumulate_grouped_array_by_group_runs(
    group_runs: &[RleRunI64],
    values: &crate::utils::aligned_vec::AlignedVec<i64>,
    bitmap: &Bitmap,
    grouped: &mut BTreeMap<String, (f64, u64)>,
    rows_scanned: &mut u64,
) {
    let mut row = 0usize;
    for run in group_runs {
        let end = row + run.length as usize;
        let filter_count = bitmap.count_ones_range(row, end);
        if filter_count > 0 {
            let entry = grouped.entry(run.value.to_string()).or_insert((0.0, 0));
            for idx in bitmap.iter_ones_range(row, end) {
                entry.0 += values[idx] as f64;
            }
            entry.1 += filter_count;
            *rows_scanned += filter_count;
        }
        row = end;
    }
}

fn accumulate_grouped_array_by_group_values(
    group_values: &crate::utils::aligned_vec::AlignedVec<i64>,
    values: &crate::utils::aligned_vec::AlignedVec<i64>,
    bitmap: &Bitmap,
    grouped: &mut BTreeMap<String, (f64, u64)>,
    rows_scanned: &mut u64,
) {
    for idx in bitmap.iter_ones() {
        let entry = grouped
            .entry(group_values[idx].to_string())
            .or_insert((0.0, 0));
        entry.0 += values[idx] as f64;
        entry.1 += 1;
        *rows_scanned += 1;
    }
}

fn accumulate_grouped_rle_by_group_values(
    group_values: &crate::utils::aligned_vec::AlignedVec<i64>,
    value_runs: &[RleRunI64],
    bitmap: &Bitmap,
    grouped: &mut BTreeMap<String, (f64, u64)>,
    rows_scanned: &mut u64,
) {
    let mut run_idx = 0usize;
    let mut run_end = value_runs.first().map(|run| run.length as usize).unwrap_or(0);
    let mut run_value = value_runs.first().map(|run| run.value).unwrap_or(0);
    for idx in bitmap.iter_ones() {
        while run_idx < value_runs.len() && idx >= run_end {
            run_idx += 1;
            if run_idx >= value_runs.len() {
                return;
            }
            run_value = value_runs[run_idx].value;
            run_end += value_runs[run_idx].length as usize;
        }
        let entry = grouped
            .entry(group_values[idx].to_string())
            .or_insert((0.0, 0));
        entry.0 += run_value as f64;
        entry.1 += 1;
        *rows_scanned += 1;
    }
}

fn rle_sum_grouped(
    group_runs: &[RleRunI64],
    value_runs: &[RleRunI64],
    filter: &Bitmap,
) -> BTreeMap<String, (f64, u64)> {
    let mut groups: BTreeMap<String, (f64, u64)> = BTreeMap::new();
    let mut gi = 0usize;
    let mut vi = 0usize;
    let mut g_rem = 0u32;
    let mut v_rem = 0u32;
    let mut row = 0usize;
    let mut cur_g = 0i64;
    let mut cur_v = 0i64;

    loop {
        if g_rem == 0 {
            if gi >= group_runs.len() {
                break;
            }
            cur_g = group_runs[gi].value;
            g_rem = group_runs[gi].length;
            gi += 1;
        }
        if v_rem == 0 {
            if vi >= value_runs.len() {
                break;
            }
            cur_v = value_runs[vi].value;
            v_rem = value_runs[vi].length;
            vi += 1;
        }
        let take = g_rem.min(v_rem) as usize;
        let count = filter.count_ones_range(row, row + take);
        if count > 0 {
            let entry = groups.entry(cur_g.to_string()).or_insert((0.0, 0));
            entry.0 += cur_v as f64 * count as f64;
            entry.1 += count;
        }
        g_rem -= take as u32;
        v_rem -= take as u32;
        row += take;
    }

    groups
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::columnar::encoding::delta::DeltaEncoded;
    use crate::types::{RangeBlock, RangeBlocksData};

    #[test]
    fn delta_range_count_uses_block_metadata_exactly() {
        let encoded = DeltaEncoded {
            base: 100,
            deltas: vec![5, 5, 5, 5, 5],
        };
        let blocks = RangeBlocksData {
            blocks: vec![
                RangeBlock {
                    row_start: 0,
                    row_count: 3,
                    start_value: 100,
                    end_value: 110,
                },
                RangeBlock {
                    row_start: 3,
                    row_count: 3,
                    start_value: 115,
                    end_value: 125,
                },
            ],
        };

        assert_eq!(delta_range_count(&encoded, &blocks, &FilterOp::Ge, 115), 3);
        assert_eq!(delta_range_count(&encoded, &blocks, &FilterOp::Gt, 110), 3);
        assert_eq!(delta_range_count(&encoded, &blocks, &FilterOp::Lt, 115), 3);
        assert_eq!(delta_range_count(&encoded, &blocks, &FilterOp::Le, 110), 3);
        assert_eq!(delta_range_count(&encoded, &blocks, &FilterOp::Eq, 120), 1);
    }
}
