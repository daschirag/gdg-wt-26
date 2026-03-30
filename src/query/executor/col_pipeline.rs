use crate::aqp::sampler::col_sampler::ColumnSampler;
use crate::errors::QueryError;
use crate::query::ast::{Aggregation, FilterOp, QueryPlan};
use crate::storage::columnar::encoding::rle::RleEncoder;
use crate::storage::columnar::reader::ColumnarReader;
use crate::types::SSTableMetadata;
use crate::types::{AggregateValue, ConfidenceFlag, QueryResult, StoragePath};
use rand::prelude::*;
use rayon::prelude::*;
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

#[derive(Debug, Clone, Copy, PartialEq)]
enum ScanStrategy {
    RunAggregate,
    LazyGroupDecode,
}

impl ColumnarPipeline {
    pub fn new(segments: Vec<String>) -> Result<Self, crate::errors::StorageError> {
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
            .collect::<Result<Vec<_>, crate::errors::StorageError>>()?;
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
        let overall_profile = crate::types::QueryProfile::default();
        let start_total = Instant::now();

        let mut stage_col_open_ms = 0.0;
        let mut stage_decode_ns = 0u128;
        let mut stage_aggregation_ns = 0u128;
        let mut io_segments_opened = 0u64;
        let mut io_col_files_opened = 0u64;
        let mut io_bytes_read = 0u64;
        let mut io_row_groups_sampled = 0u64;
        let mut rows_total_possible = 0u64;
        let mut rows_after_filter = 0u64;
        let mut scalar_sum = 0.0;
        let mut scalar_count = 0u64;
        let mut group_sums_u8: [f64; 256] = [0.0; 256];
        let mut group_counts_u8: [u64; 256] = [0; 256];
        let mut total_rows_read = 0;
        let global_total_rows_possible: u64 = self
            .segments
            .iter()
            .map(|segment| segment.metadata.row_count)
            .sum();

        let filter_col_idx = plan.filter.as_ref().and_then(|f| {
            config
                .schema
                .columns
                .iter()
                .position(|c| c.name == f.column)
        });
        let group_col_idx = plan
            .group_by
            .as_ref()
            .and_then(|g| config.schema.columns.iter().position(|c| &c.name == g));
        let agg_col_idx = match &plan.aggregation {
            Aggregation::Count => None,
            Aggregation::Sum(col) | Aggregation::Avg(col) => {
                config.schema.columns.iter().position(|c| &c.name == col)
            }
        };

        if plan.filter.is_none() && plan.group_by.is_none() {
            let mut fast_count = 0u64;
            let mut fast_sum = 0.0;
            let mut possible = true;
            for segment in &self.segments {
                let r_start = Instant::now();
                let reader = ColumnarReader {
                    path: segment.path.clone(),
                    metadata: segment.metadata.clone(),
                    seg_idx: 0,
                };
                stage_col_open_ms += r_start.elapsed().as_secs_f64() * 1000.0;
                io_segments_opened += 1;
                fast_count += reader.metadata.row_count;
                match &plan.aggregation {
                    Aggregation::Count => {}
                    Aggregation::Sum(col) | Aggregation::Avg(col) => {
                        if let Some(c) = reader.metadata.columns.get(col) {
                            fast_sum += c.sum;
                        } else if let Some(pos) = agg_col_idx {
                            let alt_key = format!("col_{}", pos);
                            if let Some(c) = reader.metadata.columns.get(&alt_key) {
                                fast_sum += c.sum;
                            } else {
                                possible = false;
                                break;
                            }
                        } else {
                            possible = false;
                            break;
                        }
                    }
                }
            }
            if possible {
                let value = match &plan.aggregation {
                    Aggregation::Count => AggregateValue::Scalar(fast_count as f64),
                    Aggregation::Sum(_) => AggregateValue::Scalar(fast_sum),
                    Aggregation::Avg(_) => {
                        if fast_count > 0 {
                            AggregateValue::Scalar(fast_sum / fast_count as f64)
                        } else {
                            AggregateValue::Empty
                        }
                    }
                };
                self.print_profile(
                    plan,
                    start_total,
                    stage_col_open_ms,
                    0,
                    0,
                    io_segments_opened,
                    0,
                    0,
                    0,
                    fast_count,
                    0,
                    0,
                    1.0,
                    1.0,
                    true,
                    false,
                );
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

        let strategy = if plan.filter.is_some() {
            ScanStrategy::LazyGroupDecode
        } else {
            ScanStrategy::RunAggregate
        };
        let sampling_rate = crate::aqp::accuracy::AccuracyCalculator::calculate_sampling_rate(
            config.accuracy_target,
            config.k,
        );
        let sst_rate = sampling_rate.sqrt();
        let row_rate = sampling_rate.sqrt();

        let sampled_segments: Vec<usize> = if sst_rate >= 1.0 {
            (0..self.segments.len()).collect()
        } else {
            let mut rng = rand::prelude::StdRng::seed_from_u64(config.seed);
            let n = ((sst_rate * self.segments.len() as f64).floor() as usize)
                .max(1)
                .min(self.segments.len());
            let mut indices: Vec<usize> = (0..self.segments.len()).collect();
            indices.shuffle(&mut rng);
            indices.into_iter().take(n).collect()
        };

        struct SegmentScanResult {
            total_rows_read: usize,
            rows_after_filter: u64,
            rows_total_possible: u64,
            scalar_sum: f64,
            scalar_count: u64,
            group_sums_u8: [f64; 256],
            group_counts_u8: [u64; 256],
            io_bytes_read: u64,
            io_col_files_opened: u64,
            io_segments_opened: u64,
            io_row_groups_eval: u64,
            io_row_groups_samp: u64,
            duration_ms: f64,
            stage_agg_ns: u128,
            stage_col_open_ns: u128,
            stage_decode_ns: u128,
            stage_filter_ns: u128,
        }

        let filter_i64_val = plan
            .filter
            .as_ref()
            .and_then(|f| f.value.parse::<i64>().ok());

        let results: Vec<SegmentScanResult> = sampled_segments
            .par_iter()
            .map(|&seg_idx| {
                let mut res = SegmentScanResult {
                    total_rows_read: 0,
                    rows_after_filter: 0,
                    rows_total_possible: 0,
                    scalar_sum: 0.0,
                    scalar_count: 0,
                    group_sums_u8: [0.0; 256],
                    group_counts_u8: [0; 256],
                    io_bytes_read: 0,
                    io_col_files_opened: 0,
                    io_segments_opened: 0,
                    io_row_groups_eval: 0,
                    io_row_groups_samp: 0,
                    duration_ms: 0.0,
                    stage_agg_ns: 0,
                    stage_col_open_ns: 0,
                    stage_decode_ns: 0,
                    stage_filter_ns: 0,
                };
                let start_seg = Instant::now();
                let segment = match self.segments.get(seg_idx) {
                    Some(segment) => segment,
                    None => return res,
                };
                let reader = ColumnarReader {
                    path: segment.path.clone(),
                    metadata: segment.metadata.clone(),
                    seg_idx: seg_idx as u64,
                };
                res.io_segments_opened += 1;
                res.rows_total_possible = reader.metadata.row_count;

                if strategy == ScanStrategy::RunAggregate {
                    if let Some(g_idx) = group_col_idx {
                        let col_name = &config.schema.columns[g_idx].name;
                        let deser_t = Instant::now();
                        if let Ok(mut key_fh) = reader.open_column(col_name) {
                            let key_runs = key_fh.get_runs_i64();
                            res.stage_col_open_ns = deser_t.elapsed().as_nanos();
                            res.io_col_files_opened += 1;
                            match &plan.aggregation {
                                Aggregation::Count => {
                                    let agg_t = Instant::now();
                                    let (counts, sampled) =
                                        RleEncoder::aggregate_counts_i64_sampled(
                                            &key_runs, row_rate,
                                        );
                                    res.stage_agg_ns = agg_t.elapsed().as_nanos();
                                    res.group_counts_u8.copy_from_slice(&counts);
                                    res.scalar_count = counts.iter().sum::<u64>();
                                    res.total_rows_read = sampled as usize;
                                }
                                _ => {
                                    if let Some(a_idx) = agg_col_idx {
                                        let deser2_t = Instant::now();
                                        if let Ok(mut val_fh) =
                                            reader.open_column(&config.schema.columns[a_idx].name)
                                        {
                                            let val_runs = val_fh.get_runs_i64();
                                            res.stage_col_open_ns += deser2_t.elapsed().as_nanos();
                                            res.io_col_files_opened += 1;
                                            let agg_t = Instant::now();
                                            let (sums, counts, sampled) =
                                                RleEncoder::aggregate_sum_by_u8_i64_sampled(
                                                    key_runs, val_runs, row_rate,
                                                );
                                            res.stage_agg_ns = agg_t.elapsed().as_nanos();
                                            res.group_sums_u8.copy_from_slice(&sums);
                                            res.group_counts_u8.copy_from_slice(&counts);
                                            res.scalar_count = counts.iter().sum::<u64>();
                                            res.scalar_sum = sums.iter().sum::<f64>();
                                            res.total_rows_read = sampled as usize;
                                        }
                                    }
                                }
                            }
                        }
                    } else {
                        let sampled = (reader.metadata.row_count as f64 * row_rate).round() as u64;
                        res.scalar_count = sampled;
                        res.total_rows_read = sampled as usize;
                    }
                    res.duration_ms = start_seg.elapsed().as_secs_f64() * 1000.0;
                    return res;
                }

                if strategy == ScanStrategy::LazyGroupDecode {
                    let f_idx = filter_col_idx.unwrap();
                    let op = &plan.filter.as_ref().unwrap().op;
                    let f_val = filter_i64_val.unwrap_or(0);
                    let group_size = config.row_group_size as usize;
                    let num_rows = reader.metadata.row_count as usize;
                    let num_groups = (num_rows + group_size - 1) / group_size;

                    let col_open_t = Instant::now();
                    if let Ok(mut f_fh) = reader.open_column(&config.schema.columns[f_idx].name) {
                        res.io_col_files_opened += 1;
                        let mut a_fh = agg_col_idx.and_then(|idx| {
                            reader.open_column(&config.schema.columns[idx].name).ok()
                        });
                        let mut g_fh = group_col_idx.and_then(|idx| {
                            reader.open_column(&config.schema.columns[idx].name).ok()
                        });

                        // Populate run caches (fs::read + bincode::deserialize) before the loop
                        f_fh.get_runs_i64();
                        if let Some(ref mut h) = a_fh {
                            h.get_runs_i64();
                        }
                        if let Some(ref mut h) = g_fh {
                            h.get_runs_i64();
                        }
                        res.stage_col_open_ns = col_open_t.elapsed().as_nanos();

                        let f_runs = f_fh.get_runs_i64();
                        let a_runs = a_fh.as_mut().map(|h| h.get_runs_i64());
                        let g_runs = g_fh.as_mut().map(|h| h.get_runs_i64());

                        let mut f_cur_run = 0usize;
                        let mut f_cur_off = 0u32;
                        let mut a_cur_run = 0usize;
                        let mut a_cur_off = 0u32;
                        let mut g_cur_run = 0usize;
                        let mut g_cur_off = 0u32;

                        let op_pass: &dyn Fn(i64) -> bool = match op {
                            FilterOp::Eq => &|v| v == f_val,
                            FilterOp::Gt => &|v| v > f_val,
                            FilterOp::Lt => &|v| v < f_val,
                            FilterOp::Ge => &|v| v >= f_val,
                            FilterOp::Le => &|v| v <= f_val,
                        };

                        for g_idx in 0..num_groups {
                            res.io_row_groups_eval += 1;
                            let start = g_idx * group_size;
                            let end = (start + group_size).min(num_rows);
                            let count = end - start;

                            if !ColumnSampler::should_sample_row_group(
                                seg_idx,
                                g_idx,
                                row_rate,
                                config.seed,
                            ) {
                                RleEncoder::skip_chunk_i64(
                                    f_runs,
                                    count,
                                    &mut f_cur_run,
                                    &mut f_cur_off,
                                );
                                if let Some(ar) = a_runs {
                                    RleEncoder::skip_chunk_i64(
                                        ar,
                                        count,
                                        &mut a_cur_run,
                                        &mut a_cur_off,
                                    );
                                }
                                if let Some(gr) = g_runs {
                                    RleEncoder::skip_chunk_i64(
                                        gr,
                                        count,
                                        &mut g_cur_run,
                                        &mut g_cur_off,
                                    );
                                }
                                continue;
                            }
                            res.io_row_groups_samp += 1;

                            let filter_t = Instant::now();
                            let passed = RleEncoder::filter_aggregate_runs(
                                f_runs,
                                a_runs,
                                g_runs,
                                &mut f_cur_run,
                                &mut f_cur_off,
                                &mut a_cur_run,
                                &mut a_cur_off,
                                &mut g_cur_run,
                                &mut g_cur_off,
                                count,
                                op_pass,
                                &mut res.group_sums_u8,
                                &mut res.group_counts_u8,
                                &mut res.scalar_sum,
                                &mut res.scalar_count,
                            );
                            res.stage_filter_ns += filter_t.elapsed().as_nanos();
                            res.rows_after_filter += passed;
                            res.total_rows_read += count;
                        }
                    }
                }
                res.duration_ms = start_seg.elapsed().as_secs_f64() * 1000.0;
                res
            })
            .collect();

        for r in results {
            total_rows_read += r.total_rows_read;
            rows_after_filter += r.rows_after_filter;
            scalar_count += r.scalar_count;
            scalar_sum += r.scalar_sum;
            rows_total_possible += r.rows_total_possible;
            io_segments_opened += r.io_segments_opened;
            io_col_files_opened += r.io_col_files_opened;
            io_bytes_read += r.io_bytes_read;
            io_row_groups_sampled += r.io_row_groups_samp;
            // Use max across segments (critical path in parallel execution)
            stage_aggregation_ns = stage_aggregation_ns.max(r.stage_filter_ns.max(r.stage_agg_ns));
            stage_decode_ns = stage_decode_ns.max(r.stage_decode_ns);
            stage_col_open_ms = stage_col_open_ms.max(r.stage_col_open_ns as f64 / 1_000_000.0);
            for k in 0..256 {
                group_sums_u8[k] += r.group_sums_u8[k];
                group_counts_u8[k] += r.group_counts_u8[k];
            }
        }

        let mut groups = Vec::new();
        if group_col_idx.is_some() {
            for k in 0..256 {
                if group_counts_u8[k] > 0 {
                    let val = match &plan.aggregation {
                        Aggregation::Avg(_) => group_sums_u8[k] / group_counts_u8[k] as f64,
                        Aggregation::Count => group_counts_u8[k] as f64,
                        _ => group_sums_u8[k],
                    };
                    groups.push((k.to_string(), val, ConfidenceFlag::High));
                }
            }
        }
        let val = if !groups.is_empty() {
            AggregateValue::Groups(groups)
        } else {
            let s = match &plan.aggregation {
                Aggregation::Avg(_) => {
                    if scalar_count > 0 {
                        scalar_sum / scalar_count as f64
                    } else {
                        0.0
                    }
                }
                _ => scalar_sum,
            };
            AggregateValue::Scalar(s)
        };

        let global_total = if global_total_rows_possible > 0 {
            global_total_rows_possible
        } else {
            1
        };
        let act_rate = total_rows_read as f64 / global_total as f64;
        let scaled =
            crate::aqp::estimator::Estimator::scale_result(val, &plan.aggregation, act_rate);

        self.print_profile(
            plan,
            start_total,
            stage_col_open_ms,
            stage_decode_ns,
            stage_aggregation_ns,
            io_segments_opened,
            io_col_files_opened,
            io_bytes_read,
            io_row_groups_sampled,
            rows_total_possible,
            rows_after_filter,
            scalar_count,
            sst_rate,
            row_rate,
            false,
            true,
        );

        Ok(QueryResult {
            value: scaled,
            confidence: ConfidenceFlag::High,
            storage_path: StoragePath::Columnar,
            rows_scanned: total_rows_read as u64,
            sampling_rate: act_rate,
            estimated_variance: 0.0,
            warnings: vec![],
            profile: overall_profile,
        })
    }

    fn print_profile(
        &self,
        plan: &QueryPlan,
        start: Instant,
        s_col: f64,
        s_dec: u128,
        s_filt: u128,
        i_seg: u64,
        i_col: u64,
        _i_bytes: u64,
        _i_rg: u64,
        r_tot: u64,
        r_filt: u64,
        r_samp: u64,
        s_sst: f64,
        s_row: f64,
        mfp: bool,
        para: bool,
    ) {
        let q = match (&plan.aggregation, plan.group_by.is_some()) {
            (Aggregation::Count, true) => "Q1",
            (Aggregation::Sum(_), false) => "Q2",
            (Aggregation::Count, false) => "Q3",
            _ => "QX",
        };
        println!(
            "[PROFILE] query={q} total_ms={:.2} stage.col_open_ms={s_col:.2} stage.decode_ms={:.2} stage.filter_agg_ms={:.2} io.seg={i_seg} io.col={i_col} rows.tot={r_tot} rows.filt={r_filt} rows.samp={r_samp} rate.sst={s_sst:.4} rate.row={s_row:.4} mfp={mfp} parallel={para}",
            start.elapsed().as_secs_f64() * 1000.0,
            s_dec as f64 / 1_000_000.0,
            s_filt as f64 / 1_000_000.0,
        );
    }
}

fn scaler_count_final_add(src: u64, dst: &mut u64) {
    *dst += src;
}
