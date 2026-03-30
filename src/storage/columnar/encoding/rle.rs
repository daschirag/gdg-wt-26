use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct RleRun<T> {
    pub value: T,
    pub length: u32,
}

#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, bytemuck::Zeroable, bytemuck::Pod)]
pub struct RleRunI64 {
    pub value: i64,
    pub length: u32,
    pub pad: u32,
}

impl RleRunI64 {
    pub fn new(value: i64, length: u32) -> Self {
        Self {
            value,
            length,
            pad: 0,
        }
    }
}

impl From<RleRun<i64>> for RleRunI64 {
    fn from(run: RleRun<i64>) -> Self {
        Self::new(run.value, run.length)
    }
}

impl From<RleRunI64> for RleRun<i64> {
    fn from(run: RleRunI64) -> Self {
        Self {
            value: run.value,
            length: run.length,
        }
    }
}

pub struct RleEncoder;

impl RleEncoder {
    pub fn encode<T: Serialize + Clone + PartialEq>(data: &[T]) -> Vec<RleRun<T>> {
        if data.is_empty() {
            return vec![];
        }
        let mut runs = vec![];
        let mut current_val = data[0].clone();
        let mut current_len = 0u32;
        for val in data {
            if *val == current_val {
                current_len += 1;
            } else {
                runs.push(RleRun {
                    value: current_val,
                    length: current_len,
                });
                current_val = val.clone();
                current_len = 1;
            }
        }
        runs.push(RleRun {
            value: current_val,
            length: current_len,
        });
        runs
    }

    pub fn decode<T: for<'de> Deserialize<'de> + Clone>(runs: &[RleRun<T>]) -> Vec<T> {
        let count: usize = runs.iter().map(|r| r.length as usize).sum();
        let mut out = Vec::with_capacity(count);
        for run in runs {
            for _ in 0..run.length {
                out.push(run.value.clone());
            }
        }
        out
    }

    pub fn encode_i64(data: &[i64]) -> Vec<RleRunI64> {
        if data.is_empty() {
            return vec![];
        }
        let mut runs = vec![];
        let mut current_val = data[0];
        let mut current_len = 0u32;
        for &val in data {
            if val == current_val {
                current_len += 1;
            } else {
                runs.push(RleRunI64::new(current_val, current_len));
                current_val = val;
                current_len = 1;
            }
        }
        runs.push(RleRunI64::new(current_val, current_len));
        runs
    }

    pub fn decode_i64(runs: &[RleRunI64]) -> Vec<i64> {
        let count: usize = runs.iter().map(|r| r.length as usize).sum();
        let mut out = Vec::with_capacity(count);
        for run in runs {
            for _ in 0..run.length {
                out.push(run.value);
            }
        }
        out
    }

    pub fn decode_range<T: for<'de> Deserialize<'de> + Clone>(
        runs: &[RleRun<T>],
        start: usize,
        end: usize,
    ) -> Vec<T> {
        let (mut run_idx, mut run_offset) = Self::seek_to(runs, start);
        let mut out = Vec::with_capacity(end - start);
        let mut needed = end - start;
        while needed > 0 && run_idx < runs.len() {
            let run = &runs[run_idx];
            let avail = (run.length - run_offset) as usize;
            let take = avail.min(needed);
            for _ in 0..take {
                out.push(run.value.clone());
            }
            needed -= take;
            run_idx += 1;
            run_offset = 0;
        }
        out
    }

    pub fn decode_chunk_i64(
        runs: &[RleRunI64],
        count: usize,
        cursor_run_idx: &mut usize,
        cursor_run_offset: &mut u32,
    ) -> Vec<i64> {
        let mut out = Vec::with_capacity(count);
        let mut needed = count;
        while needed > 0 && *cursor_run_idx < runs.len() {
            let run = &runs[*cursor_run_idx];
            let avail = (run.length - *cursor_run_offset) as usize;
            let take = avail.min(needed);
            for _ in 0..take {
                out.push(run.value);
            }
            needed -= take;
            if (take as u32) < (run.length - *cursor_run_offset) {
                *cursor_run_offset += take as u32;
            } else {
                *cursor_run_idx += 1;
                *cursor_run_offset = 0;
            }
        }
        out
    }

    /// Filter and aggregate directly on RLE runs without decoding to individual rows.
    /// Since every row in a run has the same value, the filter predicate is uniform
    /// across the entire run — accept or reject in O(1) per run instead of O(rows).
    ///
    /// Cursors for f/a/g are advanced in sync. `count` is the number of rows to process
    /// (one row group). Returns number of rows that passed the filter.
    pub fn filter_aggregate_runs(
        f_runs: &[RleRunI64],
        a_runs: Option<&[RleRunI64]>,
        g_runs: Option<&[RleRunI64]>,
        f_cur_run: &mut usize,
        f_cur_off: &mut u32,
        a_cur_run: &mut usize,
        a_cur_off: &mut u32,
        g_cur_run: &mut usize,
        g_cur_off: &mut u32,
        count: usize,
        op_pass: impl Fn(i64) -> bool,
        group_sums: &mut [f64; 256],
        group_counts: &mut [u64; 256],
        scalar_sum: &mut f64,
        scalar_count: &mut u64,
    ) -> u64 {
        let mut rows_passed = 0u64;
        let mut remaining = count;

        while remaining > 0 && *f_cur_run < f_runs.len() {
            let f_run = &f_runs[*f_cur_run];
            let avail = (f_run.length - *f_cur_off) as usize;
            let take = avail.min(remaining);
            remaining -= take;

            if op_pass(f_run.value) {
                rows_passed += take as u64;
                // Accepted: walk agg and group cursors to accumulate
                let mut left = take;
                while left > 0 {
                    let a_avail = a_runs.map_or(left, |ar| {
                        if *a_cur_run < ar.len() {
                            (ar[*a_cur_run].length - *a_cur_off) as usize
                        } else {
                            left
                        }
                    });
                    let g_avail = g_runs.map_or(left, |gr| {
                        if *g_cur_run < gr.len() {
                            (gr[*g_cur_run].length - *g_cur_off) as usize
                        } else {
                            left
                        }
                    });
                    let step = a_avail.min(g_avail).min(left);
                    if step == 0 {
                        break;
                    }

                    let aval = a_runs.map_or(1.0, |ar| {
                        if *a_cur_run < ar.len() {
                            ar[*a_cur_run].value as f64
                        } else {
                            0.0
                        }
                    });
                    let gv = g_runs.map_or(usize::MAX, |gr| {
                        if *g_cur_run < gr.len() {
                            gr[*g_cur_run].value as usize
                        } else {
                            usize::MAX
                        }
                    });

                    if gv < 256 {
                        group_sums[gv] += aval * step as f64;
                        group_counts[gv] += step as u64;
                    } else {
                        *scalar_sum += aval * step as f64;
                        *scalar_count += step as u64;
                    }

                    left -= step;
                    if let Some(ar) = a_runs {
                        *a_cur_off += step as u32;
                        if *a_cur_run < ar.len() && *a_cur_off == ar[*a_cur_run].length {
                            *a_cur_run += 1;
                            *a_cur_off = 0;
                        }
                    }
                    if let Some(gr) = g_runs {
                        *g_cur_off += step as u32;
                        if *g_cur_run < gr.len() && *g_cur_off == gr[*g_cur_run].length {
                            *g_cur_run += 1;
                            *g_cur_off = 0;
                        }
                    }
                }
            } else {
                // Rejected: skip agg and group cursors by `take` rows to stay in sync
                if let Some(ar) = a_runs {
                    Self::skip_chunk_i64(ar, take, a_cur_run, a_cur_off);
                }
                if let Some(gr) = g_runs {
                    Self::skip_chunk_i64(gr, take, g_cur_run, g_cur_off);
                }
            }

            // Advance filter cursor
            *f_cur_off += take as u32;
            if *f_cur_off == f_run.length {
                *f_cur_run += 1;
                *f_cur_off = 0;
            }
        }
        rows_passed
    }

    pub fn skip_chunk_i64(
        runs: &[RleRunI64],
        count: usize,
        cursor_run_idx: &mut usize,
        cursor_run_offset: &mut u32,
    ) {
        let mut needed = count;
        while needed > 0 && *cursor_run_idx < runs.len() {
            let run = &runs[*cursor_run_idx];
            let avail = (run.length - *cursor_run_offset) as usize;
            let take = avail.min(needed);
            needed -= take;
            if (take as u32) < (run.length - *cursor_run_offset) {
                *cursor_run_offset += take as u32;
            } else {
                *cursor_run_idx += 1;
                *cursor_run_offset = 0;
            }
        }
    }

    pub fn seek_to<T>(runs: &[RleRun<T>], target: usize) -> (usize, u32) {
        let mut current = 0;
        for (i, run) in runs.iter().enumerate() {
            if current + run.length as usize > target {
                return (i, (target - current) as u32);
            }
            current += run.length as usize;
        }
        (runs.len(), 0)
    }

    pub fn seek_to_i64(runs: &[RleRunI64], target: usize) -> (usize, u32) {
        let mut current = 0;
        for (i, run) in runs.iter().enumerate() {
            if current + run.length as usize > target {
                return (i, (target - current) as u32);
            }
            current += run.length as usize;
        }
        (runs.len(), 0)
    }

    pub fn aggregate_counts_i64(runs: &[RleRunI64]) -> [u64; 256] {
        let mut counts = [0u64; 256];
        for run in runs {
            let val = run.value as usize;
            if val < 256 {
                counts[val] += run.length as u64;
            }
        }
        counts
    }

    pub fn aggregate_counts_i64_sampled(runs: &[RleRunI64], row_rate: f64) -> ([u64; 256], u64) {
        let mut counts = [0u64; 256];
        let mut rows_sampled = 0u64;
        for run in runs {
            let s_len = (run.length as f64 * row_rate).round() as u64;
            if s_len == 0 {
                continue;
            }
            let val = run.value;
            if val >= 0 && val < 256 {
                counts[val as usize] += s_len;
            }
            rows_sampled += s_len;
        }
        (counts, rows_sampled)
    }

    pub fn aggregate_sum_by_u8_i64(
        key_runs: &[RleRunI64],
        val_runs: &[RleRunI64],
    ) -> ([f64; 256], [u64; 256]) {
        let mut sums = [0.0f64; 256];
        let mut counts = [0u64; 256];

        let mut v_idx = 0;
        let mut v_off = 0;

        for k_run in key_runs {
            let mut k_rem = k_run.length;
            let group = k_run.value as usize;

            while k_rem > 0 && v_idx < val_runs.len() {
                let v_run = &val_runs[v_idx];
                let v_avail = v_run.length - v_off;
                let take = k_rem.min(v_avail);

                if group < 256 {
                    sums[group] += v_run.value as f64 * take as f64;
                    counts[group] += take as u64;
                }

                k_rem -= take;
                v_off += take;
                if v_off == v_run.length {
                    v_idx += 1;
                    v_off = 0;
                }
            }
        }
        (sums, counts)
    }

    pub fn aggregate_sum_by_u8_i64_sampled(
        key_runs: &[RleRunI64],
        val_runs: &[RleRunI64],
        row_rate: f64,
    ) -> ([f64; 256], [u64; 256], u64) {
        let mut sums = [0.0f64; 256];
        let mut counts = [0u64; 256];
        let mut total_sampled = 0u64;

        let mut v_idx = 0;
        let mut v_off = 0;

        for k_run in key_runs {
            let sampled_k_len = (k_run.length as f64 * row_rate).round() as u32;
            if sampled_k_len == 0 {
                // Must still advance v_idx/v_off by the full length to maintain alignment
                let mut to_skip = k_run.length;
                while to_skip > 0 && v_idx < val_runs.len() {
                    let v_run = &val_runs[v_idx];
                    let v_avail = v_run.length - v_off;
                    let skip = to_skip.min(v_avail);
                    to_skip -= skip;
                    v_off += skip;
                    if v_off == v_run.length {
                        v_idx += 1;
                        v_off = 0;
                    }
                }
                continue;
            }

            let mut k_rem = sampled_k_len;
            let group = k_run.value as usize;

            while k_rem > 0 && v_idx < val_runs.len() {
                let v_run = &val_runs[v_idx];
                let v_avail = v_run.length - v_off;
                let take = k_rem.min(v_avail);

                if group < 256 {
                    sums[group] += v_run.value as f64 * take as f64;
                    counts[group] += take as u64;
                    total_sampled += take as u64;
                }

                k_rem -= take;
                v_off += take;
                if v_off == v_run.length {
                    v_idx += 1;
                    v_off = 0;
                }
            }

            // If we didn't use the full k_run (because of row_rate), we must skip the remainder
            let mut remain_to_skip = k_run.length - sampled_k_len;
            while remain_to_skip > 0 && v_idx < val_runs.len() {
                let v_run = &val_runs[v_idx];
                let v_avail = v_run.length - v_off;
                let skip = remain_to_skip.min(v_avail);
                remain_to_skip -= skip;
                v_off += skip;
                if v_off == v_run.length {
                    v_idx += 1;
                    v_off = 0;
                }
            }
        }
        (sums, counts, total_sampled)
    }
}
