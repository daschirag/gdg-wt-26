use lsm::*;
use std::time::Instant;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = match Config::load_from_file("config.toml") {
        Ok(c) => c,
        Err(e) => panic!("Failed to load config.toml: {:?}", e),
    };
    let config = std::sync::Arc::new(config);

    // Initialize Storage
    let storage = std::sync::Arc::new(crate::storage::manager::StorageManager::new(
        config.clone(),
        "data".to_string(),
    ));

    // Start background compaction
    crate::storage::compaction::scheduler::CompactionScheduler::start_background_thread(
        storage.clone(),
    );

    println!("AQE Prototype - Phase 3 (Columnar Segments)");
    println!("------------------------------------");
    println!(
        "Config: Accuracy Target={}, k={}, Seed={}",
        config.accuracy_target, config.k, config.seed
    );

    let args: Vec<String> = std::env::args().collect();
    if args.len() < 2 {
        println!("Available commands:");
        println!("  gen <num> [csv_path] - (Deprecated) Generate data in Rust");
        println!("  load <csv_path>      - Load data from CSV and flush to SSTables");
        println!("  query <sql>           - Run approximate query");
        println!("  compact              - Manual compaction");
        println!("  verify               - CRC check");
        println!("No arguments provided. Starting interactive REPL...");
        return Ok(repl::start_repl(config.clone(), storage.clone())?);
    }
    let command = &args[1];

    if command == "verify" {
        println!("Verifying all SSTables for CRC integrity...");
        for i in 0..10 {
            let path = format!("data/sstable_{}.aqe", i);
            if !std::path::Path::new(&path).exists() {
                continue;
            }
            let reader = storage::sstable::reader::SSTableReader::new(&path);
            match reader.get_metadata() {
                Ok(meta) => println!(
                    "  {}: OK ({} rows, ts: {} - {}, v: {})",
                    path, meta.row_count, meta.min_ts, meta.max_ts, meta.schema_version
                ),
                Err(e) => println!("  {}: FAILED: {:?}", path, e),
            }
        }
        return Ok(());
    } else if command == "compact" {
        println!("Triggering manual compaction...");
        let start = Instant::now();
        match storage::compaction::scheduler::CompactionScheduler::compact_now(&storage) {
            Ok(tasks) => println!(
                "Compaction finished: {} segments created in {:?}",
                tasks,
                start.elapsed()
            ),
            Err(e) => println!("Compaction failed: {:?}", e),
        }
        return Ok(());
    } else if command == "resplit" {
        let seg_path = args.get(2).expect("Missing segment path");
        println!("Resplitting segment: {}...", seg_path);
        let start = Instant::now();

        let reader = storage::columnar::reader::ColumnarReader::new(seg_path.into(), 0)?;
        let rows = reader.read_all_rows(&config)?;
        println!("Loaded {} rows in {:?}", rows.len(), start.elapsed());

        // Use ColumnarWriter directly to split
        let mut segment_idx = 0;
        let mut row_idx = 0;
        let max_rows = config.max_segment_rows as usize;

        while row_idx < rows.len() {
            let end_idx = (row_idx + max_rows).min(rows.len());
            let chunk = &rows[row_idx..end_idx];

            let parent = std::path::Path::new(seg_path).parent().unwrap();
            let base_name = std::path::Path::new(seg_path)
                .file_name()
                .unwrap()
                .to_str()
                .unwrap();
            let new_seg_name = if segment_idx == 0 {
                format!("{}_split_{:03}", base_name, segment_idx)
            } else {
                format!("{}_split_{:03}", base_name, segment_idx)
            };
            let new_seg_path = parent.join(new_seg_name);

            println!(
                "Writing segment {} ({} rows)...",
                new_seg_path.display(),
                chunk.len()
            );
            storage::columnar::writer::ColumnarWriter::write_segment(
                &new_seg_path,
                chunk,
                &config,
                0.01,
            )?;
            segment_idx += 1;
            row_idx = end_idx;
        }

        println!(
            "Resplit finished: {} segments created in {:?}",
            segment_idx,
            start.elapsed()
        );
        println!("NOTE: Please manually register these segments or restart the engine.");
        return Ok(());
    }

    if command == "gen" {
        let num_rows: usize = args.get(2).and_then(|s| s.parse().ok()).unwrap_or(100_000);
        let csv_dump_path = args.get(3).map(|s| s.as_str());

        println!("Generating {} synthetic rows (Deprecated)...", num_rows);
        let start_gen = Instant::now();
        let (rows, gen_profile) = generate_dataset(num_rows, &config, csv_dump_path);
        let duration_gen = start_gen.elapsed();

        println!("Flushing rows to SSTables...");
        let start_flush = Instant::now();
        let chunk_size = (rows.len() / 10).max(1);
        for i in 0..10 {
            let start = i * chunk_size;
            let end = if i == 9 {
                rows.len()
            } else {
                (i + 1) * chunk_size
            };
            if start >= rows.len() {
                break;
            }
            let chunk = &rows[start..end];
            let path = format!("data/sstable_{}.aqe", i);
            crate::storage::sstable::writer::SSTableWriter::write_sstable_to_file(
                &path, chunk, &config,
            )?;
            storage.register_sstable(path);
        }
        let duration_flush = start_flush.elapsed();

        println!("--- Generation Profile ---");
        println!("  Random Gen:       {:>8.2}ms", gen_profile.random_gen_ms);
        println!("  Allocation/Misc:  {:>8.2}ms", gen_profile.allocation_ms);
        println!(
            "  Total Gen:        {:>8.2}ms",
            duration_gen.as_secs_f64() * 1000.0
        );
        println!();
        println!(
            "  Total Flush:      {:>8.2}ms",
            duration_flush.as_secs_f64() * 1000.0
        );
        println!("----------------------------");
        return Ok(());
    } else if command == "load" {
        let csv_path = args.get(2).ok_or_else(|| {
            QueryError::StorageError(StorageError::InvalidFormat("Missing CSV path".to_string()))
        })?;
        println!("Loading data from {}...", csv_path);

        let start_load = Instant::now();
        let rows = crate::storage::csv_loader::CsvLoader::load_csv(csv_path, &config)
            .map_err(QueryError::StorageError)?;
        let load_ms = start_load.elapsed().as_secs_f64() * 1000.0;
        println!("Loaded {} rows from CSV in {:.2}ms", rows.len(), load_ms);

        println!("Flushing rows to SSTables...");
        let start_flush = Instant::now();
        let num_chunks = 10;
        let chunk_size = (rows.len() / num_chunks).max(1);
        for i in 0..num_chunks {
            let start = i * chunk_size;
            let end = if i == num_chunks - 1 {
                rows.len()
            } else {
                (i + 1) * chunk_size
            };
            if start >= rows.len() {
                break;
            }
            let chunk = &rows[start..end];
            let path = format!("data/load_{}.aqe", i);
            crate::storage::sstable::writer::SSTableWriter::write_sstable_to_file(
                &path, chunk, &config,
            )?;
            storage.register_sstable(path);
        }
        let flush_ms = start_flush.elapsed().as_secs_f64() * 1000.0;
        println!("Flush successful in {:.2}ms", flush_ms);
        return Ok(());
    } else if command == "query" {
        let sql = args.get(2).expect("Missing SQL string");
        let mut accuracy = config.accuracy_target;
        let requested_aqp_mode = args
            .iter()
            .position(|r| r == "--aqp-mode")
            .and_then(|pos| args.get(pos + 1))
            .cloned();
        if let Some(pos) = args.iter().position(|r| r == "--accuracy") {
            if let Some(val) = args.get(pos + 1) {
                if let Ok(num) = val.parse::<f64>() {
                    accuracy = num;
                }
            }
        }

        let plan = query::parser::Parser::parse(sql)?;
        let execution_path = query::planner::QueryPlanner::plan(&plan, &storage);
        println!("[DEBUG] Chosen execution path: {:?}", execution_path);

        // Override config accuracy with command line accuracy
        let mut run_config = (*config).clone();
        run_config.accuracy_target = accuracy;
        if let Some(mode) = requested_aqp_mode.clone() {
            run_config.aqp_mode = mode;
        }
        let config = std::sync::Arc::new(run_config);
        let aqp_enabled = requested_aqp_mode.is_some()
            || matches!(plan.aggregation, query::ast::Aggregation::ApproxPercentile(_, _));

        let start = Instant::now();

        // 1. Scan MemTable (Exact)
        let mem_rows = storage.scan_memtable(&plan);
        let mem_count = mem_rows.len() as u64;
        let mut mem_sum = 0.0;
        for row in &mem_rows {
            match &plan.aggregation {
                query::ast::Aggregation::Sum(col) | query::ast::Aggregation::Avg(col) => {
                    if let Some(crate::types::Value::Int(i)) =
                        crate::types::get_value(row, col, &config)
                    {
                        mem_sum += i as f64;
                    }
                }
                _ => {}
            }
        }

        // 2. Scan Disk Layers
        let mem_keys = storage.get_memtable_keys();
        let mask = Some(&mem_keys);

        let skip_dedup = storage.all_sources_compacted();
        let disc_result = match execution_path {
            query::planner::ExecutionPath::FastPath {
                segments,
                column,
                aggregation,
            } => {
                let mut total_count = 0;
                let mut total_sum = 0.0;
                let warnings = vec!["Fast-path: Using pre-computed metadata".to_string()];

                for path in segments {
                    if let Ok(reader) =
                        storage::columnar::reader::ColumnarReader::new(path.into(), 0)
                    {
                        total_count += reader.metadata.row_count;
                        if let Some(col_name) = &column {
                            if let Some(col_meta) = reader.metadata.columns.get(col_name) {
                                total_sum += col_meta.sum;
                            }
                        }
                    }
                }

                let final_val = match aggregation {
                    query::ast::Aggregation::Count => {
                        crate::types::AggregateValue::Scalar(total_count as f64)
                    }
                    query::ast::Aggregation::Sum(_) => {
                        crate::types::AggregateValue::Scalar(total_sum)
                    }
                    query::ast::Aggregation::Avg(_)
                    | query::ast::Aggregation::ApproxPercentile(_, _) => {
                        if total_count > 0 {
                            crate::types::AggregateValue::Scalar(total_sum / total_count as f64)
                        } else {
                            crate::types::AggregateValue::Empty
                        }
                    }
                };

                Ok(crate::types::QueryResult {
                    value: final_val,
                    confidence: crate::types::ConfidenceFlag::Exact,
                    warnings,
                    storage_path: crate::types::StoragePath::Columnar,
                    rows_scanned: 0, // Metadata only
                    sampling_rate: 1.0,
                    estimated_variance: 0.0,
                    profile: crate::types::QueryProfile::default(),
                    aqp: None,
                    next_offset: None,
                })
            }
            query::planner::ExecutionPath::Row { sstables } => {
                let pipeline = query::executor::pipeline::Pipeline::new(sstables);
                pipeline.execute(&plan, &config, mask, skip_dedup)
            }
            query::planner::ExecutionPath::Columnar {
                segments,
                columns_needed,
            } => {
                println!("[DEBUG] Path: Columnar ({} segments)", segments.len());
                if aqp_enabled
                    && query::executor::aqp_pipeline::AqpPipeline::supports_plan(&plan)
                    && query::executor::aqp_pipeline::group_by_is_low_cardinality(&segments, &plan)?
                {
                    let pipeline = query::executor::aqp_pipeline::AqpPipeline::new(segments);
                    pipeline.execute(&plan, &config, mask, skip_dedup)
                } else {
                    let pipeline = query::executor::col_pipeline::ColumnarPipeline::new(segments)?;
                    pipeline.execute(&plan, &columns_needed, &config, mask, skip_dedup)
                }
            }
            query::planner::ExecutionPath::Mixed {
                sstables,
                segments,
                columns_needed,
            } => {
                println!(
                    "[DEBUG] Path: Mixed ({} ssts, {} segments)",
                    sstables.len(),
                    segments.len()
                );
                let row_pipeline = query::executor::pipeline::Pipeline::new(sstables);
                let res_row = row_pipeline.execute(&plan, &config, mask, skip_dedup)?;

                let res_col = if aqp_enabled
                    && query::executor::aqp_pipeline::AqpPipeline::supports_plan(&plan)
                    && query::executor::aqp_pipeline::group_by_is_low_cardinality(&segments, &plan)?
                {
                    let pipeline = query::executor::aqp_pipeline::AqpPipeline::new(segments);
                    pipeline.execute(&plan, &config, mask, skip_dedup)?
                } else {
                    let pipeline = query::executor::col_pipeline::ColumnarPipeline::new(segments)?;
                    pipeline.execute(&plan, &columns_needed, &config, mask, skip_dedup)?
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
                    storage_path: crate::types::StoragePath::Mixed,
                    rows_scanned: res_row.rows_scanned + res_col.rows_scanned,
                    sampling_rate: (res_row.sampling_rate + res_col.sampling_rate) / 2.0,
                    estimated_variance: 0.0,
                    profile: res_row.profile,
                    aqp: res_col.aqp,
                    next_offset: res_col.next_offset.or(res_row.next_offset),
                })
            }
        }?;

        // 3. Robust Merge (LSM Read Path)
        let result = if mem_count > 0 {
            let final_value = match (&disc_result.value, &plan.aggregation) {
                (crate::types::AggregateValue::Scalar(s), query::ast::Aggregation::Avg(_)) => {
                    let total_scanned = disc_result.rows_scanned + mem_count;
                    crate::types::AggregateValue::Scalar(
                        (s * disc_result.rows_scanned as f64 + mem_sum) / total_scanned as f64,
                    )
                }
                (crate::types::AggregateValue::Scalar(s), _) => {
                    crate::types::AggregateValue::Scalar(
                        s + if matches!(plan.aggregation, query::ast::Aggregation::Count) {
                            mem_count as f64
                        } else {
                            mem_sum
                        },
                    )
                }
                (crate::types::AggregateValue::Empty, query::ast::Aggregation::Avg(_)) => {
                    crate::types::AggregateValue::Scalar(mem_sum / mem_count as f64)
                }
                (crate::types::AggregateValue::Empty, _) => crate::types::AggregateValue::Scalar(
                    if matches!(plan.aggregation, query::ast::Aggregation::Count) {
                        mem_count as f64
                    } else {
                        mem_sum
                    },
                ),
                _ => disc_result.value.clone(), // Fallback for Groups
            };

            crate::types::QueryResult {
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
        let duration = start.elapsed();

        println!("Execution Time: {:?}", duration);
        println!("Rows Scanned: {}", result.rows_scanned);
        println!("--- Performance Profile ---");
        println!(
            "  Bloom Filter:     {:.2}ms",
            result.profile.bloom_filter_ms
        );
        println!(
            "  SST Sampling:     {:.2}ms",
            result.profile.sst_sampling_ms
        );
        println!("  Disk IO (Read):   {:.2}ms", result.profile.io_read_ms);
        println!(
            "  Deserialization:  {:.2}ms",
            result.profile.deserialization_ms
        );
        println!("  CRC Verify:       {:.2}ms", result.profile.crc_verify_ms);
        println!("  Filtering:        {:.2}ms", result.profile.filtering_ms);
        println!("  Aggregation:      {:.2}ms", result.profile.aggregation_ms);
        println!("---------------------------");

        match result.value {
            AggregateValue::Groups(groups) => {
                for (k, v, c) in groups {
                    println!("GROUP: {}|{}|{:?}", k, v, c);
                }
            }
            AggregateValue::Scalar(s) => {
                println!("SCALAR: {}|{:?}", s, result.confidence);
            }
            AggregateValue::Empty => {
                println!("EMPTY");
            }
        }
        println!("Storage Path: {:?}", result.storage_path);
        println!("Effective Sampling Rate: {:.4}", result.sampling_rate);
        if let Some(aqp) = &result.aqp {
            println!(
                "AQP: mode={} source={} sample_rate={:.4} sample_rows={} error={:.4}",
                aqp.mode, aqp.source, aqp.sample_rate, aqp.sample_rows, aqp.estimated_error
            );
        }

        if !result.warnings.is_empty() {
            println!("Warnings: {:?}", result.warnings);
        }
        return Ok(());
    }

    Ok(())
}

fn merge_aggregate_values(
    a: &AggregateValue,
    b: &AggregateValue,
    aggregation: &query::ast::Aggregation,
    rows_a: u64,
    rows_b: u64,
) -> AggregateValue {
    use std::collections::HashMap;
    match (a, b) {
        (AggregateValue::Scalar(s1), AggregateValue::Scalar(s2)) => {
            match aggregation {
                query::ast::Aggregation::Avg(_) => {
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
            }
        }
        (AggregateValue::Scalar(s), AggregateValue::Empty)
        | (AggregateValue::Empty, AggregateValue::Scalar(s)) => AggregateValue::Scalar(*s),
        (AggregateValue::Empty, AggregateValue::Empty) => AggregateValue::Empty,
        (AggregateValue::Groups(ga), AggregateValue::Groups(gb)) => {
            // Merge two group maps key-by-key.
            let mut merged: HashMap<String, (f64, u64, crate::types::ConfidenceFlag)> =
                HashMap::new();
            for (k, v, c) in ga {
                merged.insert(k.clone(), (*v, rows_a, *c));
            }
            for (k, v, c) in gb {
                merged
                    .entry(k.clone())
                    .and_modify(|(existing_v, existing_rows, existing_c)| {
                        *existing_v = match aggregation {
                            query::ast::Aggregation::Avg(_) => {
                                let total = *existing_rows + rows_b;
                                if total > 0 {
                                    (*existing_v * *existing_rows as f64 + v * rows_b as f64)
                                        / total as f64
                                } else {
                                    *existing_v
                                }
                            }
                            _ => *existing_v + v,
                        };
                        *existing_rows += rows_b;
                        *existing_c = merge_confidence(*existing_c, *c);
                    })
                    .or_insert((*v, rows_b, *c));
            }
            let mut groups: Vec<(String, f64, crate::types::ConfidenceFlag)> = merged
                .into_iter()
                .map(|(k, (v, _, c))| (k, v, c))
                .collect();
            groups.sort_by(|a, b| a.0.cmp(&b.0));
            AggregateValue::Groups(groups)
        }
        (AggregateValue::Groups(g), AggregateValue::Empty)
        | (AggregateValue::Empty, AggregateValue::Groups(g)) => {
            AggregateValue::Groups(g.clone())
        }
        // Scalar + Groups: shouldn't happen in practice; keep the groups side.
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
