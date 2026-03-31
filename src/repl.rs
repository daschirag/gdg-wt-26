use crate::config::Config;
use crate::datagen::generate_dataset;
use crate::query::executor::pipeline::Pipeline;
use crate::query::parser::Parser;
use rustyline::error::ReadlineError;
use rustyline::{DefaultEditor, Result};
use std::time::Instant;

use crate::storage::manager::StorageManager;
use std::sync::Arc;

pub fn start_repl(config: Arc<Config>, storage: Arc<StorageManager>) -> Result<()> {
    let mut rl = DefaultEditor::new()?;
    if rl.load_history("history.txt").is_err() {
        println!("No previous history.");
    }

    println!("AQE Interactive REPL");
    println!("Type 'help' for commands, 'exit' to quit.");

    // Session-level AQP mode override: None means use config default.
    let mut aqp_mode: Option<String> = None;

    loop {
        let readline = rl.readline("aqe> ");
        match readline {
            Ok(line) => {
                rl.add_history_entry(line.as_str())?;
                let parts: Vec<&str> = line.trim().split_whitespace().collect();
                if parts.is_empty() {
                    continue;
                }

                match parts[0].to_lowercase().as_str() {
                    "exit" | "quit" => break,
                    "help" => print_help(),
                    "gen" => {
                        let num = parts
                            .get(1)
                            .and_then(|s| s.parse::<usize>().ok())
                            .unwrap_or(100_000);
                        handle_gen(num, &config, &storage);
                    }
                    "compact" => {
                        handle_compact(&storage);
                    }
                    // SET AQP MODE <fast|balanced|accurate|off>
                    "set" => {
                        let rest: Vec<&str> = parts[1..].iter().map(|s| s.to_owned()).collect();
                        if rest.len() >= 3
                            && rest[0].to_ascii_lowercase() == "aqp"
                            && rest[1].to_ascii_lowercase() == "mode"
                        {
                            match rest[2].to_ascii_lowercase().as_str() {
                                "off" => {
                                    aqp_mode = None;
                                    println!("AQP disabled (using exact execution).");
                                }
                                m @ ("fast" | "balanced" | "accurate") => {
                                    aqp_mode = Some(m.to_string());
                                    println!("AQP mode set to '{}'.", m);
                                }
                                other => {
                                    println!(
                                        "Unknown AQP mode '{}'. Use: fast, balanced, accurate, off.",
                                        other
                                    );
                                }
                            }
                        } else {
                            println!("Usage: SET AQP MODE <fast|balanced|accurate|off>");
                        }
                    }
                    "query" => {
                        // Extract query between quotes or the rest of the line
                        let query_str = if line.contains('"') {
                            line.split('"').nth(1).unwrap_or("")
                        } else {
                            line.strip_prefix("query").unwrap_or("").trim()
                        };
                        if query_str.is_empty() {
                            println!("Error: Missing query string. Use: query \"SELECT ...\"");
                        } else {
                            if let Err(e) =
                                handle_query(query_str, &config, &storage, aqp_mode.as_deref())
                            {
                                println!("Query Error: {:?}", e);
                            }
                        }
                    }
                    _ => println!(
                        "Unknown command: {}. Type 'help' for available commands.",
                        parts[0]
                    ),
                }
            }
            Err(ReadlineError::Interrupted) => {
                println!("CTRL-C");
                break;
            }
            Err(ReadlineError::Eof) => {
                println!("CTRL-D");
                break;
            }
            Err(err) => {
                println!("Error: {:?}", err);
                break;
            }
        }
    }
    rl.save_history("history.txt")?;
    Ok(())
}

fn print_help() {
    println!("Available commands:");
    println!("  gen <num>                        - Generate <num> synthetic rows and flush to SSTables");
    println!("  compact                          - Manually trigger background compaction");
    println!("  query \"<sql>\"                  - Execute an approximate SQL query");
    println!("  set aqp mode <fast|balanced|     - Enable AQP with specified accuracy mode");
    println!("               accurate|off>         (off = disable AQP, use exact execution)");
    println!("  help                             - Show this help message");
    println!("  exit / quit                      - Exit the REPL");
}

fn handle_compact(storage: &StorageManager) {
    println!("Triggering manual compaction...");
    let start = Instant::now();
    match crate::storage::compaction::scheduler::CompactionScheduler::compact_now(storage) {
        Ok(tasks) => {
            if tasks > 0 {
                println!(
                    "Compaction successful! Created {} segments in {:?}",
                    tasks,
                    start.elapsed()
                );
            } else {
                println!(
                    "No compaction tasks needed at this time (checked in {:?})",
                    start.elapsed()
                );
            }
        }
        Err(e) => println!("Compaction error: {:?}", e),
    }
}

fn handle_gen(num_rows: usize, config: &Config, _storage: &StorageManager) {
    println!("Generating {} rows...", num_rows);
    let start = Instant::now();
    let (rows, _) = generate_dataset(num_rows, config, None);

    // For now, keep direct flush for 'gen' command bypass to avoid memory limit
    let path_prefix = "data/sstable_";
    std::fs::create_dir_all("data").unwrap_or_default();

    for i in 0..10 {
        let chunk_size = num_rows / 10;
        let start_idx = i * chunk_size;
        let end_idx = if i == 9 {
            num_rows
        } else {
            (i + 1) * chunk_size
        };
        let chunk = &rows[start_idx..end_idx];
        let path = format!("{}{}.aqe", path_prefix, i);
        crate::storage::sstable::writer::SSTableWriter::write_sstable_to_file(&path, chunk, config)
            .expect("Failed to write Row SSTable");
    }
    println!(
        "Done! Generated and flushed {} rows (Row-based) in {:?}",
        num_rows,
        start.elapsed()
    );
}

fn handle_query(
    sql: &str,
    config: &Config,
    storage: &StorageManager,
    repl_aqp_mode: Option<&str>,
) -> std::result::Result<(), Box<dyn std::error::Error>> {
    let mut sstables = Vec::new();
    for i in 0..10 {
        sstables.push(format!("data/sstable_{}.aqe", i));
    }

    // Build an effective config: if the user set an AQP mode in the REPL, override it.
    let effective_config;
    let config = if let Some(mode) = repl_aqp_mode {
        effective_config = Config { aqp_mode: mode.to_string(), ..config.clone() };
        &effective_config
    } else {
        config
    };

    match Parser::parse(sql) {
        Ok(plan) => {
            let execution_path = crate::query::planner::QueryPlanner::plan(&plan, storage);
            let aqp_enabled = repl_aqp_mode.is_some()
                || matches!(
                    plan.aggregation,
                    crate::query::ast::Aggregation::ApproxPercentile(_, _)
                );

            let start = Instant::now();

            // 1. Scan MemTable (Exact)
            let mem_rows = storage.scan_memtable(&plan);
            let mem_count = mem_rows.len() as u64;
            let mut mem_sum = 0.0;
            for row in &mem_rows {
                match &plan.aggregation {
                    crate::query::ast::Aggregation::Sum(col)
                    | crate::query::ast::Aggregation::Avg(col) => {
                        if let Some(crate::types::Value::Int(i)) =
                            crate::types::get_value(row, col, config)
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
                crate::query::planner::ExecutionPath::FastPath {
                    segments,
                    column,
                    aggregation,
                } => {
                    let mut total_count = 0;
                    let mut total_sum = 0.0;
                    let warnings = vec!["Fast-path: Using pre-computed metadata".to_string()];

                    for path in segments {
                        if let Ok(reader) =
                            crate::storage::columnar::reader::ColumnarReader::new(path.into(), 0)
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
                        crate::query::ast::Aggregation::Count => {
                            crate::types::AggregateValue::Scalar(total_count as f64)
                        }
                        crate::query::ast::Aggregation::Sum(_) => {
                            crate::types::AggregateValue::Scalar(total_sum)
                        }
                        crate::query::ast::Aggregation::Avg(_)
                        | crate::query::ast::Aggregation::ApproxPercentile(_, _) => {
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
                        rows_scanned: 0,
                        sampling_rate: 1.0,
                        estimated_variance: 0.0,
                        profile: crate::types::QueryProfile::default(),
                        aqp: None,
                        next_offset: None,
                    })
                }
                crate::query::planner::ExecutionPath::Row { sstables } => {
                    let pipeline = Pipeline::new(sstables);
                    pipeline.execute(&plan, config, mask, skip_dedup)
                }
                crate::query::planner::ExecutionPath::Columnar {
                    segments,
                    columns_needed,
                } => {
                    if aqp_enabled
                        && crate::query::executor::aqp_pipeline::AqpPipeline::supports_plan(&plan)
                        && crate::query::executor::aqp_pipeline::group_by_is_low_cardinality(
                            &segments, &plan,
                        )?
                    {
                        let pipeline =
                            crate::query::executor::aqp_pipeline::AqpPipeline::new(segments);
                        pipeline.execute(&plan, config, mask, skip_dedup)
                    } else {
                        let pipeline =
                            crate::query::executor::col_pipeline::ColumnarPipeline::new(segments)?;
                        pipeline.execute(&plan, &columns_needed, config, mask, skip_dedup)
                    }
                }
                crate::query::planner::ExecutionPath::Mixed {
                    sstables,
                    segments,
                    columns_needed,
                } => {
                    let row_pipeline = Pipeline::new(sstables);
                    let res_row = row_pipeline.execute(&plan, config, mask, skip_dedup)?;

                    let res_col = if aqp_enabled
                        && crate::query::executor::aqp_pipeline::AqpPipeline::supports_plan(&plan)
                        && crate::query::executor::aqp_pipeline::group_by_is_low_cardinality(
                            &segments, &plan,
                        )?
                    {
                        let pipeline =
                            crate::query::executor::aqp_pipeline::AqpPipeline::new(segments);
                        pipeline.execute(&plan, config, mask, skip_dedup)?
                    } else {
                        let pipeline =
                            crate::query::executor::col_pipeline::ColumnarPipeline::new(segments)?;
                        pipeline.execute(&plan, &columns_needed, config, mask, skip_dedup)?
                    };

                    let merged_value = merge_aggregate_values(
                        &res_row.value,
                        &res_col.value,
                        &plan.aggregation,
                        res_row.rows_scanned,
                        res_col.rows_scanned,
                    );
                    let merged_confidence =
                        merge_confidence(res_row.confidence, res_col.confidence);

                    Ok(crate::types::QueryResult {
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
            };

            // 3. Robust Merge (LSM Read Path)
            let result = match disc_result {
                Ok(dr) => {
                    if mem_count > 0 {
                        let final_value = match (&dr.value, &plan.aggregation) {
                            (
                                crate::types::AggregateValue::Scalar(s),
                                crate::query::ast::Aggregation::Avg(_),
                            ) => {
                                let total_scanned = dr.rows_scanned + mem_count;
                                crate::types::AggregateValue::Scalar(
                                    (s * dr.rows_scanned as f64 + mem_sum) / total_scanned as f64,
                                )
                            }
                            (crate::types::AggregateValue::Scalar(s), _) => {
                                crate::types::AggregateValue::Scalar(
                                    s + if matches!(
                                        plan.aggregation,
                                        crate::query::ast::Aggregation::Count
                                    ) {
                                        mem_count as f64
                                    } else {
                                        mem_sum
                                    },
                                )
                            }
                            (
                                crate::types::AggregateValue::Empty,
                                crate::query::ast::Aggregation::Avg(_),
                            ) => crate::types::AggregateValue::Scalar(mem_sum / mem_count as f64),
                            (crate::types::AggregateValue::Empty, _) => {
                                crate::types::AggregateValue::Scalar(
                                    if matches!(
                                        plan.aggregation,
                                        crate::query::ast::Aggregation::Count
                                    ) {
                                        mem_count as f64
                                    } else {
                                        mem_sum
                                    },
                                )
                            }
                            _ => dr.value.clone(),
                        };

                        Ok(crate::types::QueryResult {
                            value: final_value,
                            rows_scanned: dr.rows_scanned + mem_count,
                            warnings: [
                                dr.warnings.clone(),
                                vec!["MemTable data included".to_string()],
                            ]
                            .concat(),
                            ..dr
                        })
                    } else {
                        Ok(dr)
                    }
                }
                Err(e) => Err(e),
            };

            match result {
                Ok(result) => {
                    println!("Execution Time: {:?}", start.elapsed());
                    println!("Rows Scanned: {}", result.rows_scanned);
                    println!("Storage Path: {:?}", result.storage_path);

                    match result.value {
                        crate::types::AggregateValue::Groups(groups) => {
                            for (k, v, c) in groups {
                                println!("  {} | {:.4} | Confidence: {:?}", k, v, c);
                            }
                        }
                        crate::types::AggregateValue::Scalar(s) => {
                            println!("  Result: {:.4} | Confidence: {:?}", s, result.confidence);
                        }
                        crate::types::AggregateValue::Empty => println!("  Empty result set."),
                    }
                    if !result.warnings.is_empty() {
                        println!("Warnings: {:?}", result.warnings);
                    }
                    if let Some(aqp) = &result.aqp {
                        println!(
                            "AQP: mode={} source={} sample_rate={:.4} sample_rows={} error={:.4}",
                            aqp.mode,
                            aqp.source,
                            aqp.sample_rate,
                            aqp.sample_rows,
                            aqp.estimated_error
                        );
                    }
                }
                Err(e) => println!("Query Execution Error: {:?}", e),
            }
        }
        Err(e) => println!("SQL Parse Error: {:?}", e),
    }
    Ok(())
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
        (AggregateValue::Scalar(s1), AggregateValue::Scalar(s2)) => {
            match aggregation {
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
            }
        }
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
                    .and_modify(|(existing_v, existing_rows, existing_c)| {
                        *existing_v = match aggregation {
                            crate::query::ast::Aggregation::Avg(_) => {
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
