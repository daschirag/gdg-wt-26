use rustyline::error::ReadlineError;
use rustyline::{DefaultEditor, Result};
use crate::config::Config;
use crate::query::parser::Parser;
use crate::query::executor::pipeline::Pipeline;
use crate::datagen::generate_dataset;
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

    loop {
        let readline = rl.readline("aqe> ");
        match readline {
            Ok(line) => {
                rl.add_history_entry(line.as_str())?;
                let parts: Vec<&str> = line.trim().split_whitespace().collect();
                if parts.is_empty() { continue; }

                match parts[0].to_lowercase().as_str() {
                    "exit" | "quit" => break,
                    "help" => print_help(),
                    "gen" => {
                        let num = parts.get(1).and_then(|s| s.parse::<usize>().ok()).unwrap_or(100_000);
                        handle_gen(num, &config, &storage);
                    }
                    "compact" => {
                        handle_compact(&storage);
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
                            if let Err(e) = handle_query(query_str, &config, &storage) {
                                println!("Query Error: {:?}", e);
                            }
                        }
                    }
                    _ => println!("Unknown command: {}. Type 'help' for available commands.", parts[0]),
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
    println!("  gen <num>      - Generate <num> synthetic rows and flush to SSTables");
    println!("  compact        - Manually trigger background compaction");
    println!("  query \"<sql>\"  - Execute an approximate SQL query");
    println!("  help           - Show this help message");
    println!("  exit / quit    - Exit the REPL");
}

fn handle_compact(storage: &StorageManager) {
    println!("Triggering manual compaction...");
    let start = Instant::now();
    match crate::storage::compaction::scheduler::CompactionScheduler::compact_now(storage) {
        Ok(tasks) => {
            if tasks > 0 {
                println!("Compaction successful! Created {} segments in {:?}", tasks, start.elapsed());
            } else {
                println!("No compaction tasks needed at this time (checked in {:?})", start.elapsed());
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
        let end_idx = if i == 9 { num_rows } else { (i + 1) * chunk_size };
        let chunk = &rows[start_idx..end_idx];
        let path = format!("{}{}.aqe", path_prefix, i);
        crate::storage::sstable::writer::SSTableWriter::write_sstable_to_file(&path, chunk, config).expect("Failed to write Row SSTable");
    }
    println!("Done! Generated and flushed {} rows (Row-based) in {:?}", num_rows, start.elapsed());
}

fn handle_query(sql: &str, config: &Config, storage: &StorageManager) -> std::result::Result<(), Box<dyn std::error::Error>> {
    let mut sstables = Vec::new();
    for i in 0..10 {
        sstables.push(format!("data/sstable_{}.aqe", i));
    }

    match Parser::parse(sql) {
        Ok(plan) => {
            let execution_path = crate::query::planner::QueryPlanner::plan(&plan, storage);
            
            let start = Instant::now();
            
            // 1. Scan MemTable (Exact)
            let mem_rows = storage.scan_memtable(&plan);
            let mem_count = mem_rows.len() as u64;
            let mut mem_sum = 0.0;
            for row in &mem_rows {
                 match &plan.aggregation {
                     crate::query::ast::Aggregation::Sum(col) | crate::query::ast::Aggregation::Avg(col) => {
                         if let Some(crate::types::Value::Int(i)) = crate::types::get_value(row, col, config) {
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
                crate::query::planner::ExecutionPath::FastPath { segments, column, aggregation } => {
                    let mut total_count = 0;
                    let mut total_sum = 0.0;
                    let warnings = vec!["Fast-path: Using pre-computed metadata".to_string()];
                    
                    for path in segments {
                        if let Ok(reader) = crate::storage::columnar::reader::ColumnarReader::new(path.into()) {
                            total_count += reader.metadata.row_count;
                            if let Some(col_name) = &column {
                                if let Some(col_meta) = reader.metadata.columns.get(col_name) {
                                    total_sum += col_meta.sum;
                                }
                            }
                        }
                    }

                    let final_val = match aggregation {
                        crate::query::ast::Aggregation::Count => crate::types::AggregateValue::Scalar(total_count as f64),
                        crate::query::ast::Aggregation::Sum(_) => crate::types::AggregateValue::Scalar(total_sum),
                        crate::query::ast::Aggregation::Avg(_) => if total_count > 0 { crate::types::AggregateValue::Scalar(total_sum / total_count as f64) } else { crate::types::AggregateValue::Empty },
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
                    })
                }
                crate::query::planner::ExecutionPath::Row { sstables } => {
                    let pipeline = Pipeline::new(sstables);
                    pipeline.execute(&plan, config, mask, skip_dedup)
                }
                crate::query::planner::ExecutionPath::Columnar { segments, columns_needed } => {
                    let pipeline = crate::query::executor::col_pipeline::ColumnarPipeline::new(segments);
                    pipeline.execute(&plan, &columns_needed, config, mask, skip_dedup)
                }
                crate::query::planner::ExecutionPath::Mixed { sstables, segments, columns_needed } => {
                    let row_pipeline = Pipeline::new(sstables);
                    let col_pipeline = crate::query::executor::col_pipeline::ColumnarPipeline::new(segments);
                    
                    let res_row = row_pipeline.execute(&plan, config, mask, skip_dedup)?;
                    let res_col = col_pipeline.execute(&plan, &columns_needed, config, mask, skip_dedup)?;
                    
                    // Simple merge for Scalar (identical to main.rs)
                    let merged_value = match (&res_row.value, &res_col.value) {
                         (crate::types::AggregateValue::Scalar(s1), crate::types::AggregateValue::Scalar(s2)) => {
                            match &plan.aggregation {
                                crate::query::ast::Aggregation::Avg(_) => {
                                    let total_scanned = res_row.rows_scanned + res_col.rows_scanned;
                                    if total_scanned > 0 {
                                        crate::types::AggregateValue::Scalar((s1 * res_row.rows_scanned as f64 + s2 * res_col.rows_scanned as f64) / total_scanned as f64)
                                    } else {
                                        crate::types::AggregateValue::Empty
                                    }
                                }
                                _ => crate::types::AggregateValue::Scalar(s1 + s2),
                            }
                        }
                        (crate::types::AggregateValue::Scalar(s), crate::types::AggregateValue::Empty) | (crate::types::AggregateValue::Empty, crate::types::AggregateValue::Scalar(s)) => {
                            crate::types::AggregateValue::Scalar(*s)
                        }
                        _ => res_row.value.clone(),
                    };

                    let merged_confidence = if res_row.confidence == crate::types::ConfidenceFlag::Low || res_col.confidence == crate::types::ConfidenceFlag::Low {
                        crate::types::ConfidenceFlag::Low
                    } else if res_row.confidence == crate::types::ConfidenceFlag::Exact && res_col.confidence == crate::types::ConfidenceFlag::Exact {
                        crate::types::ConfidenceFlag::Exact
                    } else {
                        crate::types::ConfidenceFlag::High
                    };

                    Ok(crate::types::QueryResult {
                        value: merged_value,
                        confidence: merged_confidence,
                        warnings: [res_row.warnings, res_col.warnings].concat(),
                        storage_path: crate::types::StoragePath::Mixed,
                        rows_scanned: res_row.rows_scanned + res_col.rows_scanned,
                        sampling_rate: (res_row.sampling_rate + res_col.sampling_rate) / 2.0,
                        estimated_variance: 0.0,
                        profile: res_row.profile,
                    })
                }
            };

            // 3. Robust Merge (LSM Read Path)
            let result = match disc_result {
                Ok(dr) => {
                    if mem_count > 0 {
                        let final_value = match (&dr.value, &plan.aggregation) {
                            (crate::types::AggregateValue::Scalar(s), crate::query::ast::Aggregation::Avg(_)) => {
                                let total_scanned = dr.rows_scanned + mem_count;
                                crate::types::AggregateValue::Scalar((s * dr.rows_scanned as f64 + mem_sum) / total_scanned as f64)
                            }
                            (crate::types::AggregateValue::Scalar(s), _) => {
                                crate::types::AggregateValue::Scalar(s + if matches!(plan.aggregation, crate::query::ast::Aggregation::Count) { mem_count as f64 } else { mem_sum })
                            }
                            (crate::types::AggregateValue::Empty, crate::query::ast::Aggregation::Avg(_)) => {
                                crate::types::AggregateValue::Scalar(mem_sum / mem_count as f64)
                            }
                            (crate::types::AggregateValue::Empty, _) => {
                                crate::types::AggregateValue::Scalar(if matches!(plan.aggregation, crate::query::ast::Aggregation::Count) { mem_count as f64 } else { mem_sum })
                            }
                            _ => dr.value.clone(),
                        };
                        
                        Ok(crate::types::QueryResult {
                            value: final_value,
                            rows_scanned: dr.rows_scanned + mem_count,
                            warnings: [dr.warnings.clone(), vec!["MemTable data included".to_string()]].concat(),
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
                }
                Err(e) => println!("Query Execution Error: {:?}", e),
            }
        }
        Err(e) => println!("SQL Parse Error: {:?}", e),
    }
    Ok(())
}
