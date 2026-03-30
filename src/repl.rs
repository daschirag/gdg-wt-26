use rustyline::error::ReadlineError;
use rustyline::{DefaultEditor, Result};
use crate::config::Config;
use crate::query::parser::Parser;
use crate::query::executor::pipeline::Pipeline;
use crate::datagen::generate_dataset;
use crate::storage::sstable::writer::SSTableWriter;
use std::time::Instant;

pub fn start_repl(config: Config) -> Result<()> {
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
                        handle_gen(num, &config);
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
                            handle_query(query_str, &config);
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
    println!("  query \"<sql>\"  - Execute an approximate SQL query");
    println!("  help           - Show this help message");
    println!("  exit / quit    - Exit the REPL");
}

fn handle_gen(num_rows: usize, config: &Config) {
    println!("Generating {} rows...", num_rows);
    let start = Instant::now();
    let rows = generate_dataset(num_rows, config, None);
    
    let path_prefix = "data/sstable_";
    std::fs::create_dir_all("data").unwrap_or_default();
    
    // For simplicity in REPL, we flush to 10 SSTables like in main
    for i in 0..10 {
        let chunk_size = num_rows / 10;
        let start_idx = i * chunk_size;
        let end_idx = if i == 9 { num_rows } else { (i + 1) * chunk_size };
        let chunk = &rows[start_idx..end_idx];
        let path = format!("{}{}.aqe", path_prefix, i);
        let mut writer = SSTableWriter::new(&path).expect("Failed to create SSTableWriter");
        writer.write_sstable(chunk, config.bloom_fpr, config.verify_crc).expect("Failed to write SSTable");
    }
    println!("Done! Generated and flushed {} rows in {:?}", num_rows, start.elapsed());
}

fn handle_query(sql: &str, config: &Config) {
    let mut sstables = Vec::new();
    for i in 0..10 {
        sstables.push(format!("data/sstable_{}.aqe", i));
    }

    match Parser::parse(sql) {
        Ok(plan) => {
            let pipeline = Pipeline::new(sstables);
            let start = Instant::now();
            match pipeline.execute(&plan, config) {
                Ok(result) => {
                    println!("Execution Time: {:?}", start.elapsed());
                    println!("Rows Read: {}", result.rows_read);
                    println!("--- Profile ---");
                    println!("  Bloom:    {:.2}ms", result.profile.bloom_filter_ms);
                    println!("  Sampling: {:.2}ms", result.profile.sst_sampling_ms);
                    println!("  Disk IO:  {:.2}ms", result.profile.io_read_ms);
                    println!("  SerDe:    {:.2}ms", result.profile.deserialization_ms);
                    println!("  CRC:      {:.2}ms", result.profile.crc_verify_ms);
                    println!("  Filter:   {:.2}ms", result.profile.filtering_ms);
                    println!("  Agg:      {:.2}ms", result.profile.aggregation_ms);
                    
                    match result.value {
                        crate::types::AggregateValue::Groups(groups) => {
                            for (k, v, c) in groups {
                                println!("  {} | {:.4} | Confidence: {:.4}", k, v, c.0);
                            }
                        }
                        crate::types::AggregateValue::Scalar(s) => {
                            println!("  Result: {:.4} | Confidence: {:.4}", s, result.confidence.0);
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
}
