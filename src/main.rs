use lsm::*;
use std::time::Instant;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = match Config::load_from_file("config.toml") {
        Ok(c) => c,
        Err(e) => panic!("Failed to load config.toml: {:?}", e)
    };
    println!("AQE Prototype - Phase 1 Verification");
    println!("------------------------------------");
    println!("Config: Accuracy Target={}, k={}, Seed={}", config.accuracy_target, config.k, config.seed);

    let args: Vec<String> = std::env::args().collect();
    if args.len() < 2 {
        println!("Available commands: gen <num> [csv_path], query <sql>");
        println!("No arguments provided. Starting interactive REPL...");
        return Ok(repl::start_repl(config)?);
    }
    let command = &args[1];

    if command == "verify" {
        println!("Verifying all SSTables for CRC integrity...");
        for i in 0..10 {
            let path = format!("data/sstable_{}.aqe", i);
            if !std::path::Path::new(&path).exists() { continue; }
            let reader = storage::sstable::reader::SSTableReader::new(&path);
            match reader.get_metadata() {
                Ok(meta) => println!("  {}: OK ({} rows, ts: {} - {}, v: {})", 
                    path, meta.row_count, meta.min_ts, meta.max_ts, meta.schema_version),
                Err(e) => println!("  {}: FAILED: {:?}", path, e),
            }
        }
        return Ok(());
    }

    if command == "gen" {
        let num_rows: usize = args.get(2).and_then(|s| s.parse().ok()).unwrap_or(100_000);
        let csv_dump_path = args.get(3).map(|s| s.as_str());

        // 1. Data Generation
        let start_gen = Instant::now();
        let rows = generate_dataset(num_rows, &config, csv_dump_path);
        let duration_gen = start_gen.elapsed();

        // 2. Storage - Flush to 10 SSTables
        let start_flush = Instant::now();
        std::fs::create_dir_all("data")?;
        for i in 0..10 {
            let chunk_size = num_rows / 10;
            let chunk = &rows[i * chunk_size..(i + 1) * chunk_size];
            let path = format!("data/sstable_{}.aqe", i);
            let mut writer = storage::sstable::writer::SSTableWriter::new(&path)?;
            writer.write_sstable(chunk, config.bloom_fpr, config.verify_crc)?;
        }
        let duration_flush = start_flush.elapsed();
        println!("GEN_TIME: {:?}", duration_gen);
        println!("FLUSH_TIME: {:?}", duration_flush);

        return Ok(());
    } else if command == "query" {
        let sql = args.get(2).expect("Missing SQL string");
        let mut sstables = Vec::new();
        for i in 0..10 {
            sstables.push(format!("data/sstable_{}.aqe", i));
        }

        let plan = query::parser::Parser::parse(sql)?;
        let pipeline = query::executor::pipeline::Pipeline::new(sstables);
        
        let start = Instant::now();
        let result = pipeline.execute(&plan, &config)?;
        let duration = start.elapsed();

        println!("Execution Time: {:?}", duration);
        println!("Rows Read: {}", result.rows_read);
        println!("--- Performance Profile ---");
        println!("  Bloom Filter:     {:.2}ms", result.profile.bloom_filter_ms);
        println!("  SST Sampling:     {:.2}ms", result.profile.sst_sampling_ms);
        println!("  Disk IO (Read):   {:.2}ms", result.profile.io_read_ms);
        println!("  Deserialization:  {:.2}ms", result.profile.deserialization_ms);
        println!("  CRC Verify:       {:.2}ms", result.profile.crc_verify_ms);
        println!("  Filtering:        {:.2}ms", result.profile.filtering_ms);
        println!("  Aggregation:      {:.2}ms", result.profile.aggregation_ms);
        println!("---------------------------");

        match result.value {
            AggregateValue::Groups(groups) => {
                for (k, v, c) in groups {
                    println!("GROUP: {}|{}|{:.4}", k, v, c.0);
                }
            }
            AggregateValue::Scalar(s) => {
                println!("SCALAR: {}|{:.4}", s, result.confidence.0);
            }
            AggregateValue::Empty => {
                println!("EMPTY");
            }
        }
        
        if !result.warnings.is_empty() {
            println!("Warnings: {:?}", result.warnings);
        }
        return Ok(());
    }

    Ok(())
}
