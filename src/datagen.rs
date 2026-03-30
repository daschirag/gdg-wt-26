use rand::prelude::*;
use rand_chacha::ChaCha8Rng;
use crate::types::{RowDisk, Value};
use std::fs::File;
use std::io::Write;

pub fn generate_dataset(n: usize, config: &crate::config::Config, csv_path: Option<&str>) -> (Vec<RowDisk>, crate::types::GenerationProfile) {
    let mut profile = crate::types::GenerationProfile::default();
    let start_total = std::time::Instant::now();
    
    let mut rng = ChaCha8Rng::seed_from_u64(config.seed);
    let mut data = Vec::with_capacity(n);

    let start_gen = std::time::Instant::now();
    let start_ts = 1577836800000i64;
    let end_ts = 1735603200000i64;

    let mut csv_file = csv_path.map(|p| File::create(p).unwrap());
    if let Some(f) = &mut csv_file {
        let headers: Vec<String> = config.schema.columns.iter().map(|c| c.name.clone()).collect();
        writeln!(f, "{}", headers.join(",")).unwrap();
    }

    for i in 0..n {
        let mut values = Vec::with_capacity(config.schema.columns.len());
        
        for col in &config.schema.columns {
            let val = match col.name.as_str() {
                "user_id" => Value::Int(i as i64),
                "status" => {
                    let r = rng.gen_range(0..100);
                    if r < 70 { Value::Int(0) } else if r < 90 { Value::Int(1) } else { Value::Int(2) }
                }
                "country" => Value::Int(rng.gen_range(0..5)),
                "timestamp" => Value::Int(rng.gen_range(start_ts..=end_ts)),
                _ => {
                    match col.r#type.as_str() {
                        "u64" | "i64" | "u32" | "u8" | "i8" => Value::Int(rng.gen_range(0..1000)),
                        "f64" | "f32" => Value::Float(rng.gen_range(0.0..1000.0)),
                        _ => Value::String("dynamic_val".to_string()),
                    }
                }
            };
            values.push(val);
        }

        if let Some(f) = &mut csv_file {
             let row_str: Vec<String> = values.iter().map(|v| match v {
                 Value::Int(i) => i.to_string(),
                 Value::Float(f) => f.to_string(),
                 Value::String(s) => s.clone(),
             }).collect();
             writeln!(f, "{}", row_str.join(",")).unwrap();
        }

        data.push(RowDisk {
            version: 1,
            crc: 0,
            seq: 0,
            values,
        });
    }

    profile.random_gen_ms = start_gen.elapsed().as_secs_f64() * 1000.0;
    profile.allocation_ms = start_total.elapsed().as_secs_f64() * 1000.0 - profile.random_gen_ms;

    (data, profile)
}
