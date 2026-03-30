use rand::prelude::*;
use rand_chacha::ChaCha8Rng;
use crate::types::RowDisk;
use std::fs::File;
use std::io::Write;

pub fn generate_dataset(n: usize, seed: u64, csv_path: Option<&str>) -> Vec<RowDisk> {
    let mut rng = ChaCha8Rng::seed_from_u64(seed);
    let mut data = Vec::with_capacity(n);

    let start_ts = 1577836800000u64;
    let end_ts = 1735603200000u64;

    let mut csv_file = csv_path.map(|p| File::create(p).unwrap());
    if let Some(f) = &mut csv_file {
        writeln!(f, "user_id,status,country,timestamp").unwrap();
    }

    for i in 0..n {
        let status_roll = rng.gen_range(0..100);
        let status = if status_roll < 70 {
            0
        } else if status_roll < 90 {
            1
        } else {
            2
        };

        let country = rng.gen_range(0..5);
        let timestamp = rng.gen_range(start_ts..=end_ts);

        if let Some(f) = &mut csv_file {
            writeln!(f, "{},{},{},{}", i, status, country, timestamp).unwrap();
        }

        data.push(RowDisk {
            user_id: i as u64,
            status,
            country,
            timestamp,
        });
    }

    data
}
