use serde::{Deserialize, Serialize};
use std::fs;
use crate::errors::ConfigError;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    pub accuracy_target: f64,      // default 0.9
    pub k: f64,                    // default 2.0
    pub seed: u64,                 // default 42
    pub bloom_fpr: f64,            // default 0.01
    pub memtable_row_limit: u64,   // default 10_000
    pub memtable_size_limit: u64,  // default 1_048_576 (1MB)
    pub min_group_rows: u64,       // default 50
    pub low_confidence_threshold: u64, // default 100
}

impl Default for Config {
    fn default() -> Self {
        Self {
            accuracy_target: 0.9,
            k: 2.0,
            seed: 42,
            bloom_fpr: 0.01,
            memtable_row_limit: 10_000,
            memtable_size_limit: 1_048_576,
            min_group_rows: 50,
            low_confidence_threshold: 100,
        }
    }
}

impl Config {
    pub fn load_from_file(path: &str) -> Result<Self, ConfigError> {
        let content = fs::read_to_string(path)?;
        let config: Config = toml::from_str(&content)?;
        Ok(config)
    }

    pub fn save_to_file(&self, path: &str) -> Result<(), ConfigError> {
        let content = toml::to_string_pretty(self).map_err(|e| {
             // toml::ser::Error doesn't easily convert to toml::de::Error (ConfigError::ParseError)
             // but we can just use FileError for simplicity or add another variant.
             ConfigError::FileError(std::io::Error::new(std::io::ErrorKind::Other, e))
        })?;
        fs::write(path, content)?;
        Ok(())
    }
}
