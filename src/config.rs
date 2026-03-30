use serde::{Deserialize, Serialize};
use std::fs;
use crate::errors::ConfigError;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Column {
    pub name: String,
    pub r#type: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Schema {
    pub columns: Vec<Column>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    pub accuracy_target: f64,
    pub k: f64,
    pub seed: u64,
    pub bloom_fpr: f64,
    pub memtable_row_limit: u64,
    pub memtable_size_limit: u64,
    pub min_group_rows: u64,
    pub low_confidence_threshold: u64,
    pub verify_crc: bool,
    #[serde(default)]
    pub schema: Schema,
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
            verify_crc: true,
            schema: Schema::default(),
        }
    }
}

impl Config {
    pub fn load_from_file(path: &str) -> Result<Self, ConfigError> {
        let content = fs::read_to_string(path)?;
        let mut config: Config = toml::from_str(&content)?;
        
        // Try to load schema.toml if it exists in the 같은 directory or root
        if let Ok(schema_data) = fs::read_to_string("schema.toml") {
             if let Ok(schema) = toml::from_str::<Schema>(&schema_data) {
                 config.schema = schema;
             }
        }
        
        Ok(config)
    }

    pub fn load_schema(&mut self, path: &str) -> Result<(), ConfigError> {
        let content = fs::read_to_string(path)?;
        self.schema = toml::from_str(&content)?;
        Ok(())
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
