use thiserror::Error;

#[derive(Error, Debug)]
pub enum StorageError {
    #[error("Magic bytes mismatch")]
    MagicMismatch,
    #[error("Row count mismatch: expected {expected}, actual {actual}")]
    RowCountMismatch { expected: u64, actual: u64 },
    #[error("Write error: {0}")]
    WriteError(#[from] std::io::Error),
    #[error("Read error: {0}")]
    ReadError(std::io::Error),
    #[error("Invalid format: {0}")]
    InvalidFormat(String),
    #[error("Metadata schema mismatch")]
    MetadataSchemaMismatch,
    #[error("Metadata corrupt")]
    MetadataCorrupt,
}

#[derive(Error, Debug)]
pub enum QueryError {
    #[error("Parse error: {0}")]
    ParseError(String),
    #[error("Unknown column: {0}")]
    UnknownColumn(String),
    #[error("Unsupported operation: {0}")]
    UnsupportedOperation(String),
    #[error("Storage error: {0}")]
    StorageError(#[from] StorageError),
}

#[derive(Error, Debug)]
pub enum SamplingError {
    #[error("Empty sample obtained")]
    EmptySample,
    #[error("Accuracy out of range: {0}")]
    AccuracyOutOfRange(f64),
    #[error("K parameter out of range: {0}")]
    KOutOfRange(f64),
}

#[derive(Error, Debug)]
pub enum ConfigError {
    #[error("Config parse error: {0}")]
    ParseError(#[from] toml::de::Error),
    #[error("Config file error: {0}")]
    FileError(#[from] std::io::Error),
}

#[derive(Error, Debug)]
pub enum IngestionError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Schema mismatch: expected {expected} columns, got {got}")]
    SchemaMismatch { expected: usize, got: usize },
    #[error("Storage error: {0}")]
    Storage(#[from] StorageError),
    #[error("Query error: {0}")]
    Query(#[from] QueryError),
    #[error("Config error: {0}")]
    Config(String),
}
