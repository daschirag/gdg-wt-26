use crate::errors::StorageError;
use crate::types::SSTableMetadata;
use std::path::PathBuf;

pub struct Segment {
    pub path: PathBuf,
    pub metadata: SSTableMetadata,
}

impl Segment {
    pub fn new(path: PathBuf, metadata: SSTableMetadata) -> Self {
        Self { path, metadata }
    }

    pub fn load_from_dir(path: PathBuf) -> Result<Self, StorageError> {
        let meta_path = path.join("meta.toml");
        let content = std::fs::read_to_string(meta_path)?;
        let metadata: SSTableMetadata = toml::from_str(&content).map_err(|e| {
            StorageError::ReadError(std::io::Error::new(std::io::ErrorKind::Other, e))
        })?;

        Ok(Self { path, metadata })
    }
}
