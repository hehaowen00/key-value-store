use crate::bitcask::util::Size;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone)]
pub struct Options {
    pub base_dir: PathBuf,
    pub cache_size: u64,
    pub file_size_limit: u64,

    pub max_key_size: Option<u64>,
    pub max_value_size: Option<u64>,

    pub bytes_deleted_ratio: f64,
    pub entries_deleted_ratio: f64,
}

impl Options {
    pub fn new<P>(path: P) -> Self
    where
        P: AsRef<Path>,
    {
        Self {
            base_dir: path.as_ref().to_path_buf(),
            cache_size: 8,
            file_size_limit: Size::GB(2),
            max_key_size: None,
            max_value_size: None,
            bytes_deleted_ratio: 0.5,
            entries_deleted_ratio: 0.5,
        }
    }
}

pub struct Metrics {
    pub num_bytes_deleted: u64,
    pub num_entries_deleted: u64,

    pub bytes_deleted_limit: u64,
    pub entries_deleted_ratio: f64,
}

impl Metrics {
    pub fn new() -> Self {
        Self {
            num_entries_deleted: 0,
            num_bytes_deleted: 0,

            bytes_deleted_limit: 81920,
            entries_deleted_ratio: 0.5,
        }
    }
}
