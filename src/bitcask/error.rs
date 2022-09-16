use std::error::Error;

#[derive(Debug)]
pub enum KeyValueStoreError {
    NoInsert,
    PayloadTooLarge,
    ChecksumFailed,
    ItemDeleted,
}

impl std::fmt::Display for KeyValueStoreError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NoInsert => f.write_str("no insert"),
            Self::PayloadTooLarge => f.write_str("payload too large"),
            Self::ChecksumFailed => f.write_str("checksum failed"),
            Self::ItemDeleted => f.write_str("item deleted"),
        }
    }
}

impl Error for KeyValueStoreError {}

pub type Result<T> = std::result::Result<T, KeyValueStoreError>;
