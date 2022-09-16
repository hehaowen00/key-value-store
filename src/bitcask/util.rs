use crc::{Crc, CRC_32_CKSUM};
use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};

pub const DATA_FILE_HEADER: &[u8] = b"KV-STORE - DATA FILE\0";
pub const INDEX_FILE_HEADER: &[u8] = b"KV-STORE - INDEX FILE\0";
pub const DATA_FILE_EXT: &str = "kv";
pub const INDEX_FILE_EXT: &str = "idx";

pub struct Size;

impl Size {
    #![allow(non_snake_case)]
    pub const fn B(n: u64) -> u64 {
        n
    }

    pub const fn KB(n: u64) -> u64 {
        n * 1024
    }

    pub const fn MB(n: u64) -> u64 {
        n * 1024 * 1024
    }

    pub const fn GB(n: u64) -> u64 {
        n * 1024 * 1024 * 1024
    }
}

pub fn data_path<P>(root: P, id: u64) -> PathBuf
where
    P: AsRef<Path>,
{
    root.as_ref().join(format!("{}.{}", id, DATA_FILE_EXT))
}

pub fn idx_path<P>(root: P) -> PathBuf
where
    P: AsRef<Path>,
{
    root.as_ref().join(format!("db.{}", INDEX_FILE_EXT))
}

pub fn compute_size(k: &[u8], v: &[u8]) -> u64 {
    (std::mem::size_of::<u32>()
        + std::mem::size_of::<u64>()
        + std::mem::size_of::<u8>()
        + std::mem::size_of::<u64>()
        + std::mem::size_of::<u64>()
        + k.len()
        + v.len()) as u64
}

pub fn crc_init() -> Crc<u32> {
    Crc::<u32>::new(&CRC_32_CKSUM)
}

pub fn walk_dir<P>(path: P) -> Vec<u64>
where
    P: AsRef<Path>,
{
    let mut hs = BTreeSet::new();
    let mut dir = fs::read_dir(path.as_ref()).expect("read dir");

    while let Some(Ok(e)) = dir.next() {
        if e.file_type().expect("file type").is_dir() {
            continue;
        }
        if let Some(name) = e.path().file_name() {
            if let Some(name) = name.to_str() {
                let xs: Vec<_> = name.split(".").collect();
                if xs.len() != 2 {
                    continue;
                }
                let ext = xs[1];
                if ext == "kv" {
                    let file_id: u64 = match xs[0].parse() {
                        Ok(id) => id,
                        _ => continue,
                    };
                    hs.insert(file_id);
                }
            }
        }
    }

    hs.into_iter().collect()
}

pub fn delete_files<P>(path: P)
where
    P: AsRef<Path>,
{
    let mut dir = fs::read_dir(path.as_ref()).expect("read dir");
    while let Some(Ok(e)) = dir.next() {
        if e.file_type().expect("file type").is_dir() {
            continue;
        }

        std::fs::remove_file(e.path()).expect("delete file");
    }
}
