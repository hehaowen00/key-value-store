use super::disk::DiskTable;
use super::reader::{EntryReader, IndexReader};
use super::util::INDEX_FILE_HEADER;
use crate::api::WriteExt;
use std::collections::btree_map::{Iter, Keys, Range};
use std::collections::BTreeMap;
use std::fs::OpenOptions;
use std::ops::Bound;
use std::path::Path;

#[derive(Debug)]
pub struct Index {
    pub(crate) inner: BTreeMap<Vec<u8>, Entry>,
}

impl Index {
    pub fn new() -> Self {
        Self {
            inner: BTreeMap::new(),
        }
    }

    pub fn open<P>(&mut self, path: P)
    where
        P: AsRef<Path>,
    {
        let mut ir = IndexReader::new(path);
        while let Some(entry) = ir.next() {
            self.insert(
                entry.key(),
                entry.timestamp(),
                entry.file(),
                entry.offset(),
                entry.size(),
            );
        }
    }

    pub fn from_data<P>(&mut self, path: P, file_id: u64)
    where
        P: AsRef<Path>,
    {
        let mut reader = EntryReader::new(path.as_ref());

        while let Some(entry) = reader.next() {
            if let Some(curr) = self.get(entry.key()) {
                if curr.timestamp() > entry.timestamp() {
                    continue;
                }
            }

            self.insert(
                entry.key(),
                entry.timestamp(),
                file_id,
                entry.offset(),
                entry.size(),
            );
        }
    }

    pub fn clear(&mut self) {
        self.inner.clear();
    }

    pub fn keys<'a>(&'a self) -> Keys<'a, Vec<u8>, Entry> {
        self.inner.keys()
    }

    pub fn entries<'a>(&'a self) -> Iter<'a, Vec<u8>, Entry> {
        self.inner.iter()
    }

    pub fn range<'a>(&'a self, start: &[u8], end: &[u8]) -> Range<'a, Vec<u8>, Entry> {
        self.inner.range((
            Bound::Included(start.to_vec()),
            Bound::Included(end.to_vec()),
        ))
    }

    pub fn get<'a>(&'a self, key: &[u8]) -> Option<&Entry> {
        self.inner.get(key)
    }

    pub fn insert(
        &mut self,
        key: &[u8],
        timestamp: u64,
        file: u64,
        offset: u64,
        size: u64,
    ) -> Option<Entry> {
        self.inner
            .insert(key.to_vec(), Entry::from(timestamp, file, offset, size))
    }

    pub fn delete(&mut self, key: &[u8]) -> Option<Entry> {
        let entry = self.inner.remove(key)?;
        Some(entry)
    }

    pub fn write<P>(&self, path: P)
    where
        P: AsRef<Path>,
    {
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(path.as_ref())
            .expect("file open");

        let mut disk = DiskTable::open(file);
        disk.write(INDEX_FILE_HEADER).expect("write header");

        for (k, v) in self.inner.iter() {
            disk.write_u64(k.len() as u64).expect("write u64");
            disk.write(k).expect("write key");
            disk.write_u64(v.timestamp()).expect("write u64");
            disk.write_u64(v.file()).expect("write u64");
            disk.write_u64(v.offset()).expect("write u64");
            disk.write_u64(v.size()).expect("write u64");
        }

        disk.flush();
    }
}

#[derive(Debug)]
pub struct Entry {
    file: u64,
    timestamp: u64,
    offset: u64,
    size: u64,
}

impl Entry {
    pub fn from(timestamp: u64, file: u64, offset: u64, size: u64) -> Self {
        Self {
            timestamp,
            file,
            offset,
            size,
        }
    }

    pub fn timestamp(&self) -> u64 {
        self.timestamp
    }

    pub fn file(&self) -> u64 {
        self.file
    }

    pub fn offset(&self) -> u64 {
        self.offset
    }

    pub fn size(&self) -> u64 {
        self.size
    }
}
