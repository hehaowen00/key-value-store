use super::index::Entry;
use super::reader::{FileMap, Value};
use super::util::data_path;
use crate::Bitcask;
use std::collections::btree_map::Range;
use std::collections::HashMap;
use std::path::PathBuf;

pub struct RangeIter<'a> {
    cache: HashMap<u64, FileMap>,
    range: Range<'a, Vec<u8>, Entry>,
    root: PathBuf,
}

impl<'a> RangeIter<'a> {
    pub fn new(bitcask: &'a Bitcask, start: &[u8], end: &[u8]) -> Self {
        Self {
            cache: HashMap::new(),
            range: bitcask.index.range(start, end),
            root: bitcask.root().to_path_buf(),
        }
    }
}

impl<'a> Iterator for RangeIter<'a> {
    type Item = (&'a Vec<u8>, Value);

    fn next(&mut self) -> Option<Self::Item> {
        match self.range.next() {
            Some((k, entry)) => {
                let f = match self.cache.get(&entry.file()) {
                    Some(f) => f,
                    None => {
                        let path = data_path(&self.root, entry.file());
                        let map = FileMap::new(path);

                        self.cache.insert(entry.file(), map);
                        self.cache.get(&entry.file()).unwrap()
                    }
                };
                match f.get(entry.offset(), entry.size()) {
                    Ok(value) => Some((k, value)),
                    _ => None,
                }
            }
            None => None,
        }
    }
}
