use super::index::Entry;
use super::reader::{FileMap, Value};
use super::util::data_path;
use crate::Bitcask;
use std::collections::HashMap;
use std::path::PathBuf;

pub struct RangeIter<'a> {
    cache: HashMap<u64, FileMap>,
    range: Box<dyn Iterator<Item = (&'a Vec<u8>, &'a Entry)> + 'a>,
    root: PathBuf,
}

impl<'a> RangeIter<'a> {
    pub fn new(
        bitcask: &'a Bitcask,
        iter: Box<dyn Iterator<Item = (&'a Vec<u8>, &'a Entry)> + 'a>,
    ) -> Self {
        Self {
            cache: HashMap::new(),
            range: iter,
            root: bitcask.root().to_path_buf(),
        }
    }
}

impl<'a> Iterator for RangeIter<'a> {
    type Item = (Vec<u8>, Value);

    fn next(&mut self) -> Option<Self::Item> {
        match self.range.next() {
            Some((k, entry)) => {
                println!("{:?}", k);
                let f = match self.cache.get(&entry.file()) {
                    Some(f) => {
                        println!("some");
                        f
                    }
                    None => {
                        println!("none");
                        let path = data_path(&self.root, entry.file());
                        let map = FileMap::new(path);

                        self.cache.insert(entry.file(), map);
                        self.cache.get(&entry.file()).unwrap()
                    }
                };
                match f.get(entry.offset(), entry.size()) {
                    Ok(value) => Some((k.to_vec(), value)),
                    _ => None,
                }
            }
            None => None,
        }
    }
}
