pub(crate) mod disk;
pub(crate) mod error;
pub(crate) mod index;
pub(crate) mod iter;
pub(crate) mod reader;
pub(crate) mod settings;
pub(crate) mod util;

use self::disk::DiskTable;
use self::error::Result;
use self::index::Index;
use self::iter::RangeIter;
use self::reader::{FileMap, Value};
use self::settings::{Metrics, Options};
use self::util::{compute_size, data_path, delete_files, idx_path, walk_dir, DATA_FILE_HEADER};
use crate::api::WriteExt;
use chrono::Utc;
use std::collections::HashMap;
use std::fs::OpenOptions;
use std::io::{Read, Seek, SeekFrom, Write};
use std::ops::RangeBounds;
use std::path::Path;

pub struct Bitcask {
    pub(crate) index: Index,
    disk: DiskTable,
    counter: u64,
    stats: Metrics,
    opts: Options,
}

impl Bitcask {
    pub fn open(opts: Options) -> Self {
        let root = opts.base_dir.to_path_buf();

        if !root.exists() {
            std::fs::create_dir_all(&root).expect("create dir");
            return Self::create(opts);
        }

        let mut ids = walk_dir(&root);

        if ids.len() == 0 {
            return Self::create(opts);
        }

        ids.sort();

        let mut index = Index::new();
        let i_path = idx_path(&root);
        let last_id = ids.pop().unwrap();

        match i_path.exists() {
            true => index.open(&i_path),
            false => {
                for id in ids {
                    let d_path = data_path(&root, id);
                    index.from_data(d_path, id);
                }
            }
        }

        let d_path = data_path(&root, last_id);

        let data_file = OpenOptions::new()
            .write(true)
            .append(true)
            .open(&d_path)
            .expect("open data file");

        let disk = DiskTable::open(data_file);

        match i_path.exists() {
            true => {}
            false => {
                let d_path = data_path(&root, last_id);
                index.from_data(d_path, last_id);
                index.write(&i_path);
            }
        }

        Self {
            index,
            disk,
            counter: last_id,

            opts,
            stats: Metrics::new(),
        }
    }

    pub fn create(opts: Options) -> Self {
        let counter = 0;

        let root = opts.base_dir.to_path_buf();
        let d_path = data_path(&root, counter);

        let index = Index::new();
        let disk = new_data_file(&d_path);
        let stats = Metrics::new();

        Self {
            index,
            disk,
            counter,

            opts,
            stats,
        }
    }

    pub fn close(mut self) {
        self.flush();
    }

    pub fn root(&self) -> &Path {
        &self.opts.base_dir
    }

    pub fn exists(&self, key: &[u8]) -> bool {
        match self.index.get(key) {
            Some(_) => true,
            None => false,
        }
    }

    pub fn flush(&mut self) {
        self.disk.flush();
        let path = idx_path(&self.opts.base_dir);
        self.index.write(&path);
    }

    pub fn keys<'a>(&'a self) -> impl Iterator<Item = &'a Vec<u8>> {
        self.index.keys()
    }

    pub fn keys_range<'a, R>(
        &'a self,
        start: &[u8],
        end: &[u8],
    ) -> impl Iterator<Item = &'a Vec<u8>> {
        self.index.range(start, end).map(|(k, _)| k)
    }

    pub fn get(&self, key: &[u8]) -> Option<Value> {
        let entry = self.index.get(key)?;
        let path = data_path(self.root(), entry.file());

        let map = FileMap::new(path);
        let value = map.get(entry.offset(), entry.size());

        match value {
            Ok(value) => Some(value),
            Err(e) => {
                println!("{:?}", e);
                None
            }
        }
    }

    pub fn insert(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        let ts = Utc::now().timestamp() as u64;

        if self.disk.position() + compute_size(key, value) > self.opts.file_size_limit {
            self.swap_file();
        }

        let (offset, size) = self.disk.append_entry(ts, key, value);
        let file_id = self.counter;

        match self.index.insert(key, ts, file_id, offset, size) {
            Some(prev) => {
                self.stats.num_entries_deleted += 1;
                self.stats.num_bytes_deleted += prev.size();
            }
            None => {}
        };

        self.flush();

        Ok(())
    }

    pub fn insert_if_none(&mut self, key: &[u8], value: &[u8]) -> Result<bool> {
        match self.exists(key) {
            true => return Ok(false),
            false => {
                self.insert(key, value)?;
                Ok(true)
            }
        }
    }

    pub fn delete(&mut self, key: &[u8]) -> bool {
        let res = match self.index.delete(key) {
            Some(e) => {
                let ts = Utc::now().timestamp() as u64;
                self.disk.delete(ts, key);
                self.stats.num_bytes_deleted += e.size();
                self.stats.num_entries_deleted += 1;
                true
            }
            None => false,
        };

        res
    }

    pub fn iter<'a, R>(&'a self, range: R) -> RangeIter<'a>
    where
        R: RangeBounds<Vec<u8>>,
    {
        RangeIter::new(&self, Box::new(self.index.inner.range(range)))
    }

    pub fn merge(&mut self) {
        let temp = self.opts.base_dir.join("temp");

        if !temp.exists() {
            std::fs::create_dir_all(&temp).expect("");
        }

        if self.stats.num_bytes_deleted == 0 {
            return;
        }

        let mut counter = 0;

        let d_path = data_path(&temp, counter);
        let mut disk = new_data_file(d_path);

        let mut index = Index::new();
        let mut files: HashMap<u64, std::fs::File> = HashMap::new();

        for (k, v) in self.index.entries() {
            if disk.position() + v.size() > self.opts.file_size_limit {
                let i_path = idx_path(&temp);
                index.write(i_path);
                counter += 1;

                disk = new_data_file(data_path(&temp, counter));
                index.clear();
            }

            match files.get_mut(&v.file()) {
                Some(f) => {
                    f.seek(SeekFrom::Start(v.offset())).expect("seek");

                    let mut buf = vec![0u8; v.size() as usize];
                    f.read_exact(&mut buf).expect("read exact");

                    let offset = disk.write(&buf).expect("write buf");
                    index.insert(k, v.timestamp(), counter, offset, v.size());
                }
                None => {
                    let mut f = OpenOptions::new()
                        .read(true)
                        .open(data_path(&self.opts.base_dir, v.file()))
                        .expect("");

                    f.seek(SeekFrom::Start(v.offset())).expect("seek");
                    let mut buf = vec![0u8; v.size() as usize];
                    f.read_exact(&mut buf).expect("read exact");

                    let offset = disk.write(&buf).expect("write buf");
                    index.insert(k, v.timestamp(), counter, offset, v.size());

                    files.insert(v.file(), f);
                }
            }
        }

        let i_path = idx_path(&temp);
        index.write(i_path);
        disk.flush();
        delete_files(&self.opts.base_dir);

        self.disk = disk;
        self.counter = counter;
        self.index = index;

        let src_path = idx_path(&temp);
        let dest_path = idx_path(&self.opts.base_dir);
        std::fs::rename(src_path, dest_path).unwrap();

        for i in 0..counter + 1 {
            let src_path = data_path(&temp, i);
            let dest_path = data_path(&self.opts.base_dir, i);
            std::fs::rename(src_path, dest_path).unwrap();
        }

        self.stats.num_bytes_deleted = 0;
        self.stats.num_entries_deleted = 0;
    }

    fn swap_file(&mut self) {
        self.flush();

        let path = idx_path(&self.root());
        self.index.write(path);
        self.counter += 1;

        let data_path = data_path(&self.root(), self.counter);
        let disk = new_data_file(data_path);
        self.disk = disk;
    }
}

impl Drop for Bitcask {
    fn drop(&mut self) {
        self.flush();
    }
}

fn new_data_file<P>(path: P) -> DiskTable
where
    P: AsRef<Path>,
{
    let mut file = OpenOptions::new()
        .create(true)
        .write(true)
        .append(true)
        .open(path.as_ref())
        .expect("open file");

    file.write_all(DATA_FILE_HEADER).expect("write header");
    file.flush().expect("flush");

    DiskTable::open(file)
}
