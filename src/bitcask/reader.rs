use super::error::KeyValueStoreError;
use crate::api::ReadExt;
use crate::bitcask::util::{crc_init, DATA_FILE_HEADER, INDEX_FILE_HEADER};
use memmap::Mmap;
use std::fs::{File, OpenOptions};
use std::io::{BufReader, Cursor, Read, Seek};
use std::path::{Path, PathBuf};
use std::sync::RwLock;

pub struct FileMap {
    mmap: RwLock<memmap::Mmap>,
    path: PathBuf,
}

impl FileMap {
    pub fn new<P>(path: P) -> Self
    where
        P: AsRef<Path>,
    {
        let f = OpenOptions::new()
            .read(true)
            .open(&path)
            .expect("open file");

        let mmap = unsafe { Mmap::map(&f).expect("mmap") };

        Self {
            mmap: RwLock::new(mmap),
            path: path.as_ref().to_path_buf(),
        }
    }

    pub fn get(&self, offset: u64, size: u64) -> Result<Value, KeyValueStoreError> {
        {
            let guard = self.mmap.read().expect("read guard");

            if offset + size > guard.len() as u64 {
                let f = OpenOptions::new()
                    .read(true)
                    .open(&self.path)
                    .expect("open file");

                let mmap = unsafe { memmap::Mmap::map(&f).expect("mmap") };

                let mut guard = self.mmap.write().expect("write guard");
                *guard = mmap;
            }
        }

        let guard = self.mmap.read().expect("");
        let bytes = &guard[offset as usize..(offset + size) as usize];

        let mut cursor = Cursor::new(bytes);

        let checksum = cursor.read_u32().expect("read u32");
        let crc = crc_init();

        let mut digest = crc.digest();
        digest.update(&bytes[4..]);
        let computed = digest.finalize();

        if computed != checksum {
            return Err(KeyValueStoreError::ChecksumFailed);
        }

        let timestamp = cursor.read_u64().expect("read u64");
        let deleted = cursor.read_u8().expect("read u8");
        let key_len = cursor.read_u64().expect("read u64");

        if deleted == u8::MAX {
            return Err(KeyValueStoreError::ItemDeleted);
        }

        let value_len = cursor.read_u64().expect("read u64");

        cursor
            .seek(std::io::SeekFrom::Current(key_len as i64))
            .expect("seek");

        let mut value = vec![0u8; value_len as usize];
        cursor.read_exact(&mut value).expect("read buf");

        let value = Value::from(timestamp, value);

        Ok(value)
    }
}

pub struct EntryReader {
    file: BufReader<File>,
}

impl EntryReader {
    pub fn new<P>(path: P) -> Self
    where
        P: AsRef<Path>,
    {
        let path = path.as_ref();
        let file = File::open(path).unwrap();
        let mut file = BufReader::new(file);

        let mut buf = [0u8; DATA_FILE_HEADER.len()];
        file.read_exact(&mut buf).expect("read header");
        assert_eq!(buf, DATA_FILE_HEADER);

        Self { file }
    }

    pub fn next(&mut self) -> Option<Entry> {
        let offset = self.file.stream_position().unwrap();

        let mut buf = Vec::new();
        let mut crc = [0u8; 4];
        if let Err(_) = self.file.read_exact(&mut crc) {
            return None;
        }

        let cksum0 = u32::from_be_bytes(crc);

        let mut ts = [0u8; 8];
        self.file.read_exact(&mut ts).expect("read u64");
        let timestamp = u64::from_be_bytes(ts);
        buf.extend_from_slice(&ts);

        let mut deleted = [0u8; 1];
        self.file.read_exact(&mut deleted).expect("read u8");
        buf.extend_from_slice(&deleted);

        let mut key_len = [0u8; 8];
        self.file.read_exact(&mut key_len).unwrap();
        let k_len = u64::from_be_bytes(key_len);
        buf.extend_from_slice(&key_len);

        let mut value_len = [0u8; 8];
        self.file.read_exact(&mut value_len).expect("read u64");
        let v_len = u64::from_be_bytes(value_len);
        buf.extend_from_slice(&value_len);

        let mut key = vec![0u8; k_len as usize];
        self.file.read_exact(&mut key).expect("read key");
        buf.extend_from_slice(&key);

        if deleted[0] == 0 {
            let mut value = vec![0u8; v_len as usize];
            self.file.read_exact(&mut value).expect("read value");
            buf.extend_from_slice(&value);
        }

        let crc = crc_init();
        let mut digest = crc.digest();
        digest.update(&buf);
        let cksum = digest.finalize();

        if cksum != cksum0 {
            return None;
        }

        let size = match deleted[0] {
            0 => (buf.len() + 4) as u64,
            _ => 0,
        };

        let entry = Entry::new(key, timestamp, 0, offset, size);

        Some(entry)
    }
}

pub struct IndexReader {
    file: BufReader<File>,
}

impl IndexReader {
    pub fn new<P>(path: P) -> IndexReader
    where
        P: AsRef<Path>,
    {
        let path = path.as_ref();
        let file = File::open(path).unwrap();
        let mut file = BufReader::new(file);

        let mut buf = [0u8; INDEX_FILE_HEADER.len()];
        file.read_exact(&mut buf).expect("read exact");
        assert_eq!(buf, INDEX_FILE_HEADER);

        Self { file }
    }

    pub fn next(&mut self) -> Option<Entry> {
        let key_len = match self.file.read_u64() {
            Ok(len) => len,
            Err(_) => return None,
        };

        let mut key = vec![0u8; key_len as usize];
        self.file.read_exact(&mut key).expect("read exact");

        let timestamp = self.file.read_u64().expect("read u64");
        let file = self.file.read_u64().expect("read u64");
        let offset = self.file.read_u64().expect("read u64");
        let size = self.file.read_u64().expect("read u64");

        let entry = Entry::new(key, timestamp, file, offset, size);

        Some(entry)
    }
}

#[derive(Debug)]
pub struct Entry {
    key: Vec<u8>,
    file: u64,
    timestamp: u64,
    offset: u64,
    size: u64,
}

impl Entry {
    pub fn new(key: Vec<u8>, timestamp: u64, file: u64, offset: u64, size: u64) -> Self {
        Self {
            key,
            timestamp,
            file,
            offset,
            size,
        }
    }

    pub fn key(&self) -> &[u8] {
        &self.key
    }

    pub fn file(&self) -> u64 {
        self.file
    }

    pub fn timestamp(&self) -> u64 {
        self.timestamp
    }

    pub fn offset(&self) -> u64 {
        self.offset
    }

    pub fn size(&self) -> u64 {
        self.size
    }
}

#[derive(Debug, Eq, PartialEq)]
pub struct Value {
    timestamp: u64,
    data: Vec<u8>,
}

impl Value {
    pub fn from(timestamp: u64, data: Vec<u8>) -> Self {
        Self { timestamp, data }
    }

    pub fn timestamp(&self) -> u64 {
        self.timestamp
    }

    pub fn data(&self) -> &[u8] {
        &self.data
    }
}
