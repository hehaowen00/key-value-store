use crate::api::WriteExt;
use crc::{Crc, CRC_32_CKSUM};
use std::fs::File;
use std::io::{self, BufWriter, Seek, Write};

pub struct DiskTable {
    writer: BufWriter<File>,
}

impl DiskTable {
    pub fn open(file: File) -> Self {
        let writer = BufWriter::with_capacity(8192, file);
        Self { writer }
    }

    pub fn flush(&mut self) {
        std::io::Write::flush(&mut self.writer).expect("flush");
    }

    pub fn append_entry(&mut self, timestamp: u64, key: &[u8], value: &[u8]) -> (u64, u64) {
        let crc = Crc::<u32>::new(&CRC_32_CKSUM);
        let mut digest = crc.digest();
        digest.update(&timestamp.to_be_bytes());
        digest.update(&0u8.to_be_bytes());
        digest.update(&(key.len() as u64).to_be_bytes());
        digest.update(&(value.len() as u64).to_be_bytes());
        digest.update(key);
        digest.update(value);
        let checksum = digest.finalize();

        self.writer
            .seek(std::io::SeekFrom::End(0))
            .expect("seek end");

        let offset = self.write_u32(checksum).expect("write checksum");
        self.write_u64(timestamp).expect("write u64");
        self.write_u8(0).expect("write u8");
        self.write_u64(key.len() as u64).expect("write u64");
        self.write_u64(value.len() as u64).expect("write u64");
        self.write(key).expect("write key");
        let end = self.write(value).expect("write value") + (value.len() as u64);

        (offset, end - offset)
    }

    pub fn delete(&mut self, timestamp: u64, key: &[u8]) {
        let crc = Crc::<u32>::new(&CRC_32_CKSUM);
        let mut digest = crc.digest();
        digest.update(&timestamp.to_be_bytes());
        digest.update(&u8::MAX.to_be_bytes());
        digest.update(&(key.len() as u64).to_be_bytes());
        digest.update(key);
        let checksum = digest.finalize();

        self.writer
            .seek(std::io::SeekFrom::End(0))
            .expect("seek end");

        self.write_u32(checksum).expect("write u32");
        self.write_u64(timestamp).expect("write u64");
        self.write_u8(u8::MAX).expect("write u8");
        self.write_u64(key.len() as u64).expect("write u64");
        self.write(key).expect("write key");
    }
}

impl WriteExt for DiskTable {
    fn position(&mut self) -> u64 {
        self.writer.stream_position().expect("") as u64
    }

    fn write(&mut self, bytes: &[u8]) -> io::Result<u64> {
        let pos = self.writer.stream_position().expect("stream position") as u64;
        self.writer.write_all(bytes)?;
        Ok(pos)
    }
}
