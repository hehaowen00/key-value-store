use std::io::{self, Read, Seek, Write};

pub trait WriteExt {
    fn position(&mut self) -> u64;
    fn write(&mut self, bytes: &[u8]) -> io::Result<u64>;

    fn write_u8(&mut self, val: u8) -> io::Result<u64> {
        let bytes = val.to_be_bytes();
        self.write(&bytes)
    }

    fn write_u16(&mut self, val: u16) -> io::Result<u64> {
        let bytes = val.to_be_bytes();
        self.write(&bytes)
    }

    fn write_u32(&mut self, val: u32) -> io::Result<u64> {
        let bytes = val.to_be_bytes();
        self.write(&bytes)
    }

    fn write_u64(&mut self, val: u64) -> io::Result<u64> {
        let bytes = val.to_be_bytes();
        self.write(&bytes)
    }
}

impl<W> WriteExt for W
where
    W: Write + Seek,
{
    fn position(&mut self) -> u64 {
        self.stream_position().expect("stream position")
    }

    fn write(&mut self, bytes: &[u8]) -> io::Result<u64> {
        let offset = self.stream_position().expect("stream position");
        self.write_all(bytes)?;
        Ok(offset)
    }
}

pub trait ReadExt {
    fn read(&mut self, dest: &mut [u8]) -> io::Result<()>;

    fn read_u8(&mut self) -> io::Result<u8> {
        let mut buf = [0; 1];
        self.read(&mut buf)?;
        Ok(u8::from_be(buf[0]))
    }

    fn read_u16(&mut self) -> io::Result<u16> {
        let mut buf = [0; 2];
        self.read(&mut buf)?;
        Ok(u16::from_be_bytes(buf))
    }

    fn read_u32(&mut self) -> io::Result<u32> {
        let mut buf = [0; 4];
        self.read(&mut buf)?;
        Ok(u32::from_be_bytes(buf))
    }

    fn read_u64(&mut self) -> io::Result<u64> {
        let mut buf = [0; 8];
        self.read(&mut buf)?;
        Ok(u64::from_be_bytes(buf))
    }
}

impl<R> ReadExt for R
where
    R: Read + Seek,
{
    fn read(&mut self, dest: &mut [u8]) -> std::io::Result<()> {
        self.read_exact(dest)
    }
}
