use std::collections::VecDeque;
use std::io;

type Bytes = Vec<u8>;

#[derive(Clone, Debug)]
pub struct SendBuffer {
    pending: VecDeque<Bytes>,
    offset: usize,
}

impl Default for SendBuffer {
    fn default() -> Self {
        Self::new()
    }
}

impl SendBuffer {
    /// Creates a new buffer.
    pub fn new() -> Self {
        Self {
            pending: VecDeque::new(),
            offset: 0,
        }
    }

    /// Returns `true` if the buffer is empty.
    pub fn is_empty(&self) -> bool {
        self.pending.is_empty()
    }

    /// Writes `data` into the buffer, returning the number of bytes written.
    pub fn write(&mut self, data: &[u8]) -> io::Result<usize> {
        self.pending.push_back(data.to_vec());
        Ok(data.len())
    }

    /// Reads data from the buffer into `buf`, returning the number of bytes read.
    ///
    /// Data from at most one previous write can be read into `buf`. Data from different writes
    /// will not go into a single read.
    pub fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if buf.is_empty() {
            return Ok(0);
        }

        let mut written = 0;
        if let Some(data) = self.pending.front() {
            let n = std::cmp::min(data.len() - self.offset, buf.len());
            buf[..n].copy_from_slice(&data[self.offset..self.offset + n]);

            written += n;
            if self.offset + n == data.len() {
                self.offset = 0;
                self.pending.pop_front();
            } else {
                self.offset += n;
            }
        }

        Ok(written)
    }
}

#[cfg(test)]
mod test {
    use super::*;

    const SIZE: usize = 8192;

    #[test]
    #[allow(clippy::read_zero_byte_vec)]
    fn read() {
        let mut buf = SendBuffer::new();

        // Read of empty buffer returns zero.
        let mut read_buf = vec![0; SIZE];
        let read = buf.read(&mut read_buf).unwrap();
        assert_eq!(read, 0);

        const WRITE_LEN: usize = 1024;

        const READ_LEN: usize = 784;

        let mut read_buf = vec![0; READ_LEN];

        let write_one = vec![0xef; WRITE_LEN];
        let write_two = vec![0xfe; WRITE_LEN];
        buf.write(&write_one).unwrap();
        buf.write(&write_two).unwrap();

        // Read first chunk of first write.
        let read = buf.read(&mut read_buf).unwrap();
        assert_eq!(read, READ_LEN);
        assert_eq!(read_buf[..READ_LEN], write_one[..READ_LEN]);

        // Read remaining chunk of first write.
        let mut read_buf = vec![0; READ_LEN];
        let read = buf.read(&mut read_buf).unwrap();
        assert_eq!(read, WRITE_LEN - READ_LEN);
        assert_eq!(read_buf[..WRITE_LEN - READ_LEN], write_one[READ_LEN..]);

        // Read first chunk of second write.
        let read = buf.read(&mut read_buf).unwrap();
        assert_eq!(read, READ_LEN);
        assert_eq!(read_buf[..READ_LEN], write_two[..READ_LEN]);

        // Read with empty buffer returns zero.
        let mut empty = vec![];
        let read = buf.read(&mut empty).unwrap();
        assert_eq!(read, 0);
    }

    #[test]
    fn write() {
        let mut buf = SendBuffer::new();

        const WRITE_LEN: usize = 1024;

        let data = vec![0xef; WRITE_LEN];
        let written = buf.write(data.as_slice()).unwrap();
        assert_eq!(written, WRITE_LEN);
        assert_eq!(&buf.pending.pop_front().unwrap(), &data);
    }
}
