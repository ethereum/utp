use std::collections::VecDeque;

pub type Bytes = Vec<u8>;

pub struct SendBuffer<const N: usize> {
    pending: VecDeque<Bytes>,
    offset: usize,
}

impl<const N: usize> SendBuffer<N> {
    pub fn new() -> Self {
        Self {
            pending: VecDeque::new(),
            offset: 0,
        }
    }

    pub fn available(&self) -> usize {
        N - self.pending.iter().fold(0, |acc, x| acc + x.len()) + self.offset
    }

    pub fn write(&mut self, data: &[u8]) -> std::io::Result<usize> {
        let available = self.available();
        if data.len() <= available {
            self.pending.push_back(data.to_vec());
            return Ok(data.len());
        } else {
            self.pending.push_back(data[..available].to_vec());
            return Ok(available);
        }
    }

    pub fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let mut written = 0;
        let mut remaining = buf.len();
        while remaining > 0 {
            if let Some(data) = self.pending.front() {
                let n = std::cmp::min(data.len() - self.offset, remaining);
                buf[written..n].copy_from_slice(&data[self.offset..self.offset + n]);

                written += n;
                remaining -= n;
                if self.offset + n == data.len() {
                    self.offset = 0;
                    self.pending.pop_front();
                } else {
                    self.offset += n;
                }
            } else {
                break;
            }
        }

        Ok(written)
    }
}
