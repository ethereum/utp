use std::collections::BTreeMap;

use crate::packet::SelectiveAck;

pub struct ReceiveBuffer<const N: usize> {
    buf: [u8; N],
    offset: usize,
    pending: BTreeMap<u16, Vec<u8>>,
    start: Option<u16>,
    consumed: u16,
}

impl<const N: usize> ReceiveBuffer<N> {
    /// Returns a new buffer.
    pub fn new() -> Self {
        Self {
            buf: [0; N],
            offset: 0,
            pending: BTreeMap::new(),
            start: None,
            consumed: 0,
        }
    }

    /// Returns the number of available bytes remaining in the buffer.
    pub fn available(&self) -> usize {
        N - self.offset - self.pending.values().fold(0, |acc, data| acc + data.len())
    }

    /// Reads data from the buffer into `buf`, returning the number of bytes read.
    pub fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        if buf.is_empty() {
            return Ok(0);
        }

        let n = std::cmp::min(buf.len(), self.offset);
        buf[..n].copy_from_slice(&self.buf.as_slice()[..n]);

        let remaining = self.offset - n;
        self.buf.as_mut_slice().copy_within(n..n + remaining, 0);
        self.offset = remaining;

        Ok(n)
    }

    /// Writes `data` into the buffer at `seq_num`.
    ///
    /// If `start` is `true`, then `seq_num` is taken as the initial sequence number.
    ///
    /// # Panics
    ///
    /// Panics if `start` is `true` and `seq_num` is different from a previous call to `write`
    /// where `start` was `true`.
    ///
    /// Panics if `data.len()` is greater than the amount of available bytes in the buffer.
    pub fn write(&mut self, data: &[u8], seq_num: u16, start: bool) {
        if data.len() > self.available() {
            panic!();
        }

        if start {
            match self.start {
                Some(start) => {
                    if seq_num != start {
                        panic!();
                    }
                }
                None => {
                    self.start = Some(seq_num);
                }
            }
        }

        self.pending.insert(seq_num, data.to_vec());
        if let Some(start) = self.start {
            let mut next = start.wrapping_add(self.consumed);
            while let Some(data) = self.pending.remove(&next) {
                let end = self.offset + data.len();
                self.buf.as_mut_slice()[self.offset..end].copy_from_slice(&data[..]);

                self.offset += data.len();
                self.consumed += 1;
                next = next.wrapping_add(1);
            }
        }
    }

    /// Returns the last sequence number in a contiguous sequence from the initial sequence number.
    ///
    /// Returns `None` if the initial sequence number of the remote peer is unknown.
    pub fn ack_num(&self) -> Option<u16> {
        match self.start {
            Some(start) => Some(start.wrapping_add(self.consumed)),
            None => None,
        }
    }

    /// Returns a selective ACK based on the sequence of data written into the buffer.
    pub fn selective_ack(&self) -> Option<SelectiveAck> {
        if self.pending.is_empty() {
            return None;
        }

        if let Some(ack_num) = self.ack_num() {
            let mut acked = Vec::new();
            let mut next = ack_num;

            for seq_num in self.pending.keys() {
                while *seq_num != next {
                    acked.push(false);
                    next = next.wrapping_add(1);
                }

                acked.push(true);
                next = next.wrapping_add(1);
            }

            return Some(SelectiveAck::new(acked));
        }

        None
    }
}
