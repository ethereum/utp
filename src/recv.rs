use std::collections::BTreeMap;

use crate::packet::SelectiveAck;
use crate::seq::CircularRangeInclusive;

type Bytes = Vec<u8>;

#[derive(Clone, Debug)]
pub struct ReceiveBuffer<const N: usize> {
    buf: Box<[u8; N]>,
    offset: usize,
    pending: BTreeMap<u16, Bytes>,
    init_seq_num: u16,
    consumed: u16,
}

impl<const N: usize> ReceiveBuffer<N> {
    /// Returns a new buffer.
    pub fn new(init_seq_num: u16) -> Self {
        Self {
            buf: Box::new([0; N]),
            offset: 0,
            pending: BTreeMap::new(),
            init_seq_num,
            consumed: 0,
        }
    }

    /// Returns the number of available bytes remaining in the buffer.
    pub fn available(&self) -> usize {
        N.saturating_sub(
            self.offset
                .saturating_sub(self.pending.values().fold(0, |acc, data| acc + data.len())),
        )
    }

    /// Returns `true` if the buffer is empty.
    pub fn is_empty(&self) -> bool {
        self.offset == 0 && self.pending.is_empty()
    }

    /// Returns the initial sequence number of the buffer.
    pub fn init_seq_num(&self) -> u16 {
        self.init_seq_num
    }

    /// Returns `true` if data was already written into the buffer for `seq_num`.
    pub fn was_written(&self, seq_num: u16) -> bool {
        let range = CircularRangeInclusive::new(
            self.init_seq_num,
            self.init_seq_num.wrapping_add(self.consumed),
        );
        range.contains(seq_num) || self.pending.contains_key(&seq_num)
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
    /// # Panics
    ///
    /// Panics if `data.len()` is greater than the amount of available bytes in the buffer and the
    /// data for `seq_num` has not already been written.
    pub fn write(&mut self, data: &[u8], seq_num: u16) {
        if self.was_written(seq_num) {
            return;
        }

        // TODO: Return error instead of panicking.
        if data.len() > self.available() {
            panic!("insufficient space in buffer");
        }

        // Read all sequential data from pending packets.
        self.pending.insert(seq_num, data.to_vec());
        let start = self.init_seq_num.wrapping_add(1);
        let mut next = start.wrapping_add(self.consumed);
        while let Some(data) = self.pending.remove(&next) {
            let end = self.offset + data.len();
            self.buf.as_mut_slice()[self.offset..end].copy_from_slice(&data[..]);

            self.offset = end;
            self.consumed += 1;
            next = next.wrapping_add(1);
        }
    }

    /// Returns the last sequence number in a contiguous sequence from the initial sequence number.
    pub fn ack_num(&self) -> u16 {
        self.init_seq_num.wrapping_add(self.consumed)
    }

    /// Returns a selective ACK based on the sequence of data written into the buffer.
    pub fn selective_ack(&self) -> Option<SelectiveAck> {
        if self.pending.is_empty() {
            return None;
        }

        // If there are pending packets, then the data for `ack_num + 1` must be missing.
        let mut next = self.ack_num().wrapping_add(2);

        let mut acked = Vec::new();
        for seq_num in self.pending.keys() {
            while *seq_num != next {
                acked.push(false);
                next = next.wrapping_add(1);
            }

            acked.push(true);
            next = next.wrapping_add(1);
        }

        Some(SelectiveAck::new(acked))
    }
}

#[cfg(test)]
mod test {
    use super::*;

    const SIZE: usize = 1024;

    #[test]
    fn available() {
        let init_seq_num = u16::MAX;
        let mut buf = ReceiveBuffer::<SIZE>::new(init_seq_num);

        assert_eq!(buf.available(), SIZE);

        const DATA_LEN: usize = 256;

        // Write out-of-order packet.
        let data = vec![0xef; DATA_LEN];
        let seq_num = init_seq_num.wrapping_add(2);
        buf.write(&data, seq_num);
        assert_eq!(buf.available(), SIZE - DATA_LEN);

        // Write in-order packet.
        let data = vec![0xef; DATA_LEN];
        let seq_num = init_seq_num.wrapping_add(1);
        buf.write(&data, seq_num);
        assert_eq!(buf.available(), SIZE - (DATA_LEN * 2));

        // Read all data.
        let mut data = [0; DATA_LEN * 2];
        buf.read(&mut data).unwrap();
        assert_eq!(buf.available(), SIZE);
    }

    #[test]
    fn was_written() {
        let init_seq_num = u16::MAX;
        let mut buf = ReceiveBuffer::<SIZE>::new(init_seq_num);

        let seq_num = init_seq_num.wrapping_add(2);
        assert!(!buf.was_written(seq_num));

        const DATA_LEN: usize = 64;
        let data = vec![0xef; DATA_LEN];
        buf.write(&data, seq_num);
        assert!(buf.was_written(seq_num));
    }

    #[test]
    fn write() {
        let init_seq_num = u16::MAX;
        let mut buf = ReceiveBuffer::<SIZE>::new(init_seq_num);

        const DATA_LEN: usize = 256;

        // Write out-of-order packet.
        let data_second = vec![0xef; DATA_LEN];
        let seq_num = init_seq_num.wrapping_add(2);
        buf.write(&data_second, seq_num);
        assert_eq!(buf.offset, 0);
        assert_eq!(buf.consumed, 0);
        assert!(buf.pending.contains_key(&seq_num));
        assert_eq!(*buf.pending.get(&seq_num).unwrap(), data_second);

        // Write in-order packet.
        let data_first = vec![0xfe; DATA_LEN];
        let seq_num = init_seq_num.wrapping_add(1);
        buf.write(&data_first, seq_num);
        assert_eq!(buf.offset, DATA_LEN * 2);
        assert_eq!(buf.consumed, 2);
        assert!(buf.pending.is_empty());
        assert_eq!(buf.buf[..DATA_LEN], data_first[..]);
        assert_eq!(buf.buf[DATA_LEN..DATA_LEN * 2], data_second[..]);
    }

    #[test]
    #[should_panic]
    fn write_exceeds_available() {
        let init_seq_num = u16::MAX;
        let mut buf = ReceiveBuffer::<SIZE>::new(init_seq_num);

        let seq_num = init_seq_num.wrapping_add(1);
        let data = vec![0xef; SIZE + 1];
        buf.write(&data, seq_num);
    }

    #[test]
    fn read() {
        let init_seq_num = u16::MAX;
        let mut buf = ReceiveBuffer::<SIZE>::new(init_seq_num);

        const DATA_LEN: usize = 256;

        // Write out-of-order packet.
        let data_second = vec![0xef; DATA_LEN];
        let seq_num = init_seq_num.wrapping_add(2);
        buf.write(&data_second, seq_num);

        let mut read_buf = vec![0; SIZE];
        let read = buf.read(&mut read_buf).unwrap();
        assert_eq!(read, 0);

        // Write in-order packet.
        let data_first = vec![0xfe; DATA_LEN];
        let seq_num = init_seq_num.wrapping_add(1);
        buf.write(&data_first, seq_num);

        let read = buf.read(&mut read_buf).unwrap();
        assert_eq!(read, DATA_LEN * 2);
        assert_eq!(buf.offset, 0);
        assert_eq!(read_buf[..DATA_LEN], data_first[..]);
        assert_eq!(read_buf[DATA_LEN..DATA_LEN * 2], data_second[..]);

        let read = buf.read(&mut read_buf).unwrap();
        assert_eq!(read, 0);
    }

    #[test]
    fn ack_num() {
        let init_seq_num = u16::MAX;
        let mut buf = ReceiveBuffer::<SIZE>::new(init_seq_num);

        assert_eq!(buf.ack_num(), init_seq_num);

        const DATA_LEN: usize = 64;
        let data = vec![0xef; DATA_LEN];

        // Write out-of-order packet.
        let second_seq_num = init_seq_num.wrapping_add(2);
        buf.write(&data, second_seq_num);

        assert_eq!(buf.ack_num(), init_seq_num);

        // Write in-order packet.
        let first_seq_num = init_seq_num.wrapping_add(1);
        buf.write(&data, first_seq_num);

        assert_eq!(buf.ack_num(), second_seq_num);
    }

    #[test]
    fn selective_ack() {
        let init_seq_num = u16::MAX;
        let mut buf = ReceiveBuffer::<SIZE>::new(init_seq_num);

        let selective_ack = buf.selective_ack();
        assert!(selective_ack.is_none());

        const DATA_LEN: usize = 64;
        let data = vec![0xef; DATA_LEN];

        // Write out-of-order packet.
        let seq_num = init_seq_num.wrapping_add(2);
        buf.write(&data, seq_num);

        let selective_ack = buf.selective_ack().unwrap();
        let acked = selective_ack.acked();
        assert!(acked[0]);
        for ack in acked[1..].iter() {
            assert!(!ack);
        }

        // Write in-order packet.
        let seq_num = init_seq_num.wrapping_add(1);
        buf.write(&data, seq_num);

        let selective_ack = buf.selective_ack();
        assert!(selective_ack.is_none());
    }
}
