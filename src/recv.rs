use std::collections::{BTreeMap, HashSet};

use crate::packet::SelectiveAck;
use crate::seq::CircularRangeInclusive;

type Bytes = Vec<u8>;

// https://github.com/ethereum/utp/issues/139
// Maximum number of selective ACKs that can fit within the length of 8 bits
const MAX_SELECTIVE_ACK_COUNT: usize = 32 * 63;

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
        N - self.offset - self.pending.values().fold(0, |acc, data| acc + data.len())
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
        let mut last_ack = self.ack_num().wrapping_add(2);
        let mut pending_packets = self.pending.keys().copied().collect::<HashSet<_>>();

        let mut acked = vec![];
        while !pending_packets.is_empty() && acked.len() < MAX_SELECTIVE_ACK_COUNT {
            if pending_packets.remove(&last_ack) {
                acked.push(true);
            } else {
                acked.push(false);
            }
            last_ack = last_ack.wrapping_add(1);
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

    #[test]
    fn selective_ack_overflow() {
        let init_seq_num = u16::MAX - 2;
        let mut buf = ReceiveBuffer::<SIZE>::new(init_seq_num);

        let selective_ack = buf.selective_ack();
        assert!(selective_ack.is_none());

        const DATA_LEN: usize = 64;
        let data = vec![0xef; DATA_LEN];

        // Write out-of-order packet.
        let seq_num = init_seq_num.wrapping_add(2);
        buf.write(&data, seq_num);
        // Write overflow packet, which is at seq_num 0.
        let seq_num = init_seq_num.wrapping_add(3);
        buf.write(&data, seq_num);
        // Write another out of order but received packet.
        let seq_num = init_seq_num.wrapping_add(5);
        buf.write(&data, seq_num);

        // Selective ACK should mark received packets as set.
        // Selective ACK begins with ack_num + 2, onwards.
        // Hence since we received packets 65535, 0, and 2, we should have 3 packets set, in the respective positions.
        let selective_ack = buf.selective_ack().unwrap();
        let mut acked = vec![false; 32];
        acked[0] = true;
        acked[1] = true;
        acked[3] = true;
        assert_eq!(selective_ack.acked(), acked);
    }

    /// Test to verify that stale packets (seq <= ack_num) are rejected on arrival
    #[test]
    fn stale_packet_rejection() {
        let init_seq_num = 1000;
        let mut buf = ReceiveBuffer::<SIZE>::new(init_seq_num);
        let data = vec![0xab; 10];

        // Advance the ack_num by writing a few packets
        for i in 1..=5 {
            buf.write(&data, init_seq_num.wrapping_add(i));
        }

        // Now ack_num() should be init_seq_num + 5 (1005)
        assert_eq!(buf.ack_num(), init_seq_num.wrapping_add(5));

        // Try to write a packet with seq_num <= ack_num (stale packet)
        let stale_seq = init_seq_num.wrapping_add(3); // 1003, which is < 1005
        buf.write(&data, stale_seq);

        // The stale packet should be rejected, so nothing should be added to pending
        assert!(!buf.pending.contains_key(&stale_seq));

        // Try with a sequence number equal to ack_num
        let stale_seq = init_seq_num.wrapping_add(5); // 1005, equal to ack_num
        buf.write(&data, stale_seq);

        // This should also be rejected
        assert!(!buf.pending.contains_key(&stale_seq));

        // Verify that non-stale packets are still accepted
        let valid_seq = init_seq_num.wrapping_add(10); // 1010, which is > 1005
        buf.write(&data, valid_seq);

        // This should be accepted and added to pending
        assert!(buf.pending.contains_key(&valid_seq));
    }

    /// Test to verify stale packet rejection and cleanup at the wraparound boundary
    #[test]
    fn stale_packet_wraparound() {
        let init_seq_num = u16::MAX - 5; // 65530
        let mut buf = ReceiveBuffer::<SIZE>::new(init_seq_num);
        let data = vec![0xef; 20];

        // Add several out-of-order packets, including some that wrap around
        buf.write(&data, init_seq_num.wrapping_add(10)); // 65540-> 4
        buf.write(&data, init_seq_num.wrapping_add(15)); // 65545 -> 9
        buf.write(&data, init_seq_num.wrapping_add(20)); // 65550 -> 14

        // Verify they're in pending
        assert!(buf.pending.contains_key(&init_seq_num.wrapping_add(10)));
        assert!(buf.pending.contains_key(&init_seq_num.wrapping_add(15)));
        assert!(buf.pending.contains_key(&init_seq_num.wrapping_add(20)));

        // Now write packets that wrap around and advance ack_num
        for i in 1..=12 {
            buf.write(&data, init_seq_num.wrapping_add(i));
        }

        // ack_num should now be 6 (after wrapping)
        assert_eq!(buf.ack_num(), init_seq_num.wrapping_add(12)); // 65542 -> 6

        // Pending packets with seq <= 5 should be cleaned up
        assert!(!buf.pending.contains_key(&init_seq_num.wrapping_add(10))); // 4 is < 5

        // Higher sequence packets should still be in pending
        assert!(buf.pending.contains_key(&init_seq_num.wrapping_add(15))); // 9 is > 5
        assert!(buf.pending.contains_key(&init_seq_num.wrapping_add(20))); // 14 is > 5

        // Try to write a stale packet after wraparound
        let stale_seq = init_seq_num.wrapping_add(2); // 65532, which is < 5 after wrapping
        buf.write(&data, stale_seq);

        // It should be rejected
        assert!(!buf.pending.contains_key(&stale_seq));
    }
}
