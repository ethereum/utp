use crate::packet::SelectiveAck;
use crate::seq::CircularRangeInclusive;
use std::collections::{BTreeMap, HashSet};

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
        // Reject packets that are either already received or too old.
        if self.was_written(seq_num) || !seq_after(seq_num, self.ack_num()) {
            return;
        }

        if data.len() > self.available() {
            panic!("insufficient space in buffer");
        }

        // Clean up any pending packets that are now outdated.
        // They should be strictly after ack_num()+1.
        let current_ack_plus1 = self.ack_num().wrapping_add(1);
        self.pending
            .retain(|&seq, _| seq_after(seq, current_ack_plus1));

        // Insert the new packet.
        self.pending.insert(seq_num, data.to_vec());

        // Process contiguous pending packets.
        let mut next = self.ack_num().wrapping_add(1);
        while let Some(data) = self.pending.remove(&next) {
            let end = self.offset + data.len();
            self.buf.as_mut_slice()[self.offset..end].copy_from_slice(&data[..]);
            self.offset = end;
            self.consumed += 1;
            next = self.ack_num().wrapping_add(1);
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

/// Returns `true` if `a` is strictly after `b` in the sequence space.
fn seq_after(a: u16, b: u16) -> bool {
    a.wrapping_sub(b) < 32768 && a != b
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

    #[test]
    fn test_seq_after() {
        // Basic ordering.
        assert!(seq_after(10, 9)); // 10 is after 9.
        assert!(!seq_after(9, 10)); // 9 is not after 10.
        assert!(!seq_after(10, 10)); // Equal numbers: false.

        // Wrap-around behavior.
        // For 16-bit sequence numbers, 0 is after 65535.
        assert!(seq_after(0, 65535));
        assert!(!seq_after(65535, 0));
    }

    /// When a packet’s sequence number is not strictly after ack_num()+1 (i.e. it is stale or already processed), it should be ignored.
    #[test]
    fn stale_packet_insertion_is_ignored() {
        let init_seq_num = 100;
        let mut buf = ReceiveBuffer::<SIZE>::new(init_seq_num);
        let data = vec![0xab; 10];

        // Write the in-order packet.
        buf.write(&data, init_seq_num.wrapping_add(1));
        // Now ack_num() is init_seq_num+1.
        assert_eq!(buf.ack_num(), init_seq_num.wrapping_add(1));

        // Try inserting a packet with a sequence number that is NOT strictly after ack_num()+1.
        // That is, trying to insert a packet with seq == ack_num()+1 (or lower) should be ignored.
        buf.write(&data, init_seq_num.wrapping_add(1)); // duplicate/in-order packet.
        buf.write(&data, init_seq_num); // too old
                                        // The pending set should remain empty.
        assert!(buf.pending.is_empty());
    }

    /// Simulate a scenario where an out‐of‐order packet is inserted (and sits in pending),
    /// then contiguous packets are received, advancing ack_num(), and finally a stale packet is attempted.
    /// The stale packet should be dropped during cleanup.
    #[test]
    fn stale_pending_packet_cleanup() {
        let init_seq_num = 200;
        let mut buf = ReceiveBuffer::<SIZE>::new(init_seq_num);
        let data = vec![0xcd; 10];

        // Insert an out-of-order packet (this one will be held in pending).
        let out_of_order_seq = init_seq_num.wrapping_add(2);
        buf.write(&data, out_of_order_seq);
        assert!(buf.pending.contains_key(&out_of_order_seq));

        // Now insert the missing in-order packet.
        buf.write(&data, init_seq_num.wrapping_add(1));
        // The contiguous region now includes both packets.
        assert_eq!(buf.ack_num(), out_of_order_seq);
        // And pending should have been cleaned up.
        assert!(buf.pending.is_empty());

        // Now try inserting a packet that is stale relative to the new ack.
        buf.write(&data, out_of_order_seq); // same as ack_num() (or not strictly > ack_num()+1)
                                            // The packet should be rejected, so pending remains empty.
        assert!(buf.pending.is_empty());
    }

    /// Verify that the cleanup and insertion logic works properly when the sequence numbers wrap around.
    #[test]
    fn wrap_around_behavior() {
        // Choose an init_seq_num near the maximum.
        let init_seq_num = u16::MAX - 2;
        let mut buf = ReceiveBuffer::<SIZE>::new(init_seq_num);
        let data = vec![0xef; 64];

        // Insert an out-of-order packet near the wrap-around boundary.
        let seq1 = init_seq_num.wrapping_add(2); // should be MAX (u16::MAX)
        buf.write(&data, seq1);
        assert!(buf.pending.contains_key(&seq1));

        // Insert the missing in-order packet.
        let seq0 = init_seq_num.wrapping_add(1);
        buf.write(&data, seq0);
        // Now, ack_num() should advance to seq1.
        assert_eq!(buf.ack_num(), seq1);
        // pending should be empty.
        assert!(buf.pending.is_empty());

        // Insert a packet that wraps around (seq becomes 0 after u16::MAX).
        let seq_wrap = init_seq_num.wrapping_add(3); // should be 0
        buf.write(&data, seq_wrap);
        // Since the next expected is ack_num()+1 (i.e. u16::MAX.wrapping_add(1) == 0),
        // insertion of seq_wrap (which equals 0) is NOT allowed.
        // pending should remain empty.
        assert!(!buf.pending.contains_key(&seq_wrap));
    }
}
