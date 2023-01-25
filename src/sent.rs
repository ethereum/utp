use std::collections::BTreeSet;
use std::time::{Duration, Instant};

use crate::congestion;
use crate::packet::{PacketType, SelectiveAck};
use crate::seq::CircularRangeInclusive;

const LOSS_THRESHOLD: usize = 3;

type Bytes = Vec<u8>;

struct SentPacket {
    pub seq_num: u16,
    pub packet_type: PacketType,
    pub data: Option<Bytes>,
    pub transmission: Instant,
    pub retransmissions: Vec<Instant>,
    pub acks: Vec<Instant>,
}

impl SentPacket {
    fn rtt(&self, now: Instant) -> Duration {
        let last_transmission = self.retransmissions.first().unwrap_or(&self.transmission);
        now.duration_since(*last_transmission)
    }
}

pub struct SentPackets {
    packets: Vec<SentPacket>,
    init_seq_num: u16,
    lost_packets: BTreeSet<u16>,
    congestion_ctrl: congestion::Controller,
}

impl SentPackets {
    pub fn new(init_seq_num: u16) -> Self {
        Self {
            packets: Vec::new(),
            init_seq_num,
            lost_packets: BTreeSet::new(),
            congestion_ctrl: congestion::Controller::new(congestion::Config::default()),
        }
    }

    pub fn next_seq_num(&self) -> u16 {
        // Assume cast is okay, meaning that `packets` never contains more than `u16::MAX`
        // elements.
        self.init_seq_num
            .wrapping_add(self.packets.len() as u16)
            .wrapping_add(1)
    }

    pub fn ack_num(&self) -> u16 {
        self.last_ack_num().unwrap_or(0)
    }

    pub fn seq_num_range(&self) -> CircularRangeInclusive {
        let end = self.next_seq_num().wrapping_sub(1);
        CircularRangeInclusive::new(self.init_seq_num, end)
    }

    pub fn timeout(&self) -> Duration {
        self.congestion_ctrl.timeout()
    }

    pub fn on_timeout(&mut self) {
        self.congestion_ctrl.on_timeout()
    }

    pub fn window(&self) -> u32 {
        self.congestion_ctrl.bytes_available_in_window()
    }

    pub fn has_lost_packets(&self) -> bool {
        !self.lost_packets.is_empty()
    }

    pub fn lost_packets(&self) -> Vec<(u16, PacketType, Option<Bytes>)> {
        self.lost_packets
            .iter()
            .map(|seq| {
                let index = self.seq_num_index(*seq);

                // The unwrap is safe because only sent packets may be lost.
                let packet = self.packets.get(index).unwrap();

                (packet.seq_num, packet.packet_type, packet.data.clone())
            })
            .collect()
    }

    /// # Panics
    ///
    /// Panics if `seq_num` does not correspond to the next expected packet or a previously sent
    /// packet.
    ///
    /// Panics if the transmit is not a retransmission and `len` is greater than the amount of
    /// available space in the window.
    pub fn on_transmit(
        &mut self,
        seq_num: u16,
        packet_type: PacketType,
        data: Option<Bytes>,
        len: u32,
        now: Instant,
    ) {
        let index = self.seq_num_index(seq_num);
        let is_retransmission = index < self.packets.len();

        // If the packet sequence number is beyond the next sequence number, then panic.
        if index > self.packets.len() {
            panic!("out of order transmit");
        }

        // If this is not a retransmission and the length of the packet is greater than the amount
        // of available space in the window, then panic.
        if !is_retransmission && len > self.window() {
            panic!("transmit exceeds available send window");
        }

        match self.packets.get_mut(index) {
            Some(sent) => {
                sent.retransmissions.push(now);
            }
            None => {
                let sent = SentPacket {
                    seq_num,
                    packet_type,
                    data,
                    transmission: now,
                    retransmissions: Vec::new(),
                    acks: Vec::new(),
                };
                self.packets.push(sent);
            }
        }

        let transmit = if is_retransmission {
            congestion::Transmit::Retransmission
        } else {
            congestion::Transmit::Initial { bytes: len }
        };

        // The unwrap is safe given the check above on the available window.
        self.congestion_ctrl.on_transmit(seq_num, transmit).unwrap();
    }

    /// # Panics
    ///
    /// Panics if `ack_num` does not correspond to a previously sent packet.
    pub fn on_ack(
        &mut self,
        ack_num: u16,
        selective_ack: Option<&SelectiveAck>,
        delay: Duration,
        now: Instant,
    ) {
        if let Some(sack) = selective_ack {
            self.on_selective_ack(ack_num, sack, delay, now);
        } else {
            self.ack(ack_num, delay, now);
        }

        // An ACK for `ack_num` implicitly ACKs all sequence numbers that precede `ack_num`.
        // Account for any preceding unacked packets.
        self.ack_prior_unacked(ack_num, delay, now);

        // Account for (newly) lost packets.
        let lost = self.detect_lost_packets();
        for packet in lost {
            if self.lost_packets.insert(packet) {
                self.on_lost(packet, true);
            }
        }
    }

    /// # Panics
    ///
    /// Panics if `ack_num` does not correspond to a previously sent packet.
    fn on_selective_ack(
        &mut self,
        ack_num: u16,
        selective_ack: &SelectiveAck,
        delay: Duration,
        now: Instant,
    ) {
        self.ack(ack_num, delay, now);

        let range = self.seq_num_range();

        // The first bit of the selective ACK corresponds to `ack_num + 2`, where `ack_num + 1` is
        // assumed to have been dropped.
        let mut sack_num = ack_num.wrapping_add(2);
        for ack in selective_ack.acked() {
            // Break once we exhaust all sent sequence numbers. The selective ACK length is a
            // multiple of 32, so it may be padded beyond the actual range of sequence numbers.
            if !range.contains(sack_num) {
                break;
            }

            if ack {
                self.ack(sack_num, delay, now);
            }

            sack_num = sack_num.wrapping_add(1);
        }
    }

    /// Returns a set containing the sequence numbers of lost packets.
    ///
    /// A packet is lost if it has not been acknowledged and some threshold number of packets sent
    /// after it have been acknowledged.
    fn detect_lost_packets(&self) -> BTreeSet<u16> {
        let mut acked = 0;
        let mut lost = BTreeSet::new();

        let start = match self.first_unacked_seq_num() {
            Some(first_unacked) => self.seq_num_index(first_unacked),
            None => return lost,
        };

        for packet in self.packets[start..].iter().rev() {
            if packet.acks.is_empty() && acked >= LOSS_THRESHOLD {
                lost.insert(packet.seq_num);
            }

            if !packet.acks.is_empty() {
                acked += 1;
            }
        }

        lost
    }

    /// # Panics
    ///
    /// Panics if `seq_num` does not correspond to a previously sent packet.
    fn ack(&mut self, seq_num: u16, delay: Duration, now: Instant) {
        let index = self.seq_num_index(seq_num);
        let packet = self.packets.get_mut(index).unwrap();

        let ack = congestion::Ack {
            delay,
            rtt: packet.rtt(now),
            received_at: now,
        };
        self.congestion_ctrl.on_ack(packet.seq_num, ack).unwrap();

        packet.acks.push(now);

        self.lost_packets.remove(&packet.seq_num);
    }

    /// Acknowledges any unacknowledged packets that precede `seq_num`.
    fn ack_prior_unacked(&mut self, seq_num: u16, delay: Duration, now: Instant) {
        if let Some(first_unacked) = self.first_unacked_seq_num() {
            let start = self.seq_num_index(first_unacked);
            let end = self.seq_num_index(seq_num);
            if start >= end {
                return;
            }

            let to_ack: Vec<u16> = self.packets[start..end].iter().map(|p| p.seq_num).collect();
            for seq_num in to_ack {
                self.ack(seq_num, delay, now);
            }
        }
    }

    /// # Panics
    ///
    /// Panics if `seq_num` does not correspond to a previously sent packet.
    fn on_lost(&mut self, seq_num: u16, retransmitting: bool) {
        if !self.seq_num_range().contains(seq_num) {
            panic!("cannot mark unsent packet lost");
        }

        // The unwrap is safe assuming that we do not panic above.
        self.congestion_ctrl
            .on_lost_packet(seq_num, retransmitting)
            .expect("lost packet was previously sent");
    }

    /// Returns the "normalized" index for `seq_num` based on the initial sequence number.
    fn seq_num_index(&self, seq_num: u16) -> usize {
        if seq_num > self.init_seq_num {
            usize::from(seq_num - self.init_seq_num)
        } else {
            usize::from(u16::MAX - self.init_seq_num + seq_num)
        }
    }

    /// Returns the sequence number of the last (i.e. latest) packet in a contiguous sequence of
    /// acknowledged packets.
    ///
    /// Returns `None` if none of the (possibly zero) packets have been acknowledged.
    // TODO: Cache this value, (possibly) updating on each ACK.
    fn last_ack_num(&self) -> Option<u16> {
        if self.packets.is_empty() {
            return None;
        }

        let mut num = None;
        for packet in &self.packets {
            if !packet.acks.is_empty() {
                num = Some(packet.seq_num);
            } else {
                break;
            }
        }

        num
    }

    /// Returns the sequence number of the first (i.e. earliest) packet that has not been
    /// acknowledged.
    ///
    /// Returns `None` if all (possibly zero) sent packets have been acknowledged.
    fn first_unacked_seq_num(&self) -> Option<u16> {
        if self.packets.is_empty() {
            return None;
        }

        let seq_num = match self.last_ack_num() {
            Some(last_ack_num) => {
                // If the last ACK num corresponds to the last packet, then return `None`.
                if self.packets.last().unwrap().seq_num == last_ack_num {
                    return None;
                }
                last_ack_num.wrapping_add(1)
            }
            None => self.init_seq_num.wrapping_add(1),
        };

        Some(seq_num)
    }
}

#[cfg(test)]
mod test {
    use super::*;

    use quickcheck::{quickcheck, TestResult};

    const DELAY: Duration = Duration::from_millis(100);

    // TODO: Bolster tests.

    #[test]
    fn next_seq_num() {
        fn prop(init_seq_num: u16, len: u8) -> TestResult {
            let mut sent_packets = SentPackets::new(init_seq_num);
            if len == 0 {
                return TestResult::from_bool(
                    sent_packets.next_seq_num() == init_seq_num.wrapping_add(1),
                );
            }

            let final_seq_num = init_seq_num.wrapping_add(u16::from(len));
            let range = CircularRangeInclusive::new(init_seq_num.wrapping_add(1), final_seq_num);
            let transmission = Instant::now();
            for seq_num in range {
                sent_packets.packets.push(SentPacket {
                    seq_num,
                    packet_type: PacketType::Data,
                    data: None,
                    transmission,
                    acks: Default::default(),
                    retransmissions: Default::default(),
                });
            }

            TestResult::from_bool(sent_packets.next_seq_num() == final_seq_num.wrapping_add(1))
        }
        quickcheck(prop as fn(u16, u8) -> TestResult)
    }

    #[test]
    fn on_transmit_initial() {
        let init_seq_num = u16::MAX;
        let mut sent_packets = SentPackets::new(init_seq_num);

        let seq_num = sent_packets.next_seq_num();
        let data = vec![0];
        let len = data.len() as u32;
        let now = Instant::now();
        sent_packets.on_transmit(seq_num, PacketType::Data, Some(data), len, now);

        assert_eq!(sent_packets.packets.len(), 1);

        let packet = &sent_packets.packets[0];
        assert_eq!(packet.seq_num, seq_num);
        assert_eq!(packet.transmission, now);
        assert!(packet.acks.is_empty());
        assert!(packet.retransmissions.is_empty());
    }

    #[test]
    fn on_transmit_retransmit() {
        let init_seq_num = u16::MAX;
        let mut sent_packets = SentPackets::new(init_seq_num);

        let seq_num = sent_packets.next_seq_num();
        let data = vec![0];
        let len = data.len() as u32;
        let first = Instant::now();
        let second = Instant::now();
        sent_packets.on_transmit(seq_num, PacketType::Data, Some(data.clone()), len, first);
        sent_packets.on_transmit(seq_num, PacketType::Data, Some(data), len, second);

        assert_eq!(sent_packets.packets.len(), 1);

        let packet = &sent_packets.packets[0];
        assert_eq!(packet.seq_num, seq_num);
        assert_eq!(packet.transmission, first);
        assert!(packet.acks.is_empty());
        assert_eq!(packet.retransmissions.len(), 1);
        assert_eq!(packet.retransmissions[0], second);
    }

    #[test]
    #[should_panic]
    fn on_transmit_out_of_order() {
        let init_seq_num = u16::MAX;
        let mut sent_packets = SentPackets::new(init_seq_num);

        let out_of_order_seq_num = init_seq_num.wrapping_add(2);
        let data = vec![0];
        let len = data.len() as u32;
        let now = Instant::now();

        sent_packets.on_transmit(out_of_order_seq_num, PacketType::Data, Some(data), len, now);
    }

    #[test]
    fn on_selective_ack() {
        let init_seq_num = u16::MAX;
        let mut sent_packets = SentPackets::new(init_seq_num);

        let data = vec![0];
        let len = data.len() as u32;

        const COUNT: usize = 10;
        for _ in 0..COUNT {
            let now = Instant::now();
            let seq_num = sent_packets.next_seq_num();
            sent_packets.on_transmit(seq_num, PacketType::Data, Some(data.clone()), len, now);
        }

        const SACK_LEN: usize = COUNT - 2;
        let mut acked = vec![false; SACK_LEN];
        for (i, ack) in acked.iter_mut().enumerate() {
            if i % 2 == 0 {
                *ack = true;
            }
        }
        let selective_ack = SelectiveAck::new(acked);

        let now = Instant::now();
        sent_packets.on_ack(
            init_seq_num.wrapping_add(1),
            Some(&selective_ack),
            DELAY,
            now,
        );
        assert_eq!(sent_packets.packets[0].acks.len(), 1);
        assert!(sent_packets.packets[1].acks.is_empty());
        for i in 2..COUNT {
            let is_empty = i % 2 != 0;
            assert_eq!(sent_packets.packets[i].acks.is_empty(), is_empty);
        }
    }

    #[test]
    fn detect_lost_packets() {
        let init_seq_num = u16::MAX;
        let mut sent_packets = SentPackets::new(init_seq_num);

        let data = vec![0];
        let len = data.len() as u32;

        const COUNT: usize = 10;
        const START: usize = COUNT - LOSS_THRESHOLD;
        for i in 0..COUNT {
            let now = Instant::now();
            let seq_num = sent_packets.next_seq_num();
            sent_packets.on_transmit(seq_num, PacketType::Data, Some(data.clone()), len, now);

            if i >= START {
                sent_packets.ack(seq_num, DELAY, now);
            }
        }

        let lost = sent_packets.detect_lost_packets();
        for i in 0..START {
            let packet = &sent_packets.packets[i];
            assert!(lost.contains(&packet.seq_num));
        }
    }

    #[test]
    fn ack() {
        let init_seq_num = u16::MAX;
        let mut sent_packets = SentPackets::new(init_seq_num);

        let seq_num = sent_packets.next_seq_num();
        let data = vec![0];
        let len = data.len() as u32;
        let now = Instant::now();
        sent_packets.on_transmit(seq_num, PacketType::Data, Some(data), len, now);

        // Artificially insert packet into lost packets.
        sent_packets.lost_packets.insert(seq_num);
        assert!(sent_packets.lost_packets.contains(&seq_num));

        let now = Instant::now();
        sent_packets.ack(seq_num, DELAY, now);

        let index = sent_packets.seq_num_index(seq_num);
        let packet = sent_packets.packets.get(index).unwrap();

        assert_eq!(packet.acks.len(), 1);
        assert_eq!(packet.acks[0], now);
        assert!(!sent_packets.lost_packets.contains(&seq_num));
    }

    #[test]
    fn ack_prior_unacked() {
        let init_seq_num = u16::MAX;
        let mut sent_packets = SentPackets::new(init_seq_num);

        let data = vec![0];
        let len = data.len() as u32;

        const COUNT: usize = 10;
        for _ in 0..COUNT {
            let now = Instant::now();
            let seq_num = sent_packets.next_seq_num();
            sent_packets.on_transmit(seq_num, PacketType::Data, Some(data.clone()), len, now);
        }

        const ACK_NUM: u16 = 3;
        assert!(usize::from(ACK_NUM) < COUNT);
        assert!(COUNT - usize::from(ACK_NUM) > 2);

        let now = Instant::now();
        sent_packets.ack_prior_unacked(ACK_NUM, DELAY, now);
        for i in 0..usize::from(ACK_NUM) {
            assert_eq!(sent_packets.packets[i].acks.len(), 1);
        }
    }

    #[test]
    #[should_panic]
    fn ack_unsent() {
        let init_seq_num = u16::MAX;
        let mut sent_packets = SentPackets::new(init_seq_num);

        let unsent_ack_num = init_seq_num.wrapping_add(2);
        let now = Instant::now();
        sent_packets.ack(unsent_ack_num, DELAY, now);
    }
}
