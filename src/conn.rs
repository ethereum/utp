use std::sync::mpsc;
use std::time::{Duration, Instant};

use crate::cid::ConnectionId;
use crate::congestion;
use crate::packet::{Packet, PacketBuilder, PacketType, SelectiveAck};
use crate::recv::ReceiveBuffer;
use crate::sent::SentPackets;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Error {
    EmptyDataPayload,
    InvalidAckNum,
    InvalidFin,
    InvalidSeqNum,
    InvalidSyn,
    Reset,
    SynFromAcceptor,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Endpoint {
    Initiator((u16, Option<Instant>)),
    Acceptor((u16, u16)),
}

// TODO: remove clippy allow.
#[allow(dead_code)]
#[derive(Clone)]
enum State<const N: usize> {
    Connecting,
    Established {
        recv_buf: ReceiveBuffer<N>,
        sent_packets: SentPackets,
    },
    Closing {
        local_fin: Option<u16>,
        remote_fin: Option<u16>,
        recv_buf: ReceiveBuffer<N>,
        sent_packets: SentPackets,
    },
    Closed {
        err: Option<Error>,
    },
}

pub struct Connection<const N: usize> {
    state: State<N>,
    cid: ConnectionId,
    endpoint: Endpoint,
    peer_ts_diff: Duration,
    peer_recv_window: u32,
    outgoing: mpsc::Sender<Packet>,
}

impl<const N: usize> Connection<N> {
    pub fn new(cid: ConnectionId, syn: Option<Packet>, outgoing: mpsc::Sender<Packet>) -> Self {
        let (endpoint, peer_ts_diff, peer_recv_window) = match syn {
            Some(syn) => {
                let syn_ack = rand::random();
                let endpoint = Endpoint::Acceptor((syn.seq_num(), syn_ack));

                let now = crate::time::now_micros();
                let peer_ts_diff = crate::time::duration_between(syn.ts_micros(), now);

                (endpoint, peer_ts_diff, syn.window_size())
            }
            None => {
                let syn = rand::random();
                let endpoint = Endpoint::Initiator((syn, None));
                (endpoint, Duration::ZERO, u32::MAX)
            }
        };

        Self {
            state: State::Connecting,
            cid,
            endpoint,
            peer_ts_diff,
            peer_recv_window,
            outgoing,
        }
    }

    pub fn on_packet(&mut self, packet: &Packet, now: Instant) {
        let now_micros = crate::time::now_micros();
        self.peer_recv_window = packet.window_size();

        // TODO: We probably ought to cap the diff here. If the clock on the remote machine is
        // behind the clock on the local machine, then we could end up with large (inaccurate)
        // diffs.
        self.peer_ts_diff = crate::time::duration_between(packet.ts_micros(), now_micros);

        match packet.packet_type() {
            PacketType::Syn => self.on_syn(packet.seq_num()),
            PacketType::State => {
                let delay = Duration::from_micros(packet.ts_diff_micros().into());
                self.on_state(
                    packet.seq_num(),
                    packet.ack_num(),
                    packet.selective_ack(),
                    delay,
                    now,
                );
            }
            PacketType::Data => self.on_data(packet.seq_num(), packet.payload()),
            PacketType::Fin => {
                if packet.payload().is_empty() {
                    self.on_fin(packet.seq_num(), None);
                } else {
                    self.on_fin(packet.seq_num(), Some(packet.payload()));
                }
            }
            PacketType::Reset => {
                self.on_reset();
            }
        }

        // If there are any lost packets, then queue retransmissions.
        self.retransmit_lost_packets(now);

        // If the connection is ready to be closed, then close the connection.

        // Send a STATE packet if appropriate packet and connection in appropriate state.
        //
        // NOTE: We don't call `transmit` because we do not wish to track this packet.
        match packet.packet_type() {
            PacketType::Syn | PacketType::Fin | PacketType::Data => {
                if let Some(state) = self.state_packet() {
                    self.outgoing
                        .send(state)
                        .expect("outgoing channel should be open if connection is not closed");
                }
            }
            PacketType::State | PacketType::Reset => {}
        };

        // If there is available space in the send window, then notify writer(s).

        // If there is data in the receive buffer, then notify reader(s).
    }

    fn on_syn(&mut self, seq_num: u16) {
        let err = match self.endpoint {
            // If we are the accepting endpoint, then check whether the SYN is a retransmission. A
            // non-matching sequence number is incorrect behavior.
            Endpoint::Acceptor((syn, ..)) => {
                if seq_num != syn {
                    Some(Error::InvalidSyn)
                } else {
                    None
                }
            }
            // If we are the initiating endpoint, then an incoming SYN is incorrect behavior.
            Endpoint::Initiator(..) => Some(Error::SynFromAcceptor),
        };

        if let Some(err) = err {
            if !std::matches!(self.state, State::Closed { .. }) {
                self.reset(err);
            }
        }
    }

    fn on_state(
        &mut self,
        seq_num: u16,
        ack_num: u16,
        selective_ack: Option<&SelectiveAck>,
        delay: Duration,
        now: Instant,
    ) {
        match &mut self.state {
            State::Connecting => match self.endpoint {
                // If the STATE acknowledges our SYN, then mark the connection established.
                Endpoint::Initiator((syn, ..)) => {
                    if ack_num == syn {
                        let recv_buf = ReceiveBuffer::new(seq_num);
                        let congestion_ctrl =
                            congestion::Controller::new(congestion::Config::default());
                        let sent_packets = SentPackets::new(syn, congestion_ctrl);
                        self.state = State::Established {
                            recv_buf,
                            sent_packets,
                        };
                    }
                }
                Endpoint::Acceptor(..) => {}
            },
            State::Established { sent_packets, .. } | State::Closing { sent_packets, .. } => {
                let range = sent_packets.seq_num_range();
                if range.contains(ack_num) {
                    sent_packets.on_ack(ack_num, selective_ack, delay, now);
                } else {
                    self.reset(Error::InvalidAckNum);
                }
            }
            State::Closed { .. } => {}
        }
    }

    fn on_data(&mut self, seq_num: u16, data: &[u8]) {
        // If the data payload is empty, then reset the connection. A DATA packet must contain a
        // non-empty payload.
        if data.is_empty() {
            self.reset(Error::EmptyDataPayload);
        }

        match &mut self.state {
            State::Connecting => match self.endpoint {
                // If we are the accepting endpoint, and the sequence number corresponds to the
                // sequence number after the SYN, then mark the connection established.
                Endpoint::Acceptor((syn, syn_ack)) => {
                    if seq_num == syn.wrapping_add(1) {
                        let mut recv_buf = ReceiveBuffer::new(syn);
                        recv_buf.write(data, seq_num);

                        let congestion_ctrl =
                            congestion::Controller::new(congestion::Config::default());
                        let sent_packets = SentPackets::new(syn_ack, congestion_ctrl);

                        self.state = State::Established {
                            recv_buf,
                            sent_packets,
                        };
                    }
                }
                // If we are the initiating endpoint, then we cannot mark the connection
                // established, because we are waiting on the STATE for the SYN. Without the STATE,
                // we do not know how to order this data.
                //
                // TODO: Save the data so that it may be written to the receive buffer upon the
                // connection being established.
                Endpoint::Initiator(..) => {}
            },
            // If the connection is established, and there is sufficient capacity, then incorporate
            // the data into the receive buffer.
            State::Established { recv_buf, .. } => {
                if data.len() <= recv_buf.available() && !recv_buf.was_written(seq_num) {
                    recv_buf.write(data, seq_num);
                }
            }
            // If the connection is closing and we have a remote FIN, then check whether the
            // sequence number falls within the appropriate range. If it does, and there is
            // sufficient capacity, then incorporate the data into the receive buffer. If it does
            // not, then reset the connection.
            State::Closing {
                recv_buf,
                remote_fin,
                ..
            } => {
                if let Some(remote_fin) = remote_fin {
                    let start = recv_buf.init_seq_num();
                    let range = crate::seq::CircularRangeInclusive::new(start, *remote_fin);
                    if !range.contains(seq_num) {
                        self.state = State::Closed {
                            err: Some(Error::InvalidSeqNum),
                        };
                        return;
                    }
                }

                if data.len() <= recv_buf.available() && !recv_buf.was_written(seq_num) {
                    recv_buf.write(data, seq_num);
                }
            }
            State::Closed { .. } => {}
        }
    }

    fn on_fin(&mut self, seq_num: u16, data: Option<&[u8]>) {
        match &mut self.state {
            State::Connecting => {}
            State::Established {
                recv_buf,
                sent_packets,
                ..
            } => {
                if let Some(data) = data {
                    recv_buf.write(data, seq_num);
                }

                self.state = State::Closing {
                    recv_buf: recv_buf.clone(),
                    sent_packets: sent_packets.clone(),
                    remote_fin: Some(seq_num),
                    local_fin: None,
                };
            }
            State::Closing { remote_fin, .. } => {
                match remote_fin {
                    // If we have already received a FIN, a subsequent FIN with a different
                    // sequence number is incorrect behavior.
                    Some(fin) => {
                        if seq_num != *fin {
                            self.reset(Error::InvalidFin);
                        }
                    }
                    None => {
                        *remote_fin = Some(seq_num);
                    }
                }
            }
            State::Closed { .. } => {}
        }
    }

    fn on_reset(&mut self) {
        // If the connection is not already closed or reset, then reset the connection.
        if !std::matches!(self.state, State::Closed { .. }) {
            self.reset(Error::Reset);
        }
    }

    fn reset(&mut self, err: Error) {
        self.state = State::Closed { err: Some(err) }
    }

    fn state_packet(&self) -> Option<Packet> {
        let now = crate::time::now_micros();
        let conn_id = self.cid.send;
        let ts_diff_micros = self.peer_ts_diff.as_micros() as u32;

        match &self.state {
            State::Connecting => match self.endpoint {
                Endpoint::Initiator(..) => None,
                Endpoint::Acceptor((syn, syn_ack)) => {
                    let packet =
                        PacketBuilder::new(PacketType::State, conn_id, now, N as u32, syn_ack)
                            .ts_diff_micros(ts_diff_micros)
                            .ack_num(syn)
                            .build();
                    Some(packet)
                }
            },
            State::Established {
                recv_buf,
                sent_packets,
            }
            | State::Closing {
                recv_buf,
                sent_packets,
                ..
            } => {
                let seq_num = sent_packets.seq_num_range().end();
                let ack_num = recv_buf.ack_num();
                let recv_window = recv_buf.available() as u32;
                let selective_ack = recv_buf.selective_ack();
                let packet =
                    PacketBuilder::new(PacketType::State, conn_id, now, recv_window, seq_num)
                        .ts_diff_micros(ts_diff_micros)
                        .ack_num(ack_num)
                        .selective_ack(selective_ack)
                        .build();
                Some(packet)
            }
            State::Closed { .. } => None,
        }
    }

    fn retransmit_lost_packets(&mut self, now: Instant) {
        let (sent_packets, recv_buf) = match &mut self.state {
            State::Connecting | State::Closed { .. } => return,
            State::Established {
                sent_packets,
                recv_buf,
                ..
            }
            | State::Closing {
                sent_packets,
                recv_buf,
                ..
            } => (sent_packets, recv_buf),
        };

        if !sent_packets.has_lost_packets() {
            return;
        }

        let conn_id = self.cid.send;
        let now_micros = crate::time::now_micros();
        let recv_window = recv_buf.available() as u32;
        let ts_diff_micros = self.peer_ts_diff.as_micros() as u32;
        for (seq_num, packet_type, payload) in sent_packets.lost_packets() {
            let mut packet =
                PacketBuilder::new(packet_type, conn_id, now_micros, recv_window, seq_num);
            if let Some(payload) = payload {
                packet = packet.payload(payload);
            }
            let packet = packet
                .ts_diff_micros(ts_diff_micros)
                .ack_num(recv_buf.ack_num())
                .selective_ack(recv_buf.selective_ack())
                .build();

            Self::transmit(sent_packets, &mut self.outgoing, packet, now);
        }
    }

    fn transmit(
        sent_packets: &mut SentPackets,
        outgoing: &mut mpsc::Sender<Packet>,
        packet: Packet,
        now: Instant,
    ) -> bool {
        let payload = if packet.payload().is_empty() {
            None
        } else {
            Some(packet.payload().clone())
        };

        sent_packets.on_transmit(
            packet.seq_num(),
            packet.packet_type(),
            payload,
            packet.encoded_len() as u32,
            now,
        );
        outgoing
            .send(packet)
            .expect("outgoing channel should be open if connection is not closed");

        true
    }
}

#[cfg(test)]
mod test {
    use super::*;

    use std::net::SocketAddr;

    const BUF: usize = 2048;
    const DELAY: Duration = Duration::from_millis(100);

    fn conn(endpoint: Endpoint) -> Connection<BUF> {
        let (outgoing, _) = mpsc::channel();

        let peer = SocketAddr::from(([127, 0, 0, 1], 3400));
        let cid = ConnectionId {
            send: 101,
            recv: 100,
            peer,
        };

        Connection {
            state: State::Connecting,
            cid,
            endpoint,
            peer_ts_diff: Duration::from_millis(100),
            peer_recv_window: u32::MAX,
            outgoing,
        }
    }

    #[test]
    fn on_syn_initiator() {
        let syn = 100;
        let endpoint = Endpoint::Initiator((syn, None));
        let mut conn = conn(endpoint);

        conn.on_syn(syn);
        assert!(std::matches!(
            conn.state,
            State::Closed {
                err: Some(Error::SynFromAcceptor)
            }
        ));
    }

    #[test]
    fn on_syn_acceptor() {
        let syn = 100;
        let syn_ack = 101;
        let endpoint = Endpoint::Acceptor((syn, syn_ack));
        let mut conn = conn(endpoint);

        conn.on_syn(syn);
        assert!(std::matches!(conn.state, State::Connecting { .. }));
    }

    #[test]
    fn on_syn_acceptor_non_matching_syn() {
        let syn = 100;
        let syn_ack = 101;
        let endpoint = Endpoint::Acceptor((syn, syn_ack));
        let mut conn = conn(endpoint);

        let alt_syn = 128;
        conn.on_syn(alt_syn);
        assert!(std::matches!(
            conn.state,
            State::Closed {
                err: Some(Error::InvalidSyn)
            }
        ));
    }

    #[test]
    fn on_state_connecting_initiator() {
        let syn = 100;
        let endpoint = Endpoint::Initiator((syn, None));
        let mut conn = conn(endpoint);

        let seq_num = 1;
        let now = Instant::now();
        conn.on_state(seq_num, syn, None, DELAY, now);

        assert!(std::matches!(conn.state, State::Established { .. }));
    }

    #[test]
    fn on_state_established_invalid_ack_num() {
        let syn = 100;
        let syn_ack = 101;
        let endpoint = Endpoint::Acceptor((syn, syn_ack));
        let mut conn = conn(endpoint);

        let congestion_ctrl = congestion::Controller::new(congestion::Config::default());
        let mut sent_packets = SentPackets::new(syn_ack, congestion_ctrl);

        let data = vec![0xef];
        let len = 64;
        let now = Instant::now();
        sent_packets.on_transmit(
            syn_ack.wrapping_add(1),
            PacketType::Data,
            Some(data),
            len,
            now,
        );

        let recv_buf = ReceiveBuffer::new(syn);

        conn.state = State::Established {
            sent_packets,
            recv_buf,
        };

        let now = Instant::now();
        conn.on_state(
            syn.wrapping_add(1),
            syn_ack.wrapping_add(2),
            None,
            DELAY,
            now,
        );

        assert!(std::matches!(
            conn.state,
            State::Closed {
                err: Some(Error::InvalidAckNum)
            }
        ));
    }

    #[test]
    fn on_data_connecting_acceptor() {
        let syn = 100;
        let syn_ack = 101;
        let endpoint = Endpoint::Acceptor((syn, syn_ack));
        let mut conn = conn(endpoint);

        let seq_num = syn.wrapping_add(1);
        let data = vec![0xef];
        conn.on_data(seq_num, &data);

        assert!(std::matches!(conn.state, State::Established { .. }));
    }

    #[test]
    fn on_fin_established() {
        let syn = 100;
        let syn_ack = 101;
        let endpoint = Endpoint::Acceptor((syn, syn_ack));
        let mut conn = conn(endpoint);

        let congestion_ctrl = congestion::Controller::new(congestion::Config::default());
        let sent_packets = SentPackets::new(syn_ack, congestion_ctrl);
        let recv_buf = ReceiveBuffer::new(syn);
        conn.state = State::Established {
            sent_packets,
            recv_buf,
        };

        let fin = syn.wrapping_add(3);
        conn.on_fin(fin, None);
        match conn.state {
            State::Closing {
                local_fin,
                remote_fin,
                ..
            } => {
                assert!(local_fin.is_none());
                assert_eq!(remote_fin, Some(fin));
            }
            _ => panic!("connection should be closing"),
        }
    }

    #[test]
    fn on_fin_closing() {
        let syn = 100;
        let syn_ack = 101;
        let endpoint = Endpoint::Acceptor((syn, syn_ack));
        let mut conn = conn(endpoint);

        let congestion_ctrl = congestion::Controller::new(congestion::Config::default());
        let sent_packets = SentPackets::new(syn_ack, congestion_ctrl);
        let recv_buf = ReceiveBuffer::new(syn);
        let local_fin = syn_ack.wrapping_add(3);
        conn.state = State::Closing {
            sent_packets,
            recv_buf,
            local_fin: Some(local_fin),
            remote_fin: None,
        };

        let remote_fin = syn.wrapping_add(3);
        conn.on_fin(remote_fin, None);
        match conn.state {
            State::Closing {
                local_fin: local,
                remote_fin: remote,
                ..
            } => {
                assert_eq!(local, Some(local_fin));
                assert_eq!(remote, Some(remote_fin));
            }
            _ => panic!("connection should be closing"),
        }
    }

    #[test]
    fn on_fin_closing_non_matching_fin() {
        let syn = 100;
        let syn_ack = 101;
        let endpoint = Endpoint::Acceptor((syn, syn_ack));
        let mut conn = conn(endpoint);

        let congestion_ctrl = congestion::Controller::new(congestion::Config::default());
        let sent_packets = SentPackets::new(syn_ack, congestion_ctrl);
        let recv_buf = ReceiveBuffer::new(syn);
        let fin = syn.wrapping_add(3);
        conn.state = State::Closing {
            sent_packets,
            recv_buf,
            remote_fin: Some(fin),
            local_fin: None,
        };

        let alt_fin = fin.wrapping_add(1);
        conn.on_fin(alt_fin, None);
        assert!(std::matches!(
            conn.state,
            State::Closed {
                err: Some(Error::InvalidFin)
            }
        ));
    }

    #[test]
    fn on_reset_non_closed() {
        let syn = 100;
        let endpoint = Endpoint::Initiator((syn, None));
        let mut conn = conn(endpoint);

        conn.on_reset();
        assert!(std::matches!(
            conn.state,
            State::Closed {
                err: Some(Error::Reset)
            }
        ));
    }

    #[test]
    fn on_reset_closed() {
        let syn = 100;
        let endpoint = Endpoint::Initiator((syn, None));
        let mut conn = conn(endpoint);

        conn.state = State::Closed { err: None };

        conn.on_reset();
        assert!(std::matches!(conn.state, State::Closed { err: None }));
    }
}
