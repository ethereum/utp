use std::cmp;
use std::collections::VecDeque;
use std::fmt;
use std::io;
use std::time::{Duration, Instant};

use delay_map::HashMapDelay;
use futures::StreamExt;
use tokio::sync::{mpsc, oneshot, Notify};

use crate::cid::ConnectionId;
use crate::congestion;
use crate::event::{SocketEvent, StreamEvent};
use crate::packet::{Packet, PacketBuilder, PacketType, SelectiveAck};
use crate::peer::ConnectionPeer;
use crate::peer::Peer;
use crate::recv::ReceiveBuffer;
use crate::send::SendBuffer;
use crate::sent::{SentPackets, SentPacketsError};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Error {
    EmptyDataPayload,
    InvalidAckNum,
    InvalidFin,
    InvalidSeqNum,
    InvalidSyn,
    Reset,
    SynFromAcceptor,
    TimedOut,
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            Self::EmptyDataPayload => "missing payload in DATA packet",
            Self::InvalidAckNum => "received ACK for unsent packet",
            Self::InvalidFin => "received multiple FIN packets with distinct sequence numbers",
            Self::InvalidSeqNum => {
                "received packet with sequence number outside of remote peer's [SYN,FIN] range"
            }
            Self::InvalidSyn => "received multiple SYN packets with distinct sequence numbers",
            Self::Reset => "received RESET packet from remote peer",
            Self::SynFromAcceptor => "received SYN packet from connection acceptor",
            Self::TimedOut => "connection timed out",
        };

        write!(f, "{s}")
    }
}

impl From<Error> for io::ErrorKind {
    fn from(value: Error) -> Self {
        use Error::*;
        match value {
            EmptyDataPayload | InvalidAckNum | InvalidFin | InvalidSeqNum | InvalidSyn
            | SynFromAcceptor => io::ErrorKind::InvalidData,
            Reset => io::ErrorKind::ConnectionReset,
            TimedOut => io::ErrorKind::TimedOut,
        }
    }
}

impl From<Error> for io::Error {
    fn from(err: Error) -> Self {
        let err_kind = io::ErrorKind::from(err);
        err_kind.into()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Endpoint {
    Initiator((u16, usize)),
    Acceptor((u16, u16)),
}

struct Closing {
    local_fin: Option<u16>,
    remote_fin: Option<u16>,
}

enum State<const N: usize> {
    Connecting(Option<oneshot::Sender<io::Result<()>>>),
    Connected {
        recv_buf: ReceiveBuffer<N>,
        send_buf: SendBuffer<N>,
        sent_packets: SentPackets,
        closing: Option<Closing>,
    },
    Closed {
        err: Option<Error>,
    },
}

impl<const N: usize> fmt::Debug for State<N> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Connecting(_) => write!(f, "State::Connecting"),
            Self::Connected { closing, .. } => match closing {
                Some(Closing {
                    local_fin,
                    remote_fin,
                }) => f
                    .debug_struct("State::Connected: in Closing state")
                    .field("local_fin", local_fin)
                    .field("remote_fin", remote_fin)
                    .finish(),
                None => f
                    .debug_struct("State::Connected: Established state")
                    .finish(),
            },
            Self::Closed { err } => f.debug_struct("State::Closed").field("err", err).finish(),
        }
    }
}

pub type Write = (Vec<u8>, oneshot::Sender<io::Result<usize>>);
type QueuedWrite = (
    // Remaining bytes to write
    Vec<u8>,
    // Number of bytes successfully written in previous partial writes.
    // Sometimes the content is larger than the buffer and must be written in parts.
    usize,
    // oneshot sender to notify about the final result of the write operation
    oneshot::Sender<io::Result<usize>>,
);
pub type Read = io::Result<Vec<u8>>;

#[derive(Clone, Copy, Debug)]
pub struct ConnectionConfig {
    pub max_packet_size: u16,
    /// The maximum number of connection attempts to make before giving up.
    /// Note: if the max_idle_timeout is set too low, then the connection may time out before all
    /// these attempts can be executed.
    pub max_conn_attempts: usize,
    pub max_idle_timeout: Duration,
    pub initial_timeout: Duration,
    pub min_timeout: Duration,
    pub max_timeout: Duration,
    pub target_delay: Duration,
}

pub const DEFAULT_MAX_IDLE_TIMEOUT: Duration = Duration::from_secs(60);

impl Default for ConnectionConfig {
    fn default() -> Self {
        Self {
            max_conn_attempts: 6,
            max_idle_timeout: DEFAULT_MAX_IDLE_TIMEOUT,
            max_packet_size: congestion::DEFAULT_MAX_PACKET_SIZE_BYTES as u16,
            initial_timeout: congestion::DEFAULT_INITIAL_TIMEOUT,
            min_timeout: congestion::DEFAULT_MIN_TIMEOUT,
            max_timeout: DEFAULT_MAX_IDLE_TIMEOUT / 4,
            target_delay: Duration::from_micros(congestion::DEFAULT_TARGET_MICROS.into()),
        }
    }
}

impl From<ConnectionConfig> for congestion::Config {
    fn from(value: ConnectionConfig) -> Self {
        Self {
            max_packet_size_bytes: u32::from(value.max_packet_size),
            initial_timeout: value.initial_timeout,
            min_timeout: value.min_timeout,
            max_timeout: value.max_timeout,
            target_delay_micros: value.target_delay.as_micros() as u32,
            ..Default::default()
        }
    }
}

pub struct Connection<const N: usize, P: ConnectionPeer> {
    state: State<N>,
    cid: ConnectionId<P::Id>,
    peer: Peer<P>,
    config: ConnectionConfig,
    endpoint: Endpoint,
    peer_ts_diff: Duration,
    peer_recv_window: u32,
    socket_events: mpsc::UnboundedSender<SocketEvent<P>>,
    unacked: HashMapDelay<u16, Packet>,
    reads: mpsc::UnboundedSender<Read>,
    readable: Notify,
    pending_writes: VecDeque<QueuedWrite>,
    writable: Notify,
    latest_timeout: Option<Instant>,
    /// Save the first STATE packet returned to an incoming connection request.
    /// We resend the exact packet in case the first response to the SYN packet is lost,
    /// because regenerating the STATE packet gives us the wrong sequence number, and causes the
    /// connecting peer to ignore earlier data sent to them.
    syn_state: Option<Packet>,
}

impl<const N: usize, P: ConnectionPeer> Connection<N, P> {
    pub fn new(
        cid: ConnectionId<P::Id>,
        peer: Peer<P>,
        config: ConnectionConfig,
        syn: Option<Packet>,
        connected: oneshot::Sender<io::Result<()>>,
        socket_events: mpsc::UnboundedSender<SocketEvent<P>>,
        reads: mpsc::UnboundedSender<Read>,
    ) -> Self {
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
                let endpoint = Endpoint::Initiator((syn, 0));
                (endpoint, Duration::ZERO, u32::MAX)
            }
        };

        Self {
            state: State::Connecting(Some(connected)),
            cid,
            peer,
            config,
            endpoint,
            peer_ts_diff,
            peer_recv_window,
            socket_events,
            unacked: HashMapDelay::new(config.initial_timeout),
            reads,
            readable: Notify::new(),
            pending_writes: VecDeque::new(),
            writable: Notify::new(),
            latest_timeout: None,
            syn_state: None,
        }
    }

    pub async fn event_loop(
        &mut self,
        mut stream_events: mpsc::UnboundedReceiver<StreamEvent>,
        mut writes: mpsc::UnboundedReceiver<Write>,
        mut shutdown: oneshot::Receiver<()>,
    ) -> io::Result<()> {
        tracing::debug!("uTP conn starting... {:?}", self.peer);

        // If we are the initiating endpoint, then send the SYN. If we are the accepting endpoint,
        // then send the STATE in response to the SYN.
        match self.endpoint {
            Endpoint::Initiator((syn_seq_num, ..)) => {
                let syn = self.syn_packet(syn_seq_num);
                self.socket_events
                    .send(SocketEvent::Outgoing((syn.clone(), self.peer.clone())))
                    .unwrap();
                self.unacked
                    .insert_at(syn_seq_num, syn, self.config.initial_timeout);

                self.endpoint = Endpoint::Initiator((syn_seq_num, 1));
            }
            Endpoint::Acceptor((syn, syn_ack)) => {
                let state = self.state_packet().unwrap();
                self.syn_state = Some(state.clone());
                self.socket_events
                    .send(SocketEvent::Outgoing((state, self.peer.clone())))
                    .unwrap();

                let recv_buf = ReceiveBuffer::new(syn);
                let send_buf = SendBuffer::new();

                let congestion_ctrl = congestion::Controller::new(self.config.into());

                // NOTE: We initialize with the sequence number of the SYN-ACK minus 1 because the
                // SYN-ACK contains the incremented sequence number (i.e. the next sequence
                // number). This is consistent with the reference implementation and the libtorrent
                // implementation where STATE packets set the sequence number to the next sequence
                // number.
                let sent_packets = SentPackets::new(syn_ack.wrapping_sub(1), congestion_ctrl);

                // The connection must be in the `Connecting` state. We optimistically mark the
                // connection `Established` here. This enables the accepting endpoint to send DATA
                // packets before receiving any from the initiator.
                //
                // TODO: Find a more elegant way to emit the connected message.
                if let State::Connecting(connected) = &mut self.state {
                    let _ = connected.take().unwrap().send(Ok(()));
                } else {
                    panic!("connection in invalid state prior to event loop beginning");
                }

                self.state = State::Connected {
                    recv_buf,
                    send_buf,
                    sent_packets,
                    closing: None,
                };
            }
        }

        let mut shutting_down = false;
        let idle_timeout = tokio::time::sleep(self.config.max_idle_timeout);
        tokio::pin!(idle_timeout);
        loop {
            tokio::select! {
                biased;
                Some(event) = stream_events.recv() => {
                    match event {
                        StreamEvent::Incoming(packet) => {
                            // Reset the idle timeout on any incoming packet.
                            let idle_deadline = tokio::time::Instant::now() + self.config.max_idle_timeout;
                            idle_timeout.as_mut().reset(idle_deadline);

                            self.on_packet(&packet, Instant::now());
                        }
                        StreamEvent::Shutdown => {
                            shutting_down = true;
                        }
                    }
                }
                Some(write) = writes.recv(), if !shutting_down => {
                    // Reset the idle timeout on any new write.
                    let idle_deadline = tokio::time::Instant::now() + self.config.max_idle_timeout;
                    idle_timeout.as_mut().reset(idle_deadline);

                    self.on_write(write);
                }
                _ = self.readable.notified() => {
                    self.process_reads();
                }
                _ = self.writable.notified() => {
                    self.process_writes(Instant::now());
                }
                Some(Ok(timeout)) = self.unacked.next() => {
                    let (seq, packet) = timeout;
                    tracing::debug!(seq, ack = %packet.ack_num(), packet = ?packet.packet_type(), "timeout");

                    self.on_timeout(packet, Instant::now());
                }
                () = &mut idle_timeout => {
                    if !std::matches!(self.state, State::Closed { .. }) {
                        let unacked: Vec<u16> = self.unacked.keys().copied().collect();
                        tracing::warn!(?unacked, "idle timeout expired, closing...");
                        self.state = State::Closed { err: Some(Error::TimedOut) };
                    }
                }
                _ = &mut shutdown, if !shutting_down => {
                    tracing::debug!("uTP conn initiating shutdown...");
                    shutting_down = true;
                }
            }

            if shutting_down && !std::matches!(self.state, State::Closed { .. }) {
                self.shutdown();
            }

            if let State::Closed { err } = self.state {
                tracing::debug!(?err, "uTP conn closing...");
                self.process_reads();
                self.process_writes(Instant::now());

                if let Err(err) = self
                    .socket_events
                    .send(SocketEvent::Shutdown(self.cid.clone()))
                {
                    tracing::error!(
                        "unable to send shutdown signal to uTP socket: {}",
                        err.to_string()
                    );
                }

                if let Some(err) = err {
                    return Err(err.into());
                } else {
                    return Ok(());
                }
            }
        }
    }

    fn shutdown(&mut self) {
        match &mut self.state {
            State::Connecting(..) => {
                self.state = State::Closed { err: None };
            }
            State::Connected {
                send_buf,
                sent_packets,
                recv_buf,
                closing,
            } => {
                match closing {
                    Some(Closing { local_fin, .. }) => {
                        // If we have not sent our FIN, and there are no pending writes, and there is no
                        // pending data in the send buffer, then send our FIN. If there were still data to
                        // send, then we would not know which sequence number to assign to our FIN.
                        if local_fin.is_none()
                            && self.pending_writes.is_empty()
                            && send_buf.is_empty()
                        {
                            // TODO: Helper for construction of FIN.
                            let recv_window = recv_buf.available() as u32;
                            let seq_num = sent_packets.next_seq_num();
                            let ack_num = recv_buf.ack_num();
                            let selective_ack = recv_buf.selective_ack();
                            let fin = PacketBuilder::new(
                                PacketType::Fin,
                                self.cid.send,
                                crate::time::now_micros(),
                                recv_window,
                                seq_num,
                            )
                            .ack_num(ack_num)
                            .selective_ack(selective_ack)
                            .build();

                            *local_fin = Some(seq_num);

                            tracing::debug!(seq = %seq_num, "transmitting FIN");
                            Self::transmit(
                                sent_packets,
                                &mut self.unacked,
                                &mut self.socket_events,
                                fin,
                                &self.peer,
                                Instant::now(),
                            );
                        }
                    }
                    None => {
                        let mut local_fin = None;
                        if self.pending_writes.is_empty() && send_buf.is_empty() {
                            // TODO: Helper for construction of FIN.
                            let recv_window = recv_buf.available() as u32;
                            let seq_num = sent_packets.next_seq_num();
                            let ack_num = recv_buf.ack_num();
                            let selective_ack = recv_buf.selective_ack();
                            let fin = PacketBuilder::new(
                                PacketType::Fin,
                                self.cid.send,
                                crate::time::now_micros(),
                                recv_window,
                                seq_num,
                            )
                            .ack_num(ack_num)
                            .selective_ack(selective_ack)
                            .build();

                            local_fin = Some(seq_num);

                            tracing::debug!(seq = %seq_num, "transmitting FIN");
                            Self::transmit(
                                sent_packets,
                                &mut self.unacked,
                                &mut self.socket_events,
                                fin,
                                &self.peer,
                                Instant::now(),
                            );
                        }
                        *closing = Some(Closing {
                            local_fin,
                            remote_fin: None,
                        });
                    }
                }
            }
            State::Closed { .. } => {}
        }
    }

    fn process_writes(&mut self, now: Instant) {
        let (send_buf, sent_packets, recv_buf) = match &mut self.state {
            State::Connecting(..) => return,
            State::Connected {
                send_buf,
                sent_packets,
                recv_buf,
                ..
            } => (send_buf, sent_packets, recv_buf),
            State::Closed { err, .. } => {
                let result = match err {
                    Some(err) => Err(io::ErrorKind::from(*err)),
                    None => Ok(0),
                };
                while let Some(pending) = self.pending_writes.pop_front() {
                    let (.., tx) = pending;
                    let _ = tx.send(result.map_err(io::Error::from));
                }
                return;
            }
        };

        // Compose as many data packets as possible, consuming data from the send buffer.
        let now_micros = crate::time::now_micros();
        let mut window = cmp::min(sent_packets.window(), self.peer_recv_window) as usize;
        let mut payloads = Vec::new();
        while window > 0 {
            // TODO: Do not rely on approximation here. Account for header and extensions.
            let max_data_size = cmp::min(window, usize::from(self.config.max_packet_size - 64));
            let mut data = vec![0; max_data_size];
            let n = send_buf.read(&mut data).unwrap();
            if n == 0 {
                break;
            }
            data.truncate(n);
            payloads.push(data);

            window -= n;
        }

        // Write as much data as possible into send buffer.
        while self.pending_writes.front().is_some() {
            let buf_space = send_buf.available();
            if buf_space > 0 {
                let (mut data, written, tx) = self.pending_writes.pop_front().unwrap();
                if data.len() <= buf_space {
                    send_buf.write(&data).unwrap();
                    let _ = tx.send(Ok(data.len() + written));
                } else {
                    let next_write = data.drain(..buf_space);
                    send_buf.write(next_write.as_slice()).unwrap();
                    drop(next_write);
                    // data was mutated by drain, so we only store the remaining data
                    self.pending_writes
                        .push_front((data, buf_space + written, tx));
                }
                self.writable.notify_one();
            } else {
                break;
            }
        }

        // Transmit data packets.
        // TODO: Helper for construction of DATA packet.
        let mut seq_num = sent_packets.next_seq_num();
        let recv_window = recv_buf.available() as u32;
        let ack_num = recv_buf.ack_num();
        let selective_ack = recv_buf.selective_ack();
        for payload in payloads.into_iter() {
            let packet = PacketBuilder::new(
                PacketType::Data,
                self.cid.send,
                now_micros,
                recv_window,
                seq_num,
            )
            .payload(payload)
            .ts_diff_micros(self.peer_ts_diff.as_micros() as u32)
            .ack_num(ack_num)
            .selective_ack(selective_ack.clone())
            .build();
            Self::transmit(
                sent_packets,
                &mut self.unacked,
                &mut self.socket_events,
                packet,
                &self.peer,
                now,
            );
            seq_num = seq_num.wrapping_add(1);
        }
    }

    fn on_write(&mut self, write: Write) {
        let (data, tx) = write;

        match &mut self.state {
            State::Connecting(..) => {
                // There are 0 bytes written so far
                self.pending_writes.push_back((data, 0, tx));
            }
            State::Connected { closing, .. } => match closing {
                Some(Closing {
                    local_fin,
                    remote_fin,
                }) => {
                    if local_fin.is_none() && remote_fin.is_some() {
                        self.pending_writes.push_back((data, 0, tx));
                    } else {
                        let _ = tx.send(Ok(0));
                    }
                }
                None => {
                    self.pending_writes.push_back((data, 0, tx));
                }
            },
            State::Closed { err, .. } => {
                let result = match err {
                    Some(err) => Err(io::Error::from(io::ErrorKind::from(*err))),
                    None => Ok(0),
                };
                let _ = tx.send(result);
            }
        }

        self.process_writes(Instant::now());

        self.writable.notify_waiters();
    }

    fn process_reads(&mut self) {
        let recv_buf = match &mut self.state {
            State::Connecting(..) => return,
            State::Connected { recv_buf, .. } => recv_buf,
            State::Closed { err } => {
                let result = match err {
                    Some(err) => Err(io::ErrorKind::from(*err).into()),
                    None => Ok(vec![]),
                };

                if let Err(err) = self.reads.send(result) {
                    tracing::warn!(
                        "unable to push empty vector to read buffer: {}",
                        err.to_string()
                    );
                }
                return;
            }
        };

        while !recv_buf.is_empty() {
            let mut buf = vec![0; self.config.max_packet_size as usize];
            let n = recv_buf.read(&mut buf).unwrap();
            if n == 0 {
                break;
            } else {
                buf.truncate(n);
                if let Err(err) = self.reads.send(Ok(buf)) {
                    tracing::warn!("unable to push data to read buffer: {}", err.to_string());
                }
            }
        }

        // If we have reached EOF, then send an empty result to all pending reads.
        if self.eof() {
            if let Err(err) = self.reads.send(Ok(vec![])) {
                tracing::warn!(
                    "unable to push empty vector at eof to read buffer: {}",
                    err.to_string()
                );
            }
        }
    }

    fn eof(&self) -> bool {
        match &self.state {
            State::Connecting(..) => false,
            State::Connected {
                recv_buf, closing, ..
            } => match closing {
                Some(Closing {
                    local_fin: _,
                    remote_fin: Some(fin),
                }) => recv_buf.ack_num() == *fin,
                _ => false,
            },
            State::Closed { .. } => true,
        }
    }

    fn on_timeout(&mut self, packet: Packet, now: Instant) {
        match &mut self.state {
            State::Connecting(connected) => match &mut self.endpoint {
                Endpoint::Initiator((syn, attempts)) => {
                    if *attempts >= self.config.max_conn_attempts {
                        tracing::error!("quitting connection attempt, after {} tries", *attempts);
                        let err = Error::TimedOut;
                        if let Some(connected) = connected.take() {
                            let err = io::Error::from(io::ErrorKind::from(err));
                            let _ = connected.send(Err(err));
                        }
                        self.state = State::Closed { err: Some(err) };
                    } else {
                        let seq = *syn;
                        let log_msg = format!("retrying connection, after {} attempts", *attempts);
                        match *attempts {
                            1 => tracing::trace!(log_msg),
                            2 => tracing::debug!(log_msg),
                            3 => tracing::info!(log_msg),
                            _ => tracing::warn!(log_msg),
                        }
                        *attempts += 1;

                        // Double previous timeout for exponential backoff on each attempt
                        let timeout = self
                            .config
                            .initial_timeout
                            .mul_f32((1.5_f32).powf(*attempts as f32));
                        self.unacked.insert_at(seq, packet, timeout);

                        // Re-send SYN packet.
                        let packet = self.syn_packet(seq);
                        let _ = self
                            .socket_events
                            .send(SocketEvent::Outgoing((packet, self.peer.clone())));
                    }
                }
                Endpoint::Acceptor(..) => {}
            },
            State::Connected {
                sent_packets,
                recv_buf,
                ..
            } => {
                // If the timed out packet is a SYN, then do nothing since the connection has
                // already been established.
                if std::matches!(packet.packet_type(), PacketType::Syn) {
                    return;
                }

                // To prevent timeout amplification in the event that a batch of packets sent near
                // the same time all timeout, we only register a new timeout if the time elapsed
                // since the latest timeout is greater than the existing timeout.
                //
                // For example, if the current congestion control timeout is 1s, then we only
                // register a new timeout if the time elapsed since the latest registered timeout
                // is greater than 1s.
                let timeout = match self.latest_timeout {
                    Some(latest) => latest.elapsed() > sent_packets.timeout(),
                    None => true,
                };
                if timeout {
                    sent_packets.on_timeout();
                    self.latest_timeout = Some(Instant::now());
                }

                // TODO: Limit number of retransmissions.
                let recv_window = recv_buf.available() as u32;
                let now_micros = crate::time::now_micros();
                let ts_diff_micros = self.peer_ts_diff.as_micros() as u32;
                let packet = PacketBuilder::from(packet)
                    .window_size(recv_window)
                    .ack_num(recv_buf.ack_num())
                    .selective_ack(recv_buf.selective_ack())
                    .ts_micros(now_micros)
                    .ts_diff_micros(ts_diff_micros)
                    .build();
                Self::transmit(
                    sent_packets,
                    &mut self.unacked,
                    &mut self.socket_events,
                    packet,
                    &self.peer,
                    now,
                );
            }
            State::Closed { .. } => {}
        }
    }

    fn on_packet(&mut self, packet: &Packet, now: Instant) {
        tracing::trace!("Received {:?}", packet);
        let now_micros = crate::time::now_micros();
        self.peer_recv_window = packet.window_size();

        self.peer_ts_diff = if packet.ts_micros() == 0 {
            Duration::from_micros(0)
        } else {
            Duration::from_micros(now_micros.wrapping_sub(packet.ts_micros()).into())
        };

        match packet.packet_type() {
            PacketType::Syn => self.on_syn(packet.seq_num()),
            PacketType::State => self.on_state(packet.seq_num(), packet.ack_num()),
            PacketType::Data => self.on_data(packet.seq_num(), packet.payload()),
            PacketType::Fin => self.on_fin(packet.seq_num(), packet.payload()),
            PacketType::Reset => self.on_reset(),
        }

        // Mark sent packets as acked, using the received ack_num
        match packet.packet_type() {
            PacketType::Syn | PacketType::Reset => {}
            PacketType::State | PacketType::Data | PacketType::Fin => {
                let delay = Duration::from_micros(packet.ts_diff_micros().into());
                if let Err(err) =
                    self.process_ack(packet.ack_num(), packet.selective_ack(), delay, now)
                {
                    tracing::warn!(%err, ?packet, "ack does not correspond to known seq_num");
                }
            }
        }

        // If there are any lost packets, then queue retransmissions.
        self.retransmit_lost_packets(now);

        // Send a STATE packet if appropriate packet and connection in appropriate state.
        //
        // NOTE: We don't call `transmit` because we do not wish to track this packet.
        // NOTE: We should probably move this so that multiple incoming packets can be processed
        // before we send a STATE.
        match packet.packet_type() {
            PacketType::Syn => {
                if self.syn_state.is_none() {
                    if let Some(state) = self.state_packet() {
                        tracing::warn!("Oddly missing SYN STATE, some risk to data validity");
                        // The syn_state is generated at the beginning of the event_loop, so should
                        // not be None here. So regenerating the STATE packet is a defensive
                        // measure to keep working anyway, rather than ignore repeated SYN packets.
                        // Regenerating runs the risk of causing the remote peer to ignore earlier
                        // data sent to them, because the generated sequence number will be higher
                        // than any previously sent data, which causes the remote peer to ignore
                        // it silently, but think it received all the data.
                        self.syn_state = Some(state);
                    }
                }
                if let Some(state) = &self.syn_state {
                    let event = SocketEvent::Outgoing((state.clone(), self.peer.clone()));
                    if self.socket_events.send(event).is_err() {
                        tracing::warn!("Cannot re-transmit syn ack packet: socket closed channel");
                        return;
                    }
                }
            }
            PacketType::Fin | PacketType::Data => {
                if let Some(state) = self.state_packet() {
                    let event = SocketEvent::Outgoing((state, self.peer.clone()));
                    if self.socket_events.send(event).is_err() {
                        tracing::warn!("Cannot transmit state packet: socket closed channel");
                        return;
                    }
                }
            }
            PacketType::State | PacketType::Reset => {}
        };

        // If the packet is a STATE packet, then notify writable because the send window may have
        // increased.
        if std::matches!(packet.packet_type(), PacketType::State) {
            self.writable.notify_one();
        }

        // If the packet contains a data payload, or the packet was a FIN, then notify readable
        // because there may be new data available in the receive buffer and/or reads to complete.
        if !packet.payload().is_empty() || std::matches!(packet.packet_type(), PacketType::Fin) {
            self.readable.notify_one();
        }

        // One-way Fin: Case 1 - We have sent our data and then our fin. If this fin is acked, then the receiver got all our data and we close
        if let State::Connected {
            closing:
                Some(Closing {
                    local_fin: Some(local),
                    remote_fin: _,
                }),
            sent_packets,
            ..
        } = &mut self.state
        {
            if sent_packets.last_ack_num().is_some()
                && sent_packets.last_ack_num().unwrap() == *local
            {
                self.state = State::Closed { err: None };
            }
        }

        // One-way Fin: Case 2 - We have received data and then the fin. We sent an ack, so close the connection
        if let State::Connected {
            closing:
                Some(Closing {
                    local_fin: _,
                    remote_fin: Some(remote),
                }),
            sent_packets,
            recv_buf,
            ..
        } = &mut self.state
        {
            if !sent_packets.has_unacked_packets() && recv_buf.ack_num() == *remote {
                // flush recv_buf before closing the connection
                self.process_reads();
                self.state = State::Closed { err: None };
            }
        }

        // One-way Fin: Case 3 - We sent our Fin, but for some reason they sent us a Fin after that and it didn't ack our Fin
        // We will close the connection and assume they got all of our data and tried to prematurely close the connection as an optimization
        if let State::Connected {
            closing:
                Some(Closing {
                    local_fin: Some(_),
                    remote_fin: Some(_),
                }),
            ..
        } = &mut self.state
        {
            self.state = State::Closed { err: None };
        }
    }

    fn process_ack(
        &mut self,
        ack_num: u16,
        selective_ack: Option<&SelectiveAck>,
        delay: Duration,
        now: Instant,
    ) -> Result<(), Error> {
        match &mut self.state {
            State::Connected { sent_packets, .. } => {
                match sent_packets.on_ack(ack_num, selective_ack, delay, now) {
                    Ok((full_acked, selected_acks)) => {
                        self.unacked.retain(|seq, _| !full_acked.contains(*seq));
                        for seq_num in selected_acks {
                            self.unacked.remove(&seq_num);
                        }
                        Ok(())
                    }
                    Err(err) => match err {
                        SentPacketsError::InvalidAckNum => {
                            let err = Error::InvalidAckNum;
                            self.reset(err);
                            Err(err)
                        }
                    },
                }
            }
            State::Connecting(..) | State::Closed { .. } => Ok(()),
        }
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

    fn on_state(&mut self, seq_num: u16, ack_num: u16) {
        match &mut self.state {
            State::Connecting(connected) => match self.endpoint {
                // If the STATE acknowledges our SYN, then mark the connection established.
                Endpoint::Initiator((syn, ..)) => {
                    if ack_num == syn {
                        // NOTE: In a deviation from the specification, we initialize the ACK num
                        // to the sequence number of the SYN-ACK minus 1. This is consistent with
                        // the reference implementation and the libtorrent implementation.
                        let recv_buf = ReceiveBuffer::new(seq_num.wrapping_sub(1));

                        let send_buf = SendBuffer::new();

                        let congestion_ctrl = congestion::Controller::new(self.config.into());
                        let sent_packets = SentPackets::new(syn, congestion_ctrl);

                        let _ = connected.take().unwrap().send(Ok(()));
                        self.state = State::Connected {
                            recv_buf,
                            send_buf,
                            sent_packets,
                            closing: None,
                        };
                    }
                }
                Endpoint::Acceptor(..) => {}
            },
            State::Connected { .. } | State::Closed { .. } => {}
        }
    }

    fn on_data(&mut self, seq_num: u16, data: &[u8]) {
        // If the data payload is empty, then reset the connection. A DATA packet must contain a
        // non-empty payload.
        if data.is_empty() {
            self.reset(Error::EmptyDataPayload);
        }

        match &mut self.state {
            State::Connecting(..) => match self.endpoint {
                // If we are the accepting endpoint, then the connection should have been marked
                // `Established` at the beginning of the event loop.
                // TODO: Clean this up so that we do not have to rely on `unreachable`.
                Endpoint::Acceptor(..) => unreachable!(),
                // If we are the initiating endpoint, then we cannot mark the connection
                // established, because we are waiting on the STATE for the SYN. Without the STATE,
                // we do not know how to order this data.
                //
                // TODO: Save the data so that it may be written to the receive buffer upon the
                // connection being established.
                Endpoint::Initiator(..) => {}
            },
            State::Connected {
                recv_buf, closing, ..
            } => {
                match closing {
                    // If the connection is closing and we have a remote FIN, then check whether the
                    // sequence number falls within the appropriate range. If it does, and there is
                    // sufficient capacity, then incorporate the data into the receive buffer. If it does
                    // not, then reset the connection.
                    Some(Closing { remote_fin, .. }) => {
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
                    // If the connection is established, and there is sufficient capacity, then incorporate
                    // the data into the receive buffer.
                    None => {
                        if data.len() <= recv_buf.available() && !recv_buf.was_written(seq_num) {
                            recv_buf.write(data, seq_num);
                        }
                    }
                }
                if data.len() <= recv_buf.available() && !recv_buf.was_written(seq_num) {
                    recv_buf.write(data, seq_num);
                }
            }
            State::Closed { .. } => {}
        }
    }

    fn on_fin(&mut self, seq_num: u16, data: &[u8]) {
        match &mut self.state {
            State::Connecting(..) => {}
            State::Connected {
                recv_buf, closing, ..
            } => {
                match closing {
                    Some(Closing { remote_fin, .. }) => {
                        match remote_fin {
                            // If we have already received a FIN, a subsequent FIN with a different
                            // sequence number is incorrect behavior.
                            Some(fin) => {
                                if seq_num != *fin {
                                    self.reset(Error::InvalidFin);
                                }
                            }
                            None => {
                                tracing::debug!(seq = %seq_num, "received FIN");

                                *remote_fin = Some(seq_num);
                                recv_buf.write(data, seq_num);
                            }
                        }
                    }
                    None => {
                        // Register the FIN with the receive buffer.
                        recv_buf.write(data, seq_num);

                        tracing::debug!(seq = %seq_num, "received FIN");

                        *closing = Some(Closing {
                            local_fin: None,
                            remote_fin: Some(seq_num),
                        });
                    }
                }
            }
            State::Closed { .. } => {}
        }
    }

    fn on_reset(&mut self) {
        tracing::warn!("RESET from remote");

        // If the connection is not already closed or reset, then reset the connection.
        if !std::matches!(self.state, State::Closed { .. }) {
            self.reset(Error::Reset);
        }
    }

    fn reset(&mut self, err: Error) {
        tracing::warn!(?err, "resetting connection: {err}");
        // If we already sent our fin and got a reset we assume the receiver already got our fin and has successfully closed their connection.
        // hence mark this as a successful close.
        if let State::Connected {
            closing: Some(Closing {
                local_fin: Some(_), ..
            }),
            ..
        } = self.state
        {
            self.state = State::Closed { err: None };
            return;
        }
        self.state = State::Closed { err: Some(err) }
    }

    fn syn_packet(&self, seq_num: u16) -> Packet {
        let now_micros = crate::time::now_micros();
        PacketBuilder::new(
            PacketType::Syn,
            self.cid.recv,
            now_micros,
            N as u32,
            seq_num,
        )
        .build()
    }

    fn state_packet(&self) -> Option<Packet> {
        let now = crate::time::now_micros();
        let conn_id = self.cid.send;
        let ts_diff_micros = self.peer_ts_diff.as_micros() as u32;

        match &self.state {
            State::Connecting(..) => match self.endpoint {
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
            State::Connected {
                recv_buf,
                sent_packets,
                ..
            } => {
                // NOTE: Consistent with the reference implementation and the libtorrent
                // implementation, STATE packets always include the next sequence number.
                let seq_num = sent_packets.next_seq_num();
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
            State::Connecting(..) | State::Closed { .. } => return,
            State::Connected {
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

            Self::transmit(
                sent_packets,
                &mut self.unacked,
                &mut self.socket_events,
                packet,
                &self.peer,
                now,
            );
        }
    }

    fn transmit(
        sent_packets: &mut SentPackets,
        unacked: &mut HashMapDelay<u16, Packet>,
        socket_events: &mut mpsc::UnboundedSender<SocketEvent<P>>,
        packet: Packet,
        peer: &Peer<P>,
        now: Instant,
    ) {
        let (payload, len) = if packet.payload().is_empty() {
            (None, 0)
        } else {
            (
                Some(packet.payload().clone()),
                packet.payload().len() as u32,
            )
        };
        tracing::trace!(
            "Write data cid={} packetType={} pkSeqNr={} pkAckNr={} length={}",
            packet.conn_id(),
            packet.packet_type(),
            packet.seq_num(),
            packet.ack_num(),
            packet.encoded_len()
        );

        sent_packets.on_transmit(packet.seq_num(), packet.packet_type(), payload, len, now);
        unacked.insert_at(packet.seq_num(), packet.clone(), sent_packets.timeout());
        let outbound = SocketEvent::Outgoing((packet, peer.clone()));
        if socket_events.send(outbound).is_err() {
            tracing::warn!("Cannot transmit packet: socket closed channel");
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    use std::net::SocketAddr;

    const BUF: usize = 2048;
    const DELAY: Duration = Duration::from_millis(100);

    fn conn(endpoint: Endpoint) -> Connection<BUF, SocketAddr> {
        let (connected, _) = oneshot::channel();
        let (socket_events, _) = mpsc::unbounded_channel();
        let (reads_tx, _) = mpsc::unbounded_channel();

        let peer = SocketAddr::from(([127, 0, 0, 1], 3400));
        let cid = ConnectionId {
            send: 101,
            recv: 100,
            peer_id: peer,
        };

        Connection {
            state: State::Connecting(Some(connected)),
            cid,
            peer: Peer::new(peer),
            config: ConnectionConfig::default(),
            endpoint,
            peer_ts_diff: Duration::from_millis(100),
            peer_recv_window: u32::MAX,
            socket_events,
            unacked: HashMapDelay::new(DELAY),
            reads: reads_tx,
            readable: Notify::new(),
            pending_writes: VecDeque::new(),
            writable: Notify::new(),
            latest_timeout: None,
            syn_state: None,
        }
    }

    #[test]
    fn on_syn_initiator() {
        let syn = 100;
        let endpoint = Endpoint::Initiator((syn, 1));
        let mut conn = conn(endpoint);

        conn.on_syn(syn);
        assert!(std::matches!(
            conn.state,
            State::Closed {
                err: Some(Error::SynFromAcceptor),
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
                err: Some(Error::InvalidSyn),
            }
        ));
    }

    #[test]
    fn on_state_connecting_initiator() {
        let syn = 100;
        let endpoint = Endpoint::Initiator((syn, 1));
        let mut conn = conn(endpoint);

        let seq_num = 1;
        conn.on_state(seq_num, syn);

        assert!(std::matches!(
            conn.state,
            State::Connected { closing: None, .. }
        ));
    }

    #[test]
    /// If a received ack_num is not a known sequence number, then reset the connection.
    fn on_packet_invalid_ack_num() {
        let syn = 100;
        let syn_ack = 101;
        let endpoint = Endpoint::Acceptor((syn, syn_ack));
        let mut conn = conn(endpoint);

        let congestion_ctrl = congestion::Controller::new(conn.config.into());
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

        let send_buf = SendBuffer::new();
        let recv_buf = ReceiveBuffer::new(syn);

        conn.state = State::Connected {
            sent_packets,
            send_buf,
            recv_buf,
            closing: None,
        };

        // Dummy delay, value is irrelevant
        let delay = Duration::from_millis(100);
        let ack_result = conn.process_ack(syn_ack.wrapping_add(2), None, delay, now);

        assert!(std::matches!(
            conn.state,
            State::Closed {
                err: Some(Error::InvalidAckNum),
            }
        ));
        assert! {
            ack_result.is_err(),
            "Expected received_ack to return an error"
        };
    }

    #[test]
    fn on_fin_established() {
        let syn = 100;
        let syn_ack = 101;
        let endpoint = Endpoint::Acceptor((syn, syn_ack));
        let mut conn = conn(endpoint);

        let send_buf = SendBuffer::new();
        let congestion_ctrl = congestion::Controller::new(conn.config.into());
        let sent_packets = SentPackets::new(syn_ack, congestion_ctrl);
        let recv_buf = ReceiveBuffer::new(syn);

        conn.state = State::Connected {
            send_buf,
            sent_packets,
            recv_buf,
            closing: None,
        };

        let fin = syn.wrapping_add(3);
        conn.on_fin(fin, &[]);
        match conn.state {
            State::Connected {
                closing:
                    Some(Closing {
                        local_fin,
                        remote_fin,
                    }),
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

        let send_buf = SendBuffer::new();
        let congestion_ctrl = congestion::Controller::new(conn.config.into());
        let sent_packets = SentPackets::new(syn_ack, congestion_ctrl);
        let recv_buf = ReceiveBuffer::new(syn);

        let local_fin = syn_ack.wrapping_add(3);
        conn.state = State::Connected {
            send_buf,
            sent_packets,
            recv_buf,
            closing: Some(Closing {
                local_fin: Some(local_fin),
                remote_fin: None,
            }),
        };

        let remote_fin = syn.wrapping_add(3);
        conn.on_fin(remote_fin, &[]);
        match conn.state {
            State::Connected {
                closing:
                    Some(Closing {
                        local_fin: local,
                        remote_fin: remote,
                    }),
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

        let send_buf = SendBuffer::new();
        let congestion_ctrl = congestion::Controller::new(conn.config.into());
        let sent_packets = SentPackets::new(syn_ack, congestion_ctrl);
        let recv_buf = ReceiveBuffer::new(syn);

        let fin = syn.wrapping_add(3);
        conn.state = State::Connected {
            send_buf,
            sent_packets,
            recv_buf,
            closing: Some(Closing {
                local_fin: None,
                remote_fin: Some(fin),
            }),
        };

        let alt_fin = fin.wrapping_add(1);
        conn.on_fin(alt_fin, &[]);
        assert!(std::matches!(
            conn.state,
            State::Closed {
                err: Some(Error::InvalidFin),
            }
        ));
    }

    #[test]
    fn on_reset_non_closed() {
        let syn = 100;
        let endpoint = Endpoint::Initiator((syn, 1));
        let mut conn = conn(endpoint);

        conn.on_reset();
        assert!(std::matches!(
            conn.state,
            State::Closed {
                err: Some(Error::Reset),
            }
        ));
    }

    #[test]
    fn on_reset_closed() {
        let syn = 100;
        let endpoint = Endpoint::Initiator((syn, 1));
        let mut conn = conn(endpoint);

        conn.state = State::Closed { err: None };

        conn.on_reset();
        assert!(std::matches!(conn.state, State::Closed { err: None }));
    }

    #[test]
    fn state_debug_format() {
        // Test that the state enum can be formatted with debug formatting.

        // Test Connecting
        let state: State<BUF> = State::Connecting(None);
        let expected_format = "State::Connecting";
        let actual_format = format!("{:?}", state);
        assert_eq!(actual_format, expected_format);

        // Test Established
        let state: State<BUF> = State::Connected {
            sent_packets: SentPackets::new(
                0,
                congestion::Controller::new(congestion::Config::default()),
            ),
            send_buf: SendBuffer::new(),
            recv_buf: ReceiveBuffer::new(0),
            closing: None,
        };
        let expected_format = "State::Connected: Established state";
        let actual_format = format!("{:?}", state);
        assert_eq!(actual_format, expected_format);

        // Test Closing
        let state: State<BUF> = State::Connected {
            closing: Some(Closing {
                local_fin: Some(12),
                remote_fin: Some(34),
            }),
            sent_packets: SentPackets::new(
                0,
                congestion::Controller::new(congestion::Config::default()),
            ),
            send_buf: SendBuffer::new(),
            recv_buf: ReceiveBuffer::new(0),
        };
        let expected_format =
            "State::Connected: in Closing state { local_fin: Some(12), remote_fin: Some(34) }";
        let actual_format = format!("{:?}", state);
        assert_eq!(actual_format, expected_format);

        // Test Closed
        let state: State<BUF> = State::Closed {
            err: Some(Error::Reset),
        };
        let expected_format = "State::Closed { err: Some(Reset) }";
        let actual_format = format!("{:?}", state);
        assert_eq!(actual_format, expected_format);
    }
}
