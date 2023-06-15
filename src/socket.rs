use std::collections::HashMap;
use std::io;
use std::marker::Unpin;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};

use delay_map::HashMapDelay;
use futures::StreamExt;
use tokio::net::UdpSocket;
use tokio::sync::{mpsc, oneshot};
use tokio_util::time::DelayQueue;

use crate::cid::{ConnectionId, ConnectionIdGenerator, ConnectionPeer, StdConnectionIdGenerator};
use crate::conn::ConnectionConfig;
use crate::event::{SocketEvent, StreamEvent};
use crate::packet::{Packet, PacketType};
use crate::stream::UtpStream;
use crate::udp::AsyncUdpSocket;

const DEFAULT_ACCEPT_TIMEOUT: Duration = Duration::MAX;

type ConnChannel = mpsc::UnboundedSender<StreamEvent>;

struct Accept<P> {
    stream: oneshot::Sender<io::Result<UtpStream<P>>>,
    config: ConnectionConfig,
}

const MAX_UDP_PAYLOAD_SIZE: usize = u16::MAX as usize;

pub struct UtpSocket<P> {
    conns: Arc<RwLock<HashMap<ConnectionId<P>, ConnChannel>>>,
    cid_gen: Mutex<StdConnectionIdGenerator<P>>,
    accepts: mpsc::UnboundedSender<(Accept<P>, Option<ConnectionId<P>>, Instant)>,
    socket_events: mpsc::UnboundedSender<SocketEvent<P>>,
}

impl UtpSocket<SocketAddr> {
    pub async fn bind(addr: SocketAddr) -> io::Result<Self> {
        let socket = UdpSocket::bind(addr).await?;
        let socket = Self::with_socket(socket);
        Ok(socket)
    }
}

impl<P> UtpSocket<P>
where
    P: ConnectionPeer + Unpin + 'static,
{
    pub fn with_socket<S>(socket: S) -> Self
    where
        S: AsyncUdpSocket<P> + 'static,
    {
        let conns = HashMap::new();
        let conns = Arc::new(RwLock::new(conns));

        let cid_gen = Mutex::new(StdConnectionIdGenerator::new());

        let mut awaiting_cid: HashMapDelay<ConnectionId<P>, Accept<P>> =
            HashMapDelay::new(DEFAULT_ACCEPT_TIMEOUT);
        let mut awaiting: DelayQueue<Accept<P>> = DelayQueue::new();

        let mut incoming_conns = HashMap::new();

        let (socket_event_tx, mut socket_event_rx) = mpsc::unbounded_channel();
        let (accepts_tx, mut accepts_rx) = mpsc::unbounded_channel();

        let utp = Self {
            conns: Arc::clone(&conns),
            cid_gen,
            accepts: accepts_tx,
            socket_events: socket_event_tx.clone(),
        };

        let socket = Arc::new(socket);
        tokio::spawn(async move {
            let mut buf = [0; MAX_UDP_PAYLOAD_SIZE];
            loop {
                tokio::select! {
                    Ok((n, src)) = socket.recv_from(&mut buf) => {
                        let packet = match Packet::decode(&buf[..n]) {
                            Ok(pkt) => pkt,
                            Err(..) => {
                                tracing::warn!(?src, "unable to decode uTP packet");
                                continue;
                            }
                        };

                        let init_cid = cid_from_packet(&packet, &src, false);
                        let acc_cid = cid_from_packet(&packet, &src, true);
                        let mut conns = conns.write().unwrap();
                        let conn = conns
                            .get(&acc_cid)
                            .or_else(|| conns.get(&init_cid));
                        match conn {
                            Some(conn) => {
                                let _ = conn.send(StreamEvent::Incoming(packet));
                            }
                            None => {
                                if std::matches!(packet.packet_type(), PacketType::Syn) {
                                    let cid = cid_from_packet(&packet, &src, true);

                                    // First check whether there is a pending accept that specifies
                                    // `cid`. If no pending accept was found, then try to fulfill
                                    // the pending accept with the nearest timeout deadline.
                                    let accept = if let Some(accept) = awaiting_cid.remove(&cid) {
                                        Some(accept)
                                    } else {
                                        awaiting.peek().map(|key| awaiting.remove(&key).into_inner())
                                    };

                                    // If there was a suitable waiting accept, then create a new
                                    // stream for the connection. Otherwise, add the CID and SYN to
                                    // the incoming connections.
                                    match accept {
                                        Some(accept) => {
                                            let (connected_tx, connected_rx) = oneshot::channel();
                                            let (events_tx, events_rx) = mpsc::unbounded_channel();

                                            conns.insert(cid.clone(), events_tx);

                                            let stream = UtpStream::new(
                                                cid,
                                                accept.config,
                                                Some(packet),
                                                socket_event_tx.clone(),
                                                events_rx,
                                                connected_tx
                                            );

                                            tokio::spawn(async move {
                                                Self::await_connected(stream, accept, connected_rx).await
                                            });
                                        }
                                        None => {
                                            incoming_conns.insert(cid, packet);
                                        }
                                    }
                                } else {
                                    tracing::debug!(
                                        cid = %packet.conn_id(),
                                        packet = ?packet.packet_type(),
                                        seq = %packet.seq_num(),
                                        ack = %packet.ack_num(),
                                        "received uTP packet for non-existing conn"
                                    );
                                }
                            },
                        }
                    }
                    Some(event) = socket_event_rx.recv() => {
                        match event {
                            SocketEvent::Outgoing((packet, dst)) => {
                                let encoded = packet.encode();
                                if let Err(err) = socket.send_to(&encoded, &dst).await {
                                    tracing::debug!(
                                        %err,
                                        cid = %packet.conn_id(),
                                        packet = ?packet.packet_type(),
                                        seq = %packet.seq_num(),
                                        ack = %packet.ack_num(),
                                        "unable to send uTP packet over socket"
                                    );
                                }
                            }
                            SocketEvent::Shutdown(cid) => {
                                tracing::debug!(%cid.send, %cid.recv, "uTP conn shutdown");
                                conns.write().unwrap().remove(&cid);
                            }
                        }
                    }
                    Some((accept, cid, deadline)) = accepts_rx.recv() => {
                        // If the deadline has passed, then send the timeout to the acceptor.
                        let now = Instant::now();
                        if deadline < now {
                            // Drop the accept sender. A timeout error will be sent back to the
                            // caller.
                            tracing::warn!("accept timed out, dropping accept attempt");
                            continue;
                        }

                        // Compute the timeout duration. Given the check above, the subtraction
                        // cannot fail.
                        let timeout = deadline - now;

                        // If there are no incoming connections, then queue the accept.
                        if incoming_conns.is_empty() {
                            awaiting.insert(accept, timeout);
                            continue;
                        }

                        let (cid, syn) = match cid {
                            // If a CID was given, then check for an incoming connection with that
                            // CID. If one is found, then use that connection. Otherwise, add the
                            // CID to the awaiting connections.
                            Some(cid) => {
                                if let Some(syn) = incoming_conns.remove(&cid) {
                                    (cid, syn)
                                } else {
                                    awaiting_cid.insert_at(cid, accept, timeout);
                                    continue;
                                }
                            }
                            // If a CID was not given, then pull an incoming connection, and use
                            // that connection's CID. An incoming connection is known to exist
                            // because of the check above.
                            None => {
                                let cid = incoming_conns.keys().next().unwrap().clone();
                                let syn = incoming_conns.remove(&cid).unwrap();
                                (cid, syn)
                            }
                        };

                        let (connected_tx, connected_rx) = oneshot::channel();
                        let (events_tx, events_rx) = mpsc::unbounded_channel();

                        {
                            conns
                                .write()
                                .unwrap()
                                .insert(cid.clone(), events_tx);
                        }

                        let stream = UtpStream::new(
                            cid,
                            accept.config,
                            Some(syn),
                            socket_event_tx.clone(),
                            events_rx,
                            connected_tx,
                        );


                        tokio::spawn(async move {
                            Self::await_connected(stream, accept, connected_rx).await
                        });
                    }
                    Some(Ok(_accept)) = awaiting_cid.next() => {
                        // The accept timed out, so drop it.
                        continue
                    }
                    Some(_accept) = awaiting.next() => {
                        // The accept timed out, so drop it.
                        continue
                    }
                }
            }
        });

        utp
    }

    pub fn cid(&self, peer: P, is_initiator: bool) -> ConnectionId<P> {
        self.cid_gen.lock().unwrap().cid(peer, is_initiator)
    }

    pub async fn accept(&self, config: ConnectionConfig) -> io::Result<UtpStream<P>> {
        let (stream_tx, stream_rx) = oneshot::channel();
        let accept = Accept {
            stream: stream_tx,
            config,
        };

        let deadline = Instant::now() + accept.config.initial_timeout;
        self.accepts
            .send((accept, None, deadline))
            .map_err(|_| io::Error::from(io::ErrorKind::NotConnected))?;
        match stream_rx.await {
            Ok(stream) => Ok(stream?),
            Err(..) => Err(io::Error::from(io::ErrorKind::TimedOut)),
        }
    }

    pub async fn accept_with_cid(
        &self,
        cid: ConnectionId<P>,
        config: ConnectionConfig,
    ) -> io::Result<UtpStream<P>> {
        let (stream_tx, stream_rx) = oneshot::channel();
        let accept = Accept {
            stream: stream_tx,
            config,
        };

        let deadline = Instant::now() + accept.config.initial_timeout;
        self.accepts
            .send((accept, Some(cid), deadline))
            .map_err(|_| io::Error::from(io::ErrorKind::NotConnected))?;
        match stream_rx.await {
            Ok(stream) => Ok(stream?),
            Err(..) => Err(io::Error::from(io::ErrorKind::TimedOut)),
        }
    }

    pub async fn connect(&self, peer: P, config: ConnectionConfig) -> io::Result<UtpStream<P>> {
        let cid = self.cid_gen.lock().unwrap().cid(peer, true);
        let (connected_tx, connected_rx) = oneshot::channel();
        let (events_tx, events_rx) = mpsc::unbounded_channel();

        {
            self.conns.write().unwrap().insert(cid.clone(), events_tx);
        }

        let stream = UtpStream::new(
            cid,
            config,
            None,
            self.socket_events.clone(),
            events_rx,
            connected_tx,
        );

        match connected_rx.await {
            Ok(Ok(..)) => Ok(stream),
            Ok(Err(err)) => Err(err),
            Err(..) => Err(io::Error::from(io::ErrorKind::TimedOut)),
        }
    }

    pub async fn connect_with_cid(
        &self,
        cid: ConnectionId<P>,
        config: ConnectionConfig,
    ) -> io::Result<UtpStream<P>> {
        if self.conns.read().unwrap().contains_key(&cid) {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                "connection ID unavailable".to_string(),
            ));
        }

        let (connected_tx, connected_rx) = oneshot::channel();
        let (events_tx, events_rx) = mpsc::unbounded_channel();

        {
            self.conns.write().unwrap().insert(cid.clone(), events_tx);
        }

        let stream = UtpStream::new(
            cid,
            config,
            None,
            self.socket_events.clone(),
            events_rx,
            connected_tx,
        );

        match connected_rx.await {
            Ok(Ok(..)) => Ok(stream),
            Ok(Err(err)) => Err(err),
            Err(..) => Err(io::Error::from(io::ErrorKind::TimedOut)),
        }
    }

    async fn await_connected(
        stream: UtpStream<P>,
        accept: Accept<P>,
        connected: oneshot::Receiver<io::Result<()>>,
    ) {
        match connected.await {
            Ok(Ok(..)) => {
                let _ = accept.stream.send(Ok(stream));
            }
            Ok(Err(err)) => {
                let _ = accept.stream.send(Err(err));
            }
            Err(..) => {
                let _ = accept
                    .stream
                    .send(Err(io::Error::from(io::ErrorKind::ConnectionAborted)));
            }
        }
    }
}

fn cid_from_packet<P: ConnectionPeer>(
    packet: &Packet,
    src: &P,
    from_initiator: bool,
) -> ConnectionId<P> {
    if !from_initiator {
        let (send, recv) = (packet.conn_id().wrapping_add(1), packet.conn_id());
        ConnectionId {
            send,
            recv,
            peer: src.clone(),
        }
    } else {
        let (send, recv) = match packet.packet_type() {
            PacketType::Syn => (packet.conn_id(), packet.conn_id().wrapping_add(1)),
            PacketType::State | PacketType::Data | PacketType::Fin | PacketType::Reset => {
                (packet.conn_id().wrapping_sub(1), packet.conn_id())
            }
        };
        ConnectionId {
            send,
            recv,
            peer: src.clone(),
        }
    }
}

impl<P> Drop for UtpSocket<P> {
    fn drop(&mut self) {
        for conn in self.conns.read().unwrap().values() {
            let _ = conn.send(StreamEvent::Shutdown);
        }
    }
}
