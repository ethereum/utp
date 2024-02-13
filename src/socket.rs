use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::io;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;

use delay_map::HashSetDelay;
use futures::StreamExt;
use std::hash::{Hash, Hasher};
use tokio::net::UdpSocket;
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::{mpsc, oneshot};

use crate::cid::{ConnectionId, ConnectionIdGenerator, ConnectionPeer, StdConnectionIdGenerator};
use crate::conn::ConnectionConfig;
use crate::event::{SocketEvent, StreamEvent};
use crate::packet::{Packet, PacketType};
use crate::stream::UtpStream;
use crate::udp::AsyncUdpSocket;

type ConnChannel = UnboundedSender<StreamEvent>;

struct Accept<P> {
    stream: oneshot::Sender<io::Result<UtpStream<P>>>,
    config: ConnectionConfig,
}

const MAX_UDP_PAYLOAD_SIZE: usize = u16::MAX as usize;

/// accept_with_cid() has unique interactions compared to accept()
/// accept() pulls awaiting requests off a queue, but accept_with_cid() only
/// takes a connection off if CID matches. Because of this if we are awaiting a CID
/// eventually we need to timeout the await, or the queue would never stop growing with stale awaits
/// 20 seconds is arbatrary, after the uTP cofig refactor is done that can replace this constant.
/// but thee uTP config refactor is currently very low priority.
const AWAITING_CONNECTION_TIMEOUT: Duration = Duration::from_secs(20);

pub struct UtpSocket<P> {
    conns: Arc<RwLock<HashMap<ConnectionId<P>, ConnChannel>>>,
    cid_gen: Mutex<StdConnectionIdGenerator<P>>,
    accepts: UnboundedSender<Accept<P>>,
    accepts_with_cid: UnboundedSender<(Accept<P>, ConnectionId<P>)>,
    socket_events: UnboundedSender<SocketEvent<P>>,
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
    P: ConnectionPeer + 'static,
{
    pub fn with_socket<S>(mut socket: S) -> Self
    where
        S: AsyncUdpSocket<P> + 'static,
    {
        let conns = HashMap::new();
        let conns = Arc::new(RwLock::new(conns));

        let cid_gen = Mutex::new(StdConnectionIdGenerator::new());

        // if an accept_with_cid awaiting connection isn't connected in AWAITING_CONNECTION_TIMEOUT seconds, cancel and log it
        let mut awaiting: HashMap<u64, (ConnectionId<P>, Accept<P>)> = HashMap::new();
        let mut awaiting_expirations: HashSetDelay<u64> =
            HashSetDelay::new(AWAITING_CONNECTION_TIMEOUT);

        let mut incoming_conns: HashMap<u64, (ConnectionId<P>, Packet)> = HashMap::new();
        let mut incoming_conns_expirations: HashSetDelay<u64> =
            HashSetDelay::new(AWAITING_CONNECTION_TIMEOUT);

        let (socket_event_tx, mut socket_event_rx) = mpsc::unbounded_channel();
        let (accepts_tx, mut accepts_rx) = mpsc::unbounded_channel();
        let (accepts_with_cid_tx, mut accepts_with_cid_rx) = mpsc::unbounded_channel();

        let utp = Self {
            conns: Arc::clone(&conns),
            cid_gen,
            accepts: accepts_tx,
            accepts_with_cid: accepts_with_cid_tx,
            socket_events: socket_event_tx.clone(),
        };

        tokio::spawn(async move {
            let mut buf = [0; MAX_UDP_PAYLOAD_SIZE];
            loop {
                tokio::select! {
                    biased;
                    Ok((n, src)) = socket.recv_from(&mut buf) => {
                        let packet = match Packet::decode(&buf[..n]) {
                            Ok(pkt) => pkt,
                            Err(..) => {
                                tracing::warn!(?src, "unable to decode uTP packet");
                                continue;
                            }
                        };

                        let peer_init_cid = cid_from_packet(&packet, &src, IdType::SendIdPeerInitiated);
                        let we_init_cid = cid_from_packet(&packet, &src, IdType::SendIdWeInitiated);
                        let acc_cid = cid_from_packet(&packet, &src, IdType::RecvId);
                        let mut conns = conns.write().unwrap();
                        let conn = conns
                            .get(&acc_cid)
                            .or_else(|| conns.get(&we_init_cid))
                            .or_else(|| conns.get(&peer_init_cid));
                        match conn {
                            Some(conn) => {
                                let _ = conn.send(StreamEvent::Incoming(packet));
                            }
                            None => {
                                if std::matches!(packet.packet_type(), PacketType::Syn) {
                                    let cid = cid_from_packet(&packet, &src, IdType::RecvId);

                                    // If there was an awaiting connection with the CID, then
                                    // create a new stream for that connection. Otherwise, add the
                                    // connection to the incoming connections.
                                    let cid_hash = calculate_hash(&cid);
                                    if let Some((_, accept)) = awaiting.remove(&cid_hash) {
                                        awaiting_expirations.remove(&cid_hash);
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
                                    } else {
                                        let incoming_conns_key = calculate_hash(&cid);
                                        incoming_conns.insert(incoming_conns_key, (cid, packet));
                                        incoming_conns_expirations.insert(incoming_conns_key);
                                    }
                                } else {
                                    tracing::debug!(
                                        cid = %packet.conn_id(),
                                        packet = ?packet.packet_type(),
                                        seq = %packet.seq_num(),
                                        ack = %packet.ack_num(),
                                        peer_init_cid = ?peer_init_cid,
                                        we_init_cid = ?we_init_cid,
                                        acc_cid = ?acc_cid,
                                        "received uTP packet for non-existing conn"
                                    );
                                }
                            },
                        }
                    }
                    Some((accept, cid)) = accepts_with_cid_rx.recv() => {
                        let incoming_conns_key = calculate_hash(&cid);
                        let (cid, syn) = if let Some((_, syn)) = incoming_conns.remove(&incoming_conns_key) {
                            incoming_conns_expirations.remove(&incoming_conns_key);
                            (cid, syn)
                        } else {
                            let cid_hash = calculate_hash(&cid);
                            awaiting.insert(cid_hash, (cid, accept));
                            awaiting_expirations.insert(cid_hash);
                            continue;
                        };
                        Self::select_accept_helper(cid, syn, conns.clone(), accept, socket_event_tx.clone());
                    }
                    Some(accept) = accepts_rx.recv(), if !incoming_conns.is_empty() => {
                        let incoming_conns_key = *incoming_conns.keys().next().unwrap();
                        let (cid, syn) = incoming_conns.remove(&incoming_conns_key).unwrap();
                        incoming_conns_expirations.remove(&incoming_conns_key);
                        Self::select_accept_helper(cid, syn, conns.clone(), accept, socket_event_tx.clone());
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
                    Some(Ok(awaiting_key)) = awaiting_expirations.next() => {
                        // accept_with_cid didn't recieve an inbound connection within the timeout period
                        // log it and return a timeout error
                        if let Some((cid, accept)) = awaiting.remove(&calculate_hash(&awaiting_key)) {
                            tracing::debug!(%cid.send, %cid.recv, "accept_with_cid timed out");
                            let _ = accept
                                .stream
                                .send(Err(io::Error::from(io::ErrorKind::TimedOut)));
                        } else {
                            unreachable!("Error awaiting_expirations should always contain valid awaiting items")
                        }
                    }
                    Some(Ok(incoming_conns_key)) = incoming_conns_expirations.next() => {
                        // didn't handle inbound connection within the timeout period
                        // log it and return a timeout error
                        if let Some((cid, _)) = incoming_conns.remove(&calculate_hash(&incoming_conns_key)) {
                            tracing::debug!(%cid.send, %cid.recv, "inbound connection timed out");
                        } else {
                            unreachable!("Error incoming_conns_expirations should always contain valid incoming_conns items")
                        }
                    }
                }
            }
        });

        utp
    }

    pub fn cid(&self, peer: P, is_initiator: bool) -> ConnectionId<P> {
        self.cid_gen.lock().unwrap().cid(peer, is_initiator)
    }

    /// Returns the number of connections currently open, both inbound and outbound.
    pub fn num_connections(&self) -> usize {
        self.conns.read().unwrap().len()
    }

    /// WARNING: only accept() or accept_with_cid() can be used in an application.
    /// they aren't compatible to use interchangeably in a program
    pub async fn accept(&self, config: ConnectionConfig) -> io::Result<UtpStream<P>> {
        let (stream_tx, stream_rx) = oneshot::channel();
        let accept = Accept {
            stream: stream_tx,
            config,
        };
        self.accepts
            .send(accept)
            .map_err(|_| io::Error::from(io::ErrorKind::NotConnected))?;
        match stream_rx.await {
            Ok(stream) => Ok(stream?),
            Err(..) => Err(io::Error::from(io::ErrorKind::TimedOut)),
        }
    }

    /// WARNING: only accept() or accept_with_cid() can be used in an application.
    /// they aren't compatible to use interchangeably in a program
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
        self.accepts_with_cid
            .send((accept, cid))
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
            cid.clone(),
            config,
            None,
            self.socket_events.clone(),
            events_rx,
            connected_tx,
        );

        match connected_rx.await {
            Ok(Ok(..)) => Ok(stream),
            Ok(Err(err)) => {
                tracing::error!(%err, "failed to open connection with {cid:?}");
                Err(err)
            }
            Err(_) => {
                tracing::error!("failed to open connection with {cid:?}");
                Err(io::Error::from(io::ErrorKind::TimedOut))
            }
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

    fn select_accept_helper(
        cid: ConnectionId<P>,
        syn: Packet,
        conns: Arc<RwLock<HashMap<ConnectionId<P>, UnboundedSender<StreamEvent>>>>,
        accept: Accept<P>,
        socket_event_tx: UnboundedSender<SocketEvent<P>>,
    ) {
        let (connected_tx, connected_rx) = oneshot::channel();
        let (events_tx, events_rx) = mpsc::unbounded_channel();

        {
            conns.write().unwrap().insert(cid.clone(), events_tx);
        }

        let stream = UtpStream::new(
            cid,
            accept.config,
            Some(syn),
            socket_event_tx,
            events_rx,
            connected_tx,
        );

        tokio::spawn(async move { Self::await_connected(stream, accept, connected_rx).await });
    }
}

#[derive(Copy, Clone, Debug)]
enum IdType {
    RecvId,
    SendIdWeInitiated,
    SendIdPeerInitiated,
}

fn cid_from_packet<P: ConnectionPeer>(
    packet: &Packet,
    src: &P,
    id_type: IdType,
) -> ConnectionId<P> {
    match id_type {
        IdType::RecvId => {
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
        IdType::SendIdWeInitiated => {
            let (send, recv) = (packet.conn_id().wrapping_add(1), packet.conn_id());
            ConnectionId {
                send,
                recv,
                peer: src.clone(),
            }
        }
        IdType::SendIdPeerInitiated => {
            let (send, recv) = (packet.conn_id(), packet.conn_id().wrapping_sub(1));
            ConnectionId {
                send,
                recv,
                peer: src.clone(),
            }
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

fn calculate_hash<T: Hash>(t: &T) -> u64 {
    let mut s = DefaultHasher::new();
    t.hash(&mut s);
    s.finish()
}
