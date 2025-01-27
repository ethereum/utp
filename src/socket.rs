use std::collections::HashMap;
use std::io;
use std::net::SocketAddr;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use delay_map::HashMapDelay;
use futures::StreamExt;
use rand::{thread_rng, Rng};
use tokio::net::UdpSocket;
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::{mpsc, oneshot};

use crate::cid::ConnectionId;
use crate::conn::ConnectionConfig;
use crate::event::{SocketEvent, StreamEvent};
use crate::packet::{Packet, PacketBuilder, PacketType};
use crate::peer::{ConnectionPeer, Peer};
use crate::stream::UtpStream;
use crate::udp::AsyncUdpSocket;

type ConnChannel = UnboundedSender<StreamEvent>;

struct Accept<P: ConnectionPeer> {
    stream: oneshot::Sender<io::Result<UtpStream<P>>>,
    config: ConnectionConfig,
}

struct AcceptWithCidPeer<P: ConnectionPeer> {
    cid: ConnectionId<P::Id>,
    peer: Peer<P>,
    accept: Accept<P>,
}

const MAX_UDP_PAYLOAD_SIZE: usize = u16::MAX as usize;
const CID_GENERATION_TRY_WARNING_COUNT: usize = 10;

/// accept_with_cid() has unique interactions compared to accept()
/// accept() pulls awaiting requests off a queue, but accept_with_cid() only
/// takes a connection off if CID matches. Because of this if we are awaiting a CID
/// eventually we need to timeout the await, or the queue would never stop growing with stale awaits
/// 20 seconds is arbitrary, after the uTP config refactor is done that can replace this constant.
/// but thee uTP config refactor is currently very low priority.
const AWAITING_CONNECTION_TIMEOUT: Duration = Duration::from_secs(20);

pub struct UtpSocket<P: ConnectionPeer> {
    conns: Arc<RwLock<HashMap<ConnectionId<P::Id>, ConnChannel>>>,
    accepts: UnboundedSender<Accept<P>>,
    accepts_with_cid: UnboundedSender<AcceptWithCidPeer<P>>,
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
    P: ConnectionPeer<Id: Unpin> + Unpin + 'static,
{
    pub fn with_socket<S>(mut socket: S) -> Self
    where
        S: AsyncUdpSocket<P> + 'static,
    {
        let conns = HashMap::new();
        let conns = Arc::new(RwLock::new(conns));

        let mut awaiting: HashMapDelay<ConnectionId<P::Id>, AcceptWithCidPeer<P>> =
            HashMapDelay::new(AWAITING_CONNECTION_TIMEOUT);

        let mut incoming_conns: HashMapDelay<ConnectionId<P::Id>, (Peer<P>, Packet)> =
            HashMapDelay::new(AWAITING_CONNECTION_TIMEOUT);

        let (socket_event_tx, mut socket_event_rx) = mpsc::unbounded_channel();
        let (accepts_tx, mut accepts_rx) = mpsc::unbounded_channel();
        let (accepts_with_cid_tx, mut accepts_with_cid_rx) = mpsc::unbounded_channel();

        let utp = Self {
            conns: Arc::clone(&conns),
            accepts: accepts_tx,
            accepts_with_cid: accepts_with_cid_tx,
            socket_events: socket_event_tx.clone(),
        };

        tokio::spawn(async move {
            let mut buf = [0; MAX_UDP_PAYLOAD_SIZE];
            loop {
                tokio::select! {
                    biased;
                    Ok((n, mut peer)) = socket.recv_from(&mut buf) => {
                        let peer_id = peer.id();
                        let packet = match Packet::decode(&buf[..n]) {
                            Ok(pkt) => pkt,
                            Err(..) => {
                                tracing::warn!(?peer_id, "unable to decode uTP packet");
                                continue;
                            }
                        };

                        let peer_init_cid = cid_from_packet::<P>(&packet, peer_id, IdType::SendIdPeerInitiated);
                        let we_init_cid = cid_from_packet::<P>(&packet, peer_id, IdType::SendIdWeInitiated);
                        let acc_cid = cid_from_packet::<P>(&packet, peer_id, IdType::RecvId);
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
                                    let cid = cid_from_packet::<P>(&packet, peer_id, IdType::RecvId);

                                    // If there was an awaiting connection with the CID, then
                                    // create a new stream for that connection. Otherwise, add the
                                    // connection to the incoming connections.
                                    if let Some(accept_with_cid) = awaiting.remove(&cid) {
                                        peer.merge(accept_with_cid.peer);

                                        let (connected_tx, connected_rx) = oneshot::channel();
                                        let (events_tx, events_rx) = mpsc::unbounded_channel();

                                        conns.insert(cid.clone(), events_tx);

                                        let stream = UtpStream::new(
                                            cid,
                                            peer,
                                            accept_with_cid.accept.config,
                                            Some(packet),
                                            socket_event_tx.clone(),
                                            events_rx,
                                            connected_tx
                                        );

                                        tokio::spawn(async move {
                                            Self::await_connected(stream, accept_with_cid.accept.stream, connected_rx).await
                                        });
                                    } else {
                                        incoming_conns.insert(cid, (peer, packet));
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
                                    // don't send a reset if we are receiving a reset
                                    if packet.packet_type() != PacketType::Reset {
                                        // if we get a packet from an unknown source send a reset packet.
                                        let random_seq_num = thread_rng().gen_range(0..=65535);
                                        let reset_packet =
                                            PacketBuilder::new(PacketType::Reset, packet.conn_id(), crate::time::now_micros(), 100_000, random_seq_num)
                                                .build();
                                        let event = SocketEvent::Outgoing((reset_packet, peer));
                                        if socket_event_tx.send(event).is_err() {
                                            tracing::warn!("Cannot transmit reset packet: socket closed channel");
                                            return;
                                        }
                                    }
                                }
                            },
                        }
                    }
                    Some(accept_with_cid) = accepts_with_cid_rx.recv() => {
                        let Some((mut peer, syn)) = incoming_conns.remove(&accept_with_cid.cid) else {
                            awaiting.insert(accept_with_cid.cid.clone(), accept_with_cid);
                            continue;
                        };
                        peer.merge(accept_with_cid.peer);
                        Self::select_accept_helper(accept_with_cid.cid, peer, syn, conns.clone(), accept_with_cid.accept, socket_event_tx.clone());
                    }
                    Some(accept) = accepts_rx.recv(), if !incoming_conns.is_empty() => {
                        let cid = incoming_conns.keys().next().expect("at least one incoming connection");
                        let cid = cid.clone();
                        let (peer, packet) = incoming_conns.remove(&cid).expect("to delete incoming connection");
                        Self::select_accept_helper(cid, peer, packet, conns.clone(), accept, socket_event_tx.clone());
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
                    Some(Ok((cid, accept_with_cid))) = awaiting.next() => {
                        // accept_with_cid didn't receive an inbound connection within the timeout period
                        // log it and return a timeout error
                        tracing::debug!(%cid.send, %cid.recv, "accept_with_cid timed out");
                        let _ = accept_with_cid.accept
                            .stream
                            .send(Err(io::Error::from(io::ErrorKind::TimedOut)));
                    }
                    Some(Ok((cid, _packet))) = incoming_conns.next() => {
                        // didn't handle inbound connection within the timeout period
                        // log it and return a timeout error
                        tracing::debug!(%cid.send, %cid.recv, "inbound connection timed out");
                    }
                }
            }
        });

        utp
    }

    /// Internal cid generation
    fn generate_cid(
        &self,
        peer_id: P::Id,
        is_initiator: bool,
        event_tx: Option<UnboundedSender<StreamEvent>>,
    ) -> ConnectionId<P::Id> {
        let mut cid = ConnectionId {
            send: 0,
            recv: 0,
            peer_id,
        };
        let mut generation_attempt_count = 0;
        loop {
            if generation_attempt_count > CID_GENERATION_TRY_WARNING_COUNT {
                tracing::error!("cid() tried to generate a cid {generation_attempt_count} times")
            }
            let recv: u16 = rand::random();
            let send = if is_initiator {
                recv.wrapping_add(1)
            } else {
                recv.wrapping_sub(1)
            };
            cid.send = send;
            cid.recv = recv;

            if !self.conns.read().unwrap().contains_key(&cid) {
                if let Some(event_tx) = event_tx {
                    self.conns.write().unwrap().insert(cid.clone(), event_tx);
                }
                return cid;
            }
            generation_attempt_count += 1;
        }
    }

    pub fn cid(&self, peer_id: P::Id, is_initiator: bool) -> ConnectionId<P::Id> {
        self.generate_cid(peer_id, is_initiator, None)
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
        cid: ConnectionId<P::Id>,
        peer: Peer<P>,
        config: ConnectionConfig,
    ) -> io::Result<UtpStream<P>> {
        let (stream_tx, stream_rx) = oneshot::channel();
        let accept = AcceptWithCidPeer {
            cid,
            peer,
            accept: Accept {
                stream: stream_tx,
                config,
            },
        };
        self.accepts_with_cid
            .send(accept)
            .map_err(|_| io::Error::from(io::ErrorKind::NotConnected))?;
        match stream_rx.await {
            Ok(stream) => Ok(stream?),
            Err(..) => Err(io::Error::from(io::ErrorKind::TimedOut)),
        }
    }

    pub async fn connect(
        &self,
        peer: Peer<P>,
        config: ConnectionConfig,
    ) -> io::Result<UtpStream<P>> {
        let (connected_tx, connected_rx) = oneshot::channel();
        let (events_tx, events_rx) = mpsc::unbounded_channel();
        let cid = self.generate_cid(peer.id().clone(), true, Some(events_tx));

        let stream = UtpStream::new(
            cid,
            peer,
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
        cid: ConnectionId<P::Id>,
        peer: Peer<P>,
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
            peer,
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
        callback: oneshot::Sender<io::Result<UtpStream<P>>>,
        connected: oneshot::Receiver<io::Result<()>>,
    ) {
        match connected.await {
            Ok(Ok(..)) => {
                let _ = callback.send(Ok(stream));
            }
            Ok(Err(err)) => {
                let _ = callback.send(Err(err));
            }
            Err(..) => {
                let _ = callback.send(Err(io::Error::from(io::ErrorKind::ConnectionAborted)));
            }
        }
    }

    fn select_accept_helper(
        cid: ConnectionId<P::Id>,
        peer: Peer<P>,
        syn: Packet,
        conns: Arc<RwLock<HashMap<ConnectionId<P::Id>, ConnChannel>>>,
        accept: Accept<P>,
        socket_event_tx: UnboundedSender<SocketEvent<P>>,
    ) {
        if conns.read().unwrap().contains_key(&cid) {
            let _ = accept.stream.send(Err(io::Error::new(
                io::ErrorKind::Other,
                "connection ID unavailable".to_string(),
            )));
            return;
        }

        let (connected_tx, connected_rx) = oneshot::channel();
        let (events_tx, events_rx) = mpsc::unbounded_channel();

        {
            conns.write().unwrap().insert(cid.clone(), events_tx);
        }

        let stream = UtpStream::new(
            cid,
            peer,
            accept.config,
            Some(syn),
            socket_event_tx,
            events_rx,
            connected_tx,
        );

        tokio::spawn(
            async move { Self::await_connected(stream, accept.stream, connected_rx).await },
        );
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
    peer_id: &P::Id,
    id_type: IdType,
) -> ConnectionId<P::Id> {
    let peer_id = peer_id.clone();
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
                peer_id,
            }
        }
        IdType::SendIdWeInitiated => {
            let (send, recv) = (packet.conn_id().wrapping_add(1), packet.conn_id());
            ConnectionId {
                send,
                recv,
                peer_id,
            }
        }
        IdType::SendIdPeerInitiated => {
            let (send, recv) = (packet.conn_id(), packet.conn_id().wrapping_sub(1));
            ConnectionId {
                send,
                recv,
                peer_id,
            }
        }
    }
}

impl<P: ConnectionPeer> Drop for UtpSocket<P> {
    fn drop(&mut self) {
        for conn in self.conns.read().unwrap().values() {
            let _ = conn.send(StreamEvent::Shutdown);
        }
    }
}
