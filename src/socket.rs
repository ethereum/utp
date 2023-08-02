use std::collections::HashMap;
use std::io;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex, RwLock};

use tokio::net::UdpSocket;
use tokio::sync::{mpsc, oneshot};

use crate::cid::{ConnectionId, ConnectionIdGenerator, ConnectionPeer, StdConnectionIdGenerator};
use crate::conn::ConnectionConfig;
use crate::event::{SocketEvent, StreamEvent};
use crate::packet::{Packet, PacketType};
use crate::stream::UtpStream;
use crate::udp::AsyncUdpSocket;

type ConnChannel = mpsc::UnboundedSender<StreamEvent>;

struct Accept<P> {
    stream: oneshot::Sender<io::Result<UtpStream<P>>>,
    config: ConnectionConfig,
}

const MAX_UDP_PAYLOAD_SIZE: usize = u16::MAX as usize;

pub struct UtpSocket<P> {
    conns: Arc<RwLock<HashMap<ConnectionId<P>, ConnChannel>>>,
    cid_gen: Mutex<StdConnectionIdGenerator<P>>,
    accepts: mpsc::UnboundedSender<(Accept<P>, Option<ConnectionId<P>>)>,
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
    P: ConnectionPeer + 'static,
{
    pub fn with_socket<S>(socket: S) -> Self
    where
        S: AsyncUdpSocket<P> + 'static,
    {
        let conns = HashMap::new();
        let conns = Arc::new(RwLock::new(conns));

        let cid_gen = Mutex::new(StdConnectionIdGenerator::new());

        let awaiting: HashMap<ConnectionId<P>, Accept<P>> = HashMap::new();
        let awaiting = Arc::new(RwLock::new(awaiting));

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
                    biased;
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
                                    let mut awaiting = awaiting.write().unwrap();

                                    // If there was an awaiting connection with the CID, then
                                    // create a new stream for that connection. Otherwise, add the
                                    // connection to the incoming connections.
                                    if let Some(accept) = awaiting.remove(&cid) {
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
                                        incoming_conns.insert(cid, packet);
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
                    Some((accept, cid)) = accepts_rx.recv(), if !incoming_conns.is_empty() => {
                        let (cid, syn) = match cid {
                            // If a CID was given, then check for an incoming connection with that
                            // CID. If one is found, then use that connection. Otherwise, add the
                            // CID to the awaiting connections.
                            Some(cid) => {
                                if let Some(syn) = incoming_conns.remove(&cid) {
                                    (cid, syn)
                                } else {
                                    awaiting.write().unwrap().insert(cid, accept);
                                    continue;
                                }
                            }
                            // If a CID was not given, then pull an incoming connection, and use
                            // that connection's CID. An incoming connection is known to exist
                            // because of the condition in the `select` arm.
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

    pub async fn accept(&self, config: ConnectionConfig) -> io::Result<UtpStream<P>> {
        let (stream_tx, stream_rx) = oneshot::channel();
        let accept = Accept {
            stream: stream_tx,
            config,
        };
        self.accepts
            .send((accept, None))
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
        self.accepts
            .send((accept, Some(cid)))
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
