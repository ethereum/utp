use std::collections::{HashMap, VecDeque};
use std::io;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex, RwLock};

use tokio::net::UdpSocket;
use tokio::sync::{mpsc, oneshot};

use crate::cid::{ConnectionId, ConnectionIdGenerator, StdConnectionIdGenerator};
use crate::event::StreamEvent;
use crate::packet::{Packet, PacketType};
use crate::stream::UtpStream;

type ConnChannel = mpsc::UnboundedSender<StreamEvent>;

const MAX_UDP_PAYLOAD_SIZE: usize = u16::MAX as usize;

pub struct UtpSocket {
    conns: Arc<RwLock<HashMap<ConnectionId, ConnChannel>>>,
    cid_gen: Mutex<StdConnectionIdGenerator>,
    accepts: mpsc::UnboundedSender<oneshot::Sender<io::Result<UtpStream>>>,
    outgoing: mpsc::UnboundedSender<(Packet, SocketAddr)>,
}

impl UtpSocket {
    pub async fn bind(addr: SocketAddr) -> io::Result<Self> {
        let udp = UdpSocket::bind(addr).await?;

        let conns = HashMap::new();
        let conns = Arc::new(RwLock::new(conns));

        let cid_gen = Mutex::new(StdConnectionIdGenerator::default());

        let mut incoming_conns = VecDeque::new();

        let (outgoing_tx, mut outgoing_rx) = mpsc::unbounded_channel();
        let (accepts_tx, mut accepts_rx) = mpsc::unbounded_channel();

        let socket = Self {
            conns: Arc::clone(&conns),
            cid_gen,
            accepts: accepts_tx,
            outgoing: outgoing_tx.clone(),
        };

        tokio::spawn(async move {
            let mut buf = [0; MAX_UDP_PAYLOAD_SIZE];
            loop {
                tokio::select! {
                    Ok((n, src)) = udp.recv_from(&mut buf) => {
                        let packet = match Packet::decode(&buf[..n]) {
                            Ok(pkt) => pkt,
                            Err(..) => {
                                continue;
                            }
                        };

                        let init_cid = cid_from_packet(&packet, src, true);
                        let acc_cid = cid_from_packet(&packet, src, false);
                        let conns = conns.write().unwrap();
                        let conn = conns
                            .get(&acc_cid)
                            .or_else(|| conns.get(&init_cid));
                        match conn {
                            Some(conn) => {
                                let _ = conn.send(StreamEvent::Incoming(packet));
                            }
                            None => {
                                if std::matches!(packet.packet_type(), PacketType::Syn) {
                                    incoming_conns.push_back((packet, src));
                                }
                            },
                        }
                        std::mem::drop(conns);
                    }
                    Some((outgoing, dst)) = outgoing_rx.recv() => {
                        let encoded = outgoing.encode();
                        let _ = udp.send_to(&encoded, dst).await;
                    }
                    Some(accept) = accepts_rx.recv(), if !incoming_conns.is_empty() => {
                        let (syn, src) = incoming_conns.pop_front().unwrap();

                        let cid = cid_from_packet(&syn, src, false);
                        let (connected_tx, connected_rx) = oneshot::channel();
                        let (incoming_tx, incoming_rx) = mpsc::unbounded_channel();

                        {
                            conns
                                .write()
                                .unwrap()
                                .insert(cid, incoming_tx);
                        }

                        let stream = UtpStream::new(
                            cid,
                            Some(syn),
                            outgoing_tx.clone(),
                            incoming_rx,
                            connected_tx,
                        );

                        tokio::spawn(async move {
                            match connected_rx.await {
                                Ok(Ok(..)) => {
                                    let _ = accept.send(Ok(stream));
                                }
                                Ok(Err(err)) => {
                                    let _ = accept.send(Err(err));
                                }
                                Err(..) => {
                                    let _ = accept.send(Err(io::Error::from(io::ErrorKind::ConnectionAborted)));
                                }
                            }
                        });
                    }
                }
            }
        });

        Ok(socket)
    }

    pub async fn accept(&self) -> io::Result<UtpStream> {
        let (stream_tx, stream_rx) = oneshot::channel();
        self.accepts
            .send(stream_tx)
            .map_err(|_| io::Error::from(io::ErrorKind::NotConnected))?;
        match stream_rx.await {
            Ok(stream) => Ok(stream?),
            Err(..) => Err(io::Error::from(io::ErrorKind::TimedOut)),
        }
    }

    pub async fn connect(&self, addr: SocketAddr) -> io::Result<UtpStream> {
        let cid = self.cid_gen.lock().unwrap().cid(addr);
        let (connected_tx, connected_rx) = oneshot::channel();
        let (incoming_tx, incoming_rx) = mpsc::unbounded_channel();

        {
            self.conns.write().unwrap().insert(cid, incoming_tx);
        }

        let stream = UtpStream::new(cid, None, self.outgoing.clone(), incoming_rx, connected_tx);

        match connected_rx.await {
            Ok(..) => Ok(stream),
            Err(..) => Err(io::Error::from(io::ErrorKind::TimedOut)),
        }
    }
}

fn cid_from_packet(packet: &Packet, src: SocketAddr, local_initiator: bool) -> ConnectionId {
    if local_initiator {
        let (send, recv) = (packet.conn_id().wrapping_add(1), packet.conn_id());
        ConnectionId {
            send,
            recv,
            peer: src,
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
            peer: src,
        }
    }
}

impl Drop for UtpSocket {
    fn drop(&mut self) {
        for conn in self.conns.read().unwrap().values() {
            let _ = conn.send(StreamEvent::Shutdown);
        }
    }
}
