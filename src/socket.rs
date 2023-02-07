use std::collections::{HashMap, VecDeque};
use std::io;
use std::net::SocketAddr;
use std::sync::{Arc, RwLock};

use tokio::net::UdpSocket;
use tokio::sync::{mpsc, oneshot};

use crate::packet::{Packet, PacketType};
use crate::stream::UtpStream;

type ConnKey = (SocketAddr, u16);
type ConnChannel = mpsc::UnboundedSender<Packet>;

pub struct UtpSocket {
    conns: Arc<RwLock<HashMap<ConnKey, ConnChannel>>>,
    accepts: mpsc::UnboundedSender<oneshot::Sender<io::Result<UtpStream>>>,
    outgoing: mpsc::UnboundedSender<(Packet, SocketAddr)>,
}

impl UtpSocket {
    pub async fn bind(addr: SocketAddr) -> io::Result<Self> {
        let udp = UdpSocket::bind(addr).await?;

        let conns = HashMap::new();
        let conns = Arc::new(RwLock::new(conns));

        let mut incoming_conns = VecDeque::new();

        let (outgoing_tx, mut outgoing_rx) = mpsc::unbounded_channel();
        let (accepts_tx, mut accepts_rx) = mpsc::unbounded_channel();

        let socket = Self {
            conns: Arc::clone(&conns),
            accepts: accepts_tx,
            outgoing: outgoing_tx.clone(),
        };

        tokio::spawn(async move {
            let mut buf = [0; u16::MAX as usize];
            loop {
                tokio::select! {
                    Ok((n, src)) = udp.recv_from(&mut buf) => {
                        let packet = match Packet::decode(&buf[..n]) {
                            Ok(pkt) => pkt,
                            Err(..) => {
                                continue;
                            }
                        };
                        let conn_id = match packet.packet_type() {
                            PacketType::Syn => packet.conn_id(),
                            PacketType::State
                            | PacketType::Data
                            | PacketType::Fin
                            | PacketType::Reset => packet.conn_id().wrapping_sub(1),
                        };
                        let alt_conn_id = conn_id.wrapping_add(1);

                        let conns = conns.write().unwrap();
                        let conn = conns
                            .get(&(src, conn_id))
                            .or_else(|| conns.get(&(src, alt_conn_id)));
                        match conn {
                            Some(conn) => {
                                let _ = conn.send(packet);
                            }
                            None => match packet.packet_type() {
                                PacketType::Syn => {
                                    incoming_conns.push_back((packet, src));
                                }
                                PacketType::State
                                | PacketType::Data
                                | PacketType::Fin
                                | PacketType::Reset => {}
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

                        let conn_id = syn.conn_id().wrapping_add(1);
                        let (connected_tx, connected_rx) = oneshot::channel();
                        let (incoming_tx, incoming_rx) = mpsc::unbounded_channel();

                        {
                            conns
                                .write()
                                .unwrap()
                                .insert((src, conn_id), incoming_tx);
                        }

                        let stream = UtpStream::new(
                            src,
                            conn_id,
                            Some(syn),
                            outgoing_tx.clone(),
                            incoming_rx,
                            connected_tx,
                        );

                        tokio::spawn(async move {
                            match connected_rx.await {
                                Ok(..) => {
                                    let _ = accept.send(Ok(stream));
                                }
                                Err(..) => {
                                    let err = Err(io::Error::from(io::ErrorKind::TimedOut));
                                    let _ = accept.send(err);
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
        let mut conn_id: u16 = rand::random();
        while self.conns.read().unwrap().contains_key(&(addr, conn_id)) {
            conn_id = rand::random();
        }

        let (connected_tx, connected_rx) = oneshot::channel();
        let (incoming_tx, incoming_rx) = mpsc::unbounded_channel();

        {
            self.conns
                .write()
                .unwrap()
                .insert((addr, conn_id), incoming_tx);
        }

        let stream = UtpStream::new(
            addr,
            conn_id,
            None,
            self.outgoing.clone(),
            incoming_rx,
            connected_tx,
        );

        match connected_rx.await {
            Ok(..) => Ok(stream),
            Err(..) => Err(io::Error::from(io::ErrorKind::TimedOut)),
        }
    }
}

impl Drop for UtpSocket {
    fn drop(&mut self) {}
}
