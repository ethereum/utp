use std::io;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::mpsc;

use crate::cid::{ConnectionId, ConnectionPeer};
use crate::udp::AsyncUdpSocket;

/// A mock socket that can be used to simulate a perfect link.
#[derive(Debug)]
pub struct MockUdpSocket {
    outbound: mpsc::UnboundedSender<Vec<u8>>,
    inbound: mpsc::UnboundedReceiver<Vec<u8>>,
    /// Peers identified by a letter
    pub only_peer: char,
    /// Defines whether the link is up. If not up, link will SILENTLY drop all sent packets.
    up_status: Arc<AtomicBool>,
}

impl MockUdpSocket {
    /// Get a network status controller for this socket.
    /// In order to simulate a down link, store a false value into the status.
    pub fn up_status(&mut self) -> Arc<AtomicBool> {
        self.up_status.clone()
    }

    /// Should the link send packets?
    fn is_up(&self) -> bool {
        self.up_status.load(Ordering::SeqCst)
    }
}

#[async_trait]
impl AsyncUdpSocket<char> for MockUdpSocket {
    /// # Panics
    ///
    /// Panics if `target` is not equal to `self.only_peer`. This socket is built to support
    /// exactly two peers communicating with each other, so it will panic if used with more.
    async fn send_to(&mut self, buf: &[u8], target: &char) -> io::Result<usize> {
        if target != &self.only_peer {
            panic!("MockUdpSocket only supports sending to one peer");
        }
        if !self.is_up() {
            return Ok(buf.len());
        }
        if let Err(err) = self.outbound.send(buf.to_vec()) {
            Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                format!("channel closed: {err}"),
            ))
        } else {
            Ok(buf.len())
        }
    }

    /// # Panics
    ///
    /// Panics if `buf` is smaller than the packet size.
    async fn recv_from(&mut self, buf: &mut [u8]) -> io::Result<(usize, char)> {
        let packet = self
            .inbound
            .recv()
            .await
            .ok_or_else(|| io::Error::new(io::ErrorKind::UnexpectedEof, "channel closed"))?;
        if buf.len() < packet.len() {
            panic!("buffer too small for perfect link");
        }
        let packet_len = packet.len();
        buf[..packet_len].copy_from_slice(&packet[..]);
        Ok((packet_len, self.only_peer))
    }
}

impl ConnectionPeer for char {}

fn build_link_pair() -> (MockUdpSocket, MockUdpSocket) {
    let (peer_a, peer_b): (char, char) = ('A', 'B');
    let (a_tx, a_rx) = mpsc::unbounded_channel();
    let (b_tx, b_rx) = mpsc::unbounded_channel();
    let a = MockUdpSocket {
        outbound: a_tx,
        inbound: b_rx,
        only_peer: peer_b,
        up_status: Arc::new(AtomicBool::new(true)),
    };
    let b = MockUdpSocket {
        outbound: b_tx,
        inbound: a_rx,
        only_peer: peer_a,
        up_status: Arc::new(AtomicBool::new(true)),
    };
    (a, b)
}

fn build_connection_id_pair(
    socket_a: &MockUdpSocket,
    socket_b: &MockUdpSocket,
) -> (ConnectionId<char>, ConnectionId<char>) {
    build_connection_id_pair_starting_at(socket_a, socket_b, 100)
}

fn build_connection_id_pair_starting_at(
    socket_a: &MockUdpSocket,
    socket_b: &MockUdpSocket,
    lower_id: u16,
) -> (ConnectionId<char>, ConnectionId<char>) {
    let higher_id = lower_id.wrapping_add(1);
    let a_cid = ConnectionId {
        send: higher_id,
        recv: lower_id,
        peer: socket_a.only_peer,
    };
    let b_cid = ConnectionId {
        send: lower_id,
        recv: higher_id,
        peer: socket_b.only_peer,
    };
    (a_cid, b_cid)
}

pub fn build_connected_pair() -> (
    (MockUdpSocket, ConnectionId<char>),
    (MockUdpSocket, ConnectionId<char>),
) {
    let (socket_a, socket_b) = build_link_pair();
    let (a_cid, b_cid) = build_connection_id_pair(&socket_a, &socket_b);
    ((socket_a, a_cid), (socket_b, b_cid))
}
