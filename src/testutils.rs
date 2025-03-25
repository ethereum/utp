use std::io;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::mpsc;

use crate::cid::ConnectionId;
use crate::peer::{ConnectionPeer, Peer};
use crate::udp::AsyncUdpSocket;

/// Decides whether the link between peers is up or down.
trait LinkDecider {
    /// Returns true if the link should send the packet, false otherwise.
    ///
    /// This must only be called once per packet, as it may have side-effects.
    fn should_send(&mut self) -> bool;
}

/// A mock socket that can be used to simulate a perfect link.
#[derive(Debug)]
pub struct MockUdpSocket<Link> {
    outbound: mpsc::UnboundedSender<Vec<u8>>,
    inbound: mpsc::UnboundedReceiver<Vec<u8>>,
    /// Peers identified by a letter
    pub only_peer: char,
    /// Defines whether the link is up. If not up, link will SILENTLY drop all sent packets.
    pub link: Link,
}

#[derive(Clone)]
pub struct ManualLinkDecider {
    pub up_switch: Arc<AtomicBool>,
}

impl ManualLinkDecider {
    fn new() -> Self {
        Self {
            up_switch: Arc::new(AtomicBool::new(true)),
        }
    }
}

impl LinkDecider for ManualLinkDecider {
    fn should_send(&mut self) -> bool {
        self.up_switch.load(Ordering::SeqCst)
    }
}

#[async_trait]
impl<Link: LinkDecider + std::marker::Sync + std::marker::Send> AsyncUdpSocket<char>
    for MockUdpSocket<Link>
{
    /// # Panics
    ///
    /// Panics if `target` is not equal to `self.only_peer`. This socket is built to support
    /// exactly two peers communicating with each other, so it will panic if used with more.
    async fn send_to(&mut self, buf: &[u8], peer: &Peer<char>) -> io::Result<usize> {
        if peer.id() != &self.only_peer {
            panic!("MockUdpSocket only supports sending to one peer");
        }
        if !self.link.should_send() {
            tracing::warn!("Dropping packet to {peer:?}: {buf:?}");
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
    async fn recv_from(&mut self, buf: &mut [u8]) -> io::Result<(usize, Peer<char>)> {
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
        Ok((packet_len, Peer::new(self.only_peer)))
    }
}

impl ConnectionPeer for char {
    type Id = char;

    fn id(&self) -> Self::Id {
        *self
    }

    fn consolidate(a: Self, b: Self) -> Self {
        assert!(a == b, "Consolidating non-equal peers");
        a
    }
}

fn build_link_pair<LinkAtoB, LinkBtoA>(
    a_to_b_link: LinkAtoB,
    b_to_a_link: LinkBtoA,
) -> (MockUdpSocket<LinkAtoB>, MockUdpSocket<LinkBtoA>) {
    let (peer_a, peer_b): (char, char) = ('A', 'B');
    let (a_tx, a_rx) = mpsc::unbounded_channel();
    let (b_tx, b_rx) = mpsc::unbounded_channel();
    let a = MockUdpSocket {
        outbound: a_tx,
        inbound: b_rx,
        only_peer: peer_b,
        link: a_to_b_link,
    };
    let b = MockUdpSocket {
        outbound: b_tx,
        inbound: a_rx,
        only_peer: peer_a,
        link: b_to_a_link,
    };
    (a, b)
}

fn build_connection_id_pair<LinkAtoB, LinkBtoA>(
    socket_a: &MockUdpSocket<LinkAtoB>,
    socket_b: &MockUdpSocket<LinkBtoA>,
) -> (ConnectionId<char>, ConnectionId<char>) {
    build_connection_id_pair_starting_at(socket_a, socket_b, 100)
}

fn build_connection_id_pair_starting_at<LinkAtoB, LinkBtoA>(
    socket_a: &MockUdpSocket<LinkAtoB>,
    socket_b: &MockUdpSocket<LinkBtoA>,
    lower_id: u16,
) -> (ConnectionId<char>, ConnectionId<char>) {
    let higher_id = lower_id.wrapping_add(1);
    let a_cid = ConnectionId {
        send: higher_id,
        recv: lower_id,
        peer_id: socket_a.only_peer,
    };
    let b_cid = ConnectionId {
        send: lower_id,
        recv: higher_id,
        peer_id: socket_b.only_peer,
    };
    (a_cid, b_cid)
}

/// Build a link between sockets, which we can manually control whether it is up or down
#[allow(clippy::type_complexity)]
pub fn build_manually_linked_pair() -> (
    (MockUdpSocket<ManualLinkDecider>, ConnectionId<char>),
    (MockUdpSocket<ManualLinkDecider>, ConnectionId<char>),
) {
    let (socket_a, socket_b) = build_link_pair(ManualLinkDecider::new(), ManualLinkDecider::new());
    let (a_cid, b_cid) = build_connection_id_pair(&socket_a, &socket_b);
    ((socket_a, a_cid), (socket_b, b_cid))
}
