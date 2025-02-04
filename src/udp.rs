use std::io;
use std::net::SocketAddr;

use async_trait::async_trait;
use tokio::net::UdpSocket;

use crate::peer::{ConnectionPeer, Peer};

/// An abstract representation of an asynchronous UDP socket.
#[async_trait]
pub trait AsyncUdpSocket<P: ConnectionPeer>: Send + Sync {
    /// Attempts to send data on the socket to a given peer.
    /// Note that this should return nearly immediately, rather than awaiting something internally.
    async fn send_to(&mut self, buf: &[u8], peer: &Peer<P>) -> io::Result<usize>;
    /// Attempts to receive a single datagram on the socket.
    async fn recv_from(&mut self, buf: &mut [u8]) -> io::Result<(usize, Peer<P>)>;
}

#[async_trait]
impl AsyncUdpSocket<SocketAddr> for UdpSocket {
    async fn send_to(&mut self, buf: &[u8], peer: &Peer<SocketAddr>) -> io::Result<usize> {
        UdpSocket::send_to(self, buf, peer.id()).await
    }

    async fn recv_from(&mut self, buf: &mut [u8]) -> io::Result<(usize, Peer<SocketAddr>)> {
        UdpSocket::recv_from(self, buf)
            .await
            .map(|(len, peer)| (len, Peer::new(peer)))
    }
}
