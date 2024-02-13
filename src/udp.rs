use std::io;
use std::net::SocketAddr;

use async_trait::async_trait;
use tokio::net::UdpSocket;

use crate::cid::ConnectionPeer;

/// An abstract representation of an asynchronous UDP socket.
#[async_trait]
pub trait AsyncUdpSocket<P: ConnectionPeer>: Send + Sync {
    /// Attempts to send data on the socket to a given address.
    /// Note that this should return nearly immediately, rather than awaiting something internally.
    async fn send_to(&mut self, buf: &[u8], target: &P) -> io::Result<usize>;
    /// Attempts to receive a single datagram on the socket.
    async fn recv_from(&mut self, buf: &mut [u8]) -> io::Result<(usize, P)>;
}

#[async_trait]
impl AsyncUdpSocket<SocketAddr> for UdpSocket {
    async fn send_to(&mut self, buf: &[u8], target: &SocketAddr) -> io::Result<usize> {
        UdpSocket::send_to(self, buf, target).await
    }

    async fn recv_from(&mut self, buf: &mut [u8]) -> io::Result<(usize, SocketAddr)> {
        UdpSocket::recv_from(self, buf).await
    }
}
