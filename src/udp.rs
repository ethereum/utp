use std::io;
use std::net::SocketAddr;

use async_trait::async_trait;
use tokio::net::UdpSocket;

/// An abstract representation of an asynchronous UDP socket.
#[async_trait]
pub trait AsyncUdpSocket: Send + Sync {
    /// Attempts to send data on the socket to a given address.
    async fn send_to(&self, buf: &[u8], target: &SocketAddr) -> io::Result<usize>;
    /// Attempts to receive a single datagram on the socket.
    async fn recv_from(&self, buf: &mut [u8]) -> io::Result<(usize, SocketAddr)>;
}

#[async_trait]
impl AsyncUdpSocket for UdpSocket {
    async fn send_to(&self, buf: &[u8], target: &SocketAddr) -> io::Result<usize> {
        self.send_to(buf, *target).await
    }

    async fn recv_from(&self, buf: &mut [u8]) -> io::Result<(usize, SocketAddr)> {
        self.recv_from(buf).await
    }
}
