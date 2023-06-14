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
    async fn send(&mut self, buf: &[u8], target: &P) -> io::Result<usize>;
    /// Attempts to receive a single datagram on the socket.
    async fn recv(&mut self, buf: &mut [u8]) -> io::Result<(usize, P)>;
}


// If we want to support send() implementations using await that takes a while internally, we
// probably have to split the sender and receiver into two separate structs. This is because
// we move the socket into the loop that listens for received state (in src/socket.rs), but if
// sending waits, then we need it in a separate loop that sends packets. We can move the sender
// into one spawned loop, and the receiver into another.
//
// This first came up when writing a test with a "perfect" link implementing the socket with a
// bounded channel that gets awaited when sending. That caused the socket event loop to hang and
// never recover.
//
// Note that if we take this approach, we need to do some work to make sure that we are still
// biasing in favor of reads over rights, possibly by keeping the reads/writes in the same loop,
// but putting the writes into another channel that's handled in a separate spawned loop. That
// would serve to bias the reads over writes, but still smoothly handle writes that take a while.

#[async_trait]
impl AsyncUdpSocket<SocketAddr> for UdpSocket {
    async fn send(&mut self, buf: &[u8], target: &SocketAddr) -> io::Result<usize> {
        self.send_to(buf, target).await
    }

    async fn recv(&mut self, buf: &mut [u8]) -> io::Result<(usize, SocketAddr)> {
        self.recv_from(buf).await
    }
}
