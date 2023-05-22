use thiserror::Error;
use tokio::sync::{mpsc, oneshot};
use tracing::Instrument;

use crate::cid::{ConnectionId, ConnectionPeer};
use crate::conn;
use crate::event::{SocketEvent, StreamEvent};
use crate::packet::Packet;

/// The size of the send and receive buffers.
// TODO: Make the buffer size configurable.
const BUF: usize = 1024 * 1024;

#[derive(Debug, Error)]
pub enum Error {
    #[error("uTP connection error, {0}")]
    Conn(#[from] conn::Error),
    #[error("no uTP connection")]
    NoConn,
    #[error("sending on notify channel failed")]
    NotifyShutdown,
    #[error("notify channel failed, {0}")]
    NotifyChannelRecv(#[from] oneshot::error::RecvError),
    #[error("sending on read channel failed")]
    ReadChannelSend,
    #[error("sending on write channel failed")]
    WriteChannelSend,
}

pub struct UtpStream<P> {
    cid: ConnectionId<P>,
    reads: mpsc::UnboundedSender<conn::Read>,
    writes: mpsc::UnboundedSender<conn::Write>,
    shutdown: Option<oneshot::Sender<()>>,
}

impl<P> UtpStream<P>
where
    P: ConnectionPeer + 'static,
{
    pub(crate) fn new(
        cid: ConnectionId<P>,
        config: conn::ConnectionConfig,
        syn: Option<Packet>,
        socket_events: mpsc::UnboundedSender<SocketEvent<P>>,
        stream_events: mpsc::UnboundedReceiver<StreamEvent>,
        connected: oneshot::Sender<Result<(), conn::Error>>,
    ) -> Self {
        let mut conn =
            conn::Connection::<BUF, P>::new(cid.clone(), config, syn, connected, socket_events);

        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let (reads_tx, reads_rx) = mpsc::unbounded_channel();
        let (writes_tx, writes_rx) = mpsc::unbounded_channel();
        tokio::spawn(async move {
            conn.event_loop(stream_events, writes_rx, reads_rx, shutdown_rx)
                .instrument(tracing::info_span!("uTP", send = cid.send, recv = cid.recv))
                .await
        });

        Self {
            cid,
            reads: reads_tx,
            writes: writes_tx,
            shutdown: Some(shutdown_tx),
        }
    }

    pub fn cid(&self) -> &ConnectionId<P> {
        &self.cid
    }

    pub async fn read_to_eof(&mut self, buf: &mut Vec<u8>) -> Result<usize, Error> {
        // Reserve space in the buffer to avoid expensive allocation for small reads.
        buf.reserve(2048);

        let mut n = 0;
        loop {
            let (tx, rx) = oneshot::channel();
            self.reads
                .send((buf.capacity(), tx))
                .map_err(|_| Error::ReadChannelSend)?;

            let mut data = rx.await??;
            if data.is_empty() {
                break Ok(n);
            }
            n += data.len();
            buf.append(&mut data);

            // Reserve additional space in the buffer proportional to the amount of
            // data read.
            buf.reserve(data.len());
        }
    }

    pub async fn write(&mut self, buf: &[u8]) -> Result<usize, Error> {
        if self.shutdown.is_none() {
            return Err(Error::NoConn);
        }

        let (tx, rx) = oneshot::channel();
        self.writes
            .send((buf.to_vec(), tx))
            .map_err(|_| Error::WriteChannelSend)?;

        Ok(rx.await??)
    }
}

impl<P> UtpStream<P> {
    pub fn shutdown(&mut self) -> Result<(), Error> {
        match self.shutdown.take() {
            Some(shutdown) => Ok(shutdown.send(()).map_err(|_| Error::NotifyShutdown)?),
            None => Err(Error::NoConn),
        }
    }
}

impl<P> Drop for UtpStream<P> {
    fn drop(&mut self) {
        let _ = self.shutdown();
    }
}

#[cfg(test)]
mod test {
    use crate::conn::ConnectionConfig;
    use crate::socket::UtpSocket;
    use std::net::SocketAddr;
    #[tokio::test]
    async fn test_transfer_100k_bytes() {
        // set-up test
        let sender_addr = SocketAddr::from(([127, 0, 0, 1], 3400));
        let receiver_address = SocketAddr::from(([127, 0, 0, 1], 3401));
        let config = ConnectionConfig::default();
        let sender = UtpSocket::bind(sender_addr).await.unwrap();
        let receiver = UtpSocket::bind(receiver_address).await.unwrap();
        let mut tx_stream = sender.connect(receiver_address, config).await.unwrap();
        let mut rx_stream = receiver.accept(config).await.unwrap();

        // write 100k bytes data to the remote peer over the stream.
        let data = vec![0xef; 100_000];
        tx_stream
            .write(data.as_slice())
            .await
            .expect("Should send 100k bytes");

        // read data from the remote peer until the peer indicates there is no data left to write.
        let mut data = vec![];
        rx_stream
            .read_to_eof(&mut data)
            .await
            .expect("Should receive 100k bytes");
    }
}
