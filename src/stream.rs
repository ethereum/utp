use std::io;

use tokio::sync::{mpsc, oneshot};
use tokio::task;
use tracing::Instrument;

use crate::cid::ConnectionId;
use crate::congestion::DEFAULT_MAX_PACKET_SIZE_BYTES;
use crate::conn;
use crate::event::{SocketEvent, StreamEvent};
use crate::packet::Packet;
use crate::peer::{ConnectionPeer, Peer};

/// The size of the send and receive buffers.
// TODO: Make the buffer size configurable.
const BUF: usize = 1024 * 1024;

pub struct UtpStream<P: ConnectionPeer> {
    cid: ConnectionId<P::Id>,
    reads: mpsc::UnboundedReceiver<conn::Read>,
    writes: mpsc::UnboundedSender<conn::Write>,
    shutdown: Option<oneshot::Sender<()>>,
    conn_handle: Option<task::JoinHandle<io::Result<()>>>,
}

impl<P> UtpStream<P>
where
    P: ConnectionPeer + 'static,
{
    pub(crate) fn new(
        cid: ConnectionId<P::Id>,
        peer: Peer<P>,
        config: conn::ConnectionConfig,
        syn: Option<Packet>,
        socket_events: mpsc::UnboundedSender<SocketEvent<P>>,
        stream_events: mpsc::UnboundedReceiver<StreamEvent>,
        connected: oneshot::Sender<io::Result<()>>,
    ) -> Self {
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let (reads_tx, reads_rx) = mpsc::unbounded_channel();
        let (writes_tx, writes_rx) = mpsc::unbounded_channel();
        let mut conn = conn::Connection::<BUF, P>::new(
            cid.clone(),
            peer,
            config,
            syn,
            connected,
            socket_events,
            reads_tx,
        );
        let conn_handle = tokio::spawn(async move {
            conn.event_loop(stream_events, writes_rx, shutdown_rx)
                .instrument(tracing::info_span!("uTP", send = cid.send, recv = cid.recv))
                .await
        });

        Self {
            cid,
            reads: reads_rx,
            writes: writes_tx,
            shutdown: Some(shutdown_tx),
            conn_handle: Some(conn_handle),
        }
    }

    pub fn cid(&self) -> &ConnectionId<P::Id> {
        &self.cid
    }

    pub async fn read_to_eof(&mut self, buf: &mut Vec<u8>) -> io::Result<usize> {
        // Reserve space in the buffer to avoid expensive allocation for small reads.
        buf.reserve(DEFAULT_MAX_PACKET_SIZE_BYTES as usize);

        let mut n = 0;
        loop {
            match self.reads.recv().await {
                Some(data) => match data {
                    Ok(mut data) => {
                        if data.is_empty() {
                            return Ok(n);
                        }
                        n += data.len();
                        buf.append(&mut data);

                        // Reserve additional space in the buffer proportional to the amount of
                        // data read.
                        buf.reserve(data.len());
                    }
                    Err(err) => return Err(err),
                },
                None => tracing::debug!("read buffer was sent None"),
            }
        }
    }

    pub async fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        if self.shutdown.is_none() {
            return Err(io::Error::from(io::ErrorKind::NotConnected));
        }

        let (tx, rx) = oneshot::channel();
        self.writes
            .send((buf.to_vec(), tx))
            .map_err(|_| io::Error::from(io::ErrorKind::NotConnected))?;

        match rx.await {
            Ok(n) => Ok(n?),
            Err(err) => Err(io::Error::new(io::ErrorKind::Other, format!("{err:?}"))),
        }
    }

    /// Closes the stream gracefully.
    /// Completes when the remote peer acknowledges all sent data.
    pub async fn close(&mut self) -> io::Result<()> {
        self.shutdown()?;
        match self.conn_handle.take() {
            Some(conn_handle) => conn_handle.await?,
            None => Err(io::Error::from(io::ErrorKind::NotConnected)),
        }
    }
}

impl<P: ConnectionPeer> UtpStream<P> {
    // Send signal to the connection event loop to exit, after all outgoing writes have completed.
    // Public callers should use close() instead.
    fn shutdown(&mut self) -> io::Result<()> {
        match self.shutdown.take() {
            Some(shutdown) => Ok(shutdown
                .send(())
                .map_err(|_| io::Error::from(io::ErrorKind::NotConnected))?),
            None => Err(io::Error::from(io::ErrorKind::NotConnected)),
        }
    }
}

impl<P: ConnectionPeer> Drop for UtpStream<P> {
    fn drop(&mut self) {
        let _ = self.shutdown();
    }
}
