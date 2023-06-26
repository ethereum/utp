use std::io;

use tokio::sync::{mpsc, oneshot};
use tracing::Instrument;

use crate::cid::{ConnectionId, ConnectionPeer};
use crate::conn;
use crate::event::{SocketEvent, StreamEvent};
use crate::packet::Packet;
use crate::socket;

/// The size of the send and receive buffers.
// TODO: Make the buffer size configurable.
const BUF: usize = 1024 * 1024;

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
        conn_config: conn::ConnectionConfig,
        socket_config: socket::SocketConfig,
        syn: Option<Packet>,
        socket_events: mpsc::UnboundedSender<SocketEvent<P>>,
        stream_events: mpsc::UnboundedReceiver<StreamEvent>,
        connected: oneshot::Sender<io::Result<()>>,
    ) -> Self {
        let mut conn =
            conn::Connection::<BUF, P>::new(cid.clone(), conn_config, socket_config, syn, connected, socket_events);

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

    pub async fn read_to_eof(&mut self, buf: &mut Vec<u8>) -> io::Result<usize> {
        // Reserve space in the buffer to avoid expensive allocation for small reads.
        buf.reserve(2048);

        let mut n = 0;
        loop {
            let (tx, rx) = oneshot::channel();
            self.reads
                .send((buf.capacity(), tx))
                .map_err(|_| io::Error::from(io::ErrorKind::NotConnected))?;

            match rx.await {
                Ok(result) => match result {
                    Ok(mut data) => {
                        if data.is_empty() {
                            break Ok(n);
                        }
                        n += data.len();
                        buf.append(&mut data);

                        // Reserve additional space in the buffer proportional to the amount of
                        // data read.
                        buf.reserve(data.len());
                    }
                    Err(err) => return Err(err),
                },
                Err(err) => return Err(io::Error::new(io::ErrorKind::Other, format!("{err:?}"))),
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
}

impl<P> UtpStream<P> {
    pub fn shutdown(&mut self) -> io::Result<()> {
        match self.shutdown.take() {
            Some(shutdown) => Ok(shutdown
                .send(())
                .map_err(|_| io::Error::from(io::ErrorKind::NotConnected))?),
            None => Err(io::Error::from(io::ErrorKind::NotConnected)),
        }
    }
}

impl<P> Drop for UtpStream<P> {
    fn drop(&mut self) {
        let _ = self.shutdown();
    }
}
