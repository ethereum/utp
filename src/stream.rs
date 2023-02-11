use std::io;
use std::net::SocketAddr;

use tokio::sync::{mpsc, oneshot};

use crate::cid::ConnectionId;
use crate::conn;
use crate::event::StreamEvent;
use crate::packet::Packet;

/// The size of the send and receive buffers.
// TODO: Make the buffer size configurable.
const BUF: usize = 1024 * 1024;

pub struct UtpStream {
    cid: ConnectionId,
    reads: mpsc::UnboundedSender<conn::Read>,
    writes: mpsc::UnboundedSender<conn::Write>,
    shutdown: Option<oneshot::Sender<()>>,
}

impl UtpStream {
    pub(crate) fn new(
        cid: ConnectionId,
        config: conn::ConnectionConfig,
        syn: Option<Packet>,
        outgoing: mpsc::UnboundedSender<(Packet, SocketAddr)>,
        events: mpsc::UnboundedReceiver<StreamEvent>,
        connected: oneshot::Sender<io::Result<()>>,
    ) -> Self {
        let mut conn = conn::Connection::<BUF>::new(cid, config, syn, connected, outgoing);

        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let (reads_tx, reads_rx) = mpsc::unbounded_channel();
        let (writes_tx, writes_rx) = mpsc::unbounded_channel();
        tokio::spawn(async move {
            conn.event_loop(events, writes_rx, reads_rx, shutdown_rx)
                .await
        });

        Self {
            cid,
            reads: reads_tx,
            writes: writes_tx,
            shutdown: Some(shutdown_tx),
        }
    }

    pub fn peer_addr(&self) -> SocketAddr {
        self.cid.peer
    }

    pub async fn read_to_eof(&mut self, buf: &mut Vec<u8>) -> io::Result<usize> {
        let mut n = 0;

        // Reserve space in the buffer to avoid expensive allocation for small reads.
        buf.reserve(2048);

        loop {
            let (tx, rx) = oneshot::channel();
            self.reads
                .send((buf.capacity(), tx))
                .map_err(|_| io::Error::from(io::ErrorKind::Interrupted))?;

            match rx.await {
                Ok(result) => match result {
                    Ok(mut data) => {
                        println!("read_to_eof: data len {}", data.len());
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
                Err(..) => return Err(io::Error::from(io::ErrorKind::Interrupted)),
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
            .map_err(|_| io::Error::from(io::ErrorKind::Interrupted))?;

        match rx.await {
            Ok(n) => Ok(n?),
            Err(..) => Err(io::Error::from(io::ErrorKind::Interrupted)),
        }
    }

    pub fn shutdown(&mut self) -> io::Result<()> {
        match self.shutdown.take() {
            Some(shutdown) => Ok(shutdown
                .send(())
                .map_err(|_| io::Error::from(io::ErrorKind::NotConnected))?),
            None => Err(io::Error::from(io::ErrorKind::NotConnected)),
        }
    }
}

impl Drop for UtpStream {
    fn drop(&mut self) {
        let _ = self.shutdown();
    }
}
