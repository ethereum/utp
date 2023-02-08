use std::io;
use std::net::SocketAddr;

use tokio::sync::{mpsc, oneshot};

use crate::cid::ConnectionId;
use crate::conn;
use crate::event::StreamEvent;
use crate::packet::Packet;

const N: usize = u16::MAX as usize;

pub struct UtpStream {
    cid: ConnectionId,
    reads: mpsc::UnboundedSender<conn::Read>,
    writes: mpsc::UnboundedSender<conn::Write>,
    shutdown: Option<oneshot::Sender<()>>,
}

impl UtpStream {
    pub(crate) fn new(
        cid: ConnectionId,
        syn: Option<Packet>,
        outgoing: mpsc::UnboundedSender<(Packet, SocketAddr)>,
        events: mpsc::UnboundedReceiver<StreamEvent>,
        connected: oneshot::Sender<io::Result<()>>,
    ) -> Self {
        let mut conn = conn::Connection::<N>::new(cid, syn, connected, outgoing);

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
        let mut offset = 0;

        loop {
            let (tx, rx) = oneshot::channel();
            self.reads
                .send((buf.len(), tx))
                .map_err(|_| io::Error::from(io::ErrorKind::Interrupted))?;

            match rx.await {
                Ok(result) => match result {
                    Ok(data) => {
                        if data.is_empty() {
                            break Ok(offset);
                        }
                        let end = offset + data.len();
                        buf[offset..end].copy_from_slice(data.as_slice());
                        offset = end;
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
