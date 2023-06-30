use std::collections::HashSet;
use std::fmt::Debug;
use std::hash::Hash;
use std::net::SocketAddr;

/// A remote peer.
pub trait ConnectionPeer: Clone + Debug + Eq + Hash + PartialEq + Send + Sync {}

impl ConnectionPeer for SocketAddr {}

#[derive(Clone, Copy, Debug, Hash, Eq, PartialEq)]
pub struct ConnectionId<P> {
    pub(crate) send: u16,
    pub(crate) recv: u16,
    pub(crate) peer: P,
}

impl<P> ConnectionId<P> {
    pub fn new(id: u16, peer: P, is_sender: bool) -> Self {
        if is_sender {
            Self {
                send: id,
                recv: id.wrapping_sub(1),
                peer,
            }
        } else {
            Self {
                send: id.wrapping_sub(1),
                recv: id,
                peer,
            }
        }
    }

    pub fn send(&self) -> u16 {
        self.send
    }

    pub fn recv(&self) -> u16 {
        self.recv
    }
}

pub trait ConnectionIdGenerator<P> {
    fn cid(&mut self, peer: P, is_initiator: bool) -> ConnectionId<P>;
}

#[derive(Clone, Debug, Default)]
pub struct StdConnectionIdGenerator<P> {
    cids: HashSet<ConnectionId<P>>,
}

impl<P> StdConnectionIdGenerator<P> {
    pub fn new() -> Self {
        Self {
            cids: HashSet::new(),
        }
    }
}

impl<P: ConnectionPeer> ConnectionIdGenerator<P> for StdConnectionIdGenerator<P> {
    fn cid(&mut self, peer: P, is_initiator: bool) -> ConnectionId<P> {
        loop {
            let recv: u16 = rand::random();
            let send = if is_initiator {
                recv.wrapping_add(1)
            } else {
                recv.wrapping_sub(1)
            };
            let cid = ConnectionId {
                send,
                recv,
                peer: peer.clone(),
            };

            if !self.cids.contains(&cid) {
                self.cids.insert(cid.clone());
                break cid;
            }
        }
    }
}
