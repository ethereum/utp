use std::collections::HashSet;
use std::fmt::Debug;
use std::hash::Hash;
use std::net::SocketAddr;

/// A remote peer.
pub trait ConnectionPeer: Clone + Debug + Eq + Hash + PartialEq + Send + Sync {}

impl ConnectionPeer for SocketAddr {}

#[derive(Clone, Copy, Debug, Hash, Eq, PartialEq)]
pub struct ConnectionId<P> {
    pub send: u16,
    pub recv: u16,
    pub peer: P,
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
