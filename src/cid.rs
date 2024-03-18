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
