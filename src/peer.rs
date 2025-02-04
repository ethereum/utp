use std::fmt::Debug;
use std::hash::Hash;
use std::net::SocketAddr;

/// A trait that describes remote peer
pub trait ConnectionPeer: Debug + Clone + Send + Sync {
    type Id: Debug + Clone + PartialEq + Eq + Hash + Send + Sync;

    /// Returns peer's id
    fn id(&self) -> Self::Id;

    /// Consolidates two peers into one.
    ///
    /// It's possible that we have two instances that represent the same peer (equal `peer_id`),
    /// and we need to consolidate them into one. This can happen when [Peer]-s passed with
    /// [UtpSocket::accept_with_cid](crate::socket::UtpSocket::accept_with_cid) or
    /// [UtpSocket::connect_with_cid](crate::socket::UtpSocket::connect_with_cid), and returned by
    /// [AsyncUdpSocket::recv_from](crate::udp::AsyncUdpSocket::recv_from) contain peers (not just
    /// `peer_id`).
    ///
    /// The structure implementing this trait can decide on the exact behavior. Some examples:
    /// - If structure is simple (i.e. two peers are the same iff all fields are the same), return
    ///   either (see implementation for `SocketAddr`)
    /// - If we can determine which peer is newer (e.g. using timestamp or version field), return
    ///   newer peer
    /// - If structure behaves more like a key-value map whose values don't change over time,
    ///   merge key-value pairs from both instances into one
    ///
    /// Should panic if ids are not matching.
    fn consolidate(a: Self, b: Self) -> Self;
}

impl ConnectionPeer for SocketAddr {
    type Id = Self;

    fn id(&self) -> Self::Id {
        *self
    }

    fn consolidate(a: Self, b: Self) -> Self {
        assert!(a == b, "Consolidating non-equal peers");
        a
    }
}

/// Structure that stores peer's id, and maybe peer as well.
#[derive(Debug, Clone)]
pub struct Peer<P: ConnectionPeer> {
    id: P::Id,
    peer: Option<P>,
}

impl<P: ConnectionPeer> Peer<P> {
    /// Creates new instance that stores peer
    pub fn new(peer: P) -> Self {
        Self {
            id: peer.id(),
            peer: Some(peer),
        }
    }

    /// Creates new instance that only stores peer's id
    pub fn new_id(peer_id: P::Id) -> Self {
        Self {
            id: peer_id,
            peer: None,
        }
    }

    /// Returns peer's id
    pub fn id(&self) -> &P::Id {
        &self.id
    }

    /// Returns optional reference to peer
    pub fn peer(&self) -> Option<&P> {
        self.peer.as_ref()
    }

    /// Consolidates given peer into `Self` whilst consuming it.
    ///
    /// See [ConnectionPeer::consolidate] for details.
    ///
    /// Panics if ids are not matching.
    pub fn consolidate(&mut self, other: Self) {
        assert!(self.id == other.id, "Consolidating with non-equal peer");
        let Some(other_peer) = other.peer else {
            return;
        };

        self.peer = match self.peer.take() {
            Some(peer) => Some(P::consolidate(peer, other_peer)),
            None => Some(other_peer),
        };
    }
}
