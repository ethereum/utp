use std::fmt::Debug;
use std::hash::Hash;
use std::net::SocketAddr;

/// A trait that describes remote peer
pub trait ConnectionPeer: Debug + Clone + Send + Sync {
    type Id: Debug + Clone + PartialEq + Eq + Hash + Send + Sync;

    /// Returns peer's id
    fn id(&self) -> Self::Id;

    /// Merge the given object into `Self` whilst consuming it.
    ///
    /// Should panic if ids are not matching.
    fn merge(&mut self, other: Self);
}

impl ConnectionPeer for SocketAddr {
    type Id = Self;

    fn id(&self) -> Self::Id {
        *self
    }

    fn merge(&mut self, other: Self) {
        assert!(self == &other)
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

    /// Merge the given peer into `Self` whilst consuming it.
    ///
    /// Panics if ids are not matching.
    pub fn merge(&mut self, other: Self) {
        assert!(self.id == other.id);
        let Some(other_peer) = other.peer else {
            return;
        };

        self.peer = match self.peer.take() {
            Some(mut peer) => {
                peer.merge(other_peer);
                Some(peer)
            }
            None => Some(other_peer),
        };
    }
}
