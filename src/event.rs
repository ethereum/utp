use crate::cid::ConnectionId;
use crate::packet::Packet;
use crate::peer::{ConnectionPeer, Peer};

#[derive(Clone, Debug)]
pub enum StreamEvent {
    Incoming(Packet),
    Shutdown,
}

#[derive(Clone, Debug)]
pub enum SocketEvent<P: ConnectionPeer> {
    Outgoing((Packet, Peer<P>)),
    Shutdown(ConnectionId<P::Id>),
}
