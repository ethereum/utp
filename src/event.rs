use std::time::Instant;
use crate::cid::ConnectionId;
use crate::packet::Packet;

#[derive(Clone, Debug)]
pub enum StreamEvent {
    Incoming(Packet, Instant),
    Shutdown,
}

#[derive(Clone, Debug)]
pub enum SocketEvent<P> {
    Outgoing((Packet, P)),
    Shutdown(ConnectionId<P>),
}
