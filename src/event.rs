use crate::packet::Packet;

pub enum StreamEvent {
    Incoming(Packet),
    Shutdown,
}
