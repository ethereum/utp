use crate::packet::Packet;

#[derive(Clone, Debug)]
pub enum StreamEvent {
    Incoming(Packet),
    Shutdown,
}
