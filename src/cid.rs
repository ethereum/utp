#[derive(Clone, Copy, Debug, Hash, Eq, PartialEq)]
pub struct ConnectionId<P> {
    pub send: u16,
    pub recv: u16,
    pub peer_id: P,
}
