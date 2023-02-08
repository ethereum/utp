use std::collections::HashSet;
use std::hash::Hash;
use std::net::SocketAddr;

#[derive(Clone, Copy, Debug, Hash, Eq, PartialEq)]
pub struct ConnectionId {
    pub send: u16,
    pub recv: u16,
    pub peer: SocketAddr,
}

pub trait ConnectionIdGenerator<P> {
    fn cid(&mut self, peer: P) -> ConnectionId;
}

#[derive(Clone, Debug, Default)]
pub struct StdConnectionIdGenerator {
    cids: HashSet<ConnectionId>,
}

impl ConnectionIdGenerator<SocketAddr> for StdConnectionIdGenerator {
    fn cid(&mut self, peer: SocketAddr) -> ConnectionId {
        loop {
            let recv: u16 = rand::random();
            let send = recv.wrapping_add(1);
            let cid = ConnectionId { send, recv, peer };

            if !self.cids.contains(&cid) {
                self.cids.insert(cid);
                break cid;
            }
        }
    }
}
