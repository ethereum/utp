use std::net::SocketAddr;
use std::sync::Arc;

use tokio::task::JoinHandle;

use utp_rs::cid;
use utp_rs::conn::ConnectionConfig;
use utp_rs::socket::UtpSocket;

#[tokio::test(flavor = "multi_thread")]
async fn socket() {
    tracing_subscriber::fmt::init();

    let recv_addr = SocketAddr::from(([127, 0, 0, 1], 3400));
    let send_addr = SocketAddr::from(([127, 0, 0, 1], 3401));

    let recv = UtpSocket::bind(recv_addr).await.unwrap();
    let recv = Arc::new(recv);
    let send = UtpSocket::bind(send_addr).await.unwrap();
    let send = Arc::new(send);
    let mut handles = vec![];

    // start 50 transfers, step by two to avoid cid collisions
    for i in (0..1000).step_by(2) {
        let handle = initiate_transfer(i, recv_addr, recv.clone(), send_addr, send.clone()).await;
        handles.push(handle.0);
        handles.push(handle.1);
    }

    let result = futures::future::join_all(handles).await;
    for res in result {
        res.unwrap();
    }
}

async fn initiate_transfer(
    i: u16,
    recv_addr: SocketAddr,
    recv: Arc<UtpSocket<SocketAddr>>,
    send_addr: SocketAddr,
    send: Arc<UtpSocket<SocketAddr>>,
) -> (JoinHandle<()>, JoinHandle<()>) {
    let conn_config = ConnectionConfig::default();
    let initiator_cid = 100 + i;
    let responder_cid = 100 + i + 1;
    let recv_cid = cid::ConnectionId {
        send: initiator_cid,
        recv: responder_cid,
        peer: send_addr,
    };
    let send_cid = cid::ConnectionId {
        send: responder_cid,
        recv: initiator_cid,
        peer: recv_addr,
    };

    let data = vec![0xfe; 1_000_000];
    let data_recv = data.clone();

    let recv_handle = tokio::spawn(async move {
        let mut stream = recv.accept_with_cid(recv_cid, conn_config).await.unwrap();
        let mut buf = vec![];
        let n = stream.read_to_eof(&mut buf).await.unwrap();
        tracing::info!(cid.send = %recv_cid.send, cid.recv = %recv_cid.recv, "read {n} bytes from uTP stream");

        assert_eq!(n, data_recv.len());
        assert_eq!(buf, data_recv);
    });

    let send_handle = tokio::spawn(async move {
        let mut stream = send.connect_with_cid(send_cid, conn_config).await.unwrap();
        let n = stream.write(&data).await.unwrap();
        assert_eq!(n, data.len());

        stream.shutdown().unwrap();
    });
    (send_handle, recv_handle)
}
