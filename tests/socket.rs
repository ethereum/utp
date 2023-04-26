use std::net::SocketAddr;
use std::sync::Arc;

use tokio::task::JoinHandle;
use tokio::time::Instant;

use utp_rs::cid;
use utp_rs::conn::ConnectionConfig;
use utp_rs::socket::UtpSocket;

const TEST_DATA: &[u8] = &[0xf0; 1_000_000];

#[tokio::test(flavor = "multi_thread", worker_threads = 16)]
async fn socket() {
    tracing_subscriber::fmt::init();

    let recv_addr = SocketAddr::from(([127, 0, 0, 1], 3400));
    let send_addr = SocketAddr::from(([127, 0, 0, 1], 3401));

    let recv = UtpSocket::bind(recv_addr).await.unwrap();
    let recv = Arc::new(recv);
    let send = UtpSocket::bind(send_addr).await.unwrap();
    let send = Arc::new(send);
    let mut handles = vec![];

    let start = Instant::now();
    let num_transfers = 1500;
    for i in 0..num_transfers {
        // step up cid by two to avoid collisions
        let handle = initiate_transfer(i * 2, recv_addr, recv.clone(), send_addr, send.clone()).await;
        handles.push(handle.0);
        handles.push(handle.1);
    }

    let result = futures::future::join_all(handles).await;
    for res in result {
        res.unwrap();
    }
    let elapsed = Instant::now() - start;
    let megabits_sent = num_transfers as f64 * TEST_DATA.len() as f64 * 8.0 / 1_000_000.0;
    let transfer_rate = megabits_sent / elapsed.as_secs_f64();
    tracing::info!("finished real udp load test of {} simultaneous transfers, in {:?}, at a rate of {:.0} Mbps", num_transfers, elapsed, transfer_rate);
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

    let recv_handle = tokio::spawn(async move {
        let mut stream = recv.accept_with_cid(recv_cid, conn_config).await.unwrap();
        let mut buf = vec![];
        let n = stream.read_to_eof(&mut buf).await.unwrap();
        tracing::info!(cid.send = %recv_cid.send, cid.recv = %recv_cid.recv, "read {n} bytes from uTP stream");

        assert_eq!(n, TEST_DATA.len());
        assert_eq!(buf, TEST_DATA);
    });

    let send_handle = tokio::spawn(async move {
        let mut stream = send.connect_with_cid(send_cid, conn_config).await.unwrap();
        let n = stream.write(TEST_DATA).await.unwrap();
        assert_eq!(n, TEST_DATA.len());

        stream.shutdown().unwrap();
    });
    (send_handle, recv_handle)
}
