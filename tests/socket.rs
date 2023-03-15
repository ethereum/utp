use std::net::SocketAddr;
use std::sync::Arc;

use utp_rs::cid;
use utp_rs::conn::ConnectionConfig;
use utp_rs::socket::UtpSocket;

#[tokio::test(flavor = "multi_thread")]
async fn socket() {
    tracing_subscriber::fmt::init();

    let conn_config = ConnectionConfig::default();

    let data_one = vec![0xef; 8192 * 2 * 2];
    let data_one_recv = data_one.clone();

    let recv_addr = SocketAddr::from(([127, 0, 0, 1], 3400));
    let recv = UtpSocket::bind(recv_addr).await.unwrap();
    let recv = Arc::new(recv);

    let send_addr = SocketAddr::from(([127, 0, 0, 1], 3401));
    let send = UtpSocket::bind(send_addr).await.unwrap();
    let send = Arc::new(send);

    let recv_one_cid = cid::ConnectionId {
        send: 100,
        recv: 101,
        peer: send_addr,
    };
    let send_one_cid = cid::ConnectionId {
        send: 101,
        recv: 100,
        peer: recv_addr,
    };

    let recv_arc = Arc::clone(&recv);
    let recv_one_handle = tokio::spawn(async move {
        let mut stream = recv_arc
            .accept_with_cid(recv_one_cid, conn_config)
            .await
            .unwrap();
        let mut buf = vec![];
        let n = stream.read_to_eof(&mut buf).await.unwrap();
        tracing::info!(cid.send = %recv_one_cid.send, cid.recv = %recv_one_cid.recv, "read {n} bytes from uTP stream");

        assert_eq!(n, data_one_recv.len());
        assert_eq!(buf, data_one_recv);
    });

    let send_arc = Arc::clone(&send);
    tokio::spawn(async move {
        let mut stream = send_arc
            .connect_with_cid(send_one_cid, conn_config)
            .await
            .unwrap();
        let n = stream.write(&data_one).await.unwrap();
        assert_eq!(n, data_one.len());

        let _ = stream.shutdown();
    });

    let data_two = vec![0xfe; 8192 * 2 * 2];
    let data_two_recv = data_two.clone();

    let recv_two_cid = cid::ConnectionId {
        send: 200,
        recv: 201,
        peer: send_addr,
    };
    let send_two_cid = cid::ConnectionId {
        send: 201,
        recv: 200,
        peer: recv_addr,
    };

    let recv_two_handle = tokio::spawn(async move {
        let mut stream = recv
            .accept_with_cid(recv_two_cid, conn_config)
            .await
            .unwrap();
        let mut buf = vec![];
        let n = stream.read_to_eof(&mut buf).await.unwrap();
        tracing::info!(cid.send = %recv_two_cid.send, cid.recv = %recv_two_cid.recv, "read {n} bytes from uTP stream");

        assert_eq!(n, data_two_recv.len());
        assert_eq!(buf, data_two_recv);
    });

    tokio::spawn(async move {
        let mut stream = send
            .connect_with_cid(send_two_cid, conn_config)
            .await
            .unwrap();
        let n = stream.write(&data_two).await.unwrap();
        assert_eq!(n, data_two.len());

        let _ = stream.shutdown();
    });

    let (one, two) = tokio::join!(recv_one_handle, recv_two_handle);
    one.unwrap();
    two.unwrap();
}
