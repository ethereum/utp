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

        let _ = stream.close().await;
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

        let _ = stream.close().await;
    });

    let (one, two) = tokio::join!(recv_one_handle, recv_two_handle);
    one.unwrap();
    two.unwrap();
}

// Test that a new socket has zero connections
#[tokio::test]
async fn test_empty_socket_conn_count() {
    let socket_addr = SocketAddr::from(([127, 0, 0, 1], 3402));
    let socket = UtpSocket::bind(socket_addr).await.unwrap();
    assert_eq!(socket.num_connections(), 0);
}

// Test that a socket returns 2 from num_connections after connecting twice
#[tokio::test]
async fn test_socket_reports_two_connections() {
    let conn_config = ConnectionConfig::default();

    let recv_addr = SocketAddr::from(([127, 0, 0, 1], 3404));
    let recv = UtpSocket::bind(recv_addr).await.unwrap();
    let recv = Arc::new(recv);

    let send_addr = SocketAddr::from(([127, 0, 0, 1], 3405));
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

    let recv_one = Arc::clone(&recv);
    let recv_one_handle = tokio::spawn(async move {
        recv_one
            .accept_with_cid(recv_one_cid, conn_config)
            .await
            .unwrap()
    });

    let send_one = Arc::clone(&send);
    let send_one_handle = tokio::spawn(async move {
        send_one
            .connect_with_cid(send_one_cid, conn_config)
            .await
            .unwrap()
    });

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

    let recv_two = Arc::clone(&recv);
    let recv_two_handle = tokio::spawn(async move {
        recv_two
            .accept_with_cid(recv_two_cid, conn_config)
            .await
            .unwrap()
    });

    let send_two = Arc::clone(&send);
    let send_two_handle = tokio::spawn(async move {
        send_two
            .connect_with_cid(send_two_cid, conn_config)
            .await
            .unwrap()
    });

    let (tx_one, rx_one, tx_two, rx_two) = tokio::join!(
        send_one_handle,
        recv_one_handle,
        send_two_handle,
        recv_two_handle
    );
    tx_one.unwrap();
    rx_one.unwrap();
    tx_two.unwrap();
    rx_two.unwrap();

    assert_eq!(recv.num_connections(), 2);
    assert_eq!(send.num_connections(), 2);
}
