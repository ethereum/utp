use std::net::SocketAddr;
use std::sync::Arc;

use utp_rs::cid;
use utp_rs::conn::ConnectionConfig;
use utp_rs::socket::UtpSocket;

// Test that close() returns successful, after transfer is complete
#[tokio::test]
async fn close_is_successful_when_write_completes() {
    let conn_config = ConnectionConfig::default();

    // TODO replace with a mock socket
    let recv_addr = SocketAddr::from(([127, 0, 0, 1], 3406));
    let recv = UtpSocket::bind(recv_addr).await.unwrap();
    let recv = Arc::new(recv);

    let send_addr = SocketAddr::from(([127, 0, 0, 1], 3407));
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

    let (tx_one, rx_one) = tokio::join!(send_one_handle, recv_one_handle,);
    let mut send_stream = tx_one.unwrap();
    let mut recv_stream = rx_one.unwrap();

    // data to send
    const DATA_LEN: usize = 100;
    let data = [0xa5; DATA_LEN];

    // send data
    let send_stream_handle = tokio::spawn(async move {
        match send_stream.write(&data).await {
            Ok(written_len) => assert_eq!(written_len, DATA_LEN),
            Err(e) => panic!("Error sending data: {:?}", e),
        };
        send_stream
    });

    // recv data
    let recv_stream_handle = tokio::spawn(async move {
        let mut read_buf = vec![];
        let _ = recv_stream.read_to_eof(&mut read_buf).await.unwrap();
        assert_eq!(read_buf, data.to_vec());
    });

    // wait for send to start
    let mut send_stream = send_stream_handle.await.unwrap();

    // close stream, which will wait for write to complete, and exit without a problem
    send_stream.close().await.unwrap();

    // confirm that data is received as expected
    recv_stream_handle.await.unwrap();
}

// TODO after adding PerfectLink from stash@{1}: WIP on test-packet-drops: 7408259 Allow for mutable state in Socket send/recv
// Test that close() returns a timeout, if recipient is not ACKing
// AFter adding some mechanism that sends a RESET packet, add the following test:
// Test that close() returns ConnectionReset if recipient resets during transfer
