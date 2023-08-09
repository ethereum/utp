use std::io::ErrorKind;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use tokio::time::timeout;

use utp_rs::conn::ConnectionConfig;
use utp_rs::socket::UtpSocket;

use utp_rs::testutils;

// Test that close() returns successful, after transfer is complete
#[tokio::test]
async fn close_is_successful_when_write_completes() {
    let conn_config = ConnectionConfig::default();

    let ((send_link, send_cid), (recv_link, recv_cid)) = testutils::build_connected_pair();

    let recv = UtpSocket::with_socket(recv_link);
    let recv = Arc::new(recv);

    let send = UtpSocket::with_socket(send_link);
    let send = Arc::new(send);

    let recv_one = Arc::clone(&recv);
    let recv_one_handle = tokio::spawn(async move {
        recv_one
            .accept_with_cid(recv_cid, conn_config)
            .await
            .unwrap()
    });

    // Keep a clone of the socket so that it doesn't drop when moved into the task.
    // Dropping it causes all connections to exit.
    let send_one = Arc::clone(&send);
    let send_one_handle = tokio::spawn(async move {
        send_one
            .connect_with_cid(send_cid, conn_config)
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
    // This should happen extremely quickly.
    match timeout(Duration::from_millis(20), send_stream.close()).await {
        Ok(Ok(_)) => {}
        Ok(Err(e)) => panic!("Error closing stream: {:?}", e),
        Err(_) => panic!("Timeout closing stream"),
    };

    // confirm that data is received as expected
    recv_stream_handle.await.unwrap();
}

// Test that close() returns a timeout, if recipient is not ACKing (after a successful connection)
#[tokio::test(start_paused = true)]
async fn close_errors_if_all_packets_dropped() {
    let conn_config = ConnectionConfig::default();

    let ((mut send_link, send_cid), (recv_link, recv_cid)) = testutils::build_connected_pair();
    let tx_link_up = send_link.up_status();

    let recv = UtpSocket::with_socket(recv_link);
    let recv = Arc::new(recv);

    let send = UtpSocket::with_socket(send_link);
    let send = Arc::new(send);

    let recv_one = Arc::clone(&recv);
    let recv_one_handle = tokio::spawn(async move {
        recv_one
            .accept_with_cid(recv_cid, conn_config)
            .await
            .unwrap()
    });

    // Keep a clone of the socket so that it doesn't drop when moved into the task.
    // Dropping it causes all connections to exit.
    let send_one = Arc::clone(&send);
    let send_one_handle = tokio::spawn(async move {
        send_one
            .connect_with_cid(send_cid, conn_config)
            .await
            .unwrap()
    });

    let (tx_one, rx_one) = tokio::join!(send_one_handle, recv_one_handle,);
    let mut send_stream = tx_one.unwrap();
    let mut recv_stream = rx_one.unwrap();

    // ******* DISABLE NETWORK LINK ********
    tx_link_up.store(false, Ordering::SeqCst);

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
        let read_err = recv_stream.read_to_eof(&mut read_buf).await.unwrap_err();
        assert_eq!(read_err.kind(), ErrorKind::TimedOut);
    });

    // Wait for send to start
    let mut send_stream = send_stream_handle.await.unwrap();

    // Close stream, which will fail because network is disabled.
    match timeout(Duration::from_secs(20), send_stream.close()).await {
        Ok(Ok(_)) => panic!("Stream closed successfully, but should have timed out"),
        Ok(Err(e)) => {
            // The stream must time out when waiting to close, if the network is disabled.
            assert_eq!(e.kind(), ErrorKind::TimedOut);
        }
        Err(e) => panic!(
            "The stream did not timeout on close() fast enough, giving up after: {:?}",
            e
        ),
    };

    // Wait to confirm that the read will time out, also.
    recv_stream_handle.await.unwrap();
}
