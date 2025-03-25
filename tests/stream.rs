use std::io::ErrorKind;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use tokio::time::timeout;

use utp_rs::conn::{ConnectionConfig, DEFAULT_MAX_IDLE_TIMEOUT};
use utp_rs::peer::Peer;
use utp_rs::socket::UtpSocket;

use utp_rs::testutils;

// How long should tests expect the connection to wait before timing out due to inactivity?
const EXPECTED_IDLE_TIMEOUT: Duration = DEFAULT_MAX_IDLE_TIMEOUT;

// Test that close() returns successful, after transfer is complete
#[tokio::test]
async fn close_is_successful_when_write_completes() {
    let conn_config = ConnectionConfig::default();

    let ((send_socket, send_cid), (recv_socket, recv_cid)) =
        testutils::build_manually_linked_pair();

    let recv = UtpSocket::with_socket(recv_socket);
    let recv = Arc::new(recv);

    let send = UtpSocket::with_socket(send_socket);
    let send = Arc::new(send);

    let recv_one = Arc::clone(&recv);
    let recv_one_handle = tokio::spawn(async move {
        recv_one
            .accept_with_cid(recv_cid, Peer::new_id(recv_cid.peer_id), conn_config)
            .await
            .unwrap()
    });

    // Keep a clone of the socket so that it doesn't drop when moved into the task.
    // Dropping it causes all connections to exit.
    let send_one = Arc::clone(&send);
    let send_one_handle = tokio::spawn(async move {
        send_one
            .connect_with_cid(send_cid, Peer::new_id(send_cid.peer_id), conn_config)
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

    let ((send_socket, send_cid), (recv_socket, recv_cid)) =
        testutils::build_manually_linked_pair();
    let tx_link_switch = send_socket.link.up_switch.clone();

    let recv = UtpSocket::with_socket(recv_socket);
    let recv = Arc::new(recv);

    let send = UtpSocket::with_socket(send_socket);
    let send = Arc::new(send);

    let recv_one = Arc::clone(&recv);
    let recv_one_handle = tokio::spawn(async move {
        recv_one
            .accept_with_cid(recv_cid, Peer::new_id(recv_cid.peer_id), conn_config)
            .await
            .unwrap()
    });

    // Keep a clone of the socket so that it doesn't drop when moved into the task.
    // Dropping it causes all connections to exit.
    let send_one = Arc::clone(&send);
    let send_one_handle = tokio::spawn(async move {
        send_one
            .connect_with_cid(send_cid, Peer::new_id(send_cid.peer_id), conn_config)
            .await
            .unwrap()
    });

    let (tx_one, rx_one) = tokio::join!(send_one_handle, recv_one_handle,);
    let mut send_stream = tx_one.unwrap();
    let mut recv_stream = rx_one.unwrap();

    // ******* DISABLE NETWORK LINK ********
    tx_link_switch.store(false, Ordering::SeqCst);

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
    match timeout(EXPECTED_IDLE_TIMEOUT * 2, send_stream.close()).await {
        Ok(Ok(_)) => panic!("Stream closed successfully, but should have timed out"),
        Ok(Err(e)) => {
            // The stream must time out when waiting to close, if the network is disabled.
            assert_eq!(e.kind(), ErrorKind::TimedOut);
        }
        Err(e) => {
            panic!("The stream did not timeout on close() fast enough, giving up after: {e:?}")
        }
    };

    // Wait to confirm that the read will time out, also.
    recv_stream_handle.await.unwrap();
}

// Test that close() succeeds, if the connection is only missing the FIN-ACK
#[tokio::test(start_paused = true)]
async fn close_succeeds_if_only_fin_ack_dropped() {
    let conn_config = ConnectionConfig::default();

    let ((send_socket, send_cid), (recv_socket, recv_cid)) =
        testutils::build_manually_linked_pair();
    let rx_link_switch = recv_socket.link.up_switch.clone();

    let recv = UtpSocket::with_socket(recv_socket);
    let recv = Arc::new(recv);

    let send = UtpSocket::with_socket(send_socket);
    let send = Arc::new(send);

    let recv_one = Arc::clone(&recv);
    let recv_one_handle = tokio::spawn(async move {
        recv_one
            .accept_with_cid(recv_cid, Peer::new_id(recv_cid.peer_id), conn_config)
            .await
            .unwrap()
    });

    // Keep a clone of the socket so that it doesn't drop when moved into the task.
    // Dropping it causes all connections to exit.
    let send_one = Arc::clone(&send);
    let send_one_handle = tokio::spawn(async move {
        send_one
            .connect_with_cid(send_cid, Peer::new_id(send_cid.peer_id), conn_config)
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
        recv_stream
    });

    // Wait for send to start
    let mut send_stream = send_stream_handle.await.unwrap();

    // Wait for the full data to be sent before dropping the link
    // This is a timeless sleep, because tokio time is paused
    tokio::time::sleep(EXPECTED_IDLE_TIMEOUT / 2).await;

    // ******* DISABLE NETWORK LINK ********
    // This only drops the connection from the recipient to the sender, leading to the following
    // scenario:
    //  - Sender sends FIN
    //  - Recipient receives FIN, sends FIN-ACK and its own FIN
    //  - Sender receives nothing, because link is down
    //  - Recipient is only missing its inbound FIN-ACK and closes with success
    //  - Sender is missing the recipient's FIN and times out with failure
    rx_link_switch.store(false, Ordering::SeqCst);

    match timeout(EXPECTED_IDLE_TIMEOUT * 2, send_stream.close()).await {
        Ok(Ok(_)) => panic!("Send stream closed successfully, but should have timed out"),
        Ok(Err(e)) => {
            // The stream must time out when waiting to close, because recipient's FIN is missing
            assert_eq!(e.kind(), ErrorKind::TimedOut);
        }
        Err(e) => {
            panic!("The send stream did not timeout on close() fast enough, giving up after: {e:?}")
        }
    };

    let mut recv_stream = recv_stream_handle.await.unwrap();

    // Since switching to one-way FIN-ACK, closing after reading is not allowed. We only explicitly
    // close after write() now, and close after reading should error.
    match timeout(EXPECTED_IDLE_TIMEOUT * 2, recv_stream.close()).await {
        Ok(Ok(_)) => panic!("Closing after reading should have errored, but succeeded"),
        Ok(Err(e)) => {
            // The stream will already be disconnected by the read_to_eof() call, so we expect a
            // NotConnected error here.
            assert_eq!(e.kind(), ErrorKind::NotConnected);
        }
        Err(e) => {
            panic!("The recv stream did not timeout on close() fast enough, giving up after: {e:?}")
        }
    };
}

// Test that data is delivered successfully, even if the original SYN-STATE is dropped
//
// At the time of writing, a bug in this scenario causes the first bytes (2048 of them) to be
// silently lost. The recipient thinks it received everything, but is missing the first bytes of
// the transfer. When the recipient, who started the connection, times out the original SYN, it
// resends. The sender has already sent some data. When the bug is active, the resent STATE
// packet in response to the SYN uses the sequence number after incrementing from the previously
// sent state data. This causes the recipient to ignore all data sent previously.
#[tokio::test(start_paused = true)]
async fn test_data_valid_when_resending_syn_state_response() {
    let _ = tracing_subscriber::fmt::try_init();

    let conn_config = ConnectionConfig::default();

    let ((connector_socket, connector_cid), (acceptor_socket, acceptor_cid)) =
        testutils::build_link_drops_first_n_sent_pair(1);

    let acceptor = UtpSocket::with_socket(acceptor_socket);
    let acceptor = Arc::new(acceptor);

    let connector = UtpSocket::with_socket(connector_socket);
    let connector = Arc::new(connector);

    // It's important for this scenario that the data recipient is the one creating the connection.
    let acceptor_one = Arc::clone(&acceptor);
    let acceptor_one_handle = tokio::spawn(async move {
        acceptor_one
            .accept_with_cid(
                acceptor_cid,
                Peer::new_id(acceptor_cid.peer_id),
                conn_config,
            )
            .await
            .unwrap()
    });

    // Keep a clone of the socket so that it doesn't drop when moved into the task.
    // Dropping it causes all connections to exit.
    let connector_one = Arc::clone(&connector);
    let connector_one_handle = tokio::spawn(async move {
        connector_one
            .connect_with_cid(
                connector_cid,
                Peer::new_id(connector_cid.peer_id),
                conn_config,
            )
            .await
            .unwrap()
    });

    let mut acceptor_stream = acceptor_one_handle.await.unwrap();
    tracing::debug!("Acceptor stream established");
    // Must not wait for connection to complete before writing data here, so that we can trigger
    // the bug.

    // data to send, must be longer than 2048 bytes, which are lost in the bug scenario
    const DATA_LEN: usize = 9000;
    let data = [0xa5; DATA_LEN];

    // send data
    let acceptor_stream_handle = tokio::spawn(async move {
        match acceptor_stream.write(&data).await {
            Ok(written_len) => assert_eq!(written_len, DATA_LEN),
            Err(err) => panic!("Error sending data: {err:?}"),
        };
        acceptor_stream
    });

    // Finally, we can wait for the connection to complete. If we await this any earlier in the
    // test, then the data won't be sent, and we don't trigger the bug scenario.
    let mut connector_stream = connector_one_handle.await.unwrap();

    // Test that complete data is received
    let connector_stream_handle = tokio::spawn(async move {
        let mut read_buf = vec![];
        let _ = connector_stream.read_to_eof(&mut read_buf).await.unwrap();
        assert_eq!(read_buf.len(), data.len());
        assert_eq!(read_buf, data.to_vec());
        connector_stream
    });

    // Complete the streams
    let mut acceptor_stream = acceptor_stream_handle.await.unwrap();
    acceptor_stream.close().await.unwrap();
    connector_stream_handle.await.unwrap();
}
