use std::net::SocketAddr;

use utp::socket::UtpSocket;

#[tokio::test(flavor = "multi_thread")]
async fn socket() {
    let data = [0xef; 8192];

    let recv_addr = SocketAddr::from(([127, 0, 0, 1], 3400));
    let recv = UtpSocket::bind(recv_addr).await.unwrap();

    let recv_handle = tokio::spawn(async move {
        let mut stream = recv.accept().await.unwrap();
        let mut buf = vec![0; 8192];
        let n = stream.read_to_eof(&mut buf).await.unwrap();
        assert_eq!(n, data.len());
        assert_eq!(buf, data);
    });

    let send_addr = SocketAddr::from(([127, 0, 0, 1], 3401));
    let send = UtpSocket::bind(send_addr).await.unwrap();

    let mut stream = send.connect(recv_addr).await.unwrap();
    let n = stream.write(&data).await.unwrap();
    assert_eq!(n, data.len());

    let _ = stream.shutdown();

    recv_handle.await.unwrap();
}
