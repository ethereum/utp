# utp
A Rust library for the [uTorrent transport protocol (uTP)](https://www.bittorrent.org/beps/bep_0029.html).

## 🚧 WARNING: UNDER CONSTRUCTION 🚧
This library is currently unstable, with known issues. Use at your own discretion.

# Usage

A stream can transfer 1 MB. For larger transfers, data must be chunked to 1 MB chunks and each
chunk sent on its own stream.

```rust
use std::net::SocketAddr;

use utp_rs::conn::ConnectionConfig;
use utp_rs::socket::UtpSocket;
use utp_rs::udp::AsyncUdpSocket;

#[tokio::main]
fn main() {
	// bind a standard UDP socket. (transport is over a `tokio::net::UdpSocket`.)
	let socket_addr = SocketAddr::from(([127, 0, 0, 1], 3400));
	let udp_based_socket = UtpSocket::bind(socket_addr).await.unwrap();

	// bind a custom UDP socket. here we assume `CustomSocket` implements `AsyncUdpSocket`.
	let async_udp_socket = CustomSocket::new(..);
	let custom_socket = UtpSocket::with_socket(async_udp_socket).await.unwrap();

	// connect to a remote peer over uTP.
	let remote = SocketAddr::from(..);
	let config = ConnectionConfig::default();
	let mut stream = udp_socket::connect(remote, config).await.unwrap();

	// write data to the remote peer over the stream.
	let data = vec![0xef; 2048];
	let n = stream.write(data.as_slice()).await.unwrap();

	// accept a connection from a remote peer.
	let config = ConnectionConfig::default();
	let stream = udp_socket.accept(config).await;

	// read data from the remote peer until the peer indicates there is no data left to write.
	let mut data = vec![];
	let n = stream.read_to_eof(&mut data).await.unwrap();
}
```

