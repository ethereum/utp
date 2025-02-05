use std::env;
use std::net::SocketAddr;
use clap::{arg, Args, Parser, Subcommand};
use utp_rs::conn::ConnectionConfig;
use utp_rs::socket::UtpSocket;
use utp_rs::udp::AsyncUdpSocket;
use std::sync::Arc;
use std::io::Write;
use std::time::Duration;
use utp_rs::cid;

#[derive(Parser, Debug, PartialEq, Clone)]
#[command()]
pub struct TestParams {
    #[command(subcommand)]
    pub command: Option<Commands>,
}

#[derive(Subcommand, Debug, Clone, PartialEq)]
#[allow(clippy::enum_variant_names)]
pub enum Commands {
    Send,
    Recv,
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    let config = TestParams::try_parse_from(env::args_os()).unwrap();

    match config.command {
        Some(comm) => match comm {
            Commands::Send => sender().await,
            Commands::Recv => receiver().await,
        },
        None => panic!("no command given"),
    }
}

async fn sender() {
    let recv_addr = SocketAddr::from(([127, 0, 0, 1], 9078));
    let send_addr = SocketAddr::from(([127, 0, 0, 1], 9077));

    let send = UtpSocket::bind(send_addr).await.unwrap();
    let send = Arc::new(send);

    // start 50 transfers, step by two to avoid cid collisions
    let conn_config = ConnectionConfig::default();

    let data = "Hello from nim implementation".as_bytes();
    print!("hi {:?}",data.clone().to_vec());
    std::io::stdout().flush().unwrap();


    let send_handle = tokio::spawn(async move {

        let mut stream = send.connect(recv_addr, conn_config).await.expect("buckets");

        let n = stream.write(&data).await.unwrap();
        //assert_eq!(send_handle.unwrap(), data.len());
        print!("hi");

        //stream.shutdown().unwrap();

    });

    send_handle.await;

    loop {
    }
}

async fn receiver() {
    let conn_config = ConnectionConfig::default();
    let recv_addr = SocketAddr::from(([127, 0, 0, 1], 9078));
    let recv = UtpSocket::bind(recv_addr).await.unwrap();
    let recv = Arc::new(recv);

    let recv_arc = Arc::clone(&recv);
    let recv_one_handle = tokio::spawn(async move {
        let mut stream = recv_arc
            .accept(conn_config)
            .await
            .unwrap();
        let mut buf = vec![];
        let n = stream.read_to_eof(&mut buf).await.unwrap();
        print!("\n\n\nsize: {} :: buf {:?}\n\n\n", n, buf)
    });

    recv_one_handle.await;

    loop {
    }
}