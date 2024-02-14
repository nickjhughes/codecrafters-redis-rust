use bytes::BytesMut;
use message::Message;
use std::{
    net::{Ipv4Addr, SocketAddrV4},
    sync::Arc,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::Mutex,
};

use config::{Config, ConfigKey};
use resp_value::RespValue;
use state::State;

mod config;
mod message;
mod rdb;
mod resp_value;
mod state;
mod store;

const ADDRESS: Ipv4Addr = Ipv4Addr::LOCALHOST;
const DEFAULT_PORT: u16 = 6379;
const REPLICATION_ID: &str = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb";

async fn handle_connection(mut stream: TcpStream, state: Arc<Mutex<State>>) {
    let mut input_buf = [0; 512];
    let mut output_buf = BytesMut::with_capacity(512);
    loop {
        if let Some(message) = state.lock().await.next_outgoing().unwrap() {
            output_buf.clear();
            message.serialize(&mut output_buf);
            stream
                .write_all(&output_buf)
                .await
                .expect("failed to write to stream");
        }

        match stream.read(&mut input_buf).await {
            Ok(bytes_read) => {
                if bytes_read == 0 {
                    continue;
                }

                // TODO: Deal with incomplete frames of data

                output_buf.clear();
                match Message::deserialize(&input_buf[0..bytes_read]) {
                    Ok(message) => {
                        if let Some(response) = state
                            .lock()
                            .await
                            .handle_incoming(&message)
                            .unwrap_or_else(|_| panic!("failed to handle message {:?}", message))
                        {
                            response.serialize(&mut output_buf);
                            stream
                                .write_all(&output_buf)
                                .await
                                .expect("failed to write to stream");
                        }
                    }
                    Err(e) => {
                        RespValue::SimpleError(&format!("ERR {:?}", e)).serialize(&mut output_buf);
                        stream
                            .write_all(&output_buf)
                            .await
                            .expect("failed to write to stream");
                        eprintln!("failed to deserialize request: {:?}", e)
                    }
                }
            }
            Err(e) => {
                eprintln!("stream read error: {:?}", e);
                break;
            }
        }
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let config = Config::parse(std::env::args())?;
    let port = config
        .0
        .get(&ConfigKey::Port)
        .map(|s| {
            s[0].parse::<u16>()
                .unwrap_or_else(|_| panic!("invalid port {:?}", s))
        })
        .unwrap_or(DEFAULT_PORT);
    let replica_of = config.0.get(&ConfigKey::ReplicaOf).cloned();
    let state = Arc::new(Mutex::new(State::new(config)?));

    if state.lock().await.is_slave() {
        let ip_addr = match replica_of.as_ref().unwrap()[0].as_str() {
            "localhost" => Ipv4Addr::new(127, 0, 0, 1),
            ip => ip.parse()?,
        };
        let master_address = SocketAddrV4::new(ip_addr, replica_of.as_ref().unwrap()[1].parse()?);
        let stream = TcpStream::connect(master_address).await?;
        let state = state.clone();
        tokio::spawn(async move {
            handle_connection(stream, state).await;
        });
    }

    let listener = TcpListener::bind(SocketAddrV4::new(ADDRESS, port)).await?;
    loop {
        let (stream, _) = listener.accept().await?;
        let state = state.clone();
        tokio::spawn(async move {
            handle_connection(stream, state).await;
        });
    }
}
