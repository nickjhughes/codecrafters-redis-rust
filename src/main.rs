use bytes::BytesMut;
use std::{
    net::{Ipv4Addr, SocketAddrV4},
    sync::{Arc, Mutex},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};

use config::{Config, Parameter};
use request::Request;
use resp_value::RespValue;
use state::State;

mod config;
mod rdb;
mod request;
mod resp_value;
mod response;
mod state;
mod store;

const ADDRESS: Ipv4Addr = Ipv4Addr::LOCALHOST;
const DEFAULT_PORT: u16 = 6379;
const REPLICATION_ID: &str = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb";

async fn handle_connection(mut stream: TcpStream, state: Arc<Mutex<State>>) {
    let mut input_buf = [0; 512];
    let mut output_buf = BytesMut::with_capacity(512);
    loop {
        match stream.read(&mut input_buf).await {
            Ok(bytes_read) => {
                if bytes_read == 0 {
                    continue;
                }

                // TODO: Deal with incomplete frames of data

                output_buf.clear();
                match Request::deserialize(&input_buf[0..bytes_read]) {
                    Ok(request) => {
                        state
                            .lock()
                            .expect("failed to get lock")
                            .handle_request(&request)
                            .unwrap_or_else(|_| panic!("failed to handle request {:?}", request))
                            .serialize(&mut output_buf);
                        stream
                            .write_all(&output_buf)
                            .await
                            .expect("failed to write to stream");
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
    let args = Config::parse(std::env::args())?;
    let port = args
        .0
        .get(&Parameter::Port)
        .map(|s| {
            s[0].parse::<u16>()
                .unwrap_or_else(|_| panic!("invalid port {:?}", s))
        })
        .unwrap_or(DEFAULT_PORT);
    let state = Arc::new(Mutex::new(State::new(args)?));
    let listener = TcpListener::bind(SocketAddrV4::new(ADDRESS, port)).await?;
    loop {
        let (stream, _) = listener.accept().await?;
        let state = state.clone();
        tokio::spawn(async move {
            handle_connection(stream, state).await;
        });
    }
}
