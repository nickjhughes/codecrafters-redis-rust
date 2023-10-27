use bytes::BytesMut;
use std::{
    env::Args,
    net::{Ipv4Addr, SocketAddrV4},
    sync::{Arc, Mutex},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};

use config::{Config, Parameter};
use request::Request;
use state::State;

mod config;
mod rdb;
mod request;
mod resp_value;
mod response;
mod state;
mod store;

const ADDRESS: Ipv4Addr = Ipv4Addr::LOCALHOST;
const PORT: u16 = 6379;

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

                match Request::deserialize(&input_buf[0..bytes_read]) {
                    Ok(request) => {
                        output_buf.clear();
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
                    Err(e) => eprintln!("failed to deserialize request: {:?}", e),
                }
            }
            Err(e) => {
                eprintln!("stream read error: {:?}", e);
                break;
            }
        }
    }
}

/// Load config from command line arguments
fn parse_args(args: Args) -> anyhow::Result<Config> {
    let args = args.skip(1);

    let mut config = Config::default();
    let mut current_key = None;
    for arg in args {
        if let Some(current_key) = current_key.take() {
            config.0.insert(current_key, arg);
        } else if arg.starts_with("--") {
            current_key = Some(Parameter::deserialize(arg.strip_prefix("--").unwrap())?);
        } else {
            anyhow::bail!("invalid argument {:?}", arg)
        }
    }
    Ok(config)
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let state = Arc::new(Mutex::new(State::new(parse_args(std::env::args())?)));

    let listener = TcpListener::bind(SocketAddrV4::new(ADDRESS, PORT)).await?;
    loop {
        let (stream, _) = listener.accept().await?;
        let state = state.clone();
        tokio::spawn(async move {
            handle_connection(stream, state).await;
        });
    }
}
