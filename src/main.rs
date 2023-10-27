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
use memory::Memory;
use request::Request;

mod config;
mod memory;
mod request;
mod resp_value;
mod response;

const ADDRESS: Ipv4Addr = Ipv4Addr::LOCALHOST;
const PORT: u16 = 6379;

async fn handle_connection(mut stream: TcpStream, memory: Arc<Mutex<Memory>>, config: Arc<Config>) {
    let mut buf = [0; 512];
    loop {
        match stream.read(&mut buf).await {
            Ok(bytes_read) => {
                if bytes_read == 0 {
                    continue;
                }

                match Request::deserialize(&buf[0..bytes_read]) {
                    Ok(request) => {
                        let response = memory
                            .lock()
                            .expect("failed to get lock")
                            .handle_request(&request, config.clone())
                            .unwrap_or_else(|_| panic!("failed to handle request {:?}", request))
                            .serialize();
                        stream
                            .write_all(&response)
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
    let config = Arc::new(parse_args(std::env::args())?);
    let memory = Arc::new(Mutex::new(Memory::default()));

    let listener = TcpListener::bind(SocketAddrV4::new(ADDRESS, PORT)).await?;
    loop {
        let (stream, _) = listener.accept().await?;
        let thread_memory = memory.clone();
        let thread_config = config.clone();
        tokio::spawn(async move {
            handle_connection(stream, thread_memory, thread_config).await;
        });
    }
}
