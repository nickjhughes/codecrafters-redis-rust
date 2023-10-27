use std::{
    net::{Ipv4Addr, SocketAddrV4},
    sync::{Arc, Mutex},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};

use memory::Memory;
use request::Request;

mod memory;
mod request;
mod resp_value;
mod response;

const ADDRESS: Ipv4Addr = Ipv4Addr::LOCALHOST;
const PORT: u16 = 6379;

async fn handle_connection(mut stream: TcpStream, memory: Arc<Mutex<Memory>>) {
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
                            .handle_request(&request)
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

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let memory = Arc::new(Mutex::new(Memory::default()));

    let listener = TcpListener::bind(SocketAddrV4::new(ADDRESS, PORT)).await?;
    loop {
        let (stream, _) = listener.accept().await?;
        let thread_memory = memory.clone();
        tokio::spawn(async move {
            handle_connection(stream, thread_memory).await;
        });
    }
}
