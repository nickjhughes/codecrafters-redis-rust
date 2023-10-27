use anyhow::Context;
use std::net::{Ipv4Addr, SocketAddrV4};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};

const ADDRESS: Ipv4Addr = Ipv4Addr::LOCALHOST;
const PORT: u16 = 6379;

#[derive(Debug)]
enum Request {
    Ping,
}

impl Request {
    fn deserialize(buf: &str) -> anyhow::Result<Self> {
        match buf {
            "*1\r\n$4\r\nping\r\n" => Ok(Request::Ping),
            _ => Err(anyhow::format_err!("unsupported request: {:?}", buf)),
        }
    }

    fn generate_response(&self) -> anyhow::Result<Response> {
        match self {
            Request::Ping => Ok(Response::Pong),
        }
    }
}

#[derive(Debug)]
enum Response {
    Pong,
}

impl Response {
    fn serialize(&self) -> String {
        match self {
            Response::Pong => "+PONG\r\n".into(),
        }
    }
}

async fn handle_connection(mut stream: TcpStream) {
    let mut buf = [0; 512];
    loop {
        match stream.read(&mut buf).await {
            Ok(bytes_read) => {
                if bytes_read == 0 {
                    continue;
                }

                let request_str = std::str::from_utf8(&buf[0..bytes_read])
                    .context("request should be valid utf-8")
                    .unwrap();
                if let Ok(request) = Request::deserialize(request_str) {
                    if let Ok(response) = request.generate_response() {
                        let _ = stream
                            .write_all(response.serialize().as_bytes())
                            .await
                            .expect("failed to write to stream");
                    } else {
                        eprintln!("failed to generate a response to {:?}", request)
                    }
                } else {
                    eprintln!("failed to parse request {:?}", request_str)
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
    let listener = TcpListener::bind(SocketAddrV4::new(ADDRESS, PORT)).await?;

    loop {
        let (stream, _) = listener.accept().await?;
        tokio::spawn(async move {
            handle_connection(stream).await;
        });
    }
}
