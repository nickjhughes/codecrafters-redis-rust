use std::net::{Ipv4Addr, SocketAddrV4};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};

use resp::RespValue;

mod resp;

const ADDRESS: Ipv4Addr = Ipv4Addr::LOCALHOST;
const PORT: u16 = 6379;

#[derive(Debug)]
enum Request<'data> {
    Ping,
    Echo(&'data str),
}

impl<'data> Request<'data> {
    fn deserialize(data: &'data [u8]) -> anyhow::Result<Self> {
        if data.is_empty() {
            return Err(anyhow::format_err!("empty request"));
        }
        let (request_value, _) = RespValue::deserialize(data)?;
        match request_value {
            RespValue::Array(elements) => {
                if elements.is_empty() {
                    return Err(anyhow::format_err!("empty array request"));
                }
                match elements[0] {
                    RespValue::BulkString(s) => match s.to_lowercase().as_str() {
                        "ping" => {
                            return Ok(Request::Ping);
                        }
                        "echo" => {
                            if elements.len() == 2 {
                                return Ok(Request::Echo(s));
                            }
                        }
                        _ => {}
                    },
                    _ => {}
                }
            }
            _ => {}
        }
        Err(anyhow::format_err!("unhandled request"))
    }

    fn generate_response(&self) -> anyhow::Result<Response> {
        match self {
            Request::Ping => Ok(Response::Pong),
            Request::Echo(message) => Ok(Response::Echo(message)),
        }
    }
}

#[derive(Debug)]
enum Response<'data> {
    Pong,
    Echo(&'data str),
}

impl<'data> Response<'data> {
    fn serialize(&self) -> Vec<u8> {
        match self {
            Response::Pong => RespValue::SimpleString("PONG").serialize(),
            Response::Echo(s) => RespValue::Array(vec![RespValue::BulkString(s)]).serialize(),
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

                if let Ok(request) = Request::deserialize(&buf[0..bytes_read]) {
                    if let Ok(response) = request.generate_response() {
                        stream
                            .write_all(&response.serialize())
                            .await
                            .expect("failed to write to stream");
                    } else {
                        eprintln!("failed to generate a response");
                    }
                } else {
                    eprintln!("failed to parse request");
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
