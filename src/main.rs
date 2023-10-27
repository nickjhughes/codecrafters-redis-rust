use anyhow::Context;
use std::{
    io,
    net::{Ipv4Addr, SocketAddr, SocketAddrV4},
};
use tokio::{
    io::AsyncWriteExt,
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

async fn handle_connection(mut stream: TcpStream, socket: SocketAddr) {
    // stream
    //     .readable()
    //     .await
    //     .expect("could not wait for stream to be readable");

    eprintln!("New connection from {:?}", socket);

    let mut buf = [0; 1024];
    loop {
        match stream.try_read(&mut buf) {
            Ok(bytes_read) => {
                eprintln!("Read {} bytes from {:?}", bytes_read, socket);
                if bytes_read == 0 {
                    continue;
                }

                let request_str = std::str::from_utf8(&buf[0..bytes_read])
                    .context("request should be valid utf-8")
                    .unwrap();
                if let Ok(request) = Request::deserialize(request_str) {
                    eprintln!("Got request {:?} from {:?}", &request, socket);
                    if let Ok(response) = request.generate_response() {
                        stream
                            .write(response.serialize().as_bytes())
                            .await
                            .expect("failed to write to stream");
                        eprintln!("Sent response {:?} to {:?}", response, socket);
                    } else {
                        eprintln!("failed to generate a response to {:?}", request)
                    }
                } else {
                    eprintln!("failed to parse request {:?}", request_str)
                }
            }
            Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                continue;
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
        let (stream, socket) = listener.accept().await?;
        tokio::spawn(async move {
            handle_connection(stream, socket).await;
        });
    }
}
