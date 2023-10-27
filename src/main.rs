use anyhow::Context;
use std::{
    io::{Read, Write},
    net::{Ipv4Addr, SocketAddrV4, TcpListener, TcpStream},
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

fn handle_connection(mut stream: TcpStream) -> anyhow::Result<()> {
    let mut buf = [0u8; 1024];
    let bytes_read = stream.read(&mut buf)?;

    let request_str = std::str::from_utf8(&buf[0..bytes_read])
        .context("request should be valid utf-8")
        .unwrap();
    if let Ok(request) = Request::deserialize(request_str) {
        if let Ok(response) = request.generate_response() {
            stream.write_all(response.serialize().as_bytes())?;
        } else {
            eprintln!("failed to generate a response to {:?}", request)
        }
    } else {
        eprintln!("failed to parse request {:?}", request_str)
    }

    Ok(())
}

fn main() -> anyhow::Result<()> {
    let listener = TcpListener::bind(SocketAddrV4::new(ADDRESS, PORT))?;
    for stream in listener.incoming() {
        handle_connection(stream?)?;
    }
    Ok(())
}
