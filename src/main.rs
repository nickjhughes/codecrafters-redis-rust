use std::{
    collections::HashMap,
    net::{Ipv4Addr, SocketAddrV4},
    sync::{Arc, Mutex},
};
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
    CommandDocs,
    Ping,
    Echo(&'data str),
    Set { key: &'data str, value: &'data str },
    Get(&'data str),
}

impl<'data> Request<'data> {
    fn deserialize(data: &'data [u8]) -> anyhow::Result<Self> {
        if data.is_empty() {
            return Err(anyhow::format_err!("empty request"));
        }
        let (request_value, _) = RespValue::deserialize(data)?;
        match request_value {
            RespValue::Array(elements) => match elements.get(0) {
                Some(RespValue::BulkString(s)) => match s.to_lowercase().as_str() {
                    "ping" => Ok(Request::Ping),
                    "echo" => match elements.get(1) {
                        Some(RespValue::BulkString(s)) => Ok(Request::Echo(s)),
                        _ => Err(anyhow::format_err!("malformed ECHO command")),
                    },
                    "command" => match elements.get(1) {
                        Some(RespValue::BulkString(s)) => match s.to_lowercase().as_str() {
                            "docs" => Ok(Request::CommandDocs),
                            _ => Err(anyhow::format_err!("malformed COMMAND DOCS command")),
                        },
                        _ => Err(anyhow::format_err!("malformed COMMAND DOCS command")),
                    },
                    "set" => {
                        let key = match elements.get(1) {
                            Some(RespValue::BulkString(s)) => *s,
                            _ => return Err(anyhow::format_err!("malformed SET command")),
                        };
                        let value = match elements.get(2) {
                            Some(RespValue::BulkString(s)) => *s,
                            _ => return Err(anyhow::format_err!("malformed SET command")),
                        };
                        Ok(Request::Set { key, value })
                    }
                    "get" => {
                        let key = match elements.get(1) {
                            Some(RespValue::BulkString(s)) => *s,
                            _ => return Err(anyhow::format_err!("malformed GET command")),
                        };
                        Ok(Request::Get(key))
                    }
                    command => Err(anyhow::format_err!(
                        "unhandled command {:?}",
                        command.to_uppercase()
                    )),
                },
                _ => Err(anyhow::format_err!(
                    "requests must start with a bulk string"
                )),
            },
            _ => Err(anyhow::format_err!("requests must be arrays")),
        }
    }
}

#[derive(Default)]
struct Memory {
    data: HashMap<String, String>,
}

impl Memory {
    fn handle_request<'request, 'memory>(
        &'memory mut self,
        request: &'request Request,
    ) -> anyhow::Result<Response<'request, 'memory>> {
        match request {
            Request::Ping => Ok(Response::Pong),
            Request::Echo(message) => Ok(Response::Echo(message)),
            Request::CommandDocs => Ok(Response::CommandDocs),
            Request::Set { key, value } => {
                self.data.insert(key.to_string(), value.to_string());
                Ok(Response::Set(SetResponse::Ok))
            }
            Request::Get(key) => match self.data.get(*key) {
                Some(value) => Ok(Response::Get(GetResponse::Found(value))),
                None => Ok(Response::Get(GetResponse::NotFound)),
            },
        }
    }
}

#[derive(Debug)]
enum Response<'request, 'memory> {
    CommandDocs,
    Pong,
    Echo(&'request str),
    Set(SetResponse),
    Get(GetResponse<'memory>),
}

#[derive(Debug)]
enum GetResponse<'memory> {
    Found(&'memory str),
    NotFound,
}

#[derive(Debug)]
enum SetResponse {
    Ok,
}

impl<'request, 'memory> Response<'request, 'memory> {
    fn serialize(&self) -> Vec<u8> {
        match self {
            Response::Pong => RespValue::SimpleString("PONG"),
            Response::Echo(s) => RespValue::BulkString(s),
            Response::CommandDocs => RespValue::Array(vec![]),
            Response::Set(set_response) => match set_response {
                SetResponse::Ok => RespValue::SimpleString("OK"),
            },
            Response::Get(get_response) => match get_response {
                GetResponse::Found(value) => RespValue::BulkString(value),
                GetResponse::NotFound => RespValue::Null,
            },
        }
        .serialize()
    }
}

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
                            .unwrap()
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
