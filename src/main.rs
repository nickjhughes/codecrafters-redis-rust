use bytes::BytesMut;
use message::Message;
use std::{
    net::{Ipv4Addr, SocketAddrV4},
    sync::Arc,
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::{
        mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender},
        Mutex,
    },
    time::timeout,
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

#[derive(Debug)]
pub struct Connection {
    pub ty: ConnectionType,
    pub send_rdb: bool,
}

#[derive(Debug)]
pub enum ConnectionType {
    Client,
    Slave,
    Master,
}

async fn handle_connection(
    mut stream: TcpStream,
    state: Arc<Mutex<State>>,
    replica_senders: Arc<Mutex<Vec<UnboundedSender<Message>>>>,
    connection_type: ConnectionType,
) {
    let mut input_buf = [0; 512];
    let mut output_buf = BytesMut::with_capacity(512);

    let mut reciever: Option<UnboundedReceiver<Message>> = None;

    let mut connection = Connection {
        ty: connection_type,
        send_rdb: false,
    };

    loop {
        if let Some(message) = state.lock().await.next_outgoing(&mut connection).unwrap() {
            output_buf.clear();
            message.serialize(&mut output_buf);
            stream
                .write_all(&output_buf)
                .await
                .expect("failed to write to stream");
        }

        if let Some(reciever) = reciever.as_mut() {
            if let Ok(Some(message)) = timeout(Duration::ZERO, reciever.recv()).await {
                output_buf.clear();
                message.serialize(&mut output_buf);
                stream
                    .write_all(&output_buf)
                    .await
                    .expect("failed to write to stream");
            }
        }

        if let Ok(maybe_bytes_read) = timeout(Duration::ZERO, stream.read(&mut input_buf)).await {
            match maybe_bytes_read {
                Ok(bytes_read) => {
                    if bytes_read == 0 {
                        continue;
                    }

                    // TODO: Deal with incomplete frames of data

                    let mut input = &input_buf[0..bytes_read];
                    while !input.is_empty() {
                        output_buf.clear();
                        match Message::deserialize(input) {
                            Ok((message, remainder)) => {
                                input = remainder;
                                if let Some(response) = state
                                    .lock()
                                    .await
                                    .handle_incoming(&message, &mut connection)
                                    .unwrap_or_else(|_| {
                                        panic!("failed to handle message {:?}", message)
                                    })
                                {
                                    response.serialize(&mut output_buf);
                                    stream
                                        .write_all(&output_buf)
                                        .await
                                        .expect("failed to write to stream");
                                }

                                if state.lock().await.is_slave()
                                    && matches!(connection.ty, ConnectionType::Master)
                                    && !matches!(
                                        message,
                                        Message::DatabaseFile(_) | Message::FullResync { .. }
                                    )
                                {
                                    let mut msg_buf = BytesMut::new();
                                    message.serialize(&mut msg_buf);
                                    let message_len = msg_buf.len();
                                    state.lock().await.increment_offset(message_len);
                                }

                                if state.lock().await.is_master()
                                    && matches!(connection.ty, ConnectionType::Slave)
                                    && reciever.is_none()
                                {
                                    let (s, r) = unbounded_channel::<Message>();
                                    reciever = Some(r);
                                    replica_senders.lock().await.push(s);
                                }

                                if state.lock().await.is_master()
                                    && message.is_write_command()
                                    && matches!(connection.ty, ConnectionType::Client)
                                {
                                    for replica in replica_senders.lock().await.iter() {
                                        replica
                                            .send(message.clone())
                                            .expect("failed to propagate message to replica");
                                    }
                                }
                            }
                            Err(e) => {
                                RespValue::SimpleError(&format!("ERR {:?}", e))
                                    .serialize(&mut output_buf);
                                stream
                                    .write_all(&output_buf)
                                    .await
                                    .expect("failed to write to stream");
                                eprintln!("failed to deserialize request: {:?}", e)
                            }
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

    let replica_senders = Arc::new(Mutex::new(Vec::new()));

    if state.lock().await.is_slave() {
        let ip_addr = match replica_of.as_ref().unwrap()[0].as_str() {
            "localhost" => Ipv4Addr::new(127, 0, 0, 1),
            ip => ip.parse()?,
        };
        let master_address = SocketAddrV4::new(ip_addr, replica_of.as_ref().unwrap()[1].parse()?);
        let stream = TcpStream::connect(master_address).await?;
        let state = state.clone();
        let replica_senders = replica_senders.clone();
        tokio::spawn(async move {
            handle_connection(stream, state, replica_senders, ConnectionType::Master).await;
        });
    }

    let listener = TcpListener::bind(SocketAddrV4::new(ADDRESS, port)).await?;
    loop {
        let (stream, _) = listener.accept().await?;
        let state = state.clone();
        let replica_senders = replica_senders.clone();
        tokio::spawn(async move {
            handle_connection(stream, state, replica_senders, ConnectionType::Client).await;
        });
    }
}
