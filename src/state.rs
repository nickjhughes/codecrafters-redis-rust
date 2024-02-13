use std::{
    path::PathBuf,
    time::{Instant, SystemTime, UNIX_EPOCH},
};

use crate::{
    client_request::{ClientRequest, SetRequest},
    client_response::{ClientResponse, ConfigGetResponse, GetResponse, SetResponse},
    config::{Config, Parameter},
    master_response::MasterResponse,
    rdb::read_rdb_file,
    resp_value::RespValue,
    slave_request::SlaveRequest,
    store::{Store, StoreExpiry, StoreValue},
    REPLICATION_ID,
};

pub struct State {
    store: Store,
    config: Config,
    role_state: RoleState,
}

enum RoleState {
    Slave(SlaveState),
    Master(MasterState),
}

#[allow(dead_code)]
#[derive(Default)]
struct SlaveState {
    handshake_state: HandshakeState,
}

#[derive(Default)]
#[allow(dead_code)]
enum HandshakeState {
    #[default]
    Init,
    PingSent,
    PongRcvd,
    ReplConf1Sent,
    ReplConf1Rcvd,
    ReplConf2Sent,
    ReplConf2Rcvd,
    PSyncSent,
    Complete,
}

struct MasterState {
    replication_id: String,
    replication_offset: usize,
}

impl Default for MasterState {
    fn default() -> Self {
        MasterState {
            replication_id: REPLICATION_ID.into(),
            replication_offset: 0,
        }
    }
}

impl State {
    pub fn new(config: Config) -> anyhow::Result<Self> {
        let store = if config.0.contains_key(&Parameter::Dir)
            && config.0.contains_key(&Parameter::DbFilename)
        {
            let path = {
                let mut p = PathBuf::new();
                p.push(config.0.get(&Parameter::Dir).unwrap()[0].clone());
                p.push(config.0.get(&Parameter::DbFilename).unwrap()[0].clone());
                p
            };
            if path.exists() {
                read_rdb_file(path)?
            } else {
                eprintln!("warning: database file {:?} doesn't exist", path);
                Store::default()
            }
        } else {
            Store::default()
        };

        let role_state = if config.0.contains_key(&Parameter::ReplicaOf) {
            RoleState::Slave(SlaveState::default())
        } else {
            RoleState::Master(MasterState::default())
        };

        Ok(State {
            store,
            config,
            role_state,
        })
    }

    pub fn is_slave(&self) -> bool {
        matches!(self.role_state, RoleState::Slave(_))
    }

    pub fn handle_response(&mut self, response: &MasterResponse) {
        match &mut self.role_state {
            RoleState::Slave(slave_state) => {
                if matches!(slave_state.handshake_state, HandshakeState::PingSent)
                    && matches!(response, MasterResponse::Pong)
                {
                    slave_state.handshake_state = HandshakeState::PongRcvd;
                } else if matches!(slave_state.handshake_state, HandshakeState::ReplConf1Sent)
                    && matches!(response, MasterResponse::Ok)
                {
                    slave_state.handshake_state = HandshakeState::ReplConf1Rcvd;
                } else if matches!(slave_state.handshake_state, HandshakeState::ReplConf2Sent)
                    && matches!(response, MasterResponse::Ok)
                {
                    slave_state.handshake_state = HandshakeState::ReplConf2Rcvd;
                } else if matches!(slave_state.handshake_state, HandshakeState::PSyncSent)
                    && matches!(response, MasterResponse::Ok)
                {
                    slave_state.handshake_state = HandshakeState::Complete;
                }
            }
            RoleState::Master(_) => {}
        }
    }

    pub fn next_request(&mut self) -> anyhow::Result<Option<SlaveRequest>> {
        Ok(match &mut self.role_state {
            RoleState::Slave(slave_state) => match slave_state.handshake_state {
                HandshakeState::Init => {
                    slave_state.handshake_state = HandshakeState::PingSent;
                    Some(SlaveRequest::Ping)
                }
                HandshakeState::PongRcvd => {
                    slave_state.handshake_state = HandshakeState::ReplConf1Sent;
                    Some(SlaveRequest::ReplConf(vec![
                        "listening-port".into(),
                        self.config.0.get(&Parameter::Port).unwrap()[0].to_string(),
                    ]))
                }
                HandshakeState::ReplConf1Rcvd => {
                    slave_state.handshake_state = HandshakeState::ReplConf2Sent;
                    Some(SlaveRequest::ReplConf(vec!["capa".into(), "psync2".into()]))
                }
                HandshakeState::ReplConf2Rcvd => {
                    slave_state.handshake_state = HandshakeState::PSyncSent;
                    Some(SlaveRequest::PSync {
                        replication_id: "?".into(),
                        offset: -1,
                    })
                }
                _ => None,
            },
            RoleState::Master(_) => None,
        })
    }

    pub fn handle_request<'request, 'state>(
        &'state mut self,
        request: &'request ClientRequest,
    ) -> anyhow::Result<ClientResponse<'request, 'state>> {
        match request {
            ClientRequest::Ping => Ok(ClientResponse::Pong),
            ClientRequest::Echo(message) => Ok(ClientResponse::Echo(message)),
            ClientRequest::CommandDocs => Ok(ClientResponse::CommandDocs),
            ClientRequest::Set(SetRequest { key, value, expiry }) => {
                let value = StoreValue {
                    data: value.to_string(),
                    updated: Instant::now(),
                    expiry: expiry.map(StoreExpiry::Duration),
                };
                self.store.data.insert(key.to_string(), value);
                Ok(ClientResponse::Set(SetResponse::Ok))
            }
            ClientRequest::Get(key) => match self.store.data.get(*key) {
                Some(value) => {
                    match value.expiry {
                        Some(StoreExpiry::Duration(d)) => {
                            if Instant::now() > value.updated + d {
                                // Key has expired
                                Ok(ClientResponse::Get(GetResponse::NotFound))
                            } else {
                                Ok(ClientResponse::Get(GetResponse::Found(&value.data)))
                            }
                        }
                        Some(StoreExpiry::UnixTimestampMillis(t)) => {
                            let unix_time =
                                SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis() as u64;
                            if t < unix_time {
                                // Key has expired
                                Ok(ClientResponse::Get(GetResponse::NotFound))
                            } else {
                                Ok(ClientResponse::Get(GetResponse::Found(&value.data)))
                            }
                        }
                        None => Ok(ClientResponse::Get(GetResponse::Found(&value.data))),
                    }
                }
                None => Ok(ClientResponse::Get(GetResponse::NotFound)),
            },
            ClientRequest::ConfigGet(parameter) => match self.config.0.get(parameter) {
                Some(values) => Ok(ClientResponse::ConfigGet(Some(ConfigGetResponse {
                    parameter: *parameter,
                    values,
                }))),
                None => Ok(ClientResponse::ConfigGet(None)),
            },
            ClientRequest::Keys => {
                let keys = self
                    .store
                    .data
                    .keys()
                    .map(|k| RespValue::BulkString(k))
                    .collect();
                Ok(ClientResponse::Keys(keys))
            }
            ClientRequest::Info(sections) => {
                if sections.is_empty() || sections.contains(&"replication") {
                    let mut values: Vec<String> = Vec::new();
                    values.push(format!("role:{}", self.role_state));
                    if let RoleState::Master(master_state) = &self.role_state {
                        values.push(format!("master_replid:{}", master_state.replication_id));
                        values.push(format!(
                            "master_repl_offset:{}",
                            master_state.replication_offset
                        ));
                    }
                    Ok(ClientResponse::Info(RespValue::OwnedBulkString(
                        values.join("\n"),
                    )))
                } else {
                    Ok(ClientResponse::Info(RespValue::NullBulkString))
                }
            }
        }
    }
}

impl std::fmt::Display for RoleState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RoleState::Master(_) => write!(f, "master"),
            RoleState::Slave(_) => write!(f, "slave"),
        }
    }
}
