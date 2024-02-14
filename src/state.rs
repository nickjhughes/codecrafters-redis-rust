use std::{
    collections::HashMap,
    path::PathBuf,
    time::{Instant, SystemTime, UNIX_EPOCH},
};

use crate::{
    config::{Config, ConfigKey},
    message::{ConfigGetResponse, GetResponse, Message},
    rdb::read_rdb_file,
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
        let store = if config.0.contains_key(&ConfigKey::Dir)
            && config.0.contains_key(&ConfigKey::DbFilename)
        {
            let path = {
                let mut p = PathBuf::new();
                p.push(config.0.get(&ConfigKey::Dir).unwrap()[0].clone());
                p.push(config.0.get(&ConfigKey::DbFilename).unwrap()[0].clone());
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

        let role_state = if config.0.contains_key(&ConfigKey::ReplicaOf) {
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

    pub fn handle_response(&mut self, response: &Message) {
        match &mut self.role_state {
            RoleState::Slave(slave_state) => {
                if matches!(slave_state.handshake_state, HandshakeState::PingSent)
                    && matches!(response, Message::Pong)
                {
                    slave_state.handshake_state = HandshakeState::PongRcvd;
                } else if matches!(slave_state.handshake_state, HandshakeState::ReplConf1Sent)
                    && matches!(response, Message::Ok)
                {
                    slave_state.handshake_state = HandshakeState::ReplConf1Rcvd;
                } else if matches!(slave_state.handshake_state, HandshakeState::ReplConf2Sent)
                    && matches!(response, Message::Ok)
                {
                    slave_state.handshake_state = HandshakeState::ReplConf2Rcvd;
                } else if matches!(slave_state.handshake_state, HandshakeState::PSyncSent)
                    && matches!(response, Message::Ok)
                {
                    slave_state.handshake_state = HandshakeState::Complete;
                }
            }
            RoleState::Master(_) => {}
        }
    }

    pub fn next_request(&mut self) -> anyhow::Result<Option<Message>> {
        Ok(match &mut self.role_state {
            RoleState::Slave(slave_state) => match slave_state.handshake_state {
                HandshakeState::Init => {
                    slave_state.handshake_state = HandshakeState::PingSent;
                    Some(Message::Ping)
                }
                HandshakeState::PongRcvd => {
                    slave_state.handshake_state = HandshakeState::ReplConf1Sent;
                    Some(Message::ReplicationConfig {
                        key: "listening-port".to_string(),
                        value: self.config.0.get(&ConfigKey::Port).unwrap()[0].to_string(),
                    })
                }
                HandshakeState::ReplConf1Rcvd => {
                    slave_state.handshake_state = HandshakeState::ReplConf2Sent;
                    Some(Message::ReplicationConfig {
                        key: "capa".to_string(),
                        value: "psync2".to_string(),
                    })
                }
                HandshakeState::ReplConf2Rcvd => {
                    slave_state.handshake_state = HandshakeState::PSyncSent;
                    Some(Message::PSync {
                        replication_id: "?".into(),
                        offset: -1,
                    })
                }
                _ => None,
            },
            RoleState::Master(_) => None,
        })
    }

    pub fn handle_request(&mut self, request: &Message) -> anyhow::Result<Message> {
        match request {
            Message::Ping => Ok(Message::Pong),
            Message::Echo(message) => Ok(Message::Echo(message.to_owned())),
            Message::CommandDocs => Ok(Message::CommandDocs),
            Message::Set { key, value, expiry } => {
                let value = StoreValue {
                    data: value.to_string(),
                    updated: Instant::now(),
                    expiry: expiry.map(StoreExpiry::Duration),
                };
                self.store.data.insert(key.to_string(), value);
                Ok(Message::Ok)
            }
            Message::GetRequest { key } => match self.store.data.get(key) {
                Some(value) => {
                    match value.expiry {
                        Some(StoreExpiry::Duration(d)) => {
                            if Instant::now() > value.updated + d {
                                // Key has expired
                                Ok(Message::GetResponse(GetResponse::NotFound))
                            } else {
                                Ok(Message::GetResponse(GetResponse::Found(value.data.clone())))
                            }
                        }
                        Some(StoreExpiry::UnixTimestampMillis(t)) => {
                            let unix_time =
                                SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis() as u64;
                            if t < unix_time {
                                // Key has expired
                                Ok(Message::GetResponse(GetResponse::NotFound))
                            } else {
                                Ok(Message::GetResponse(GetResponse::Found(value.data.clone())))
                            }
                        }
                        None => Ok(Message::GetResponse(GetResponse::Found(value.data.clone()))),
                    }
                }
                None => Ok(Message::GetResponse(GetResponse::NotFound)),
            },
            Message::ConfigGetRequest { key } => match self.config.0.get(key) {
                Some(values) => Ok(Message::ConfigGetResponse(Some(ConfigGetResponse {
                    key: *key,
                    values: values.to_owned(),
                }))),
                None => Ok(Message::ConfigGetResponse(None)),
            },
            Message::KeysRequest => {
                let keys = self.store.data.keys().cloned().collect();
                Ok(Message::KeysResponse { keys })
            }
            Message::InfoRequest { sections } => {
                let mut section_maps = HashMap::new();
                if sections.is_empty() || sections.contains(&"replication".to_string()) {
                    let mut section_map = HashMap::new();
                    section_map.insert("role".to_string(), self.role_state.to_string());
                    if let RoleState::Master(master_state) = &self.role_state {
                        section_map.insert(
                            "master_replid".to_string(),
                            master_state.replication_id.clone(),
                        );
                        section_map.insert(
                            "master_repl_offset".to_string(),
                            master_state.replication_offset.to_string(),
                        );
                    }
                    section_maps.insert("Replication".to_string(), section_map);
                }
                Ok(Message::InfoResponse {
                    sections: section_maps,
                })
            }
            Message::ReplicationConfig { .. } => {
                // Ignore for now
                Ok(Message::Ok)
            }
            _ => Err(anyhow::format_err!(
                "invalid message from client {:?}",
                request
            )),
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
