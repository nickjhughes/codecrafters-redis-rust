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

const EMPTY_RDB_FILE: &[u8] = &[
    0x52, 0x45, 0x44, 0x49, 0x53, 0x30, 0x30, 0x31, 0x31, 0xfa, 0x09, 0x72, 0x65, 0x64, 0x69, 0x73,
    0x2d, 0x76, 0x65, 0x72, 0x05, 0x37, 0x2e, 0x32, 0x2e, 0x30, 0xfa, 0x0a, 0x72, 0x65, 0x64, 0x69,
    0x73, 0x2d, 0x62, 0x69, 0x74, 0x73, 0xc0, 0x40, 0xfa, 0x05, 0x63, 0x74, 0x69, 0x6d, 0x65, 0xc2,
    0x6d, 0x08, 0xbc, 0x65, 0xfa, 0x08, 0x75, 0x73, 0x65, 0x64, 0x2d, 0x6d, 0x65, 0x6d, 0xc2, 0xb0,
    0xc4, 0x10, 0x00, 0xfa, 0x08, 0x61, 0x6f, 0x66, 0x2d, 0x62, 0x61, 0x73, 0x65, 0xc0, 0x00, 0xff,
    0xf0, 0x6e, 0x3b, 0xfe, 0xc0, 0xff, 0x5a, 0xa2,
];

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
    replication_offset: isize,
    send_rdb: bool,
}

impl Default for MasterState {
    fn default() -> Self {
        MasterState {
            replication_id: REPLICATION_ID.into(),
            replication_offset: 0,
            send_rdb: false,
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

    pub fn next_outgoing(&mut self) -> anyhow::Result<Option<Message>> {
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
            RoleState::Master(master_state) => {
                if master_state.send_rdb {
                    master_state.send_rdb = false;
                    Some(Message::DatabaseFile(EMPTY_RDB_FILE.to_vec()))
                } else {
                    None
                }
            }
        })
    }

    pub fn handle_incoming(&mut self, message: &Message) -> anyhow::Result<Option<Message>> {
        match &mut self.role_state {
            RoleState::Slave(slave_state) => match message {
                Message::Pong => {
                    if matches!(slave_state.handshake_state, HandshakeState::PingSent) {
                        slave_state.handshake_state = HandshakeState::PongRcvd;
                    }
                    Ok(None)
                }
                Message::Ok => {
                    if matches!(slave_state.handshake_state, HandshakeState::ReplConf1Sent) {
                        slave_state.handshake_state = HandshakeState::ReplConf1Rcvd;
                    } else if matches!(slave_state.handshake_state, HandshakeState::ReplConf2Sent) {
                        slave_state.handshake_state = HandshakeState::ReplConf2Rcvd;
                    }
                    Ok(None)
                }
                Message::FullResync { .. } => {
                    if matches!(slave_state.handshake_state, HandshakeState::PSyncSent) {
                        slave_state.handshake_state = HandshakeState::Complete;
                    }
                    Ok(None)
                }
                Message::InfoRequest { sections } => {
                    let mut section_maps = HashMap::new();
                    if sections.is_empty() || sections.contains(&"replication".to_string()) {
                        let mut section_map = HashMap::new();
                        section_map.insert("role".to_string(), "slave".to_string());
                        section_maps.insert("Replication".to_string(), section_map);
                    }
                    Ok(Some(Message::InfoResponse {
                        sections: section_maps,
                    }))
                }
                _ => Err(anyhow::format_err!(
                    "invalid message from master {:?}",
                    message
                )),
            },
            RoleState::Master(master_state) => {
                match message {
                    Message::Ping => Ok(Some(Message::Pong)),
                    Message::Echo(message) => Ok(Some(Message::Echo(message.to_owned()))),
                    Message::CommandDocs => Ok(Some(Message::CommandDocs)),
                    Message::Set { key, value, expiry } => {
                        let value = StoreValue {
                            data: value.to_string(),
                            updated: Instant::now(),
                            expiry: expiry.map(StoreExpiry::Duration),
                        };
                        self.store.data.insert(key.to_string(), value);
                        Ok(Some(Message::Ok))
                    }
                    Message::GetRequest { key } => match self.store.data.get(key) {
                        Some(value) => {
                            match value.expiry {
                                Some(StoreExpiry::Duration(d)) => {
                                    if Instant::now() > value.updated + d {
                                        // Key has expired
                                        Ok(Some(Message::GetResponse(GetResponse::NotFound)))
                                    } else {
                                        Ok(Some(Message::GetResponse(GetResponse::Found(
                                            value.data.clone(),
                                        ))))
                                    }
                                }
                                Some(StoreExpiry::UnixTimestampMillis(t)) => {
                                    let unix_time =
                                        SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis()
                                            as u64;
                                    if t < unix_time {
                                        // Key has expired
                                        Ok(Some(Message::GetResponse(GetResponse::NotFound)))
                                    } else {
                                        Ok(Some(Message::GetResponse(GetResponse::Found(
                                            value.data.clone(),
                                        ))))
                                    }
                                }
                                None => Ok(Some(Message::GetResponse(GetResponse::Found(
                                    value.data.clone(),
                                )))),
                            }
                        }
                        None => Ok(Some(Message::GetResponse(GetResponse::NotFound))),
                    },
                    Message::ConfigGetRequest { key } => match self.config.0.get(key) {
                        Some(values) => {
                            Ok(Some(Message::ConfigGetResponse(Some(ConfigGetResponse {
                                key: *key,
                                values: values.to_owned(),
                            }))))
                        }
                        None => Ok(Some(Message::ConfigGetResponse(None))),
                    },
                    Message::KeysRequest => {
                        let keys = self.store.data.keys().cloned().collect();
                        Ok(Some(Message::KeysResponse { keys }))
                    }
                    Message::InfoRequest { sections } => {
                        let mut section_maps = HashMap::new();
                        if sections.is_empty() || sections.contains(&"replication".to_string()) {
                            let mut section_map = HashMap::new();
                            section_map.insert("role".to_string(), "master".to_string());
                            section_map.insert(
                                "master_replid".to_string(),
                                master_state.replication_id.clone(),
                            );
                            section_map.insert(
                                "master_repl_offset".to_string(),
                                master_state.replication_offset.to_string(),
                            );
                            section_maps.insert("Replication".to_string(), section_map);
                        }
                        Ok(Some(Message::InfoResponse {
                            sections: section_maps,
                        }))
                    }
                    Message::ReplicationConfig { .. } => {
                        // Ignore for now
                        Ok(Some(Message::Ok))
                    }
                    Message::PSync {
                        replication_id,
                        offset,
                    } => {
                        if replication_id == "?" && *offset == -1 {
                            master_state.send_rdb = true;
                            Ok(Some(Message::FullResync {
                                replication_id: master_state.replication_id.clone(),
                                offset: master_state.replication_offset,
                            }))
                        } else {
                            Ok(None)
                        }
                    }
                    _ => Err(anyhow::format_err!(
                        "invalid message from client/replica {:?}",
                        message
                    )),
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
