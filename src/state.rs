use std::{
    path::PathBuf,
    time::{Instant, SystemTime, UNIX_EPOCH},
};

use crate::{
    config::{Config, Parameter},
    rdb::read_rdb_file,
    request::{Request, SetRequest},
    resp_value::RespValue,
    response::{ConfigGetResponse, GetResponse, Response, SetResponse},
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
    handshake_step_completed: HandshakeStep,
}

#[derive(Default)]
#[allow(dead_code)]
enum HandshakeStep {
    #[default]
    None,
    Ping,
    ReplConf,
    PSync,
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

    // pub fn perform_handshake(&mut self) {
    //     match &mut self.role_state {
    //         RoleState::Slave(slave_state) => match slave_state.handshake_step_completed {
    //             HandshakeStep::None => {
    //                 // TODO: Send ping to master
    //             }
    //             HandshakeStep::Ping => {}
    //             HandshakeStep::ReplConf => {}
    //             HandshakeStep::PSync => {}
    //         },
    //         RoleState::Master(_) => {}
    //     }
    // }

    pub fn handle_request<'request, 'state>(
        &'state mut self,
        request: &'request Request,
    ) -> anyhow::Result<Response<'request, 'state>> {
        match request {
            Request::Ping => Ok(Response::Pong),
            Request::Echo(message) => Ok(Response::Echo(message)),
            Request::CommandDocs => Ok(Response::CommandDocs),
            Request::Set(SetRequest { key, value, expiry }) => {
                let value = StoreValue {
                    data: value.to_string(),
                    updated: Instant::now(),
                    expiry: expiry.map(StoreExpiry::Duration),
                };
                self.store.data.insert(key.to_string(), value);
                Ok(Response::Set(SetResponse::Ok))
            }
            Request::Get(key) => match self.store.data.get(*key) {
                Some(value) => {
                    match value.expiry {
                        Some(StoreExpiry::Duration(d)) => {
                            if Instant::now() > value.updated + d {
                                // Key has expired
                                Ok(Response::Get(GetResponse::NotFound))
                            } else {
                                Ok(Response::Get(GetResponse::Found(&value.data)))
                            }
                        }
                        Some(StoreExpiry::UnixTimestampMillis(t)) => {
                            let unix_time =
                                SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis() as u64;
                            if t < unix_time {
                                // Key has expired
                                Ok(Response::Get(GetResponse::NotFound))
                            } else {
                                Ok(Response::Get(GetResponse::Found(&value.data)))
                            }
                        }
                        None => Ok(Response::Get(GetResponse::Found(&value.data))),
                    }
                }
                None => Ok(Response::Get(GetResponse::NotFound)),
            },
            Request::ConfigGet(parameter) => match self.config.0.get(parameter) {
                Some(values) => Ok(Response::ConfigGet(Some(ConfigGetResponse {
                    parameter: *parameter,
                    values,
                }))),
                None => Ok(Response::ConfigGet(None)),
            },
            Request::Keys => {
                let keys = self
                    .store
                    .data
                    .keys()
                    .map(|k| RespValue::BulkString(k))
                    .collect();
                Ok(Response::Keys(keys))
            }
            Request::Info(sections) => {
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
                    Ok(Response::Info(RespValue::OwnedBulkString(
                        values.join("\n"),
                    )))
                } else {
                    Ok(Response::Info(RespValue::NullBulkString))
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
