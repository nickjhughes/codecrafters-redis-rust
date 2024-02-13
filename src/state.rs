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
};

pub struct State {
    store: Store,
    config: Config,
}

impl State {
    pub fn new(config: Config) -> anyhow::Result<Self> {
        let store = if config.0.contains_key(&Parameter::Dir)
            && config.0.contains_key(&Parameter::DbFilename)
        {
            let path = {
                let mut p = PathBuf::new();
                p.push(config.0.get(&Parameter::Dir).unwrap());
                p.push(config.0.get(&Parameter::DbFilename).unwrap());
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

        Ok(State { store, config })
    }

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
                Some(value) => Ok(Response::ConfigGet(Some(ConfigGetResponse {
                    parameter: parameter.clone(),
                    value,
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
                    Ok(Response::Info(RespValue::BulkString("role:master")))
                } else {
                    Ok(Response::Info(RespValue::NullBulkString))
                }
            }
        }
    }
}
