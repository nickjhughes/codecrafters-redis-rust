use std::{path::PathBuf, time::Instant};

use crate::{
    config::{Config, Parameter},
    rdb::read_rdb_file,
    request::{Request, SetRequest},
    resp_value::RespValue,
    response::{ConfigGetResponse, GetResponse, Response, SetResponse},
    store::{Store, StoreValue},
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
            dbg!(&path);
            read_rdb_file(path)?
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
                    expiry: *expiry,
                };
                self.store.data.insert(key.to_string(), value);
                Ok(Response::Set(SetResponse::Ok))
            }
            Request::Get(key) => match self.store.data.get(*key) {
                Some(value) => {
                    if let Some(expiry) = value.expiry {
                        if Instant::now() > value.updated + expiry {
                            // Key has expired
                            Ok(Response::Get(GetResponse::NotFound))
                        } else {
                            Ok(Response::Get(GetResponse::Found(&value.data)))
                        }
                    } else {
                        Ok(Response::Get(GetResponse::Found(&value.data)))
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
        }
    }
}
