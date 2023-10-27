use std::time::Instant;

use crate::{
    config::Config,
    request::{Request, SetRequest},
    response::{ConfigGetResponse, GetResponse, Response, SetResponse},
    store::{Store, StoreValue},
};

pub struct State {
    store: Store,
    config: Config,
}

impl State {
    pub fn new(config: Config) -> Self {
        State {
            store: Store::default(),
            config,
        }
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
        }
    }
}
