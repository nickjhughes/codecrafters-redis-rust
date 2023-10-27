use std::{
    collections::HashMap,
    time::{Duration, Instant},
};

use crate::{
    request::{Request, SetRequest},
    response::{GetResponse, Response, SetResponse},
};

#[derive(Default)]
pub struct Memory {
    data: HashMap<String, Value>,
}

#[derive(Debug)]
pub struct Value {
    pub data: String,
    pub updated: Instant,
    pub expiry: Option<Duration>,
}

impl Memory {
    pub fn handle_request<'request, 'memory>(
        &'memory mut self,
        request: &'request Request,
    ) -> anyhow::Result<Response<'request, 'memory>> {
        match request {
            Request::Ping => Ok(Response::Pong),
            Request::Echo(message) => Ok(Response::Echo(message)),
            Request::CommandDocs => Ok(Response::CommandDocs),
            Request::Set(SetRequest { key, value, expiry }) => {
                let value = Value {
                    data: value.to_string(),
                    updated: Instant::now(),
                    expiry: *expiry,
                };
                self.data.insert(key.to_string(), value);
                Ok(Response::Set(SetResponse::Ok))
            }
            Request::Get(key) => match self.data.get(*key) {
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
        }
    }
}
