use std::collections::HashMap;

use crate::{
    request::Request,
    response::{GetResponse, Response, SetResponse},
};

#[derive(Default)]
pub struct Memory {
    data: HashMap<String, String>,
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
