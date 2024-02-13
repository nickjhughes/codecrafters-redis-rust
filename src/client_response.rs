use bytes::BytesMut;

use crate::{config::Parameter, resp_value::RespValue};

#[derive(Debug)]
pub enum ClientResponse<'request, 'state> {
    CommandDocs,
    Pong,
    Echo(&'request str),
    Set(SetResponse),
    Get(GetResponse<'state>),
    ConfigGet(Option<ConfigGetResponse<'state>>),
    Keys(Vec<RespValue<'state>>),
    Info(RespValue<'state>),
}

#[derive(Debug)]
pub enum GetResponse<'store> {
    Found(&'store str),
    NotFound,
}

#[derive(Debug)]
pub enum SetResponse {
    Ok,
}

#[derive(Debug)]
pub struct ConfigGetResponse<'config> {
    pub parameter: Parameter,
    pub values: &'config [String],
}

impl<'request, 'state> ClientResponse<'request, 'state> {
    pub fn serialize(&self, buf: &mut BytesMut) {
        let response_value = match self {
            ClientResponse::Pong => RespValue::SimpleString("PONG"),
            ClientResponse::Echo(s) => RespValue::BulkString(s),
            ClientResponse::CommandDocs => RespValue::Array(vec![]),
            ClientResponse::Set(set_response) => match set_response {
                SetResponse::Ok => RespValue::SimpleString("OK"),
            },
            ClientResponse::Get(get_response) => match get_response {
                GetResponse::Found(value) => RespValue::BulkString(value),
                GetResponse::NotFound => RespValue::NullBulkString,
            },
            ClientResponse::ConfigGet(config_get_response) => match config_get_response {
                Some(response) => {
                    let mut values = Vec::new();
                    values.push(RespValue::BulkString(response.parameter.serialize()));
                    values.extend(response.values.iter().map(|v| RespValue::BulkString(v)));
                    RespValue::Array(values)
                }
                None => RespValue::NullBulkString,
            },
            ClientResponse::Keys(keys) => RespValue::Array(keys.to_vec()),
            ClientResponse::Info(keys) => keys.clone(),
        };
        response_value.serialize(buf);
    }
}
