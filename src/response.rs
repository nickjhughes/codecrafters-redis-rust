use bytes::BytesMut;

use crate::{config::Parameter, resp_value::RespValue};

#[derive(Debug)]
pub enum Response<'request, 'state> {
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
    pub value: &'config str,
}

impl<'request, 'state> Response<'request, 'state> {
    pub fn serialize(&self, buf: &mut BytesMut) {
        let response_value = match self {
            Response::Pong => RespValue::SimpleString("PONG"),
            Response::Echo(s) => RespValue::BulkString(s),
            Response::CommandDocs => RespValue::Array(vec![]),
            Response::Set(set_response) => match set_response {
                SetResponse::Ok => RespValue::SimpleString("OK"),
            },
            Response::Get(get_response) => match get_response {
                GetResponse::Found(value) => RespValue::BulkString(value),
                GetResponse::NotFound => RespValue::NullBulkString,
            },
            Response::ConfigGet(config_get_response) => match config_get_response {
                Some(response) => RespValue::Array(vec![
                    RespValue::BulkString(response.parameter.serialize()),
                    RespValue::BulkString(response.value),
                ]),
                None => RespValue::NullBulkString,
            },
            Response::Keys(keys) => RespValue::Array(keys.to_vec()),
            Response::Info(keys) => keys.clone(),
        };
        response_value.serialize(buf);
    }
}
