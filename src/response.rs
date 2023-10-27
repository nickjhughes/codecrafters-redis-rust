use crate::{config::Parameter, resp_value::RespValue};

#[derive(Debug)]
pub enum Response<'request, 'memory> {
    CommandDocs,
    Pong,
    Echo(&'request str),
    Set(SetResponse),
    Get(GetResponse<'memory>),
    ConfigGet(Option<ConfigGetResponse>),
}

#[derive(Debug)]
pub enum GetResponse<'memory> {
    Found(&'memory str),
    NotFound,
}

#[derive(Debug)]
pub enum SetResponse {
    Ok,
}

#[derive(Debug)]
pub struct ConfigGetResponse {
    pub parameter: Parameter,
    pub value: String,
}

impl<'request, 'memory> Response<'request, 'memory> {
    pub fn serialize(&self) -> Vec<u8> {
        match self {
            Response::Pong => RespValue::SimpleString("PONG").serialize(),
            Response::Echo(s) => RespValue::BulkString(s).serialize(),
            Response::CommandDocs => RespValue::Array(vec![]).serialize(),
            Response::Set(set_response) => match set_response {
                SetResponse::Ok => RespValue::SimpleString("OK").serialize(),
            },
            Response::Get(get_response) => match get_response {
                GetResponse::Found(value) => RespValue::BulkString(value).serialize(),
                GetResponse::NotFound => RespValue::NullBulkString.serialize(),
            },
            Response::ConfigGet(config_get_response) => match config_get_response {
                Some(response) => RespValue::Array(vec![
                    RespValue::BulkString(response.parameter.serialize()),
                    RespValue::BulkString(&response.value),
                ])
                .serialize(),
                None => RespValue::NullBulkString.serialize(),
            },
        }
    }
}
