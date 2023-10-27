use crate::resp_value::RespValue;

#[derive(Debug)]
pub enum Response<'request, 'memory> {
    CommandDocs,
    Pong,
    Echo(&'request str),
    Set(SetResponse),
    Get(GetResponse<'memory>),
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

impl<'request, 'memory> Response<'request, 'memory> {
    pub fn serialize(&self) -> Vec<u8> {
        match self {
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
        }
        .serialize()
    }
}
