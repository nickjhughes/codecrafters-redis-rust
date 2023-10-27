use crate::resp_value::RespValue;

#[derive(Debug)]
pub enum Request<'data> {
    CommandDocs,
    Ping,
    Echo(&'data str),
    Set { key: &'data str, value: &'data str },
    Get(&'data str),
}

impl<'data> Request<'data> {
    pub fn deserialize(data: &'data [u8]) -> anyhow::Result<Self> {
        if data.is_empty() {
            return Err(anyhow::format_err!("empty request"));
        }
        let (request_value, _) = RespValue::deserialize(data)?;
        match request_value {
            RespValue::Array(elements) => match elements.get(0) {
                Some(RespValue::BulkString(s)) => match s.to_lowercase().as_str() {
                    "ping" => Ok(Request::Ping),
                    "echo" => match elements.get(1) {
                        Some(RespValue::BulkString(s)) => Ok(Request::Echo(s)),
                        _ => Err(anyhow::format_err!("malformed ECHO command")),
                    },
                    "command" => match elements.get(1) {
                        Some(RespValue::BulkString(s)) => match s.to_lowercase().as_str() {
                            "docs" => Ok(Request::CommandDocs),
                            _ => Err(anyhow::format_err!("malformed COMMAND DOCS command")),
                        },
                        _ => Err(anyhow::format_err!("malformed COMMAND DOCS command")),
                    },
                    "set" => {
                        let key = match elements.get(1) {
                            Some(RespValue::BulkString(s)) => *s,
                            _ => return Err(anyhow::format_err!("malformed SET command")),
                        };
                        let value = match elements.get(2) {
                            Some(RespValue::BulkString(s)) => *s,
                            _ => return Err(anyhow::format_err!("malformed SET command")),
                        };
                        Ok(Request::Set { key, value })
                    }
                    "get" => {
                        let key = match elements.get(1) {
                            Some(RespValue::BulkString(s)) => *s,
                            _ => return Err(anyhow::format_err!("malformed GET command")),
                        };
                        Ok(Request::Get(key))
                    }
                    command => Err(anyhow::format_err!(
                        "unhandled command {:?}",
                        command.to_uppercase()
                    )),
                },
                _ => Err(anyhow::format_err!(
                    "requests must start with a bulk string"
                )),
            },
            _ => Err(anyhow::format_err!("requests must be arrays")),
        }
    }
}
