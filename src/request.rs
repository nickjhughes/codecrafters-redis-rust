use std::time::Duration;

use crate::{config::Parameter, resp_value::RespValue};

#[derive(Debug)]
pub enum Request<'data> {
    CommandDocs,
    Ping,
    Echo(&'data str),
    Set(SetRequest<'data>),
    Get(&'data str),
    ConfigGet(Parameter),
    Keys,
    Info(Vec<&'data str>),
}

#[derive(Debug)]
pub struct SetRequest<'data> {
    pub key: &'data str,
    pub value: &'data str,
    pub expiry: Option<Duration>,
}

impl<'data> Request<'data> {
    pub fn deserialize(data: &'data [u8]) -> anyhow::Result<Self> {
        if data.is_empty() {
            return Err(anyhow::format_err!("empty request"));
        }
        let (request_value, _) = RespValue::deserialize(data)?;
        match request_value {
            RespValue::Array(elements) => match elements.get(0) {
                Some(RespValue::BulkString(s)) => match s.to_ascii_lowercase().as_str() {
                    "ping" => Ok(Request::Ping),
                    "echo" => match elements.get(1) {
                        Some(RespValue::BulkString(s)) => Ok(Request::Echo(s)),
                        _ => Err(anyhow::format_err!("malformed ECHO command")),
                    },
                    "command" => match elements.get(1) {
                        Some(RespValue::BulkString(s)) => match s.to_ascii_lowercase().as_str() {
                            "docs" => Ok(Request::CommandDocs),
                            _ => Err(anyhow::format_err!("malformed COMMAND DOCS command")),
                        },
                        _ => Err(anyhow::format_err!("malformed COMMAND command")),
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
                        let expiry = match elements.get(3) {
                            Some(RespValue::BulkString(s)) => {
                                if s.to_ascii_lowercase() == "px" {
                                    match elements.get(4) {
                                        Some(RespValue::BulkString(millis_string)) => {
                                            if let Ok(millis) = millis_string.parse::<u64>() {
                                                Some(Duration::from_millis(millis))
                                            } else {
                                                None
                                            }
                                        }
                                        _ => None,
                                    }
                                } else {
                                    None
                                }
                            }
                            _ => None,
                        };

                        Ok(Request::Set(SetRequest { key, value, expiry }))
                    }
                    "get" => {
                        let key = match elements.get(1) {
                            Some(RespValue::BulkString(s)) => *s,
                            _ => return Err(anyhow::format_err!("malformed GET command")),
                        };
                        Ok(Request::Get(key))
                    }
                    "config" => match elements.get(1) {
                        Some(RespValue::BulkString(s)) => match s.to_ascii_lowercase().as_str() {
                            "get" => match elements.get(2) {
                                Some(RespValue::BulkString(s)) => match Parameter::deserialize(s) {
                                    Ok(parameter) => Ok(Request::ConfigGet(parameter)),
                                    Err(_) => {
                                        Err(anyhow::format_err!("invalid config parameter {:?}", s))
                                    }
                                },
                                _ => Err(anyhow::format_err!("malformed CONFIG GET command")),
                            },
                            command => Err(anyhow::format_err!(
                                "unhandled CONFIG command {:?}",
                                command.to_uppercase()
                            )),
                        },
                        _ => Err(anyhow::format_err!("malformed CONFIG command")),
                    },
                    "keys" => match elements.get(1) {
                        Some(RespValue::BulkString(_)) => Ok(Request::Keys),
                        _ => Err(anyhow::format_err!("malformed KEYS command",)),
                    },
                    "info" => {
                        let mut sections = Vec::new();
                        for element in elements.iter().skip(1) {
                            match element {
                                RespValue::BulkString(section) => sections.push(*section),
                                _ => return Err(anyhow::format_err!("malformed INFO command",)),
                            }
                        }
                        Ok(Request::Info(sections))
                    }
                    command => Err(anyhow::format_err!(
                        "unknown command {:?}",
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
