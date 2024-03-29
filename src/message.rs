use bytes::BytesMut;
use std::{collections::HashMap, time::Duration};

use crate::{config::ConfigKey, resp_value::RespValue};

#[derive(Debug, Clone)]
pub enum Message {
    Ping,
    Pong,
    InfoRequest {
        sections: Vec<String>,
    },
    InfoResponse {
        sections: HashMap<String, HashMap<String, String>>,
    },
    KeysRequest,
    KeysResponse {
        keys: Vec<String>,
    },
    CommandDocs,
    Echo(String),
    ReplicationConfig {
        key: String,
        value: String,
    },
    Ok,
    PSync {
        replication_id: String,
        offset: isize,
    },
    FullResync {
        replication_id: String,
        offset: isize,
    },
    Set {
        key: String,
        value: String,
        expiry: Option<Duration>,
    },
    GetRequest {
        key: String,
    },
    GetResponse(GetResponse),
    ConfigGetRequest {
        key: ConfigKey,
    },
    ConfigGetResponse(Option<ConfigGetResponse>),
    DatabaseFile(Vec<u8>),
    Wait {
        num_replicas: usize,
        timeout: Duration,
    },
    WaitReply {
        num_replicas: usize,
    },
}

#[derive(Debug, Clone)]
pub enum GetResponse {
    Found(String),
    NotFound,
}

#[derive(Debug, Clone)]
pub struct ConfigGetResponse {
    pub key: ConfigKey,
    pub values: Vec<String>,
}

impl Message {
    pub fn is_write_command(&self) -> bool {
        matches!(self, Message::Set { .. } | Message::GetRequest { .. })
    }

    pub fn serialize(&self, buf: &mut BytesMut) {
        let response_value = match self {
            Message::Ping => RespValue::Array(vec![RespValue::BulkString("PING")]),
            Message::Pong => RespValue::SimpleString("PONG"),
            Message::Echo(s) => RespValue::BulkString(s),
            Message::CommandDocs => RespValue::Array(vec![]),
            Message::Ok => RespValue::SimpleString("OK"),
            Message::Set { key, value, expiry } => {
                let mut values = vec![
                    RespValue::BulkString("SET"),
                    RespValue::BulkString(key),
                    RespValue::BulkString(value),
                ];
                if let Some(expiry) = expiry {
                    values.push(RespValue::BulkString("PX"));
                    values.push(RespValue::OwnedBulkString(expiry.as_millis().to_string()));
                }
                RespValue::Array(values)
            }
            Message::GetRequest { key } => RespValue::Array(vec![
                RespValue::BulkString("GET"),
                RespValue::BulkString(key),
            ]),
            Message::GetResponse(get_response) => match get_response {
                GetResponse::Found(value) => RespValue::BulkString(value),
                GetResponse::NotFound => RespValue::NullBulkString,
            },
            Message::ConfigGetRequest { key } => RespValue::Array(vec![
                RespValue::BulkString("CONFIG"),
                RespValue::BulkString("GET"),
                RespValue::BulkString(key.serialize()),
            ]),
            Message::ConfigGetResponse(config_get_response) => match config_get_response {
                Some(response) => {
                    let mut values = Vec::new();
                    values.push(RespValue::BulkString(response.key.serialize()));
                    values.extend(response.values.iter().map(|v| RespValue::BulkString(v)));
                    RespValue::Array(values)
                }
                None => RespValue::NullBulkString,
            },
            Message::KeysRequest => RespValue::Array(vec![RespValue::BulkString("KEYS")]),
            Message::KeysResponse { keys } => {
                RespValue::Array(keys.iter().map(|k| RespValue::BulkString(k)).collect())
            }
            Message::InfoRequest { sections } => {
                let mut values = vec![RespValue::BulkString("INFO")];
                values.extend(sections.iter().map(|s| RespValue::BulkString(s)));
                RespValue::Array(values)
            }
            Message::InfoResponse { sections } => {
                let mut lines = Vec::new();
                for (name, map) in sections.iter() {
                    lines.push(format!("#{name}"));
                    for (key, value) in map.iter() {
                        lines.push(format!("{key}:{value}"));
                    }
                }
                if lines.is_empty() {
                    RespValue::NullBulkString
                } else {
                    RespValue::OwnedBulkString(lines.join("\n"))
                }
            }
            Message::ReplicationConfig { key, value } => RespValue::Array(vec![
                RespValue::BulkString("REPLCONF"),
                RespValue::BulkString(key),
                RespValue::BulkString(value),
            ]),
            Message::PSync {
                replication_id,
                offset,
            } => RespValue::Array(vec![
                RespValue::BulkString("PSYNC"),
                RespValue::BulkString(replication_id),
                RespValue::OwnedBulkString(offset.to_string()),
            ]),
            Message::FullResync {
                replication_id,
                offset,
            } => RespValue::OwnedSimpleString(format!("FULLRESYNC {replication_id} {offset}")),
            Message::DatabaseFile(bytes) => RespValue::RawBytes(bytes),
            Message::Wait {
                num_replicas,
                timeout,
            } => RespValue::Array(vec![
                RespValue::BulkString("WAIT"),
                RespValue::OwnedBulkString(num_replicas.to_string()),
                RespValue::OwnedBulkString(timeout.as_millis().to_string()),
            ]),
            Message::WaitReply { num_replicas } => RespValue::Integer(*num_replicas as i64),
        };
        response_value.serialize(buf);
    }

    pub fn deserialize(data: &[u8]) -> anyhow::Result<(Self, &[u8])> {
        if data.is_empty() {
            return Err(anyhow::format_err!("empty message"));
        }
        let (response_value, remainder) = RespValue::deserialize(data)?;

        match response_value {
            RespValue::RawBytes(bytes) => Ok((Message::DatabaseFile(bytes.to_vec()), remainder)),
            RespValue::SimpleString(s) => match s.to_ascii_uppercase().as_str() {
                "PONG" => Ok((Message::Pong, remainder)),
                "OK" => Ok((Message::Ok, remainder)),
                response if response.starts_with("FULLRESYNC") => {
                    let parts = response.split_ascii_whitespace().collect::<Vec<&str>>();
                    Ok((
                        Message::FullResync {
                            replication_id: parts[1].to_owned(),
                            offset: parts[2].parse::<isize>()?,
                        },
                        remainder,
                    ))
                }
                _ => Err(anyhow::format_err!("unknown message {:?}", s)),
            },
            RespValue::Array(elements) => match elements.get(0) {
                Some(RespValue::BulkString(s)) => match s.to_ascii_uppercase().as_str() {
                    "PING" => Ok((Message::Ping, remainder)),
                    "ECHO" => match elements.get(1) {
                        Some(RespValue::BulkString(s)) => {
                            Ok((Message::Echo(s.to_string()), remainder))
                        }
                        _ => Err(anyhow::format_err!("malformed ECHO command")),
                    },
                    "COMMAND" => match elements.get(1) {
                        Some(RespValue::BulkString(s)) => match s.to_ascii_uppercase().as_str() {
                            "DOCS" => Ok((Message::CommandDocs, remainder)),
                            _ => Err(anyhow::format_err!("malformed COMMAND DOCS command")),
                        },
                        _ => Err(anyhow::format_err!("malformed COMMAND command")),
                    },
                    "SET" => {
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
                                if s.to_ascii_uppercase() == "PX" {
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
                        Ok((
                            Message::Set {
                                key: key.to_string(),
                                value: value.to_string(),
                                expiry,
                            },
                            remainder,
                        ))
                    }
                    "GET" => {
                        let key = match elements.get(1) {
                            Some(RespValue::BulkString(s)) => *s,
                            _ => return Err(anyhow::format_err!("malformed GET command")),
                        };
                        Ok((
                            Message::GetRequest {
                                key: key.to_string(),
                            },
                            remainder,
                        ))
                    }
                    "CONFIG" => match elements.get(1) {
                        Some(RespValue::BulkString(s)) => match s.to_ascii_uppercase().as_str() {
                            "GET" => match elements.get(2) {
                                Some(RespValue::BulkString(s)) => match ConfigKey::deserialize(s) {
                                    Ok(key) => Ok((Message::ConfigGetRequest { key }, remainder)),
                                    Err(_) => {
                                        Err(anyhow::format_err!("invalid config key {:?}", s))
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
                    "KEYS" => match elements.get(1) {
                        Some(RespValue::BulkString(_)) => Ok((Message::KeysRequest, remainder)),
                        _ => Err(anyhow::format_err!("malformed KEYS command",)),
                    },
                    "INFO" => {
                        let mut sections = Vec::new();
                        for element in elements.iter().skip(1) {
                            match element {
                                RespValue::BulkString(section) => {
                                    sections.push(section.to_string())
                                }
                                _ => return Err(anyhow::format_err!("malformed INFO command",)),
                            }
                        }
                        Ok((Message::InfoRequest { sections }, remainder))
                    }
                    "REPLCONF" => {
                        let key = match elements.get(1) {
                            Some(RespValue::BulkString(s)) => *s,
                            _ => return Err(anyhow::format_err!("malformed REPLCONF command")),
                        };
                        let value = match elements.get(2) {
                            Some(RespValue::BulkString(s)) => *s,
                            _ => return Err(anyhow::format_err!("malformed REPLCONF command")),
                        };
                        Ok((
                            Message::ReplicationConfig {
                                key: key.to_string(),
                                value: value.to_string(),
                            },
                            remainder,
                        ))
                    }
                    "PSYNC" => {
                        let replication_id = match elements.get(1) {
                            Some(RespValue::BulkString(s)) => *s,
                            _ => return Err(anyhow::format_err!("malformed PSYNC command")),
                        };
                        let offset = match elements.get(2) {
                            Some(RespValue::BulkString(s)) => s.parse::<isize>()?,
                            _ => return Err(anyhow::format_err!("malformed PSYNC command")),
                        };
                        Ok((
                            Message::PSync {
                                replication_id: replication_id.to_string(),
                                offset,
                            },
                            remainder,
                        ))
                    }
                    "WAIT" => {
                        let num_replicas = match elements.get(1) {
                            Some(RespValue::BulkString(s)) => s.parse::<usize>()?,
                            _ => return Err(anyhow::format_err!("malformed WAIT command")),
                        };
                        let timeout = match elements.get(2) {
                            Some(RespValue::BulkString(s)) => {
                                Duration::from_millis(s.parse::<u64>()?)
                            }
                            _ => return Err(anyhow::format_err!("malformed WAIT command")),
                        };
                        Ok((
                            Message::Wait {
                                num_replicas,
                                timeout,
                            },
                            remainder,
                        ))
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
            _ => Err(anyhow::format_err!(
                "unsupported message: {:?}",
                response_value
            )),
        }
    }
}
