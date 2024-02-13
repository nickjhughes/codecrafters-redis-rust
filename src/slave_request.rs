use bytes::BytesMut;

use crate::resp_value::RespValue;

#[derive(Debug)]
pub enum SlaveRequest {
    Ping,
    ReplConf(Vec<String>),
    PSync {
        replication_id: String,
        offset: isize,
    },
}

impl SlaveRequest {
    pub fn serialize(&self, buf: &mut BytesMut) {
        let response_value = match self {
            SlaveRequest::Ping => RespValue::Array(vec![RespValue::BulkString("ping")]),
            SlaveRequest::ReplConf(values) => {
                let mut all_values = vec![RespValue::BulkString("replconf")];
                for value in values.iter() {
                    all_values.push(RespValue::BulkString(value));
                }
                RespValue::Array(all_values)
            }
            SlaveRequest::PSync {
                replication_id,
                offset,
            } => RespValue::Array(vec![
                RespValue::BulkString("psync"),
                RespValue::BulkString(replication_id),
                RespValue::OwnedBulkString(offset.to_string()),
            ]),
        };
        response_value.serialize(buf);
    }
}
