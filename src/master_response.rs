use crate::resp_value::RespValue;

#[derive(Debug)]
pub enum MasterResponse {
    Pong,
    Ok,
    FullResync {
        replication_id: String,
        offset: usize,
    },
}

impl MasterResponse {
    pub fn deserialize(data: &[u8]) -> anyhow::Result<Self> {
        if data.is_empty() {
            return Err(anyhow::format_err!("empty response"));
        }
        let (response_value, _) = RespValue::deserialize(data)?;
        match response_value {
            RespValue::SimpleString(s) => match s.to_ascii_lowercase().as_str() {
                "pong" => Ok(MasterResponse::Pong),
                "ok" => Ok(MasterResponse::Ok),
                response => {
                    if response.starts_with("fullresync") {
                        let parts = response.split_ascii_whitespace().collect::<Vec<&str>>();
                        Ok(MasterResponse::FullResync {
                            replication_id: parts[1].to_owned(),
                            offset: parts[2].parse::<usize>()?,
                        })
                    } else {
                        Err(anyhow::format_err!(
                            "unknown response {:?}",
                            response.to_uppercase()
                        ))
                    }
                }
            },
            _ => Err(anyhow::format_err!(
                "master responses must be simple strings"
            )),
        }
    }
}
