use std::collections::HashMap;

#[derive(Debug, Default)]
pub struct Config(pub HashMap<Parameter, String>);

#[derive(Debug, Hash, PartialEq, Eq, Clone)]
pub enum Parameter {
    Dir,
    DbFilename,
    Unknown,
}

impl Parameter {
    pub fn deserialize(s: &str) -> anyhow::Result<Self> {
        match s.to_ascii_lowercase().as_str() {
            "dir" => Ok(Parameter::Dir),
            "dbfilename" => Ok(Parameter::DbFilename),
            _ => Ok(Parameter::Unknown),
        }
    }

    pub fn serialize(&self) -> &'static str {
        match self {
            Parameter::Dir => "dir",
            Parameter::DbFilename => "dbfilename",
            Parameter::Unknown => unreachable!(),
        }
    }
}
