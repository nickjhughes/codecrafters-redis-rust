use std::{collections::HashMap, env::Args};

#[derive(Debug, Default)]
pub struct Config(pub HashMap<ConfigKey, Vec<String>>);

impl Config {
    /// Load config from command line arguments.
    pub fn parse(args: Args) -> anyhow::Result<Config> {
        let args = args.skip(1);

        let mut config = Config::default();
        let mut current_key: Option<ConfigKey> = None;
        let mut current_values = Vec::new();
        for arg in args {
            if let Some(some_current_key) = current_key {
                current_values.push(arg);
                if current_values.len() == some_current_key.value_count() {
                    config.0.insert(some_current_key, current_values.clone());
                    current_values.clear();
                    current_key = None;
                }
            } else if arg.starts_with("--") {
                current_key = Some(ConfigKey::deserialize(arg.strip_prefix("--").unwrap())?);
            } else {
                anyhow::bail!("invalid argument {:?}", arg)
            }
        }
        Ok(config)
    }
}

#[derive(Debug, Hash, PartialEq, Eq, Clone, Copy)]
pub enum ConfigKey {
    Dir,
    DbFilename,
    Port,
    ReplicaOf,
    Unknown,
}

impl ConfigKey {
    pub fn deserialize(s: &str) -> anyhow::Result<Self> {
        match s.to_ascii_lowercase().as_str() {
            "dir" => Ok(ConfigKey::Dir),
            "dbfilename" => Ok(ConfigKey::DbFilename),
            "port" => Ok(ConfigKey::Port),
            "replicaof" => Ok(ConfigKey::ReplicaOf),
            _ => Ok(ConfigKey::Unknown),
        }
    }

    pub fn serialize(&self) -> &'static str {
        match self {
            ConfigKey::Dir => "dir",
            ConfigKey::DbFilename => "dbfilename",
            ConfigKey::Port => "port",
            ConfigKey::ReplicaOf => "replicaof",
            ConfigKey::Unknown => unreachable!(),
        }
    }

    pub fn value_count(&self) -> usize {
        match self {
            ConfigKey::ReplicaOf => 2,
            _ => 1,
        }
    }
}
