use std::{
    collections::HashMap,
    time::{Duration, Instant},
};

#[derive(Default)]
pub struct Store {
    pub data: HashMap<String, StoreValue>,
}

#[derive(Debug)]
pub struct StoreValue {
    pub data: String,
    pub updated: Instant,
    pub expiry: Option<Duration>,
}
