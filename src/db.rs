use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

pub type Db = Arc<Mutex<HashMap<String, RedisValue>>>;

pub enum ValueType {
    String(String),
    List(Vec<String>),
    Set(),
    Zset(),
    Hash(),
    Stream(),
    Vectorset(),
}

pub struct RedisValue {
    pub value: ValueType,
    pub expires_at: Option<Instant>,
}

impl RedisValue {
    pub fn new(value: ValueType, expires_at: Option<Instant>) -> Self {
        Self { value, expires_at }
    }
}
