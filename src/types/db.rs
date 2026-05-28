use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use crate::types::{StreamEntry, Zset};

pub type Db = Arc<Mutex<HashMap<String, RedisValue>>>;

#[allow(unused)]
pub enum ValueType {
    String(String),
    List(Vec<String>),
    Stream(Vec<StreamEntry>),
    Set(),
    Zset(Zset),
    Hash(),
    Vectorset(),
}

pub struct RedisValue {
    pub value: ValueType,
    pub expires_at: Option<Instant>,
    pub version: u64,
}

impl RedisValue {
    pub fn new(value: ValueType, expires_at: Option<Instant>) -> Self {
        Self {
            value,
            expires_at,
            version: 0,
        }
    }
}
