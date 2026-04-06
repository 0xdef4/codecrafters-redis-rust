#![allow(unused)]

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

pub type Db = Arc<Mutex<HashMap<String, RedisValue>>>;

pub struct StreamEntry {
    pub id: String,
    pub fields: Vec<(String, String)>,
}

impl StreamEntry {
    pub fn new(id: String, fields: Vec<(String, String)>) -> Self {
        Self { id, fields }
    }

    pub fn get_entry_id(&self) -> String {
        self.id.clone()
    }
}

pub enum ValueType {
    String(String),
    List(Vec<String>),
    Stream(Vec<StreamEntry>),
    Set(),
    Zset(),
    Hash(),
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
