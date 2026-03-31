use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Instant, Duration};

pub type Db = Arc<Mutex<HashMap<String, MapValue>>>;

pub struct MapValue {
    pub value: String,
    pub expires_at: Option<Instant>
}

impl MapValue {
    pub fn new(value: String, expires_at: Option<Instant>) -> Self {
        Self {
            value,
            expires_at
        }
    }
}

