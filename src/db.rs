use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use crate::resp::RespValue;

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
}

impl RedisValue {
    pub fn new(value: ValueType, expires_at: Option<Instant>) -> Self {
        Self { value, expires_at }
    }
}

pub struct Zset {
    sorted: BTreeMap<(u64, String), f64>,
    scores: HashMap<String, f64>,
}

impl Zset {
    pub fn new() -> Self {
        Self {
            sorted: BTreeMap::new(),
            scores: HashMap::new(),
        }
    }

    pub fn add(&mut self, score: f64, member: String) -> usize {
        if let Some(old_score) = self.scores.get(&member) {
            self.sorted
                .remove(&(score_bits(*old_score), member.clone()));
        }

        let is_new = !self.scores.contains_key(&member);

        self.scores.insert(member.clone(), score);
        self.sorted
            .insert((score_bits(score), member.clone()), score);

        if is_new { 1 } else { 0 }
    }

    pub fn query_length(&self) -> usize {
        self.sorted.len()
    }

    pub fn query_score(&self, member: String) -> f64 {
        *self.scores.get(&member).unwrap()
    }

    pub fn query_index(&self, member: String) -> Option<usize> {
        self.sorted
            .keys()
            .enumerate()
            .find(|(_, (_, m))| m == &member)
            .map(|(index, _)| index)
    }

    pub fn query_range(&self, start_index: i64, stop_index: i64) -> Vec<String> {
        let len = self.sorted.len() as i64;

        let start = if start_index < 0 {
            (len + start_index).max(0)
        } else {
            start_index
        };
        let stop = if stop_index < 0 {
            (len + stop_index).max(0)
        } else {
            stop_index.min(len - 1)
        };

        if start > stop {
            return vec![];
        }

        self.sorted
            .iter()
            .skip(start as usize)
            .take((stop - start + 1) as usize)
            .map(|((_, member), _)| member.clone())
            .collect()
    }
}

fn score_bits(score: f64) -> u64 {
    let bits = score.to_bits();
    if bits >> 63 == 0 {
        bits | (1 << 63)
    } else {
        !bits
    }
}

#[derive(Debug, Clone)]
pub struct StreamEntry {
    id: String,
    fields: Vec<(String, String)>,
}

impl StreamEntry {
    pub fn new(id: String, fields: Vec<(String, String)>) -> Self {
        Self { id, fields }
    }

    pub fn get_entry_id(&self) -> String {
        self.id.clone()
    }

    pub fn get_fields(&self) -> Vec<(String, String)> {
        self.fields.clone()
    }

    pub fn to_resp_value(&self) -> RespValue {
        let mut output = Vec::new();
        output.push(RespValue::BulkString(self.get_entry_id()));
        let mut fields_vec = Vec::new();
        for e in self.get_fields() {
            fields_vec.push(RespValue::BulkString(e.0));
            fields_vec.push(RespValue::BulkString(e.1));
        }
        output.push(RespValue::Array(fields_vec));

        RespValue::Array(output)
    }
}
