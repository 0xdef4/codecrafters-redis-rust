use crate::protocol::RespValue;
use crate::types::{Db, RedisValue, ValueType, Zset};

pub fn execute_zadd(command: &[String], db: &Db) -> Option<RespValue> {
    match command {
        [cmd, zset_key, score, member] if cmd.to_uppercase() == "ZADD".to_string() => {
            let num_new_members_added = {
                let mut db = db.lock().unwrap();

                if let Some(redis_value) = db.get_mut(zset_key) {
                    if let ValueType::Zset(sorted_set) = &mut redis_value.value {
                        sorted_set.add(score.parse().unwrap(), member.to_string())
                    } else {
                        unimplemented!()
                    }
                } else {
                    let mut zset = Zset::new();
                    zset.add(score.parse().unwrap(), member.to_string());

                    let redis_value = RedisValue::new(ValueType::Zset(zset), None);

                    db.insert(zset_key.to_string(), redis_value);
                    1
                }
            };
            Some(RespValue::Integers(num_new_members_added as i64))
        }
        _ => unreachable!(),
    }
}

pub fn execute_zrank(command: &[String], db: &Db) -> Option<RespValue> {
    match command {
        [cmd, zset_key, member] if cmd.to_uppercase() == "ZRANK".to_string() => {
            let response: Option<usize> = {
                let db = db.lock().unwrap();
                if let Some(redis_value) = db.get(zset_key) {
                    if let ValueType::Zset(sorted_set) = &redis_value.value {
                        sorted_set.query_index(member.to_string())
                    } else {
                        None
                    }
                } else {
                    None
                }
            };

            match response {
                Some(rank) => Some(RespValue::Integers(rank as i64)),
                None => Some(RespValue::BulkStringNull),
            }
        }
        _ => unreachable!(),
    }
}

pub fn execute_zrange(command: &[String], db: &Db) -> Option<RespValue> {
    match command {
        [cmd, zset_key, start_index, stop_index] if cmd.to_uppercase() == "ZRANGE".to_string() => {
            let ranged = {
                let db = db.lock().unwrap();
                if let Some(redis_value) = db.get(zset_key) {
                    if let ValueType::Zset(sorted_set) = &redis_value.value {
                        sorted_set
                            .query_range(start_index.parse().unwrap(), stop_index.parse().unwrap())
                    } else {
                        vec![]
                    }
                } else {
                    vec![]
                }
            };

            let resp_value_vec = ranged
                .iter()
                .map(|e| RespValue::BulkString(e.to_string()))
                .collect::<Vec<_>>();

            Some(RespValue::Array(resp_value_vec))
        }
        _ => unreachable!(),
    }
}

pub fn execute_zscore(command: &[String], db: &Db) -> Option<RespValue> {
    match command {
        [cmd, zset_key, member] if cmd.to_uppercase() == "ZSCORE".to_string() => {
            let score: Option<f64> = {
                let db = db.lock().unwrap();
                if let Some(redis_value) = db.get(zset_key) {
                    if let ValueType::Zset(sorted_set) = &redis_value.value {
                        sorted_set.query_score(member.to_string())
                    } else {
                        None
                    }
                } else {
                    None
                }
            };

            match score {
                Some(score) => Some(RespValue::BulkString(score.to_string())),
                None => Some(RespValue::BulkStringNull),
            }
        }
        _ => unreachable!(),
    }
}

pub fn execute_zrem(command: &[String], db: &Db) -> Option<RespValue> {
    match command {
        [cmd, zset_key, member] if cmd.to_uppercase() == "ZREM".to_string() => {
            let num_members_removed = {
                let mut db = db.lock().unwrap();

                if let Some(redis_value) = db.get_mut(zset_key) {
                    if let ValueType::Zset(sorted_set) = &mut redis_value.value {
                        sorted_set.remove(member.to_string())
                    } else {
                        0
                    }
                } else {
                    0
                }
            };

            Some(RespValue::Integers(num_members_removed as i64))
        }
        _ => unreachable!(),
    }
}

pub fn execute_zcard(command: &[String], db: &Db) -> Option<RespValue> {
    match command {
        [cmd, zset_key] if cmd.to_uppercase() == "ZCARD".to_string() => {
            let num_of_elements = {
                let db = db.lock().unwrap();
                if let Some(redis_value) = db.get(zset_key) {
                    if let ValueType::Zset(sorted_set) = &redis_value.value {
                        sorted_set.query_length()
                    } else {
                        0
                    }
                } else {
                    0
                }
            };

            Some(RespValue::Integers(num_of_elements as i64))
        }
        _ => unreachable!(),
    }
}
