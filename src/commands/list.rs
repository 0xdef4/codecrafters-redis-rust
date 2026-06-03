use std::sync::Arc;
use std::time::Duration;

use tokio::sync::Notify;
use tokio::time::timeout;

use crate::protocol::RespValue;
use crate::types::{Db, RedisValue, ValueType};

pub fn execute_lpush(command: &[String], db: &Db, notify: &Arc<Notify>) -> Option<RespValue> {
    match command {
        [cmd, list_key, list_values @ ..] if cmd.to_uppercase() == "LPUSH" => {
            let list_length = {
                let mut db = db.lock().unwrap();

                if let Some(redis_value) = db.get_mut(list_key) {
                    if let ValueType::List(list) = &mut redis_value.value {
                        for el in list_values {
                            list.insert(0, el.to_string());
                        }
                        notify.notify_one();

                        list.len()
                    } else {
                        unimplemented!()
                    }
                } else {
                    let mut list = Vec::new();
                    for el in list_values {
                        list.insert(0, el.to_string());
                    }
                    notify.notify_one();

                    let len = list.len();

                    let redis_value = RedisValue::new(ValueType::List(list), None);

                    db.insert(list_key.to_string(), redis_value);

                    len
                }
            };

            Some(RespValue::Integers(list_length as i64))
        }
        _ => unreachable!(),
    }
}

pub fn execute_rpush(command: &[String], db: &Db, notify: &Arc<Notify>) -> Option<RespValue> {
    match command {
        [cmd, list_key, list_values @ ..] if cmd.to_uppercase() == "RPUSH" => {
            let list_length = {
                let mut db = db.lock().unwrap();

                if let Some(redis_value) = db.get_mut(list_key) {
                    if let ValueType::List(list) = &mut redis_value.value {
                        for el in list_values {
                            list.push(el.to_string());
                        }
                        notify.notify_one();

                        list.len()
                    } else {
                        unimplemented!()
                    }
                } else {
                    let mut list = Vec::new();
                    for el in list_values {
                        list.push(el.to_string());
                    }
                    notify.notify_one();

                    let len = list.len();

                    let redis_value = RedisValue::new(ValueType::List(list), None);

                    db.insert(list_key.to_string(), redis_value);

                    len
                }
            };
            Some(RespValue::Integers(list_length as i64))
        }
        _ => unreachable!(),
    }
}

pub fn execute_lpop(command: &[String], db: &Db) -> Option<RespValue> {
    match command {
        [cmd, list_key, optional_args @ ..] if cmd.to_uppercase() == "LPOP" => {
            match optional_args {
                [] => {
                    let removed: Option<String> = {
                        let mut db = db.lock().unwrap();

                        if let Some(redis_value) = db.get_mut(list_key) {
                            match &mut redis_value.value {
                                ValueType::List(list) => {
                                    if list.len() == 0 {
                                        None
                                    } else {
                                        Some(list.remove(0))
                                    }
                                }
                                _ => {
                                    unimplemented!()
                                }
                            }
                        } else {
                            None
                        }
                    };

                    match removed {
                        Some(removed) => Some(RespValue::BulkString(removed)),
                        None => Some(RespValue::BulkStringNull),
                    }
                }
                [num_to_remove] => {
                    let removed = {
                        let mut db = db.lock().unwrap();

                        if let Some(redis_value) = db.get_mut(list_key) {
                            match &mut redis_value.value {
                                ValueType::List(list) => list
                                    .drain(..num_to_remove.parse::<usize>().unwrap())
                                    .collect::<Vec<_>>(),
                                _ => {
                                    unimplemented!()
                                }
                            }
                        } else {
                            unimplemented!()
                        }
                    };
                    Some(RespValue::Array(
                        removed
                            .iter()
                            .map(|e| RespValue::BulkString(e.clone()))
                            .collect(),
                    ))
                }
                _ => unimplemented!(),
            }
        }
        _ => unreachable!(),
    }
}

pub fn execute_lrange(command: &[String], db: &Db) -> Option<RespValue> {
    match command {
        [cmd, list_key, start_index, stop_index] if cmd.to_uppercase() == "LRANGE" => {
            let slice = {
                let db = db.lock().unwrap();

                if let Some(redis_value) = db.get(list_key) {
                    if let ValueType::List(list) = &redis_value.value {
                        let list_length = list.len();
                        let start_index: i64 = start_index.parse().unwrap();
                        let stop_index: i64 = stop_index.parse().unwrap();

                        let start = if start_index < 0 && start_index.abs() > list_length as i64 {
                            0
                        } else if start_index < 0 {
                            (list_length as i64 + start_index).max(0) as usize
                        } else {
                            start_index as usize
                        };

                        let mut stop = if stop_index < 0 && stop_index.abs() > list_length as i64 {
                            0
                        } else if stop_index < 0 {
                            (list_length as i64 + stop_index).max(0) as usize
                        } else {
                            stop_index as usize
                        };

                        println!("start : {:?}", start);
                        println!("stop : {:?}", stop);

                        if start >= list_length || start > stop {
                            Vec::new()
                        } else if stop >= list_length {
                            stop = list_length - 1;
                            list[start..=stop].to_vec()
                        } else {
                            list[start..=stop].to_vec()
                        }
                    } else {
                        unimplemented!()
                    }
                } else {
                    Vec::new()
                }
            };
            Some(RespValue::Array(
                slice
                    .iter()
                    .map(|s| RespValue::BulkString(s.clone()))
                    .collect(),
            ))
        }
        _ => unreachable!(),
    }
}

pub fn execute_llen(command: &[String], db: &Db) -> Option<RespValue> {
    match command {
        [cmd, list_key] if cmd.to_uppercase() == "LLEN" => {
            let response = {
                let db = db.lock().unwrap();

                if let Some(redis_value) = db.get(list_key) {
                    match &redis_value.value {
                        ValueType::List(list) => list.len(),
                        _ => 0,
                    }
                } else {
                    0
                }
            };
            Some(RespValue::Integers(response as i64))
        }
        _ => unreachable!(),
    }
}

pub async fn execute_blpop(command: &[String], db: &Db, notify: &Arc<Notify>) -> Option<RespValue> {
    match command {
        [cmd, list_key, timeout_seconds] if cmd.to_uppercase() == "BLPOP" => {
            let seconds: f64 = timeout_seconds.parse().unwrap();
            let removed = {
                loop {
                    let notified = notify.notified();

                    let has_value = {
                        let mut db = db.lock().unwrap();
                        if let Some(redis_value) = db.get_mut(list_key) {
                            if let ValueType::List(list) = &mut redis_value.value {
                                if list.len() == 0 { false } else { true }
                            } else {
                                unimplemented!()
                            }
                        } else {
                            false
                        }
                    };

                    if has_value {
                        let mut db = db.lock().unwrap();
                        if let Some(redis_value) = db.get_mut(list_key) {
                            if let ValueType::List(list) = &mut redis_value.value {
                                break list.remove(0);
                            } else {
                                unimplemented!()
                            }
                        } else {
                            unimplemented!()
                        }
                    }

                    match seconds {
                        0.0 => {
                            notified.await;
                        }
                        _ => {
                            if let Err(_) =
                                timeout(Duration::from_secs_f64(seconds), notified).await
                            {
                                return None;
                            }
                        }
                    }
                }
            };
            Some(RespValue::Array(vec![
                RespValue::BulkString(list_key.to_string()),
                RespValue::BulkString(removed),
            ]))
        }
        _ => unreachable!(),
    }
}
