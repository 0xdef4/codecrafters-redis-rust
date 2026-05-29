use std::time::{Duration, Instant};

use crate::protocol::RespValue;
use crate::types::{Db, RedisValue, ValueType};

pub fn execute_set(command: &[String], db: &Db) -> Option<RespValue> {
    match command {
        [cmd, key, value, optional_args @ ..] if cmd.to_uppercase() == "SET" => match optional_args
        {
            [] => {
                let mut db = db.lock().unwrap();
                let prev_version = db.get(key).map(|v| v.version).unwrap_or(0);
                let mut redis_value = RedisValue::new(ValueType::String(value.to_string()), None);
                redis_value.version = prev_version + 1;

                db.insert(key.to_string(), redis_value);

                Some(RespValue::SimpleString("OK".to_string()))
            }
            [option, seconds] if option.to_uppercase() == "EX" => {
                let mut db = db.lock().unwrap();
                let prev_version = db.get(key).map(|v| v.version).unwrap_or(0);

                let now = Instant::now();
                let expires_at = now + Duration::from_secs(seconds.parse().unwrap());

                let mut redis_value =
                    RedisValue::new(ValueType::String(value.to_string()), Some(expires_at));
                redis_value.version = prev_version + 1;

                db.insert(key.to_string(), redis_value);

                Some(RespValue::SimpleString("OK".to_string()))
            }
            [option, milliseconds] if option.to_uppercase() == "PX" => {
                let mut db = db.lock().unwrap();
                let prev_version = db.get(key).map(|v| v.version).unwrap_or(0);

                let now = Instant::now();
                let expires_at = now + Duration::from_millis(milliseconds.parse().unwrap());

                let mut redis_value =
                    RedisValue::new(ValueType::String(value.to_string()), Some(expires_at));
                redis_value.version = prev_version + 1;

                db.insert(key.to_string(), redis_value);

                Some(RespValue::SimpleString("OK".to_string()))
            }
            _ => unreachable!(),
        },
        _ => Some(RespValue::SimpleError("ERR syntax error".to_string())),
    }
}

pub fn execute_get(command: &[String], db: &Db) -> Option<RespValue> {
    match command {
        [cmd, key] if cmd.to_uppercase() == "GET" => {
            let db = db.lock().unwrap();

            if let Some(redis_value) = db.get(key) {
                match redis_value.expires_at {
                    Some(instant) => {
                        if Instant::now() > instant {
                            Some(RespValue::BulkStringNull)
                        } else {
                            match &redis_value.value {
                                ValueType::String(string) => {
                                    Some(RespValue::BulkString(string.to_string()))
                                }
                                _ => unimplemented!(),
                            }
                        }
                    }
                    None => match &redis_value.value {
                        ValueType::String(string) => {
                            Some(RespValue::BulkString(string.to_string()))
                        }
                        _ => unimplemented!(),
                    },
                }
            } else {
                Some(RespValue::BulkStringNull)
            }
        }
        _ => unreachable!(),
    }
}

pub fn execute_incr(command: &[String], db: &Db) -> Option<RespValue> {
    match command {
        [cmd, key] if cmd.to_uppercase() == "INCR" => {
            let mut db = db.lock().unwrap();

            if let Some(redis_value) = db.get_mut(key) {
                match &mut redis_value.value {
                    ValueType::String(string) => match string.parse::<i64>() {
                        Ok(n) => {
                            *string = format!("{}", n + 1);

                            Some(RespValue::Integers(n + 1))
                        }
                        Err(_) => Some(RespValue::SimpleError(
                            "ERR value is not an integer or out of range".to_string(),
                        )),
                    },
                    _ => {
                        unreachable!()
                    }
                }
            } else {
                let redis_value = RedisValue::new(ValueType::String("1".to_string()), None);

                db.insert(key.to_string(), redis_value);

                Some(RespValue::Integers(1))
            }
        }
        _ => unreachable!(),
    }
}
