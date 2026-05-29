use std::collections::HashMap;
use std::sync::Arc;

use tokio::sync::Notify;

use crate::Config;
use crate::commands::dispatch_command_inner;
use crate::protocol::RespValue;
use crate::types::Db;

pub fn execute_multi(command: &[String], in_multi: &mut bool) -> Option<RespValue> {
    match command {
        [cmd] if cmd.to_uppercase() == "MULTI".to_string() => {
            *in_multi = true;

            Some(RespValue::SimpleString("OK".to_string()))
        }
        _ => unreachable!(),
    }
}

pub async fn execute_exec(
    command: &[String],
    db: &Db,
    notify: &Arc<Notify>,
    config: &Arc<Config>,
    in_multi: &mut bool,
    command_queue: &mut Vec<Vec<String>>,
    watched_keys: &mut HashMap<String, u64>,
) -> Option<RespValue> {
    match command {
        [cmd] if cmd.to_uppercase() == "EXEC".to_string() => {
            let mut responses = Vec::new();

            if *in_multi {
                let is_dirty: bool = {
                    let db = db.lock().unwrap();
                    watched_keys.iter().any(|(key, &watched_version)| {
                        db.get(key).map(|v| v.version).unwrap_or(0) != watched_version
                    })
                };

                if is_dirty {
                    *in_multi = false;
                    command_queue.clear();
                    watched_keys.clear();

                    return Some(RespValue::ArrayNull);
                } else {
                    for command in command_queue.clone() {
                        if let Some(resp) = dispatch_command_inner(&command, db, notify, config) {
                            responses.push(resp);
                        }
                    }

                    *in_multi = false;
                    command_queue.clear();
                    watched_keys.clear();

                    return Some(RespValue::Array(responses));
                }
            } else {
                Some(RespValue::SimpleError("ERR EXEC without MULTI".to_string()))
            }
        }
        _ => unreachable!(),
    }
}

pub fn execute_discard(
    command: &[String],
    in_multi: &mut bool,
    command_queue: &mut Vec<Vec<String>>,
    watched_keys: &mut HashMap<String, u64>,
) -> Option<RespValue> {
    match command {
        [cmd] if cmd.to_uppercase() == "DISCARD".to_string() => {
            if *in_multi {
                command_queue.clear();
                watched_keys.clear();

                *in_multi = false;

                Some(RespValue::SimpleString("OK".to_string()))
            } else {
                Some(RespValue::SimpleError(
                    "ERR DISCARD without MULTI".to_string(),
                ))
            }
        }
        _ => unreachable!(),
    }
}

pub fn execute_watch(
    command: &[String],
    db: &Db,
    in_multi: &mut bool,
    watched_keys: &mut HashMap<String, u64>,
) -> Option<RespValue> {
    match command {
        [cmd, keys @ ..] if cmd.to_uppercase() == "WATCH".to_string() => {
            if *in_multi {
                Some(RespValue::SimpleError(
                    "ERR WATCH inside MULTI is not allowed".to_string(),
                ))
            } else {
                for key in keys {
                    let version: Option<u64> = {
                        let db = db.lock().unwrap();
                        if let Some(redis_value) = db.get(&key.to_string()) {
                            Some(redis_value.version)
                        } else {
                            None
                        }
                    };

                    match version {
                        Some(version) => {
                            watched_keys.insert(key.to_string(), version);
                        }
                        None => {
                            watched_keys.insert(key.to_string(), 0);
                        }
                    }
                }

                Some(RespValue::SimpleString("OK".to_string()))
            }
        }
        _ => unreachable!(),
    }
}

pub fn execute_unwatch(
    command: &[String],
    watched_keys: &mut HashMap<String, u64>,
) -> Option<RespValue> {
    match command {
        [cmd] if cmd.to_uppercase() == "UNWATCH".to_string() => {
            watched_keys.clear();

            Some(RespValue::SimpleString("OK".to_string()))
        }
        _ => unreachable!(),
    }
}
