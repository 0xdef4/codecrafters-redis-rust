use std::sync::Arc;

use crate::Config;
use crate::protocol::RespValue;
use crate::types::{Db, ValueType};

pub fn execute_ping(command: &[String]) -> Option<RespValue> {
    match command {
        [cmd] if cmd.to_uppercase() == "PING".to_string() => {
            Some(RespValue::SimpleString("PONG".to_string()))
        }
        _ => unreachable!(),
    }
}

pub fn execute_echo(command: &[String]) -> Option<RespValue> {
    match command {
        [cmd, arg] if cmd.to_uppercase() == "ECHO".to_string() => {
            Some(RespValue::BulkString(arg.clone()))
        }
        _ => unreachable!(),
    }
}

pub fn execute_info(command: &[String], role: &str) -> Option<RespValue> {
    match command {
        [cmd, optional] if cmd.to_uppercase() == "INFO".to_string() => match optional {
            option if option.to_uppercase() == "REPLICATION".to_string() => match role {
                "slave" => Some(RespValue::BulkString("role:slave".to_string())),
                "master" => {
                    Some(RespValue::BulkString("role:master\r\nmaster_replid:8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb\r\nmaster_repl_offset:0".to_string()))
                }
                _ => {
                    unreachable!()
                }
            },
            _ => {
                unimplemented!()
            }
        },
        _ => unreachable!(),
    }
}

pub fn execute_config(command: &[String], config: &Arc<Config>) -> Option<RespValue> {
    match command {
        [cmd, subcmd, rest @ ..]
            if cmd.to_uppercase() == "CONFIG".to_string()
                && subcmd.to_uppercase() == "GET".to_string() =>
        {
            match rest {
                [] => {
                    unimplemented!()
                }
                [val] => match val.as_str() {
                    "dir" => Some(RespValue::Array(vec![
                        RespValue::BulkString("dir".to_string()),
                        RespValue::BulkString(config.dir.to_string_lossy().to_string()),
                    ])),
                    "dbfilename" => Some(RespValue::Array(vec![
                        RespValue::BulkString("dbfilename".to_string()),
                        RespValue::BulkString(
                            config.dbfilename.as_deref().unwrap_or_default().to_string(),
                        ),
                    ])),
                    "appendonly" => Some(RespValue::Array(vec![
                        RespValue::BulkString("appendonly".to_string()),
                        RespValue::BulkString(config.appendonly.clone()),
                    ])),
                    "appenddirname" => Some(RespValue::Array(vec![
                        RespValue::BulkString("appenddirname".to_string()),
                        RespValue::BulkString(config.appenddirname.clone()),
                    ])),
                    "appendfilename" => Some(RespValue::Array(vec![
                        RespValue::BulkString("appendfilename".to_string()),
                        RespValue::BulkString(config.appendfilename.clone()),
                    ])),
                    "appendfsync" => Some(RespValue::Array(vec![
                        RespValue::BulkString("appendfsync".to_string()),
                        RespValue::BulkString(config.appendfsync.clone()),
                    ])),
                    _ => {
                        unimplemented!()
                    }
                },
                _ => {
                    unimplemented!()
                }
            }
        }
        _ => unreachable!(),
    }
}

pub fn execute_keys(command: &[String], db: &Db) -> Option<RespValue> {
    match command {
        [cmd, pattern] if cmd.to_uppercase() == "KEYS" => {
            let keys: Vec<RespValue> = {
                let db = db.lock().unwrap();
                db.keys()
                    .map(|k| RespValue::BulkString(k.clone()))
                    .collect()
            };

            Some(RespValue::Array(keys))
        }
        _ => unreachable!(),
    }
}

pub fn execute_type(command: &[String], db: &Db) -> Option<RespValue> {
    match command {
        [cmd, list_key] if cmd.to_uppercase() == "TYPE".to_string() => {
            let type_of_value = {
                let db = db.lock().unwrap();

                if let Some(redis_value) = db.get(list_key) {
                    match &redis_value.value {
                        ValueType::String(_) => "string".to_string(),
                        ValueType::List(_) => "list".to_string(),
                        ValueType::Set() => "set".to_string(),
                        ValueType::Zset(_) => "zset".to_string(),
                        ValueType::Hash() => "hash".to_string(),
                        ValueType::Stream(_) => "stream".to_string(),
                        ValueType::Vectorset() => "vectorset".to_string(),
                    }
                } else {
                    "none".to_string()
                }
            };

            Some(RespValue::SimpleString(type_of_value))
        }
        _ => unreachable!(),
    }
}
