use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tokio::sync::Notify;
use tokio::time::timeout;

use crate::protocol::RespValue;
use crate::types::{Db, RedisValue, StreamEntry, ValueType};

pub fn execute_xadd(command: &[String], db: &Db, notify: &Arc<Notify>) -> Option<RespValue> {
    match command {
        [cmd, stream_key, entry_id, pairs @ ..] if cmd.to_uppercase() == "XADD" => {
            // generate entry id
            let (generated_milliseconds, generated_sqeuence_number) = {
                let (current_milliseconds, current_sequence_number) = match entry_id.split_once("-")
                {
                    Some((a, b)) => (a, b),
                    None => ("*", "*"),
                };

                match (current_milliseconds, current_sequence_number) {
                    ("*", "*") => {
                        let db = db.lock().unwrap();

                        let unix_time_millis = SystemTime::now()
                            .duration_since(UNIX_EPOCH)
                            .unwrap()
                            .as_millis() as u64;

                        if let Some(redis_value) = db.get(stream_key) {
                            match &redis_value.value {
                                ValueType::Stream(stream) => {
                                    let mut last_entry_with_same_time = String::new();

                                    for el in stream {
                                        let entry_id = el.get_entry_id();
                                        match entry_id.split_once("-") {
                                            Some((a, b)) => {
                                                if a.to_string() == unix_time_millis.to_string() {
                                                    last_entry_with_same_time = b.to_string();
                                                }
                                            }
                                            None => {
                                                unreachable!()
                                            }
                                        }
                                    }
                                    let next_seq = if last_entry_with_same_time.is_empty() {
                                        0
                                    } else {
                                        last_entry_with_same_time.parse::<u64>().unwrap() + 1
                                    };
                                    (unix_time_millis.to_string(), next_seq.to_string())
                                }
                                _ => {
                                    unimplemented!()
                                }
                            }
                        } else {
                            (unix_time_millis.to_string(), "0".to_string())
                        }
                    }
                    (current_milliseconds, "*") => {
                        let db = db.lock().unwrap();

                        if let Some(redis_value) = db.get(stream_key) {
                            match &redis_value.value {
                                ValueType::Stream(stream) => {
                                    if let Some(last) = stream.last() {
                                        let last_entry_id = last.get_entry_id();

                                        let (last_milliseconds, last_sequence_number) =
                                            last_entry_id.split_once("-").unwrap();

                                        if current_milliseconds.parse::<u64>().unwrap() == 0 {
                                            (current_milliseconds.to_string(), "1".to_string())
                                        } else if last_milliseconds != current_milliseconds {
                                            (current_milliseconds.to_string(), "0".to_string())
                                        } else if last_milliseconds == current_milliseconds {
                                            (
                                                current_milliseconds.to_string(),
                                                (last_sequence_number.parse::<u64>().unwrap() + 1)
                                                    .to_string(),
                                            )
                                        } else {
                                            unimplemented!()
                                        }
                                    } else {
                                        (current_milliseconds.to_string(), "0".to_string())
                                    }
                                }
                                _ => {
                                    unimplemented!()
                                }
                            }
                        } else {
                            if current_milliseconds.parse::<u64>().unwrap() == 0 {
                                (current_milliseconds.to_string(), "1".to_string())
                            } else {
                                (current_milliseconds.to_string(), "0".to_string())
                            }
                        }
                    }
                    _ => (
                        current_milliseconds.to_string(),
                        current_sequence_number.to_string(),
                    ),
                }
            };

            let entry_id = format!("{}-{}", generated_milliseconds, generated_sqeuence_number);

            // validate entry id
            let error_message = {
                let mut db = db.lock().unwrap();

                if let Some(redis_value) = db.get_mut(stream_key) {
                    match &mut redis_value.value {
                        ValueType::Stream(stream) => {
                            if let Some(last) = stream.last() {
                                let last_entry_id = last.get_entry_id();

                                let (last_milliseconds, last_sequence_number) =
                                    last_entry_id.split_once("-").unwrap();
                                let (current_milliseconds, current_sequence_number) =
                                    entry_id.split_once("-").unwrap();

                                let last_milliseconds = last_milliseconds.parse::<u64>().unwrap();
                                let last_sequence_number =
                                    last_sequence_number.parse::<u64>().unwrap();
                                let current_milliseconds =
                                    current_milliseconds.parse::<u64>().unwrap();
                                let current_sequence_number =
                                    current_sequence_number.parse::<u64>().unwrap();

                                if current_milliseconds == 0 && current_sequence_number == 0 {
                                    "ERR The ID specified in XADD must be greater than 0-0"
                                        .to_string()
                                } else if last_milliseconds > current_milliseconds {
                                    "ERR The ID specified in XADD is equal or smaller than the target stream top item".to_string()
                                } else if last_milliseconds == current_milliseconds
                                    && last_sequence_number >= current_sequence_number
                                {
                                    "ERR The ID specified in XADD is equal or smaller than the target stream top item".to_string()
                                } else {
                                    "".to_string()
                                }
                            } else {
                                "".to_string()
                            }
                        }
                        _ => {
                            unimplemented!()
                        }
                    }
                } else {
                    let (current_milliseconds, current_sequence_number) =
                        entry_id.split_once("-").unwrap();

                    let current_milliseconds = current_milliseconds.parse::<u64>().unwrap();
                    let current_sequence_number = current_sequence_number.parse::<u64>().unwrap();

                    if current_milliseconds == 0 && current_sequence_number == 0 {
                        "ERR The ID specified in XADD must be greater than 0-0".to_string()
                    } else {
                        "".to_string()
                    }
                }
            };

            if !error_message.is_empty() {
                return Some(RespValue::SimpleError(error_message));
            }

            // if config.appendonly == "yes" && is_write_command(&command) {
            //     append_to_aof(&command, &config);
            // }

            // respond
            let response = {
                let mut db = db.lock().unwrap();

                if let Some(redis_value) = db.get_mut(stream_key) {
                    match &mut redis_value.value {
                        ValueType::Stream(stream) => {
                            let fields = pairs
                                .chunks(2)
                                .map(|e| (e[0].clone(), e[1].clone()))
                                .collect();
                            let stream_entry = StreamEntry::new(entry_id.to_string(), fields);

                            stream.push(stream_entry);

                            notify.notify_one();

                            entry_id.to_string()
                        }
                        _ => {
                            unimplemented!()
                        }
                    }
                } else {
                    let fields = pairs
                        .chunks(2)
                        .map(|e| (e[0].clone(), e[1].clone()))
                        .collect();
                    let stream_entry = StreamEntry::new(entry_id.to_string(), fields);

                    let value = ValueType::Stream(vec![stream_entry]);
                    let redis_value = RedisValue::new(value, None);

                    db.insert(stream_key.to_string(), redis_value);

                    notify.notify_one();

                    entry_id.to_string()
                }
            };
            Some(RespValue::BulkString(response))
        }
        _ => unreachable!(),
    }
}

pub fn execute_xrange(command: &[String], db: &Db) -> Option<RespValue> {
    match command {
        [cmd, stream_key, start_id, end_id] if cmd.to_uppercase() == "XRANGE" => {
            let filtered = {
                let db = db.lock().unwrap();

                if let Some(redis_value) = db.get(stream_key) {
                    match &redis_value.value {
                        ValueType::Stream(stream) => {
                            let (sm, ss) = match start_id.split_once("-") {
                                Some((m, s)) => {
                                    if m.is_empty() && s.is_empty() {
                                        (0, 0)
                                    } else {
                                        (m.parse::<u64>().unwrap(), s.parse::<u64>().unwrap())
                                    }
                                }
                                None => (start_id.parse::<u64>().unwrap(), 0),
                            };

                            let (em, es) = match end_id.split_once("-") {
                                Some((m, s)) => {
                                    (m.parse::<u64>().unwrap(), s.parse::<u64>().unwrap())
                                }
                                None => {
                                    if end_id == "+" {
                                        (u64::MAX, u64::MAX)
                                    } else {
                                        (end_id.parse::<u64>().unwrap(), u64::MAX)
                                    }
                                }
                            };

                            let filtered = stream
                                .iter()
                                .filter(|e| {
                                    let entry_id = e.get_entry_id();
                                    let (m, s) = entry_id.split_once("-").unwrap();
                                    let (m, s) =
                                        (m.parse::<u64>().unwrap(), s.parse::<u64>().unwrap());
                                    (m, s) >= (sm, ss) && (m, s) <= (em, es)
                                })
                                .cloned()
                                .collect::<Vec<_>>();

                            println!("filtered : {:?}", filtered);

                            filtered
                        }
                        _ => {
                            unimplemented!()
                        }
                    }
                } else {
                    unimplemented!()
                }
            };

            let response = filtered
                .iter()
                .map(|e| e.to_resp_value())
                .collect::<Vec<_>>();

            Some(RespValue::Array(response))
        }
        _ => unreachable!(),
    }
}

pub async fn execute_xread(command: &[String], db: &Db, notify: &Arc<Notify>) -> Option<RespValue> {
    match command {
        [cmd, rest @ ..] if cmd.to_uppercase() == "XREAD" => {
            let (block_ms, rest) = if rest[0].to_uppercase() == "BLOCK" {
                (Some(rest[1].parse::<u64>().unwrap_or(0)), &rest[3..]) // skip BLOCK ms STREAMS
            } else {
                (None, &rest[1..]) // skip STREAMS
            };

            let half = rest.len() / 2;
            let keys = &rest[..half];
            let ids = &rest[half..];

            let mut streams = Vec::new();

            for (stream_key, entry_id) in keys.iter().zip(ids) {
                let mut stream = Vec::new();

                let filtered = {
                    let resolved = match entry_id.as_str() {
                        "$" => {
                            let db = db.lock().unwrap();
                            if let Some(rv) = db.get(stream_key) {
                                if let ValueType::Stream(s) = &rv.value {
                                    s.last()
                                        .map(|e| {
                                            let id = e.get_entry_id();
                                            let (m, s) = id.split_once("-").unwrap();
                                            (m.parse::<u64>().unwrap(), s.parse::<u64>().unwrap())
                                        })
                                        .unwrap_or((0, 0))
                                } else {
                                    (0, 0)
                                }
                            } else {
                                (0, 0)
                            }
                        }
                        _ => {
                            let (m, s) = entry_id.split_once("-").unwrap();
                            (m.parse::<u64>().unwrap(), s.parse::<u64>().unwrap())
                        }
                    };

                    loop {
                        let notified = notify.notified();

                        let has_value = {
                            let db = db.lock().unwrap();
                            if let Some(redis_value) = db.get(stream_key) {
                                if let ValueType::Stream(stream) = &redis_value.value {
                                    let filtered = stream
                                        .iter()
                                        .filter(|e| {
                                            let entry_id = e.get_entry_id();
                                            let (m, s) = entry_id.split_once("-").unwrap();
                                            let (m, s) = (
                                                m.parse::<u64>().unwrap(),
                                                s.parse::<u64>().unwrap(),
                                            );
                                            (m, s) > resolved
                                        })
                                        .cloned()
                                        .collect::<Vec<_>>();

                                    if filtered.is_empty() { false } else { true }
                                } else {
                                    unimplemented!()
                                }
                            } else {
                                false
                            }
                        };

                        if has_value {
                            let db = db.lock().unwrap();
                            if let Some(redis_value) = db.get(stream_key) {
                                match &redis_value.value {
                                    ValueType::Stream(stream) => {
                                        let filtered = stream
                                            .iter()
                                            .filter(|e| {
                                                let entry_id = e.get_entry_id();
                                                let (m, s) = entry_id.split_once("-").unwrap();
                                                let (m, s) = (
                                                    m.parse::<u64>().unwrap(),
                                                    s.parse::<u64>().unwrap(),
                                                );
                                                (m, s) > resolved
                                            })
                                            .cloned()
                                            .collect::<Vec<_>>();

                                        break filtered;
                                    }
                                    _ => {
                                        unimplemented!()
                                    }
                                }
                            } else {
                                unimplemented!()
                            }
                        }

                        match block_ms {
                            Some(block_ms) => match block_ms {
                                0 => {
                                    notified.await;
                                }
                                n => {
                                    if let Err(_) =
                                        timeout(Duration::from_millis(n), notified).await
                                    {
                                        return None;
                                    }
                                }
                            },
                            None => {
                                let key_exists = {
                                    let db = db.lock().unwrap();
                                    if db.get(stream_key).is_none() {
                                        false
                                    } else {
                                        true
                                    }
                                };
                                if !key_exists {
                                    return None;
                                }

                                break Vec::new();
                            }
                        }
                    }
                };

                let filtered_resp_value = filtered
                    .iter()
                    .map(|e| e.to_resp_value())
                    .collect::<Vec<_>>();

                stream.push(RespValue::BulkString(stream_key.to_string()));
                stream.push(RespValue::Array(filtered_resp_value));

                streams.push(RespValue::Array(stream));
            }

            Some(RespValue::Array(streams))
        }
        _ => unreachable!(),
    }
}
