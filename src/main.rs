#![allow(unused_imports)]
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Notify;
use tokio::time::timeout;

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

mod db;
mod resp;

use db::*;
use resp::*;

#[tokio::main]
async fn main() {
    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();

    let db = Arc::new(Mutex::new(HashMap::new()));
    let notify = Arc::new(Notify::new());

    loop {
        match listener.accept().await {
            Ok((stream, _addr)) => {
                let db = Arc::clone(&db);
                let notify = Arc::clone(&notify);

                tokio::spawn(async move {
                    handle_stream(stream, db, notify).await;
                });
            }
            Err(e) => println!("couldn't get client: {:?}", e),
        }
    }
}

async fn handle_stream(stream: TcpStream, db: Db, notify: Arc<Notify>) {
    let (rd, mut wr) = stream.into_split();
    let mut rd = BufReader::new(rd);

    let mut buf = [0u8; 512];

    loop {
        match rd.read(&mut buf).await {
            Ok(0) => break,
            Ok(n) => {
                let received = String::from_utf8_lossy(&buf[..n]);

                println!("received: {:?}", received);

                let resp_array = decode_arrays(&received);

                println!("resp_array: {:?}", resp_array);

                match resp_array.as_slice() {
                    [cmd] if cmd.to_uppercase() == "PING".to_string() => {
                        let _ = wr.write_all(b"+PONG\r\n").await;
                    }
                    [cmd, arg] if cmd.to_uppercase() == "ECHO".to_string() => {
                        let _ = wr
                            .write_all(encode_bulk_strings(arg.clone()).as_bytes())
                            .await;
                    }
                    [cmd, key, value, optional_args @ ..]
                        if cmd.to_uppercase() == "SET".to_string() =>
                    {
                        match optional_args {
                            [] => {
                                let redis_value =
                                    RedisValue::new(ValueType::String(value.to_string()), None);
                                {
                                    let mut db = db.lock().unwrap();
                                    db.insert(key.to_string(), redis_value);
                                }
                                let _ = wr
                                    .write_all(encode_simple_strings("OK".to_string()).as_bytes())
                                    .await;
                            }
                            [option, seconds] if option.to_uppercase() == "EX" => {
                                let now = Instant::now();
                                let expires_at =
                                    now + Duration::from_secs(seconds.parse().unwrap());

                                let redis_value = RedisValue::new(
                                    ValueType::String(value.to_string()),
                                    Some(expires_at),
                                );
                                {
                                    let mut db = db.lock().unwrap();
                                    db.insert(key.to_string(), redis_value);
                                }
                                let _ = wr
                                    .write_all(encode_simple_strings("OK".to_string()).as_bytes())
                                    .await;
                            }
                            [option, milliseconds] if option.to_uppercase() == "PX" => {
                                let now = Instant::now();
                                let expires_at =
                                    now + Duration::from_millis(milliseconds.parse().unwrap());

                                let redis_value = RedisValue::new(
                                    ValueType::String(value.to_string()),
                                    Some(expires_at),
                                );
                                {
                                    let mut db = db.lock().unwrap();
                                    db.insert(key.to_string(), redis_value);
                                }
                                let _ = wr
                                    .write_all(encode_simple_strings("OK".to_string()).as_bytes())
                                    .await;
                            }
                            _ => unreachable!(),
                        }
                    }
                    [cmd, key] if cmd.to_uppercase() == "GET".to_string() => {
                        let response = {
                            let db = db.lock().unwrap();

                            if let Some(redis_value) = db.get(key) {
                                match redis_value.expires_at {
                                    Some(instant) => {
                                        if Instant::now() > instant {
                                            encode_bulk_strings("".to_string())
                                        } else {
                                            match &redis_value.value {
                                                ValueType::String(string) => {
                                                    encode_bulk_strings(string.to_string())
                                                }
                                                _ => unimplemented!(),
                                            }
                                        }
                                    }
                                    None => match &redis_value.value {
                                        ValueType::String(string) => {
                                            encode_bulk_strings(string.to_string())
                                        }
                                        _ => unimplemented!(),
                                    },
                                }
                            } else {
                                encode_bulk_strings("".to_string())
                            }
                        };

                        let _ = wr.write_all(response.as_bytes()).await;
                    }
                    [cmd, list_key, list_values @ ..]
                        if cmd.to_uppercase() == "RPUSH".to_string() =>
                    {
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
                        let _ = wr
                            .write_all(encode_integers(list_length as i64).as_bytes())
                            .await;
                    }
                    [cmd, list_key, start_index, stop_index]
                        if cmd.to_uppercase() == "LRANGE".to_string() =>
                    {
                        let slice = {
                            let db = db.lock().unwrap();

                            if let Some(redis_value) = db.get(list_key) {
                                if let ValueType::List(list) = &redis_value.value {
                                    let list_length = list.len();
                                    let start_index: i64 = start_index.parse().unwrap();
                                    let stop_index: i64 = stop_index.parse().unwrap();

                                    let start = if start_index < 0
                                        && start_index.abs() > list_length as i64
                                    {
                                        0
                                    } else if start_index < 0 {
                                        (list_length as i64 + start_index).max(0) as usize
                                    } else {
                                        start_index as usize
                                    };

                                    let mut stop = if stop_index < 0
                                        && stop_index.abs() > list_length as i64
                                    {
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
                        let refs: Vec<&str> = slice.iter().map(|s| s.as_str()).collect();
                        let _ = wr.write_all(encode_arrays(&refs).as_bytes()).await;
                    }
                    [cmd, list_key, list_values @ ..]
                        if cmd.to_uppercase() == "LPUSH".to_string() =>
                    {
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
                        let _ = wr
                            .write_all(encode_integers(list_length as i64).as_bytes())
                            .await;
                    }
                    [cmd, list_key] if cmd.to_uppercase() == "LLEN".to_string() => {
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
                        let _ = wr
                            .write_all(encode_integers(response as i64).as_bytes())
                            .await;
                    }
                    [cmd, list_key, optional_args @ ..]
                        if cmd.to_uppercase() == "LPOP".to_string() =>
                    {
                        match optional_args {
                            [] => {
                                let removed = {
                                    let mut db = db.lock().unwrap();

                                    if let Some(redis_value) = db.get_mut(list_key) {
                                        match &mut redis_value.value {
                                            ValueType::List(list) => {
                                                if list.len() == 0 {
                                                    "".to_string()
                                                } else {
                                                    list.remove(0)
                                                }
                                            }
                                            _ => {
                                                unimplemented!()
                                            }
                                        }
                                    } else {
                                        "".to_string()
                                    }
                                };
                                let _ = wr.write_all(encode_bulk_strings(removed).as_bytes()).await;
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
                                let refs: Vec<&str> = removed.iter().map(|s| s.as_str()).collect();
                                let _ = wr.write_all(encode_arrays(&refs).as_bytes()).await;
                            }
                            _ => unimplemented!(),
                        }
                    }
                    [cmd, list_key, timeout_seconds]
                        if cmd.to_uppercase() == "BLPOP".to_string() =>
                    {
                        let seconds: f64 = timeout_seconds.parse().unwrap();
                        let removed = {
                            loop {
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
                                        notify.notified().await;
                                    }
                                    _ => {
                                        if let Err(_) = timeout(
                                            Duration::from_secs_f64(seconds),
                                            notify.notified(),
                                        )
                                        .await
                                        {
                                            let _ =
                                                wr.write_all(encode_null_array().as_bytes()).await;
                                            return;
                                        }
                                    }
                                }
                            }
                        };

                        let removed_ref = removed.as_str();
                        let response: &[&str] = &[list_key.as_str(), removed_ref];
                        let _ = wr.write_all(encode_arrays(response).as_bytes()).await;
                    }
                    [cmd, list_key] if cmd.to_uppercase() == "TYPE".to_string() => {
                        let type_of_value = {
                            let db = db.lock().unwrap();

                            if let Some(redis_value) = db.get(list_key) {
                                match &redis_value.value {
                                    ValueType::String(_) => "string".to_string(),
                                    ValueType::List(_) => "list".to_string(),
                                    ValueType::Set() => "set".to_string(),
                                    ValueType::Zset() => "zset".to_string(),
                                    ValueType::Hash() => "hash".to_string(),
                                    ValueType::Stream(_) => "stream".to_string(),
                                    ValueType::Vectorset() => "vectorset".to_string(),
                                }
                            } else {
                                "none".to_string()
                            }
                        };

                        let _ = wr
                            .write_all(encode_simple_strings(type_of_value).as_bytes())
                            .await;
                    }
                    [cmd, stream_key, entry_id, pairs @ ..]
                        if cmd.to_uppercase() == "XADD".to_string() =>
                    {
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

                                            println!("last_milliseconds : {}", last_milliseconds);
                                            println!("last_sequence_number : {}", last_sequence_number);

                                            println!("current_milliseconds : {}", current_milliseconds);
                                            println!("current_sequence_number : {}", current_sequence_number);
                                            
                                            if last_milliseconds.parse::<u64>().unwrap()
                                                > current_milliseconds.parse::<u64>().unwrap()
                                            {
                                                println!("reaching here??");
                                                "ERR The ID specified in XADD is equal or smaller than the target stream top item".to_string()
                                            } else if last_milliseconds.parse::<u64>().unwrap()
                                                == current_milliseconds.parse::<u64>().unwrap()
                                                && last_sequence_number.parse::<u64>().unwrap()
                                                    >= current_sequence_number
                                                        .parse::<u64>()
                                                        .unwrap()
                                            {
                                                "ERR The ID specified in XADD is equal or smaller than the target stream top item".to_string()
                                            } else {
                                                "".to_string()
                                            }
                                        } else {
                                            unreachable!()
                                        }
                                    }
                                    _ => {
                                        unimplemented!()
                                    }
                                }
                            } else {
                                let (current_milliseconds, current_sequence_number) = entry_id.split_once("-").unwrap();

                                if current_milliseconds.parse::<u64>().unwrap() == 0 && current_sequence_number.parse::<u64>().unwrap() == 0 {
                                    "ERR The ID specified in XADD must be greater than 0-0".to_string()
                                } else {
                                    "".to_string()
                                }
                            }
                        };

                        if !error_message.is_empty() {
                            println!("reaching here!!!?");
                            let _ = wr
                                .write_all(encode_simple_errors(error_message).as_bytes())
                                .await;
                            continue;
                        }

                        let response = {
                            let mut db = db.lock().unwrap();

                            if let Some(redis_value) = db.get_mut(stream_key) {
                                match &mut redis_value.value {
                                    ValueType::Stream(stream) => {
                                        let fields = pairs
                                            .chunks(2)
                                            .map(|e| (e[0].clone(), e[1].clone()))
                                            .collect();
                                        let stream_entry =
                                            StreamEntry::new(entry_id.to_string(), fields);

                                        stream.push(stream_entry);
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
                                entry_id.to_string()
                            }
                        };
                        let _ = wr.write_all(encode_bulk_strings(response).as_bytes()).await;
                    }
                    _ => unreachable!(),
                }
            }
            Err(_) => break,
        }
    }
}

// TODO : XADD 구현하고 enum 에 impl 붙이는 작업하기
