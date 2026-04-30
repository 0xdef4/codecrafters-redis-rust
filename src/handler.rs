use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use tokio::sync::{Notify, mpsc};
use tokio::time::timeout;

use std::collections::HashSet;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use crate::{
    Config, Db, Pubsub, RedisValue, Replicas, RespValue, StreamEntry, ValueType, decode_arrays,
    encode,
};

pub async fn handle_stream(
    stream: TcpStream,
    db: Db,
    notify: Arc<Notify>,
    role: String,
    replicas: Replicas,
    config: Arc<Config>,
    pubsub: Pubsub,
) {
    let mut in_multi: bool = false;
    let mut command_queue: Vec<Vec<String>> = Vec::new();
    let mut subscribed_channels: HashSet<String> = HashSet::new();

    let mut master_repl_offset: usize = 0;

    let (rd, mut wr) = stream.into_split();
    let mut rd = BufReader::new(rd);

    let mut buf = [0u8; 512];

    loop {
        match rd.read(&mut buf).await {
            Ok(0) => break,
            Ok(n) => {
                let received = String::from_utf8_lossy(&buf[..n]);
                println!("received: {:?}", received);

                let commands = decode_arrays(&received);
                for resp_array in commands {
                    println!("resp_array: {:?}", resp_array);
                    let cmd_upper = resp_array[0].to_uppercase();

                    if in_multi
                        && cmd_upper != "EXEC"
                        && cmd_upper != "MULTI"
                        && cmd_upper != "DISCARD"
                    {
                        command_queue.push(resp_array.clone());
                        let _ = wr
                            .write_all(
                                encode(RespValue::SimpleString("QUEUED".to_string())).as_bytes(),
                            )
                            .await;
                        continue;
                    }

                    match resp_array.as_slice() {
                        [cmd] if cmd.to_uppercase() == "PING".to_string() => {
                            let _ = wr.write_all(b"+PONG\r\n").await;
                        }
                        [cmd, arg] if cmd.to_uppercase() == "ECHO".to_string() => {
                            let _ = wr
                                .write_all(encode(RespValue::BulkString(arg.clone())).as_bytes())
                                .await;
                        }
                        [cmd, key, value, optional_args @ ..]
                            if cmd.to_uppercase() == "SET".to_string() =>
                        {
                            let resp = execute_single_command(&resp_array, &db);
                            let _ = wr.write_all(encode(resp).as_bytes()).await;

                            if role == "master" {
                                let command_to_propagate = RespValue::Array(
                                    resp_array
                                        .iter()
                                        .map(|e| RespValue::BulkString(e.clone()))
                                        .collect::<Vec<_>>(),
                                );

                                master_repl_offset +=
                                    encode(command_to_propagate.clone()).as_bytes().len();

                                let mut replicas = replicas.lock().await;
                                for (replica_writer, _replica_reader) in replicas.iter_mut() {
                                    let _ = replica_writer
                                        .write_all(encode(command_to_propagate.clone()).as_bytes())
                                        .await;
                                }
                            }
                        }
                        [cmd, key] if cmd.to_uppercase() == "GET".to_string() => {
                            let resp = execute_single_command(&resp_array, &db);
                            let _ = wr.write_all(encode(resp).as_bytes()).await;
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
                                .write_all(
                                    encode(RespValue::Integers(list_length as i64)).as_bytes(),
                                )
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
                            let array = RespValue::Array(
                                slice
                                    .iter()
                                    .map(|s| RespValue::BulkString(s.clone()))
                                    .collect(),
                            );
                            let _ = wr.write_all(encode(array).as_bytes()).await;
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
                                .write_all(
                                    encode(RespValue::Integers(list_length as i64)).as_bytes(),
                                )
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
                                .write_all(encode(RespValue::Integers(response as i64)).as_bytes())
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
                                    let _ = wr
                                        .write_all(
                                            encode(RespValue::BulkString(removed)).as_bytes(),
                                        )
                                        .await;
                                }
                                [num_to_remove] => {
                                    let removed = {
                                        let mut db = db.lock().unwrap();

                                        if let Some(redis_value) = db.get_mut(list_key) {
                                            match &mut redis_value.value {
                                                ValueType::List(list) => list
                                                    .drain(
                                                        ..num_to_remove.parse::<usize>().unwrap(),
                                                    )
                                                    .collect::<Vec<_>>(),
                                                _ => {
                                                    unimplemented!()
                                                }
                                            }
                                        } else {
                                            unimplemented!()
                                        }
                                    };
                                    let array = RespValue::Array(
                                        removed
                                            .iter()
                                            .map(|e| RespValue::BulkString(e.clone()))
                                            .collect(),
                                    );
                                    let _ = wr.write_all(encode(array).as_bytes()).await;
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
                                                timeout(Duration::from_secs_f64(seconds), notified)
                                                    .await
                                            {
                                                let _ = wr
                                                    .write_all(
                                                        encode(RespValue::ArrayNull).as_bytes(),
                                                    )
                                                    .await;
                                                return;
                                            }
                                        }
                                    }
                                }
                            };

                            let response = RespValue::Array(vec![
                                RespValue::BulkString(list_key.to_string()),
                                RespValue::BulkString(removed),
                            ]);
                            let _ = wr.write_all(encode(response).as_bytes()).await;
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
                                .write_all(
                                    encode(RespValue::SimpleString(type_of_value)).as_bytes(),
                                )
                                .await;
                        }
                        [cmd, stream_key, entry_id, pairs @ ..]
                            if cmd.to_uppercase() == "XADD".to_string() =>
                        {
                            // generate entry id
                            let (generated_milliseconds, generated_sqeuence_number) = {
                                let (current_milliseconds, current_sequence_number) =
                                    match entry_id.split_once("-") {
                                        Some((a, b)) => (a, b),
                                        None => ("*", "*"),
                                    };

                                match (current_milliseconds, current_sequence_number) {
                                    ("*", "*") => {
                                        let db = db.lock().unwrap();

                                        let unix_time_millis = SystemTime::now()
                                            .duration_since(UNIX_EPOCH)
                                            .unwrap()
                                            .as_millis()
                                            as u64;

                                        if let Some(redis_value) = db.get(stream_key) {
                                            match &redis_value.value {
                                                ValueType::Stream(stream) => {
                                                    let mut last_entry_with_same_time =
                                                        String::new();

                                                    for el in stream {
                                                        let entry_id = el.get_entry_id();
                                                        match entry_id.split_once("-") {
                                                            Some((a, b)) => {
                                                                if a.to_string()
                                                                    == unix_time_millis.to_string()
                                                                {
                                                                    last_entry_with_same_time =
                                                                        b.to_string();
                                                                }
                                                            }
                                                            None => {
                                                                unreachable!()
                                                            }
                                                        }
                                                    }
                                                    let next_seq =
                                                        if last_entry_with_same_time.is_empty() {
                                                            0
                                                        } else {
                                                            last_entry_with_same_time
                                                                .parse::<u64>()
                                                                .unwrap()
                                                                + 1
                                                        };
                                                    (
                                                        unix_time_millis.to_string(),
                                                        next_seq.to_string(),
                                                    )
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

                                                        let (
                                                            last_milliseconds,
                                                            last_sequence_number,
                                                        ) = last_entry_id.split_once("-").unwrap();

                                                        if current_milliseconds
                                                            .parse::<u64>()
                                                            .unwrap()
                                                            == 0
                                                        {
                                                            (
                                                                current_milliseconds.to_string(),
                                                                "1".to_string(),
                                                            )
                                                        } else if last_milliseconds
                                                            != current_milliseconds
                                                        {
                                                            (
                                                                current_milliseconds.to_string(),
                                                                "0".to_string(),
                                                            )
                                                        } else if last_milliseconds
                                                            == current_milliseconds
                                                        {
                                                            (
                                                                current_milliseconds.to_string(),
                                                                (last_sequence_number
                                                                    .parse::<u64>()
                                                                    .unwrap()
                                                                    + 1)
                                                                .to_string(),
                                                            )
                                                        } else {
                                                            unimplemented!()
                                                        }
                                                    } else {
                                                        (
                                                            current_milliseconds.to_string(),
                                                            "0".to_string(),
                                                        )
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

                            let entry_id =
                                format!("{}-{}", generated_milliseconds, generated_sqeuence_number);

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

                                                let last_milliseconds =
                                                    last_milliseconds.parse::<u64>().unwrap();
                                                let last_sequence_number =
                                                    last_sequence_number.parse::<u64>().unwrap();
                                                let current_milliseconds =
                                                    current_milliseconds.parse::<u64>().unwrap();
                                                let current_sequence_number =
                                                    current_sequence_number.parse::<u64>().unwrap();

                                                if current_milliseconds == 0
                                                    && current_sequence_number == 0
                                                {
                                                    "ERR The ID specified in XADD must be greater than 0-0".to_string()
                                                } else if last_milliseconds > current_milliseconds {
                                                    "ERR The ID specified in XADD is equal or smaller than the target stream top item".to_string()
                                                } else if last_milliseconds == current_milliseconds
                                                    && last_sequence_number
                                                        >= current_sequence_number
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

                                    let current_milliseconds =
                                        current_milliseconds.parse::<u64>().unwrap();
                                    let current_sequence_number =
                                        current_sequence_number.parse::<u64>().unwrap();

                                    if current_milliseconds == 0 && current_sequence_number == 0 {
                                        "ERR The ID specified in XADD must be greater than 0-0"
                                            .to_string()
                                    } else {
                                        "".to_string()
                                    }
                                }
                            };

                            if !error_message.is_empty() {
                                let _ = wr
                                    .write_all(
                                        encode(RespValue::SimpleError(error_message)).as_bytes(),
                                    )
                                    .await;
                                continue;
                            }

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
                                            let stream_entry =
                                                StreamEntry::new(entry_id.to_string(), fields);

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
                                    let stream_entry =
                                        StreamEntry::new(entry_id.to_string(), fields);

                                    let value = ValueType::Stream(vec![stream_entry]);
                                    let redis_value = RedisValue::new(value, None);

                                    db.insert(stream_key.to_string(), redis_value);

                                    notify.notify_one();

                                    entry_id.to_string()
                                }
                            };
                            let _ = wr
                                .write_all(encode(RespValue::BulkString(response)).as_bytes())
                                .await;
                        }
                        [cmd, stream_key, start_id, end_id]
                            if cmd.to_uppercase() == "XRANGE".to_string() =>
                        {
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
                                                        (
                                                            m.parse::<u64>().unwrap(),
                                                            s.parse::<u64>().unwrap(),
                                                        )
                                                    }
                                                }
                                                None => (start_id.parse::<u64>().unwrap(), 0),
                                            };

                                            let (em, es) = match end_id.split_once("-") {
                                                Some((m, s)) => (
                                                    m.parse::<u64>().unwrap(),
                                                    s.parse::<u64>().unwrap(),
                                                ),
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
                                                    let (m, s) = (
                                                        m.parse::<u64>().unwrap(),
                                                        s.parse::<u64>().unwrap(),
                                                    );
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

                            let _ = wr
                                .write_all(encode(RespValue::Array(response)).as_bytes())
                                .await;
                        }
                        [cmd, rest @ ..] if cmd.to_uppercase() == "XREAD".to_string() => {
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
                                                            let (m, s) =
                                                                id.split_once("-").unwrap();
                                                            (
                                                                m.parse::<u64>().unwrap(),
                                                                s.parse::<u64>().unwrap(),
                                                            )
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
                                                if let ValueType::Stream(stream) =
                                                    &redis_value.value
                                                {
                                                    let filtered = stream
                                                        .iter()
                                                        .filter(|e| {
                                                            let entry_id = e.get_entry_id();
                                                            let (m, s) =
                                                                entry_id.split_once("-").unwrap();
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
                                                                let (m, s) = entry_id
                                                                    .split_once("-")
                                                                    .unwrap();
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
                                                        timeout(Duration::from_millis(n), notified)
                                                            .await
                                                    {
                                                        let _ = wr
                                                            .write_all(
                                                                encode(RespValue::ArrayNull)
                                                                    .as_bytes(),
                                                            )
                                                            .await;
                                                        return;
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
                                                    let _ = wr
                                                        .write_all(
                                                            encode(RespValue::ArrayNull).as_bytes(),
                                                        )
                                                        .await;
                                                    return;
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

                            let _ = wr
                                .write_all(encode(RespValue::Array(streams)).as_bytes())
                                .await;
                        }
                        [cmd, key] if cmd.to_uppercase() == "INCR".to_string() => {
                            let resp = execute_single_command(&resp_array, &db);
                            let _ = wr.write_all(encode(resp).as_bytes()).await;
                        }
                        [cmd] if cmd.to_uppercase() == "MULTI".to_string() => {
                            in_multi = true;

                            let _ = wr
                                .write_all(
                                    encode(RespValue::SimpleString("OK".to_string())).as_bytes(),
                                )
                                .await;
                        }
                        [cmd] if cmd.to_uppercase() == "EXEC".to_string() => {
                            let mut responses = Vec::new();

                            if in_multi {
                                for command in &command_queue {
                                    let response: RespValue = execute_single_command(command, &db);

                                    responses.push(response);
                                }

                                let _ = wr
                                    .write_all(encode(RespValue::Array(responses)).as_bytes())
                                    .await;

                                in_multi = false;
                                continue;
                            } else {
                                let _ = wr
                                    .write_all(
                                        encode(RespValue::SimpleError(
                                            "ERR EXEC without MULTI".to_string(),
                                        ))
                                        .as_bytes(),
                                    )
                                    .await;
                            }
                        }
                        [cmd] if cmd.to_uppercase() == "DISCARD".to_string() => {
                            if in_multi {
                                command_queue = Vec::new();

                                in_multi = false;
                                let _ = wr
                                    .write_all(
                                        encode(RespValue::SimpleString("OK".to_string()))
                                            .as_bytes(),
                                    )
                                    .await;
                            } else {
                                let _ = wr
                                    .write_all(
                                        encode(RespValue::SimpleError(
                                            "ERR DISCARD without MULTI".to_string(),
                                        ))
                                        .as_bytes(),
                                    )
                                    .await;
                            }
                        }
                        [cmd, optional] if cmd.to_uppercase() == "INFO".to_string() => {
                            match optional {
                                option if option.to_uppercase() == "REPLICATION".to_string() => {
                                    match role.as_str() {
                                        "slave" => {
                                            let _ = wr
                                                .write_all(
                                                    encode(RespValue::BulkString(
                                                        "role:slave".to_string(),
                                                    ))
                                                    .as_bytes(),
                                                )
                                                .await;
                                        }
                                        "master" => {
                                            let _ = wr
                                        .write_all(
                                            encode(RespValue::BulkString(
                                                "role:master\r\nmaster_replid:8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb\r\nmaster_repl_offset:0".to_string(),
                                            ))
                                            .as_bytes(),
                                        )
                                        .await;
                                        }
                                        _ => {
                                            unreachable!()
                                        }
                                    }
                                }
                                _ => {
                                    unimplemented!()
                                }
                            }
                        }
                        [cmd, rest @ ..] if cmd.to_uppercase() == "REPLCONF".to_string() => {
                            let _ = wr
                                .write_all(
                                    encode(RespValue::SimpleString("OK".to_string())).as_bytes(),
                                )
                                .await;
                        }
                        [cmd, rest @ ..] if cmd.to_uppercase() == "PSYNC".to_string() => {
                            // 1. It acknowledges with a FULLRESYNC response
                            let _ = wr
                                .write_all(
                                    encode(RespValue::SimpleString(
                                        "FULLRESYNC 8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb 0"
                                            .to_string(),
                                    ))
                                    .as_bytes(),
                                )
                                .await;

                            // 2. It sends a snapshot of its current state as an RDB file.(initially an empty RDB)
                            // 2-1. RDB header (length)
                            let rdb = hex::decode("524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2").unwrap();
                            let header = format!("${}\r\n", rdb.len());
                            let _ = wr.write_all(header.as_bytes()).await;

                            // 2-2. RDB binary (without the trailing \r\n)
                            let _ = wr.write_all(&rdb).await;

                            // 3. save write half of handshake stream to shared
                            let mut replicas = replicas.lock().await;
                            replicas.push((wr, rd.into_inner()));
                            return;
                        }
                        [cmd, numreplicas, timeout] if cmd.to_uppercase() == "WAIT".to_string() => {
                            let mut replicas = replicas.lock().await;

                            if master_repl_offset == 0 {
                                let count = replicas.len();
                                let _ = wr
                                    .write_all(encode(RespValue::Integers(count as i64)).as_bytes())
                                    .await;
                            } else {
                                let command_to_send_to_replica = RespValue::Array(vec![
                                    RespValue::BulkString("REPLCONF".to_string()),
                                    RespValue::BulkString("GETACK".to_string()),
                                    RespValue::BulkString("*".to_string()),
                                ]);

                                let timeout_ms = timeout.parse::<u64>().unwrap();

                                let ack_count = Arc::new(Mutex::new(0usize));
                                let ack_count_clone = Arc::clone(&ack_count);

                                let _ = tokio::time::timeout(
                                    Duration::from_millis(timeout_ms),
                                    async {
                                        let mut buf = [0u8; 512];

                                        // 1. send GETACK command to replicas all at once
                                        for (replica_writer, _) in replicas.iter_mut() {
                                            let _ = replica_writer
                                                .write_all(
                                                    encode(command_to_send_to_replica.clone())
                                                        .as_bytes(),
                                                )
                                                .await;
                                        }

                                        // 2. collect the ACK responses
                                        for (_, replica_reader) in replicas.iter_mut() {
                                            // read offset response from replica and count acknowledged replicas
                                            if let Ok(n) = replica_reader.read(&mut buf).await {
                                                let received = String::from_utf8_lossy(&buf[..n]);
                                                let commands = decode_arrays(&received);
                                                for resp_array in commands {
                                                    // REPLCONF ACK <offset>
                                                    if let [cmd, subcmd, offset] =
                                                        resp_array.as_slice()
                                                    {
                                                        if cmd.to_uppercase() == "REPLCONF"
                                                            && subcmd.to_uppercase() == "ACK"
                                                        {
                                                            let replica_offset = offset
                                                                .parse::<usize>()
                                                                .unwrap_or(0);
                                                            if replica_offset >= master_repl_offset
                                                            {
                                                                *ack_count_clone.lock().unwrap() +=
                                                                    1;
                                                            }
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    },
                                )
                                .await;

                                let count = *ack_count.lock().unwrap();

                                let _ = wr
                                    .write_all(encode(RespValue::Integers(count as i64)).as_bytes())
                                    .await;
                            }
                        }
                        [cmd, subcmd, rest @ ..]
                            if cmd.to_uppercase() == "CONFIG".to_string()
                                && subcmd.to_uppercase() == "GET".to_string() =>
                        {
                            match rest {
                                [] => {
                                    unimplemented!()
                                }
                                [val] if val == "dir" => {
                                    let _ = wr
                                        .write_all(
                                            encode(RespValue::Array(vec![
                                                RespValue::BulkString("dir".to_string()),
                                                RespValue::BulkString(
                                                    config
                                                        .dir
                                                        .as_deref()
                                                        .unwrap_or_default()
                                                        .to_string(),
                                                ),
                                            ]))
                                            .as_bytes(),
                                        )
                                        .await;
                                }
                                [val] if val == "dbfilename" => {
                                    let _ = wr
                                        .write_all(
                                            encode(RespValue::Array(vec![
                                                RespValue::BulkString("dbfilename".to_string()),
                                                RespValue::BulkString(
                                                    config
                                                        .dbfilename
                                                        .as_deref()
                                                        .unwrap_or_default()
                                                        .to_string(),
                                                ),
                                            ]))
                                            .as_bytes(),
                                        )
                                        .await;
                                }
                                _ => {}
                            }
                        }
                        [cmd, pattern] if cmd.to_uppercase() == "KEYS" => {
                            let keys: Vec<RespValue> = {
                                let db = db.lock().unwrap();
                                db.keys()
                                    .map(|k| RespValue::BulkString(k.clone()))
                                    .collect()
                            };
                            let _ = wr
                                .write_all(encode(RespValue::Array(keys)).as_bytes())
                                .await;
                        }
                        [cmd, channelname] if cmd.to_uppercase() == "SUBSCRIBE".to_string() => {
                            let (tx, mut _rx) = mpsc::channel::<usize>(100);
                            {
                                let mut pubsub = pubsub.lock().unwrap();

                                pubsub.entry(channelname.to_string()).or_default().push(tx);
                            };

                            subscribed_channels.insert(channelname.to_string());
                            let subscribed_channels_count = subscribed_channels.len();

                            let _ = wr
                                .write_all(
                                    encode(RespValue::Array(vec![
                                        RespValue::BulkString("subscribe".to_string()),
                                        RespValue::BulkString(channelname.to_string()),
                                        RespValue::Integers(subscribed_channels_count as i64),
                                    ]))
                                    .as_bytes(),
                                )
                                .await;

                            // rx.recv().await;
                        }
                        _ => unreachable!(),
                    }
                }
            }
            Err(_) => break,
        }
    }
}

pub fn execute_single_command(cmd: &[String], db: &Db) -> RespValue {
    match cmd {
        [cmd, key, value, optional_args @ ..] if cmd.to_uppercase() == "SET" => match optional_args
        {
            [] => {
                let redis_value = RedisValue::new(ValueType::String(value.to_string()), None);
                {
                    let mut db = db.lock().unwrap();
                    db.insert(key.to_string(), redis_value);
                }

                RespValue::SimpleString("OK".to_string())
            }
            [option, seconds] if option.to_uppercase() == "EX" => {
                let now = Instant::now();
                let expires_at = now + Duration::from_secs(seconds.parse().unwrap());

                let redis_value =
                    RedisValue::new(ValueType::String(value.to_string()), Some(expires_at));
                {
                    let mut db = db.lock().unwrap();
                    db.insert(key.to_string(), redis_value);
                }

                RespValue::SimpleString("OK".to_string())
            }
            [option, milliseconds] if option.to_uppercase() == "PX" => {
                let now = Instant::now();
                let expires_at = now + Duration::from_millis(milliseconds.parse().unwrap());

                let redis_value =
                    RedisValue::new(ValueType::String(value.to_string()), Some(expires_at));
                {
                    let mut db = db.lock().unwrap();
                    db.insert(key.to_string(), redis_value);
                }

                RespValue::SimpleString("OK".to_string())
            }
            _ => unreachable!(),
        },
        [cmd, key] if cmd.to_uppercase() == "GET" => {
            let db = db.lock().unwrap();

            if let Some(redis_value) = db.get(key) {
                match redis_value.expires_at {
                    Some(instant) => {
                        if Instant::now() > instant {
                            RespValue::BulkStringNull
                        } else {
                            match &redis_value.value {
                                ValueType::String(string) => {
                                    RespValue::BulkString(string.to_string())
                                }
                                _ => unimplemented!(),
                            }
                        }
                    }
                    None => match &redis_value.value {
                        ValueType::String(string) => RespValue::BulkString(string.to_string()),
                        _ => unimplemented!(),
                    },
                }
            } else {
                RespValue::BulkStringNull
            }
        }
        [cmd, key] if cmd.to_uppercase() == "INCR" => {
            let mut db = db.lock().unwrap();

            if let Some(redis_value) = db.get_mut(key) {
                match &mut redis_value.value {
                    ValueType::String(string) => match string.parse::<i64>() {
                        Ok(n) => {
                            *string = format!("{}", n + 1);

                            RespValue::Integers(n + 1)
                        }
                        Err(_) => RespValue::SimpleError(
                            "ERR value is not an integer or out of range".to_string(),
                        ),
                    },
                    _ => {
                        unreachable!()
                    }
                }
            } else {
                let redis_value = RedisValue::new(ValueType::String("1".to_string()), None);

                db.insert(key.to_string(), redis_value);

                RespValue::Integers(1)
            }
        }
        _ => RespValue::SimpleError("ERR unknown command".to_string()),
    }
}
