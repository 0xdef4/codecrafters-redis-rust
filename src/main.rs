#![allow(unused_imports)]
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};

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

    loop {
        match listener.accept().await {
            Ok((stream, _addr)) => {
                // println!("socket addr: {:?}", addr);

                let db = Arc::clone(&db);

                tokio::spawn(async move {
                    handle_stream(stream, db).await;
                });
            }
            Err(e) => println!("couldn't get client: {:?}", e),
        }
    }
}

async fn handle_stream(stream: TcpStream, db: Db) {
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
                    [cmd, arg1, arg2, rest @ ..] if cmd.to_uppercase() == "SET".to_string() => {
                        match rest {
                            [] => {
                                let redis_value =
                                    RedisValue::new(ValueType::String(arg2.to_string()), None);
                                {
                                    let mut db = db.lock().unwrap();
                                    db.insert(arg1.to_string(), redis_value);
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
                                    ValueType::String(arg2.to_string()),
                                    Some(expires_at),
                                );
                                {
                                    let mut db = db.lock().unwrap();
                                    db.insert(arg1.to_string(), redis_value);
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
                                    ValueType::String(arg2.to_string()),
                                    Some(expires_at),
                                );
                                {
                                    let mut db = db.lock().unwrap();
                                    db.insert(arg1.to_string(), redis_value);
                                }
                                let _ = wr
                                    .write_all(encode_simple_strings("OK".to_string()).as_bytes())
                                    .await;
                            }
                            _ => unreachable!(),
                        }
                    }
                    [cmd, arg] if cmd.to_uppercase() == "GET".to_string() => {
                        let response = {
                            let db = db.lock().unwrap();

                            if let Some(redis_value) = db.get(arg) {
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
                    [cmd, arg1, arg2] if cmd.to_uppercase() == "RPUSH".to_string() => {
                        let list_length = {
                            let mut db = db.lock().unwrap();

                            if let Some(redis_value) = db.get_mut(arg1) {
                                if let ValueType::List(list) = &mut redis_value.value {
                                    list.push(arg2.to_string());

                                    list.len()
                                } else {
                                    unimplemented!()
                                }
                            } else {
                                let mut list = Vec::new();
                                list.push(arg2.to_string());
                                let len = list.len();

                                let redis_value = RedisValue::new(ValueType::List(list), None);

                                db.insert(arg1.to_string(), redis_value);

                                len
                            }
                        };

                        let _ = wr
                            .write_all(encode_integers(list_length as i64).as_bytes())
                            .await;
                    }
                    _ => unreachable!(),
                }
            }
            Err(_) => break,
        }
    }
}
