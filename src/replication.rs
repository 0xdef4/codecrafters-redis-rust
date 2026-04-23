use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::net::tcp::OwnedWriteHalf;
use tokio::sync::Mutex as TokioMutex;

use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::{Db, RedisValue, RespValue, ValueType, decode_arrays, encode};

pub type ReplicaWriters = Arc<TokioMutex<Vec<OwnedWriteHalf>>>;

pub async fn start_replica_handshake(replicaof: String, port: u16, db: Db) {
    if let Some((master_ip, master_port)) = replicaof.split_once(' ') {
        let master_addr = format!("{}:{}", master_ip, master_port);
        let mut master_stream = TcpStream::connect(master_addr).await.unwrap();

        // send PING
        master_stream
            .write_all(
                encode(RespValue::Array(vec![RespValue::BulkString(
                    "PING".to_string(),
                )]))
                .as_bytes(),
            )
            .await
            .unwrap();

        let mut buf = [0u8; 512];
        master_stream.read(&mut buf).await.unwrap();

        // send REPLCONF listening-port <PORT>
        master_stream
            .write_all(
                encode(RespValue::Array(vec![
                    RespValue::BulkString("REPLCONF".to_string()),
                    RespValue::BulkString("listening-port".to_string()),
                    RespValue::BulkString(port.to_string()),
                ]))
                .as_bytes(),
            )
            .await
            .unwrap();

        master_stream.read(&mut buf).await.unwrap();

        // send REPLCONF capa psync2
        master_stream
            .write_all(
                encode(RespValue::Array(vec![
                    RespValue::BulkString("REPLCONF".to_string()),
                    RespValue::BulkString("capa".to_string()),
                    RespValue::BulkString("psync2".to_string()),
                ]))
                .as_bytes(),
            )
            .await
            .unwrap();

        master_stream.read(&mut buf).await.unwrap();

        // send PSYNC <replication_id> <offset>
        master_stream
            .write_all(
                encode(RespValue::Array(vec![
                    RespValue::BulkString("PSYNC".to_string()),
                    RespValue::BulkString("?".to_string()),
                    RespValue::BulkString("-1".to_string()),
                ]))
                .as_bytes(),
            )
            .await
            .unwrap();

        // wait for FULLRESYNC response
        let mut line = String::new();
        loop {
            let b = master_stream.read_u8().await.unwrap();
            line.push(b as char);
            if line.ends_with("\r\n") {
                break;
            }
        }
        // println!("FULLRESYNC: {}", line);

        // read RDB header: $<len>\r\n
        let mut rdb_header = String::new();
        loop {
            let b = master_stream.read_u8().await.unwrap();
            rdb_header.push(b as char);
            if rdb_header.ends_with("\r\n") {
                break;
            }
        }
        // parse RDB header for RDB length: "$88\r\n" -> 88 파싱
        let rdb_len: usize = rdb_header.trim_start_matches('$').trim().parse().unwrap();

        // wait for RDB binary using exact length
        let mut rdb_buf = vec![0u8; rdb_len];
        master_stream.read_exact(&mut rdb_buf).await.unwrap();
        // println!("RDB read: {} bytes", rdb_len);

        loop {
            match master_stream.read(&mut buf).await {
                Ok(0) => break,
                Ok(n) => {
                    let received = String::from_utf8_lossy(&buf[..n]);
                    println!("received (in replica): {:?}", received);

                    let commands = decode_arrays(&received);
                    for resp_array in commands {
                        println!("resp_array (in replica): {:?}", resp_array);
                        match resp_array.as_slice() {
                            [cmd, key, value, optional_args @ ..]
                                if cmd.to_uppercase() == "SET".to_string() =>
                            {
                                match optional_args {
                                    [] => {
                                        let redis_value = RedisValue::new(
                                            ValueType::String(value.to_string()),
                                            None,
                                        );

                                        let mut db = db.lock().unwrap();
                                        db.insert(key.to_string(), redis_value);
                                    }
                                    [option, seconds] if option.to_uppercase() == "EX" => {
                                        let now = Instant::now();
                                        let expires_at =
                                            now + Duration::from_secs(seconds.parse().unwrap());

                                        let redis_value = RedisValue::new(
                                            ValueType::String(value.to_string()),
                                            Some(expires_at),
                                        );
                                        let mut db = db.lock().unwrap();
                                        db.insert(key.to_string(), redis_value);
                                    }
                                    [option, milliseconds] if option.to_uppercase() == "PX" => {
                                        let now = Instant::now();
                                        let expires_at = now
                                            + Duration::from_millis(milliseconds.parse().unwrap());

                                        let redis_value = RedisValue::new(
                                            ValueType::String(value.to_string()),
                                            Some(expires_at),
                                        );
                                        let mut db = db.lock().unwrap();
                                        db.insert(key.to_string(), redis_value);
                                    }
                                    _ => unreachable!(),
                                }
                            }
                            _ => {}
                        }
                    }
                }
                Err(_) => break,
            }
        }
    }
}
