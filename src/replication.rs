use std::sync::Arc;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use crate::commands::execute_set;
use crate::protocol::{RespValue, decode_arrays, encode};
use crate::types::Db;
use crate::{Config, Replicas};

pub async fn start_replica_handshake(config: Arc<Config>, db: Db) {
    if let Some((master_ip, master_port)) = config.replicaof.as_ref().unwrap().split_once(' ') {
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
                    RespValue::BulkString(config.port.to_string()),
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

        start_replica_loop(master_stream, db).await;
    }
}

async fn start_replica_loop(mut master_stream: TcpStream, db: Db) {
    // track total byte size received from master
    let mut track_total_bytes = 0;

    let mut buf = [0u8; 512];
    loop {
        match master_stream.read(&mut buf).await {
            Ok(0) => break,
            Ok(n) => {
                let received = String::from_utf8_lossy(&buf[..n]);
                println!("received (in replica): {:?}", received);

                let commands = decode_arrays(&received);
                for command in commands {
                    println!("command (in replica): {:?}", command);

                    // calculate the byte size of the command
                    let byte_size_of_command = encode(RespValue::Array(
                        command
                            .iter()
                            .map(|e| RespValue::BulkString(e.to_string()))
                            .collect::<Vec<_>>(),
                    ))
                    .as_bytes()
                    .len();

                    match command.as_slice() {
                        [cmd, ..] if cmd.to_uppercase() == "PING" => {
                            track_total_bytes += byte_size_of_command;
                        }
                        [cmd, ..] if cmd.to_uppercase() == "SET" => {
                            let _ = execute_set(&command, &db);

                            track_total_bytes += byte_size_of_command;
                        }
                        [cmd, subcmd, arg]
                            if cmd.to_uppercase() == "REPLCONF"
                                && subcmd.to_uppercase() == "GETACK" =>
                        {
                            master_stream
                                .write_all(
                                    encode(RespValue::Array(vec![
                                        RespValue::BulkString("REPLCONF".to_string()),
                                        RespValue::BulkString("ACK".to_string()),
                                        RespValue::BulkString(track_total_bytes.to_string()),
                                    ]))
                                    .as_bytes(),
                                )
                                .await
                                .unwrap();

                            track_total_bytes += byte_size_of_command;
                        }
                        _ => {}
                    }
                }
            }
            Err(_) => break,
        }
    }
}

pub fn start_if_replica(db: &Db, config: Arc<Config>) {
    if config.replicaof.is_none() {
        return;
    }

    let db = Arc::clone(db);
    tokio::spawn(async move {
        start_replica_handshake(config, db).await;
    });
}

pub async fn propagate_to_replicas(
    command: &[String],
    replicas: &Replicas,
    master_repl_offset: &mut usize,
) {
    let command_to_propagate = RespValue::Array(
        command
            .iter()
            .map(|e| RespValue::BulkString(e.clone()))
            .collect::<Vec<_>>(),
    );

    *master_repl_offset += encode(command_to_propagate.clone()).as_bytes().len();

    let mut replicas = replicas.lock().await;
    for (replica_writer, _replica_reader) in replicas.iter_mut() {
        let _ = replica_writer
            .write_all(encode(command_to_propagate.clone()).as_bytes())
            .await;
    }
}
