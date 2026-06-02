use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader};

use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};

use crate::Replicas;
use crate::protocol::{RespValue, decode_arrays, encode};

pub fn execute_replconf(command: &[String]) -> Option<RespValue> {
    match command {
        [cmd, rest @ ..] if cmd.to_uppercase() == "REPLCONF".to_string() => {
            Some(RespValue::SimpleString("OK".to_string()))
        }
        _ => unreachable!(),
    }
}

pub async fn execute_psync(
    command: &[String],
    mut wr: OwnedWriteHalf,
    rd: BufReader<OwnedReadHalf>,
    replicas: &Replicas,
) {
    match command {
        [cmd, _replid, _offset] if cmd.to_uppercase() == "PSYNC".to_string() => {
            let _ = wr
                .write_all(
                    encode(RespValue::SimpleString(
                        "FULLRESYNC 8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb 0".to_string(),
                    ))
                    .as_bytes(),
                )
                .await;

            let rdb = hex::decode("524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2").unwrap();
            let header = format!("${}\r\n", rdb.len());
            let _ = wr.write_all(header.as_bytes()).await;
            let _ = wr.write_all(&rdb).await;

            let mut replicas = replicas.lock().await;
            replicas.push((wr, rd.into_inner()));
            return;
        }
        _ => unreachable!(),
    }
}

pub async fn execute_wait(
    command: &[String],
    wr: &mut OwnedWriteHalf,
    rd: &mut BufReader<OwnedReadHalf>,
    replicas: &Replicas,
    master_repl_offset: usize,
) {
    match command {
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

                let _ = tokio::time::timeout(Duration::from_millis(timeout_ms), async {
                    let mut buf = [0u8; 512];
                    for (replica_writer, _) in replicas.iter_mut() {
                        let _ = replica_writer
                            .write_all(encode(command_to_send_to_replica.clone()).as_bytes())
                            .await;
                    }
                    for (_, replica_reader) in replicas.iter_mut() {
                        if let Ok(n) = replica_reader.read(&mut buf).await {
                            let received = String::from_utf8_lossy(&buf[..n]);
                            let commands = decode_arrays(&received);
                            for command in commands {
                                if let [cmd, subcmd, offset] = command.as_slice() {
                                    if cmd.to_uppercase() == "REPLCONF"
                                        && subcmd.to_uppercase() == "ACK"
                                    {
                                        let replica_offset = offset.parse::<usize>().unwrap_or(0);
                                        if replica_offset >= master_repl_offset {
                                            *ack_count_clone.lock().unwrap() += 1;
                                        }
                                    }
                                }
                            }
                        }
                    }
                })
                .await;

                let count = *ack_count.lock().unwrap();
                let _ = wr
                    .write_all(encode(RespValue::Integers(count as i64)).as_bytes())
                    .await;
            }
        }
        _ => unreachable!(),
    }
}
