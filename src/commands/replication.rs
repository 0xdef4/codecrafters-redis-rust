use std::ops::Deref;

use tokio::io::{AsyncWriteExt, BufReader};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};

use crate::Replicas;
use crate::protocol::{RespValue, encode};

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
        [cmd] if cmd.to_uppercase() == "PSYNC".to_string() => {
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
