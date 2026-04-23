use tokio::net::{TcpStream};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::RespValue;
use crate::encode;

pub async fn start_replica_handshake(replicaof: String, port: u16) {
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

        master_stream.read(&mut buf).await.unwrap();
    }
}
