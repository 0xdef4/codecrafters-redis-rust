use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use tokio::sync::{Notify, mpsc};

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use crate::aof::append_to_aof;
use crate::commands::{dispatch_command, execute_psync, execute_subscribe};
use crate::protocol::{RespValue, decode_arrays, encode};
use crate::replication::propagate_to_replicas;
use crate::types::{AclDb, Db, Pubsub, Replicas};
use crate::utils::is_write_command;
use crate::{Config, pubsub::handle_subscribe_loop};

static CLIENT_ID_COUNTER: AtomicU64 = AtomicU64::new(0);

pub async fn handle_stream(
    stream: TcpStream,
    db: Db,
    notify: Arc<Notify>,
    role: String,
    replicas: Replicas,
    config: Arc<Config>,
    pubsub: Pubsub,
    acl_db: AclDb,
) {
    let client_id = CLIENT_ID_COUNTER.fetch_add(1, Ordering::Relaxed);
    let mut in_multi: bool = false;
    let mut command_queue: Vec<Vec<String>> = Vec::new();
    let mut subscribed_channels: HashSet<String> = HashSet::new();
    let mut master_repl_offset: usize = 0;
    let mut is_authenticated: bool = {
        let acl_db = acl_db.lock().unwrap();

        if let Some(acl_user) = acl_db.get(&"default".to_string()) {
            acl_user.get_flags().contains(&"nopass".to_string())
        } else {
            false
        }
    };
    let mut watched_keys: HashMap<String, u64> = HashMap::new();

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
                for command in commands {
                    println!("command: {:?}", command);
                    let cmd_upper = command[0].to_uppercase();

                    if in_multi
                        && cmd_upper != "EXEC"
                        && cmd_upper != "MULTI"
                        && cmd_upper != "DISCARD"
                        && cmd_upper != "WATCH"
                    {
                        command_queue.push(command.clone());
                        let _ = wr
                            .write_all(
                                encode(RespValue::SimpleString("QUEUED".to_string())).as_bytes(),
                            )
                            .await;
                        continue;
                    }

                    if !is_authenticated && cmd_upper != "AUTH" {
                        let _ = wr
                            .write_all(
                                encode(RespValue::SimpleError(
                                    "NOAUTH Authentication required.".to_string(),
                                ))
                                .as_bytes(),
                            )
                            .await;
                        continue;
                    }

                    if config.appendonly == "yes"
                        && is_write_command(&command)
                        && !matches!(cmd_upper.as_str(), "XADD" | "GEOADD")
                    {
                        append_to_aof(&command, &config);
                    }

                    match cmd_upper.as_str() {
                        "SUBSCRIBE" => {
                            execute_subscribe(
                                command.as_slice(),
                                &pubsub,
                                &client_id,
                                &mut subscribed_channels,
                                &mut wr,
                                &mut rd,
                            )
                            .await;
                        }
                        "PSYNC" => {
                            execute_psync(command.as_slice(), wr, rd, &replicas).await;
                            return;
                            // let _ = wr
                            //     .write_all(
                            //         encode(RespValue::SimpleString(
                            //             "FULLRESYNC 8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb 0"
                            //                 .to_string(),
                            //         ))
                            //         .as_bytes(),
                            //     )
                            //     .await;

                            // let rdb = hex::decode("524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2").unwrap();
                            // let header = format!("${}\r\n", rdb.len());
                            // let _ = wr.write_all(header.as_bytes()).await;
                            // let _ = wr.write_all(&rdb).await;

                            // let mut replicas = replicas.lock().await;
                            // replicas.push((wr, rd.into_inner()));
                            // return;
                        }
                        "WAIT" => {
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

                                let timeout_ms = command[2].parse::<u64>().unwrap();
                                let ack_count = Arc::new(Mutex::new(0usize));
                                let ack_count_clone = Arc::clone(&ack_count);

                                let _ = tokio::time::timeout(
                                    Duration::from_millis(timeout_ms),
                                    async {
                                        let mut buf = [0u8; 512];
                                        for (replica_writer, _) in replicas.iter_mut() {
                                            let _ = replica_writer
                                                .write_all(
                                                    encode(command_to_send_to_replica.clone())
                                                        .as_bytes(),
                                                )
                                                .await;
                                        }
                                        for (_, replica_reader) in replicas.iter_mut() {
                                            if let Ok(n) = replica_reader.read(&mut buf).await {
                                                let received = String::from_utf8_lossy(&buf[..n]);
                                                let commands = decode_arrays(&received);
                                                for command in commands {
                                                    if let [cmd, subcmd, offset] =
                                                        command.as_slice()
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
                        _ => {
                            let resp = dispatch_command(
                                &command,
                                &db,
                                &notify,
                                &config,
                                &role,
                                &mut in_multi,
                                &mut command_queue,
                                &mut watched_keys,
                                &pubsub,
                                &acl_db,
                                &mut is_authenticated,
                            )
                            .await;

                            match resp {
                                Some(resp) => {
                                    let _ = wr.write_all(encode(resp).as_bytes()).await;
                                }
                                None => {
                                    let _ =
                                        wr.write_all(encode(RespValue::ArrayNull).as_bytes()).await;
                                    return;
                                }
                            }
                        }
                    }
                    if role == "master" && is_write_command(&command) {
                        propagate_to_replicas(&command, &replicas, &mut master_repl_offset).await;
                    }
                }
            }
            Err(_) => break,
        }
    }
}
