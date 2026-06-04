use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use tokio::sync::Notify;

use anyhow::Result;

use crate::Config;
use crate::aof::append_to_aof;
use crate::commands::{dispatch_command, execute_psync, execute_subscribe, execute_wait};
use crate::protocol::{RespValue, decode_arrays, encode};
use crate::replication::propagate_to_replicas;
use crate::types::{AclDb, Db, Pubsub, Replicas};
use crate::utils::is_write_command;

static CLIENT_ID_COUNTER: AtomicU64 = AtomicU64::new(0);

pub struct ClientState {
    pub in_multi: bool,
    pub command_queue: Vec<Vec<String>>,
    pub subscribed_channels: HashSet<String>,
    pub master_repl_offset: usize,
    pub is_authenticated: bool,
    pub watched_keys: HashMap<String, u64>,
}

impl ClientState {
    pub fn new(is_authenticated: bool) -> Self {
        Self {
            in_multi: false,
            command_queue: Vec::new(),
            subscribed_channels: HashSet::new(),
            master_repl_offset: 0,
            is_authenticated,
            watched_keys: HashMap::new(),
        }
    }
}

pub async fn handle_stream(
    stream: TcpStream,
    db: Db,
    notify: Arc<Notify>,
    role: String,
    replicas: Replicas,
    config: Arc<Config>,
    pubsub: Pubsub,
    acl_db: AclDb,
) -> Result<()> {
    let client_id = CLIENT_ID_COUNTER.fetch_add(1, Ordering::Relaxed);

    let mut client_state = ClientState::new({
        let acl_db = acl_db.lock().unwrap();
        if let Some(acl_user) = acl_db.get("default") {
            acl_user.get_flags().contains(&"nopass".to_string())
        } else {
            false
        }
    });

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

                    if client_state.in_multi
                        && cmd_upper != "EXEC"
                        && cmd_upper != "MULTI"
                        && cmd_upper != "DISCARD"
                        && cmd_upper != "WATCH"
                    {
                        let _ = &mut client_state.command_queue.push(command.clone());
                        wr.write_all(
                            encode(RespValue::SimpleString("QUEUED".to_string())).as_bytes(),
                        )
                        .await?;
                        continue;
                    }

                    if !client_state.is_authenticated && cmd_upper != "AUTH" {
                        wr.write_all(
                            encode(RespValue::SimpleError(
                                "NOAUTH Authentication required.".to_string(),
                            ))
                            .as_bytes(),
                        )
                        .await?;
                        continue;
                    }

                    if config.appendonly == "yes"
                        && is_write_command(&command)
                        && !matches!(cmd_upper.as_str(), "XADD" | "GEOADD")
                    {
                        if let Err(e) = append_to_aof(&command, &config) {
                            eprintln!("error appending to aof: {}", e);
                        }
                    }

                    match cmd_upper.as_str() {
                        "SUBSCRIBE" => {
                            execute_subscribe(
                                command.as_slice(),
                                &pubsub,
                                &client_id,
                                &mut client_state.subscribed_channels,
                                &mut wr,
                                &mut rd,
                            )
                            .await;
                        }
                        "PSYNC" => {
                            if let Err(e) =
                                execute_psync(command.as_slice(), wr, rd, &replicas).await
                            {
                                eprint!("error executing psync command: {}", e)
                            }
                            return Ok(());
                        }
                        "WAIT" => {
                            if let Err(e) = execute_wait(
                                command.as_slice(),
                                &mut wr,
                                &replicas,
                                client_state.master_repl_offset,
                            )
                            .await
                            {
                                eprint!("error executing psync command: {}", e)
                            }
                        }
                        _ => {
                            let resp = dispatch_command(
                                &command,
                                &db,
                                &notify,
                                &config,
                                &role,
                                &pubsub,
                                &acl_db,
                                &mut client_state,
                            )
                            .await;

                            match resp {
                                Some(resp) => {
                                    wr.write_all(encode(resp).as_bytes()).await?;
                                }
                                None => {
                                    wr.write_all(encode(RespValue::ArrayNull).as_bytes())
                                        .await?;
                                    return Ok(());
                                }
                            }
                        }
                    }
                    if role == "master" && is_write_command(&command) {
                        if let Err(e) = propagate_to_replicas(
                            &command,
                            &replicas,
                            &mut client_state.master_repl_offset,
                        )
                        .await
                        {
                            eprintln!("error propagating to replicas: {}", e);
                        }
                    }
                }
            }
            Err(_) => break,
        }
    }

    Ok(())
}
