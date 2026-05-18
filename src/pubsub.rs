use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::sync::mpsc;
use tokio::sync::mpsc::Sender;

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

use crate::protocol::resp::{RespValue, decode_arrays, encode};

pub type Pubsub = Arc<Mutex<HashMap<String, Vec<(u64, Sender<(String, String)>)>>>>;

pub async fn handle_subscribe_loop(
    mut wr: OwnedWriteHalf,
    mut rd: BufReader<OwnedReadHalf>,
    pubsub: Pubsub,
    client_id: u64,
    tx: mpsc::Sender<(String, String)>,
    mut rx: mpsc::Receiver<(String, String)>,
    mut subscribed_channels: HashSet<String>,
) {
    let mut buf = [0u8; 512];

    loop {
        tokio::select! {
            Ok(n) = rd.read(&mut buf) => {
                   let received = String::from_utf8_lossy(&buf[..n]);
                   let commands = decode_arrays(&received);

                   for resp_array in commands {
                       match resp_array.as_slice() {
                           [cmd, channel_name]
                               if cmd.to_uppercase()
                                   == "SUBSCRIBE".to_string() =>
                           {
                               {
                                   let mut pubsub = pubsub.lock().unwrap();
                                   pubsub
                                       .entry(channel_name.to_string())
                                       .or_default()
                                       .push((client_id, tx.clone()));
                               };

                               subscribed_channels
                                   .insert(channel_name.to_string());
                               let subscribed_channels_count =
                                   subscribed_channels.len();

                               let _ = wr
                                   .write_all(
                                       encode(RespValue::Array(vec![
                                           RespValue::BulkString(
                                               "subscribe".to_string(),
                                           ),
                                           RespValue::BulkString(
                                               channel_name.to_string(),
                                           ),
                                           RespValue::Integers(
                                               subscribed_channels_count
                                                   as i64,
                                           ),
                                       ]))
                                       .as_bytes(),
                                   )
                                   .await;
                           }
                           [cmd, channel_name]
                               if cmd.to_uppercase()
                                   == "UNSUBSCRIBE".to_string() => {
                                {
                                   let mut pubsub = pubsub.lock().unwrap();
                                   if let Some(list) = pubsub.get_mut(channel_name) {
                                     list.retain(|(id, _)| *id != client_id);
                                   }
                                }

                                subscribed_channels.remove(channel_name);
                                let subscribed_channels_count = subscribed_channels.len();

                               let _ = wr
                                   .write_all(
                                       encode(RespValue::Array(vec![
                                           RespValue::BulkString(
                                               "unsubscribe".to_string(),
                                           ),
                                           RespValue::BulkString(
                                               channel_name.to_string(),
                                           ),
                                           RespValue::Integers(
                                               subscribed_channels_count
                                                   as i64,
                                           ),
                                       ]))
                                       .as_bytes(),
                                   )
                                   .await;
                            }
                           [cmd]
                               if cmd.to_uppercase()
                                   == "PSUBSCRIBE".to_string() => {}
                           [cmd]
                               if cmd.to_uppercase()
                                   == "PUNSUBSCRIBE".to_string() => {}
                           [cmd]
                               if cmd.to_uppercase() == "PING".to_string() =>
                           {
                               let _ = wr
                                   .write_all(
                                       encode(RespValue::Array(vec![
                                           RespValue::BulkString(
                                               "pong".to_string(),
                                           ),
                                           RespValue::BulkString(
                                               "".to_string(),
                                           ),
                                       ]))
                                       .as_bytes(),
                                   )
                                   .await;
                           }
                           [cmd]
                               if cmd.to_uppercase() == "QUIT".to_string() => {
                           }
                           _ => {
                               if let [cmd, _rest @ ..] = resp_array.as_slice()
                               {
                                   let error_message = format!(
                                       "ERR Can't execute '{}': only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING / QUIT / RESET are allowed in this context",
                                       cmd
                                   );
                                   let _ = wr
                                       .write_all(
                                           encode(RespValue::SimpleError(
                                               error_message,
                                           ))
                                           .as_bytes(),
                                       )
                                       .await;
                               }
                           }
                       }
                   }
                }
            Some((channel_name, msg)) = rx.recv() => {
                let _ = wr.write_all(encode(
                    RespValue::Array(
                        vec![
                            RespValue::BulkString("message".to_string()),
                            RespValue::BulkString(channel_name.to_string()),
                            RespValue::BulkString(msg.to_string()),
                        ]
                    )
                ).as_bytes()).await;
            }
        }
    }
}
