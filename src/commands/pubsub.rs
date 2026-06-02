use std::collections::HashSet;

use tokio::io::{AsyncWriteExt, BufReader};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};

use tokio::sync::mpsc;

use crate::protocol::RespValue;
use crate::protocol::encode;
use crate::pubsub::handle_subscribe_loop;
use crate::types::Pubsub;

pub async fn execute_publish(command: &[String], pubsub: &Pubsub) -> Option<RespValue> {
    match command {
        [cmd, channel_name, message_contents] if cmd.to_uppercase() == "PUBLISH".to_string() => {
            let tx_list = {
                let pubsub = pubsub.lock().unwrap();
                pubsub.get(channel_name).cloned().unwrap_or_default()
            };

            for tx in tx_list.iter() {
                let _ =
                    tx.1.send((channel_name.to_string(), message_contents.to_string()))
                        .await;
            }

            Some(RespValue::Integers(tx_list.len() as i64))
        }
        _ => unreachable!(),
    }
}

pub async fn execute_subscribe(
    command: &[String],
    pubsub: &Pubsub,
    client_id: &u64,
    subscribed_channels: &mut HashSet<String>,
    wr: &mut OwnedWriteHalf,
    rd: &mut BufReader<OwnedReadHalf>,
) {
    match command {
        [cmd] if cmd.to_uppercase() == "SUBSCRIBE".to_string() => {
            let (tx, rx) = mpsc::channel::<(String, String)>(100);
            {
                let mut pubsub = pubsub.lock().unwrap();
                pubsub
                    .entry(command[1].to_string())
                    .or_default()
                    .push((*client_id, tx.clone()));
            };

            subscribed_channels.insert(command[1].to_string());
            let subscribed_channels_count = subscribed_channels.len();

            let _ = wr
                .write_all(
                    encode(RespValue::Array(vec![
                        RespValue::BulkString("subscribe".to_string()),
                        RespValue::BulkString(command[1].to_string()),
                        RespValue::Integers(subscribed_channels_count as i64),
                    ]))
                    .as_bytes(),
                )
                .await;

            handle_subscribe_loop(wr, rd, pubsub, client_id, tx, rx, subscribed_channels).await;
            return;
        }
        _ => unreachable!(),
    }
}
