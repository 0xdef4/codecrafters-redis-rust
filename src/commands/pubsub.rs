use crate::protocol::RespValue;
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
