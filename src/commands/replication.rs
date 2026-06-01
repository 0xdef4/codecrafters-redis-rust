use crate::protocol::RespValue;

pub fn execute_replconf(command: &[String]) -> Option<RespValue> {
    match command {
        [cmd, rest @ ..] if cmd.to_uppercase() == "REPLCONF".to_string() => {
            Some(RespValue::SimpleString("OK".to_string()))
        }
        _ => unreachable!(),
    }
}
