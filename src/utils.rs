pub fn is_write_command(command: &[String]) -> bool {
    match command.first().map(|s| s.to_uppercase()).as_deref() {
        Some("SET") | Some("DEL") | Some("INCR") | Some("RPUSH") | Some("LPUSH") | Some("LPOP")
        | Some("ZADD") | Some("ZREM") | Some("XADD") | Some("GEOADD") => true,
        _ => false,
    }
}
