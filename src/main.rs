#![allow(unused_imports)]
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Instant, Duration};

struct MapValue {
    value: String,
    expires_at: Option<Instant>
}

impl MapValue {
    fn new(value: String, expires_at: Option<Instant>) -> Self {
        Self {
            value,
            expires_at
        }
    }
}

type Db = Arc<Mutex<HashMap<String, MapValue>>>;

#[tokio::main]
async fn main() {
    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();

    let db = Arc::new(Mutex::new(HashMap::new()));

    loop {
        match listener.accept().await {
            Ok((stream, _addr)) => {
                // println!("socket addr: {:?}", addr);

                let db = Arc::clone(&db);

                tokio::spawn(async move {
                    handle_stream(stream, db).await;
                });
            }
            Err(e) => println!("couldn't get client: {:?}", e),
        }
    }
}

async fn handle_stream(stream: TcpStream, db: Db) {
    let (rd, mut wr) = stream.into_split();
    let mut rd = BufReader::new(rd);

    let mut buf = [0u8; 512];

    loop {
        match rd.read(&mut buf).await {
            Ok(0) => break,
            Ok(n) => {
                let received = String::from_utf8_lossy(&buf[..n]);
                println!("received: {:?}", received);

                let resp_array = decode_arrays(&received);

                println!("resp_array: {:?}", resp_array);

                match resp_array.as_slice() {
                    [cmd] if cmd.to_uppercase() == "PING".to_string() => {
                        let _ = wr.write_all(b"+PONG\r\n").await;
                    }
                    [cmd, arg] if cmd.to_uppercase() == "ECHO".to_string() => {
                        let _ = wr.write_all(encode_bulk_strings(arg.clone()).as_bytes()).await;
                    }
                    [cmd, arg1, arg2, rest @ ..] if cmd.to_uppercase() == "SET".to_string() => {
                        match rest {
                            [] => {
                                let value = MapValue::new(arg2.to_string(), None);
                                {
                                    let mut db = db.lock().unwrap();
                                    db.insert(arg1.to_string(), value);
                                }
                                let _ = wr.write_all(encode_simple_strings("OK".to_string()).as_bytes()).await;
                            },
                            [option, seconds] if option.to_uppercase() == "EX" => {
                                let now = Instant::now();
                                let expires_at = now + Duration::from_secs(seconds.parse().unwrap());

                                let value = MapValue::new(arg2.to_string(), Some(expires_at));
                                {
                                    let mut db = db.lock().unwrap();
                                    db.insert(arg1.to_string(), value);
                                }
                                let _ = wr.write_all(encode_simple_strings("OK".to_string()).as_bytes()).await;
                            },
                            [option, milliseconds] if option.to_uppercase() == "PX" => {
                                let now = Instant::now();
                                let expires_at = now + Duration::from_millis(milliseconds.parse().unwrap());

                                let value = MapValue::new(arg2.to_string(), Some(expires_at));
                                {
                                    let mut db = db.lock().unwrap();
                                    db.insert(arg1.to_string(), value);
                                }
                                let _ = wr.write_all(encode_simple_strings("OK".to_string()).as_bytes()).await;
                            }
                            _=> unreachable!()
                        }
                    }
                    [cmd, arg] if cmd.to_uppercase() == "GET".to_string() => {
                        let response = {
                            let db = db.lock().unwrap();
                            
                            if let Some(value) = db.get(arg) {
                                match value.expires_at {
                                    Some(instant) => {
                                        if Instant::now() > instant {
                                            encode_bulk_strings("".to_string())
                                        } else {
                                            encode_bulk_strings(value.value.clone())
                                        }
                                    }
                                    None => {
                                        encode_bulk_strings(value.value.clone())
                                    }
                                }
                            } else {
                                encode_bulk_strings("".to_string())
                            }
                        };

                        let _ = wr.write_all(response.as_bytes()).await;
                    }
                    _ => unreachable!(),
                }
            }
            Err(_) => break,
        }
    }
}


/// Simple strings 
/// 
/// Simple strings are encoded as a plus (+) character, followed by a string. The string mustn't contain a CR (\r) or LF (\n) character and is terminated by CRLF (i.e., \r\n).
/// Simple strings transmit short, non-binary strings with minimal overhead. For example, many Redis commands reply with just "OK" on success. 
/// The encoding of this Simple String is the following 5 bytes:
/// ```text
/// +OK\r\n
/// ```
/// When Redis replies with a simple string, a client library should return to the caller a string value composed of the first character after the + up to the end of the string, excluding the final CRLF bytes.
/// To send binary strings, use bulk strings instead.
fn encode_simple_strings(input: String) -> String {
    format!("+{}\r\n", input)
}

/// RESP decode simple strings
fn decode_simple_strings(input: String) -> String {
    input.trim_end_matches("\r\n").trim_start_matches("+").to_string()
}

/// RESP encode bulk strings
///
/// ```text
/// $<length>\r\n<data>\r\n
/// ```
///
/// $ : The dollar sign ($) as the first byte.
///
/// `<length>` : One or more decimal digits (0..9) as the string's length, in bytes, as an unsigned, base-10 value.
///
/// \r\n : The CRLF terminator.
///
/// `<data>` : The data.
///
/// \r\n : A final CRLF.
///
/// # Examples
///
/// So the string "hello" is encoded as follows:
///
/// ```text
/// $5\r\nhello\r\n
/// ```
fn encode_bulk_strings(input: String) -> String {
    if input.is_empty() {
        format!("$-1\r\n")
    } else {
        format!("${}\r\n{}\r\n", input.len(), input)
    }
}

/// RESP decode bulk strings
fn decode_bulk_strings(input: String) -> String {
    todo!()
}

/// RESP encode arrays'
///
/// ```text
/// *<number-of-elements>\r\n<element-1>...<element-n>
/// ```
///
/// * : An asterisk (*) as the first byte.
///
/// `<number-of-elements>` : One or more decimal digits (0..9) as the number of elements in the array as an unsigned, base-10 value.
///
/// \r\n : The CRLF terminator.
///
/// `<element-1>...<element-n>` : An additional RESP type for every element of the array.
///
/// # Examples
///
/// So the encoding of an array consisting of the two bulk strings "hello" and "world" is:
///
/// ```text
/// *2\r\n$5\r\nhello\r\n$5\r\nworld\r\n
/// ```
fn encode_arrays(arr: &[&str]) -> String {
    let mut output = String::new();
    output.push_str("*");
    output.push_str(&arr.len().to_string());
    output.push_str("\r\n");

    for el in arr {
        output.push_str(&encode_bulk_strings(el.to_string()));
    }

    output
}

/// RESP decode arrays'
/// 
/// # Examples
///
/// So the the following,
///
/// ```text
/// *2\r\n $4\r\nECHO\r\n$3\r\nhey\r\n
/// ```
/// 
/// is decoded to,
/// 
/// ```text
/// ["ECHO", "hey"]
/// ```
fn decode_arrays(input: &str) -> Vec<String> {
    input.split("\r\n").filter(|e| !e.is_empty())
    .filter(|e| !e.starts_with('*'))
    .filter(|e| !e.starts_with('$'))
    .map(|e| e.to_string())
    .collect::<Vec<_>>()
}
