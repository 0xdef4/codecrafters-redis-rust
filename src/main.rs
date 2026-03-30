#![allow(unused_imports)]
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};

#[tokio::main]
async fn main() {
    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();

    loop {
        match listener.accept().await {
            Ok((stream, _addr)) => {
                // println!("socket addr: {:?}", addr);

                tokio::spawn(async move {
                    handle_stream(stream).await;
                });
            }
            Err(e) => println!("couldn't get client: {:?}", e),
        }
    }
}

async fn handle_stream(stream: TcpStream) {
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
                        let response = encode_bulk_strings(arg.clone());
                        let _ = wr.write_all(response.as_bytes()).await;
                    }
                    _ => unreachable!(),
                }
            }
            Err(_) => break,
        }
    }
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
    format!("${}\r\n{}\r\n", input.len(), input)
}

/// RESP decode bulk strings
fn decode_bulk_strings(input: String) -> String {
    input.split_ascii_whitespace().nth(1).unwrap().to_string()
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
fn decode_arrays(input: &str) -> Vec<String> {
    // Objective : *2\r\n $4\r\nECHO\r\n$3\r\nhey\r\n     ->     ["ECHO", "hey"]

    input.split("\r\n").filter(|e| !e.is_empty())
    .filter(|e| !e.starts_with('*'))
    .filter(|e| !e.starts_with('$'))
    .map(|e| e.to_string())
    .collect::<Vec<_>>()
}
