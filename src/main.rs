#![allow(unused_imports)]
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader, AsyncBufReadExt};
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
    // let mut buffer = [0; 512];
    let (rd, mut wr) = stream.into_split();
    let mut rd = BufReader::new(rd);

    let mut buf = String::new();
    
    loop {
        match rd.read_line(&mut buf).await {
                Ok(0) => break,
                Ok(_) => {
                    println!("buf: {}", buf);
                    let _ = wr.write_all(b"+PONG\r\n").await;
                }
                Err(_) => break,
        }
    }
}
