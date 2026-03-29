#![allow(unused_imports)]
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};

#[tokio::main]
async fn main() {
    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();

    loop {
        match listener.accept().await {
            Ok((stream, addr)) => {
                println!("socket addr: {:?}", addr);

                tokio::spawn(async move {
                    handle_stream(stream).await;
                });
            }
            Err(e) => println!("couldn't get client: {:?}", e),
        }
    }
}

async fn handle_stream(stream: TcpStream) {
    let mut buffer = [0; 512];
    let (mut rd, mut wr) = stream.into_split();
    
    loop {
        match rd.read(&mut buffer).await {
            Ok(0) => break,
            Ok(_) => {
                let _ = wr.write_all(b"+PONG\r\n").await;
            }
            Err(_) => break,
        }
    }
}

/*
use tokio::net::TcpListener;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[tokio::main]
async fn main() {
    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();

    loop {
        let stream = listener.accept().await;

        match stream {
            Ok((mut stream, _)) => {
                println!("accepted new connection");

                tokio::spawn(async move {
                    let mut buf = [0; 512];
                    loop {
                        let read_count = stream.read(&mut buf).await.unwrap();
                        if read_count == 0 {
                            break;
                        }

                        stream.write(b"+PONG\r\n").await.unwrap();
                    }
                });
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}
*/
