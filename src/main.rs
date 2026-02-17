// #![allow(unused_imports)]
use std::{
    io::{BufReader, Read, Write},
    net::{TcpListener, TcpStream},
    thread,
};

fn main() {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");

    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();

    for stream in listener.incoming() {
        match stream {
            Ok(mut stream) => {
                println!("accepted new connection");

                let _ = thread::spawn(move || {
                    handle_connection(&mut stream);
                });
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}

fn handle_connection(stream: &mut TcpStream) {
    loop {
        let mut reader = BufReader::new(&mut *stream);

        let mut buffer = [0; 512];
        match reader.read(&mut buffer) {
            Ok(0) => break,
            Ok(_) => {
                reader.get_mut().write_all(b"+PONG\r\n").unwrap();
            }
            Err(_) => break,
        }
    }
}
