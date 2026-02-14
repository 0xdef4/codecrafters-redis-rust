#![allow(unused_imports)]
use std::{io::{BufRead, BufReader, Read, Write}, net::{TcpListener, TcpStream}};

fn main() {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");

    // Uncomment the code below to pass the first stage
    
    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();
    
    for stream in listener.incoming() {
        match stream {
            Ok(mut stream) => {
                println!("accepted new connection");

                handle_connection(&mut stream);
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}

fn handle_connection(stream: &mut TcpStream) {
    // loop {
    //     let reader = BufReader::new(&mut *stream);

    //     for _ in reader.lines() {
    //         stream.write_all(b"+PONG\r\n").unwrap();
    //     }

    // }




    let mut reader = BufReader::new(&mut *stream);
    loop {
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
