use tokio::net::TcpListener;
use tokio::sync::{Mutex as TokioMutex, Notify};

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

mod db;
mod handler;
mod replication;
mod resp;

use db::*;
use handler::*;
use replication::*;
use resp::*;

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().collect();
    let mut port = 6379u16;
    let mut replicaof: Option<String> = None;

    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--port" => {
                port = args[i + 1].parse::<u16>().unwrap();
                i += 2;
            }
            "--replicaof" => {
                replicaof = Some(args[i + 1].clone());
                i += 2;
            }
            _ => i += 1,
        }
    }

    let listener = TcpListener::bind(format!("127.0.0.1:{}", port))
        .await
        .unwrap();

    let db = Arc::new(Mutex::new(HashMap::new()));
    let notify = Arc::new(Notify::new());
    let role = if replicaof.is_some() {
        "slave"
    } else {
        "master"
    };
    let replicas: Replicas = Arc::new(TokioMutex::new(Vec::new()));

    if let Some(replicaof) = replicaof {
        let db = Arc::clone(&db);

        tokio::spawn(async move {
            start_replica_handshake(replicaof, port, db).await;
        });
    }

    loop {
        match listener.accept().await {
            Ok((stream, _addr)) => {
                let db = Arc::clone(&db);
                let notify = Arc::clone(&notify);
                let replicas = Arc::clone(&replicas);

                tokio::spawn(async move {
                    handle_stream(stream, db, notify, role.to_string(), replicas).await;
                });
            }
            Err(e) => println!("couldn't get client: {:?}", e),
        }
    }
}
