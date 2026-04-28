use tokio::net::TcpListener;
use tokio::sync::{Mutex as TokioMutex, Notify};

use std::collections::HashMap;
use std::fmt::format;
use std::fs::File;
use std::path::Path;
use std::sync::{Arc, Mutex};

mod db;
mod handler;
mod rdb;
mod replication;
mod resp;

use db::*;
use handler::*;
use rdb::*;
use replication::*;
use resp::*;

#[derive(Clone)]
pub struct Config {
    pub port: u16,
    pub replicaof: Option<String>,
    pub dir: Option<String>,
    pub dbfilename: Option<String>,
}

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().collect();

    let mut config = Config {
        port: 6379u16,
        replicaof: None,
        dir: None,
        dbfilename: None,
    };

    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--port" => {
                config.port = args[i + 1].parse::<u16>().unwrap();
                i += 2;
            }
            "--replicaof" => {
                config.replicaof = Some(args[i + 1].clone());
                i += 2;
            }
            "--dir" => {
                config.dir = Some(args[i + 1].clone());
                i += 2;
            }
            "--dbfilename" => {
                config.dbfilename = Some(args[i + 1].clone());
                i += 2;
            }
            _ => i += 1,
        }
    }

    let listener = TcpListener::bind(format!("127.0.0.1:{}", config.port))
        .await
        .unwrap();

    let db = Arc::new(Mutex::new(HashMap::new()));
    let notify = Arc::new(Notify::new());
    let role = if config.replicaof.is_some() {
        "slave"
    } else {
        "master"
    };
    let replicas: Replicas = Arc::new(TokioMutex::new(Vec::new()));
    let config = Arc::new(config);

    if config.replicaof.is_some() {
        let db = Arc::clone(&db);
        let config = Arc::clone(&config);

        tokio::spawn(async move {
            start_replica_handshake(config, db).await;
        });
    }

    // read RDB file from path

    if config.dir.is_some() && config.dbfilename.is_some() {
        let dir = config.dir.as_ref().unwrap();
        let dbfilename = config.dbfilename.as_ref().unwrap();

        // Note: this example does work on Windows
        let path_string = format!("{}/{}", dir, dbfilename);
        let path = Path::new(path_string.as_str());

        let f = File::open(path);

        match f {
            Ok(mut rdb_file) => {
                // RDB 파일 존재하면 파싱해서 db에 insert
                let db = Arc::clone(&db);
                parse_rdb(&mut rdb_file, db);
            }
            Err(_) => {
                // do nothing
            }
        }
    }

    loop {
        match listener.accept().await {
            Ok((stream, _addr)) => {
                let db = Arc::clone(&db);
                let notify = Arc::clone(&notify);
                let replicas = Arc::clone(&replicas);
                let config = Arc::clone(&config);

                tokio::spawn(async move {
                    handle_stream(stream, db, notify, role.to_string(), replicas, config).await;
                });
            }
            Err(e) => println!("couldn't get client: {:?}", e),
        }
    }
}
