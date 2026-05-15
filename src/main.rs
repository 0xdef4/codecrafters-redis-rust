use tokio::net::TcpListener;
use tokio::sync::{Mutex as TokioMutex, Notify};

use std::collections::HashMap;
use std::fs::File;
use std::path::Path;
use std::sync::{Arc, Mutex};

mod acl;
mod config;
mod db;
mod geospatial;
mod handler;
mod pubsub;
mod rdb;
mod replication;
mod resp;

use acl::*;
use config::*;
use db::*;
use geospatial::*;
use handler::*;
use pubsub::*;
use rdb::*;
use replication::*;
use resp::*;

#[tokio::main]
async fn main() {
    let config = Arc::new(Config::from_args());

    let db = Arc::new(Mutex::new(HashMap::new()));
    let notify = Arc::new(Notify::new());
    let replicas: Replicas = Arc::new(TokioMutex::new(Vec::new()));
    let pubsub = Arc::new(Mutex::new(HashMap::new()));
    let acl_db: AclDb = Arc::new(Mutex::new({
        let mut acl = HashMap::new();
        acl.insert("default".to_string(), AclUser::new());
        acl
    }));

    rdb::load_if_exists(&db, &config);
    replication::start_if_replica(&db, Arc::clone(&config));

    // TODO : check
    // if config.dir.is_some() && config.dbfilename.is_some() {
    //     let dir = config.dir.as_ref().unwrap();
    //     let dbfilename = config.dbfilename.as_ref().unwrap();

    //     let path_string = format!("{}/{}", dir, dbfilename);
    //     let path = Path::new(path_string.as_str());

    //     let f = File::open(path);

    //     match f {
    //         Ok(mut rdb_file) => {
    //             // RDB 파일 존재하면 파싱해서 db에 insert
    //             let db = Arc::clone(&db);
    //             parse_rdb(&mut rdb_file, db);
    //         }
    //         Err(_) => {
    //             // do nothing
    //         }
    //     }
    // }

    let listener = TcpListener::bind(format!("127.0.0.1:{}", config.port))
        .await
        .unwrap();

    loop {
        match listener.accept().await {
            Ok((stream, _addr)) => {
                let db = Arc::clone(&db);
                let notify = Arc::clone(&notify);
                let replicas = Arc::clone(&replicas);
                let config = Arc::clone(&config);
                let pubsub = Arc::clone(&pubsub);
                let acl_db = Arc::clone(&acl_db);

                tokio::spawn(async move {
                    handle_stream(
                        stream,
                        db,
                        notify,
                        config.role().to_string(),
                        replicas,
                        config,
                        pubsub,
                        acl_db,
                    )
                    .await;
                });
            }
            Err(e) => println!("couldn't get client: {:?}", e),
        }
    }
}
