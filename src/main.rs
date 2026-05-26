use tokio::net::TcpListener;
use tokio::sync::{Mutex as TokioMutex, Notify};

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

mod acl;
mod aof;
mod config;
mod geospatial;
mod handler;
mod protocol;
mod pubsub;
mod rdb;
mod replication;
mod types;

use config::*;
use handler::*;

use crate::types::acl::{AclDb, AclUser};
use crate::types::db::Db;
use crate::types::pubsub::Pubsub;
use crate::types::replication::Replicas;

#[tokio::main]
async fn main() {
    let config = Arc::new(Config::from_args());

    let db: Db = Arc::new(Mutex::new(HashMap::new()));
    let notify = Arc::new(Notify::new());
    let replicas: Replicas = Arc::new(TokioMutex::new(Vec::new()));
    let pubsub: Pubsub = Arc::new(Mutex::new(HashMap::new()));
    let acl_db: AclDb = Arc::new(Mutex::new({
        let mut acl = HashMap::new();
        acl.insert("default".to_string(), AclUser::new());
        acl
    }));

    rdb::load_if_exists(&db, &config);
    replication::start_if_replica(&db, Arc::clone(&config));
    aof::init_aof_if_enabled(&config);

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
