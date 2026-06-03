/*
src/
  commands.rs          // dispatch_command, dispatch_command_inner
  commands/
    string.rs          // SET, GET, INCR
    list.rs            // LPUSH, RPUSH, LPOP, LRANGE, LLEN, BLPOP
    stream.rs          // XADD, XRANGE, XREAD
    zset.rs            // ZADD, ZRANK, ZRANGE, ZSCORE, ZREM, ZCARD
    geo.rs             // GEOADD, GEOPOS, GEODIST, GEOSEARCH
    server.rs          // PING, ECHO, INFO, CONFIG, KEYS, TYPE
    transaction.rs     // MULTI, EXEC, DISCARD, WATCH, UNWATCH
    pubsub.rs          // SUBSCRIBE, PUBLISH
    acl.rs             // ACL, AUTH
    replication.rs     // REPLCONF, PSYNC, WAIT
*/
use std::sync::Arc;

use tokio::sync::Notify;

use crate::Config;
use crate::handler::ClientState;
use crate::protocol::RespValue;
use crate::types::{AclDb, Db, Pubsub};

mod acl;
mod geo;
mod list;
mod pubsub;
mod replication;
mod server;
mod stream;
mod string;
mod transaction;
mod zset;

pub use acl::{execute_acl, execute_auth};
pub use geo::{execute_geoadd, execute_geodist, execute_geopos, execute_geosearch};
pub use list::{
    execute_blpop, execute_llen, execute_lpop, execute_lpush, execute_lrange, execute_rpush,
};
pub use pubsub::{execute_publish, execute_subscribe};
pub use replication::{execute_psync, execute_replconf, execute_wait};
pub use server::{
    execute_config, execute_echo, execute_info, execute_keys, execute_ping, execute_type,
};
pub use stream::{execute_xadd, execute_xrange, execute_xread};
pub use string::{execute_get, execute_incr, execute_set};
pub use transaction::{
    execute_discard, execute_exec, execute_multi, execute_unwatch, execute_watch,
};
pub use zset::{
    execute_zadd, execute_zcard, execute_zrange, execute_zrank, execute_zrem, execute_zscore,
};

// general purpose dispatch fn
pub async fn dispatch_command(
    command: &[String],
    db: &Db,
    notify: &Arc<Notify>,
    config: &Arc<Config>,
    role: &str,
    pubsub: &Pubsub,
    acl_db: &AclDb,
    client_state: &mut ClientState,
) -> Option<RespValue> {
    match command[0].to_uppercase().as_str() {
        // string.rs
        "SET" => execute_set(command, db),
        "GET" => execute_get(command, db),
        "INCR" => execute_incr(command, db),
        // list.rs
        "LPUSH" => execute_lpush(command, db, notify),
        "RPUSH" => execute_rpush(command, db, notify),
        "LPOP" => execute_lpop(command, db),
        "LRANGE" => execute_lrange(command, db),
        "LLEN" => execute_llen(command, db),
        "BLPOP" => execute_blpop(command, db, notify).await,
        // stream.rs
        "XADD" => execute_xadd(command, db, notify),
        "XRANGE" => execute_xrange(command, db),
        "XREAD" => execute_xread(command, db, notify).await,
        // zset.rs
        "ZADD" => execute_zadd(command, db),
        "ZRANK" => execute_zrank(command, db),
        "ZRANGE" => execute_zrange(command, db),
        "ZSCORE" => execute_zscore(command, db),
        "ZREM" => execute_zrem(command, db),
        "ZCARD" => execute_zcard(command, db),
        // geo.rs
        "GEOADD" => execute_geoadd(command, db),
        "GEOPOS" => execute_geopos(command, db),
        "GEODIST" => execute_geodist(command, db),
        "GEOSEARCH" => execute_geosearch(command, db),
        // server.rs
        "PING" => execute_ping(command),
        "ECHO" => execute_echo(command),
        "INFO" => execute_info(command, role),
        "CONFIG" => execute_config(command, config),
        "KEYS" => execute_keys(command, db),
        "TYPE" => execute_type(command, db),
        // transaction.rs
        "MULTI" => execute_multi(command, &mut client_state.in_multi),
        "EXEC" => {
            execute_exec(
                command,
                db,
                notify,
                config,
                &mut client_state.in_multi,
                &mut client_state.command_queue,
                &mut client_state.watched_keys,
            )
            .await
        }
        "DISCARD" => execute_discard(
            command,
            &mut client_state.in_multi,
            &mut client_state.command_queue,
            &mut client_state.watched_keys,
        ),
        "WATCH" => execute_watch(
            command,
            db,
            &mut client_state.in_multi,
            &mut client_state.watched_keys,
        ),
        "UNWATCH" => execute_unwatch(command, &mut client_state.watched_keys),

        // pubsub.rs
        "PUBLISH" => execute_publish(command, pubsub).await,
        // acl.rs
        "ACL" => execute_acl(command, acl_db),
        "AUTH" => execute_auth(command, acl_db, &mut client_state.is_authenticated),
        // replication.rs
        "REPLCONF" => execute_replconf(command),

        _ => Some(RespValue::SimpleError("ERR unknown command".to_string())),
    }
}

// only used for specific use cases : AOF replay, EXEC command
pub fn dispatch_command_inner(
    command: &[String],
    db: &Db,
    notify: &Arc<Notify>,
    config: &Arc<Config>,
) -> Option<RespValue> {
    match command[0].to_uppercase().as_str() {
        // string.rs
        "SET" => execute_set(command, db),
        "GET" => execute_get(command, db),
        "INCR" => execute_incr(command, db),

        // list.rs (BLPOP excluded)
        "LPUSH" => execute_lpush(command, db, notify),
        "RPUSH" => execute_rpush(command, db, notify),
        "LPOP" => execute_lpop(command, db),
        "LRANGE" => execute_lrange(command, db),
        "LLEN" => execute_llen(command, db),

        // stream.rs (XREAD excluded)
        "XADD" => execute_xadd(command, db, notify),
        "XRANGE" => execute_xrange(command, db),

        // zset.rs
        "ZADD" => execute_zadd(command, db),
        "ZRANK" => execute_zrank(command, db),
        "ZRANGE" => execute_zrange(command, db),
        "ZSCORE" => execute_zscore(command, db),
        "ZREM" => execute_zrem(command, db),
        "ZCARD" => execute_zcard(command, db),

        // geo.rs
        "GEOADD" => execute_geoadd(command, db),
        "GEOPOS" => execute_geopos(command, db),
        "GEODIST" => execute_geodist(command, db),
        "GEOSEARCH" => execute_geosearch(command, db),

        // server.rs (INFO excluded)
        "PING" => execute_ping(command),
        "ECHO" => execute_echo(command),
        "CONFIG" => execute_config(command, config),
        "KEYS" => execute_keys(command, db),
        "TYPE" => execute_type(command, db),

        // pubsub.rs (SUBSCRIBE PUBLISH excluded)

        // acl.rs (ACL AUTH excluded)

        // transaction.rs (MULTI EXEC DISCARD WATCH UNWATCH excluded)

        // replication.rs (REPLCONF excluded)
        _ => Some(RespValue::SimpleError("ERR unknown command".to_string())),
    }
}
