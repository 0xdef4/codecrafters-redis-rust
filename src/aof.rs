use std::fs;
use std::io::{Read, Write};
use std::sync::Arc;

use tokio::sync::Notify;

use crate::commands::dispatch_command_inner;
use crate::protocol::{RespValue, decode_arrays, encode};
use crate::{Config, Db};

pub fn init_aof_if_enabled(config: &Config) {
    // if appendonly is set to yes
    if config.appendonly == "yes" {
        // Create append-only directory
        let (dir, appenddirname, appendfilename) =
            (&config.dir, &config.appenddirname, &config.appendfilename);

        let path = dir.join(appenddirname);
        let _ = fs::create_dir_all(&path);

        // Create the Append-Only File
        let aof_filename = format!("{}.1.incr.aof", appendfilename);
        if !path.join(&aof_filename).exists() {
            let _ = fs::File::create(&path.join(&aof_filename));
        }

        // Create the manifest File
        let manifest_filename = format!("{}.manifest", appendfilename);
        if !path.join(&manifest_filename).exists() {
            let mut f = fs::File::create(&path.join(manifest_filename)).unwrap();
            // and write
            let _ = f.write_all(format!("file {} seq 1 type i", &aof_filename).as_bytes());
        }
    }
}

pub fn append_to_aof(command: &[String], config: &Arc<Config>) {
    let command_to_append_in_resp_format: String = encode(RespValue::Array(
        command
            .iter()
            .map(|e| RespValue::BulkString(e.to_string()))
            .collect::<Vec<_>>(),
    ));

    // server should read the manifest file
    let (dir, appenddirname, appendfilename) =
        (&config.dir, &config.appenddirname, &config.appendfilename);
    let path = dir.join(appenddirname);
    let manifest_filename = format!("{}.manifest", appendfilename);

    let mut f = fs::File::open(&path.join(manifest_filename)).unwrap();
    let mut buf = [0u8; 512];

    match f.read(&mut buf) {
        Ok(n) => {
            let received = String::from_utf8_lossy(&buf[..n]);

            // find the name of the AOF file to write to.
            let aof_filename = received.split_ascii_whitespace().nth(1).unwrap();

            // write to AOF file
            let mut f = fs::OpenOptions::new()
                .append(true)
                .open(&path.join(aof_filename))
                .unwrap();
            let _ = f.write_all(command_to_append_in_resp_format.as_bytes());

            if config.appendfsync == "always" {
                let _ = f.flush(); // BufWriter 버퍼 → OS 버퍼
                let _ = f.sync_all(); // OS 버퍼 → 실제 디스크
            }
        }
        _ => {
            unimplemented!()
        }
    }
}

pub fn replay_commands(config: &Arc<Config>, db: &Db, notify: &Arc<Notify>) {
    let (dir, appenddirname, appendfilename) =
        (&config.dir, &config.appenddirname, &config.appendfilename);

    let path = dir.join(appenddirname);

    // When the server starts with --appendonly yes and the append-only directory already exists,
    if config.appendonly != "yes" || !fs::exists(&path).unwrap_or(false) {
        return;
    }

    let manifest_filename = format!("{}.manifest", appendfilename);
    let manifest_path = path.join(&manifest_filename);

    let Ok(manifest_content) = fs::read_to_string(&manifest_path) else {
        return; // if no manifest just return (first attempt case)
    };

    // get filename from "file <filename> seq 1 type i"
    let Some(aof_filename) = manifest_content.split_ascii_whitespace().nth(1) else {
        return;
    };

    let Ok(aof_content) = fs::read_to_string(&path.join(aof_filename)) else {
        return;
    };

    // and parse the RESP-encoded commands inside it
    let commands = decode_arrays(&aof_content);

    for command in commands {
        if command.is_empty() {
            continue;
        }

        dispatch_command_inner(&command, db, &notify, &config);
    }
}
