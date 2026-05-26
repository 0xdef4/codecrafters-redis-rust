use std::fs;
use std::io::{Read, Write};
use std::sync::Arc;

use crate::Config;
use crate::protocol::resp::{RespValue, encode};

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
        let _ = fs::File::create(&path.join(&aof_filename));

        // Create the manifest File
        let manifest_filename = format!("{}.manifest", appendfilename);
        let mut f = fs::File::create(&path.join(manifest_filename)).unwrap();
        // and write
        let _ = f.write_all(format!("file {} seq 1 type i", &aof_filename).as_bytes());
    }
}

pub fn append_to_aof(resp_array: &[String], config: &Arc<Config>) {
    let command_to_append_in_resp_format: String = encode(RespValue::Array(
        resp_array
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
