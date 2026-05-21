use std::env;
use std::path::PathBuf;

#[derive(Clone)]
pub struct Config {
    pub port: u16,
    pub replicaof: Option<String>,
    // The base directory where Redis stores its data files
    pub dir: PathBuf,
    pub dbfilename: Option<String>,
    // Controls whether AOF persistence is enabled or disabled
    pub appendonly: String,
    // The subdirectory under dir where AOF and manifest files are stored
    pub appenddirname: String,
    // The name of the append-only file that records write operations
    pub appendfilename: String,
    // How often buffered writes are flushed to the AOF file on disk
    pub appendfsync: String,
}

impl Config {
    pub fn from_args() -> Self {
        let args: Vec<String> = std::env::args().collect();

        let mut config = Config {
            port: 6379u16,
            replicaof: None,
            dir: env::current_dir().unwrap(),
            dbfilename: None,
            appendonly: "no".to_string(),
            appenddirname: "appendonlydir".to_string(),
            appendfilename: "appendonly.aof".to_string(),
            appendfsync: "everysec".to_string(),
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
                    config.dir = args[i + 1].clone().into();
                    i += 2;
                }
                "--dbfilename" => {
                    config.dbfilename = Some(args[i + 1].clone());
                    i += 2;
                }
                "--appendonly" => {
                    config.appendonly = args[i + 1].clone().into();
                    i += 2;
                }
                "--appenddirname" => {
                    config.appenddirname = args[i + 1].clone().into();
                    i += 2;
                }
                "--appendfilename" => {
                    config.appendfilename = args[i + 1].clone().into();
                    i += 2;
                }
                "--appendfsync" => {
                    config.appendfsync = args[i + 1].clone().into();
                    i += 2;
                }

                _ => i += 1,
            }
        }

        config
    }

    pub fn role(&self) -> &'static str {
        if self.replicaof.is_some() {
            "slave"
        } else {
            "master"
        }
    }
}
