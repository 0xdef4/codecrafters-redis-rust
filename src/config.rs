#[derive(Clone)]
pub struct Config {
    pub port: u16,
    pub replicaof: Option<String>,
    pub dir: Option<String>,
    pub dbfilename: Option<String>,
}

impl Config {
    pub fn from_args() -> Self {
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
