// Access Control List

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

// ACL DB (username -> AclUser)
pub type AclDb = Arc<Mutex<HashMap<String, AclUser>>>;

#[derive(Debug, Clone)]
pub struct AclUser {
    flags: Vec<String>, // "on", "nopass", "allkeys" etc
    passwords: Vec<String>, // hashed passwords
                        // commands: String,       // "@all", "-@dangerous" etc
                        // keys: String,           // "*" etc
}

impl AclUser {
    pub fn new() -> Self {
        Self {
            flags: vec!["nopass".to_string()],
            passwords: vec![],
            // commands: "".to_string(),
            // keys: "".to_string(),
        }
    }

    pub fn store_password(&mut self, password_hash: String) {
        self.passwords.push(password_hash);

        if !self.passwords.is_empty() {
            self.flags.retain(|e| *e != "nopass".to_string());
        }
    }

    pub fn get_flags(&self) -> Vec<String> {
        self.flags.clone()
    }

    pub fn get_passwords(&self) -> Vec<String> {
        self.passwords.clone()
    }

    pub fn is_valid_password(&self, password_hash: String) -> bool {
        if self.flags.contains(&"nopass".to_string()) {
            return true;
        }

        self.passwords.contains(&password_hash)
    }
}
