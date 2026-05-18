// Access Control List

use sha2::{Digest, Sha256};

pub fn sha256_hash(password: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(password.as_bytes());
    let hash = hasher.finalize();
    let hash = hex::encode(hash);

    hash
}
