use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use tokio::sync::mpsc::Sender;

pub type Pubsub = Arc<Mutex<HashMap<String, Vec<(u64, Sender<(String, String)>)>>>>;
