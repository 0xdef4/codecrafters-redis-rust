use tokio::sync::mpsc::Sender;

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

pub type Pubsub = Arc<Mutex<HashMap<String, Vec<(u64, Sender<(String, String)>)>>>>;
