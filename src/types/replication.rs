use std::sync::Arc;

use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::sync::Mutex as TokioMutex;

pub type Replicas = Arc<TokioMutex<Vec<(OwnedWriteHalf, OwnedReadHalf)>>>;
