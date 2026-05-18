use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::sync::Mutex as TokioMutex;

use std::sync::Arc;

pub type Replicas = Arc<TokioMutex<Vec<(OwnedWriteHalf, OwnedReadHalf)>>>;
