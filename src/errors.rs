use polars::prelude::PolarsError;
use serde_json::Error as SerdeJsonError;
use std::io::Error as IoError;
use tokio::task::JoinError;

#[derive(Debug)]
pub enum ConsumerError {
    Async(JoinError),
    DataFrame(PolarsError),
    Deserialize(SerdeJsonError),
    InvalidMessage(),
    FileWrite(IoError),
}

impl From<JoinError> for ConsumerError {
    fn from(item: JoinError) -> Self {
        ConsumerError::Async(item)
    }
}

impl From<PolarsError> for ConsumerError {
    fn from(item: PolarsError) -> Self {
        ConsumerError::DataFrame(item)
    }
}

impl From<SerdeJsonError> for ConsumerError {
    fn from(item: SerdeJsonError) -> Self {
        ConsumerError::Deserialize(item)
    }
}

impl From<IoError> for ConsumerError {
    fn from(item: IoError) -> Self {
        ConsumerError::FileWrite(item)
    }
}

impl std::fmt::Display for ConsumerError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            ConsumerError::Async(e) => write!(f, "Async error: {}", e),
            ConsumerError::DataFrame(e) => write!(f, "DataFrame error: {}", e),
            ConsumerError::Deserialize(e) => write!(f, "DataFrame error: {}", e),
            ConsumerError::InvalidMessage() => write!(f, "Invalid message received"),
            ConsumerError::FileWrite(e) => write!(f, "I/O error: {}", e),
        }
    }
}

pub type ConsumerResult<T> = Result<T, ConsumerError>;
