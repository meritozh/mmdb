use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("storage error: {0}")]
    Storage(String),
    #[error("codec error: {0}")]
    Codec(String),
    #[error("not found")]
    NotFound,
    #[error("invalid argument: {0}")]
    InvalidArgument(String),
    #[error("state revision conflict in {namespace}: expected {expected}, found {actual}")]
    StateRevisionConflict {
        namespace: String,
        key: Vec<u8>,
        expected: u64,
        actual: u64,
    },
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),
}

pub type Result<T, E = Error> = std::result::Result<T, E>;
