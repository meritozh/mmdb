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
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),
}

pub type Result<T, E = Error> = std::result::Result<T, E>;
