use super::{ContextRef, InvalidTemporalFacts};
use crate::OperationId;
use mmdb_kernel::KernelError;
use std::fmt;

#[derive(Debug)]
pub enum ContextError {
    Storage(KernelError),
    Io(std::io::Error),
    Serialization(serde_json::Error),
    InvalidInput(String),
    Corrupt(String),
    UnsupportedFormat,
    AccessDenied,
    NotFound(ContextRef),
    Unavailable(ContextRef),
    RevisionConflict { expected: u64, actual: u64 },
    AvailabilityConflict { expected: u64, actual: u64 },
    PositionConflict { expected: u64, actual: u64 },
    TypeConflict(String),
    IdempotencyConflict,
    CommitUnknown(OperationId),
    BudgetExceeded,
}

impl fmt::Display for ContextError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Storage(e) => write!(f, "{e}"),
            Self::Io(e) => write!(f, "context input I/O: {e}"),
            Self::Serialization(e) => write!(f, "context serialization: {e}"),
            Self::InvalidInput(e) => write!(f, "invalid context input: {e}"),
            Self::Corrupt(e) => write!(f, "corrupt context: {e}"),
            Self::UnsupportedFormat => write!(f, "unsupported context store format"),
            Self::AccessDenied => write!(f, "context reference is outside the allowed owner or scope"),
            Self::NotFound(r) => write!(f, "context record not found: {}", r.id),
            Self::Unavailable(r) => write!(f, "context record or its source is unavailable: {}", r.id),
            Self::RevisionConflict { expected, actual } => write!(f, "context revision conflict: expected {expected}, actual {actual}"),
            Self::AvailabilityConflict { expected, actual } => write!(f, "context availability conflict: expected {expected}, actual {actual}"),
            Self::PositionConflict { expected, actual } => write!(f, "session log position conflict: expected {expected}, actual {actual}"),
            Self::TypeConflict(name) => write!(f, "context type already has a different definition: {name}"),
            Self::IdempotencyConflict => write!(f, "operation or event identity was reused with different content"),
            Self::CommitUnknown(id) => write!(f, "context commit outcome unknown for operation {id}; inspect its receipt or retry the same operation"),
            Self::BudgetExceeded => write!(f, "context validation or query budget exceeded"),
        }
    }
}

impl std::error::Error for ContextError {}
impl From<InvalidTemporalFacts> for ContextError {
    fn from(error: InvalidTemporalFacts) -> Self {
        Self::InvalidInput(error.to_string())
    }
}
impl From<std::io::Error> for ContextError {
    fn from(e: std::io::Error) -> Self {
        Self::Io(e)
    }
}
impl From<serde_json::Error> for ContextError {
    fn from(e: serde_json::Error) -> Self {
        Self::Serialization(e)
    }
}
impl From<KernelError> for ContextError {
    fn from(e: KernelError) -> Self {
        Self::Storage(e)
    }
}

pub type ContextResult<T> = Result<T, ContextError>;
