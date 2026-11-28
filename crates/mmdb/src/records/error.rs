use super::{ContextRef, InvalidTemporalFacts};
use crate::OperationId;
use mmdb_kernel::KernelError;
use std::fmt;

/// The first turn or step rule that refused an append.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TurnError {
    /// The harness supplied Interrupted or Forked.
    ReservedReason,
    /// Starting a turn requires a run in the writer's access.
    RunRequired,
    /// This session already has an open turn.
    TurnOpen,
    /// This call already has a result.
    Answered,
    /// The event requires an open turn.
    OutsideTurn,
    /// Only the opening run can write this event.
    OtherRun,
    /// The end event names another turn.
    OtherTurn,
    /// This turn already has an open step.
    StepOpen,
    /// The required step is closed or belongs to another message.
    OutsideStep,
    /// The step end names another step.
    OtherStep,
    /// This step still owes a terminal or a result.
    StepUnfinished,
    /// This turn still has an open step or an unanswered call.
    TurnUnfinished,
}

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
    Reset(ContextRef),
    RevisionConflict { expected: u64, actual: u64 },
    AvailabilityConflict { expected: u64, actual: u64 },
    PositionConflict { expected: u64, actual: u64 },
    ConversationCacheStale,
    ConversationCacheReplaced,
    ForkTooDeep,
    UnknownSession(String),
    TypeConflict(String),
    Turn(TurnError),
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
            Self::Reset(r) => write!(f, "context record belongs to a reset module era: {}", r.id),
            Self::Unavailable(r) => write!(f, "context record or its source is unavailable: {}", r.id),
            Self::RevisionConflict { expected, actual } => write!(f, "context revision conflict: expected {expected}, actual {actual}"),
            Self::AvailabilityConflict { expected, actual } => write!(f, "context availability conflict: expected {expected}, actual {actual}"),
            Self::PositionConflict { expected, actual } => write!(f, "session log position conflict: expected {expected}, actual {actual}"),
            Self::Turn(rule) => write!(f, "session turn refused the event: {rule:?}"),
            Self::ConversationCacheStale => write!(f, "conversation cache is stale"),
            Self::ConversationCacheReplaced => write!(f, "conversation cache was replaced or removed"),
            Self::ForkTooDeep => write!(f, "session fork chain exceeds 64 sessions"),
            Self::UnknownSession(name) => write!(f, "unknown session: {name}"),
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
