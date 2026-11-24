//! Harness-independent context on the native memory transaction kernel.
//!
//! Open a fresh context format with [`MemoryDatabase::create_context`], then
//! obtain an owner-scoped handle with [`MemoryDatabase::context`]. The handle
//! shares the native lease, write lock, keyspace and synchronous batch commit.

mod actions;
mod catalog;
mod checkpoints;
mod history;
mod records;
mod search;
pub(crate) mod storage;
mod types;

#[cfg(test)]
mod tests;

pub use types::*;

use super::MemoryDatabase;
use crate::records::InvalidTemporalFacts;
use crate::{OperationId, MAX_ACCESS_SCOPES};
use mmdb_kernel::{EraPartitions, KernelError, KernelResult, Partition, WriteGuard};
use std::fmt;

pub const CONTEXT_STORE_FORMAT_ID: &str = "mmdb-context-dev7";
pub const PAYLOAD_CHUNK_BYTES: usize = 32 * 1024;
pub const MAX_PAYLOAD_PAGE_BYTES: usize = 1024 * 1024;
pub const MAX_RECORD_BYTES: usize = 128 * 1024;
pub const MAX_CONTEXT_NAME_BYTES: usize = 256;
pub const MAX_CONTEXT_SOURCES: usize = 64;

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
    HistoryConflict { expected: u64, actual: u64 },
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
            Self::UnsupportedFormat => write!(f, "context operations require a fresh {CONTEXT_STORE_FORMAT_ID} store; reset rather than migrate legacy data"),
            Self::AccessDenied => write!(f, "context reference is outside the allowed owner or scope"),
            Self::NotFound(r) => write!(f, "context record not found: {}", r.id),
            Self::Unavailable(r) => write!(f, "context record or its source is unavailable: {}", r.id),
            Self::RevisionConflict { expected, actual } => write!(f, "context revision conflict: expected {expected}, actual {actual}"),
            Self::AvailabilityConflict { expected, actual } => write!(f, "context availability conflict: expected {expected}, actual {actual}"),
            Self::HistoryConflict { expected, actual } => write!(f, "history position conflict: expected {expected}, actual {actual}"),
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

// The kernel schema and the handles must describe the same partitions.
macro_rules! context_partitions {
    ($($field:ident => $name:literal),+ $(,)?) => {
        pub(super) const PARTITION_NAMES: &[&str] = &[$($name),+];

        pub(crate) struct ContextPartitions {
            $(pub(crate) $field: Partition,)+
            #[cfg(test)]
            pub(crate) fail_commit_ack: std::sync::atomic::AtomicBool,
        }

        impl ContextPartitions {
            pub(super) fn open(era: &EraPartitions) -> KernelResult<Self> {
                Ok(Self {
                    $($field: era.partition($name)?,)+
                    #[cfg(test)]
                    fail_commit_ack: std::sync::atomic::AtomicBool::new(false),
                })
            }
        }
    };
}

context_partitions! {
    headers => "context_headers_v1",
    heads => "context_heads_v1",
    payloads => "context_payloads_v1",
    names => "context_type_names_v1",
    action_names => "context_action_names_v1",
    operations => "context_operations_v1",
    history_order => "context_history_order_v1",
    history_heads => "context_history_heads_v1",
    events => "context_history_events_v1",
    calls => "context_tool_calls_v1",
    results => "context_tool_results_v1",
    postings => "context_postings_v1",
    record_terms => "context_record_terms_v1",
    adjacency => "context_adjacency_v1",
    pending => "context_pending_uploads_v1",
    messages => "context_messages_v1",
    checkpoints => "context_checkpoints_v1",
    lifecycle => "context_lifecycle_v1",
    entity_identity => "context_entity_identity_v1",
    extraction_receipts => "context_extraction_receipts_v1",
    extraction_incomplete => "context_extraction_incomplete_v1",
}

pub struct ContextStore<'a> {
    pub(crate) db: &'a MemoryDatabase,
    pub(crate) parts: &'a ContextPartitions,
    pub(crate) access: ContextAccess,
}

/// Keeps validated source heads and availability stable until dropped.
///
/// This guard holds the context database's write lock. Only perform bounded,
/// synchronous work while holding it; do not call back into this database,
/// await, or run external effects. When coordinating with a separate state
/// store, always acquire this guard before that store's write lock.
#[must_use = "source validity is protected only while the guard is held"]
pub struct CurrentSourcesGuard<'a> {
    _lock: WriteGuard<'a>,
    availability_epoch: u64,
}

impl CurrentSourcesGuard<'_> {
    pub fn availability_epoch(&self) -> u64 {
        self.availability_epoch
    }
}

impl MemoryDatabase {
    pub fn context(&self, access: ContextAccess) -> ContextResult<ContextStore<'_>> {
        storage::validate_name(&access.owner, "owner")?;
        for value in [
            &access.agent,
            &access.session,
            &access.run,
            &access.parent_run,
        ]
        .into_iter()
        .flatten()
        {
            storage::validate_name(value, "provenance identity")?;
        }
        if access.parent_run.is_some() && (access.run.is_none() || access.parent_run == access.run)
        {
            return Err(ContextError::InvalidInput(
                "a child run requires a distinct parent identity".into(),
            ));
        }
        if access.scopes.is_empty() || access.scopes.len() > MAX_ACCESS_SCOPES {
            return Err(ContextError::InvalidInput(
                "provide 1..=32 allowed scopes".into(),
            ));
        }
        Ok(ContextStore {
            db: self,
            parts: self
                .context
                .as_ref()
                .ok_or(ContextError::UnsupportedFormat)?,
            access,
        })
    }
}
