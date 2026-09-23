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

use super::{MemoryDatabase, MemoryError, MemoryResult, OperationId};
use fjall::{Keyspace, PartitionHandle};
use std::fmt;

pub const CONTEXT_STORE_FORMAT_ID: &str = "mmdb-context-v1";
pub const PAYLOAD_CHUNK_BYTES: usize = 32 * 1024;
pub const MAX_PAYLOAD_PAGE_BYTES: usize = 1024 * 1024;
pub const MAX_RECORD_BYTES: usize = 128 * 1024;
pub const MAX_CONTEXT_NAME_BYTES: usize = 256;
pub const MAX_CONTEXT_SOURCES: usize = 64;

#[derive(Debug)]
pub enum ContextError {
    Native(MemoryError),
    Io(std::io::Error),
    Serialization(serde_json::Error),
    InvalidInput(String),
    Corrupt(String),
    UnsupportedFormat,
    AccessDenied,
    NotFound(ContextRef),
    Unavailable(ContextRef),
    RevisionConflict { expected: u64, actual: u64 },
    HistoryConflict { expected: u64, actual: u64 },
    TypeConflict(String),
    IdempotencyConflict,
    CommitUnknown(OperationId),
    BudgetExceeded,
}

impl fmt::Display for ContextError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Native(e) => write!(f, "{e}"),
            Self::Io(e) => write!(f, "context input I/O: {e}"),
            Self::Serialization(e) => write!(f, "context serialization: {e}"),
            Self::InvalidInput(e) => write!(f, "invalid context input: {e}"),
            Self::Corrupt(e) => write!(f, "corrupt context: {e}"),
            Self::UnsupportedFormat => write!(f, "context operations require a fresh {CONTEXT_STORE_FORMAT_ID} store; reset rather than migrate legacy data"),
            Self::AccessDenied => write!(f, "context reference is outside the allowed owner or scope"),
            Self::NotFound(r) => write!(f, "context record not found: {}", r.id),
            Self::Unavailable(r) => write!(f, "context record or its source is unavailable: {}", r.id),
            Self::RevisionConflict { expected, actual } => write!(f, "context revision conflict: expected {expected}, actual {actual}"),
            Self::HistoryConflict { expected, actual } => write!(f, "history position conflict: expected {expected}, actual {actual}"),
            Self::TypeConflict(name) => write!(f, "context type already has a different definition: {name}"),
            Self::IdempotencyConflict => write!(f, "operation or event identity was reused with different content"),
            Self::CommitUnknown(id) => write!(f, "context commit outcome unknown for operation {id}; inspect its receipt or retry the same operation"),
            Self::BudgetExceeded => write!(f, "context validation or query budget exceeded"),
        }
    }
}

impl std::error::Error for ContextError {}
impl From<MemoryError> for ContextError {
    fn from(e: MemoryError) -> Self {
        Self::Native(e)
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
impl From<fjall::Error> for ContextError {
    fn from(e: fjall::Error) -> Self {
        Self::Native(super::storage_error(e))
    }
}

pub type ContextResult<T> = Result<T, ContextError>;

pub(crate) struct ContextPartitions {
    pub(crate) headers: PartitionHandle,
    pub(crate) heads: PartitionHandle,
    pub(crate) payloads: PartitionHandle,
    pub(crate) names: PartitionHandle,
    pub(crate) action_names: PartitionHandle,
    pub(crate) operations: PartitionHandle,
    pub(crate) history_order: PartitionHandle,
    pub(crate) history_heads: PartitionHandle,
    pub(crate) events: PartitionHandle,
    pub(crate) calls: PartitionHandle,
    pub(crate) results: PartitionHandle,
    pub(crate) postings: PartitionHandle,
    pub(crate) record_terms: PartitionHandle,
    pub(crate) adjacency: PartitionHandle,
    pub(crate) pending: PartitionHandle,
    pub(crate) messages: PartitionHandle,
    pub(crate) checkpoints: PartitionHandle,
    pub(crate) lifecycle: PartitionHandle,
    /// Entity identity MVP (A2): secondary index mapping the globally-unique
    /// `(namespace, entity_type, external_key)` triple (and its aliases) to a
    /// context record id. Mirrors the `names` / `action_names` secondary
    /// indexes but is dedicated to entity identity resolution.
    pub(crate) entity_identity: PartitionHandle,
    /// Extraction transactions (A3): persisted `ExtractionReceipt` keyed by the
    /// caller-supplied operation id, plus an `Incomplete` crash-marker set
    /// before the durable batch commits.
    pub(crate) extraction_receipts: PartitionHandle,
    pub(crate) extraction_incomplete: PartitionHandle,
    #[cfg(test)]
    pub(crate) fail_commit_ack: std::sync::atomic::AtomicBool,
}

impl ContextPartitions {
    pub(super) fn open(keyspace: &Keyspace) -> MemoryResult<Self> {
        let p = |name| super::open_partition(keyspace, name);
        Ok(Self {
            headers: p("context_headers_v1")?,
            heads: p("context_heads_v1")?,
            payloads: p("context_payloads_v1")?,
            names: p("context_type_names_v1")?,
            action_names: p("context_action_names_v1")?,
            operations: p("context_operations_v1")?,
            history_order: p("context_history_order_v1")?,
            history_heads: p("context_history_heads_v1")?,
            events: p("context_history_events_v1")?,
            calls: p("context_tool_calls_v1")?,
            results: p("context_tool_results_v1")?,
            postings: p("context_postings_v1")?,
            record_terms: p("context_record_terms_v1")?,
            adjacency: p("context_adjacency_v1")?,
            pending: p("context_pending_uploads_v1")?,
            messages: p("context_messages_v1")?,
            checkpoints: p("context_checkpoints_v1")?,
            lifecycle: p("context_lifecycle_v1")?,
            entity_identity: p("context_entity_identity_v1")?,
            extraction_receipts: p("context_extraction_receipts_v1")?,
            extraction_incomplete: p("context_extraction_incomplete_v1")?,
            #[cfg(test)]
            fail_commit_ack: std::sync::atomic::AtomicBool::new(false),
        })
    }
}

pub struct ContextStore<'a> {
    pub(crate) db: &'a MemoryDatabase,
    pub(crate) parts: &'a ContextPartitions,
    pub(crate) access: ContextAccess,
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
        if access.scopes.is_empty() || access.scopes.len() > super::MAX_RECALL_SCOPES {
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
