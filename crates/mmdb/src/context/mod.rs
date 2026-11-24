//! Domain types, objects, relations and actions in the store's memory module.
//!
//! Use [`crate::Store::context`] to obtain an owner-scoped handle.

mod actions;
mod catalog;
mod records;
mod search;
pub(crate) mod storage;
mod types;

#[cfg(test)]
mod tests;

pub use types::*;

use crate::records::Scoped;
pub use crate::records::{
    CheckpointMetadata, CheckpointScope, ContextAccess, ContextError, ContextRef, ContextResult,
    Endpoints, EventKind, InformationKind, LogEvent, MessageStatus, PayloadDescriptor, PayloadPage,
    Provenance, RecordHeader, RecordKind, RecordPin, RecoveryReport, StoreModule, ToolOutcome,
    WriteReceipt, MAX_CONTEXT_NAME_BYTES, MAX_CONTEXT_SOURCES, MAX_PAYLOAD_PAGE_BYTES,
    MAX_RECORD_BYTES, PAYLOAD_CHUNK_BYTES,
};
use mmdb_kernel::WriteGuard;

module_partitions!(ContextParts, MEMORY_PARTITIONS, {
    names => "context_type_names_v1",
    action_names => "context_action_names_v1",
    postings => "context_postings_v1",
    record_terms => "context_record_terms_v1",
    adjacency => "context_adjacency_v1",
    entity_identity => "context_entity_identity_v1",
    extraction_receipts => "context_extraction_receipts_v1",
    extraction_incomplete => "context_extraction_incomplete_v1",
});

pub struct ContextStore<'a> {
    pub(crate) scoped: Scoped<'a>,
    pub(crate) parts: &'a ContextParts,
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
