//! Harness-independent context backed by the store's session and memory modules.
//!
//! Use [`crate::Store::context`] to obtain an owner-scoped handle.

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

use crate::records::Scoped;
pub use crate::records::{
    CheckpointMetadata, CheckpointScope, ContextAccess, ContextError, ContextRef, ContextResult,
    Endpoints, HistoryEntry, HistoryKind, InformationKind, MessageStatus, PayloadDescriptor,
    PayloadPage, Provenance, RecordHeader, RecordKind, RecordPin, RecoveryReport, StoreModule,
    ToolOutcome, WriteReceipt, MAX_CONTEXT_NAME_BYTES, MAX_CONTEXT_SOURCES, MAX_PAYLOAD_PAGE_BYTES,
    MAX_RECORD_BYTES, PAYLOAD_CHUNK_BYTES,
};
use mmdb_kernel::{EraPartitions, KernelResult, Partition, WriteGuard};

// Each module's partition declaration supplies both its schema and its handles.
macro_rules! context_partitions {
    ($( $era:ident => $schema:ident { $($field:ident => $name:literal),+ $(,)? } )+) => {
        $(pub(crate) const $schema: &[&str] = &[$($name),+];)+

        pub(crate) struct ContextPartitions {
            $($(pub(crate) $field: Partition,)+)+
        }

        impl ContextPartitions {
            pub(crate) fn open($($era: &EraPartitions),+) -> KernelResult<Self> {
                Ok(Self {
                    $($($field: $era.partition($name)?,)+)+
                })
            }
        }
    };
}

context_partitions! {
    session => SESSION_PARTITIONS {
        history_order => "session_order_v1",
        history_heads => "session_heads_v1",
        events => "session_events_v1",
        calls => "session_tool_calls_v1",
        results => "session_tool_results_v1",
        messages => "session_messages_v1",
        checkpoints => "session_checkpoints_v1",
    }
    memory => MEMORY_PARTITIONS {
        names => "context_type_names_v1",
        action_names => "context_action_names_v1",
        postings => "context_postings_v1",
        record_terms => "context_record_terms_v1",
        adjacency => "context_adjacency_v1",
        entity_identity => "context_entity_identity_v1",
        extraction_receipts => "context_extraction_receipts_v1",
        extraction_incomplete => "context_extraction_incomplete_v1",
    }
}

pub struct ContextStore<'a> {
    pub(crate) scoped: Scoped<'a>,
    pub(crate) parts: &'a ContextPartitions,
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
