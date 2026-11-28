//! Continuous, owner-scoped session events and recovery checkpoints.

mod caches;
pub use caches::{ConversationCache, ConversationCacheInput, ConversationCachePage};
mod catalog;
pub use catalog::{NewSession, SessionPage, SessionQuery, SessionState};
mod checkpoints;
mod coverage;
mod fork;
mod history;
mod search;
mod turns;
mod types;
mod view;

use crate::records::codec::{owner_key, segment};
pub use crate::records::RecoveryReport;
use crate::records::*;
use crate::OperationId;
pub(crate) use history::{end_interrupted_turns, LogGate};
pub use types::*;

const CALLS: &str = "session_tool_calls_v1";
const MESSAGES: &str = "session_messages_v1";
module_partitions!(SessionParts, SESSION_PARTITIONS, {
    caches => "session_caches_v1",
    cache_chunks => "session_cache_chunks_v1"; LargeValues,
    cache_pending => "session_cache_pending_v1",
    source_scopes => "session_source_scopes_v1",
    withdrawals => "session_withdrawals_v1",
    lineage => "session_lineage_v1",
    order => "session_order_v1",
    changes => "session_changes_v1",
    heads => "session_heads_v1",
    open_turns => "session_open_turns_v1",
    owed => "session_owed_v1",
    events => "session_events_v1",
    calls => CALLS,
    results => "session_tool_results_v1",
    messages => MESSAGES,
    checkpoints => "session_checkpoints_v1",
});

/// Read and append session events and checkpoints under one trusted access profile.
pub struct SessionLog<'a> {
    pub(crate) scoped: Scoped<'a>,
    pub(crate) parts: &'a SessionParts,
}

pub(crate) fn session_key(owner: &str, session: &str) -> Vec<u8> {
    let mut key = owner_key(owner);
    segment(&mut key, session.as_bytes());
    key
}

impl SessionLog<'_> {
    pub fn head(&self, record: &ContextRef) -> ContextResult<RecordHeader> {
        record.kind.require_module(StoreModule::Session)?;
        let _guard = self.scoped.lock();
        self.scoped.head_locked(record)
    }

    pub fn inspect_header(&self, pin: &RecordPin) -> ContextResult<RecordHeader> {
        pin.record.kind.require_module(StoreModule::Session)?;
        let _guard = self.scoped.lock();
        self.scoped.header_locked(pin)
    }

    pub fn read_payload(
        &self,
        pin: &RecordPin,
        offset: u64,
        limit: usize,
    ) -> ContextResult<PayloadPage> {
        pin.record.kind.require_module(StoreModule::Session)?;
        let _guard = self.scoped.lock();
        let header = self.scoped.readable_header_locked(pin)?;
        self.scoped.payload_slice_locked(&header, offset, limit)
    }

    pub fn operation_receipt(
        &self,
        operation: crate::OperationId,
    ) -> ContextResult<Option<WriteReceipt>> {
        self.scoped.operation_receipt(operation)
    }

    pub fn retract(
        &self,
        operation: OperationId,
        expected: RecordPin,
    ) -> ContextResult<WriteReceipt> {
        self.change_state(operation, expected, RecordState::Retracted)
    }

    /// Hides this record and dependent memory records immediately.
    /// Physical chunk cleanup is bounded and resumable with `recover_payloads`.
    pub fn purge(
        &self,
        operation: OperationId,
        expected: RecordPin,
    ) -> ContextResult<WriteReceipt> {
        self.change_state(operation, expected, RecordState::Purged)
    }

    fn change_state(
        &self,
        operation: OperationId,
        expected: RecordPin,
        state: RecordState,
    ) -> ContextResult<WriteReceipt> {
        expected.record.kind.require_module(StoreModule::Session)?;
        if expected.record.kind == RecordKind::Session {
            return Err(ContextError::InvalidInput(
                "session headers are permanent".into(),
            ));
        }
        let redacts = state == RecordState::Purged || expected.record.kind == RecordKind::Event;
        if redacts && !matches!(self.scoped.access().actor, Actor::User | Actor::Operator) {
            return Err(ContextError::AccessDenied);
        }
        self.scoped.commit_state_change(
            operation,
            expected,
            state,
            redacts,
            |batch, _, header, epoch| {
                if let Some(event) = &header.event {
                    self.withdraw_event(batch, event, epoch)?;
                    let mut head = self.session_head(&event.session)?;
                    catalog::Changes::default().put_head(
                        self,
                        batch,
                        &event.session,
                        &mut head,
                        None,
                    )?;
                }
                Ok(())
            },
        )
    }

    /// Cleans abandoned uploads and purged payloads, never live committed data.
    /// The chunk and upload counts bound work even for very large originals.
    pub fn recover_payloads(
        &self,
        max_chunks: usize,
        max_uploads: usize,
    ) -> ContextResult<RecoveryReport> {
        self.scoped
            .recover_payloads(StoreModule::Session, max_chunks, max_uploads)
    }
}
