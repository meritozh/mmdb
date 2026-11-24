use super::{Actor, RecordState, Scope, TemporalFacts};
use crate::{EraId, OperationId};
use serde::{Deserialize, Serialize};
use ulid::Ulid;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RecordKind {
    Type,
    Object,
    Relation,
    Event,
    Checkpoint,
    Action,
    ActionExecution,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct ContextRef {
    pub era: EraId,
    pub owner: String,
    pub kind: RecordKind,
    pub id: Ulid,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct RecordPin {
    pub record: ContextRef,
    pub revision: u64,
}

/// Supplied by the embedding harness, never taken from model-authored content.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContextAccess {
    pub owner: String,
    pub actor: Actor,
    pub agent: Option<String>,
    pub session: Option<String>,
    pub run: Option<String>,
    #[serde(default)]
    pub parent_run: Option<String>,
    pub scopes: Vec<Scope>,
}

impl ContextAccess {
    pub fn new(owner: impl Into<String>, actor: Actor) -> Self {
        Self {
            owner: owner.into(),
            actor,
            agent: None,
            session: None,
            run: None,
            parent_run: None,
            scopes: vec![Scope::Personal],
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Provenance {
    pub actor: Actor,
    pub agent: Option<String>,
    pub session: Option<String>,
    pub run: Option<String>,
    #[serde(default)]
    pub parent_run: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InformationKind {
    Observation,
    Inference,
    Instruction,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ToolOutcome {
    Succeeded,
    RejectedBeforeDispatch,
    FailedNoEffect,
    EffectUnknown,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum EventKind {
    UserMessage,
    AssistantMessage,
    AssistantStarted {
        message_id: String,
    },
    AssistantFragment {
        message_id: String,
        ordinal: u64,
    },
    AssistantTerminal {
        message_id: String,
        status: MessageStatus,
    },
    ToolCall {
        call_id: String,
        tool_name: String,
        /// The assistant aggregate containing this call's arguments, if any.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        message_id: Option<String>,
    },
    ToolResult {
        call_id: String,
        outcome: ToolOutcome,
    },
    Notice,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MessageStatus {
    Completed,
    Failed,
    Interrupted,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LogEvent {
    pub session: String,
    pub event_id: String,
    pub sequence: u64,
    pub kind: EventKind,
    pub media_type: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Endpoints {
    pub from: ContextRef,
    pub to: ContextRef,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PayloadDescriptor {
    pub revision: u64,
    pub bytes: u64,
    pub digest: [u8; 32],
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RecordHeader {
    pub pin: RecordPin,
    pub scope: Scope,
    pub state: RecordState,
    pub type_pin: Option<RecordPin>,
    pub sources: Vec<RecordPin>,
    /// Additional scopes required to read the session snapshot.
    pub source_scopes: Vec<Scope>,
    pub provenance: Provenance,
    pub information: InformationKind,
    pub temporal: TemporalFacts,
    pub recorded_at_ms: i64,
    pub payload: PayloadDescriptor,
    pub endpoints: Option<Endpoints>,
    pub event: Option<LogEvent>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub checkpoint: Option<CheckpointMetadata>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CheckpointScope {
    #[default]
    Session,
    Run,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CheckpointMetadata {
    #[serde(default)]
    pub scope: CheckpointScope,
    pub session: String,
    pub run: String,
    pub window: u64,
    pub sequence: u64,
    pub previous: Option<RecordPin>,
    pub availability_epoch: u64,
}

impl RecordHeader {
    /// Whether this record is currently "live" for recall at `at_ms`: it must be
    /// in the [`RecordState::Active`] lifecycle state **and** its
    /// [`TemporalFacts::is_valid_at`] window must cover `at_ms`. This mirrors
    /// `MemoryNode::is_valid_at` at the working-context layer.
    pub fn is_valid_at(&self, at_ms: i64) -> bool {
        self.state == RecordState::Active && self.temporal.is_valid_at(at_ms)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WriteReceipt {
    pub pin: RecordPin,
    pub sequence: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PayloadPage {
    pub bytes: Vec<u8>,
    pub next_offset: Option<u64>,
    pub total_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RecoveryReport {
    pub chunks_removed: usize,
    pub uploads_removed: usize,
    pub more: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct StoredOperation {
    pub digest: [u8; 32],
    pub receipt: WriteReceipt,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) struct StagedUpload {
    pub pin: RecordPin,
    pub operation: OperationId,
    pub purge: bool,
}
