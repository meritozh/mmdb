use super::super::{Actor, EraId, OperationId, RecordState, Scope, TemporalFacts};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;
use ulid::Ulid;

pub type Properties = BTreeMap<String, Value>;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RecordKind {
    Type,
    Object,
    Relation,
    History,
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum PropertyType {
    Text,
    Boolean,
    Integer,
    Number,
    Timestamp,
    Reference { target_type: Option<ContextRef> },
    List { element: Box<PropertyType> },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PropertyDefinition {
    pub value_type: PropertyType,
    pub required: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum TypeKind {
    Object,
    Relation {
        from_types: Vec<ContextRef>,
        to_types: Vec<ContextRef>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TypeDefinition {
    pub name: String,
    pub scope: Scope,
    pub kind: TypeKind,
    pub properties: BTreeMap<String, PropertyDefinition>,
}

impl TypeDefinition {
    /// An optional starting schema, registered and versioned through `define_type`
    /// exactly like application-defined types. Creating a handle does not write it.
    pub fn note(scope: Scope) -> Self {
        Self {
            name: "mmdb.note".into(),
            scope,
            kind: TypeKind::Object,
            properties: BTreeMap::from([
                (
                    "title".into(),
                    PropertyDefinition {
                        value_type: PropertyType::Text,
                        required: false,
                    },
                ),
                (
                    "text".into(),
                    PropertyDefinition {
                        value_type: PropertyType::Text,
                        required: true,
                    },
                ),
            ]),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InformationKind {
    Observation,
    Inference,
    Instruction,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ObjectInput {
    pub type_pin: RecordPin,
    pub scope: Scope,
    pub properties: Properties,
    pub sources: Vec<RecordPin>,
    pub information: InformationKind,
    pub temporal: TemporalFacts,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RelationInput {
    pub type_pin: RecordPin,
    pub scope: Scope,
    pub from: ContextRef,
    pub to: ContextRef,
    pub properties: Properties,
    pub sources: Vec<RecordPin>,
    pub information: InformationKind,
    pub temporal: TemporalFacts,
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
pub enum HistoryKind {
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
pub struct MessageState {
    pub next_fragment: u64,
    pub terminal: Option<MessageStatus>,
    /// A withdrawn fragment also invalidates aggregate copies of the message.
    #[serde(default)]
    pub unavailable: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HistoryInput {
    /// Availability dependencies for material copied into this original (for
    /// example, a model request containing recalled records).
    #[serde(default)]
    pub sources: Vec<RecordPin>,
    pub session: String,
    pub event_id: String,
    pub scope: Scope,
    pub kind: HistoryKind,
    pub media_type: String,
    pub occurred_at_ms: i64,
    /// Last committed sequence; zero denotes an empty session. None appends
    /// after the current head while still serializing sequence allocation.
    pub expected_sequence: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HistoryEntry {
    pub session: String,
    pub event_id: String,
    pub sequence: u64,
    pub kind: HistoryKind,
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
    pub provenance: Provenance,
    pub information: InformationKind,
    pub temporal: TemporalFacts,
    pub recorded_at_ms: i64,
    pub payload: PayloadDescriptor,
    pub endpoints: Option<Endpoints>,
    pub history: Option<HistoryEntry>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub checkpoint: Option<CheckpointMetadata>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
pub enum RecordBody {
    Type(TypeDefinition),
    Object(Properties),
    Relation(Properties),
    /// Raw history bytes are read separately with a bounded payload request.
    History(HistoryEntry),
    Checkpoint(Value),
    Action(ActionDefinition),
    ActionExecution(ActionExecution),
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
    pub history_sequence: u64,
    pub previous: Option<RecordPin>,
    pub availability_epoch: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CheckpointInput {
    #[serde(default)]
    pub scope: CheckpointScope,
    pub session: String,
    pub run: String,
    pub history_sequence: u64,
    pub expected_previous: Option<RecordPin>,
    /// Epoch captured before assembling/generating the summary.
    pub expected_availability_epoch: u64,
    pub sources: Vec<RecordPin>,
    pub payload: Value,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ContextRecord {
    pub header: RecordHeader,
    pub current_state: RecordState,
    pub body: RecordBody,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WriteReceipt {
    pub pin: RecordPin,
    pub history_sequence: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PayloadPage {
    pub bytes: Vec<u8>,
    pub next_offset: Option<u64>,
    pub total_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TypePage {
    pub definitions: Vec<(RecordPin, TypeDefinition)>,
    pub next: Option<ContextRef>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HistoryPage {
    pub entries: Vec<RecordHeader>,
    pub next_sequence: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RecallBudget {
    pub max_candidates: usize,
    pub max_edges: usize,
    pub max_depth: usize,
    pub max_results: usize,
}

impl Default for RecallBudget {
    fn default() -> Self {
        Self {
            max_candidates: 4096,
            max_edges: 512,
            max_depth: 2,
            max_results: 12,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContextQuery {
    pub text: String,
    pub seeds: Vec<ContextRef>,
    pub valid_at_ms: Option<i64>,
    pub budget: RecallBudget,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ContextHit {
    pub record: ContextRecord,
    pub matched_terms: usize,
    pub path: Vec<RecordPin>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ContextRecall {
    pub hits: Vec<ContextHit>,
    pub candidates_examined: usize,
    pub edges_examined: usize,
    pub truncated: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RelationDirection {
    Incoming,
    Outgoing,
    Both,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RelationCursor {
    pub(super) query_digest: [u8; 32],
    pub(super) key: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RelationQuery {
    pub endpoint: ContextRef,
    pub direction: RelationDirection,
    pub type_filter: Option<ContextRef>,
    pub valid_at_ms: Option<i64>,
    pub cursor: Option<RelationCursor>,
    pub max_edges: usize,
    pub limit: usize,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RelationPage {
    pub relations: Vec<ContextRecord>,
    pub cursor: Option<RelationCursor>,
    pub edges_examined: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HistorySearchCursor {
    pub(super) query_digest: [u8; 32],
    pub(super) key: Vec<u8>,
    pub(super) offset: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HistoryQuery {
    pub text: String,
    pub session: Option<String>,
    pub cursor: Option<HistorySearchCursor>,
    pub max_events: usize,
    pub max_bytes: usize,
    pub limit: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HistoryMatch {
    pub header: RecordHeader,
    pub byte_offset: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HistorySearch {
    pub matches: Vec<HistoryMatch>,
    pub cursor: Option<HistorySearchCursor>,
    pub bytes_examined: usize,
    pub events_examined: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RecoveryReport {
    pub chunks_removed: usize,
    pub uploads_removed: usize,
    pub more: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) struct StoredOperation {
    pub digest: [u8; 32],
    pub receipt: WriteReceipt,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) struct StagedUpload {
    pub pin: RecordPin,
    pub operation: OperationId,
    pub purge: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) struct StoredEvent {
    pub digest: [u8; 32],
    pub receipt: WriteReceipt,
}

/// Business semantics only. The harness owns tool bindings and authorization.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ActionDefinition {
    pub name: String,
    pub description: String,
    pub scope: Scope,
    pub target_type: RecordPin,
    pub inputs: BTreeMap<String, PropertyDefinition>,
    pub preconditions: Vec<ActionCondition>,
    pub sources: Vec<RecordPin>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ActionCondition {
    Equals { property: String, value: Value },
    Exists { property: String },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ActionOffer {
    pub pin: RecordPin,
    pub definition: ActionDefinition,
    pub preconditions_met: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ActionPage {
    pub actions: Vec<ActionOffer>,
    pub next: Option<ContextRef>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ActionInvocation {
    pub action: RecordPin,
    pub target: RecordPin,
    pub inputs: Properties,
    pub sources: Vec<RecordPin>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ActionStatus {
    Prepared,
    Succeeded,
    RejectedBeforeDispatch,
    FailedNoEffect,
    EffectUnknown,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ActionExecution {
    pub invocation: ActionInvocation,
    pub status: ActionStatus,
    pub result: Value,
    pub result_sources: Vec<RecordPin>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ActionPreparation {
    pub receipt: WriteReceipt,
    pub already_present: bool,
}

/// Owner-scoped session catalog for trusted harness recovery.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HistorySessionPage {
    pub sessions: Vec<String>,
    pub next: Option<String>,
}
