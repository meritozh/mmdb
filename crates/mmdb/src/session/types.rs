use crate::records::*;
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MessageState {
    pub next_fragment: u64,
    pub terminal: Option<MessageStatus>,
    /// A withdrawn fragment also invalidates aggregate copies of the message.
    #[serde(default)]
    pub unavailable: bool,
    /// Session-wide step number of the opening AssistantStarted event.
    pub step: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EventInput {
    /// Availability dependencies for material copied into this original (for
    /// example, a model request containing recalled records).
    #[serde(default)]
    pub sources: Vec<RecordPin>,
    pub session: String,
    pub event_id: String,
    pub scope: Scope,
    pub kind: EventKind,
    pub media_type: String,
    pub occurred_at_ms: i64,
    /// Last committed sequence; zero denotes an empty session. None appends
    /// after the current head while still serializing sequence allocation.
    pub expected_sequence: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CheckpointInput {
    #[serde(default)]
    pub scope: CheckpointScope,
    pub session: String,
    pub run: String,
    pub sequence: u64,
    pub expected_previous: Option<RecordPin>,
    /// Epoch captured before assembling/generating the summary.
    pub expected_availability_epoch: u64,
    pub sources: Vec<RecordPin>,
    pub payload: Value,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EventPage {
    pub entries: Vec<RecordHeader>,
    pub next_sequence: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LogSearchCursor {
    pub(super) query_digest: [u8; 32],
    pub(super) key: Vec<u8>,
    pub(super) offset: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LogQuery {
    pub text: String,
    pub session: Option<String>,
    pub cursor: Option<LogSearchCursor>,
    pub max_events: usize,
    pub max_bytes: usize,
    pub limit: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LogMatch {
    pub header: RecordHeader,
    pub byte_offset: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LogSearch {
    pub matches: Vec<LogMatch>,
    pub cursor: Option<LogSearchCursor>,
    pub bytes_examined: usize,
    pub events_examined: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) struct StoredEvent {
    pub digest: [u8; 32],
    pub receipt: WriteReceipt,
}

/// Owner-scoped session catalog for trusted harness recovery.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionPage {
    pub sessions: Vec<String>,
    pub next: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CheckpointRecord {
    pub header: RecordHeader,
    pub payload: Value,
}
