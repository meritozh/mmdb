//! Clean-slate, evidence-first memory storage.
//!
//! This module deliberately does not share records or persisted formats with the
//! legacy `MemoryNode` database.  The manifest is checked before fjall is opened,
//! so an old or otherwise unrecognised root is never adopted accidentally.

use crate::store_format::{
    require_managed_store, StoreEraId, StoreFormatError, StoreLease,
    StoreManifest as OuterStoreManifest,
};
use fjall::{Config, Keyspace, PartitionCreateOptions, PartitionHandle, PersistMode};
use parking_lot::Mutex;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::fmt;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};
use ulid::Ulid;

/// Exact outer format recognized by both native opening and reset planning.
pub const MEMORY_STORE_FORMAT_ID: &str = "mmdb-native-memory-v1";

const PART_NATIVE_METADATA: &str = "native_internal_metadata_v1";
const NATIVE_METADATA_KEY: &[u8] = b"native-memory-metadata";
const PART_EVIDENCE: &str = "native_evidence_headers_v1";
const PART_EVIDENCE_AVAILABILITY: &str = "native_evidence_availability_v1";
const PART_EVIDENCE_HEADS: &str = "native_evidence_heads_v1";
const PART_CLAIMS: &str = "native_claim_revisions_v1";
const PART_CLAIM_HEADS: &str = "native_claim_heads_v1";
const PART_RELATIONS: &str = "native_relation_revisions_v1";
const PART_RELATION_HEADS: &str = "native_relation_heads_v1";
const PART_ENTITIES: &str = "native_entity_revisions_v1";
const PART_ENTITY_HEADS: &str = "native_entity_heads_v1";
const PART_PROPOSALS: &str = "native_proposal_revisions_v1";
const PART_PROPOSAL_HEADS: &str = "native_proposal_heads_v1";
const PART_PROPOSAL_SOURCES: &str = "native_proposal_sources_v1";
const PART_PENDING_PROPOSALS: &str = "native_pending_proposals_v1";
const PART_AWAITING_ADJUDICATION: &str = "native_awaiting_adjudication_v1";
const PART_PROPOSAL_REVIEWS: &str = "native_proposal_review_revisions_v1";
const PART_PROPOSAL_REVIEW_HEADS: &str = "native_proposal_review_heads_v1";
const PART_LATEST_PROPOSAL_REVIEW: &str = "native_latest_proposal_review_v1";
const PART_RECALL_FEEDBACK: &str = "native_recall_feedback_v1";
const PART_ARTIFACT_COLLECTIONS: &str = "native_artifact_collection_headers_v1";
const PART_ARTIFACT_COLLECTION_AVAILABILITY: &str = "native_artifact_collection_availability_v1";
const PART_ARTIFACT_COLLECTION_HEADS: &str = "native_artifact_collection_heads_v1";
const PART_ARTIFACT_SNAPSHOTS: &str = "native_artifact_snapshot_headers_v1";
const PART_ARTIFACT_SNAPSHOT_AVAILABILITY: &str = "native_artifact_snapshot_availability_v1";
const PART_ARTIFACT_SNAPSHOT_HEADS: &str = "native_artifact_snapshot_heads_v1";
const PART_ARTIFACT_SNAPSHOT_BLOBS: &str = "native_artifact_snapshot_blobs_v1";
const PART_ARTIFACT_PASSAGES: &str = "native_artifact_passage_headers_v1";
const PART_ARTIFACT_PASSAGE_AVAILABILITY: &str = "native_artifact_passage_availability_v1";
const PART_ARTIFACT_PASSAGE_HEADS: &str = "native_artifact_passage_heads_v1";
const PART_ARTIFACT_PASSAGE_ORDINALS: &str = "native_artifact_passage_ordinals_v1";
const PART_ARTIFACT_BY_EVIDENCE: &str = "native_artifact_by_evidence_v1";
const PART_PAYLOADS: &str = "native_erasable_payloads_v1";
const PART_LEXICAL_DOCS: &str = "native_lexical_documents_v1";
const PART_LEXICAL_POSTINGS: &str = "native_lexical_postings_v1";
const PART_TIME_INDEX: &str = "native_observed_time_v1";
const PART_DEPENDENCIES: &str = "native_dependencies_v1";
const PART_RECALL_CASES: &str = "native_recall_cases_v1";
const PART_RELATION_EVALUATORS: &str = "native_relation_evaluator_revisions_v1";
const PART_RELATION_EVALUATOR_HEADS: &str = "native_relation_evaluator_heads_v1";
const PART_RELATION_PROFILES: &str = "native_relation_profile_revisions_v1";
const PART_RELATION_PROFILE_HEADS: &str = "native_relation_profile_heads_v1";
const PART_RELATION_SIGNALS: &str = "native_relation_signal_revisions_v1";
const PART_RELATION_SIGNAL_HEADS: &str = "native_relation_signal_heads_v1";
const PART_RELATION_SIGNAL_PAYLOADS: &str = "native_relation_signal_payloads_v1";
const PART_RELATION_SIGNAL_PAIRS: &str = "native_relation_signal_pairs_v1";
const PART_RELATION_SIGNALS_BY_RECORD: &str = "native_relation_signals_by_record_v1";
const PART_RELATION_SIGNALS_BY_EVALUATOR: &str = "native_relation_signals_by_evaluator_v1";
const PART_RELATION_SIGNALS_BY_PROFILE: &str = "native_relation_signals_by_profile_v1";
const PART_ACTIVATION_TRACES: &str = "native_activation_traces_v1";
const PART_ACTIVATION_TRACE_PAYLOADS: &str = "native_activation_trace_payloads_v1";
const PART_ACTIVATION_TRACES_BY_RECORD: &str = "native_activation_traces_by_record_v1";
const PART_ACTIVATION_TRACES_BY_EVALUATOR: &str = "native_activation_traces_by_evaluator_v1";
const PART_ACTIVATION_TRACES_BY_PROFILE: &str = "native_activation_traces_by_profile_v1";
const PART_AUDIT: &str = "native_audit_events_v1";
const PART_OPERATIONS: &str = "native_operations_v1";

const MAX_RECALL_LIMIT: usize = 100;
const MAX_RECALL_TERMS: usize = 64;
const MAX_RECALL_CANDIDATES: usize = 4_096;
pub const MAX_RECALL_SCOPES: usize = 32;
pub const MAX_TEXT_BYTES: usize = 64 * 1024;
/// Exact observations may be larger than derived semantic text. This keeps a
/// normal long-form user turn lossless while retaining a hard per-record cap.
pub const MAX_EVIDENCE_TEXT_BYTES: usize = 1024 * 1024;
pub const MAX_RECALL_QUERY_BYTES: usize = 32 * 1024;
pub const MAX_EVIDENCE_SOURCES: usize = 64;
const MAX_INDEX_TERMS_PER_RECORD: usize = 8_192;
pub const MAX_PROPOSAL_CHANGES: usize = 128;
pub const MAX_PROPOSAL_ENCODED_BYTES: usize = 1024 * 1024;
pub const MAX_PROPOSAL_DEPENDENCY_EDGES: usize = 4_096;
pub const MAX_REVIEW_FINDINGS: usize = 64;
pub const MAX_REVIEW_PINS_PER_FINDING: usize = 64;
pub const MAX_OPERATOR_LIST_LIMIT: usize = 100;
pub const MAX_ENTITY_ALIASES: usize = 64;
pub const MAX_PROPOSAL_ALIASES: usize = 1_024;
pub const MAX_OPERATIONAL_FINGERPRINT_DOMAIN_BYTES: usize = 64;
pub const MAX_OPERATIONAL_FINGERPRINT_INPUT_BYTES: usize = 1024 * 1024;
pub const MAX_SOURCE_EVENT_ID_BYTES: usize = 256;
pub const MAX_ARTIFACT_SNAPSHOT_BYTES: usize = 32 * 1024 * 1024;
pub const MAX_ARTIFACT_LABEL_BYTES: usize = 1024;
pub const MAX_ARTIFACT_MEDIA_TYPE_BYTES: usize = 1024;
pub const MAX_ARTIFACT_PASSAGE_BATCH: usize = 512;
pub const MAX_ARTIFACT_PASSAGE_BATCH_BYTES: usize = 8 * 1024 * 1024;
pub const MAX_RELATION_DIMENSIONS: usize = 16;
pub const MAX_RELATION_HEADS: usize = 16;
pub const RELATION_FIXED_POINT_SCALE: i32 = 1_000_000;
pub const MAX_RELATION_SIGNAL_BATCH: usize = 1_024;
pub const MAX_ACTIVATION_CANDIDATES: usize = 64;
pub const MAX_RELATION_CANDIDATE_PAIRS: usize = 4_096;
pub const MAX_ACTIVATION_TRACE_CONTRIBUTIONS: usize = 4_096;
pub const PURGE_PREVIEW_TTL_MS: i64 = 10 * 60 * 1_000;

macro_rules! id_type {
    ($name:ident) => {
        #[derive(
            Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
        )]
        pub struct $name(pub Ulid);

        impl $name {
            pub fn new() -> Self {
                Self(Ulid::new())
            }

            pub fn as_ulid(self) -> Ulid {
                self.0
            }
        }

        impl Default for $name {
            fn default() -> Self {
                Self::new()
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                self.0.fmt(f)
            }
        }
    };
}

id_type!(EraId);
id_type!(OperationId);
id_type!(EvidenceId);
id_type!(ClaimId);
id_type!(RelationId);
id_type!(EntityId);
id_type!(RecallCaseId);
id_type!(AuditEventId);
id_type!(ProposalId);
id_type!(ProposalSourceJobId);
id_type!(ProposalDraftId);
id_type!(ProposalReviewCaseId);
id_type!(RecallFeedbackId);
id_type!(ArtifactCollectionId);
id_type!(ArtifactSnapshotId);
id_type!(ArtifactPassageId);
id_type!(RelationEvaluatorId);
id_type!(RelationProfileId);
id_type!(RelationSignalId);
id_type!(ActivationTraceId);

#[derive(Debug)]
pub enum MemoryError {
    Io(std::io::Error),
    StoreFormat(StoreFormatError),
    Storage(String),
    Codec(serde_json::Error),
    Corrupt(String),
    StoreAlreadyExists(PathBuf),
    StoreEraMismatch {
        outer: String,
        internal: String,
    },
    NotFound(RecordRef),
    OperationConflict(OperationId),
    RevisionConflict {
        expected: u64,
        actual: u64,
    },
    InadmissibleSource {
        evidence_id: EvidenceId,
        class: EvidenceClass,
        domain: ClaimDomain,
    },
    SourceUnavailable(EvidenceId),
    ScopeMismatch,
    Unauthorized,
    InvalidInput(String),
    StalePurgePreview,
    PurgePreviewNotYetValid {
        issued_at_ms: i64,
        now_ms: i64,
    },
    PurgePreviewExpired {
        expires_at_ms: i64,
        now_ms: i64,
    },
}

impl fmt::Display for MemoryError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(error) => write!(f, "I/O error: {error}"),
            Self::StoreFormat(error) => error.fmt(f),
            Self::Storage(error) => write!(f, "storage error: {error}"),
            Self::Codec(error) => write!(f, "codec error: {error}"),
            Self::Corrupt(message) => write!(f, "corrupt native memory store: {message}"),
            Self::StoreAlreadyExists(path) => write!(
                f,
                "refusing to initialize an existing store root: {}",
                path.display()
            ),
            Self::StoreEraMismatch { outer, internal } => write!(
                f,
                "native metadata era {internal} does not match outer store era {outer}"
            ),
            Self::NotFound(record) => write!(f, "record not found: {record:?}"),
            Self::OperationConflict(id) => {
                write!(
                    f,
                    "operation {id} was already used for a different mutation"
                )
            }
            Self::RevisionConflict { expected, actual } => write!(
                f,
                "revision conflict: expected {expected}, current revision is {actual}"
            ),
            Self::InadmissibleSource {
                evidence_id,
                class,
                domain,
            } => write!(
                f,
                "evidence {evidence_id} of class {class:?} is inadmissible for {domain:?}"
            ),
            Self::SourceUnavailable(id) => write!(f, "evidence {id} is unavailable"),
            Self::ScopeMismatch => write!(f, "record scopes do not match"),
            Self::Unauthorized => write!(f, "actor is not authorized for this operation"),
            Self::InvalidInput(message) => write!(f, "invalid input: {message}"),
            Self::StalePurgePreview => write!(f, "purge preview no longer matches current state"),
            Self::PurgePreviewNotYetValid {
                issued_at_ms,
                now_ms,
            } => write!(
                f,
                "purge preview is not valid before {issued_at_ms}ms (now {now_ms}ms)"
            ),
            Self::PurgePreviewExpired {
                expires_at_ms,
                now_ms,
            } => write!(
                f,
                "purge preview expired at {expires_at_ms}ms (now {now_ms}ms)"
            ),
        }
    }
}

impl std::error::Error for MemoryError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            Self::StoreFormat(error) => Some(error),
            Self::Codec(error) => Some(error),
            _ => None,
        }
    }
}

impl From<std::io::Error> for MemoryError {
    fn from(value: std::io::Error) -> Self {
        Self::Io(value)
    }
}

impl From<serde_json::Error> for MemoryError {
    fn from(value: serde_json::Error) -> Self {
        Self::Codec(value)
    }
}

impl From<StoreFormatError> for MemoryError {
    fn from(value: StoreFormatError) -> Self {
        Self::StoreFormat(value)
    }
}

pub type MemoryResult<T> = std::result::Result<T, MemoryError>;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Actor {
    User,
    Assistant,
    System,
    Operator,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct OperationContext {
    pub id: OperationId,
    pub actor: Actor,
}

impl OperationContext {
    pub fn new(actor: Actor) -> Self {
        Self {
            id: OperationId::new(),
            actor,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum Scope {
    Personal,
    Workspace(Ulid),
    Session(Ulid),
    Artifact(Ulid),
    System,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct TemporalFacts {
    pub observed_at_ms: i64,
    pub source_at_ms: Option<i64>,
    pub valid_from_ms: Option<i64>,
    pub valid_to_ms: Option<i64>,
}

impl TemporalFacts {
    pub fn observed_at(observed_at_ms: i64) -> Self {
        Self {
            observed_at_ms,
            source_at_ms: None,
            valid_from_ms: None,
            valid_to_ms: None,
        }
    }

    fn validate(self) -> MemoryResult<()> {
        if let (Some(from), Some(to)) = (self.valid_from_ms, self.valid_to_ms) {
            if from >= to {
                return Err(MemoryError::InvalidInput(
                    "valid time must use a non-empty [from, to) interval".into(),
                ));
            }
        }
        Ok(())
    }

    fn contains_valid_time(self, at_ms: i64) -> bool {
        self.valid_from_ms.is_none_or(|from| at_ms >= from)
            && self.valid_to_ms.is_none_or(|to| at_ms < to)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RecordState {
    Active,
    Retracted,
    Superseded,
    Unsupported,
    Purged,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum RecordRef {
    Evidence(EvidenceId),
    Claim(ClaimId),
    Entity(EntityId),
    SemanticRelation(RelationId),
    Proposal(ProposalId),
    ProposalReview(ProposalReviewCaseId),
    ArtifactCollection(ArtifactCollectionId),
    ArtifactSnapshot(ArtifactSnapshotId),
    ArtifactPassage(ArtifactPassageId),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RevisionSelector {
    Head,
    Exact(u64),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum EvidenceClass {
    UserAssertion,
    UserCorrection,
    ImportedSource,
    ToolObservation,
    ActionOutcome,
    AssistantCommitment,
    AssistantUtterance,
    SystemObservation,
    ArtifactSnapshot,
    ArtifactPassage,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TerminalTurnStatus {
    Completed,
    Failed,
    Cancelled,
    Interrupted,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum EvidenceLifecycle {
    Direct,
    TerminalTurn {
        /// Opaque caller-owned event or run identifier. It is intentionally
        /// not parsed as a storage-native ID.
        source_event_id: String,
        status: TerminalTurnStatus,
    },
}

/// Content-free lifecycle truth persisted in evidence headers and citations.
/// Caller-owned terminal identifiers are replaced by a store-keyed digest at
/// the ingestion boundary.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum EvidenceLifecycleTruth {
    Direct,
    TerminalTurn {
        source_event_digest: [u8; 32],
        status: TerminalTurnStatus,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ClaimDomain {
    UserProfile,
    UserPreference,
    UserNote,
    ExternalFact,
    WorkspaceFact,
    SessionContext,
    ArtifactContent,
    SystemFact,
    AssistantCommitment,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NewEvidence {
    pub class: EvidenceClass,
    pub scope: Scope,
    pub temporal: TemporalFacts,
    pub lifecycle: EvidenceLifecycle,
    pub text: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EvidenceHeader {
    pub id: EvidenceId,
    pub class: EvidenceClass,
    pub captured_by: Actor,
    pub scope: Scope,
    pub temporal: TemporalFacts,
    pub lifecycle: EvidenceLifecycleTruth,
    pub recorded_at_ms: i64,
    pub operation_id: OperationId,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EvidenceAvailabilityRevision {
    pub evidence_id: EvidenceId,
    pub revision: u64,
    pub state: RecordState,
    pub recorded_at_ms: i64,
    pub operation_id: OperationId,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EvidenceRecord {
    pub header: EvidenceHeader,
    pub availability: EvidenceAvailabilityRevision,
    /// `None` means that the separately stored erasable payload is unavailable.
    pub text: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EvidenceReceipt {
    pub id: EvidenceId,
    pub availability_revision: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NewArtifactCollection {
    pub label: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArtifactCollectionHeader {
    pub id: ArtifactCollectionId,
    pub scope: Scope,
    pub created_by: Actor,
    pub recorded_at_ms: i64,
    pub operation_id: OperationId,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArtifactAvailabilityRevision<I> {
    pub id: I,
    pub revision: u64,
    pub state: RecordState,
    pub recorded_at_ms: i64,
    pub operation_id: OperationId,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArtifactCollectionRecord {
    pub header: ArtifactCollectionHeader,
    pub availability: ArtifactAvailabilityRevision<ArtifactCollectionId>,
    pub label: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArtifactCollectionReceipt {
    pub id: ArtifactCollectionId,
    pub availability_revision: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NewArtifactSnapshot {
    pub collection_id: ArtifactCollectionId,
    pub expected_collection_revision: u64,
    pub temporal: TemporalFacts,
    pub media_type: String,
    pub bytes: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArtifactSnapshotHeader {
    pub id: ArtifactSnapshotId,
    pub collection_id: ArtifactCollectionId,
    pub evidence_id: EvidenceId,
    pub scope: Scope,
    pub temporal: TemporalFacts,
    pub byte_len: u64,
    pub recorded_at_ms: i64,
    pub operation_id: OperationId,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArtifactSnapshotRecord {
    pub header: ArtifactSnapshotHeader,
    pub availability: ArtifactAvailabilityRevision<ArtifactSnapshotId>,
    pub media_type: Option<String>,
    pub content_digest: Option<[u8; 32]>,
    pub payload_available: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArtifactSnapshotBlob {
    pub id: ArtifactSnapshotId,
    pub media_type: String,
    pub content_digest: [u8; 32],
    pub bytes: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArtifactSnapshotReceipt {
    pub id: ArtifactSnapshotId,
    pub availability_revision: u64,
    pub evidence: EvidenceReceipt,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArtifactRange {
    pub start: u64,
    pub end_exclusive: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArtifactLocator {
    pub ordinal: u32,
    pub byte_range: Option<ArtifactRange>,
    pub page_range: Option<ArtifactRange>,
    pub time_range_ms: Option<ArtifactRange>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NewArtifactPassage {
    pub locator: ArtifactLocator,
    pub text: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NewArtifactPassageBatch {
    pub snapshot_id: ArtifactSnapshotId,
    pub expected_snapshot_revision: u64,
    pub passages: Vec<NewArtifactPassage>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArtifactPassageHeader {
    pub id: ArtifactPassageId,
    pub collection_id: ArtifactCollectionId,
    pub snapshot_id: ArtifactSnapshotId,
    pub evidence_id: EvidenceId,
    pub scope: Scope,
    pub temporal: TemporalFacts,
    pub locator: ArtifactLocator,
    pub recorded_at_ms: i64,
    pub operation_id: OperationId,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArtifactPassageRecord {
    pub header: ArtifactPassageHeader,
    pub availability: ArtifactAvailabilityRevision<ArtifactPassageId>,
    pub text: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArtifactPassageReceipt {
    pub id: ArtifactPassageId,
    pub availability_revision: u64,
    pub evidence: EvidenceReceipt,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArtifactPassageBatchReceipt {
    pub passages: Vec<ArtifactPassageReceipt>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ArtifactEvidenceProvenance {
    Snapshot {
        collection_id: ArtifactCollectionId,
        snapshot_id: ArtifactSnapshotId,
        byte_len: u64,
    },
    Passage {
        collection_id: ArtifactCollectionId,
        snapshot_id: ArtifactSnapshotId,
        passage_id: ArtifactPassageId,
        locator: ArtifactLocator,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NewClaim {
    pub domain: ClaimDomain,
    pub scope: Scope,
    pub temporal: TemporalFacts,
    pub proposition: String,
    pub evidence_ids: Vec<EvidenceId>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RememberUserClaim {
    pub domain: ClaimDomain,
    pub scope: Scope,
    pub temporal: TemporalFacts,
    /// The user's original assertion, retained as immutable evidence.
    pub evidence_text: String,
    /// The durable proposition derived from that assertion.
    pub proposition: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CorrectUserClaim {
    pub target: ClaimId,
    pub expected_revision: u64,
    pub temporal: TemporalFacts,
    /// The user's original correction, retained as immutable evidence.
    pub evidence_text: String,
    pub replacement_proposition: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClaimRevisionHeader {
    pub id: ClaimId,
    pub revision: u64,
    pub previous_revision: Option<u64>,
    pub domain: ClaimDomain,
    pub scope: Scope,
    pub temporal: TemporalFacts,
    pub evidence_ids: Vec<EvidenceId>,
    pub state: RecordState,
    pub recorded_at_ms: i64,
    pub operation_id: OperationId,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClaimRecord {
    pub header: ClaimRevisionHeader,
    /// Purge removes this separately stored payload without rewriting history.
    pub proposition: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClaimReceipt {
    pub id: ClaimId,
    pub revision: u64,
    pub state: RecordState,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RememberReceipt {
    pub evidence: EvidenceReceipt,
    pub claim: ClaimReceipt,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CorrectionReceipt {
    pub evidence: EvidenceReceipt,
    pub superseded: RecordRevision,
    pub replacement: ClaimReceipt,
    pub supersedes_relation: RelationReceipt,
    pub invalidated: Vec<RecordRevision>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum EntityKind {
    Person,
    Organization,
    Place,
    Work,
    Concept,
    Artifact,
    Event,
    Other,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EntityRevisionHeader {
    pub id: EntityId,
    pub revision: u64,
    pub previous_revision: Option<u64>,
    pub kind: EntityKind,
    pub scope: Scope,
    pub temporal: TemporalFacts,
    pub evidence_ids: Vec<EvidenceId>,
    pub state: RecordState,
    pub recorded_at_ms: i64,
    pub operation_id: OperationId,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct EntityPayload {
    canonical_name: String,
    aliases: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EntityRecord {
    pub header: EntityRevisionHeader,
    pub canonical_name: Option<String>,
    pub aliases: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EntityReceipt {
    pub id: EntityId,
    pub revision: u64,
    pub state: RecordState,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RelationKind {
    Supports,
    Contradicts,
    Supersedes,
    About,
    RefersTo,
    DerivedFrom,
    CanonicalizesTo,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NewSemanticRelation {
    pub from: RecordRef,
    pub to: RecordRef,
    pub kind: RelationKind,
    pub scope: Scope,
    pub evidence_ids: Vec<EvidenceId>,
    pub qualifier: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SemanticRelationRevisionHeader {
    pub id: RelationId,
    pub revision: u64,
    pub previous_revision: Option<u64>,
    /// These three identity fields never change for a `RelationId`.
    pub from: RecordRef,
    pub to: RecordRef,
    pub kind: RelationKind,
    pub scope: Scope,
    pub evidence_ids: Vec<EvidenceId>,
    pub state: RecordState,
    pub recorded_at_ms: i64,
    pub operation_id: OperationId,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SemanticRelationRecord {
    pub header: SemanticRelationRevisionHeader,
    pub qualifier: Option<String>,
    pub payload_available: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum InspectedRecord {
    Evidence(EvidenceRecord),
    Claim(ClaimRecord),
    Entity(EntityRecord),
    SemanticRelation(SemanticRelationRecord),
    Proposal(ProposalRecord),
    ProposalReview(ProposalReviewCase),
    ArtifactCollection(ArtifactCollectionRecord),
    ArtifactSnapshot(ArtifactSnapshotRecord),
    ArtifactPassage(ArtifactPassageRecord),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RelationReceipt {
    pub id: RelationId,
    pub revision: u64,
    pub state: RecordState,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct RelationEvaluatorPin {
    pub id: RelationEvaluatorId,
    pub revision: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum RelationDimension {
    Support,
    Contradiction,
    Supersession,
    EntityIdentity,
    TemporalRelevance,
    TaskRelevance,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RelationEvaluatorKind {
    DeterministicRules,
    ExternalProjection,
    OfflineLearnedProjection,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NewRelationEvaluatorRevision {
    pub id: RelationEvaluatorId,
    /// `None` creates the ID; `Some` performs a compare-and-swap revision.
    pub expected_revision: Option<u64>,
    pub kind: RelationEvaluatorKind,
    pub schema_version: u32,
    pub dimensions: Vec<RelationDimension>,
    /// Opaque provenance for the exact evaluator artifact/ruleset. It is never
    /// interpreted, fetched, loaded, or executed by mmdb.
    pub provenance_digest: [u8; 32],
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RelationEvaluatorRevisionHeader {
    pub id: RelationEvaluatorId,
    pub revision: u64,
    pub previous_revision: Option<u64>,
    pub kind: RelationEvaluatorKind,
    pub schema_version: u32,
    pub dimensions: Vec<RelationDimension>,
    pub provenance_digest: [u8; 32],
    pub state: RecordState,
    pub recorded_at_ms: i64,
    pub operation_id: OperationId,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RelationEvaluatorRecord {
    pub header: RelationEvaluatorRevisionHeader,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct RelationEvaluatorReceipt {
    pub pin: RelationEvaluatorPin,
    pub state: RecordState,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct RelationProfilePin {
    pub id: RelationProfileId,
    pub revision: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct RelationHeadWeight {
    pub dimension: RelationDimension,
    /// Signed fixed-point weight using [`RELATION_FIXED_POINT_SCALE`].
    pub weight_micros: i32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NewRelationProfileRevision {
    pub id: RelationProfileId,
    pub expected_revision: Option<u64>,
    pub evaluator: RelationEvaluatorPin,
    pub heads: Vec<RelationHeadWeight>,
    pub provenance_digest: [u8; 32],
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RelationProfileRevisionHeader {
    pub id: RelationProfileId,
    pub revision: u64,
    pub previous_revision: Option<u64>,
    pub evaluator: RelationEvaluatorPin,
    pub heads: Vec<RelationHeadWeight>,
    pub provenance_digest: [u8; 32],
    pub state: RecordState,
    pub recorded_at_ms: i64,
    pub operation_id: OperationId,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RelationProfileAvailability {
    Available,
    StaleEvaluator,
    Inactive,
    UnavailableEvaluator,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RelationProfileRecord {
    pub header: RelationProfileRevisionHeader,
    pub availability: RelationProfileAvailability,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct RelationProfileReceipt {
    pub pin: RelationProfilePin,
    pub state: RecordState,
    pub availability: RelationProfileAvailability,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct RelationSignalPin {
    pub id: RelationSignalId,
    pub revision: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct RelationDimensionScore {
    pub dimension: RelationDimension,
    /// Signed fixed-point score using [`RELATION_FIXED_POINT_SCALE`].
    pub score_micros: i32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NewRelationSignal {
    pub from: RecordRevisionPin,
    pub to: RecordRevisionPin,
    pub expected_signal: Option<RelationSignalPin>,
    pub scores: Vec<RelationDimensionScore>,
    /// Opaque producer/run provenance. It is erased with the score payload.
    pub provenance_digest: [u8; 32],
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NewRelationSignalBatch {
    pub evaluator: RelationEvaluatorPin,
    pub profile: RelationProfilePin,
    pub signals: Vec<NewRelationSignal>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RelationSignalRevisionHeader {
    pub id: RelationSignalId,
    pub revision: u64,
    pub previous_revision: Option<u64>,
    pub evaluator: RelationEvaluatorPin,
    pub profile: RelationProfilePin,
    pub from: RecordRevisionPin,
    pub to: RecordRevisionPin,
    pub state: RecordState,
    pub recorded_at_ms: i64,
    pub operation_id: OperationId,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct RelationSignalPayload {
    revision: u64,
    scores: Vec<RelationDimensionScore>,
    provenance_digest: [u8; 32],
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RelationSignalStaleReason {
    EvaluatorAdvanced,
    EvaluatorInactive,
    ProfileAdvanced,
    ProfileInactive,
    SourceAdvanced,
    SourceInactive,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RelationSignalUnavailableReason {
    PayloadPurged,
    HistoricalPayloadUnavailable,
    EvaluatorPurged,
    ProfilePurged,
    SourcePurged,
    MissingDependency,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RelationSignalAvailability {
    Available,
    Stale(RelationSignalStaleReason),
    Unavailable(RelationSignalUnavailableReason),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RelationSignalRecord {
    pub header: RelationSignalRevisionHeader,
    pub scores: Option<Vec<RelationDimensionScore>>,
    pub provenance_digest: Option<[u8; 32]>,
    pub availability: RelationSignalAvailability,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct RelationSignalReceipt {
    pub pin: RelationSignalPin,
    pub state: RecordState,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RelationSignalBatchReceipt {
    pub signals: Vec<RelationSignalReceipt>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum DerivedDefinitionStateChange {
    Retract,
    Purge,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RelationEvaluatorStateReceipt {
    pub evaluator: RelationEvaluatorReceipt,
    pub purged_relation_signals: Vec<RelationSignalPin>,
    pub purged_activation_traces: Vec<ActivationTraceId>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RelationProfileStateReceipt {
    pub profile: RelationProfileReceipt,
    pub purged_relation_signals: Vec<RelationSignalPin>,
    pub purged_activation_traces: Vec<ActivationTraceId>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct RecallCasePin {
    pub id: RecallCaseId,
    pub revision: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShadowActivationRequest {
    pub recall_case: RecallCasePin,
    pub evaluator: RelationEvaluatorPin,
    pub profile: RelationProfilePin,
    pub candidates: Vec<RecordRevisionPin>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ActivationContribution {
    pub signal: RelationSignalPin,
    pub from: RecordRevisionPin,
    pub to: RecordRevisionPin,
    pub weighted_score_micros: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ActivationCandidateTrace {
    pub candidate: RecordRevisionPin,
    pub baseline_rank: u32,
    pub activation_score_micros: i64,
    pub shadow_rank: u32,
}

/// Query-local structural shadow output. It contains no query text, excerpts,
/// terms, or free-form rationale and never replaces the pinned recall case.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ActivationTrace {
    pub id: ActivationTraceId,
    pub revision: u64,
    pub era_id: EraId,
    pub operation_id: OperationId,
    pub recall_case: RecallCasePin,
    pub evaluator: RelationEvaluatorPin,
    pub profile: RelationProfilePin,
    pub input_digest: [u8; 32],
    pub candidates: Vec<ActivationCandidateTrace>,
    pub contributions: Vec<ActivationContribution>,
    pub recorded_at_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct ActivationTraceHeader {
    id: ActivationTraceId,
    revision: u64,
    era_id: EraId,
    operation_id: OperationId,
    recall_case: RecallCasePin,
    evaluator: RelationEvaluatorPin,
    profile: RelationProfilePin,
    input_digest: [u8; 32],
    recorded_at_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct ActivationTracePayload {
    candidates: Vec<ActivationCandidateTrace>,
    contributions: Vec<ActivationContribution>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
struct StoredActivationTraceReceipt {
    id: ActivationTraceId,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RecallQuery {
    pub text: String,
    pub scopes: Vec<Scope>,
    pub observed_from_ms: Option<i64>,
    /// Exclusive upper bound.
    pub observed_to_ms: Option<i64>,
    pub valid_at_ms: Option<i64>,
    pub limit: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RecallCitation {
    pub record: RecordRef,
    pub revision: u64,
    pub scope: Scope,
    pub temporal: TemporalFacts,
    pub evidence_ids: Vec<EvidenceId>,
    pub evidence: Vec<RecallEvidenceCitation>,
    pub text: String,
    pub exact_match: bool,
    pub matched_term_count: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct RecallEvidenceCitation {
    pub id: EvidenceId,
    pub revision: u64,
    pub class: EvidenceClass,
    pub lifecycle: EvidenceLifecycleTruth,
    pub artifact: Option<ArtifactEvidenceProvenance>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RecallResult {
    pub case_id: RecallCaseId,
    pub citations: Vec<RecallCitation>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BeginTurnRecallResult {
    pub evidence: EvidenceReceipt,
    pub recall: RecallResult,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RecallCaseCandidate {
    pub record: RecordRef,
    pub revision: u64,
    pub rank: u32,
    pub exact_match: bool,
    pub matched_term_count: u32,
}

/// Structural trace only: no query, excerpt, terms, or free-form rationale.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RecallCase {
    pub id: RecallCaseId,
    pub revision: u64,
    pub era_id: EraId,
    pub query_digest: [u8; 32],
    pub scopes: Vec<Scope>,
    pub candidates: Vec<RecallCaseCandidate>,
    pub recorded_at_ms: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct RecordRevisionPin {
    pub record: RecordRef,
    pub revision: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct EvidenceRevisionPin {
    pub id: EvidenceId,
    pub revision: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ProposalStatus {
    PendingReview,
    ReviewedApprove,
    ReviewedReject,
    NeedsUser,
    Applied,
    Rejected,
    Stale,
    Unsupported,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ProposalEndpoint {
    Draft(ProposalDraftId),
    Existing(RecordRevisionPin),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ProposalChange {
    CreateClaim {
        draft_id: ProposalDraftId,
        domain: ClaimDomain,
        temporal: TemporalFacts,
        proposition: String,
        evidence_ids: Vec<EvidenceId>,
    },
    CreateEntity {
        draft_id: ProposalDraftId,
        kind: EntityKind,
        temporal: TemporalFacts,
        canonical_name: String,
        aliases: Vec<String>,
        evidence_ids: Vec<EvidenceId>,
    },
    CreateRelation {
        draft_id: ProposalDraftId,
        from: ProposalEndpoint,
        to: ProposalEndpoint,
        kind: RelationKind,
        evidence_ids: Vec<EvidenceId>,
        qualifier: Option<String>,
    },
    Retract {
        target: RecordRevisionPin,
    },
    Supersede {
        target: RecordRevisionPin,
        replacement: ProposalDraftId,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NewProposalBundle {
    pub source_job_id: ProposalSourceJobId,
    pub scope: Scope,
    pub source_evidence: Vec<EvidenceRevisionPin>,
    pub changes: Vec<ProposalChange>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProposalRevisionHeader {
    pub id: ProposalId,
    pub revision: u64,
    pub previous_revision: Option<u64>,
    pub source_job_id: ProposalSourceJobId,
    pub scope: Scope,
    pub status: ProposalStatus,
    /// Availability of the separately erasable proposal bundle.
    pub state: RecordState,
    pub recorded_at_ms: i64,
    pub operation_id: OperationId,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct ProposalPayload {
    source_evidence: Vec<EvidenceRevisionPin>,
    changes: Vec<ProposalChange>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProposalRecord {
    pub header: ProposalRevisionHeader,
    pub source_evidence: Vec<EvidenceRevisionPin>,
    pub changes: Option<Vec<ProposalChange>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProposalReceipt {
    pub id: ProposalId,
    pub revision: u64,
    pub status: ProposalStatus,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ProposalSourceIndex {
    proposal_id: ProposalId,
    input_digest: [u8; 32],
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ProposalReviewPointer {
    review_case_id: ProposalReviewCaseId,
    revision: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ProposalReviewVerdict {
    Approve,
    Reject,
    NeedsUser,
    Unsupported,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ReviewFindingCode {
    EvidenceInsufficient,
    EvidenceContradiction,
    ScopeMismatch,
    TemporalConflict,
    DuplicateRecord,
    UnsafeMutation,
    AmbiguousIdentity,
    UnsupportedChange,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProposalReviewFinding {
    pub code: ReviewFindingCode,
    pub change_index: Option<u32>,
    pub pins: Vec<RecordRevisionPin>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NewProposalReview {
    pub proposal_id: ProposalId,
    pub proposal_revision: u64,
    pub recall_case_id: RecallCaseId,
    pub recall_case_revision: u64,
    pub verdict: ProposalReviewVerdict,
    pub findings: Vec<ProposalReviewFinding>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProposalReviewCaseHeader {
    pub id: ProposalReviewCaseId,
    pub revision: u64,
    pub previous_revision: Option<u64>,
    pub proposal_id: ProposalId,
    pub proposal_revision: u64,
    pub recall_case_id: RecallCaseId,
    pub recall_case_revision: u64,
    pub verdict: ProposalReviewVerdict,
    pub scope: Scope,
    pub state: RecordState,
    pub recorded_at_ms: i64,
    pub operation_id: OperationId,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct ProposalReviewPayload {
    findings: Vec<ProposalReviewFinding>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProposalReviewCase {
    pub header: ProposalReviewCaseHeader,
    pub findings: Option<Vec<ProposalReviewFinding>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProposalReviewReceipt {
    pub review_case_id: ProposalReviewCaseId,
    pub review_revision: u64,
    pub proposal: ProposalReceipt,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ProposalDecision {
    Accept,
    Reject,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum AdjudicationAuthority {
    ExplicitUser,
    ExplicitOperator,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProposalAdjudication {
    pub proposal_id: ProposalId,
    pub expected_proposal_revision: u64,
    pub review_case_id: ProposalReviewCaseId,
    pub expected_review_revision: u64,
    pub decision: ProposalDecision,
    pub authority: AdjudicationAuthority,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum AppliedRecord {
    Claim(ClaimId),
    Entity(EntityId),
    SemanticRelation(RelationId),
}

impl AppliedRecord {
    fn as_record_ref(self) -> RecordRef {
        match self {
            Self::Claim(id) => RecordRef::Claim(id),
            Self::Entity(id) => RecordRef::Entity(id),
            Self::SemanticRelation(id) => RecordRef::SemanticRelation(id),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DraftMapping {
    pub draft_id: ProposalDraftId,
    pub record: AppliedRecord,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProposalAdjudicationReceipt {
    pub proposal: ProposalReceipt,
    pub draft_mappings: Vec<DraftMapping>,
    pub changed_records: Vec<RecordRevision>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RecallFeedbackKind {
    Relevant,
    Irrelevant,
    Outdated,
    Unsafe,
    MissingExpectedRecord,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NewRecallFeedback {
    pub recall_case_id: RecallCaseId,
    pub recall_case_revision: u64,
    pub candidate: Option<RecordRevisionPin>,
    pub kind: RecallFeedbackKind,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RecallFeedback {
    pub id: RecallFeedbackId,
    pub era_id: EraId,
    pub operation_id: OperationId,
    pub actor: Actor,
    pub recall_case_id: RecallCaseId,
    pub recall_case_revision: u64,
    pub candidate: Option<RecordRevisionPin>,
    pub kind: RecallFeedbackKind,
    pub recorded_at_ms: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum AuditAction {
    EvidenceCaptured,
    ClaimCreated,
    RelationCreated,
    RecordRetracted,
    UserClaimRemembered,
    UserClaimCorrected,
    PurgeCommitted,
    ProposalSubmitted,
    ProposalReviewed,
    ProposalAdjudicated,
    RecallFeedbackRecorded,
    ArtifactCollectionCreated,
    ArtifactSnapshotImported,
    ArtifactPassagesCreated,
    RelationEvaluatorPut,
    RelationProfilePut,
    RelationSignalsPut,
    ShadowActivationRecorded,
    RelationEvaluatorStateChanged,
    RelationProfileStateChanged,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum AuditOutcome {
    Committed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct AdjudicationAudit {
    pub decision: ProposalDecision,
    pub authority: AdjudicationAuthority,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum DerivedRecordRef {
    RelationEvaluator(RelationEvaluatorId),
    RelationProfile(RelationProfileId),
    RelationSignal(RelationSignalId),
    ActivationTrace(ActivationTraceId),
}

/// The audit header intentionally has no string or content-hash field.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuditEvent {
    pub id: AuditEventId,
    pub era_id: EraId,
    pub operation_id: OperationId,
    pub actor: Actor,
    pub action: AuditAction,
    pub subjects: Vec<RecordRef>,
    pub derived_subjects: Vec<DerivedRecordRef>,
    pub adjudication: Option<AdjudicationAudit>,
    pub outcome: AuditOutcome,
    pub recorded_at_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PurgeDependency {
    pub record: RecordRef,
    pub expected_revision: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct RelationSignalPurgeDependency {
    pub signal: RelationSignalPin,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PurgePreview {
    pub target: RecordRef,
    pub expected_revision: u64,
    /// Logical payload removals. LSM tombstones do not guarantee physical byte erasure.
    pub payloads_to_make_unavailable: u32,
    pub invalidations: Vec<PurgeDependency>,
    pub relation_signal_invalidations: Vec<RelationSignalPurgeDependency>,
    pub activation_trace_invalidations: Vec<ActivationTraceId>,
    pub issued_at_ms: i64,
    pub expires_at_ms: i64,
    pub token: [u8; 32],
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RecordRevision {
    pub record: RecordRef,
    pub revision: u64,
    pub state: RecordState,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PurgeReceipt {
    pub target: RecordRevision,
    pub invalidated: Vec<RecordRevision>,
    pub purged_relation_signals: Vec<RelationSignalPin>,
    pub purged_activation_traces: Vec<ActivationTraceId>,
    /// Payloads made unreadable through the database API.
    pub payloads_made_unavailable: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RetractionReceipt {
    pub target: RecordRevision,
    pub invalidated: Vec<RecordRevision>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct NativeMetadata {
    store_era_id: String,
    digest_key: [u8; 32],
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct StoredOperation {
    digest: [u8; 32],
    receipt: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct LexicalDocument {
    record: RecordRef,
    revision: u64,
    scope: Scope,
    temporal: TemporalFacts,
    term_hashes: Vec<[u8; 32]>,
    exact_hash: [u8; 32],
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct RelationPayload {
    qualifier: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ArtifactSnapshotPayload {
    media_type: String,
    content_digest: [u8; 32],
}

#[derive(Debug, Clone, Copy, Default)]
struct CandidateMatch {
    exact: bool,
    matched_terms: u32,
}

struct ArtifactEvidenceInsert<'a> {
    id: EvidenceId,
    class: EvidenceClass,
    scope: Scope,
    temporal: TemporalFacts,
    text: &'a str,
    recorded_at_ms: i64,
}

pub struct MemoryDatabase {
    keyspace: Keyspace,
    era_id: EraId,
    digest_key: [u8; 32],
    evidence: PartitionHandle,
    evidence_availability: PartitionHandle,
    evidence_heads: PartitionHandle,
    claims: PartitionHandle,
    claim_heads: PartitionHandle,
    relations: PartitionHandle,
    relation_heads: PartitionHandle,
    entities: PartitionHandle,
    entity_heads: PartitionHandle,
    proposals: PartitionHandle,
    proposal_heads: PartitionHandle,
    proposal_sources: PartitionHandle,
    pending_proposals: PartitionHandle,
    awaiting_adjudication: PartitionHandle,
    proposal_reviews: PartitionHandle,
    proposal_review_heads: PartitionHandle,
    latest_proposal_review: PartitionHandle,
    recall_feedback: PartitionHandle,
    artifact_collections: PartitionHandle,
    artifact_collection_availability: PartitionHandle,
    artifact_collection_heads: PartitionHandle,
    artifact_snapshots: PartitionHandle,
    artifact_snapshot_availability: PartitionHandle,
    artifact_snapshot_heads: PartitionHandle,
    artifact_snapshot_blobs: PartitionHandle,
    artifact_passages: PartitionHandle,
    artifact_passage_availability: PartitionHandle,
    artifact_passage_heads: PartitionHandle,
    artifact_passage_ordinals: PartitionHandle,
    artifact_by_evidence: PartitionHandle,
    payloads: PartitionHandle,
    lexical_docs: PartitionHandle,
    lexical_postings: PartitionHandle,
    time_index: PartitionHandle,
    dependencies: PartitionHandle,
    recall_cases: PartitionHandle,
    relation_evaluators: PartitionHandle,
    relation_evaluator_heads: PartitionHandle,
    relation_profiles: PartitionHandle,
    relation_profile_heads: PartitionHandle,
    relation_signals: PartitionHandle,
    relation_signal_heads: PartitionHandle,
    relation_signal_payloads: PartitionHandle,
    relation_signal_pairs: PartitionHandle,
    relation_signals_by_record: PartitionHandle,
    relation_signals_by_evaluator: PartitionHandle,
    relation_signals_by_profile: PartitionHandle,
    activation_traces: PartitionHandle,
    activation_trace_payloads: PartitionHandle,
    activation_traces_by_record: PartitionHandle,
    activation_traces_by_evaluator: PartitionHandle,
    activation_traces_by_profile: PartitionHandle,
    audit: PartitionHandle,
    operations: PartitionHandle,
    write_lock: Mutex<()>,
    _lease: StoreLease,
}

impl MemoryDatabase {
    /// Explicitly initialize a new store at an absent path, write the sole
    /// outer marker, and then open it through the same validation path as every
    /// later process.
    pub fn create(root: impl AsRef<Path>) -> MemoryResult<Self> {
        let root = root.as_ref();
        let lease = StoreLease::acquire(root)?;
        match fs::symlink_metadata(root) {
            Ok(_) => return Err(MemoryError::StoreAlreadyExists(root.to_path_buf())),
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(MemoryError::Io(error)),
        }
        fs::create_dir(root)?;
        let store_era_id = StoreEraId::new();
        OuterStoreManifest::new(MEMORY_STORE_FORMAT_ID, store_era_id)?.write_new(root)?;
        Self::open_with_lease(root, lease, true)
    }

    /// Open only an existing root with the exact clean-store marker and format.
    /// The outer check happens before fjall sees the path.
    pub fn open(root: impl AsRef<Path>) -> MemoryResult<Self> {
        let root = root.as_ref();
        let lease = StoreLease::acquire(root)?;
        let allow_metadata_initialization = root_contains_only_outer_manifest(root)?;
        Self::open_with_lease(root, lease, allow_metadata_initialization)
    }

    fn open_with_lease(
        root: &Path,
        lease: StoreLease,
        allow_metadata_initialization: bool,
    ) -> MemoryResult<Self> {
        let managed = require_managed_store(root, MEMORY_STORE_FORMAT_ID)?;
        let outer_store_era_id = managed.manifest().store_era_id().as_str().to_owned();
        let era_ulid = outer_store_era_id.parse::<Ulid>().map_err(|error| {
            MemoryError::Corrupt(format!("outer store era is not a ULID: {error}"))
        })?;
        let keyspace = Config::new(managed.canonical_root())
            .open()
            .map_err(|error| MemoryError::Storage(error.to_string()))?;
        let metadata_partition = open_partition(&keyspace, PART_NATIVE_METADATA)?;
        let metadata = load_or_create_native_metadata(
            &keyspace,
            &metadata_partition,
            &outer_store_era_id,
            allow_metadata_initialization,
        )?;
        if metadata.store_era_id != outer_store_era_id {
            return Err(MemoryError::StoreEraMismatch {
                outer: outer_store_era_id,
                internal: metadata.store_era_id,
            });
        }
        Ok(Self {
            evidence: open_partition(&keyspace, PART_EVIDENCE)?,
            evidence_availability: open_partition(&keyspace, PART_EVIDENCE_AVAILABILITY)?,
            evidence_heads: open_partition(&keyspace, PART_EVIDENCE_HEADS)?,
            claims: open_partition(&keyspace, PART_CLAIMS)?,
            claim_heads: open_partition(&keyspace, PART_CLAIM_HEADS)?,
            relations: open_partition(&keyspace, PART_RELATIONS)?,
            relation_heads: open_partition(&keyspace, PART_RELATION_HEADS)?,
            entities: open_partition(&keyspace, PART_ENTITIES)?,
            entity_heads: open_partition(&keyspace, PART_ENTITY_HEADS)?,
            proposals: open_partition(&keyspace, PART_PROPOSALS)?,
            proposal_heads: open_partition(&keyspace, PART_PROPOSAL_HEADS)?,
            proposal_sources: open_partition(&keyspace, PART_PROPOSAL_SOURCES)?,
            pending_proposals: open_partition(&keyspace, PART_PENDING_PROPOSALS)?,
            awaiting_adjudication: open_partition(&keyspace, PART_AWAITING_ADJUDICATION)?,
            proposal_reviews: open_partition(&keyspace, PART_PROPOSAL_REVIEWS)?,
            proposal_review_heads: open_partition(&keyspace, PART_PROPOSAL_REVIEW_HEADS)?,
            latest_proposal_review: open_partition(&keyspace, PART_LATEST_PROPOSAL_REVIEW)?,
            recall_feedback: open_partition(&keyspace, PART_RECALL_FEEDBACK)?,
            artifact_collections: open_partition(&keyspace, PART_ARTIFACT_COLLECTIONS)?,
            artifact_collection_availability: open_partition(
                &keyspace,
                PART_ARTIFACT_COLLECTION_AVAILABILITY,
            )?,
            artifact_collection_heads: open_partition(&keyspace, PART_ARTIFACT_COLLECTION_HEADS)?,
            artifact_snapshots: open_partition(&keyspace, PART_ARTIFACT_SNAPSHOTS)?,
            artifact_snapshot_availability: open_partition(
                &keyspace,
                PART_ARTIFACT_SNAPSHOT_AVAILABILITY,
            )?,
            artifact_snapshot_heads: open_partition(&keyspace, PART_ARTIFACT_SNAPSHOT_HEADS)?,
            artifact_snapshot_blobs: open_partition(&keyspace, PART_ARTIFACT_SNAPSHOT_BLOBS)?,
            artifact_passages: open_partition(&keyspace, PART_ARTIFACT_PASSAGES)?,
            artifact_passage_availability: open_partition(
                &keyspace,
                PART_ARTIFACT_PASSAGE_AVAILABILITY,
            )?,
            artifact_passage_heads: open_partition(&keyspace, PART_ARTIFACT_PASSAGE_HEADS)?,
            artifact_passage_ordinals: open_partition(&keyspace, PART_ARTIFACT_PASSAGE_ORDINALS)?,
            artifact_by_evidence: open_partition(&keyspace, PART_ARTIFACT_BY_EVIDENCE)?,
            payloads: open_partition(&keyspace, PART_PAYLOADS)?,
            lexical_docs: open_partition(&keyspace, PART_LEXICAL_DOCS)?,
            lexical_postings: open_partition(&keyspace, PART_LEXICAL_POSTINGS)?,
            time_index: open_partition(&keyspace, PART_TIME_INDEX)?,
            dependencies: open_partition(&keyspace, PART_DEPENDENCIES)?,
            recall_cases: open_partition(&keyspace, PART_RECALL_CASES)?,
            relation_evaluators: open_partition(&keyspace, PART_RELATION_EVALUATORS)?,
            relation_evaluator_heads: open_partition(&keyspace, PART_RELATION_EVALUATOR_HEADS)?,
            relation_profiles: open_partition(&keyspace, PART_RELATION_PROFILES)?,
            relation_profile_heads: open_partition(&keyspace, PART_RELATION_PROFILE_HEADS)?,
            relation_signals: open_partition(&keyspace, PART_RELATION_SIGNALS)?,
            relation_signal_heads: open_partition(&keyspace, PART_RELATION_SIGNAL_HEADS)?,
            relation_signal_payloads: open_partition(&keyspace, PART_RELATION_SIGNAL_PAYLOADS)?,
            relation_signal_pairs: open_partition(&keyspace, PART_RELATION_SIGNAL_PAIRS)?,
            relation_signals_by_record: open_partition(&keyspace, PART_RELATION_SIGNALS_BY_RECORD)?,
            relation_signals_by_evaluator: open_partition(
                &keyspace,
                PART_RELATION_SIGNALS_BY_EVALUATOR,
            )?,
            relation_signals_by_profile: open_partition(
                &keyspace,
                PART_RELATION_SIGNALS_BY_PROFILE,
            )?,
            activation_traces: open_partition(&keyspace, PART_ACTIVATION_TRACES)?,
            activation_trace_payloads: open_partition(&keyspace, PART_ACTIVATION_TRACE_PAYLOADS)?,
            activation_traces_by_record: open_partition(
                &keyspace,
                PART_ACTIVATION_TRACES_BY_RECORD,
            )?,
            activation_traces_by_evaluator: open_partition(
                &keyspace,
                PART_ACTIVATION_TRACES_BY_EVALUATOR,
            )?,
            activation_traces_by_profile: open_partition(
                &keyspace,
                PART_ACTIVATION_TRACES_BY_PROFILE,
            )?,
            audit: open_partition(&keyspace, PART_AUDIT)?,
            operations: open_partition(&keyspace, PART_OPERATIONS)?,
            keyspace,
            era_id: EraId(era_ulid),
            digest_key: metadata.digest_key,
            write_lock: Mutex::new(()),
            _lease: lease,
        })
    }

    pub fn era_id(&self) -> EraId {
        self.era_id
    }

    /// Domain-separated, per-store keyed fingerprint for durable operational
    /// idempotency. The private key never crosses the database facade.
    pub fn operational_fingerprint(&self, domain: &str, input: &[u8]) -> MemoryResult<[u8; 32]> {
        if domain.is_empty() || domain.len() > MAX_OPERATIONAL_FINGERPRINT_DOMAIN_BYTES {
            return Err(MemoryError::InvalidInput(format!(
                "fingerprint domain must contain 1..={MAX_OPERATIONAL_FINGERPRINT_DOMAIN_BYTES} bytes"
            )));
        }
        if input.len() > MAX_OPERATIONAL_FINGERPRINT_INPUT_BYTES {
            return Err(MemoryError::InvalidInput(format!(
                "fingerprint input exceeds {MAX_OPERATIONAL_FINGERPRINT_INPUT_BYTES} bytes"
            )));
        }
        let mut separated = Vec::with_capacity(domain.len() + input.len() + 9);
        separated.extend_from_slice(&(domain.len() as u64).to_be_bytes());
        separated.extend_from_slice(domain.as_bytes());
        separated.extend_from_slice(input);
        Ok(self.index_hash(0x20, &separated))
    }

    /// Recompute the store-keyed identifier retained for terminal evidence so
    /// runtime state can link its source event without persisting the raw ID in
    /// semantic memory.
    pub fn source_event_fingerprint(&self, source_event_id: &str) -> MemoryResult<[u8; 32]> {
        if source_event_id.is_empty() || source_event_id.len() > MAX_SOURCE_EVENT_ID_BYTES {
            return Err(MemoryError::InvalidInput(format!(
                "terminal source event ID must contain 1..={MAX_SOURCE_EVENT_ID_BYTES} bytes"
            )));
        }
        Ok(self.index_hash(0x05, source_event_id.as_bytes()))
    }

    pub fn capture_evidence(
        &self,
        operation: OperationContext,
        input: NewEvidence,
    ) -> MemoryResult<EvidenceReceipt> {
        let _guard = self.write_lock.lock();
        self.capture_evidence_locked(operation, input)
    }

    fn capture_evidence_locked(
        &self,
        operation: OperationContext,
        mut input: NewEvidence,
    ) -> MemoryResult<EvidenceReceipt> {
        input.temporal.validate()?;
        input.text = validate_exact_evidence_text(input.text, "evidence text")?;
        validate_evidence_capture(operation.actor, input.class, &input.lifecycle)?;
        let digest = self.mutation_digest("capture_evidence", operation.actor, &input)?;
        if let Some(receipt) = self.replayed_operation(operation.id, digest)? {
            return Ok(receipt);
        }

        let recorded_at_ms = now_ms();
        let id = EvidenceId::new();
        let lifecycle = self.evidence_lifecycle_truth(&input.lifecycle);
        let header = EvidenceHeader {
            id,
            class: input.class,
            captured_by: operation.actor,
            scope: input.scope.clone(),
            temporal: input.temporal,
            lifecycle,
            recorded_at_ms,
            operation_id: operation.id,
        };
        let availability = EvidenceAvailabilityRevision {
            evidence_id: id,
            revision: 1,
            state: RecordState::Active,
            recorded_at_ms,
            operation_id: operation.id,
        };
        let receipt = EvidenceReceipt {
            id,
            availability_revision: 1,
        };
        let audit = self.new_audit_event(
            operation,
            AuditAction::EvidenceCaptured,
            vec![RecordRef::Evidence(id)],
            recorded_at_ms,
        );

        let mut batch = self.durable_batch();
        batch.insert(&self.evidence, id_key(id.0), encode(&header)?);
        batch.insert(
            &self.evidence_availability,
            revision_key(id.0, 1),
            encode(&availability)?,
        );
        batch.insert(&self.evidence_heads, id_key(id.0), 1_u64.to_be_bytes());
        batch.insert(
            &self.payloads,
            payload_key(RecordRef::Evidence(id)),
            input.text.as_bytes(),
        );
        self.insert_lexical_document(
            &mut batch,
            RecordRef::Evidence(id),
            1,
            input.scope,
            input.temporal,
            &input.text,
        )?;
        self.insert_audit(&mut batch, &audit)?;
        self.insert_operation(&mut batch, operation.id, digest, &receipt)?;
        commit(batch)?;
        Ok(receipt)
    }

    pub fn inspect_evidence(&self, id: EvidenceId) -> MemoryResult<Option<EvidenceRecord>> {
        let Some(header) = get_decoded(&self.evidence, id_key(id.0))? else {
            return Ok(None);
        };
        let revision = get_head(&self.evidence_heads, id.0)?
            .ok_or_else(|| MemoryError::Corrupt(format!("evidence {id} has no head")))?;
        let availability: EvidenceAvailabilityRevision =
            get_decoded(&self.evidence_availability, revision_key(id.0, revision))?.ok_or_else(
                || {
                    MemoryError::Corrupt(format!(
                        "evidence {id} is missing availability revision {revision}"
                    ))
                },
            )?;
        let text = if availability.state == RecordState::Purged {
            None
        } else {
            get_payload(&self.payloads, RecordRef::Evidence(id))?
        };
        Ok(Some(EvidenceRecord {
            header,
            availability,
            text,
        }))
    }

    pub fn inspect_artifact_collection(
        &self,
        id: ArtifactCollectionId,
    ) -> MemoryResult<Option<ArtifactCollectionRecord>> {
        let Some(header) = get_decoded(&self.artifact_collections, id_key(id.0))? else {
            return Ok(None);
        };
        let Some(revision) = get_head(&self.artifact_collection_heads, id.0)? else {
            return Err(MemoryError::Corrupt(format!(
                "artifact collection {id} has no head"
            )));
        };
        self.inspect_artifact_collection_revision(header, revision)
    }

    fn inspect_artifact_collection_revision(
        &self,
        header: ArtifactCollectionHeader,
        revision: u64,
    ) -> MemoryResult<Option<ArtifactCollectionRecord>> {
        let Some(availability): Option<ArtifactAvailabilityRevision<ArtifactCollectionId>> =
            get_decoded(
                &self.artifact_collection_availability,
                revision_key(header.id.0, revision),
            )?
        else {
            return Ok(None);
        };
        let label = if availability.state == RecordState::Purged {
            None
        } else {
            get_payload(&self.payloads, RecordRef::ArtifactCollection(header.id))?
        };
        Ok(Some(ArtifactCollectionRecord {
            header,
            availability,
            label,
        }))
    }

    pub fn inspect_artifact_snapshot(
        &self,
        id: ArtifactSnapshotId,
    ) -> MemoryResult<Option<ArtifactSnapshotRecord>> {
        let Some(header) = get_decoded(&self.artifact_snapshots, id_key(id.0))? else {
            return Ok(None);
        };
        let Some(revision) = get_head(&self.artifact_snapshot_heads, id.0)? else {
            return Err(MemoryError::Corrupt(format!(
                "artifact snapshot {id} has no head"
            )));
        };
        self.inspect_artifact_snapshot_revision(header, revision)
    }

    fn inspect_artifact_snapshot_revision(
        &self,
        header: ArtifactSnapshotHeader,
        revision: u64,
    ) -> MemoryResult<Option<ArtifactSnapshotRecord>> {
        let Some(availability): Option<ArtifactAvailabilityRevision<ArtifactSnapshotId>> =
            get_decoded(
                &self.artifact_snapshot_availability,
                revision_key(header.id.0, revision),
            )?
        else {
            return Ok(None);
        };
        let payload: Option<ArtifactSnapshotPayload> = if availability.state == RecordState::Purged
        {
            None
        } else {
            get_decoded(
                &self.payloads,
                payload_key(RecordRef::ArtifactSnapshot(header.id)),
            )?
        };
        Ok(Some(ArtifactSnapshotRecord {
            header,
            availability,
            media_type: payload.as_ref().map(|payload| payload.media_type.clone()),
            content_digest: payload.as_ref().map(|payload| payload.content_digest),
            payload_available: payload.is_some(),
        }))
    }

    /// Explicitly materialize and verify the exact snapshot bytes. Metadata,
    /// revision checks, retraction, and purge never call this method.
    pub fn materialize_artifact_snapshot(
        &self,
        id: ArtifactSnapshotId,
    ) -> MemoryResult<Option<ArtifactSnapshotBlob>> {
        self.materialize_artifact_snapshot_with_observer(id, || {})
    }

    fn materialize_artifact_snapshot_with_observer<F>(
        &self,
        id: ArtifactSnapshotId,
        after_metadata: F,
    ) -> MemoryResult<Option<ArtifactSnapshotBlob>>
    where
        F: FnOnce(),
    {
        // Keep metadata, erasable payload, and blob observation on the same
        // side of any concurrent retract/purge commit. Without this gate a
        // purge between the metadata and blob reads could either surface a
        // false corruption error or return bytes after the purge linearized.
        let _guard = self.write_lock.lock();
        let Some(record) = self.inspect_artifact_snapshot(id)? else {
            return Ok(None);
        };
        if !record.payload_available {
            return Ok(None);
        }
        let media_type = record.media_type.ok_or_else(|| {
            MemoryError::Corrupt(format!("artifact snapshot {id} has incomplete metadata"))
        })?;
        let content_digest = record.content_digest.ok_or_else(|| {
            MemoryError::Corrupt(format!("artifact snapshot {id} has incomplete metadata"))
        })?;
        after_metadata();
        let Some(bytes) = self
            .artifact_snapshot_blobs
            .get(id_key(id.0))
            .map_err(storage_error)?
            .map(|value| value.to_vec())
        else {
            return Err(MemoryError::Corrupt(format!(
                "artifact snapshot {id} metadata exists without its blob"
            )));
        };
        if bytes.len() as u64 != record.header.byte_len
            || blake3::hash(&bytes).as_bytes() != &content_digest
        {
            return Err(MemoryError::Corrupt(format!(
                "artifact snapshot {id} blob does not match its erasable metadata"
            )));
        }
        Ok(Some(ArtifactSnapshotBlob {
            id,
            media_type,
            content_digest,
            bytes,
        }))
    }

    pub fn artifact_provenance_for_evidence(
        &self,
        evidence_id: EvidenceId,
    ) -> MemoryResult<Option<ArtifactEvidenceProvenance>> {
        get_decoded(&self.artifact_by_evidence, id_key(evidence_id.0))
    }

    pub fn inspect_artifact_passage(
        &self,
        id: ArtifactPassageId,
    ) -> MemoryResult<Option<ArtifactPassageRecord>> {
        let Some(header) = get_decoded(&self.artifact_passages, id_key(id.0))? else {
            return Ok(None);
        };
        let Some(revision) = get_head(&self.artifact_passage_heads, id.0)? else {
            return Err(MemoryError::Corrupt(format!(
                "artifact passage {id} has no head"
            )));
        };
        self.inspect_artifact_passage_revision(header, revision)
    }

    fn inspect_artifact_passage_revision(
        &self,
        header: ArtifactPassageHeader,
        revision: u64,
    ) -> MemoryResult<Option<ArtifactPassageRecord>> {
        let Some(availability): Option<ArtifactAvailabilityRevision<ArtifactPassageId>> =
            get_decoded(
                &self.artifact_passage_availability,
                revision_key(header.id.0, revision),
            )?
        else {
            return Ok(None);
        };
        let text = if availability.state == RecordState::Purged {
            None
        } else {
            get_payload(&self.payloads, RecordRef::ArtifactPassage(header.id))?
        };
        Ok(Some(ArtifactPassageRecord {
            header,
            availability,
            text,
        }))
    }

    pub fn create_artifact_collection(
        &self,
        operation: OperationContext,
        mut input: NewArtifactCollection,
    ) -> MemoryResult<ArtifactCollectionReceipt> {
        require_artifact_actor(operation.actor)?;
        input.label =
            validate_exact_bounded_text(input.label, "artifact label", MAX_ARTIFACT_LABEL_BYTES)?;
        let digest = self.mutation_digest("create_artifact_collection", operation.actor, &input)?;
        let _guard = self.write_lock.lock();
        if let Some(receipt) = self.replayed_operation(operation.id, digest)? {
            return Ok(receipt);
        }
        let id = ArtifactCollectionId::new();
        let recorded_at_ms = now_ms();
        let header = ArtifactCollectionHeader {
            id,
            scope: Scope::Artifact(id.0),
            created_by: operation.actor,
            recorded_at_ms,
            operation_id: operation.id,
        };
        let availability = ArtifactAvailabilityRevision {
            id,
            revision: 1,
            state: RecordState::Active,
            recorded_at_ms,
            operation_id: operation.id,
        };
        let receipt = ArtifactCollectionReceipt {
            id,
            availability_revision: 1,
        };
        let mut batch = self.durable_batch();
        batch.insert(&self.artifact_collections, id_key(id.0), encode(&header)?);
        batch.insert(
            &self.artifact_collection_availability,
            revision_key(id.0, 1),
            encode(&availability)?,
        );
        batch.insert(
            &self.artifact_collection_heads,
            id_key(id.0),
            1_u64.to_be_bytes(),
        );
        batch.insert(
            &self.payloads,
            payload_key(RecordRef::ArtifactCollection(id)),
            input.label.as_bytes(),
        );
        let audit = self.new_audit_event(
            operation,
            AuditAction::ArtifactCollectionCreated,
            vec![RecordRef::ArtifactCollection(id)],
            recorded_at_ms,
        );
        self.insert_audit(&mut batch, &audit)?;
        self.insert_operation(&mut batch, operation.id, digest, &receipt)?;
        commit(batch)?;
        Ok(receipt)
    }

    pub fn import_artifact_snapshot(
        &self,
        operation: OperationContext,
        mut input: NewArtifactSnapshot,
    ) -> MemoryResult<ArtifactSnapshotReceipt> {
        require_artifact_actor(operation.actor)?;
        input.temporal.validate()?;
        input.media_type = validate_exact_bounded_text(
            input.media_type,
            "artifact media type",
            MAX_ARTIFACT_MEDIA_TYPE_BYTES,
        )?;
        if input.bytes.is_empty() || input.bytes.len() > MAX_ARTIFACT_SNAPSHOT_BYTES {
            return Err(MemoryError::InvalidInput(format!(
                "artifact snapshot bytes must contain 1..={MAX_ARTIFACT_SNAPSHOT_BYTES} bytes"
            )));
        }
        let digest = self.artifact_snapshot_mutation_digest(operation.actor, &input)?;
        let _guard = self.write_lock.lock();
        if let Some(receipt) = self.replayed_operation(operation.id, digest)? {
            return Ok(receipt);
        }
        let collection = self
            .inspect_artifact_collection(input.collection_id)?
            .ok_or(MemoryError::NotFound(RecordRef::ArtifactCollection(
                input.collection_id,
            )))?;
        require_active_revision(
            collection.availability.state,
            collection.availability.revision,
            input.expected_collection_revision,
        )?;
        let id = ArtifactSnapshotId::new();
        let evidence_id = EvidenceId::new();
        let content_digest = *blake3::hash(&input.bytes).as_bytes();
        let recorded_at_ms = now_ms();
        let scope = collection.header.scope;
        let header = ArtifactSnapshotHeader {
            id,
            collection_id: input.collection_id,
            evidence_id,
            scope: scope.clone(),
            temporal: input.temporal,
            byte_len: input.bytes.len() as u64,
            recorded_at_ms,
            operation_id: operation.id,
        };
        let availability = ArtifactAvailabilityRevision {
            id,
            revision: 1,
            state: RecordState::Active,
            recorded_at_ms,
            operation_id: operation.id,
        };
        let evidence = EvidenceReceipt {
            id: evidence_id,
            availability_revision: 1,
        };
        let receipt = ArtifactSnapshotReceipt {
            id,
            availability_revision: 1,
            evidence: evidence.clone(),
        };
        let mut batch = self.durable_batch();
        batch.insert(&self.artifact_snapshots, id_key(id.0), encode(&header)?);
        batch.insert(
            &self.artifact_snapshot_availability,
            revision_key(id.0, 1),
            encode(&availability)?,
        );
        batch.insert(
            &self.artifact_snapshot_heads,
            id_key(id.0),
            1_u64.to_be_bytes(),
        );
        batch.insert(
            &self.payloads,
            payload_key(RecordRef::ArtifactSnapshot(id)),
            encode(&ArtifactSnapshotPayload {
                media_type: input.media_type.clone(),
                content_digest,
            })?,
        );
        batch.insert(&self.artifact_snapshot_blobs, id_key(id.0), &input.bytes);
        self.insert_artifact_evidence(
            &mut batch,
            operation,
            ArtifactEvidenceInsert {
                id: evidence_id,
                class: EvidenceClass::ArtifactSnapshot,
                scope,
                temporal: input.temporal,
                text: &input.media_type,
                recorded_at_ms,
            },
        )?;
        insert_bidirectional_dependency(
            &mut batch,
            &self.dependencies,
            RecordRef::ArtifactSnapshot(id),
            RecordRef::Evidence(evidence_id),
        );
        batch.insert(
            &self.artifact_by_evidence,
            id_key(evidence_id.0),
            encode(&ArtifactEvidenceProvenance::Snapshot {
                collection_id: input.collection_id,
                snapshot_id: id,
                byte_len: input.bytes.len() as u64,
            })?,
        );
        batch.insert(
            &self.dependencies,
            dependency_key(
                RecordRef::ArtifactCollection(input.collection_id),
                RecordRef::ArtifactSnapshot(id),
            ),
            [],
        );
        let audit = self.new_audit_event(
            operation,
            AuditAction::ArtifactSnapshotImported,
            vec![
                RecordRef::ArtifactSnapshot(id),
                RecordRef::Evidence(evidence_id),
            ],
            recorded_at_ms,
        );
        self.insert_audit(&mut batch, &audit)?;
        self.insert_operation(&mut batch, operation.id, digest, &receipt)?;
        commit(batch)?;
        Ok(receipt)
    }

    pub fn create_artifact_passages(
        &self,
        operation: OperationContext,
        mut input: NewArtifactPassageBatch,
    ) -> MemoryResult<ArtifactPassageBatchReceipt> {
        require_artifact_actor(operation.actor)?;
        validate_artifact_passage_batch(&mut input)?;
        let digest = self.mutation_digest("create_artifact_passages", operation.actor, &input)?;
        let _guard = self.write_lock.lock();
        if let Some(receipt) = self.replayed_operation(operation.id, digest)? {
            return Ok(receipt);
        }
        let snapshot =
            self.inspect_artifact_snapshot(input.snapshot_id)?
                .ok_or(MemoryError::NotFound(RecordRef::ArtifactSnapshot(
                    input.snapshot_id,
                )))?;
        require_active_revision(
            snapshot.availability.state,
            snapshot.availability.revision,
            input.expected_snapshot_revision,
        )?;
        for passage in &input.passages {
            if passage
                .locator
                .byte_range
                .is_some_and(|range| range.end_exclusive > snapshot.header.byte_len)
            {
                return Err(MemoryError::InvalidInput(
                    "artifact passage byte range exceeds its snapshot".into(),
                ));
            }
            if self
                .artifact_passage_ordinals
                .get(artifact_passage_ordinal_key(
                    input.snapshot_id,
                    passage.locator.ordinal,
                ))
                .map_err(storage_error)?
                .is_some()
            {
                return Err(MemoryError::InvalidInput(format!(
                    "artifact snapshot {} already has passage ordinal {}",
                    input.snapshot_id, passage.locator.ordinal
                )));
            }
        }
        let recorded_at_ms = now_ms();
        let mut batch = self.durable_batch();
        let mut receipts = Vec::with_capacity(input.passages.len());
        let mut subjects = Vec::with_capacity(input.passages.len() * 2);
        for passage in &input.passages {
            let id = ArtifactPassageId::new();
            let evidence_id = EvidenceId::new();
            let header = ArtifactPassageHeader {
                id,
                collection_id: snapshot.header.collection_id,
                snapshot_id: input.snapshot_id,
                evidence_id,
                scope: snapshot.header.scope.clone(),
                temporal: snapshot.header.temporal,
                locator: passage.locator,
                recorded_at_ms,
                operation_id: operation.id,
            };
            let availability = ArtifactAvailabilityRevision {
                id,
                revision: 1,
                state: RecordState::Active,
                recorded_at_ms,
                operation_id: operation.id,
            };
            batch.insert(&self.artifact_passages, id_key(id.0), encode(&header)?);
            batch.insert(
                &self.artifact_passage_availability,
                revision_key(id.0, 1),
                encode(&availability)?,
            );
            batch.insert(
                &self.artifact_passage_heads,
                id_key(id.0),
                1_u64.to_be_bytes(),
            );
            batch.insert(
                &self.payloads,
                payload_key(RecordRef::ArtifactPassage(id)),
                passage.text.as_bytes(),
            );
            self.insert_artifact_evidence(
                &mut batch,
                operation,
                ArtifactEvidenceInsert {
                    id: evidence_id,
                    class: EvidenceClass::ArtifactPassage,
                    scope: snapshot.header.scope.clone(),
                    temporal: snapshot.header.temporal,
                    text: &passage.text,
                    recorded_at_ms,
                },
            )?;
            insert_bidirectional_dependency(
                &mut batch,
                &self.dependencies,
                RecordRef::ArtifactPassage(id),
                RecordRef::Evidence(evidence_id),
            );
            batch.insert(
                &self.dependencies,
                dependency_key(
                    RecordRef::ArtifactSnapshot(input.snapshot_id),
                    RecordRef::ArtifactPassage(id),
                ),
                [],
            );
            batch.insert(
                &self.artifact_passage_ordinals,
                artifact_passage_ordinal_key(input.snapshot_id, passage.locator.ordinal),
                id_key(id.0),
            );
            batch.insert(
                &self.artifact_by_evidence,
                id_key(evidence_id.0),
                encode(&ArtifactEvidenceProvenance::Passage {
                    collection_id: snapshot.header.collection_id,
                    snapshot_id: input.snapshot_id,
                    passage_id: id,
                    locator: passage.locator,
                })?,
            );
            receipts.push(ArtifactPassageReceipt {
                id,
                availability_revision: 1,
                evidence: EvidenceReceipt {
                    id: evidence_id,
                    availability_revision: 1,
                },
            });
            subjects.push(RecordRef::ArtifactPassage(id));
            subjects.push(RecordRef::Evidence(evidence_id));
        }
        let receipt = ArtifactPassageBatchReceipt { passages: receipts };
        let audit = self.new_audit_event(
            operation,
            AuditAction::ArtifactPassagesCreated,
            subjects,
            recorded_at_ms,
        );
        self.insert_audit(&mut batch, &audit)?;
        self.insert_operation(&mut batch, operation.id, digest, &receipt)?;
        commit(batch)?;
        Ok(receipt)
    }

    fn insert_artifact_evidence(
        &self,
        batch: &mut fjall::Batch,
        operation: OperationContext,
        input: ArtifactEvidenceInsert<'_>,
    ) -> MemoryResult<()> {
        let header = EvidenceHeader {
            id: input.id,
            class: input.class,
            captured_by: operation.actor,
            scope: input.scope.clone(),
            temporal: input.temporal,
            lifecycle: EvidenceLifecycleTruth::Direct,
            recorded_at_ms: input.recorded_at_ms,
            operation_id: operation.id,
        };
        let availability = EvidenceAvailabilityRevision {
            evidence_id: input.id,
            revision: 1,
            state: RecordState::Active,
            recorded_at_ms: input.recorded_at_ms,
            operation_id: operation.id,
        };
        batch.insert(&self.evidence, id_key(input.id.0), encode(&header)?);
        batch.insert(
            &self.evidence_availability,
            revision_key(input.id.0, 1),
            encode(&availability)?,
        );
        batch.insert(
            &self.evidence_heads,
            id_key(input.id.0),
            1_u64.to_be_bytes(),
        );
        batch.insert(
            &self.payloads,
            payload_key(RecordRef::Evidence(input.id)),
            input.text.as_bytes(),
        );
        self.insert_lexical_document(
            batch,
            RecordRef::Evidence(input.id),
            1,
            input.scope,
            input.temporal,
            input.text,
        )
    }

    /// Retract any active native record with revision CAS. Claims and semantic
    /// relations that depend on the target are invalidated in the same durable
    /// batch, so no recall can observe a live projection backed by a retracted
    /// source.
    pub fn retract(
        &self,
        operation: OperationContext,
        record: RecordRef,
        expected_revision: u64,
    ) -> MemoryResult<RetractionReceipt> {
        if operation.actor == Actor::Assistant {
            return Err(MemoryError::Unauthorized);
        }
        let mutation = RetractMutation {
            record,
            expected_revision,
        };
        let digest = self.mutation_digest("retract", operation.actor, &mutation)?;
        let _guard = self.write_lock.lock();
        if let Some(receipt) = self.replayed_operation(operation.id, digest)? {
            return Ok(receipt);
        }

        let (_, state, actual_revision) = self.record_scope_state_revision(record)?;
        if actual_revision != expected_revision {
            return Err(MemoryError::RevisionConflict {
                expected: expected_revision,
                actual: actual_revision,
            });
        }
        if state != RecordState::Active {
            return Err(MemoryError::InvalidInput(
                "only an active record can be retracted".into(),
            ));
        }
        let dependencies = self.active_dependency_closure(record)?;
        let recorded_at_ms = now_ms();
        let mut batch = self.durable_batch();
        let target = self.append_state_revision(
            &mut batch,
            record,
            expected_revision,
            RecordState::Retracted,
            operation.id,
            recorded_at_ms,
        )?;
        self.remove_lexical_document(&mut batch, record)?;

        let mut invalidated = Vec::with_capacity(dependencies.len());
        for dependency in dependencies {
            let revision = self.append_state_revision(
                &mut batch,
                dependency.record,
                dependency.expected_revision,
                RecordState::Unsupported,
                operation.id,
                recorded_at_ms,
            )?;
            self.remove_lexical_document(&mut batch, dependency.record)?;
            invalidated.push(revision);
        }
        let receipt = RetractionReceipt {
            target,
            invalidated,
        };
        let mut subjects = vec![record];
        subjects.extend(receipt.invalidated.iter().map(|entry| entry.record));
        let audit = self.new_audit_event(
            operation,
            AuditAction::RecordRetracted,
            subjects,
            recorded_at_ms,
        );
        self.insert_audit(&mut batch, &audit)?;
        self.insert_operation(&mut batch, operation.id, digest, &receipt)?;
        commit(batch)?;
        Ok(receipt)
    }

    /// Atomically retain the user's assertion and activate the evidence-backed
    /// claim. A retry with the same operation ID returns the same two IDs.
    pub fn remember_user_claim(
        &self,
        operation: OperationContext,
        mut input: RememberUserClaim,
    ) -> MemoryResult<RememberReceipt> {
        if operation.actor != Actor::User {
            return Err(MemoryError::Unauthorized);
        }
        input.temporal.validate()?;
        input.evidence_text = validate_text(input.evidence_text, "evidence text")?;
        input.proposition = validate_text(input.proposition, "claim proposition")?;
        if !source_is_admissible(EvidenceClass::UserAssertion, input.domain) {
            return Err(MemoryError::InvalidInput(
                "a user assertion cannot support the requested claim domain".into(),
            ));
        }
        validate_claim_activation(
            operation.actor,
            input.domain,
            &[EvidenceClass::UserAssertion],
        )?;
        let digest = self.mutation_digest("remember_user_claim", operation.actor, &input)?;
        let _guard = self.write_lock.lock();
        if let Some(receipt) = self.replayed_operation(operation.id, digest)? {
            return Ok(receipt);
        }

        let recorded_at_ms = now_ms();
        let evidence_id = EvidenceId::new();
        let claim_id = ClaimId::new();
        let evidence_header = EvidenceHeader {
            id: evidence_id,
            class: EvidenceClass::UserAssertion,
            captured_by: Actor::User,
            scope: input.scope.clone(),
            temporal: input.temporal,
            lifecycle: EvidenceLifecycleTruth::Direct,
            recorded_at_ms,
            operation_id: operation.id,
        };
        let availability = EvidenceAvailabilityRevision {
            evidence_id,
            revision: 1,
            state: RecordState::Active,
            recorded_at_ms,
            operation_id: operation.id,
        };
        let claim_header = ClaimRevisionHeader {
            id: claim_id,
            revision: 1,
            previous_revision: None,
            domain: input.domain,
            scope: input.scope.clone(),
            temporal: input.temporal,
            evidence_ids: vec![evidence_id],
            state: RecordState::Active,
            recorded_at_ms,
            operation_id: operation.id,
        };
        let receipt = RememberReceipt {
            evidence: EvidenceReceipt {
                id: evidence_id,
                availability_revision: 1,
            },
            claim: ClaimReceipt {
                id: claim_id,
                revision: 1,
                state: RecordState::Active,
            },
        };
        let audit = self.new_audit_event(
            operation,
            AuditAction::UserClaimRemembered,
            vec![RecordRef::Evidence(evidence_id), RecordRef::Claim(claim_id)],
            recorded_at_ms,
        );

        let mut batch = self.durable_batch();
        batch.insert(
            &self.evidence,
            id_key(evidence_id.0),
            encode(&evidence_header)?,
        );
        batch.insert(
            &self.evidence_availability,
            revision_key(evidence_id.0, 1),
            encode(&availability)?,
        );
        batch.insert(
            &self.evidence_heads,
            id_key(evidence_id.0),
            1_u64.to_be_bytes(),
        );
        batch.insert(
            &self.payloads,
            payload_key(RecordRef::Evidence(evidence_id)),
            input.evidence_text.as_bytes(),
        );
        self.insert_lexical_document(
            &mut batch,
            RecordRef::Evidence(evidence_id),
            1,
            input.scope.clone(),
            input.temporal,
            &input.evidence_text,
        )?;
        batch.insert(
            &self.claims,
            revision_key(claim_id.0, 1),
            encode(&claim_header)?,
        );
        batch.insert(&self.claim_heads, id_key(claim_id.0), 1_u64.to_be_bytes());
        batch.insert(
            &self.payloads,
            payload_key(RecordRef::Claim(claim_id)),
            input.proposition.as_bytes(),
        );
        batch.insert(
            &self.dependencies,
            dependency_key(RecordRef::Evidence(evidence_id), RecordRef::Claim(claim_id)),
            [],
        );
        self.insert_lexical_document(
            &mut batch,
            RecordRef::Claim(claim_id),
            1,
            input.scope,
            input.temporal,
            &input.proposition,
        )?;
        self.insert_audit(&mut batch, &audit)?;
        self.insert_operation(&mut batch, operation.id, digest, &receipt)?;
        commit(batch)?;
        Ok(receipt)
    }

    /// Atomically capture a user's correction, supersede the old claim, create
    /// its replacement and the explicit `Supersedes` relation, and invalidate
    /// relations that still project the old claim as active.
    pub fn correct_user_claim(
        &self,
        operation: OperationContext,
        mut input: CorrectUserClaim,
    ) -> MemoryResult<CorrectionReceipt> {
        if operation.actor != Actor::User {
            return Err(MemoryError::Unauthorized);
        }
        input.temporal.validate()?;
        input.evidence_text = validate_text(input.evidence_text, "evidence text")?;
        input.replacement_proposition =
            validate_text(input.replacement_proposition, "replacement proposition")?;
        let digest = self.mutation_digest("correct_user_claim", operation.actor, &input)?;
        let _guard = self.write_lock.lock();
        if let Some(receipt) = self.replayed_operation(operation.id, digest)? {
            return Ok(receipt);
        }

        let current = self
            .inspect_claim(input.target)?
            .ok_or(MemoryError::NotFound(RecordRef::Claim(input.target)))?;
        if current.header.revision != input.expected_revision {
            return Err(MemoryError::RevisionConflict {
                expected: input.expected_revision,
                actual: current.header.revision,
            });
        }
        if current.header.state != RecordState::Active {
            return Err(MemoryError::InvalidInput(
                "only an active claim can be corrected".into(),
            ));
        }
        if !source_is_admissible(EvidenceClass::UserCorrection, current.header.domain) {
            return Err(MemoryError::InvalidInput(
                "a user correction cannot support the target claim domain".into(),
            ));
        }
        validate_claim_activation(
            operation.actor,
            current.header.domain,
            &[EvidenceClass::UserCorrection],
        )?;
        let dependencies = self.active_dependency_closure(RecordRef::Claim(input.target))?;

        let recorded_at_ms = now_ms();
        let evidence_id = EvidenceId::new();
        let replacement_id = ClaimId::new();
        let relation_id = RelationId::new();
        let evidence_header = EvidenceHeader {
            id: evidence_id,
            class: EvidenceClass::UserCorrection,
            captured_by: Actor::User,
            scope: current.header.scope.clone(),
            temporal: input.temporal,
            lifecycle: EvidenceLifecycleTruth::Direct,
            recorded_at_ms,
            operation_id: operation.id,
        };
        let availability = EvidenceAvailabilityRevision {
            evidence_id,
            revision: 1,
            state: RecordState::Active,
            recorded_at_ms,
            operation_id: operation.id,
        };
        let replacement_header = ClaimRevisionHeader {
            id: replacement_id,
            revision: 1,
            previous_revision: None,
            domain: current.header.domain,
            scope: current.header.scope.clone(),
            temporal: input.temporal,
            evidence_ids: vec![evidence_id],
            state: RecordState::Active,
            recorded_at_ms,
            operation_id: operation.id,
        };
        let supersedes_header = SemanticRelationRevisionHeader {
            id: relation_id,
            revision: 1,
            previous_revision: None,
            from: RecordRef::Claim(replacement_id),
            to: RecordRef::Claim(input.target),
            kind: RelationKind::Supersedes,
            scope: current.header.scope.clone(),
            evidence_ids: vec![evidence_id],
            state: RecordState::Active,
            recorded_at_ms,
            operation_id: operation.id,
        };

        let mut batch = self.durable_batch();
        let superseded = self.append_state_revision(
            &mut batch,
            RecordRef::Claim(input.target),
            input.expected_revision,
            RecordState::Superseded,
            operation.id,
            recorded_at_ms,
        )?;
        self.remove_lexical_document(&mut batch, RecordRef::Claim(input.target))?;
        let mut invalidated = Vec::with_capacity(dependencies.len());
        for dependency in dependencies {
            let revision = self.append_state_revision(
                &mut batch,
                dependency.record,
                dependency.expected_revision,
                RecordState::Unsupported,
                operation.id,
                recorded_at_ms,
            )?;
            self.remove_lexical_document(&mut batch, dependency.record)?;
            invalidated.push(revision);
        }

        batch.insert(
            &self.evidence,
            id_key(evidence_id.0),
            encode(&evidence_header)?,
        );
        batch.insert(
            &self.evidence_availability,
            revision_key(evidence_id.0, 1),
            encode(&availability)?,
        );
        batch.insert(
            &self.evidence_heads,
            id_key(evidence_id.0),
            1_u64.to_be_bytes(),
        );
        batch.insert(
            &self.payloads,
            payload_key(RecordRef::Evidence(evidence_id)),
            input.evidence_text.as_bytes(),
        );
        self.insert_lexical_document(
            &mut batch,
            RecordRef::Evidence(evidence_id),
            1,
            current.header.scope.clone(),
            input.temporal,
            &input.evidence_text,
        )?;

        batch.insert(
            &self.claims,
            revision_key(replacement_id.0, 1),
            encode(&replacement_header)?,
        );
        batch.insert(
            &self.claim_heads,
            id_key(replacement_id.0),
            1_u64.to_be_bytes(),
        );
        batch.insert(
            &self.payloads,
            payload_key(RecordRef::Claim(replacement_id)),
            input.replacement_proposition.as_bytes(),
        );
        batch.insert(
            &self.dependencies,
            dependency_key(
                RecordRef::Evidence(evidence_id),
                RecordRef::Claim(replacement_id),
            ),
            [],
        );
        self.insert_lexical_document(
            &mut batch,
            RecordRef::Claim(replacement_id),
            1,
            current.header.scope.clone(),
            input.temporal,
            &input.replacement_proposition,
        )?;

        batch.insert(
            &self.relations,
            revision_key(relation_id.0, 1),
            encode(&supersedes_header)?,
        );
        batch.insert(
            &self.relation_heads,
            id_key(relation_id.0),
            1_u64.to_be_bytes(),
        );
        batch.insert(
            &self.payloads,
            payload_key(RecordRef::SemanticRelation(relation_id)),
            encode(&RelationPayload { qualifier: None })?,
        );
        for source in [
            RecordRef::Claim(replacement_id),
            RecordRef::Claim(input.target),
            RecordRef::Evidence(evidence_id),
        ] {
            batch.insert(
                &self.dependencies,
                dependency_key(source, RecordRef::SemanticRelation(relation_id)),
                [],
            );
        }

        let receipt = CorrectionReceipt {
            evidence: EvidenceReceipt {
                id: evidence_id,
                availability_revision: 1,
            },
            superseded,
            replacement: ClaimReceipt {
                id: replacement_id,
                revision: 1,
                state: RecordState::Active,
            },
            supersedes_relation: RelationReceipt {
                id: relation_id,
                revision: 1,
                state: RecordState::Active,
            },
            invalidated,
        };
        let mut subjects = vec![
            RecordRef::Evidence(evidence_id),
            RecordRef::Claim(input.target),
            RecordRef::Claim(replacement_id),
            RecordRef::SemanticRelation(relation_id),
        ];
        subjects.extend(receipt.invalidated.iter().map(|entry| entry.record));
        let audit = self.new_audit_event(
            operation,
            AuditAction::UserClaimCorrected,
            subjects,
            recorded_at_ms,
        );
        self.insert_audit(&mut batch, &audit)?;
        self.insert_operation(&mut batch, operation.id, digest, &receipt)?;
        commit(batch)?;
        Ok(receipt)
    }

    pub fn create_claim(
        &self,
        operation: OperationContext,
        mut input: NewClaim,
    ) -> MemoryResult<ClaimReceipt> {
        if operation.actor == Actor::Assistant {
            return Err(MemoryError::Unauthorized);
        }
        input.temporal.validate()?;
        input.proposition = validate_text(input.proposition, "claim proposition")?;
        input.evidence_ids.sort();
        input.evidence_ids.dedup();
        if input.evidence_ids.is_empty() {
            return Err(MemoryError::InvalidInput(
                "a claim requires at least one evidence source".into(),
            ));
        }
        if input.evidence_ids.len() > MAX_EVIDENCE_SOURCES {
            return Err(MemoryError::InvalidInput(format!(
                "a claim cannot cite more than {MAX_EVIDENCE_SOURCES} evidence sources"
            )));
        }
        let digest = self.mutation_digest("create_claim", operation.actor, &input)?;
        let _guard = self.write_lock.lock();
        if let Some(receipt) = self.replayed_operation(operation.id, digest)? {
            return Ok(receipt);
        }

        let mut source_classes = Vec::with_capacity(input.evidence_ids.len());
        for evidence_id in &input.evidence_ids {
            let evidence = self
                .inspect_evidence(*evidence_id)?
                .ok_or(MemoryError::SourceUnavailable(*evidence_id))?;
            if evidence.availability.state != RecordState::Active {
                return Err(MemoryError::SourceUnavailable(*evidence_id));
            }
            if evidence.header.scope != input.scope {
                return Err(MemoryError::ScopeMismatch);
            }
            if !source_is_admissible(evidence.header.class, input.domain) {
                return Err(MemoryError::InadmissibleSource {
                    evidence_id: *evidence_id,
                    class: evidence.header.class,
                    domain: input.domain,
                });
            }
            source_classes.push(evidence.header.class);
        }
        validate_claim_activation(operation.actor, input.domain, &source_classes)?;

        let recorded_at_ms = now_ms();
        let id = ClaimId::new();
        let header = ClaimRevisionHeader {
            id,
            revision: 1,
            previous_revision: None,
            domain: input.domain,
            scope: input.scope.clone(),
            temporal: input.temporal,
            evidence_ids: input.evidence_ids.clone(),
            state: RecordState::Active,
            recorded_at_ms,
            operation_id: operation.id,
        };
        let receipt = ClaimReceipt {
            id,
            revision: 1,
            state: RecordState::Active,
        };
        let audit = self.new_audit_event(
            operation,
            AuditAction::ClaimCreated,
            vec![RecordRef::Claim(id)],
            recorded_at_ms,
        );

        let mut batch = self.durable_batch();
        batch.insert(&self.claims, revision_key(id.0, 1), encode(&header)?);
        batch.insert(&self.claim_heads, id_key(id.0), 1_u64.to_be_bytes());
        batch.insert(
            &self.payloads,
            payload_key(RecordRef::Claim(id)),
            input.proposition.as_bytes(),
        );
        for evidence_id in &input.evidence_ids {
            batch.insert(
                &self.dependencies,
                dependency_key(RecordRef::Evidence(*evidence_id), RecordRef::Claim(id)),
                [],
            );
        }
        self.insert_lexical_document(
            &mut batch,
            RecordRef::Claim(id),
            1,
            input.scope,
            input.temporal,
            &input.proposition,
        )?;
        self.insert_audit(&mut batch, &audit)?;
        self.insert_operation(&mut batch, operation.id, digest, &receipt)?;
        commit(batch)?;
        Ok(receipt)
    }

    pub fn inspect_claim(&self, id: ClaimId) -> MemoryResult<Option<ClaimRecord>> {
        let Some(revision) = get_head(&self.claim_heads, id.0)? else {
            return Ok(None);
        };
        let header: ClaimRevisionHeader = get_decoded(&self.claims, revision_key(id.0, revision))?
            .ok_or_else(|| {
                MemoryError::Corrupt(format!("claim {id} is missing revision {revision}"))
            })?;
        let proposition = if header.state == RecordState::Purged {
            None
        } else {
            get_payload(&self.payloads, RecordRef::Claim(id))?
        };
        Ok(Some(ClaimRecord {
            header,
            proposition,
        }))
    }

    pub fn create_semantic_relation(
        &self,
        operation: OperationContext,
        mut input: NewSemanticRelation,
    ) -> MemoryResult<RelationReceipt> {
        if operation.actor == Actor::Assistant {
            return Err(MemoryError::Unauthorized);
        }
        if input.from == input.to {
            return Err(MemoryError::InvalidInput(
                "a semantic relation requires distinct endpoints".into(),
            ));
        }
        if matches!(
            input.from,
            RecordRef::SemanticRelation(_) | RecordRef::Proposal(_) | RecordRef::ProposalReview(_)
        ) || matches!(
            input.to,
            RecordRef::SemanticRelation(_) | RecordRef::Proposal(_) | RecordRef::ProposalReview(_)
        ) {
            return Err(MemoryError::InvalidInput(
                "semantic relations cannot use another relation as an endpoint".into(),
            ));
        }
        let from_kind = semantic_endpoint_kind(input.from).ok_or_else(|| {
            MemoryError::InvalidInput("unsupported semantic relation endpoint".into())
        })?;
        let to_kind = semantic_endpoint_kind(input.to).ok_or_else(|| {
            MemoryError::InvalidInput("unsupported semantic relation endpoint".into())
        })?;
        validate_relation_endpoint_kinds(input.kind, from_kind, to_kind)?;
        if let Some(qualifier) = input.qualifier.take() {
            input.qualifier = Some(validate_text(qualifier, "relation qualifier")?);
        }
        input.evidence_ids.sort();
        input.evidence_ids.dedup();
        if input.evidence_ids.is_empty() {
            return Err(MemoryError::InvalidInput(
                "a semantic relation requires at least one evidence source".into(),
            ));
        }
        if input.evidence_ids.len() > MAX_EVIDENCE_SOURCES {
            return Err(MemoryError::InvalidInput(format!(
                "a semantic relation cannot cite more than {MAX_EVIDENCE_SOURCES} evidence sources"
            )));
        }
        let digest = self.mutation_digest("create_semantic_relation", operation.actor, &input)?;
        let _guard = self.write_lock.lock();
        if let Some(receipt) = self.replayed_operation(operation.id, digest)? {
            return Ok(receipt);
        }

        for endpoint in [input.from, input.to] {
            let (scope, state, _) = self.record_scope_state_revision(endpoint)?;
            if scope != input.scope {
                return Err(MemoryError::ScopeMismatch);
            }
            if state != RecordState::Active {
                return Err(MemoryError::InvalidInput(
                    "semantic relation endpoints must be active".into(),
                ));
            }
        }
        let mut source_classes = Vec::with_capacity(input.evidence_ids.len());
        for evidence_id in &input.evidence_ids {
            let evidence = self
                .inspect_evidence(*evidence_id)?
                .ok_or(MemoryError::SourceUnavailable(*evidence_id))?;
            if evidence.availability.state != RecordState::Active {
                return Err(MemoryError::SourceUnavailable(*evidence_id));
            }
            if evidence.header.scope != input.scope {
                return Err(MemoryError::ScopeMismatch);
            }
            source_classes.push(evidence.header.class);
        }
        validate_relation_activation(operation.actor, &source_classes)?;
        if operation.actor == Actor::User {
            for endpoint in [input.from, input.to] {
                if !self.user_may_activate_endpoint(endpoint)? {
                    return Err(MemoryError::Unauthorized);
                }
            }
        }

        let recorded_at_ms = now_ms();
        let id = RelationId::new();
        let header = SemanticRelationRevisionHeader {
            id,
            revision: 1,
            previous_revision: None,
            from: input.from,
            to: input.to,
            kind: input.kind,
            scope: input.scope,
            evidence_ids: input.evidence_ids.clone(),
            state: RecordState::Active,
            recorded_at_ms,
            operation_id: operation.id,
        };
        let payload = RelationPayload {
            qualifier: input.qualifier,
        };
        let receipt = RelationReceipt {
            id,
            revision: 1,
            state: RecordState::Active,
        };
        let audit = self.new_audit_event(
            operation,
            AuditAction::RelationCreated,
            vec![RecordRef::SemanticRelation(id)],
            recorded_at_ms,
        );

        let mut batch = self.durable_batch();
        batch.insert(&self.relations, revision_key(id.0, 1), encode(&header)?);
        batch.insert(&self.relation_heads, id_key(id.0), 1_u64.to_be_bytes());
        batch.insert(
            &self.payloads,
            payload_key(RecordRef::SemanticRelation(id)),
            encode(&payload)?,
        );
        for source in std::iter::once(input.from)
            .chain(std::iter::once(input.to))
            .chain(input.evidence_ids.iter().copied().map(RecordRef::Evidence))
        {
            batch.insert(
                &self.dependencies,
                dependency_key(source, RecordRef::SemanticRelation(id)),
                [],
            );
        }
        self.insert_audit(&mut batch, &audit)?;
        self.insert_operation(&mut batch, operation.id, digest, &receipt)?;
        commit(batch)?;
        Ok(receipt)
    }

    pub fn inspect_semantic_relation(
        &self,
        id: RelationId,
    ) -> MemoryResult<Option<SemanticRelationRecord>> {
        let Some(revision) = get_head(&self.relation_heads, id.0)? else {
            return Ok(None);
        };
        let header: SemanticRelationRevisionHeader =
            get_decoded(&self.relations, revision_key(id.0, revision))?.ok_or_else(|| {
                MemoryError::Corrupt(format!("relation {id} is missing revision {revision}"))
            })?;
        let payload = if header.state == RecordState::Purged {
            None
        } else {
            get_decoded::<RelationPayload>(
                &self.payloads,
                payload_key(RecordRef::SemanticRelation(id)),
            )?
        };
        Ok(Some(SemanticRelationRecord {
            header,
            qualifier: payload
                .as_ref()
                .and_then(|payload| payload.qualifier.clone()),
            payload_available: payload.is_some(),
        }))
    }

    pub fn put_relation_evaluator(
        &self,
        operation: OperationContext,
        mut input: NewRelationEvaluatorRevision,
    ) -> MemoryResult<RelationEvaluatorReceipt> {
        if operation.actor != Actor::Operator {
            return Err(MemoryError::Unauthorized);
        }
        if input.schema_version == 0 {
            return Err(MemoryError::InvalidInput(
                "relation evaluator schema version must be positive".into(),
            ));
        }
        if input.dimensions.is_empty() || input.dimensions.len() > MAX_RELATION_DIMENSIONS {
            return Err(MemoryError::InvalidInput(format!(
                "relation evaluator must declare 1..={MAX_RELATION_DIMENSIONS} dimensions"
            )));
        }
        let original_dimension_count = input.dimensions.len();
        input.dimensions.sort();
        input.dimensions.dedup();
        if input.dimensions.len() != original_dimension_count {
            return Err(MemoryError::InvalidInput(
                "relation evaluator dimensions must be unique".into(),
            ));
        }

        let digest = self.mutation_digest("put_relation_evaluator", operation.actor, &input)?;
        let _guard = self.write_lock.lock();
        if let Some(receipt) = self.replayed_operation(operation.id, digest)? {
            return Ok(receipt);
        }

        let current_revision = get_head(&self.relation_evaluator_heads, input.id.0)?;
        match (input.expected_revision, current_revision) {
            (None, None) => {}
            (None, Some(actual)) => {
                return Err(MemoryError::RevisionConflict {
                    expected: 0,
                    actual,
                });
            }
            (Some(expected), Some(actual)) if expected == actual => {
                let current: RelationEvaluatorRevisionHeader =
                    get_decoded(&self.relation_evaluators, revision_key(input.id.0, actual))?
                        .ok_or_else(|| {
                            MemoryError::Corrupt(format!(
                                "relation evaluator {} is missing revision {actual}",
                                input.id
                            ))
                        })?;
                if current.state != RecordState::Active {
                    return Err(MemoryError::InvalidInput(
                        "only an active relation evaluator can be revised".into(),
                    ));
                }
            }
            (Some(expected), actual) => {
                return Err(MemoryError::RevisionConflict {
                    expected,
                    actual: actual.unwrap_or(0),
                });
            }
        }

        let revision = current_revision
            .unwrap_or(0)
            .checked_add(1)
            .ok_or_else(|| MemoryError::Corrupt("relation evaluator revision overflow".into()))?;
        let recorded_at_ms = now_ms();
        let header = RelationEvaluatorRevisionHeader {
            id: input.id,
            revision,
            previous_revision: current_revision,
            kind: input.kind,
            schema_version: input.schema_version,
            dimensions: input.dimensions,
            provenance_digest: input.provenance_digest,
            state: RecordState::Active,
            recorded_at_ms,
            operation_id: operation.id,
        };
        let receipt = RelationEvaluatorReceipt {
            pin: RelationEvaluatorPin {
                id: input.id,
                revision,
            },
            state: RecordState::Active,
        };
        let audit = self.new_derived_audit_event(
            operation,
            AuditAction::RelationEvaluatorPut,
            vec![DerivedRecordRef::RelationEvaluator(input.id)],
            recorded_at_ms,
        );
        let mut batch = self.durable_batch();
        batch.insert(
            &self.relation_evaluators,
            revision_key(input.id.0, revision),
            encode(&header)?,
        );
        batch.insert(
            &self.relation_evaluator_heads,
            id_key(input.id.0),
            revision.to_be_bytes(),
        );
        self.insert_audit(&mut batch, &audit)?;
        self.insert_operation(&mut batch, operation.id, digest, &receipt)?;
        commit(batch)?;
        Ok(receipt)
    }

    pub fn inspect_relation_evaluator(
        &self,
        id: RelationEvaluatorId,
        selector: RevisionSelector,
    ) -> MemoryResult<Option<RelationEvaluatorRecord>> {
        let revision = match selector {
            RevisionSelector::Head => match get_head(&self.relation_evaluator_heads, id.0)? {
                Some(revision) => revision,
                None => return Ok(None),
            },
            RevisionSelector::Exact(revision) => revision,
        };
        Ok(get_decoded::<RelationEvaluatorRevisionHeader>(
            &self.relation_evaluators,
            revision_key(id.0, revision),
        )?
        .map(|header| RelationEvaluatorRecord { header }))
    }

    pub fn set_relation_evaluator_state(
        &self,
        operation: OperationContext,
        pin: RelationEvaluatorPin,
        change: DerivedDefinitionStateChange,
    ) -> MemoryResult<RelationEvaluatorStateReceipt> {
        if operation.actor != Actor::Operator {
            return Err(MemoryError::Unauthorized);
        }
        let digest = self.mutation_digest(
            "set_relation_evaluator_state",
            operation.actor,
            &(pin, change),
        )?;
        let _guard = self.write_lock.lock();
        if let Some(receipt) = self.replayed_operation(operation.id, digest)? {
            return Ok(receipt);
        }
        let actual = get_head(&self.relation_evaluator_heads, pin.id.0)?.unwrap_or(0);
        if actual != pin.revision {
            return Err(MemoryError::RevisionConflict {
                expected: pin.revision,
                actual,
            });
        }
        let mut next: RelationEvaluatorRevisionHeader = get_decoded(
            &self.relation_evaluators,
            revision_key(pin.id.0, pin.revision),
        )?
        .ok_or_else(|| {
            MemoryError::Corrupt(format!(
                "relation evaluator {} is missing revision {}",
                pin.id, pin.revision
            ))
        })?;
        let target_state = match change {
            DerivedDefinitionStateChange::Retract => RecordState::Retracted,
            DerivedDefinitionStateChange::Purge => RecordState::Purged,
        };
        if next.state == RecordState::Purged || next.state == target_state {
            return Err(MemoryError::InvalidInput(
                "relation evaluator already reached the requested terminal state".into(),
            ));
        }
        let revision = pin
            .revision
            .checked_add(1)
            .ok_or_else(|| MemoryError::Corrupt("relation evaluator revision overflow".into()))?;
        let recorded_at_ms = now_ms();
        next.revision = revision;
        next.previous_revision = Some(pin.revision);
        next.state = target_state;
        next.recorded_at_ms = recorded_at_ms;
        next.operation_id = operation.id;
        let mut batch = self.durable_batch();
        let mut purged_relation_signals = Vec::new();
        let mut purged_activation_traces = Vec::new();
        if change == DerivedDefinitionStateChange::Purge {
            for signal in
                self.relation_signal_pins_for_owner(&self.relation_signals_by_evaluator, pin.id.0)?
            {
                purged_relation_signals.push(self.append_relation_signal_state_revision(
                    &mut batch,
                    signal,
                    RecordState::Purged,
                    operation.id,
                    recorded_at_ms,
                )?);
            }
            purged_activation_traces = self
                .activation_trace_ids_for_owner(&self.activation_traces_by_evaluator, pin.id.0)?;
            for trace_id in &purged_activation_traces {
                batch.remove(&self.activation_trace_payloads, id_key(trace_id.0));
            }
        }
        batch.insert(
            &self.relation_evaluators,
            revision_key(pin.id.0, revision),
            encode(&next)?,
        );
        batch.insert(
            &self.relation_evaluator_heads,
            id_key(pin.id.0),
            revision.to_be_bytes(),
        );
        let receipt = RelationEvaluatorStateReceipt {
            evaluator: RelationEvaluatorReceipt {
                pin: RelationEvaluatorPin {
                    id: pin.id,
                    revision,
                },
                state: target_state,
            },
            purged_relation_signals,
            purged_activation_traces,
        };
        let mut derived_subjects = vec![DerivedRecordRef::RelationEvaluator(pin.id)];
        derived_subjects.extend(
            receipt
                .purged_relation_signals
                .iter()
                .map(|signal| DerivedRecordRef::RelationSignal(signal.id)),
        );
        derived_subjects.extend(
            receipt
                .purged_activation_traces
                .iter()
                .copied()
                .map(DerivedRecordRef::ActivationTrace),
        );
        let audit = self.new_derived_audit_event(
            operation,
            AuditAction::RelationEvaluatorStateChanged,
            derived_subjects,
            recorded_at_ms,
        );
        self.insert_audit(&mut batch, &audit)?;
        self.insert_operation(&mut batch, operation.id, digest, &receipt)?;
        commit(batch)?;
        Ok(receipt)
    }

    pub fn put_relation_profile(
        &self,
        operation: OperationContext,
        mut input: NewRelationProfileRevision,
    ) -> MemoryResult<RelationProfileReceipt> {
        if operation.actor != Actor::Operator {
            return Err(MemoryError::Unauthorized);
        }
        if input.heads.is_empty() || input.heads.len() > MAX_RELATION_HEADS {
            return Err(MemoryError::InvalidInput(format!(
                "relation profile must declare 1..={MAX_RELATION_HEADS} heads"
            )));
        }
        let original_head_count = input.heads.len();
        input.heads.sort_by_key(|head| head.dimension);
        input.heads.dedup_by_key(|head| head.dimension);
        if input.heads.len() != original_head_count {
            return Err(MemoryError::InvalidInput(
                "relation profile head dimensions must be unique".into(),
            ));
        }
        if input
            .heads
            .iter()
            .any(|head| i64::from(head.weight_micros).abs() > i64::from(RELATION_FIXED_POINT_SCALE))
        {
            return Err(MemoryError::InvalidInput(format!(
                "relation profile weights must be within +/-{RELATION_FIXED_POINT_SCALE}"
            )));
        }

        let digest = self.mutation_digest("put_relation_profile", operation.actor, &input)?;
        let _guard = self.write_lock.lock();
        if let Some(receipt) = self.replayed_operation(operation.id, digest)? {
            return Ok(receipt);
        }

        let evaluator = self
            .inspect_relation_evaluator(input.evaluator.id, RevisionSelector::Head)?
            .ok_or_else(|| MemoryError::InvalidInput("relation evaluator does not exist".into()))?;
        if evaluator.header.revision != input.evaluator.revision {
            return Err(MemoryError::RevisionConflict {
                expected: input.evaluator.revision,
                actual: evaluator.header.revision,
            });
        }
        if evaluator.header.state != RecordState::Active {
            return Err(MemoryError::InvalidInput(
                "relation evaluator is not active".into(),
            ));
        }
        if input
            .heads
            .iter()
            .any(|head| !evaluator.header.dimensions.contains(&head.dimension))
        {
            return Err(MemoryError::InvalidInput(
                "relation profile head is not declared by its evaluator".into(),
            ));
        }

        let current_revision = get_head(&self.relation_profile_heads, input.id.0)?;
        match (input.expected_revision, current_revision) {
            (None, None) => {}
            (None, Some(actual)) => {
                return Err(MemoryError::RevisionConflict {
                    expected: 0,
                    actual,
                });
            }
            (Some(expected), Some(actual)) if expected == actual => {
                let current: RelationProfileRevisionHeader =
                    get_decoded(&self.relation_profiles, revision_key(input.id.0, actual))?
                        .ok_or_else(|| {
                            MemoryError::Corrupt(format!(
                                "relation profile {} is missing revision {actual}",
                                input.id
                            ))
                        })?;
                if current.state != RecordState::Active {
                    return Err(MemoryError::InvalidInput(
                        "only an active relation profile can be revised".into(),
                    ));
                }
            }
            (Some(expected), actual) => {
                return Err(MemoryError::RevisionConflict {
                    expected,
                    actual: actual.unwrap_or(0),
                });
            }
        }

        let revision = current_revision
            .unwrap_or(0)
            .checked_add(1)
            .ok_or_else(|| MemoryError::Corrupt("relation profile revision overflow".into()))?;
        let recorded_at_ms = now_ms();
        let header = RelationProfileRevisionHeader {
            id: input.id,
            revision,
            previous_revision: current_revision,
            evaluator: input.evaluator,
            heads: input.heads,
            provenance_digest: input.provenance_digest,
            state: RecordState::Active,
            recorded_at_ms,
            operation_id: operation.id,
        };
        let receipt = RelationProfileReceipt {
            pin: RelationProfilePin {
                id: input.id,
                revision,
            },
            state: RecordState::Active,
            availability: RelationProfileAvailability::Available,
        };
        let audit = self.new_derived_audit_event(
            operation,
            AuditAction::RelationProfilePut,
            vec![DerivedRecordRef::RelationProfile(input.id)],
            recorded_at_ms,
        );
        let mut batch = self.durable_batch();
        batch.insert(
            &self.relation_profiles,
            revision_key(input.id.0, revision),
            encode(&header)?,
        );
        batch.insert(
            &self.relation_profile_heads,
            id_key(input.id.0),
            revision.to_be_bytes(),
        );
        self.insert_audit(&mut batch, &audit)?;
        self.insert_operation(&mut batch, operation.id, digest, &receipt)?;
        commit(batch)?;
        Ok(receipt)
    }

    pub fn inspect_relation_profile(
        &self,
        id: RelationProfileId,
        selector: RevisionSelector,
    ) -> MemoryResult<Option<RelationProfileRecord>> {
        let revision = match selector {
            RevisionSelector::Head => match get_head(&self.relation_profile_heads, id.0)? {
                Some(revision) => revision,
                None => return Ok(None),
            },
            RevisionSelector::Exact(revision) => revision,
        };
        let Some(header): Option<RelationProfileRevisionHeader> =
            get_decoded(&self.relation_profiles, revision_key(id.0, revision))?
        else {
            return Ok(None);
        };
        let availability = self.relation_profile_availability(&header)?;
        Ok(Some(RelationProfileRecord {
            header,
            availability,
        }))
    }

    pub fn set_relation_profile_state(
        &self,
        operation: OperationContext,
        pin: RelationProfilePin,
        change: DerivedDefinitionStateChange,
    ) -> MemoryResult<RelationProfileStateReceipt> {
        if operation.actor != Actor::Operator {
            return Err(MemoryError::Unauthorized);
        }
        let digest = self.mutation_digest(
            "set_relation_profile_state",
            operation.actor,
            &(pin, change),
        )?;
        let _guard = self.write_lock.lock();
        if let Some(receipt) = self.replayed_operation(operation.id, digest)? {
            return Ok(receipt);
        }
        let actual = get_head(&self.relation_profile_heads, pin.id.0)?.unwrap_or(0);
        if actual != pin.revision {
            return Err(MemoryError::RevisionConflict {
                expected: pin.revision,
                actual,
            });
        }
        let mut next: RelationProfileRevisionHeader = get_decoded(
            &self.relation_profiles,
            revision_key(pin.id.0, pin.revision),
        )?
        .ok_or_else(|| {
            MemoryError::Corrupt(format!(
                "relation profile {} is missing revision {}",
                pin.id, pin.revision
            ))
        })?;
        let target_state = match change {
            DerivedDefinitionStateChange::Retract => RecordState::Retracted,
            DerivedDefinitionStateChange::Purge => RecordState::Purged,
        };
        if next.state == RecordState::Purged || next.state == target_state {
            return Err(MemoryError::InvalidInput(
                "relation profile already reached the requested terminal state".into(),
            ));
        }
        let revision = pin
            .revision
            .checked_add(1)
            .ok_or_else(|| MemoryError::Corrupt("relation profile revision overflow".into()))?;
        let recorded_at_ms = now_ms();
        next.revision = revision;
        next.previous_revision = Some(pin.revision);
        next.state = target_state;
        next.recorded_at_ms = recorded_at_ms;
        next.operation_id = operation.id;
        let mut batch = self.durable_batch();
        let mut purged_relation_signals = Vec::new();
        let mut purged_activation_traces = Vec::new();
        if change == DerivedDefinitionStateChange::Purge {
            for signal in
                self.relation_signal_pins_for_owner(&self.relation_signals_by_profile, pin.id.0)?
            {
                purged_relation_signals.push(self.append_relation_signal_state_revision(
                    &mut batch,
                    signal,
                    RecordState::Purged,
                    operation.id,
                    recorded_at_ms,
                )?);
            }
            purged_activation_traces =
                self.activation_trace_ids_for_owner(&self.activation_traces_by_profile, pin.id.0)?;
            for trace_id in &purged_activation_traces {
                batch.remove(&self.activation_trace_payloads, id_key(trace_id.0));
            }
        }
        batch.insert(
            &self.relation_profiles,
            revision_key(pin.id.0, revision),
            encode(&next)?,
        );
        batch.insert(
            &self.relation_profile_heads,
            id_key(pin.id.0),
            revision.to_be_bytes(),
        );
        let receipt = RelationProfileStateReceipt {
            profile: RelationProfileReceipt {
                pin: RelationProfilePin {
                    id: pin.id,
                    revision,
                },
                state: target_state,
                availability: RelationProfileAvailability::Inactive,
            },
            purged_relation_signals,
            purged_activation_traces,
        };
        let mut derived_subjects = vec![DerivedRecordRef::RelationProfile(pin.id)];
        derived_subjects.extend(
            receipt
                .purged_relation_signals
                .iter()
                .map(|signal| DerivedRecordRef::RelationSignal(signal.id)),
        );
        derived_subjects.extend(
            receipt
                .purged_activation_traces
                .iter()
                .copied()
                .map(DerivedRecordRef::ActivationTrace),
        );
        let audit = self.new_derived_audit_event(
            operation,
            AuditAction::RelationProfileStateChanged,
            derived_subjects,
            recorded_at_ms,
        );
        self.insert_audit(&mut batch, &audit)?;
        self.insert_operation(&mut batch, operation.id, digest, &receipt)?;
        commit(batch)?;
        Ok(receipt)
    }

    fn relation_profile_availability(
        &self,
        header: &RelationProfileRevisionHeader,
    ) -> MemoryResult<RelationProfileAvailability> {
        if header.state != RecordState::Active {
            return Ok(RelationProfileAvailability::Inactive);
        }
        let Some(evaluator) =
            self.inspect_relation_evaluator(header.evaluator.id, RevisionSelector::Head)?
        else {
            return Ok(RelationProfileAvailability::UnavailableEvaluator);
        };
        if evaluator.header.revision != header.evaluator.revision
            || evaluator.header.state != RecordState::Active
        {
            return Ok(RelationProfileAvailability::StaleEvaluator);
        }
        Ok(RelationProfileAvailability::Available)
    }

    pub fn put_relation_signals(
        &self,
        operation: OperationContext,
        mut input: NewRelationSignalBatch,
    ) -> MemoryResult<RelationSignalBatchReceipt> {
        if operation.actor != Actor::System {
            return Err(MemoryError::Unauthorized);
        }
        if input.signals.is_empty() || input.signals.len() > MAX_RELATION_SIGNAL_BATCH {
            return Err(MemoryError::InvalidInput(format!(
                "relation signal batch must contain 1..={MAX_RELATION_SIGNAL_BATCH} signals"
            )));
        }
        let mut pairs = BTreeSet::new();
        for signal in &mut input.signals {
            if signal.from.record == signal.to.record {
                return Err(MemoryError::InvalidInput(
                    "relation signal endpoints must be distinct".into(),
                ));
            }
            if !pairs.insert((signal.from.record, signal.to.record)) {
                return Err(MemoryError::InvalidInput(
                    "relation signal batch pairs must be unique".into(),
                ));
            }
            if signal.scores.is_empty() || signal.scores.len() > MAX_RELATION_DIMENSIONS {
                return Err(MemoryError::InvalidInput(format!(
                    "relation signal must contain 1..={MAX_RELATION_DIMENSIONS} dimension scores"
                )));
            }
            let original_score_count = signal.scores.len();
            signal.scores.sort_by_key(|score| score.dimension);
            signal.scores.dedup_by_key(|score| score.dimension);
            if signal.scores.len() != original_score_count {
                return Err(MemoryError::InvalidInput(
                    "relation signal score dimensions must be unique".into(),
                ));
            }
            if signal.scores.iter().any(|score| {
                i64::from(score.score_micros).abs() > i64::from(RELATION_FIXED_POINT_SCALE)
            }) {
                return Err(MemoryError::InvalidInput(format!(
                    "relation signal scores must be within +/-{RELATION_FIXED_POINT_SCALE}"
                )));
            }
        }

        let digest = self.mutation_digest("put_relation_signals", operation.actor, &input)?;
        let _guard = self.write_lock.lock();
        if let Some(receipt) = self.replayed_operation(operation.id, digest)? {
            return Ok(receipt);
        }

        let evaluator = self
            .inspect_relation_evaluator(input.evaluator.id, RevisionSelector::Head)?
            .ok_or_else(|| MemoryError::InvalidInput("relation evaluator does not exist".into()))?;
        if evaluator.header.revision != input.evaluator.revision {
            return Err(MemoryError::RevisionConflict {
                expected: input.evaluator.revision,
                actual: evaluator.header.revision,
            });
        }
        if evaluator.header.state != RecordState::Active {
            return Err(MemoryError::InvalidInput(
                "relation evaluator is not active".into(),
            ));
        }
        let profile = self
            .inspect_relation_profile(input.profile.id, RevisionSelector::Head)?
            .ok_or_else(|| MemoryError::InvalidInput("relation profile does not exist".into()))?;
        if profile.header.revision != input.profile.revision {
            return Err(MemoryError::RevisionConflict {
                expected: input.profile.revision,
                actual: profile.header.revision,
            });
        }
        if profile.availability != RelationProfileAvailability::Available
            || profile.header.evaluator != input.evaluator
        {
            return Err(MemoryError::InvalidInput(
                "relation profile and evaluator pins are not jointly current".into(),
            ));
        }
        let profile_dimensions: BTreeSet<_> = profile
            .header
            .heads
            .iter()
            .map(|head| head.dimension)
            .collect();

        struct PreparedSignal {
            header: RelationSignalRevisionHeader,
            payload: RelationSignalPayload,
            is_new: bool,
            previous_evaluator_id: Option<RelationEvaluatorId>,
        }
        let recorded_at_ms = now_ms();
        let mut prepared = Vec::with_capacity(input.signals.len());
        for signal in input.signals {
            if signal
                .scores
                .iter()
                .any(|score| !profile_dimensions.contains(&score.dimension))
            {
                return Err(MemoryError::InvalidInput(
                    "relation signal dimension is not enabled by its profile".into(),
                ));
            }
            self.require_current_pin(signal.from)?;
            self.require_current_pin(signal.to)?;

            let pair_key =
                relation_signal_pair_key(input.profile.id, signal.from.record, signal.to.record);
            let existing_id: Option<RelationSignalId> =
                get_decoded(&self.relation_signal_pairs, pair_key)?;
            let (id, previous_revision, is_new, previous_evaluator_id) =
                match (existing_id, signal.expected_signal) {
                    (None, None) => (RelationSignalId::new(), None, true, None),
                    (Some(id), Some(expected)) if id == expected.id => {
                        let actual =
                            get_head(&self.relation_signal_heads, id.0)?.ok_or_else(|| {
                                MemoryError::Corrupt(format!(
                                    "relation signal pair index points to missing signal {id}"
                                ))
                            })?;
                        if actual != expected.revision {
                            return Err(MemoryError::RevisionConflict {
                                expected: expected.revision,
                                actual,
                            });
                        }
                        let current: RelationSignalRevisionHeader =
                            get_decoded(&self.relation_signals, revision_key(id.0, actual))?
                                .ok_or_else(|| {
                                    MemoryError::Corrupt(format!(
                                        "relation signal {id} is missing revision {actual}"
                                    ))
                                })?;
                        if current.state != RecordState::Active {
                            return Err(MemoryError::InvalidInput(
                                "a purged relation signal cannot be revised".into(),
                            ));
                        }
                        if current.profile.id != input.profile.id
                            || current.from.record != signal.from.record
                            || current.to.record != signal.to.record
                        {
                            return Err(MemoryError::Corrupt(
                                "relation signal pair index does not match its current header"
                                    .into(),
                            ));
                        }
                        (id, Some(actual), false, Some(current.evaluator.id))
                    }
                    (Some(id), Some(expected)) => {
                        return Err(MemoryError::InvalidInput(format!(
                            "relation signal pair belongs to {id}, not {}",
                            expected.id
                        )));
                    }
                    (Some(id), None) => {
                        let actual = get_head(&self.relation_signal_heads, id.0)?.unwrap_or(0);
                        return Err(MemoryError::RevisionConflict {
                            expected: 0,
                            actual,
                        });
                    }
                    (None, Some(expected)) => {
                        return Err(MemoryError::RevisionConflict {
                            expected: expected.revision,
                            actual: 0,
                        });
                    }
                };
            let revision = previous_revision
                .unwrap_or(0)
                .checked_add(1)
                .ok_or_else(|| MemoryError::Corrupt("relation signal revision overflow".into()))?;
            prepared.push(PreparedSignal {
                header: RelationSignalRevisionHeader {
                    id,
                    revision,
                    previous_revision,
                    evaluator: input.evaluator,
                    profile: input.profile,
                    from: signal.from,
                    to: signal.to,
                    state: RecordState::Active,
                    recorded_at_ms,
                    operation_id: operation.id,
                },
                payload: RelationSignalPayload {
                    revision,
                    scores: signal.scores,
                    provenance_digest: signal.provenance_digest,
                },
                is_new,
                previous_evaluator_id,
            });
        }

        let receipt = RelationSignalBatchReceipt {
            signals: prepared
                .iter()
                .map(|entry| RelationSignalReceipt {
                    pin: RelationSignalPin {
                        id: entry.header.id,
                        revision: entry.header.revision,
                    },
                    state: entry.header.state,
                })
                .collect(),
        };
        let audit = self.new_derived_audit_event(
            operation,
            AuditAction::RelationSignalsPut,
            prepared
                .iter()
                .map(|entry| DerivedRecordRef::RelationSignal(entry.header.id))
                .collect(),
            recorded_at_ms,
        );
        let mut batch = self.durable_batch();
        for entry in &prepared {
            batch.insert(
                &self.relation_signals,
                revision_key(entry.header.id.0, entry.header.revision),
                encode(&entry.header)?,
            );
            batch.insert(
                &self.relation_signal_heads,
                id_key(entry.header.id.0),
                entry.header.revision.to_be_bytes(),
            );
            batch.insert(
                &self.relation_signal_payloads,
                id_key(entry.header.id.0),
                encode(&entry.payload)?,
            );
            if entry.is_new {
                batch.insert(
                    &self.relation_signal_pairs,
                    relation_signal_pair_key(
                        entry.header.profile.id,
                        entry.header.from.record,
                        entry.header.to.record,
                    ),
                    encode(&entry.header.id)?,
                );
                for record in [entry.header.from.record, entry.header.to.record] {
                    batch.insert(
                        &self.relation_signals_by_record,
                        relation_signal_record_index_key(record, entry.header.id),
                        [],
                    );
                }
                batch.insert(
                    &self.relation_signals_by_profile,
                    relation_signal_owner_index_key(entry.header.profile.id.0, entry.header.id),
                    [],
                );
            }
            if entry
                .previous_evaluator_id
                .is_some_and(|id| id != entry.header.evaluator.id)
            {
                batch.remove(
                    &self.relation_signals_by_evaluator,
                    relation_signal_owner_index_key(
                        entry.previous_evaluator_id.expect("checked as present").0,
                        entry.header.id,
                    ),
                );
            }
            batch.insert(
                &self.relation_signals_by_evaluator,
                relation_signal_owner_index_key(entry.header.evaluator.id.0, entry.header.id),
                [],
            );
        }
        self.insert_audit(&mut batch, &audit)?;
        self.insert_operation(&mut batch, operation.id, digest, &receipt)?;
        commit(batch)?;
        Ok(receipt)
    }

    pub fn inspect_relation_signal(
        &self,
        id: RelationSignalId,
        selector: RevisionSelector,
    ) -> MemoryResult<Option<RelationSignalRecord>> {
        let head_revision = get_head(&self.relation_signal_heads, id.0)?;
        let revision = match selector {
            RevisionSelector::Head => match head_revision {
                Some(revision) => revision,
                None => return Ok(None),
            },
            RevisionSelector::Exact(revision) => revision,
        };
        let Some(header): Option<RelationSignalRevisionHeader> =
            get_decoded(&self.relation_signals, revision_key(id.0, revision))?
        else {
            return Ok(None);
        };
        let payload: Option<RelationSignalPayload> =
            get_decoded(&self.relation_signal_payloads, id_key(id.0))?;
        let payload = payload.filter(|payload| payload.revision == revision);
        let availability = self.relation_signal_availability(&header, payload.as_ref())?;
        Ok(Some(RelationSignalRecord {
            header,
            scores: payload.as_ref().map(|payload| payload.scores.clone()),
            provenance_digest: payload.map(|payload| payload.provenance_digest),
            availability,
        }))
    }

    fn relation_signal_availability(
        &self,
        header: &RelationSignalRevisionHeader,
        payload: Option<&RelationSignalPayload>,
    ) -> MemoryResult<RelationSignalAvailability> {
        if header.state == RecordState::Purged {
            return Ok(RelationSignalAvailability::Unavailable(
                RelationSignalUnavailableReason::PayloadPurged,
            ));
        }
        if payload.is_none() {
            return Ok(RelationSignalAvailability::Unavailable(
                RelationSignalUnavailableReason::HistoricalPayloadUnavailable,
            ));
        }
        let Some(evaluator) =
            self.inspect_relation_evaluator(header.evaluator.id, RevisionSelector::Head)?
        else {
            return Ok(RelationSignalAvailability::Unavailable(
                RelationSignalUnavailableReason::MissingDependency,
            ));
        };
        if evaluator.header.state == RecordState::Purged {
            return Ok(RelationSignalAvailability::Unavailable(
                RelationSignalUnavailableReason::EvaluatorPurged,
            ));
        }
        if evaluator.header.state != RecordState::Active {
            return Ok(RelationSignalAvailability::Stale(
                RelationSignalStaleReason::EvaluatorInactive,
            ));
        }
        if evaluator.header.revision != header.evaluator.revision {
            return Ok(RelationSignalAvailability::Stale(
                RelationSignalStaleReason::EvaluatorAdvanced,
            ));
        }
        let Some(profile) =
            self.inspect_relation_profile(header.profile.id, RevisionSelector::Head)?
        else {
            return Ok(RelationSignalAvailability::Unavailable(
                RelationSignalUnavailableReason::MissingDependency,
            ));
        };
        if profile.header.state == RecordState::Purged {
            return Ok(RelationSignalAvailability::Unavailable(
                RelationSignalUnavailableReason::ProfilePurged,
            ));
        }
        if profile.header.state != RecordState::Active {
            return Ok(RelationSignalAvailability::Stale(
                RelationSignalStaleReason::ProfileInactive,
            ));
        }
        if profile.header.revision != header.profile.revision {
            return Ok(RelationSignalAvailability::Stale(
                RelationSignalStaleReason::ProfileAdvanced,
            ));
        }
        for source in [header.from, header.to] {
            match self.record_scope_state_revision(source.record) {
                Ok((_, RecordState::Purged, _)) => {
                    return Ok(RelationSignalAvailability::Unavailable(
                        RelationSignalUnavailableReason::SourcePurged,
                    ));
                }
                Ok((_, state, _)) if state != RecordState::Active => {
                    return Ok(RelationSignalAvailability::Stale(
                        RelationSignalStaleReason::SourceInactive,
                    ));
                }
                Ok((_, _, revision)) if revision != source.revision => {
                    return Ok(RelationSignalAvailability::Stale(
                        RelationSignalStaleReason::SourceAdvanced,
                    ));
                }
                Ok(_) => {}
                Err(MemoryError::NotFound(_)) => {
                    return Ok(RelationSignalAvailability::Unavailable(
                        RelationSignalUnavailableReason::MissingDependency,
                    ));
                }
                Err(error) => return Err(error),
            }
        }
        Ok(RelationSignalAvailability::Available)
    }

    pub fn shadow_activate(
        &self,
        operation: OperationContext,
        mut input: ShadowActivationRequest,
    ) -> MemoryResult<ActivationTrace> {
        if operation.actor != Actor::System {
            return Err(MemoryError::Unauthorized);
        }
        if input.candidates.is_empty() || input.candidates.len() > MAX_ACTIVATION_CANDIDATES {
            return Err(MemoryError::InvalidInput(format!(
                "shadow activation must contain 1..={MAX_ACTIVATION_CANDIDATES} candidates"
            )));
        }
        let candidate_count = input.candidates.len();
        let pair_count = candidate_count
            .checked_mul(candidate_count.saturating_sub(1))
            .ok_or_else(|| {
                MemoryError::InvalidInput("activation candidate pair overflow".into())
            })?;
        if pair_count > MAX_RELATION_CANDIDATE_PAIRS {
            return Err(MemoryError::InvalidInput(format!(
                "shadow activation cannot inspect more than {MAX_RELATION_CANDIDATE_PAIRS} directed candidate pairs"
            )));
        }
        let unique: BTreeSet<_> = input.candidates.iter().copied().collect();
        if unique.len() != input.candidates.len() {
            return Err(MemoryError::InvalidInput(
                "shadow activation candidates must be unique".into(),
            ));
        }

        let _guard = self.write_lock.lock();
        let recall_case = self
            .inspect_recall_case(input.recall_case.id)?
            .ok_or_else(|| MemoryError::InvalidInput("recall case does not exist".into()))?;
        if recall_case.revision != input.recall_case.revision {
            return Err(MemoryError::RevisionConflict {
                expected: input.recall_case.revision,
                actual: recall_case.revision,
            });
        }
        let baseline_by_pin: BTreeMap<_, _> = recall_case
            .candidates
            .iter()
            .map(|candidate| {
                (
                    RecordRevisionPin {
                        record: candidate.record,
                        revision: candidate.revision,
                    },
                    candidate.rank,
                )
            })
            .collect();
        if input
            .candidates
            .iter()
            .any(|candidate| !baseline_by_pin.contains_key(candidate))
        {
            return Err(MemoryError::InvalidInput(
                "shadow activation candidates must be exact pins from the recall case".into(),
            ));
        }
        input.candidates.sort_by_key(|candidate| {
            (
                baseline_by_pin.get(candidate).copied().unwrap_or(u32::MAX),
                *candidate,
            )
        });

        let digest = self.mutation_digest("shadow_activate", operation.actor, &input)?;
        if let Some(receipt) =
            self.replayed_operation::<StoredActivationTraceReceipt>(operation.id, digest)?
        {
            return self.inspect_activation_trace(receipt.id)?.ok_or_else(|| {
                MemoryError::InvalidInput("activation trace payload is no longer available".into())
            });
        }
        let evaluator = self
            .inspect_relation_evaluator(input.evaluator.id, RevisionSelector::Head)?
            .ok_or_else(|| MemoryError::InvalidInput("relation evaluator does not exist".into()))?;
        if evaluator.header.revision != input.evaluator.revision
            || evaluator.header.state != RecordState::Active
        {
            return Err(MemoryError::InvalidInput(
                "shadow activation evaluator pin is not current and active".into(),
            ));
        }
        let profile = self
            .inspect_relation_profile(input.profile.id, RevisionSelector::Head)?
            .ok_or_else(|| MemoryError::InvalidInput("relation profile does not exist".into()))?;
        if profile.header.revision != input.profile.revision
            || profile.availability != RelationProfileAvailability::Available
            || profile.header.evaluator != input.evaluator
        {
            return Err(MemoryError::InvalidInput(
                "shadow activation profile and evaluator pins are not jointly current".into(),
            ));
        }
        for candidate in &input.candidates {
            self.require_current_pin(*candidate)?;
        }

        let weights: BTreeMap<_, _> = profile
            .header
            .heads
            .iter()
            .map(|head| (head.dimension, i64::from(head.weight_micros)))
            .collect();
        let mut totals: BTreeMap<RecordRevisionPin, i64> = input
            .candidates
            .iter()
            .copied()
            .map(|candidate| (candidate, 0))
            .collect();
        let mut contributions = Vec::new();
        for from in &input.candidates {
            for to in &input.candidates {
                if from == to {
                    continue;
                }
                let Some(signal_id): Option<RelationSignalId> = get_decoded(
                    &self.relation_signal_pairs,
                    relation_signal_pair_key(input.profile.id, from.record, to.record),
                )?
                else {
                    continue;
                };
                let Some(signal) =
                    self.inspect_relation_signal(signal_id, RevisionSelector::Head)?
                else {
                    return Err(MemoryError::Corrupt(format!(
                        "relation signal pair index points to missing signal {signal_id}"
                    )));
                };
                if signal.availability != RelationSignalAvailability::Available
                    || signal.header.evaluator != input.evaluator
                    || signal.header.profile != input.profile
                    || signal.header.from != *from
                    || signal.header.to != *to
                {
                    continue;
                }
                let mut weighted_numerator = 0_i64;
                for score in signal.scores.as_deref().unwrap_or_default() {
                    let Some(weight) = weights.get(&score.dimension) else {
                        continue;
                    };
                    let product = i64::from(score.score_micros)
                        .checked_mul(*weight)
                        .ok_or_else(|| {
                            MemoryError::InvalidInput(
                                "relation activation multiplication overflow".into(),
                            )
                        })?;
                    weighted_numerator =
                        weighted_numerator.checked_add(product).ok_or_else(|| {
                            MemoryError::InvalidInput(
                                "relation activation head sum overflow".into(),
                            )
                        })?;
                }
                let weighted_score_micros =
                    weighted_numerator / i64::from(RELATION_FIXED_POINT_SCALE);
                let target_total = totals.get_mut(to).ok_or_else(|| {
                    MemoryError::Corrupt("activation target was not initialized".into())
                })?;
                *target_total =
                    target_total
                        .checked_add(weighted_score_micros)
                        .ok_or_else(|| {
                            MemoryError::InvalidInput("relation activation total overflow".into())
                        })?;
                contributions.push(ActivationContribution {
                    signal: RelationSignalPin {
                        id: signal.header.id,
                        revision: signal.header.revision,
                    },
                    from: *from,
                    to: *to,
                    weighted_score_micros,
                });
                if contributions.len() > MAX_ACTIVATION_TRACE_CONTRIBUTIONS {
                    return Err(MemoryError::InvalidInput(format!(
                        "activation trace cannot contain more than {MAX_ACTIVATION_TRACE_CONTRIBUTIONS} contributions"
                    )));
                }
            }
        }

        let mut shadow_order = input.candidates.clone();
        shadow_order.sort_by(|left, right| {
            totals
                .get(right)
                .copied()
                .unwrap_or_default()
                .cmp(&totals.get(left).copied().unwrap_or_default())
                .then_with(|| {
                    baseline_by_pin
                        .get(left)
                        .copied()
                        .unwrap_or(u32::MAX)
                        .cmp(&baseline_by_pin.get(right).copied().unwrap_or(u32::MAX))
                })
                .then_with(|| left.cmp(right))
        });
        let shadow_ranks: BTreeMap<_, _> = shadow_order
            .into_iter()
            .enumerate()
            .map(|(rank, candidate)| (candidate, rank as u32))
            .collect();
        let candidates = input
            .candidates
            .iter()
            .map(|candidate| ActivationCandidateTrace {
                candidate: *candidate,
                baseline_rank: baseline_by_pin.get(candidate).copied().unwrap_or(u32::MAX),
                activation_score_micros: totals.get(candidate).copied().unwrap_or_default(),
                shadow_rank: shadow_ranks.get(candidate).copied().unwrap_or(u32::MAX),
            })
            .collect();
        let recorded_at_ms = now_ms();
        let trace = ActivationTrace {
            id: ActivationTraceId::new(),
            revision: 1,
            era_id: self.era_id,
            operation_id: operation.id,
            recall_case: input.recall_case,
            evaluator: input.evaluator,
            profile: input.profile,
            input_digest: self.index_hash(0x21, &encode(&input)?),
            candidates,
            contributions,
            recorded_at_ms,
        };
        let audit = self.new_derived_audit_event(
            operation,
            AuditAction::ShadowActivationRecorded,
            vec![DerivedRecordRef::ActivationTrace(trace.id)],
            recorded_at_ms,
        );
        let mut batch = self.durable_batch();
        let header = ActivationTraceHeader {
            id: trace.id,
            revision: trace.revision,
            era_id: trace.era_id,
            operation_id: trace.operation_id,
            recall_case: trace.recall_case,
            evaluator: trace.evaluator,
            profile: trace.profile,
            input_digest: trace.input_digest,
            recorded_at_ms: trace.recorded_at_ms,
        };
        let payload = ActivationTracePayload {
            candidates: trace.candidates.clone(),
            contributions: trace.contributions.clone(),
        };
        batch.insert(
            &self.activation_traces,
            id_key(trace.id.0),
            encode(&header)?,
        );
        batch.insert(
            &self.activation_trace_payloads,
            id_key(trace.id.0),
            encode(&payload)?,
        );
        for candidate in &trace.candidates {
            batch.insert(
                &self.activation_traces_by_record,
                activation_trace_record_index_key(candidate.candidate.record, trace.id),
                [],
            );
        }
        batch.insert(
            &self.activation_traces_by_evaluator,
            activation_trace_owner_index_key(trace.evaluator.id.0, trace.id),
            [],
        );
        batch.insert(
            &self.activation_traces_by_profile,
            activation_trace_owner_index_key(trace.profile.id.0, trace.id),
            [],
        );
        self.insert_audit(&mut batch, &audit)?;
        self.insert_operation(
            &mut batch,
            operation.id,
            digest,
            &StoredActivationTraceReceipt { id: trace.id },
        )?;
        commit(batch)?;
        Ok(trace)
    }

    pub fn inspect_activation_trace(
        &self,
        id: ActivationTraceId,
    ) -> MemoryResult<Option<ActivationTrace>> {
        let Some(header): Option<ActivationTraceHeader> =
            get_decoded(&self.activation_traces, id_key(id.0))?
        else {
            return Ok(None);
        };
        let Some(payload): Option<ActivationTracePayload> =
            get_decoded(&self.activation_trace_payloads, id_key(id.0))?
        else {
            return Ok(None);
        };
        Ok(Some(ActivationTrace {
            id: header.id,
            revision: header.revision,
            era_id: header.era_id,
            operation_id: header.operation_id,
            recall_case: header.recall_case,
            evaluator: header.evaluator,
            profile: header.profile,
            input_digest: header.input_digest,
            candidates: payload.candidates,
            contributions: payload.contributions,
            recorded_at_ms: header.recorded_at_ms,
        }))
    }

    pub fn inspect_entity(&self, id: EntityId) -> MemoryResult<Option<EntityRecord>> {
        let Some(revision) = get_head(&self.entity_heads, id.0)? else {
            return Ok(None);
        };
        let header: EntityRevisionHeader =
            get_decoded(&self.entities, revision_key(id.0, revision))?.ok_or_else(|| {
                MemoryError::Corrupt(format!("entity {id} is missing revision {revision}"))
            })?;
        let payload: Option<EntityPayload> =
            get_decoded(&self.payloads, payload_key(RecordRef::Entity(id)))?;
        Ok(Some(EntityRecord {
            header,
            canonical_name: payload
                .as_ref()
                .map(|payload| payload.canonical_name.clone()),
            aliases: payload.map_or_else(Vec::new, |payload| payload.aliases),
        }))
    }

    pub fn inspect_proposal(&self, id: ProposalId) -> MemoryResult<Option<ProposalRecord>> {
        let Some(revision) = get_head(&self.proposal_heads, id.0)? else {
            return Ok(None);
        };
        self.inspect_proposal_revision(id, revision)
    }

    fn inspect_proposal_revision(
        &self,
        id: ProposalId,
        revision: u64,
    ) -> MemoryResult<Option<ProposalRecord>> {
        let Some(header): Option<ProposalRevisionHeader> =
            get_decoded(&self.proposals, revision_key(id.0, revision))?
        else {
            return Ok(None);
        };
        let payload: Option<ProposalPayload> =
            get_decoded(&self.payloads, payload_key(RecordRef::Proposal(id)))?;
        Ok(Some(ProposalRecord {
            header,
            source_evidence: payload
                .as_ref()
                .map_or_else(Vec::new, |payload| payload.source_evidence.clone()),
            changes: payload.map(|payload| payload.changes),
        }))
    }

    pub fn proposal_by_source_job(
        &self,
        source_job_id: ProposalSourceJobId,
    ) -> MemoryResult<Option<ProposalRecord>> {
        let Some(index): Option<ProposalSourceIndex> =
            get_decoded(&self.proposal_sources, id_key(source_job_id.0))?
        else {
            return Ok(None);
        };
        self.inspect_proposal(index.proposal_id)
    }

    /// Bounded structural proposal headers in descending proposal-ID order.
    /// Payloads are intentionally omitted so one operator list cannot
    /// materialize many large bundles.
    pub fn list_proposals(&self, limit: usize) -> MemoryResult<Vec<ProposalRevisionHeader>> {
        let limit = limit.min(MAX_OPERATOR_LIST_LIMIT);
        let mut proposals = Vec::with_capacity(limit);
        for item in self.proposal_heads.iter().rev().take(limit) {
            let (key, value) = item.map_err(storage_error)?;
            let id = ProposalId(ulid_from_id_key(&key)?);
            let revision = decode_u64(&value)?;
            let header =
                get_decoded(&self.proposals, revision_key(id.0, revision))?.ok_or_else(|| {
                    MemoryError::Corrupt(format!(
                        "proposal {id} is missing head revision {revision}"
                    ))
                })?;
            proposals.push(header);
        }
        Ok(proposals)
    }

    /// Pending proposals from a maintained bounded-read index.
    pub fn list_pending_proposals(
        &self,
        limit: usize,
    ) -> MemoryResult<Vec<ProposalRevisionHeader>> {
        let limit = limit.min(MAX_OPERATOR_LIST_LIMIT);
        let mut proposals = Vec::with_capacity(limit);
        for item in self.pending_proposals.iter().rev().take(limit) {
            let (key, _) = item.map_err(storage_error)?;
            let id = ProposalId(ulid_from_id_key(&key)?);
            let proposal = self.inspect_proposal(id)?.ok_or_else(|| {
                MemoryError::Corrupt(format!("pending proposal index points to missing {id}"))
            })?;
            if proposal.header.state != RecordState::Active
                || proposal.header.status != ProposalStatus::PendingReview
            {
                return Err(MemoryError::Corrupt(format!(
                    "pending proposal index points to non-pending {id}"
                )));
            }
            proposals.push(proposal.header);
        }
        Ok(proposals)
    }

    /// Proposals requiring an explicit user/operator decision from a maintained
    /// bounded-read index.
    /// `Unsupported` reviews are inspect-only and are not indexed here.
    pub fn list_awaiting_adjudication(
        &self,
        limit: usize,
    ) -> MemoryResult<Vec<ProposalRevisionHeader>> {
        let limit = limit.min(MAX_OPERATOR_LIST_LIMIT);
        let mut proposals = Vec::with_capacity(limit);
        for item in self.awaiting_adjudication.iter().rev().take(limit) {
            let (key, _) = item.map_err(storage_error)?;
            let id = ProposalId(ulid_from_id_key(&key)?);
            let proposal = self.inspect_proposal(id)?.ok_or_else(|| {
                MemoryError::Corrupt(format!(
                    "awaiting-adjudication index points to missing {id}"
                ))
            })?;
            if proposal.header.state != RecordState::Active
                || !proposal_status_awaits_adjudication(proposal.header.status)
            {
                return Err(MemoryError::Corrupt(format!(
                    "awaiting-adjudication index points to non-actionable {id}"
                )));
            }
            proposals.push(proposal.header);
        }
        Ok(proposals)
    }

    pub fn latest_proposal_review(
        &self,
        proposal_id: ProposalId,
    ) -> MemoryResult<Option<ProposalReviewCase>> {
        let Some(pointer): Option<ProposalReviewPointer> =
            get_decoded(&self.latest_proposal_review, id_key(proposal_id.0))?
        else {
            return Ok(None);
        };
        self.inspect_proposal_review_revision(pointer.review_case_id, pointer.revision)
    }

    pub fn inspect_proposal_review(
        &self,
        id: ProposalReviewCaseId,
    ) -> MemoryResult<Option<ProposalReviewCase>> {
        let Some(revision) = get_head(&self.proposal_review_heads, id.0)? else {
            return Ok(None);
        };
        self.inspect_proposal_review_revision(id, revision)
    }

    fn inspect_proposal_review_revision(
        &self,
        id: ProposalReviewCaseId,
        revision: u64,
    ) -> MemoryResult<Option<ProposalReviewCase>> {
        let Some(header): Option<ProposalReviewCaseHeader> =
            get_decoded(&self.proposal_reviews, revision_key(id.0, revision))?
        else {
            return Ok(None);
        };
        let payload: Option<ProposalReviewPayload> =
            get_decoded(&self.payloads, payload_key(RecordRef::ProposalReview(id)))?;
        Ok(Some(ProposalReviewCase {
            header,
            findings: payload.map(|payload| payload.findings),
        }))
    }

    /// Inspect a record through one typed seam, either at its current head or
    /// at an exact durable revision. Unavailable payloads remain unavailable even
    /// when an older structural revision is selected.
    pub fn inspect(
        &self,
        record: RecordRef,
        revision: RevisionSelector,
    ) -> MemoryResult<Option<InspectedRecord>> {
        if revision == RevisionSelector::Head {
            return match record {
                RecordRef::Evidence(id) => self
                    .inspect_evidence(id)
                    .map(|record| record.map(InspectedRecord::Evidence)),
                RecordRef::Claim(id) => self
                    .inspect_claim(id)
                    .map(|record| record.map(InspectedRecord::Claim)),
                RecordRef::Entity(id) => self
                    .inspect_entity(id)
                    .map(|record| record.map(InspectedRecord::Entity)),
                RecordRef::SemanticRelation(id) => self
                    .inspect_semantic_relation(id)
                    .map(|record| record.map(InspectedRecord::SemanticRelation)),
                RecordRef::Proposal(id) => self
                    .inspect_proposal(id)
                    .map(|record| record.map(InspectedRecord::Proposal)),
                RecordRef::ProposalReview(id) => self
                    .inspect_proposal_review(id)
                    .map(|record| record.map(InspectedRecord::ProposalReview)),
                RecordRef::ArtifactCollection(id) => self
                    .inspect_artifact_collection(id)
                    .map(|record| record.map(InspectedRecord::ArtifactCollection)),
                RecordRef::ArtifactSnapshot(id) => self
                    .inspect_artifact_snapshot(id)
                    .map(|record| record.map(InspectedRecord::ArtifactSnapshot)),
                RecordRef::ArtifactPassage(id) => self
                    .inspect_artifact_passage(id)
                    .map(|record| record.map(InspectedRecord::ArtifactPassage)),
            };
        }
        let RevisionSelector::Exact(revision) = revision else {
            unreachable!("head selector returned above")
        };
        match record {
            RecordRef::Evidence(id) => {
                let Some(header): Option<EvidenceHeader> =
                    get_decoded(&self.evidence, id_key(id.0))?
                else {
                    return Ok(None);
                };
                let Some(availability): Option<EvidenceAvailabilityRevision> =
                    get_decoded(&self.evidence_availability, revision_key(id.0, revision))?
                else {
                    return Ok(None);
                };
                let text = if availability.state == RecordState::Purged {
                    None
                } else {
                    get_payload(&self.payloads, record)?
                };
                Ok(Some(InspectedRecord::Evidence(EvidenceRecord {
                    header,
                    availability,
                    text,
                })))
            }
            RecordRef::Claim(id) => {
                let Some(header): Option<ClaimRevisionHeader> =
                    get_decoded(&self.claims, revision_key(id.0, revision))?
                else {
                    return Ok(None);
                };
                let proposition = if header.state == RecordState::Purged {
                    None
                } else {
                    get_payload(&self.payloads, record)?
                };
                Ok(Some(InspectedRecord::Claim(ClaimRecord {
                    header,
                    proposition,
                })))
            }
            RecordRef::Entity(id) => {
                let Some(header): Option<EntityRevisionHeader> =
                    get_decoded(&self.entities, revision_key(id.0, revision))?
                else {
                    return Ok(None);
                };
                let payload: Option<EntityPayload> =
                    get_decoded(&self.payloads, payload_key(record))?;
                Ok(Some(InspectedRecord::Entity(EntityRecord {
                    header,
                    canonical_name: payload
                        .as_ref()
                        .map(|payload| payload.canonical_name.clone()),
                    aliases: payload.map_or_else(Vec::new, |payload| payload.aliases),
                })))
            }
            RecordRef::SemanticRelation(id) => {
                let Some(header): Option<SemanticRelationRevisionHeader> =
                    get_decoded(&self.relations, revision_key(id.0, revision))?
                else {
                    return Ok(None);
                };
                let payload = if header.state == RecordState::Purged {
                    None
                } else {
                    get_decoded::<RelationPayload>(&self.payloads, payload_key(record))?
                };
                Ok(Some(InspectedRecord::SemanticRelation(
                    SemanticRelationRecord {
                        header,
                        qualifier: payload
                            .as_ref()
                            .and_then(|payload| payload.qualifier.clone()),
                        payload_available: payload.is_some(),
                    },
                )))
            }
            RecordRef::Proposal(id) => self
                .inspect_proposal_revision(id, revision)
                .map(|record| record.map(InspectedRecord::Proposal)),
            RecordRef::ProposalReview(id) => self
                .inspect_proposal_review_revision(id, revision)
                .map(|record| record.map(InspectedRecord::ProposalReview)),
            RecordRef::ArtifactCollection(id) => {
                let Some(header) = get_decoded(&self.artifact_collections, id_key(id.0))? else {
                    return Ok(None);
                };
                self.inspect_artifact_collection_revision(header, revision)
                    .map(|record| record.map(InspectedRecord::ArtifactCollection))
            }
            RecordRef::ArtifactSnapshot(id) => {
                let Some(header) = get_decoded(&self.artifact_snapshots, id_key(id.0))? else {
                    return Ok(None);
                };
                self.inspect_artifact_snapshot_revision(header, revision)
                    .map(|record| record.map(InspectedRecord::ArtifactSnapshot))
            }
            RecordRef::ArtifactPassage(id) => {
                let Some(header) = get_decoded(&self.artifact_passages, id_key(id.0))? else {
                    return Ok(None);
                };
                self.inspect_artifact_passage_revision(header, revision)
                    .map(|record| record.map(InspectedRecord::ArtifactPassage))
            }
        }
    }

    /// Durably capture the raw user evidence, then recall against the exact
    /// pre-turn semantic view while the write gate remains held.  The new raw
    /// evidence is explicitly excluded and no concurrent claim/relation write
    /// can enter between capture and case construction.
    pub fn begin_turn_recall(
        &self,
        operation: OperationContext,
        evidence: NewEvidence,
        query: RecallQuery,
    ) -> MemoryResult<BeginTurnRecallResult> {
        if operation.actor != Actor::User
            || !matches!(
                evidence.class,
                EvidenceClass::UserAssertion | EvidenceClass::UserCorrection
            )
        {
            return Err(MemoryError::Unauthorized);
        }
        let _guard = self.write_lock.lock();
        let evidence = self.capture_evidence_locked(operation, evidence)?;
        let excluded = BTreeSet::from([RecordRef::Evidence(evidence.id)]);
        let recall = self.recall_locked(query, &excluded)?;
        Ok(BeginTurnRecallResult { evidence, recall })
    }

    pub fn recall(&self, query: RecallQuery) -> MemoryResult<RecallResult> {
        // Recall persists a case, so it shares the mutation lock.  More
        // importantly, a concurrent purge cannot return content after its
        // immediate-unreadability commit point.
        let _guard = self.write_lock.lock();
        self.recall_locked(query, &BTreeSet::new())
    }

    fn recall_locked(
        &self,
        mut query: RecallQuery,
        excluded: &BTreeSet<RecordRef>,
    ) -> MemoryResult<RecallResult> {
        query.scopes.sort();
        query.scopes.dedup();
        if query.scopes.is_empty() {
            return Err(MemoryError::InvalidInput(
                "recall requires at least one visible scope".into(),
            ));
        }
        if query.scopes.len() > MAX_RECALL_SCOPES {
            return Err(MemoryError::InvalidInput(format!(
                "recall cannot include more than {MAX_RECALL_SCOPES} visible scopes"
            )));
        }
        if let (Some(from), Some(to)) = (query.observed_from_ms, query.observed_to_ms) {
            if from >= to {
                return Err(MemoryError::InvalidInput(
                    "observed time must use a non-empty [from, to) interval".into(),
                ));
            }
        }
        if query.text.len() > MAX_RECALL_QUERY_BYTES {
            return Err(MemoryError::InvalidInput(format!(
                "recall query exceeds {MAX_RECALL_QUERY_BYTES} bytes"
            )));
        }
        query.limit = query.limit.min(MAX_RECALL_LIMIT);
        let normalized = normalize_exact(&query.text);
        if normalized.is_empty()
            && query.observed_from_ms.is_none()
            && query.observed_to_ms.is_none()
        {
            return Err(MemoryError::InvalidInput(
                "recall requires lexical text or an observed-time bound".into(),
            ));
        }

        let mut candidates: BTreeMap<RecordRef, CandidateMatch> = BTreeMap::new();
        if !normalized.is_empty() {
            let exact_hash = self.index_hash(0x02, normalized.as_bytes());
            for record in self.posting_records(0x02, &exact_hash, excluded)? {
                candidates.entry(record).or_default().exact = true;
            }

            let terms: Vec<_> = tokenize(&query.text)
                .into_iter()
                .collect::<BTreeSet<_>>()
                .into_iter()
                .take(MAX_RECALL_TERMS)
                .collect();
            for term in terms {
                let hash = self.index_hash(0x01, term.as_bytes());
                for record in self.posting_records(0x01, &hash, excluded)? {
                    if !candidates.contains_key(&record)
                        && candidates.len() >= MAX_RECALL_CANDIDATES
                    {
                        continue;
                    }
                    candidates.entry(record).or_default().matched_terms += 1;
                }
            }
        } else {
            for record in self.records_in_observed_range(
                query.observed_from_ms,
                query.observed_to_ms,
                MAX_RECALL_CANDIDATES,
                excluded,
            )? {
                candidates.entry(record).or_default();
            }
        }

        let mut citations = Vec::new();
        for (record, matched) in candidates {
            if excluded.contains(&record) {
                continue;
            }
            let Some(mut citation) = self.recallable_record(record)? else {
                continue;
            };
            if !query.scopes.contains(&citation.scope) {
                continue;
            }
            let observed = citation.temporal.observed_at_ms;
            if query.observed_from_ms.is_some_and(|from| observed < from)
                || query.observed_to_ms.is_some_and(|to| observed >= to)
            {
                continue;
            }
            if query
                .valid_at_ms
                .is_some_and(|at| !citation.temporal.contains_valid_time(at))
            {
                continue;
            }
            citation.exact_match = matched.exact;
            citation.matched_term_count = matched.matched_terms;
            citations.push(citation);
        }
        citations.sort_by(|left, right| {
            right
                .exact_match
                .cmp(&left.exact_match)
                .then_with(|| right.matched_term_count.cmp(&left.matched_term_count))
                .then_with(|| {
                    right
                        .temporal
                        .observed_at_ms
                        .cmp(&left.temporal.observed_at_ms)
                })
                .then_with(|| left.record.cmp(&right.record))
        });
        citations.truncate(query.limit);

        let case_id = RecallCaseId::new();
        let case = RecallCase {
            id: case_id,
            revision: 1,
            era_id: self.era_id,
            query_digest: self.index_hash(0x03, &encode(&query)?),
            scopes: query.scopes,
            candidates: citations
                .iter()
                .enumerate()
                .map(|(rank, citation)| RecallCaseCandidate {
                    record: citation.record,
                    revision: citation.revision,
                    rank: rank as u32,
                    exact_match: citation.exact_match,
                    matched_term_count: citation.matched_term_count,
                })
                .collect(),
            recorded_at_ms: now_ms(),
        };
        let mut batch = self.durable_batch();
        batch.insert(&self.recall_cases, id_key(case_id.0), encode(&case)?);
        commit(batch)?;
        Ok(RecallResult { case_id, citations })
    }

    pub fn inspect_recall_case(&self, id: RecallCaseId) -> MemoryResult<Option<RecallCase>> {
        get_decoded(&self.recall_cases, id_key(id.0))
    }

    /// Persist a Dream-produced bundle without activating any semantic record.
    /// The source-job index is unique across operation retries and process
    /// restarts.
    pub fn submit_proposal(
        &self,
        operation: OperationContext,
        input: NewProposalBundle,
    ) -> MemoryResult<ProposalReceipt> {
        let input = normalize_proposal_bundle(input)?;
        let digest = self.mutation_digest("submit_proposal", operation.actor, &input)?;
        let _guard = self.write_lock.lock();
        if let Some(receipt) = self.replayed_operation(operation.id, digest)? {
            return Ok(receipt);
        }

        if let Some(existing) = get_decoded::<ProposalSourceIndex>(
            &self.proposal_sources,
            id_key(input.source_job_id.0),
        )? {
            if existing.input_digest != digest {
                return Err(MemoryError::InvalidInput(format!(
                    "source job {} already identifies a different proposal bundle",
                    input.source_job_id
                )));
            }
            let proposal = self
                .inspect_proposal(existing.proposal_id)?
                .ok_or_else(|| MemoryError::Corrupt("proposal source index is dangling".into()))?;
            let receipt = proposal_receipt(&proposal.header);
            let mut batch = self.durable_batch();
            self.insert_operation(&mut batch, operation.id, digest, &receipt)?;
            commit(batch)?;
            return Ok(receipt);
        }

        for pin in &input.source_evidence {
            let evidence = self
                .inspect_evidence(pin.id)?
                .ok_or(MemoryError::SourceUnavailable(pin.id))?;
            if evidence.availability.revision != pin.revision
                || evidence.availability.state != RecordState::Active
            {
                return Err(MemoryError::SourceUnavailable(pin.id));
            }
            if evidence.header.scope != input.scope {
                return Err(MemoryError::ScopeMismatch);
            }
        }
        for pin in proposal_existing_pins(&input.changes) {
            let (scope, state, revision) = self.record_scope_state_revision(pin.record)?;
            if scope != input.scope {
                return Err(MemoryError::ScopeMismatch);
            }
            if state != RecordState::Active || revision != pin.revision {
                return Err(MemoryError::RevisionConflict {
                    expected: pin.revision,
                    actual: revision,
                });
            }
        }

        let recorded_at_ms = now_ms();
        let id = ProposalId::new();
        let header = ProposalRevisionHeader {
            id,
            revision: 1,
            previous_revision: None,
            source_job_id: input.source_job_id,
            scope: input.scope,
            status: ProposalStatus::PendingReview,
            state: RecordState::Active,
            recorded_at_ms,
            operation_id: operation.id,
        };
        let payload = ProposalPayload {
            source_evidence: input.source_evidence.clone(),
            changes: input.changes.clone(),
        };
        let receipt = proposal_receipt(&header);
        let audit = self.new_audit_event(
            operation,
            AuditAction::ProposalSubmitted,
            vec![RecordRef::Proposal(id)],
            recorded_at_ms,
        );

        let mut batch = self.durable_batch();
        batch.insert(&self.proposals, revision_key(id.0, 1), encode(&header)?);
        batch.insert(&self.proposal_heads, id_key(id.0), 1_u64.to_be_bytes());
        batch.insert(&self.pending_proposals, id_key(id.0), []);
        batch.insert(
            &self.proposal_sources,
            id_key(input.source_job_id.0),
            encode(&ProposalSourceIndex {
                proposal_id: id,
                input_digest: digest,
            })?,
        );
        batch.insert(
            &self.payloads,
            payload_key(RecordRef::Proposal(id)),
            encode(&payload)?,
        );
        let mut dependencies = BTreeSet::new();
        dependencies.extend(
            input
                .source_evidence
                .iter()
                .map(|pin| RecordRef::Evidence(pin.id)),
        );
        dependencies.extend(
            proposal_existing_pins(&input.changes)
                .into_iter()
                .map(|pin| pin.record),
        );
        for source in dependencies {
            batch.insert(
                &self.dependencies,
                dependency_key(source, RecordRef::Proposal(id)),
                [],
            );
        }
        self.insert_audit(&mut batch, &audit)?;
        self.insert_operation(&mut batch, operation.id, digest, &receipt)?;
        commit(batch)?;
        Ok(receipt)
    }

    pub fn review_proposal(
        &self,
        operation: OperationContext,
        mut input: NewProposalReview,
    ) -> MemoryResult<ProposalReviewReceipt> {
        if !matches!(
            operation.actor,
            Actor::Assistant | Actor::System | Actor::Operator
        ) {
            return Err(MemoryError::Unauthorized);
        }
        normalize_review(&mut input)?;
        let digest = self.mutation_digest("review_proposal", operation.actor, &input)?;
        let _guard = self.write_lock.lock();
        if let Some(receipt) = self.replayed_operation(operation.id, digest)? {
            return Ok(receipt);
        }

        let proposal = self
            .inspect_proposal(input.proposal_id)?
            .ok_or(MemoryError::NotFound(RecordRef::Proposal(
                input.proposal_id,
            )))?;
        if proposal.header.revision != input.proposal_revision {
            return Err(MemoryError::RevisionConflict {
                expected: input.proposal_revision,
                actual: proposal.header.revision,
            });
        }
        if proposal.header.state != RecordState::Active
            || proposal.header.status != ProposalStatus::PendingReview
            || proposal.changes.is_none()
        {
            return Err(MemoryError::InvalidInput(
                "only an available pending proposal can be reviewed".into(),
            ));
        }
        let change_count = proposal.changes.as_ref().map_or(0, Vec::len);
        if input.findings.iter().any(|finding| {
            finding
                .change_index
                .is_some_and(|index| index as usize >= change_count)
        }) {
            return Err(MemoryError::InvalidInput(
                "review finding change index is outside the pinned proposal".into(),
            ));
        }
        let recall_case = self
            .inspect_recall_case(input.recall_case_id)?
            .ok_or_else(|| MemoryError::InvalidInput("recall case does not exist".into()))?;
        if recall_case.revision != input.recall_case_revision {
            return Err(MemoryError::RevisionConflict {
                expected: input.recall_case_revision,
                actual: recall_case.revision,
            });
        }
        if !recall_case.scopes.contains(&proposal.header.scope) {
            return Err(MemoryError::ScopeMismatch);
        }
        if input.verdict == ProposalReviewVerdict::Approve && !input.findings.is_empty() {
            return Err(MemoryError::InvalidInput(
                "an approve verdict cannot contain review findings".into(),
            ));
        }
        let allowed_finding_pins: BTreeSet<_> = proposal
            .source_evidence
            .iter()
            .map(|pin| RecordRevisionPin {
                record: RecordRef::Evidence(pin.id),
                revision: pin.revision,
            })
            .chain(
                recall_case
                    .candidates
                    .iter()
                    .map(|candidate| RecordRevisionPin {
                        record: candidate.record,
                        revision: candidate.revision,
                    }),
            )
            .collect();
        for finding in &input.findings {
            for pin in &finding.pins {
                if !allowed_finding_pins.contains(pin) {
                    return Err(MemoryError::InvalidInput(
                        "review finding pins must come from exact proposal evidence or recall case candidates"
                            .into(),
                    ));
                }
                let (scope, _, _) = self.record_scope_state_revision(pin.record)?;
                if scope != proposal.header.scope {
                    return Err(MemoryError::ScopeMismatch);
                }
                self.require_current_pin(*pin)?;
            }
        }

        let status = match input.verdict {
            ProposalReviewVerdict::Approve => ProposalStatus::ReviewedApprove,
            ProposalReviewVerdict::Reject => ProposalStatus::ReviewedReject,
            ProposalReviewVerdict::NeedsUser => ProposalStatus::NeedsUser,
            ProposalReviewVerdict::Unsupported => ProposalStatus::Unsupported,
        };
        let recorded_at_ms = now_ms();
        let proposal_revision = proposal
            .header
            .revision
            .checked_add(1)
            .ok_or_else(|| MemoryError::Corrupt("proposal revision overflow".into()))?;
        let mut proposal_header = proposal.header.clone();
        proposal_header.revision = proposal_revision;
        proposal_header.previous_revision = Some(proposal.header.revision);
        proposal_header.status = status;
        proposal_header.recorded_at_ms = recorded_at_ms;
        proposal_header.operation_id = operation.id;

        let review_id = ProposalReviewCaseId::new();
        let review_header = ProposalReviewCaseHeader {
            id: review_id,
            revision: 1,
            previous_revision: None,
            proposal_id: input.proposal_id,
            proposal_revision: input.proposal_revision,
            recall_case_id: input.recall_case_id,
            recall_case_revision: input.recall_case_revision,
            verdict: input.verdict,
            scope: proposal.header.scope,
            state: RecordState::Active,
            recorded_at_ms,
            operation_id: operation.id,
        };
        let receipt = ProposalReviewReceipt {
            review_case_id: review_id,
            review_revision: 1,
            proposal: proposal_receipt(&proposal_header),
        };
        let mut batch = self.durable_batch();
        batch.insert(
            &self.proposals,
            revision_key(input.proposal_id.0, proposal_revision),
            encode(&proposal_header)?,
        );
        batch.insert(
            &self.proposal_heads,
            id_key(input.proposal_id.0),
            proposal_revision.to_be_bytes(),
        );
        batch.insert(
            &self.proposal_reviews,
            revision_key(review_id.0, 1),
            encode(&review_header)?,
        );
        batch.insert(
            &self.proposal_review_heads,
            id_key(review_id.0),
            1_u64.to_be_bytes(),
        );
        batch.remove(&self.pending_proposals, id_key(input.proposal_id.0));
        if proposal_status_awaits_adjudication(status) {
            batch.insert(&self.awaiting_adjudication, id_key(input.proposal_id.0), []);
        }
        batch.insert(
            &self.latest_proposal_review,
            id_key(input.proposal_id.0),
            encode(&ProposalReviewPointer {
                review_case_id: review_id,
                revision: 1,
            })?,
        );
        batch.insert(
            &self.payloads,
            payload_key(RecordRef::ProposalReview(review_id)),
            encode(&ProposalReviewPayload {
                findings: input.findings.clone(),
            })?,
        );
        let mut dependencies = BTreeSet::from([RecordRef::Proposal(input.proposal_id)]);
        dependencies.extend(
            recall_case
                .candidates
                .iter()
                .map(|candidate| candidate.record),
        );
        dependencies.extend(
            input
                .findings
                .iter()
                .flat_map(|finding| finding.pins.iter().map(|pin| pin.record)),
        );
        for source in dependencies {
            batch.insert(
                &self.dependencies,
                dependency_key(source, RecordRef::ProposalReview(review_id)),
                [],
            );
        }
        let audit = self.new_audit_event(
            operation,
            AuditAction::ProposalReviewed,
            vec![
                RecordRef::Proposal(input.proposal_id),
                RecordRef::ProposalReview(review_id),
            ],
            recorded_at_ms,
        );
        self.insert_audit(&mut batch, &audit)?;
        self.insert_operation(&mut batch, operation.id, digest, &receipt)?;
        commit(batch)?;
        Ok(receipt)
    }

    /// Apply or reject a reviewed proposal. Semantic activation requires an
    /// explicit user or operator decision; an assistant can propose and review,
    /// but can never activate.
    pub fn adjudicate_proposal(
        &self,
        operation: OperationContext,
        input: ProposalAdjudication,
    ) -> MemoryResult<ProposalAdjudicationReceipt> {
        validate_adjudication_authority(operation.actor, input.authority)?;
        let digest = self.mutation_digest("adjudicate_proposal", operation.actor, &input)?;
        let _guard = self.write_lock.lock();
        if let Some(receipt) = self.replayed_operation(operation.id, digest)? {
            return Ok(receipt);
        }

        let proposal = self
            .inspect_proposal(input.proposal_id)?
            .ok_or(MemoryError::NotFound(RecordRef::Proposal(
                input.proposal_id,
            )))?;
        if proposal.header.revision != input.expected_proposal_revision {
            return Err(MemoryError::RevisionConflict {
                expected: input.expected_proposal_revision,
                actual: proposal.header.revision,
            });
        }
        let Some(changes) = proposal.changes.clone() else {
            return Err(MemoryError::InvalidInput(
                "proposal payload is unavailable".into(),
            ));
        };
        if proposal.header.status == ProposalStatus::Stale {
            let recorded_at_ms = now_ms();
            let receipt = ProposalAdjudicationReceipt {
                proposal: proposal_receipt(&proposal.header),
                draft_mappings: Vec::new(),
                changed_records: Vec::new(),
            };
            let mut batch = self.durable_batch();
            batch.remove(&self.awaiting_adjudication, id_key(proposal.header.id.0));
            let audit = self.new_adjudication_audit_event(
                operation,
                vec![RecordRef::Proposal(proposal.header.id)],
                recorded_at_ms,
                &input,
            );
            self.insert_audit(&mut batch, &audit)?;
            self.insert_operation(&mut batch, operation.id, digest, &receipt)?;
            commit(batch)?;
            return Ok(receipt);
        }
        if proposal.header.state != RecordState::Active {
            return Err(MemoryError::InvalidInput(
                "proposal workflow record is unavailable".into(),
            ));
        }

        let review =
            self.inspect_proposal_review(input.review_case_id)?
                .ok_or(MemoryError::NotFound(RecordRef::ProposalReview(
                    input.review_case_id,
                )))?;
        if review.header.revision != input.expected_review_revision {
            return Err(MemoryError::RevisionConflict {
                expected: input.expected_review_revision,
                actual: review.header.revision,
            });
        }
        if review.header.proposal_id != input.proposal_id
            || proposal.header.previous_revision != Some(review.header.proposal_revision)
        {
            return Err(MemoryError::InvalidInput(
                "review case does not pin the adjudicated proposal revision".into(),
            ));
        }

        if input.decision == ProposalDecision::Reject {
            let recorded_at_ms = now_ms();
            let (next, receipt) = next_proposal_status(
                &proposal.header,
                ProposalStatus::Rejected,
                operation.id,
                recorded_at_ms,
            )?;
            let result = ProposalAdjudicationReceipt {
                proposal: receipt,
                draft_mappings: Vec::new(),
                changed_records: Vec::new(),
            };
            let mut batch = self.durable_batch();
            batch.remove(&self.awaiting_adjudication, id_key(next.id.0));
            batch.insert(
                &self.proposals,
                revision_key(next.id.0, next.revision),
                encode(&next)?,
            );
            batch.insert(
                &self.proposal_heads,
                id_key(next.id.0),
                next.revision.to_be_bytes(),
            );
            let audit = self.new_adjudication_audit_event(
                operation,
                vec![RecordRef::Proposal(next.id)],
                recorded_at_ms,
                &input,
            );
            self.insert_audit(&mut batch, &audit)?;
            self.insert_operation(&mut batch, operation.id, digest, &result)?;
            commit(batch)?;
            return Ok(result);
        }

        let status_allows_accept = matches!(
            proposal.header.status,
            ProposalStatus::ReviewedApprove | ProposalStatus::NeedsUser
        );
        if !status_allows_accept {
            return Err(MemoryError::InvalidInput(
                "proposal review status does not permit acceptance by this authority".into(),
            ));
        }

        let recall_case = self
            .inspect_recall_case(review.header.recall_case_id)?
            .ok_or_else(|| MemoryError::InvalidInput("pinned recall case is unavailable".into()))?;
        let mut pins_current = review.header.state == RecordState::Active
            && recall_case.revision == review.header.recall_case_revision
            && recall_case.scopes.contains(&proposal.header.scope)
            && review.header.scope == proposal.header.scope;
        let Some(findings) = review.findings.as_ref() else {
            return Err(MemoryError::InvalidInput(
                "review finding payload is unavailable".into(),
            ));
        };
        if review.header.verdict == ProposalReviewVerdict::Approve && !findings.is_empty() {
            return Err(MemoryError::InvalidInput(
                "an approve verdict cannot contain review findings".into(),
            ));
        }
        let allowed_finding_pins: BTreeSet<_> = proposal
            .source_evidence
            .iter()
            .map(|pin| RecordRevisionPin {
                record: RecordRef::Evidence(pin.id),
                revision: pin.revision,
            })
            .chain(
                recall_case
                    .candidates
                    .iter()
                    .map(|candidate| RecordRevisionPin {
                        record: candidate.record,
                        revision: candidate.revision,
                    }),
            )
            .collect();
        for source in &proposal.source_evidence {
            pins_current &= self.evidence_pin_is_current(*source, &proposal.header.scope)?;
        }
        for pin in proposal_existing_pins(&changes) {
            pins_current &= self.record_pin_is_current(pin, &proposal.header.scope)?;
        }
        for candidate in &recall_case.candidates {
            pins_current &= self.record_pin_revision_is_current(RecordRevisionPin {
                record: candidate.record,
                revision: candidate.revision,
            })?;
        }
        for pin in findings.iter().flat_map(|finding| &finding.pins) {
            pins_current &= allowed_finding_pins.contains(pin)
                && self.record_pin_is_current(*pin, &proposal.header.scope)?;
        }
        if !pins_current {
            let recorded_at_ms = now_ms();
            let (next, receipt) = next_proposal_status(
                &proposal.header,
                ProposalStatus::Stale,
                operation.id,
                recorded_at_ms,
            )?;
            let result = ProposalAdjudicationReceipt {
                proposal: receipt,
                draft_mappings: Vec::new(),
                changed_records: Vec::new(),
            };
            let mut batch = self.durable_batch();
            batch.remove(&self.awaiting_adjudication, id_key(next.id.0));
            batch.insert(
                &self.proposals,
                revision_key(next.id.0, next.revision),
                encode(&next)?,
            );
            batch.insert(
                &self.proposal_heads,
                id_key(next.id.0),
                next.revision.to_be_bytes(),
            );
            let audit = self.new_adjudication_audit_event(
                operation,
                vec![RecordRef::Proposal(next.id)],
                recorded_at_ms,
                &input,
            );
            self.insert_audit(&mut batch, &audit)?;
            self.insert_operation(&mut batch, operation.id, digest, &result)?;
            commit(batch)?;
            return Ok(result);
        }

        let evidence_classes =
            self.proposal_evidence_classes(&proposal.source_evidence, &proposal.header.scope)?;
        validate_proposal_activation(
            operation.actor,
            &proposal.header.scope,
            &changes,
            &evidence_classes,
        )?;
        if operation.actor == Actor::User {
            for pin in proposal_existing_pins(&changes) {
                if !self.user_may_activate_endpoint(pin.record)?
                    && changes.iter().any(|change| {
                        matches!(
                            change,
                            ProposalChange::CreateRelation {
                                from: ProposalEndpoint::Existing(endpoint),
                                ..
                            } if endpoint.record == pin.record
                        ) || matches!(
                            change,
                            ProposalChange::CreateRelation {
                                to: ProposalEndpoint::Existing(endpoint),
                                ..
                            } if endpoint.record == pin.record
                        )
                    })
                {
                    return Err(MemoryError::Unauthorized);
                }
            }
        }

        let mut draft_records = BTreeMap::new();
        for change in &changes {
            let mapping = match change {
                ProposalChange::CreateClaim { draft_id, .. } => {
                    Some((*draft_id, AppliedRecord::Claim(ClaimId::new())))
                }
                ProposalChange::CreateEntity { draft_id, .. } => {
                    Some((*draft_id, AppliedRecord::Entity(EntityId::new())))
                }
                ProposalChange::CreateRelation { draft_id, .. } => Some((
                    *draft_id,
                    AppliedRecord::SemanticRelation(RelationId::new()),
                )),
                ProposalChange::Retract { .. } | ProposalChange::Supersede { .. } => None,
            };
            if let Some((draft_id, record)) = mapping {
                draft_records.insert(draft_id, record);
            }
        }
        let draft_mappings: Vec<_> = draft_records
            .iter()
            .map(|(draft_id, record)| DraftMapping {
                draft_id: *draft_id,
                record: *record,
            })
            .collect();
        let recorded_at_ms = now_ms();
        let mut batch = self.durable_batch();
        batch.remove(&self.awaiting_adjudication, id_key(proposal.header.id.0));
        let mut changed_records = Vec::new();

        for change in &changes {
            match change {
                ProposalChange::CreateClaim {
                    draft_id,
                    domain,
                    temporal,
                    proposition,
                    evidence_ids,
                } => {
                    let AppliedRecord::Claim(id) = draft_records[draft_id] else {
                        unreachable!("claim draft mapping changed kind")
                    };
                    let header = ClaimRevisionHeader {
                        id,
                        revision: 1,
                        previous_revision: None,
                        domain: *domain,
                        scope: proposal.header.scope.clone(),
                        temporal: *temporal,
                        evidence_ids: evidence_ids.clone(),
                        state: RecordState::Active,
                        recorded_at_ms,
                        operation_id: operation.id,
                    };
                    batch.insert(&self.claims, revision_key(id.0, 1), encode(&header)?);
                    batch.insert(&self.claim_heads, id_key(id.0), 1_u64.to_be_bytes());
                    batch.insert(
                        &self.payloads,
                        payload_key(RecordRef::Claim(id)),
                        proposition.as_bytes(),
                    );
                    for evidence_id in evidence_ids {
                        batch.insert(
                            &self.dependencies,
                            dependency_key(RecordRef::Evidence(*evidence_id), RecordRef::Claim(id)),
                            [],
                        );
                    }
                    self.insert_lexical_document(
                        &mut batch,
                        RecordRef::Claim(id),
                        1,
                        proposal.header.scope.clone(),
                        *temporal,
                        proposition,
                    )?;
                    changed_records.push(RecordRevision {
                        record: RecordRef::Claim(id),
                        revision: 1,
                        state: RecordState::Active,
                    });
                }
                ProposalChange::CreateEntity {
                    draft_id,
                    kind,
                    temporal,
                    canonical_name,
                    aliases,
                    evidence_ids,
                } => {
                    let AppliedRecord::Entity(id) = draft_records[draft_id] else {
                        unreachable!("entity draft mapping changed kind")
                    };
                    let header = EntityRevisionHeader {
                        id,
                        revision: 1,
                        previous_revision: None,
                        kind: *kind,
                        scope: proposal.header.scope.clone(),
                        temporal: *temporal,
                        evidence_ids: evidence_ids.clone(),
                        state: RecordState::Active,
                        recorded_at_ms,
                        operation_id: operation.id,
                    };
                    batch.insert(&self.entities, revision_key(id.0, 1), encode(&header)?);
                    batch.insert(&self.entity_heads, id_key(id.0), 1_u64.to_be_bytes());
                    batch.insert(
                        &self.payloads,
                        payload_key(RecordRef::Entity(id)),
                        encode(&EntityPayload {
                            canonical_name: canonical_name.clone(),
                            aliases: aliases.clone(),
                        })?,
                    );
                    for evidence_id in evidence_ids {
                        batch.insert(
                            &self.dependencies,
                            dependency_key(
                                RecordRef::Evidence(*evidence_id),
                                RecordRef::Entity(id),
                            ),
                            [],
                        );
                    }
                    let indexed_text = entity_index_text(canonical_name, aliases);
                    self.insert_lexical_document(
                        &mut batch,
                        RecordRef::Entity(id),
                        1,
                        proposal.header.scope.clone(),
                        *temporal,
                        &indexed_text,
                    )?;
                    changed_records.push(RecordRevision {
                        record: RecordRef::Entity(id),
                        revision: 1,
                        state: RecordState::Active,
                    });
                }
                ProposalChange::CreateRelation { .. }
                | ProposalChange::Retract { .. }
                | ProposalChange::Supersede { .. } => {}
            }
        }

        for change in &changes {
            let ProposalChange::CreateRelation {
                draft_id,
                from,
                to,
                kind,
                evidence_ids,
                qualifier,
            } = change
            else {
                continue;
            };
            let AppliedRecord::SemanticRelation(id) = draft_records[draft_id] else {
                unreachable!("relation draft mapping changed kind")
            };
            let from = resolve_proposal_endpoint(*from, &draft_records)?;
            let to = resolve_proposal_endpoint(*to, &draft_records)?;
            let header = SemanticRelationRevisionHeader {
                id,
                revision: 1,
                previous_revision: None,
                from,
                to,
                kind: *kind,
                scope: proposal.header.scope.clone(),
                evidence_ids: evidence_ids.clone(),
                state: RecordState::Active,
                recorded_at_ms,
                operation_id: operation.id,
            };
            batch.insert(&self.relations, revision_key(id.0, 1), encode(&header)?);
            batch.insert(&self.relation_heads, id_key(id.0), 1_u64.to_be_bytes());
            batch.insert(
                &self.payloads,
                payload_key(RecordRef::SemanticRelation(id)),
                encode(&RelationPayload {
                    qualifier: qualifier.clone(),
                })?,
            );
            for source in std::iter::once(from)
                .chain(std::iter::once(to))
                .chain(evidence_ids.iter().copied().map(RecordRef::Evidence))
            {
                batch.insert(
                    &self.dependencies,
                    dependency_key(source, RecordRef::SemanticRelation(id)),
                    [],
                );
            }
            changed_records.push(RecordRevision {
                record: RecordRef::SemanticRelation(id),
                revision: 1,
                state: RecordState::Active,
            });
        }

        let mutation_targets: BTreeSet<_> = changes
            .iter()
            .filter_map(|change| match change {
                ProposalChange::Retract { target } | ProposalChange::Supersede { target, .. } => {
                    Some(target.record)
                }
                _ => None,
            })
            .collect();
        let workflow_records = BTreeSet::from([
            RecordRef::Proposal(input.proposal_id),
            RecordRef::ProposalReview(input.review_case_id),
        ]);
        let mut invalidated_seen = BTreeSet::new();
        for change in &changes {
            let (target, state, replacement) = match change {
                ProposalChange::Retract { target } => (*target, RecordState::Retracted, None),
                ProposalChange::Supersede {
                    target,
                    replacement,
                } => (*target, RecordState::Superseded, Some(*replacement)),
                _ => continue,
            };
            let dependencies = self.active_dependency_closure(target.record)?;
            let changed = self.append_state_revision(
                &mut batch,
                target.record,
                target.revision,
                state,
                operation.id,
                recorded_at_ms,
            )?;
            self.remove_lexical_document(&mut batch, target.record)?;
            changed_records.push(changed);
            if let Some(replacement) = replacement {
                let from = draft_records
                    .get(&replacement)
                    .copied()
                    .map(AppliedRecord::as_record_ref)
                    .ok_or_else(|| {
                        MemoryError::InvalidInput(
                            "supersession replacement draft is unresolved".into(),
                        )
                    })?;
                let relation_id = RelationId::new();
                let evidence_ids: Vec<_> =
                    proposal.source_evidence.iter().map(|pin| pin.id).collect();
                let header = SemanticRelationRevisionHeader {
                    id: relation_id,
                    revision: 1,
                    previous_revision: None,
                    from,
                    to: target.record,
                    kind: RelationKind::Supersedes,
                    scope: proposal.header.scope.clone(),
                    evidence_ids: evidence_ids.clone(),
                    state: RecordState::Active,
                    recorded_at_ms,
                    operation_id: operation.id,
                };
                batch.insert(
                    &self.relations,
                    revision_key(relation_id.0, 1),
                    encode(&header)?,
                );
                batch.insert(
                    &self.relation_heads,
                    id_key(relation_id.0),
                    1_u64.to_be_bytes(),
                );
                batch.insert(
                    &self.payloads,
                    payload_key(RecordRef::SemanticRelation(relation_id)),
                    encode(&RelationPayload { qualifier: None })?,
                );
                for source in std::iter::once(from)
                    .chain(std::iter::once(target.record))
                    .chain(evidence_ids.into_iter().map(RecordRef::Evidence))
                {
                    batch.insert(
                        &self.dependencies,
                        dependency_key(source, RecordRef::SemanticRelation(relation_id)),
                        [],
                    );
                }
                changed_records.push(RecordRevision {
                    record: RecordRef::SemanticRelation(relation_id),
                    revision: 1,
                    state: RecordState::Active,
                });
            }
            for dependency in dependencies {
                if workflow_records.contains(&dependency.record)
                    || mutation_targets.contains(&dependency.record)
                    || !invalidated_seen.insert(dependency.record)
                {
                    continue;
                }
                let changed = self.append_state_revision(
                    &mut batch,
                    dependency.record,
                    dependency.expected_revision,
                    RecordState::Unsupported,
                    operation.id,
                    recorded_at_ms,
                )?;
                self.remove_lexical_document(&mut batch, dependency.record)?;
                changed_records.push(changed);
            }
        }

        let (next, proposal_receipt) = next_proposal_status(
            &proposal.header,
            ProposalStatus::Applied,
            operation.id,
            recorded_at_ms,
        )?;
        batch.insert(
            &self.proposals,
            revision_key(next.id.0, next.revision),
            encode(&next)?,
        );
        batch.insert(
            &self.proposal_heads,
            id_key(next.id.0),
            next.revision.to_be_bytes(),
        );
        let result = ProposalAdjudicationReceipt {
            proposal: proposal_receipt,
            draft_mappings,
            changed_records,
        };
        let mut subjects = vec![RecordRef::Proposal(next.id)];
        subjects.extend(result.changed_records.iter().map(|entry| entry.record));
        let audit = self.new_adjudication_audit_event(operation, subjects, recorded_at_ms, &input);
        self.insert_audit(&mut batch, &audit)?;
        self.insert_operation(&mut batch, operation.id, digest, &result)?;
        commit(batch)?;
        Ok(result)
    }

    pub fn record_recall_feedback(
        &self,
        operation: OperationContext,
        input: NewRecallFeedback,
    ) -> MemoryResult<RecallFeedback> {
        if operation.actor == Actor::Assistant {
            return Err(MemoryError::Unauthorized);
        }
        match (input.kind, input.candidate) {
            (RecallFeedbackKind::MissingExpectedRecord, Some(_)) => {
                return Err(MemoryError::InvalidInput(
                    "missing-expected-record feedback must not name a candidate".into(),
                ));
            }
            (RecallFeedbackKind::MissingExpectedRecord, None) => {}
            (_, None) => {
                return Err(MemoryError::InvalidInput(
                    "candidate-specific recall feedback requires a candidate".into(),
                ));
            }
            (_, Some(_)) => {}
        }
        let digest = self.mutation_digest("record_recall_feedback", operation.actor, &input)?;
        let _guard = self.write_lock.lock();
        if let Some(receipt) = self.replayed_operation(operation.id, digest)? {
            return Ok(receipt);
        }
        let recall_case = self
            .inspect_recall_case(input.recall_case_id)?
            .ok_or_else(|| MemoryError::InvalidInput("recall case does not exist".into()))?;
        if recall_case.revision != input.recall_case_revision {
            return Err(MemoryError::RevisionConflict {
                expected: input.recall_case_revision,
                actual: recall_case.revision,
            });
        }
        if let Some(candidate) = input.candidate {
            if !recall_case.candidates.iter().any(|pinned| {
                pinned.record == candidate.record && pinned.revision == candidate.revision
            }) {
                return Err(MemoryError::InvalidInput(
                    "feedback candidate is not pinned by the recall case".into(),
                ));
            }
        }
        let feedback = RecallFeedback {
            id: RecallFeedbackId::new(),
            era_id: self.era_id,
            operation_id: operation.id,
            actor: operation.actor,
            recall_case_id: input.recall_case_id,
            recall_case_revision: input.recall_case_revision,
            candidate: input.candidate,
            kind: input.kind,
            recorded_at_ms: now_ms(),
        };
        let mut batch = self.durable_batch();
        batch.insert(
            &self.recall_feedback,
            id_key(feedback.id.0),
            encode(&feedback)?,
        );
        let audit = self.new_audit_event(
            operation,
            AuditAction::RecallFeedbackRecorded,
            input
                .candidate
                .map_or_else(Vec::new, |pin| vec![pin.record]),
            feedback.recorded_at_ms,
        );
        self.insert_audit(&mut batch, &audit)?;
        self.insert_operation(&mut batch, operation.id, digest, &feedback)?;
        commit(batch)?;
        Ok(feedback)
    }

    pub fn inspect_recall_feedback(
        &self,
        id: RecallFeedbackId,
    ) -> MemoryResult<Option<RecallFeedback>> {
        get_decoded(&self.recall_feedback, id_key(id.0))
    }

    pub fn preview_purge(&self, actor: Actor, target: RecordRef) -> MemoryResult<PurgePreview> {
        if actor != Actor::Operator {
            return Err(MemoryError::Unauthorized);
        }
        let _guard = self.write_lock.lock();
        let issued_at_ms = now_ms();
        let expires_at_ms = issued_at_ms
            .checked_add(PURGE_PREVIEW_TTL_MS)
            .ok_or_else(|| MemoryError::Corrupt("purge preview time overflow".into()))?;
        self.build_purge_preview_at(target, issued_at_ms, expires_at_ms)
    }

    pub fn commit_purge(
        &self,
        operation: OperationContext,
        preview: PurgePreview,
    ) -> MemoryResult<PurgeReceipt> {
        if operation.actor != Actor::Operator {
            return Err(MemoryError::Unauthorized);
        }
        let digest = self.mutation_digest("commit_purge", operation.actor, &preview)?;
        let _guard = self.write_lock.lock();
        if let Some(receipt) = self.replayed_operation(operation.id, digest)? {
            return Ok(receipt);
        }
        let current_time_ms = now_ms();
        if current_time_ms < preview.issued_at_ms {
            return Err(MemoryError::PurgePreviewNotYetValid {
                issued_at_ms: preview.issued_at_ms,
                now_ms: current_time_ms,
            });
        }
        if current_time_ms >= preview.expires_at_ms {
            return Err(MemoryError::PurgePreviewExpired {
                expires_at_ms: preview.expires_at_ms,
                now_ms: current_time_ms,
            });
        }
        if preview.issued_at_ms.checked_add(PURGE_PREVIEW_TTL_MS) != Some(preview.expires_at_ms)
            || self.build_purge_preview_at(
                preview.target,
                preview.issued_at_ms,
                preview.expires_at_ms,
            )? != preview
        {
            return Err(MemoryError::StalePurgePreview);
        }

        let recorded_at_ms = now_ms();
        let mut batch = self.durable_batch();
        let target = self.append_state_revision(
            &mut batch,
            preview.target,
            preview.expected_revision,
            RecordState::Purged,
            operation.id,
            recorded_at_ms,
        )?;
        self.remove_lexical_document(&mut batch, preview.target)?;
        self.remove_record_payload(&mut batch, preview.target);

        let mut invalidated = Vec::with_capacity(preview.invalidations.len());
        for dependency in &preview.invalidations {
            let (_, state, actual_revision) =
                self.record_scope_state_revision(dependency.record)?;
            if actual_revision != dependency.expected_revision {
                return Err(MemoryError::RevisionConflict {
                    expected: dependency.expected_revision,
                    actual: actual_revision,
                });
            }
            if state == RecordState::Active {
                invalidated.push(self.append_state_revision(
                    &mut batch,
                    dependency.record,
                    dependency.expected_revision,
                    RecordState::Unsupported,
                    operation.id,
                    recorded_at_ms,
                )?);
            }
            self.remove_lexical_document(&mut batch, dependency.record)?;
            self.remove_record_payload(&mut batch, dependency.record);
        }
        let mut purged_relation_signals =
            Vec::with_capacity(preview.relation_signal_invalidations.len());
        for dependency in &preview.relation_signal_invalidations {
            purged_relation_signals.push(self.append_relation_signal_state_revision(
                &mut batch,
                dependency.signal,
                RecordState::Purged,
                operation.id,
                recorded_at_ms,
            )?);
        }
        for trace_id in &preview.activation_trace_invalidations {
            batch.remove(&self.activation_trace_payloads, id_key(trace_id.0));
        }
        let receipt = PurgeReceipt {
            target,
            invalidated,
            purged_relation_signals,
            purged_activation_traces: preview.activation_trace_invalidations.clone(),
            payloads_made_unavailable: preview.payloads_to_make_unavailable,
        };
        let mut subjects = vec![preview.target];
        subjects.extend(preview.invalidations.iter().map(|entry| entry.record));
        let mut audit = self.new_audit_event(
            operation,
            AuditAction::PurgeCommitted,
            subjects,
            recorded_at_ms,
        );
        audit.derived_subjects = receipt
            .purged_relation_signals
            .iter()
            .map(|pin| DerivedRecordRef::RelationSignal(pin.id))
            .collect();
        audit.derived_subjects.extend(
            receipt
                .purged_activation_traces
                .iter()
                .copied()
                .map(DerivedRecordRef::ActivationTrace),
        );
        self.insert_audit(&mut batch, &audit)?;
        self.insert_operation(&mut batch, operation.id, digest, &receipt)?;
        commit(batch)?;
        Ok(receipt)
    }

    pub fn audit_events(&self, limit: usize) -> MemoryResult<Vec<AuditEvent>> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let mut events = Vec::new();
        for item in self.audit.iter() {
            let (_, value) = item.map_err(storage_error)?;
            events.push(decode(&value)?);
            if events.len() == limit {
                break;
            }
        }
        Ok(events)
    }

    fn remove_record_payload(&self, batch: &mut fjall::Batch, record: RecordRef) {
        batch.remove(&self.payloads, payload_key(record));
        if let RecordRef::ArtifactSnapshot(id) = record {
            batch.remove(&self.artifact_snapshot_blobs, id_key(id.0));
        }
    }

    fn mutation_digest<T: Serialize>(
        &self,
        mutation: &'static str,
        actor: Actor,
        input: &T,
    ) -> MemoryResult<[u8; 32]> {
        #[derive(Serialize)]
        struct DigestInput<'a, T> {
            mutation: &'a str,
            actor: Actor,
            input: &'a T,
        }
        let bytes = encode(&DigestInput {
            mutation,
            actor,
            input,
        })?;
        Ok(*blake3::keyed_hash(&self.digest_key, &bytes).as_bytes())
    }

    fn artifact_snapshot_mutation_digest(
        &self,
        actor: Actor,
        input: &NewArtifactSnapshot,
    ) -> MemoryResult<[u8; 32]> {
        #[derive(Serialize)]
        struct Metadata<'a> {
            collection_id: ArtifactCollectionId,
            expected_collection_revision: u64,
            temporal: TemporalFacts,
            media_type: &'a str,
        }
        let metadata = encode(&Metadata {
            collection_id: input.collection_id,
            expected_collection_revision: input.expected_collection_revision,
            temporal: input.temporal,
            media_type: &input.media_type,
        })?;
        let mut hasher = blake3::Hasher::new_keyed(&self.digest_key);
        update_framed_digest(&mut hasher, b"import_artifact_snapshot");
        update_framed_digest(&mut hasher, &[actor_tag(actor)]);
        update_framed_digest(&mut hasher, &metadata);
        update_framed_digest(&mut hasher, &input.bytes);
        Ok(*hasher.finalize().as_bytes())
    }

    fn replayed_operation<T: DeserializeOwned>(
        &self,
        id: OperationId,
        digest: [u8; 32],
    ) -> MemoryResult<Option<T>> {
        let Some(stored): Option<StoredOperation> = get_decoded(&self.operations, id_key(id.0))?
        else {
            return Ok(None);
        };
        if stored.digest != digest {
            return Err(MemoryError::OperationConflict(id));
        }
        Ok(Some(serde_json::from_value(stored.receipt)?))
    }

    fn insert_operation<T: Serialize>(
        &self,
        batch: &mut fjall::Batch,
        id: OperationId,
        digest: [u8; 32],
        receipt: &T,
    ) -> MemoryResult<()> {
        let stored = StoredOperation {
            digest,
            receipt: serde_json::to_value(receipt)?,
        };
        batch.insert(&self.operations, id_key(id.0), encode(&stored)?);
        Ok(())
    }

    fn durable_batch(&self) -> fjall::Batch {
        self.keyspace.batch().durability(Some(PersistMode::SyncAll))
    }

    fn new_audit_event(
        &self,
        operation: OperationContext,
        action: AuditAction,
        subjects: Vec<RecordRef>,
        recorded_at_ms: i64,
    ) -> AuditEvent {
        AuditEvent {
            id: AuditEventId::new(),
            era_id: self.era_id,
            operation_id: operation.id,
            actor: operation.actor,
            action,
            subjects,
            derived_subjects: Vec::new(),
            adjudication: None,
            outcome: AuditOutcome::Committed,
            recorded_at_ms,
        }
    }

    fn new_derived_audit_event(
        &self,
        operation: OperationContext,
        action: AuditAction,
        derived_subjects: Vec<DerivedRecordRef>,
        recorded_at_ms: i64,
    ) -> AuditEvent {
        let mut event = self.new_audit_event(operation, action, Vec::new(), recorded_at_ms);
        event.derived_subjects = derived_subjects;
        event
    }

    fn new_adjudication_audit_event(
        &self,
        operation: OperationContext,
        subjects: Vec<RecordRef>,
        recorded_at_ms: i64,
        adjudication: &ProposalAdjudication,
    ) -> AuditEvent {
        let mut event = self.new_audit_event(
            operation,
            AuditAction::ProposalAdjudicated,
            subjects,
            recorded_at_ms,
        );
        event.adjudication = Some(AdjudicationAudit {
            decision: adjudication.decision,
            authority: adjudication.authority,
        });
        event
    }

    fn insert_audit(&self, batch: &mut fjall::Batch, event: &AuditEvent) -> MemoryResult<()> {
        batch.insert(&self.audit, audit_key(event), encode(event)?);
        Ok(())
    }

    fn insert_lexical_document(
        &self,
        batch: &mut fjall::Batch,
        record: RecordRef,
        revision: u64,
        scope: Scope,
        temporal: TemporalFacts,
        text: &str,
    ) -> MemoryResult<()> {
        // Lexical postings are a bounded, rebuildable projection. Canonical
        // payload authority is not constrained by the index budget: retain the
        // exact payload and deterministically index the first distinct terms.
        let mut term_hashes = BTreeSet::new();
        for term in tokenize(text) {
            term_hashes.insert(self.index_hash(0x01, term.as_bytes()));
            if term_hashes.len() == MAX_INDEX_TERMS_PER_RECORD {
                break;
            }
        }
        let term_hashes: Vec<_> = term_hashes.into_iter().collect();
        let exact_hash = self.index_hash(0x02, normalize_exact(text).as_bytes());
        let document = LexicalDocument {
            record,
            revision,
            scope,
            temporal,
            term_hashes,
            exact_hash,
        };
        let key = record_key(record);
        batch.insert(&self.lexical_docs, key, encode(&document)?);
        for term_hash in &document.term_hashes {
            batch.insert(
                &self.lexical_postings,
                posting_key(0x01, term_hash, record),
                [],
            );
        }
        batch.insert(
            &self.lexical_postings,
            posting_key(0x02, &document.exact_hash, record),
            [],
        );
        batch.insert(
            &self.time_index,
            time_key(document.temporal.observed_at_ms, record),
            [],
        );
        Ok(())
    }

    fn remove_lexical_document(
        &self,
        batch: &mut fjall::Batch,
        record: RecordRef,
    ) -> MemoryResult<()> {
        let Some(document): Option<LexicalDocument> =
            get_decoded(&self.lexical_docs, record_key(record))?
        else {
            return Ok(());
        };
        for term_hash in &document.term_hashes {
            batch.remove(&self.lexical_postings, posting_key(0x01, term_hash, record));
        }
        batch.remove(
            &self.lexical_postings,
            posting_key(0x02, &document.exact_hash, record),
        );
        batch.remove(
            &self.time_index,
            time_key(document.temporal.observed_at_ms, record),
        );
        batch.remove(&self.lexical_docs, record_key(record));
        Ok(())
    }

    fn index_hash(&self, domain: u8, value: &[u8]) -> [u8; 32] {
        let mut input = Vec::with_capacity(value.len() + 1);
        input.push(domain);
        input.extend_from_slice(value);
        *blake3::keyed_hash(&self.digest_key, &input).as_bytes()
    }

    fn evidence_lifecycle_truth(&self, lifecycle: &EvidenceLifecycle) -> EvidenceLifecycleTruth {
        match lifecycle {
            EvidenceLifecycle::Direct => EvidenceLifecycleTruth::Direct,
            EvidenceLifecycle::TerminalTurn {
                source_event_id,
                status,
            } => EvidenceLifecycleTruth::TerminalTurn {
                source_event_digest: self.index_hash(0x05, source_event_id.as_bytes()),
                status: *status,
            },
        }
    }

    fn posting_records(
        &self,
        domain: u8,
        hash: &[u8; 32],
        excluded: &BTreeSet<RecordRef>,
    ) -> MemoryResult<Vec<RecordRef>> {
        let mut prefix = Vec::with_capacity(33);
        prefix.push(domain);
        prefix.extend_from_slice(hash);
        let mut upper = prefix.clone();
        upper.push(0xff);
        let mut records = Vec::new();
        for item in self.lexical_postings.range(prefix.clone()..upper) {
            let (key, _) = item.map_err(storage_error)?;
            if key.len() != prefix.len() + 17 {
                return Err(MemoryError::Corrupt("invalid lexical posting key".into()));
            }
            let record = record_from_key(&key[prefix.len()..])?;
            if excluded.contains(&record) {
                continue;
            }
            records.push(record);
            if records.len() == MAX_RECALL_CANDIDATES {
                break;
            }
        }
        Ok(records)
    }

    fn records_in_observed_range(
        &self,
        from_ms: Option<i64>,
        to_ms: Option<i64>,
        limit: usize,
        excluded: &BTreeSet<RecordRef>,
    ) -> MemoryResult<Vec<RecordRef>> {
        let lower = ordered_time(from_ms.unwrap_or(i64::MIN))
            .to_be_bytes()
            .to_vec();
        let upper = to_ms
            .map(|to| ordered_time(to).to_be_bytes().to_vec())
            .unwrap_or_else(|| vec![0xff; 26]);
        let mut records = Vec::new();
        for item in self.time_index.range(lower..upper).rev() {
            let (key, _) = item.map_err(storage_error)?;
            if key.len() != 25 {
                return Err(MemoryError::Corrupt("invalid observed-time key".into()));
            }
            let record = record_from_key(&key[8..])?;
            if excluded.contains(&record) {
                continue;
            }
            records.push(record);
            if records.len() == limit {
                break;
            }
        }
        Ok(records)
    }

    fn recallable_record(&self, record: RecordRef) -> MemoryResult<Option<RecallCitation>> {
        match record {
            RecordRef::Evidence(id) => {
                let Some(evidence) = self.inspect_evidence(id)? else {
                    return Ok(None);
                };
                if evidence.availability.state != RecordState::Active {
                    return Ok(None);
                }
                if evidence.header.class == EvidenceClass::AssistantUtterance {
                    return Ok(None);
                }
                let Some(text) = evidence.text else {
                    return Ok(None);
                };
                Ok(Some(RecallCitation {
                    record,
                    revision: evidence.availability.revision,
                    scope: evidence.header.scope,
                    temporal: evidence.header.temporal,
                    evidence_ids: vec![id],
                    evidence: vec![RecallEvidenceCitation {
                        id,
                        revision: evidence.availability.revision,
                        class: evidence.header.class,
                        lifecycle: evidence.header.lifecycle,
                        artifact: self.artifact_provenance_for_evidence(id)?,
                    }],
                    text,
                    exact_match: false,
                    matched_term_count: 0,
                }))
            }
            RecordRef::Claim(id) => {
                let Some(claim) = self.inspect_claim(id)? else {
                    return Ok(None);
                };
                if claim.header.state != RecordState::Active {
                    return Ok(None);
                }
                let Some(text) = claim.proposition else {
                    return Ok(None);
                };
                let Some(evidence) = self.recall_evidence_truth(&claim.header.evidence_ids)? else {
                    return Ok(None);
                };
                Ok(Some(RecallCitation {
                    record,
                    revision: claim.header.revision,
                    scope: claim.header.scope,
                    temporal: claim.header.temporal,
                    evidence_ids: claim.header.evidence_ids,
                    evidence,
                    text,
                    exact_match: false,
                    matched_term_count: 0,
                }))
            }
            RecordRef::Entity(id) => {
                let Some(entity) = self.inspect_entity(id)? else {
                    return Ok(None);
                };
                if entity.header.state != RecordState::Active {
                    return Ok(None);
                }
                let Some(text) = entity.canonical_name else {
                    return Ok(None);
                };
                let Some(evidence) = self.recall_evidence_truth(&entity.header.evidence_ids)?
                else {
                    return Ok(None);
                };
                Ok(Some(RecallCitation {
                    record,
                    revision: entity.header.revision,
                    scope: entity.header.scope,
                    temporal: entity.header.temporal,
                    evidence_ids: entity.header.evidence_ids,
                    evidence,
                    text,
                    exact_match: false,
                    matched_term_count: 0,
                }))
            }
            RecordRef::SemanticRelation(_)
            | RecordRef::Proposal(_)
            | RecordRef::ProposalReview(_)
            | RecordRef::ArtifactCollection(_)
            | RecordRef::ArtifactSnapshot(_)
            | RecordRef::ArtifactPassage(_) => Ok(None),
        }
    }

    fn recall_evidence_truth(
        &self,
        evidence_ids: &[EvidenceId],
    ) -> MemoryResult<Option<Vec<RecallEvidenceCitation>>> {
        let mut citations = Vec::with_capacity(evidence_ids.len());
        for id in evidence_ids {
            let Some(evidence) = self.inspect_evidence(*id)? else {
                return Ok(None);
            };
            if evidence.availability.state != RecordState::Active
                || evidence.text.is_none()
                || evidence.header.class == EvidenceClass::AssistantUtterance
            {
                return Ok(None);
            }
            citations.push(RecallEvidenceCitation {
                id: *id,
                revision: evidence.availability.revision,
                class: evidence.header.class,
                lifecycle: evidence.header.lifecycle,
                artifact: self.artifact_provenance_for_evidence(*id)?,
            });
        }
        Ok(Some(citations))
    }

    fn build_purge_preview_at(
        &self,
        target: RecordRef,
        issued_at_ms: i64,
        expires_at_ms: i64,
    ) -> MemoryResult<PurgePreview> {
        let (_, target_state, expected_revision) = self.record_scope_state_revision(target)?;
        if target_state == RecordState::Purged {
            return Err(MemoryError::InvalidInput(
                "record payload was already purged".into(),
            ));
        }
        let invalidations = self.purge_dependency_closure(target)?;
        let dependency_records: Vec<_> = std::iter::once(target)
            .chain(invalidations.iter().map(|dependency| dependency.record))
            .collect();
        let relation_signal_invalidations =
            self.relation_signals_depending_on_records(&dependency_records)?;
        let activation_trace_invalidations =
            self.activation_traces_depending_on_records(&dependency_records)?;
        let mut payloads_to_make_unavailable = 0_u32;
        for record in &dependency_records {
            if self
                .payloads
                .get(payload_key(*record))
                .map_err(storage_error)?
                .is_some()
            {
                payloads_to_make_unavailable = payloads_to_make_unavailable
                    .checked_add(1)
                    .ok_or_else(|| MemoryError::InvalidInput("too many purge payloads".into()))?;
            }
        }
        for dependency in &relation_signal_invalidations {
            if self
                .relation_signal_payloads
                .get(id_key(dependency.signal.id.0))
                .map_err(storage_error)?
                .is_some()
            {
                payloads_to_make_unavailable = payloads_to_make_unavailable
                    .checked_add(1)
                    .ok_or_else(|| MemoryError::InvalidInput("too many purge payloads".into()))?;
            }
        }
        for trace_id in &activation_trace_invalidations {
            if self
                .activation_trace_payloads
                .get(id_key(trace_id.0))
                .map_err(storage_error)?
                .is_some()
            {
                payloads_to_make_unavailable = payloads_to_make_unavailable
                    .checked_add(1)
                    .ok_or_else(|| MemoryError::InvalidInput("too many purge payloads".into()))?;
            }
        }
        let token_input = PurgeTokenInput {
            era_id: self.era_id,
            target,
            expected_revision,
            payloads_to_make_unavailable,
            issued_at_ms,
            expires_at_ms,
            invalidations: &invalidations,
            relation_signal_invalidations: &relation_signal_invalidations,
            activation_trace_invalidations: &activation_trace_invalidations,
        };
        let token = self.index_hash(0x04, &encode(&token_input)?);
        Ok(PurgePreview {
            target,
            expected_revision,
            payloads_to_make_unavailable,
            invalidations,
            relation_signal_invalidations,
            activation_trace_invalidations,
            issued_at_ms,
            expires_at_ms,
            token,
        })
    }

    fn activation_traces_depending_on_records(
        &self,
        records: &[RecordRef],
    ) -> MemoryResult<Vec<ActivationTraceId>> {
        let mut ids = BTreeSet::new();
        for record in records {
            let prefix = record_key(*record);
            let mut upper = prefix.clone();
            upper.push(0xff);
            for item in self
                .activation_traces_by_record
                .range(prefix.clone()..upper)
            {
                let (key, _) = item.map_err(storage_error)?;
                if key.len() != prefix.len() + 16 {
                    return Err(MemoryError::Corrupt(
                        "invalid activation-trace record dependency key".into(),
                    ));
                }
                let id = ActivationTraceId(ulid_from_id_key(&key[prefix.len()..])?);
                if self
                    .activation_trace_payloads
                    .get(id_key(id.0))
                    .map_err(storage_error)?
                    .is_some()
                {
                    ids.insert(id);
                }
            }
        }
        Ok(ids.into_iter().collect())
    }

    fn relation_signals_depending_on_records(
        &self,
        records: &[RecordRef],
    ) -> MemoryResult<Vec<RelationSignalPurgeDependency>> {
        let mut ids = BTreeSet::new();
        for record in records {
            let prefix = record_key(*record);
            let mut upper = prefix.clone();
            upper.push(0xff);
            for item in self.relation_signals_by_record.range(prefix.clone()..upper) {
                let (key, _) = item.map_err(storage_error)?;
                if key.len() != prefix.len() + 16 {
                    return Err(MemoryError::Corrupt(
                        "invalid relation-signal record dependency key".into(),
                    ));
                }
                ids.insert(RelationSignalId(ulid_from_id_key(&key[prefix.len()..])?));
            }
        }
        let mut dependencies = Vec::new();
        for id in ids {
            let Some(revision) = get_head(&self.relation_signal_heads, id.0)? else {
                return Err(MemoryError::Corrupt(format!(
                    "relation-signal dependency index points to missing signal {id}"
                )));
            };
            let header: RelationSignalRevisionHeader =
                get_decoded(&self.relation_signals, revision_key(id.0, revision))?.ok_or_else(
                    || {
                        MemoryError::Corrupt(format!(
                            "relation signal {id} is missing revision {revision}"
                        ))
                    },
                )?;
            if header.state != RecordState::Purged {
                dependencies.push(RelationSignalPurgeDependency {
                    signal: RelationSignalPin { id, revision },
                });
            }
        }
        dependencies.sort_by_key(|dependency| dependency.signal);
        Ok(dependencies)
    }

    fn active_dependency_closure(&self, target: RecordRef) -> MemoryResult<Vec<PurgeDependency>> {
        self.dependency_closure(target, false)
    }

    fn purge_dependency_closure(&self, target: RecordRef) -> MemoryResult<Vec<PurgeDependency>> {
        self.dependency_closure(target, true)
    }

    fn dependency_closure(
        &self,
        target: RecordRef,
        include_inactive: bool,
    ) -> MemoryResult<Vec<PurgeDependency>> {
        let mut seen = BTreeSet::from([target]);
        let mut queue = VecDeque::from([target]);
        let mut dependencies = Vec::new();
        while let Some(source) = queue.pop_front() {
            let prefix = record_key(source);
            let mut upper = prefix.clone();
            upper.push(0xff);
            for item in self.dependencies.range(prefix.clone()..upper) {
                let (key, _) = item.map_err(storage_error)?;
                if key.len() != prefix.len() + 17 {
                    return Err(MemoryError::Corrupt("invalid dependency key".into()));
                }
                let dependent = record_from_key(&key[prefix.len()..])?;
                if !seen.insert(dependent) {
                    continue;
                }
                let (_, state, revision) = self.record_scope_state_revision(dependent)?;
                // Always keep traversing historical edges. An inactive record
                // can still be the only path to a live descendant, and privacy
                // purge must reach every non-purged descendant.
                queue.push_back(dependent);
                if state == RecordState::Active
                    || (include_inactive && state != RecordState::Purged)
                {
                    dependencies.push(PurgeDependency {
                        record: dependent,
                        expected_revision: revision,
                    });
                }
            }
        }
        dependencies.sort_by_key(|entry| entry.record);
        Ok(dependencies)
    }

    fn append_relation_signal_state_revision(
        &self,
        batch: &mut fjall::Batch,
        pin: RelationSignalPin,
        state: RecordState,
        operation_id: OperationId,
        recorded_at_ms: i64,
    ) -> MemoryResult<RelationSignalPin> {
        let actual = get_head(&self.relation_signal_heads, pin.id.0)?.ok_or_else(|| {
            MemoryError::Corrupt(format!("relation signal {} has no head", pin.id))
        })?;
        if actual != pin.revision {
            return Err(MemoryError::RevisionConflict {
                expected: pin.revision,
                actual,
            });
        }
        let mut next: RelationSignalRevisionHeader =
            get_decoded(&self.relation_signals, revision_key(pin.id.0, pin.revision))?.ok_or_else(
                || {
                    MemoryError::Corrupt(format!(
                        "relation signal {} is missing revision {}",
                        pin.id, pin.revision
                    ))
                },
            )?;
        let revision = pin
            .revision
            .checked_add(1)
            .ok_or_else(|| MemoryError::Corrupt("relation signal revision overflow".into()))?;
        next.revision = revision;
        next.previous_revision = Some(pin.revision);
        next.state = state;
        next.recorded_at_ms = recorded_at_ms;
        next.operation_id = operation_id;
        batch.insert(
            &self.relation_signals,
            revision_key(pin.id.0, revision),
            encode(&next)?,
        );
        batch.insert(
            &self.relation_signal_heads,
            id_key(pin.id.0),
            revision.to_be_bytes(),
        );
        if state == RecordState::Purged {
            batch.remove(&self.relation_signal_payloads, id_key(pin.id.0));
        }
        Ok(RelationSignalPin {
            id: pin.id,
            revision,
        })
    }

    fn relation_signal_pins_for_owner(
        &self,
        index: &PartitionHandle,
        owner_id: Ulid,
    ) -> MemoryResult<Vec<RelationSignalPin>> {
        let prefix = id_key(owner_id).to_vec();
        let mut upper = prefix.clone();
        upper.push(0xff);
        let mut pins = Vec::new();
        for item in index.range(prefix.clone()..upper) {
            let (key, _) = item.map_err(storage_error)?;
            if key.len() != prefix.len() + 16 {
                return Err(MemoryError::Corrupt(
                    "invalid relation-signal owner dependency key".into(),
                ));
            }
            let id = RelationSignalId(ulid_from_id_key(&key[prefix.len()..])?);
            let revision = get_head(&self.relation_signal_heads, id.0)?.ok_or_else(|| {
                MemoryError::Corrupt(format!(
                    "relation-signal owner index points to missing signal {id}"
                ))
            })?;
            let header: RelationSignalRevisionHeader =
                get_decoded(&self.relation_signals, revision_key(id.0, revision))?.ok_or_else(
                    || {
                        MemoryError::Corrupt(format!(
                            "relation signal {id} is missing revision {revision}"
                        ))
                    },
                )?;
            if header.state != RecordState::Purged {
                pins.push(RelationSignalPin { id, revision });
            }
        }
        pins.sort();
        pins.dedup();
        Ok(pins)
    }

    fn activation_trace_ids_for_owner(
        &self,
        index: &PartitionHandle,
        owner_id: Ulid,
    ) -> MemoryResult<Vec<ActivationTraceId>> {
        let prefix = id_key(owner_id).to_vec();
        let mut upper = prefix.clone();
        upper.push(0xff);
        let mut ids = Vec::new();
        for item in index.range(prefix.clone()..upper) {
            let (key, _) = item.map_err(storage_error)?;
            if key.len() != prefix.len() + 16 {
                return Err(MemoryError::Corrupt(
                    "invalid activation-trace owner dependency key".into(),
                ));
            }
            let id = ActivationTraceId(ulid_from_id_key(&key[prefix.len()..])?);
            if self
                .activation_trace_payloads
                .get(id_key(id.0))
                .map_err(storage_error)?
                .is_some()
            {
                ids.push(id);
            }
        }
        ids.sort();
        ids.dedup();
        Ok(ids)
    }

    fn append_state_revision(
        &self,
        batch: &mut fjall::Batch,
        record: RecordRef,
        expected_revision: u64,
        state: RecordState,
        operation_id: OperationId,
        recorded_at_ms: i64,
    ) -> MemoryResult<RecordRevision> {
        let (_, _, actual_revision) = self.record_scope_state_revision(record)?;
        if actual_revision != expected_revision {
            return Err(MemoryError::RevisionConflict {
                expected: expected_revision,
                actual: actual_revision,
            });
        }
        let revision = expected_revision
            .checked_add(1)
            .ok_or_else(|| MemoryError::Corrupt("revision counter overflow".into()))?;
        match record {
            RecordRef::Evidence(id) => {
                let current: EvidenceAvailabilityRevision = get_decoded(
                    &self.evidence_availability,
                    revision_key(id.0, expected_revision),
                )?
                .ok_or_else(|| {
                    MemoryError::Corrupt(format!(
                        "evidence {id} is missing availability revision {expected_revision}"
                    ))
                })?;
                let next = EvidenceAvailabilityRevision {
                    evidence_id: id,
                    revision,
                    state,
                    recorded_at_ms,
                    operation_id,
                };
                debug_assert_eq!(current.evidence_id, next.evidence_id);
                batch.insert(
                    &self.evidence_availability,
                    revision_key(id.0, revision),
                    encode(&next)?,
                );
                batch.insert(&self.evidence_heads, id_key(id.0), revision.to_be_bytes());
            }
            RecordRef::Claim(id) => {
                let current = self
                    .inspect_claim(id)?
                    .ok_or(MemoryError::NotFound(record))?;
                let mut next = current.header;
                next.revision = revision;
                next.previous_revision = Some(expected_revision);
                next.state = state;
                next.recorded_at_ms = recorded_at_ms;
                next.operation_id = operation_id;
                batch.insert(&self.claims, revision_key(id.0, revision), encode(&next)?);
                batch.insert(&self.claim_heads, id_key(id.0), revision.to_be_bytes());
            }
            RecordRef::Entity(id) => {
                let current = self
                    .inspect_entity(id)?
                    .ok_or(MemoryError::NotFound(record))?;
                let mut next = current.header;
                next.revision = revision;
                next.previous_revision = Some(expected_revision);
                next.state = state;
                next.recorded_at_ms = recorded_at_ms;
                next.operation_id = operation_id;
                batch.insert(&self.entities, revision_key(id.0, revision), encode(&next)?);
                batch.insert(&self.entity_heads, id_key(id.0), revision.to_be_bytes());
            }
            RecordRef::SemanticRelation(id) => {
                let current = self
                    .inspect_semantic_relation(id)?
                    .ok_or(MemoryError::NotFound(record))?;
                let mut next = current.header;
                next.revision = revision;
                next.previous_revision = Some(expected_revision);
                next.state = state;
                next.recorded_at_ms = recorded_at_ms;
                next.operation_id = operation_id;
                batch.insert(
                    &self.relations,
                    revision_key(id.0, revision),
                    encode(&next)?,
                );
                batch.insert(&self.relation_heads, id_key(id.0), revision.to_be_bytes());
            }
            RecordRef::Proposal(id) => {
                let current = self
                    .inspect_proposal(id)?
                    .ok_or(MemoryError::NotFound(record))?;
                if current.header.status == ProposalStatus::PendingReview {
                    batch.remove(&self.pending_proposals, id_key(id.0));
                }
                if proposal_status_awaits_adjudication(current.header.status) {
                    batch.remove(&self.awaiting_adjudication, id_key(id.0));
                }
                let mut next = current.header;
                next.revision = revision;
                next.previous_revision = Some(expected_revision);
                next.state = state;
                if state == RecordState::Unsupported {
                    next.status = ProposalStatus::Stale;
                } else if state == RecordState::Retracted {
                    next.status = ProposalStatus::Rejected;
                }
                next.recorded_at_ms = recorded_at_ms;
                next.operation_id = operation_id;
                batch.insert(
                    &self.proposals,
                    revision_key(id.0, revision),
                    encode(&next)?,
                );
                batch.insert(&self.proposal_heads, id_key(id.0), revision.to_be_bytes());
            }
            RecordRef::ProposalReview(id) => {
                let current = self
                    .inspect_proposal_review(id)?
                    .ok_or(MemoryError::NotFound(record))?;
                let mut next = current.header;
                next.revision = revision;
                next.previous_revision = Some(expected_revision);
                next.state = state;
                next.recorded_at_ms = recorded_at_ms;
                next.operation_id = operation_id;
                batch.insert(
                    &self.proposal_reviews,
                    revision_key(id.0, revision),
                    encode(&next)?,
                );
                batch.insert(
                    &self.proposal_review_heads,
                    id_key(id.0),
                    revision.to_be_bytes(),
                );
            }
            RecordRef::ArtifactCollection(id) => {
                let next = ArtifactAvailabilityRevision {
                    id,
                    revision,
                    state,
                    recorded_at_ms,
                    operation_id,
                };
                batch.insert(
                    &self.artifact_collection_availability,
                    revision_key(id.0, revision),
                    encode(&next)?,
                );
                batch.insert(
                    &self.artifact_collection_heads,
                    id_key(id.0),
                    revision.to_be_bytes(),
                );
            }
            RecordRef::ArtifactSnapshot(id) => {
                let next = ArtifactAvailabilityRevision {
                    id,
                    revision,
                    state,
                    recorded_at_ms,
                    operation_id,
                };
                batch.insert(
                    &self.artifact_snapshot_availability,
                    revision_key(id.0, revision),
                    encode(&next)?,
                );
                batch.insert(
                    &self.artifact_snapshot_heads,
                    id_key(id.0),
                    revision.to_be_bytes(),
                );
            }
            RecordRef::ArtifactPassage(id) => {
                let next = ArtifactAvailabilityRevision {
                    id,
                    revision,
                    state,
                    recorded_at_ms,
                    operation_id,
                };
                batch.insert(
                    &self.artifact_passage_availability,
                    revision_key(id.0, revision),
                    encode(&next)?,
                );
                batch.insert(
                    &self.artifact_passage_heads,
                    id_key(id.0),
                    revision.to_be_bytes(),
                );
            }
        }
        Ok(RecordRevision {
            record,
            revision,
            state,
        })
    }

    fn record_scope_state_revision(
        &self,
        record: RecordRef,
    ) -> MemoryResult<(Scope, RecordState, u64)> {
        match record {
            RecordRef::Evidence(id) => {
                let evidence = self
                    .inspect_evidence(id)?
                    .ok_or(MemoryError::NotFound(record))?;
                Ok((
                    evidence.header.scope,
                    evidence.availability.state,
                    evidence.availability.revision,
                ))
            }
            RecordRef::Claim(id) => {
                let claim = self
                    .inspect_claim(id)?
                    .ok_or(MemoryError::NotFound(record))?;
                Ok((
                    claim.header.scope,
                    claim.header.state,
                    claim.header.revision,
                ))
            }
            RecordRef::Entity(id) => {
                let entity = self
                    .inspect_entity(id)?
                    .ok_or(MemoryError::NotFound(record))?;
                Ok((
                    entity.header.scope,
                    entity.header.state,
                    entity.header.revision,
                ))
            }
            RecordRef::SemanticRelation(id) => {
                let relation = self
                    .inspect_semantic_relation(id)?
                    .ok_or(MemoryError::NotFound(record))?;
                Ok((
                    relation.header.scope,
                    relation.header.state,
                    relation.header.revision,
                ))
            }
            RecordRef::Proposal(id) => {
                let proposal = self
                    .inspect_proposal(id)?
                    .ok_or(MemoryError::NotFound(record))?;
                Ok((
                    proposal.header.scope,
                    proposal.header.state,
                    proposal.header.revision,
                ))
            }
            RecordRef::ProposalReview(id) => {
                let review = self
                    .inspect_proposal_review(id)?
                    .ok_or(MemoryError::NotFound(record))?;
                Ok((
                    review.header.scope,
                    review.header.state,
                    review.header.revision,
                ))
            }
            RecordRef::ArtifactCollection(id) => {
                let artifact = self
                    .inspect_artifact_collection(id)?
                    .ok_or(MemoryError::NotFound(record))?;
                Ok((
                    artifact.header.scope,
                    artifact.availability.state,
                    artifact.availability.revision,
                ))
            }
            RecordRef::ArtifactSnapshot(id) => {
                let artifact = self
                    .inspect_artifact_snapshot(id)?
                    .ok_or(MemoryError::NotFound(record))?;
                Ok((
                    artifact.header.scope,
                    artifact.availability.state,
                    artifact.availability.revision,
                ))
            }
            RecordRef::ArtifactPassage(id) => {
                let artifact = self
                    .inspect_artifact_passage(id)?
                    .ok_or(MemoryError::NotFound(record))?;
                Ok((
                    artifact.header.scope,
                    artifact.availability.state,
                    artifact.availability.revision,
                ))
            }
        }
    }

    fn require_current_pin(&self, pin: RecordRevisionPin) -> MemoryResult<()> {
        let (_, state, revision) = self.record_scope_state_revision(pin.record)?;
        if revision != pin.revision {
            return Err(MemoryError::RevisionConflict {
                expected: pin.revision,
                actual: revision,
            });
        }
        if state != RecordState::Active {
            return Err(MemoryError::InvalidInput(
                "pinned record is no longer active".into(),
            ));
        }
        Ok(())
    }

    fn evidence_pin_is_current(
        &self,
        pin: EvidenceRevisionPin,
        scope: &Scope,
    ) -> MemoryResult<bool> {
        let Some(evidence) = self.inspect_evidence(pin.id)? else {
            return Ok(false);
        };
        Ok(evidence.header.scope == *scope
            && evidence.availability.state == RecordState::Active
            && evidence.availability.revision == pin.revision)
    }

    fn record_pin_is_current(&self, pin: RecordRevisionPin, scope: &Scope) -> MemoryResult<bool> {
        match self.record_scope_state_revision(pin.record) {
            Ok((actual_scope, state, revision)) => Ok(actual_scope == *scope
                && state == RecordState::Active
                && revision == pin.revision),
            Err(MemoryError::NotFound(_)) => Ok(false),
            Err(error) => Err(error),
        }
    }

    fn record_pin_revision_is_current(&self, pin: RecordRevisionPin) -> MemoryResult<bool> {
        match self.record_scope_state_revision(pin.record) {
            Ok((_, state, revision)) => {
                Ok(state == RecordState::Active && revision == pin.revision)
            }
            Err(MemoryError::NotFound(_)) => Ok(false),
            Err(error) => Err(error),
        }
    }

    fn proposal_evidence_classes(
        &self,
        pins: &[EvidenceRevisionPin],
        scope: &Scope,
    ) -> MemoryResult<BTreeMap<EvidenceId, EvidenceClass>> {
        let mut classes = BTreeMap::new();
        for pin in pins {
            let evidence = self
                .inspect_evidence(pin.id)?
                .ok_or(MemoryError::SourceUnavailable(pin.id))?;
            if evidence.header.scope != *scope
                || evidence.availability.state != RecordState::Active
                || evidence.availability.revision != pin.revision
            {
                return Err(MemoryError::SourceUnavailable(pin.id));
            }
            classes.insert(pin.id, evidence.header.class);
        }
        Ok(classes)
    }

    fn user_may_activate_endpoint(&self, record: RecordRef) -> MemoryResult<bool> {
        match record {
            RecordRef::Evidence(id) => Ok(self.inspect_evidence(id)?.is_some_and(|evidence| {
                matches!(
                    evidence.header.class,
                    EvidenceClass::UserAssertion | EvidenceClass::UserCorrection
                )
            })),
            RecordRef::Claim(id) => Ok(self.inspect_claim(id)?.is_some_and(|claim| {
                matches!(
                    claim.header.domain,
                    ClaimDomain::UserProfile
                        | ClaimDomain::UserPreference
                        | ClaimDomain::UserNote
                        | ClaimDomain::SessionContext
                )
            })),
            RecordRef::Entity(id) => Ok(self.inspect_entity(id)?.is_some_and(|entity| {
                entity.header.evidence_ids.iter().all(|evidence_id| {
                    self.inspect_evidence(*evidence_id)
                        .ok()
                        .flatten()
                        .is_some_and(|evidence| {
                            matches!(
                                evidence.header.class,
                                EvidenceClass::UserAssertion | EvidenceClass::UserCorrection
                            )
                        })
                })
            })),
            RecordRef::SemanticRelation(_)
            | RecordRef::Proposal(_)
            | RecordRef::ProposalReview(_)
            | RecordRef::ArtifactCollection(_)
            | RecordRef::ArtifactSnapshot(_)
            | RecordRef::ArtifactPassage(_) => Ok(false),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ProposalDraftKind {
    Claim,
    Entity,
    Relation,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SemanticEndpointKind {
    Evidence,
    Claim,
    Entity,
}

fn semantic_endpoint_kind(record: RecordRef) -> Option<SemanticEndpointKind> {
    match record {
        RecordRef::Evidence(_) => Some(SemanticEndpointKind::Evidence),
        RecordRef::Claim(_) => Some(SemanticEndpointKind::Claim),
        RecordRef::Entity(_) => Some(SemanticEndpointKind::Entity),
        RecordRef::SemanticRelation(_)
        | RecordRef::Proposal(_)
        | RecordRef::ProposalReview(_)
        | RecordRef::ArtifactCollection(_)
        | RecordRef::ArtifactSnapshot(_)
        | RecordRef::ArtifactPassage(_) => None,
    }
}

fn proposal_endpoint_kind(
    endpoint: ProposalEndpoint,
    drafts: &BTreeMap<ProposalDraftId, ProposalDraftKind>,
) -> MemoryResult<SemanticEndpointKind> {
    match endpoint {
        ProposalEndpoint::Existing(pin) => semantic_endpoint_kind(pin.record).ok_or_else(|| {
            MemoryError::InvalidInput(
                "relation endpoints must be evidence, claims, or entities".into(),
            )
        }),
        ProposalEndpoint::Draft(id) => match drafts.get(&id) {
            Some(ProposalDraftKind::Claim) => Ok(SemanticEndpointKind::Claim),
            Some(ProposalDraftKind::Entity) => Ok(SemanticEndpointKind::Entity),
            Some(ProposalDraftKind::Relation) | None => Err(MemoryError::InvalidInput(
                "relation draft endpoints must resolve to a claim or entity".into(),
            )),
        },
    }
}

fn validate_relation_endpoint_kinds(
    kind: RelationKind,
    from: SemanticEndpointKind,
    to: SemanticEndpointKind,
) -> MemoryResult<()> {
    let valid = match kind {
        RelationKind::Supports | RelationKind::Contradicts => {
            matches!(
                from,
                SemanticEndpointKind::Evidence | SemanticEndpointKind::Claim
            ) && to == SemanticEndpointKind::Claim
        }
        RelationKind::Supersedes => matches!(
            (from, to),
            (SemanticEndpointKind::Claim, SemanticEndpointKind::Claim)
                | (SemanticEndpointKind::Entity, SemanticEndpointKind::Entity)
        ),
        RelationKind::About => matches!(
            (from, to),
            (SemanticEndpointKind::Entity, SemanticEndpointKind::Evidence)
                | (SemanticEndpointKind::Entity, SemanticEndpointKind::Claim)
                | (SemanticEndpointKind::Evidence, SemanticEndpointKind::Entity)
                | (SemanticEndpointKind::Claim, SemanticEndpointKind::Entity)
        ),
        RelationKind::RefersTo => {
            to == SemanticEndpointKind::Entity
                && matches!(
                    from,
                    SemanticEndpointKind::Evidence
                        | SemanticEndpointKind::Claim
                        | SemanticEndpointKind::Entity
                )
        }
        RelationKind::DerivedFrom => {
            matches!(
                from,
                SemanticEndpointKind::Claim | SemanticEndpointKind::Entity
            )
        }
        RelationKind::CanonicalizesTo => {
            from == SemanticEndpointKind::Entity && to == SemanticEndpointKind::Entity
        }
    };
    if valid {
        Ok(())
    } else if kind == RelationKind::Supersedes {
        Err(MemoryError::InvalidInput(
            "Supersedes requires same-kind claim or entity endpoints".into(),
        ))
    } else {
        Err(MemoryError::InvalidInput(format!(
            "{kind:?} relation endpoints do not match the relation kind"
        )))
    }
}

fn validate_adjudication_authority(
    actor: Actor,
    authority: AdjudicationAuthority,
) -> MemoryResult<()> {
    let authorized = matches!(
        (actor, authority),
        (Actor::User, AdjudicationAuthority::ExplicitUser)
            | (Actor::Operator, AdjudicationAuthority::ExplicitOperator)
    );
    if authorized {
        Ok(())
    } else {
        Err(MemoryError::Unauthorized)
    }
}

fn next_proposal_status(
    current: &ProposalRevisionHeader,
    status: ProposalStatus,
    operation_id: OperationId,
    recorded_at_ms: i64,
) -> MemoryResult<(ProposalRevisionHeader, ProposalReceipt)> {
    let revision = current
        .revision
        .checked_add(1)
        .ok_or_else(|| MemoryError::Corrupt("proposal revision overflow".into()))?;
    let mut next = current.clone();
    next.revision = revision;
    next.previous_revision = Some(current.revision);
    next.status = status;
    next.recorded_at_ms = recorded_at_ms;
    next.operation_id = operation_id;
    let receipt = proposal_receipt(&next);
    Ok((next, receipt))
}

fn entity_index_text(canonical_name: &str, aliases: &[String]) -> String {
    let extra_bytes: usize = aliases.iter().map(|alias| alias.len() + 1).sum();
    let mut text = String::with_capacity(canonical_name.len() + extra_bytes);
    text.push_str(canonical_name);
    for alias in aliases {
        text.push(' ');
        text.push_str(alias);
    }
    text
}

fn resolve_proposal_endpoint(
    endpoint: ProposalEndpoint,
    drafts: &BTreeMap<ProposalDraftId, AppliedRecord>,
) -> MemoryResult<RecordRef> {
    match endpoint {
        ProposalEndpoint::Existing(pin) => Ok(pin.record),
        ProposalEndpoint::Draft(id) => drafts
            .get(&id)
            .copied()
            .map(AppliedRecord::as_record_ref)
            .ok_or_else(|| MemoryError::InvalidInput("unresolved proposal draft endpoint".into())),
    }
}

fn validate_proposal_activation(
    actor: Actor,
    _scope: &Scope,
    changes: &[ProposalChange],
    evidence_classes: &BTreeMap<EvidenceId, EvidenceClass>,
) -> MemoryResult<()> {
    for change in changes {
        let evidence_ids = match change {
            ProposalChange::CreateClaim { evidence_ids, .. }
            | ProposalChange::CreateEntity { evidence_ids, .. }
            | ProposalChange::CreateRelation { evidence_ids, .. } => evidence_ids,
            ProposalChange::Retract { .. } | ProposalChange::Supersede { .. } => continue,
        };
        let classes: Vec<_> = evidence_ids
            .iter()
            .map(|id| {
                evidence_classes.get(id).copied().ok_or_else(|| {
                    MemoryError::InvalidInput("proposed evidence is not pinned".into())
                })
            })
            .collect::<MemoryResult<_>>()?;
        match change {
            ProposalChange::CreateClaim {
                domain,
                evidence_ids,
                ..
            } => {
                for (evidence_id, class) in evidence_ids.iter().zip(classes.iter().copied()) {
                    if !source_is_admissible(class, *domain) {
                        return Err(MemoryError::InadmissibleSource {
                            evidence_id: *evidence_id,
                            class,
                            domain: *domain,
                        });
                    }
                }
                validate_claim_activation(actor, *domain, &classes)?;
            }
            ProposalChange::CreateEntity { .. } => {
                let authorized = match actor {
                    Actor::Assistant => false,
                    Actor::System | Actor::Operator => true,
                    Actor::User => classes.iter().all(|class| {
                        matches!(
                            class,
                            EvidenceClass::UserAssertion | EvidenceClass::UserCorrection
                        )
                    }),
                };
                if !authorized {
                    return Err(MemoryError::Unauthorized);
                }
            }
            ProposalChange::CreateRelation { .. } => {
                validate_relation_activation(actor, &classes)?;
            }
            ProposalChange::Retract { .. } | ProposalChange::Supersede { .. } => unreachable!(),
        }
    }
    if changes
        .iter()
        .any(|change| matches!(change, ProposalChange::Supersede { .. }))
    {
        let classes: Vec<_> = evidence_classes.values().copied().collect();
        validate_relation_activation(actor, &classes)?;
    }
    Ok(())
}

fn proposal_receipt(header: &ProposalRevisionHeader) -> ProposalReceipt {
    ProposalReceipt {
        id: header.id,
        revision: header.revision,
        status: header.status,
    }
}

fn proposal_status_awaits_adjudication(status: ProposalStatus) -> bool {
    matches!(
        status,
        ProposalStatus::ReviewedApprove
            | ProposalStatus::ReviewedReject
            | ProposalStatus::NeedsUser
    )
}

fn normalize_evidence_ids(
    evidence_ids: &mut Vec<EvidenceId>,
    source_ids: &BTreeSet<EvidenceId>,
) -> MemoryResult<()> {
    evidence_ids.sort();
    evidence_ids.dedup();
    if evidence_ids.is_empty() {
        return Err(MemoryError::InvalidInput(
            "a proposed semantic record requires evidence".into(),
        ));
    }
    if evidence_ids.len() > MAX_EVIDENCE_SOURCES {
        return Err(MemoryError::InvalidInput(format!(
            "a proposed semantic record cannot cite more than {MAX_EVIDENCE_SOURCES} sources"
        )));
    }
    if evidence_ids.iter().any(|id| !source_ids.contains(id)) {
        return Err(MemoryError::InvalidInput(
            "every proposed evidence ID must have an exact source pin".into(),
        ));
    }
    Ok(())
}

fn normalize_proposal_bundle(mut input: NewProposalBundle) -> MemoryResult<NewProposalBundle> {
    input.source_evidence.sort();
    input.source_evidence.dedup();
    if input.source_evidence.is_empty() {
        return Err(MemoryError::InvalidInput(
            "a proposal requires at least one pinned evidence source".into(),
        ));
    }
    if input.source_evidence.len() > MAX_EVIDENCE_SOURCES {
        return Err(MemoryError::InvalidInput(format!(
            "a proposal cannot pin more than {MAX_EVIDENCE_SOURCES} evidence sources"
        )));
    }
    if input.changes.is_empty() || input.changes.len() > MAX_PROPOSAL_CHANGES {
        return Err(MemoryError::InvalidInput(format!(
            "a proposal must contain 1..={MAX_PROPOSAL_CHANGES} changes"
        )));
    }
    let source_ids: BTreeSet<_> = input.source_evidence.iter().map(|pin| pin.id).collect();
    let mut draft_kinds = BTreeMap::new();
    let mut mutation_targets = BTreeSet::new();
    let mut total_aliases = 0_usize;

    for change in &mut input.changes {
        let draft = match change {
            ProposalChange::CreateClaim {
                draft_id,
                temporal,
                proposition,
                evidence_ids,
                ..
            } => {
                temporal.validate()?;
                *proposition = validate_text(std::mem::take(proposition), "claim proposition")?;
                normalize_evidence_ids(evidence_ids, &source_ids)?;
                Some((*draft_id, ProposalDraftKind::Claim))
            }
            ProposalChange::CreateEntity {
                draft_id,
                temporal,
                canonical_name,
                aliases,
                evidence_ids,
                ..
            } => {
                temporal.validate()?;
                *canonical_name =
                    validate_text(std::mem::take(canonical_name), "entity canonical name")?;
                if aliases.len() > MAX_ENTITY_ALIASES {
                    return Err(MemoryError::InvalidInput(format!(
                        "an entity cannot have more than {MAX_ENTITY_ALIASES} aliases"
                    )));
                }
                for alias in aliases.iter_mut() {
                    *alias = validate_text(std::mem::take(alias), "entity alias")?;
                }
                aliases.sort();
                aliases.dedup();
                total_aliases = total_aliases.checked_add(aliases.len()).ok_or_else(|| {
                    MemoryError::InvalidInput("proposal total aliases overflow".into())
                })?;
                if total_aliases > MAX_PROPOSAL_ALIASES {
                    return Err(MemoryError::InvalidInput(format!(
                        "proposal cannot contain more than {MAX_PROPOSAL_ALIASES} total aliases"
                    )));
                }
                normalize_evidence_ids(evidence_ids, &source_ids)?;
                Some((*draft_id, ProposalDraftKind::Entity))
            }
            ProposalChange::CreateRelation {
                draft_id,
                from,
                to,
                evidence_ids,
                qualifier,
                ..
            } => {
                if from == to {
                    return Err(MemoryError::InvalidInput(
                        "a proposed relation requires distinct endpoints".into(),
                    ));
                }
                if let Some(value) = qualifier.take() {
                    *qualifier = Some(validate_text(value, "relation qualifier")?);
                }
                normalize_evidence_ids(evidence_ids, &source_ids)?;
                Some((*draft_id, ProposalDraftKind::Relation))
            }
            ProposalChange::Retract { target } => {
                if !mutation_targets.insert(target.record) {
                    return Err(MemoryError::InvalidInput(
                        "a proposal cannot mutate one target more than once".into(),
                    ));
                }
                None
            }
            ProposalChange::Supersede { target, .. } => {
                if !mutation_targets.insert(target.record) {
                    return Err(MemoryError::InvalidInput(
                        "a proposal cannot mutate one target more than once".into(),
                    ));
                }
                None
            }
        };
        if let Some((draft_id, kind)) = draft {
            if draft_kinds.insert(draft_id, kind).is_some() {
                return Err(MemoryError::InvalidInput(
                    "proposal draft IDs must be unique".into(),
                ));
            }
        }
    }

    for change in &input.changes {
        match change {
            ProposalChange::CreateRelation { from, to, kind, .. } => {
                let from_kind = proposal_endpoint_kind(*from, &draft_kinds)?;
                let to_kind = proposal_endpoint_kind(*to, &draft_kinds)?;
                validate_relation_endpoint_kinds(*kind, from_kind, to_kind)?;
            }
            ProposalChange::Retract { target } => {
                if matches!(
                    target.record,
                    RecordRef::Proposal(_) | RecordRef::ProposalReview(_)
                ) {
                    return Err(MemoryError::InvalidInput(
                        "proposal bundles cannot target proposal workflow records".into(),
                    ));
                }
            }
            ProposalChange::Supersede {
                target,
                replacement,
            } => {
                let compatible = matches!(
                    (target.record, draft_kinds.get(replacement)),
                    (RecordRef::Claim(_), Some(ProposalDraftKind::Claim))
                        | (RecordRef::Entity(_), Some(ProposalDraftKind::Entity))
                );
                if !compatible {
                    return Err(MemoryError::InvalidInput(
                        "supersession replacement must be a same-kind claim or entity draft".into(),
                    ));
                }
            }
            ProposalChange::CreateClaim { .. } | ProposalChange::CreateEntity { .. } => {}
        }
    }
    if mutation_targets
        .iter()
        .any(|record| matches!(record, RecordRef::Evidence(id) if source_ids.contains(id)))
    {
        return Err(MemoryError::InvalidInput(
            "a proposal cannot mutate one of its own evidence sources".into(),
        ));
    }
    let endpoint_records: BTreeSet<_> = input
        .changes
        .iter()
        .filter_map(|change| match change {
            ProposalChange::CreateRelation { from, to, .. } => Some([from, to]),
            _ => None,
        })
        .flatten()
        .filter_map(|endpoint| match endpoint {
            ProposalEndpoint::Existing(pin) => Some(pin.record),
            ProposalEndpoint::Draft(_) => None,
        })
        .collect();
    if mutation_targets
        .iter()
        .any(|record| endpoint_records.contains(record))
    {
        return Err(MemoryError::InvalidInput(
            "a proposal cannot mutate an endpoint it also activates".into(),
        ));
    }
    let mut proposal_dependencies: BTreeSet<_> = input
        .source_evidence
        .iter()
        .map(|pin| RecordRef::Evidence(pin.id))
        .collect();
    proposal_dependencies.extend(
        proposal_existing_pins(&input.changes)
            .into_iter()
            .map(|pin| pin.record),
    );
    let mut dependency_edges = proposal_dependencies.len();
    for change in &input.changes {
        let additional = match change {
            ProposalChange::CreateClaim { evidence_ids, .. }
            | ProposalChange::CreateEntity { evidence_ids, .. } => evidence_ids.len(),
            ProposalChange::CreateRelation { evidence_ids, .. } => evidence_ids.len() + 2,
            ProposalChange::Supersede { .. } => input.source_evidence.len() + 2,
            ProposalChange::Retract { .. } => 0,
        };
        dependency_edges = dependency_edges.checked_add(additional).ok_or_else(|| {
            MemoryError::InvalidInput("proposal dependency edges overflow".into())
        })?;
        if dependency_edges > MAX_PROPOSAL_DEPENDENCY_EDGES {
            return Err(MemoryError::InvalidInput(format!(
                "proposal cannot create more than {MAX_PROPOSAL_DEPENDENCY_EDGES} dependency edges"
            )));
        }
    }
    if encode(&input)?.len() > MAX_PROPOSAL_ENCODED_BYTES {
        return Err(MemoryError::InvalidInput(format!(
            "proposal bundle exceeds {MAX_PROPOSAL_ENCODED_BYTES} encoded bytes"
        )));
    }
    Ok(input)
}

fn proposal_existing_pins(changes: &[ProposalChange]) -> Vec<RecordRevisionPin> {
    let mut pins = BTreeSet::new();
    for change in changes {
        match change {
            ProposalChange::CreateRelation { from, to, .. } => {
                for endpoint in [from, to] {
                    if let ProposalEndpoint::Existing(pin) = endpoint {
                        pins.insert(*pin);
                    }
                }
            }
            ProposalChange::Retract { target } | ProposalChange::Supersede { target, .. } => {
                pins.insert(*target);
            }
            ProposalChange::CreateClaim { .. } | ProposalChange::CreateEntity { .. } => {}
        }
    }
    pins.into_iter().collect()
}

fn normalize_review(input: &mut NewProposalReview) -> MemoryResult<()> {
    if input.findings.len() > MAX_REVIEW_FINDINGS {
        return Err(MemoryError::InvalidInput(format!(
            "a proposal review cannot contain more than {MAX_REVIEW_FINDINGS} findings"
        )));
    }
    for finding in &mut input.findings {
        finding.pins.sort();
        finding.pins.dedup();
        if finding.pins.len() > MAX_REVIEW_PINS_PER_FINDING {
            return Err(MemoryError::InvalidInput(format!(
                "a review finding cannot pin more than {MAX_REVIEW_PINS_PER_FINDING} records"
            )));
        }
    }
    Ok(())
}

#[derive(Debug, Clone, Copy, Serialize)]
struct RetractMutation {
    record: RecordRef,
    expected_revision: u64,
}

#[derive(Serialize)]
struct PurgeTokenInput<'a> {
    era_id: EraId,
    target: RecordRef,
    expected_revision: u64,
    payloads_to_make_unavailable: u32,
    issued_at_ms: i64,
    expires_at_ms: i64,
    invalidations: &'a [PurgeDependency],
    relation_signal_invalidations: &'a [RelationSignalPurgeDependency],
    activation_trace_invalidations: &'a [ActivationTraceId],
}

fn validate_text(text: String, field: &str) -> MemoryResult<String> {
    let text = text.trim().to_owned();
    if text.is_empty() {
        return Err(MemoryError::InvalidInput(format!(
            "{field} cannot be empty"
        )));
    }
    if text.len() > MAX_TEXT_BYTES {
        return Err(MemoryError::InvalidInput(format!(
            "{field} exceeds {MAX_TEXT_BYTES} bytes"
        )));
    }
    Ok(text)
}

fn validate_exact_evidence_text(text: String, field: &str) -> MemoryResult<String> {
    if text.trim().is_empty() {
        return Err(MemoryError::InvalidInput(format!(
            "{field} cannot be empty"
        )));
    }
    if text.len() > MAX_EVIDENCE_TEXT_BYTES {
        return Err(MemoryError::InvalidInput(format!(
            "{field} exceeds {MAX_EVIDENCE_TEXT_BYTES} bytes"
        )));
    }
    Ok(text)
}

fn require_artifact_actor(actor: Actor) -> MemoryResult<()> {
    if matches!(actor, Actor::User | Actor::System | Actor::Operator) {
        Ok(())
    } else {
        Err(MemoryError::Unauthorized)
    }
}

fn require_active_revision(
    state: RecordState,
    actual_revision: u64,
    expected_revision: u64,
) -> MemoryResult<()> {
    if actual_revision != expected_revision {
        return Err(MemoryError::RevisionConflict {
            expected: expected_revision,
            actual: actual_revision,
        });
    }
    if state != RecordState::Active {
        return Err(MemoryError::InvalidInput(
            "artifact parent is not active".into(),
        ));
    }
    Ok(())
}

fn validate_exact_bounded_text(
    text: String,
    field: &str,
    max_bytes: usize,
) -> MemoryResult<String> {
    if text.trim().is_empty() {
        return Err(MemoryError::InvalidInput(format!(
            "{field} cannot be empty"
        )));
    }
    if text.len() > max_bytes {
        return Err(MemoryError::InvalidInput(format!(
            "{field} exceeds {max_bytes} bytes"
        )));
    }
    Ok(text)
}

fn validate_artifact_range(range: ArtifactRange, field: &str) -> MemoryResult<()> {
    if range.start >= range.end_exclusive {
        return Err(MemoryError::InvalidInput(format!(
            "{field} must be a non-empty [start, end) range"
        )));
    }
    Ok(())
}

fn validate_artifact_passage_batch(input: &mut NewArtifactPassageBatch) -> MemoryResult<()> {
    if input.passages.is_empty() || input.passages.len() > MAX_ARTIFACT_PASSAGE_BATCH {
        return Err(MemoryError::InvalidInput(format!(
            "artifact passage batch must contain 1..={MAX_ARTIFACT_PASSAGE_BATCH} passages"
        )));
    }
    let mut aggregate = 0_usize;
    let mut previous_ordinal = None;
    for passage in &mut input.passages {
        passage.text = validate_exact_bounded_text(
            std::mem::take(&mut passage.text),
            "artifact passage text",
            MAX_TEXT_BYTES,
        )?;
        aggregate = aggregate.checked_add(passage.text.len()).ok_or_else(|| {
            MemoryError::InvalidInput("artifact passage byte count overflow".into())
        })?;
        if aggregate > MAX_ARTIFACT_PASSAGE_BATCH_BYTES {
            return Err(MemoryError::InvalidInput(format!(
                "artifact passage batch exceeds {MAX_ARTIFACT_PASSAGE_BATCH_BYTES} text bytes"
            )));
        }
        if previous_ordinal.is_some_and(|ordinal| ordinal >= passage.locator.ordinal) {
            return Err(MemoryError::InvalidInput(
                "artifact passage locators must have strictly increasing ordinals".into(),
            ));
        }
        previous_ordinal = Some(passage.locator.ordinal);
        for (range, field) in [
            (passage.locator.byte_range, "artifact byte range"),
            (passage.locator.page_range, "artifact page range"),
            (passage.locator.time_range_ms, "artifact time range"),
        ] {
            if let Some(range) = range {
                validate_artifact_range(range, field)?;
            }
        }
        if tokenize(&passage.text).len() > MAX_INDEX_TERMS_PER_RECORD {
            return Err(MemoryError::InvalidInput(format!(
                "indexed text exceeds {MAX_INDEX_TERMS_PER_RECORD} terms"
            )));
        }
    }
    Ok(())
}

fn insert_bidirectional_dependency(
    batch: &mut fjall::Batch,
    dependencies: &PartitionHandle,
    first: RecordRef,
    second: RecordRef,
) {
    batch.insert(dependencies, dependency_key(first, second), []);
    batch.insert(dependencies, dependency_key(second, first), []);
}

fn actor_tag(actor: Actor) -> u8 {
    match actor {
        Actor::User => 0x01,
        Actor::Assistant => 0x02,
        Actor::System => 0x03,
        Actor::Operator => 0x04,
    }
}

fn update_framed_digest(hasher: &mut blake3::Hasher, value: &[u8]) {
    hasher.update(&(value.len() as u64).to_be_bytes());
    hasher.update(value);
}

fn validate_evidence_capture(
    actor: Actor,
    class: EvidenceClass,
    lifecycle: &EvidenceLifecycle,
) -> MemoryResult<()> {
    if matches!(
        class,
        EvidenceClass::ArtifactSnapshot | EvidenceClass::ArtifactPassage
    ) {
        return Err(MemoryError::InvalidInput(
            "artifact evidence must be created through the first-class artifact API".into(),
        ));
    }
    let authorized = match class {
        EvidenceClass::UserAssertion | EvidenceClass::UserCorrection => actor == Actor::User,
        EvidenceClass::ImportedSource => {
            matches!(actor, Actor::User | Actor::System | Actor::Operator)
        }
        EvidenceClass::ToolObservation
        | EvidenceClass::ActionOutcome
        | EvidenceClass::SystemObservation => matches!(actor, Actor::System | Actor::Operator),
        EvidenceClass::ArtifactSnapshot | EvidenceClass::ArtifactPassage => unreachable!(),
        EvidenceClass::AssistantCommitment => matches!(actor, Actor::System | Actor::Operator),
        EvidenceClass::AssistantUtterance => {
            matches!(actor, Actor::Assistant | Actor::System | Actor::Operator)
        }
    };
    if !authorized {
        return Err(MemoryError::Unauthorized);
    }
    if let EvidenceLifecycle::TerminalTurn {
        source_event_id, ..
    } = lifecycle
    {
        if source_event_id.is_empty() || source_event_id.len() > MAX_SOURCE_EVENT_ID_BYTES {
            return Err(MemoryError::InvalidInput(format!(
                "terminal source event ID must contain 1..={MAX_SOURCE_EVENT_ID_BYTES} bytes"
            )));
        }
    }
    let lifecycle_is_valid = match class {
        EvidenceClass::AssistantCommitment => matches!(
            lifecycle,
            EvidenceLifecycle::TerminalTurn {
                status: TerminalTurnStatus::Completed,
                ..
            }
        ),
        EvidenceClass::ToolObservation
        | EvidenceClass::ActionOutcome
        | EvidenceClass::AssistantUtterance => matches!(
            lifecycle,
            EvidenceLifecycle::Direct | EvidenceLifecycle::TerminalTurn { .. }
        ),
        _ => matches!(lifecycle, EvidenceLifecycle::Direct),
    };
    if !lifecycle_is_valid {
        return Err(MemoryError::InvalidInput(
            "evidence class is incompatible with its capture lifecycle".into(),
        ));
    }
    Ok(())
}

fn source_is_admissible(class: EvidenceClass, domain: ClaimDomain) -> bool {
    match class {
        EvidenceClass::UserAssertion => matches!(
            domain,
            ClaimDomain::UserProfile
                | ClaimDomain::UserPreference
                | ClaimDomain::UserNote
                | ClaimDomain::SessionContext
        ),
        EvidenceClass::UserCorrection => matches!(
            domain,
            ClaimDomain::UserProfile
                | ClaimDomain::UserPreference
                | ClaimDomain::UserNote
                | ClaimDomain::ExternalFact
                | ClaimDomain::WorkspaceFact
                | ClaimDomain::SessionContext
        ),
        EvidenceClass::ImportedSource => matches!(
            domain,
            ClaimDomain::ExternalFact | ClaimDomain::WorkspaceFact | ClaimDomain::ArtifactContent
        ),
        EvidenceClass::ToolObservation => matches!(
            domain,
            ClaimDomain::ExternalFact
                | ClaimDomain::WorkspaceFact
                | ClaimDomain::SessionContext
                | ClaimDomain::SystemFact
        ),
        EvidenceClass::ActionOutcome => matches!(
            domain,
            ClaimDomain::ExternalFact
                | ClaimDomain::WorkspaceFact
                | ClaimDomain::SessionContext
                | ClaimDomain::SystemFact
        ),
        EvidenceClass::AssistantCommitment => domain == ClaimDomain::AssistantCommitment,
        EvidenceClass::AssistantUtterance => false,
        EvidenceClass::SystemObservation => {
            matches!(
                domain,
                ClaimDomain::SessionContext | ClaimDomain::SystemFact
            )
        }
        EvidenceClass::ArtifactSnapshot | EvidenceClass::ArtifactPassage => matches!(
            domain,
            ClaimDomain::ArtifactContent | ClaimDomain::ExternalFact | ClaimDomain::WorkspaceFact
        ),
    }
}

fn validate_claim_activation(
    actor: Actor,
    domain: ClaimDomain,
    source_classes: &[EvidenceClass],
) -> MemoryResult<()> {
    let authorized = match actor {
        Actor::Assistant => false,
        Actor::System | Actor::Operator => true,
        Actor::User => {
            matches!(
                domain,
                ClaimDomain::UserProfile
                    | ClaimDomain::UserPreference
                    | ClaimDomain::UserNote
                    | ClaimDomain::SessionContext
            ) && source_classes.iter().all(|class| {
                matches!(
                    class,
                    EvidenceClass::UserAssertion | EvidenceClass::UserCorrection
                )
            })
        }
    };
    if authorized {
        Ok(())
    } else {
        Err(MemoryError::Unauthorized)
    }
}

fn validate_relation_activation(
    actor: Actor,
    source_classes: &[EvidenceClass],
) -> MemoryResult<()> {
    let authorized = match actor {
        Actor::Assistant => false,
        Actor::System | Actor::Operator => true,
        Actor::User => source_classes.iter().all(|class| {
            matches!(
                class,
                EvidenceClass::UserAssertion | EvidenceClass::UserCorrection
            )
        }),
    };
    if authorized {
        Ok(())
    } else {
        Err(MemoryError::Unauthorized)
    }
}

fn encode<T: Serialize>(value: &T) -> MemoryResult<Vec<u8>> {
    Ok(serde_json::to_vec(value)?)
}

fn decode<T: DeserializeOwned>(value: &[u8]) -> MemoryResult<T> {
    Ok(serde_json::from_slice(value)?)
}

fn get_decoded<T: DeserializeOwned>(
    partition: &PartitionHandle,
    key: impl AsRef<[u8]>,
) -> MemoryResult<Option<T>> {
    partition
        .get(key.as_ref())
        .map_err(storage_error)?
        .map(|value| decode(&value))
        .transpose()
}

fn get_payload(partition: &PartitionHandle, record: RecordRef) -> MemoryResult<Option<String>> {
    partition
        .get(payload_key(record))
        .map_err(storage_error)?
        .map(|value| {
            String::from_utf8(value.to_vec())
                .map_err(|error| MemoryError::Corrupt(error.to_string()))
        })
        .transpose()
}

fn get_head(partition: &PartitionHandle, id: Ulid) -> MemoryResult<Option<u64>> {
    partition
        .get(id_key(id))
        .map_err(storage_error)?
        .map(|value| decode_u64(&value))
        .transpose()
}

fn decode_u64(bytes: &[u8]) -> MemoryResult<u64> {
    let bytes: [u8; 8] = bytes
        .try_into()
        .map_err(|_| MemoryError::Corrupt("expected an eight-byte integer".into()))?;
    Ok(u64::from_be_bytes(bytes))
}

fn storage_error(error: fjall::Error) -> MemoryError {
    MemoryError::Storage(error.to_string())
}

fn commit(batch: fjall::Batch) -> MemoryResult<()> {
    batch.commit().map_err(storage_error)
}

fn id_key(id: Ulid) -> [u8; 16] {
    id.0.to_be_bytes()
}

fn ulid_from_id_key(key: &[u8]) -> MemoryResult<Ulid> {
    let bytes: [u8; 16] = key
        .try_into()
        .map_err(|_| MemoryError::Corrupt("invalid native ID key".into()))?;
    Ok(Ulid(u128::from_be_bytes(bytes)))
}

fn revision_key(id: Ulid, revision: u64) -> Vec<u8> {
    let mut key = Vec::with_capacity(24);
    key.extend_from_slice(&id_key(id));
    key.extend_from_slice(&revision.to_be_bytes());
    key
}

fn artifact_passage_ordinal_key(snapshot_id: ArtifactSnapshotId, ordinal: u32) -> Vec<u8> {
    let mut key = Vec::with_capacity(20);
    key.extend_from_slice(&id_key(snapshot_id.0));
    key.extend_from_slice(&ordinal.to_be_bytes());
    key
}

fn record_key(record: RecordRef) -> Vec<u8> {
    let (tag, id) = match record {
        RecordRef::Evidence(id) => (0x01, id.0),
        RecordRef::Claim(id) => (0x02, id.0),
        RecordRef::SemanticRelation(id) => (0x03, id.0),
        RecordRef::Entity(id) => (0x04, id.0),
        RecordRef::Proposal(id) => (0x05, id.0),
        RecordRef::ProposalReview(id) => (0x06, id.0),
        RecordRef::ArtifactCollection(id) => (0x07, id.0),
        RecordRef::ArtifactSnapshot(id) => (0x08, id.0),
        RecordRef::ArtifactPassage(id) => (0x09, id.0),
    };
    let mut key = Vec::with_capacity(17);
    key.push(tag);
    key.extend_from_slice(&id_key(id));
    key
}

fn record_from_key(key: &[u8]) -> MemoryResult<RecordRef> {
    if key.len() != 17 {
        return Err(MemoryError::Corrupt("invalid record key".into()));
    }
    let mut id = [0_u8; 16];
    id.copy_from_slice(&key[1..]);
    let id = Ulid(u128::from_be_bytes(id));
    match key[0] {
        0x01 => Ok(RecordRef::Evidence(EvidenceId(id))),
        0x02 => Ok(RecordRef::Claim(ClaimId(id))),
        0x03 => Ok(RecordRef::SemanticRelation(RelationId(id))),
        0x04 => Ok(RecordRef::Entity(EntityId(id))),
        0x05 => Ok(RecordRef::Proposal(ProposalId(id))),
        0x06 => Ok(RecordRef::ProposalReview(ProposalReviewCaseId(id))),
        0x07 => Ok(RecordRef::ArtifactCollection(ArtifactCollectionId(id))),
        0x08 => Ok(RecordRef::ArtifactSnapshot(ArtifactSnapshotId(id))),
        0x09 => Ok(RecordRef::ArtifactPassage(ArtifactPassageId(id))),
        _ => Err(MemoryError::Corrupt("unknown record key tag".into())),
    }
}

fn payload_key(record: RecordRef) -> Vec<u8> {
    record_key(record)
}

fn relation_signal_pair_key(
    profile_id: RelationProfileId,
    from: RecordRef,
    to: RecordRef,
) -> Vec<u8> {
    let mut key = Vec::with_capacity(50);
    key.extend_from_slice(&id_key(profile_id.0));
    key.extend_from_slice(&record_key(from));
    key.extend_from_slice(&record_key(to));
    key
}

fn relation_signal_record_index_key(record: RecordRef, signal_id: RelationSignalId) -> Vec<u8> {
    let mut key = record_key(record);
    key.extend_from_slice(&id_key(signal_id.0));
    key
}

fn relation_signal_owner_index_key(owner_id: Ulid, signal_id: RelationSignalId) -> Vec<u8> {
    let mut key = Vec::with_capacity(32);
    key.extend_from_slice(&id_key(owner_id));
    key.extend_from_slice(&id_key(signal_id.0));
    key
}

fn activation_trace_record_index_key(record: RecordRef, trace_id: ActivationTraceId) -> Vec<u8> {
    let mut key = record_key(record);
    key.extend_from_slice(&id_key(trace_id.0));
    key
}

fn activation_trace_owner_index_key(owner_id: Ulid, trace_id: ActivationTraceId) -> Vec<u8> {
    let mut key = Vec::with_capacity(32);
    key.extend_from_slice(&id_key(owner_id));
    key.extend_from_slice(&id_key(trace_id.0));
    key
}

fn dependency_key(source: RecordRef, dependent: RecordRef) -> Vec<u8> {
    let mut key = record_key(source);
    key.extend_from_slice(&record_key(dependent));
    key
}

fn posting_key(domain: u8, hash: &[u8; 32], record: RecordRef) -> Vec<u8> {
    let mut key = Vec::with_capacity(50);
    key.push(domain);
    key.extend_from_slice(hash);
    key.extend_from_slice(&record_key(record));
    key
}

fn time_key(observed_at_ms: i64, record: RecordRef) -> Vec<u8> {
    let ordered = ordered_time(observed_at_ms);
    let mut key = Vec::with_capacity(25);
    key.extend_from_slice(&ordered.to_be_bytes());
    key.extend_from_slice(&record_key(record));
    key
}

fn ordered_time(timestamp_ms: i64) -> u64 {
    (timestamp_ms as u64) ^ (1_u64 << 63)
}

fn audit_key(event: &AuditEvent) -> Vec<u8> {
    let ordered = (event.recorded_at_ms as u64) ^ (1_u64 << 63);
    let mut key = Vec::with_capacity(24);
    key.extend_from_slice(&ordered.to_be_bytes());
    key.extend_from_slice(&id_key(event.id.0));
    key
}

fn normalize_exact(input: &str) -> String {
    let mut output = String::new();
    let mut pending_space = false;
    for character in input.chars().flat_map(char::to_lowercase) {
        if character.is_whitespace() {
            pending_space = !output.is_empty();
        } else {
            if pending_space {
                output.push(' ');
                pending_space = false;
            }
            output.push(character);
        }
    }
    output
}

fn tokenize(input: &str) -> Vec<String> {
    let mut tokens = Vec::new();
    let mut word = String::new();
    let mut cjk = Vec::new();
    let flush_word = |word: &mut String, tokens: &mut Vec<String>| {
        if !word.is_empty() {
            let token: String = std::mem::take(word).chars().take(128).collect();
            tokens.push(token);
        }
    };
    let flush_cjk = |cjk: &mut Vec<char>, tokens: &mut Vec<String>| {
        for character in cjk.iter() {
            tokens.push(character.to_string());
        }
        for pair in cjk.windows(2) {
            tokens.push(pair.iter().collect());
        }
        cjk.clear();
    };
    for character in input.chars().flat_map(char::to_lowercase) {
        if is_cjk(character) {
            flush_word(&mut word, &mut tokens);
            cjk.push(character);
        } else if character.is_alphanumeric() || character == '_' {
            flush_cjk(&mut cjk, &mut tokens);
            word.push(character);
        } else {
            flush_word(&mut word, &mut tokens);
            flush_cjk(&mut cjk, &mut tokens);
        }
    }
    flush_word(&mut word, &mut tokens);
    flush_cjk(&mut cjk, &mut tokens);
    tokens
}

fn is_cjk(character: char) -> bool {
    matches!(character as u32, 0x3400..=0x4dbf | 0x4e00..=0x9fff | 0xf900..=0xfaff)
}

fn load_or_create_native_metadata(
    keyspace: &Keyspace,
    partition: &PartitionHandle,
    outer_store_era_id: &str,
    allow_initialization: bool,
) -> MemoryResult<NativeMetadata> {
    if let Some(metadata) = get_decoded(partition, NATIVE_METADATA_KEY)? {
        return Ok(metadata);
    }
    if !allow_initialization {
        return Err(MemoryError::Corrupt(
            "native metadata is missing from a non-pristine store".into(),
        ));
    }
    let first = Ulid::new().0.to_be_bytes();
    let second = Ulid::new().0.to_be_bytes();
    let mut digest_key = [0_u8; 32];
    digest_key[..16].copy_from_slice(&first);
    digest_key[16..].copy_from_slice(&second);
    let metadata = NativeMetadata {
        store_era_id: outer_store_era_id.to_owned(),
        digest_key,
    };
    let mut batch = keyspace.batch().durability(Some(PersistMode::SyncAll));
    batch.insert(partition, NATIVE_METADATA_KEY, encode(&metadata)?);
    commit(batch)?;
    Ok(metadata)
}

fn root_contains_only_outer_manifest(root: &Path) -> MemoryResult<bool> {
    let mut entries = fs::read_dir(root).map_err(MemoryError::Io)?;
    let Some(entry) = entries.next() else {
        return Ok(false);
    };
    let entry = entry.map_err(MemoryError::Io)?;
    Ok(entry.file_name() == crate::store_format::STORE_MANIFEST_FILE && entries.next().is_none())
}

fn open_partition(keyspace: &Keyspace, name: &str) -> MemoryResult<PartitionHandle> {
    keyspace
        .open_partition(name, PartitionCreateOptions::default())
        .map_err(|error| MemoryError::Storage(error.to_string()))
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(i64::MAX as u128) as i64
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store_format::{ResetPlan, ResetSafety, STORE_MANIFEST_FILE};
    use std::time::Duration;

    fn create_test_database() -> (tempfile::TempDir, PathBuf, MemoryDatabase) {
        let parent = tempfile::tempdir().unwrap();
        let root = parent.path().join("memory");
        let database = MemoryDatabase::create(&root).unwrap();
        (parent, root, database)
    }

    #[test]
    fn fresh_store_gets_a_stable_era_and_foreign_roots_are_refused() {
        assert_eq!(MEMORY_STORE_FORMAT_ID, "mmdb-native-memory-v1");
        let fresh = tempfile::tempdir().unwrap();
        let fresh_root = fresh.path().join("memory");
        let first_era = {
            let database = MemoryDatabase::create(&fresh_root).unwrap();
            database.era_id()
        };
        let reopened = MemoryDatabase::open(&fresh_root).unwrap();
        assert_eq!(reopened.era_id(), first_era);
        assert!(fresh_root.join(STORE_MANIFEST_FILE).is_file());
        assert!(!fresh_root.join("mmdb-native-manifest.json").exists());

        let existing_empty = tempfile::tempdir().unwrap();
        assert!(matches!(
            MemoryDatabase::open(existing_empty.path()),
            Err(MemoryError::StoreFormat(
                StoreFormatError::MissingManagedMarker(_)
            ))
        ));
        assert!(matches!(
            MemoryDatabase::create(existing_empty.path()),
            Err(MemoryError::StoreAlreadyExists(path)) if path == existing_empty.path()
        ));

        let foreign = tempfile::tempdir().unwrap();
        fs::write(foreign.path().join("legacy.data"), b"legacy").unwrap();
        assert!(matches!(
            MemoryDatabase::open(foreign.path()),
            Err(MemoryError::StoreFormat(
                StoreFormatError::MissingManagedMarker(_)
            ))
        ));
    }

    #[test]
    fn memory_database_holds_an_exclusive_root_lease_for_its_lifetime() {
        let parent = tempfile::tempdir().unwrap();
        let root = parent.path().join("memory");
        let database = MemoryDatabase::create(&root).unwrap();

        let error = match MemoryDatabase::open(&root) {
            Ok(_) => panic!("a second database handle must not open the same root"),
            Err(error) => error,
        };
        assert!(matches!(
            error,
            MemoryError::StoreFormat(StoreFormatError::StoreBusy(path))
                if path == std::fs::canonicalize(&root).unwrap()
        ));

        drop(database);
        MemoryDatabase::open(&root).expect("dropping the owner releases the root lease");
    }

    #[test]
    fn reset_plan_recognizes_the_memory_database_outer_manifest() {
        let (_parent, root, database) = create_test_database();
        let era = database.era_id();
        drop(database);
        let safety = ResetSafety::new(None, Vec::new()).unwrap();
        let canonical_root = fs::canonicalize(&root).unwrap();
        let plan = ResetPlan::build(
            std::slice::from_ref(&canonical_root),
            MEMORY_STORE_FORMAT_ID,
            UNIX_EPOCH + Duration::from_secs(1_000),
            Duration::from_secs(600),
            &safety,
        )
        .unwrap();

        assert_eq!(plan.expected_format_id(), MEMORY_STORE_FORMAT_ID);
        assert_eq!(plan.targets().len(), 1);
        assert_eq!(plan.targets()[0].store_era_id().as_str(), era.to_string());
    }

    #[test]
    fn pristine_marker_only_store_initializes_native_metadata_once() {
        let parent = tempfile::tempdir().unwrap();
        let root = parent.path().join("reset-replacement");
        std::fs::create_dir(&root).unwrap();
        let era = StoreEraId::parse(Ulid::new().to_string()).unwrap();
        OuterStoreManifest::new(MEMORY_STORE_FORMAT_ID, era.clone())
            .unwrap()
            .write_new(&root)
            .unwrap();

        let database = MemoryDatabase::open(&root).unwrap();
        assert_eq!(database.era_id().to_string(), era.as_str());
        drop(database);

        MemoryDatabase::open(&root).expect("persisted metadata reopens without rotation");
    }

    #[test]
    fn exact_format_mismatch_is_refused_before_fjall_opens_the_root() {
        let parent = tempfile::tempdir().unwrap();
        let root = parent.path().join("wrong-format");
        fs::create_dir(&root).unwrap();
        let era = StoreEraId::parse(Ulid::new().to_string()).unwrap();
        OuterStoreManifest::new("some-other-format", era)
            .unwrap()
            .write_new(&root)
            .unwrap();

        assert!(matches!(
            MemoryDatabase::open(&root),
            Err(MemoryError::StoreFormat(StoreFormatError::FormatMismatch {
                expected,
                found,
                ..
            })) if expected == MEMORY_STORE_FORMAT_ID && found == "some-other-format"
        ));
        let entries: Vec<_> = fs::read_dir(&root).unwrap().collect();
        assert_eq!(entries.len(), 1, "fjall must not touch a mismatched root");
        assert!(root.join(STORE_MANIFEST_FILE).is_file());
    }

    #[cfg(unix)]
    #[test]
    fn symlink_store_roots_are_refused() {
        let (parent, root, database) = create_test_database();
        drop(database);
        let linked = parent.path().join("linked-memory");
        std::os::unix::fs::symlink(&root, &linked).unwrap();

        assert!(matches!(
            MemoryDatabase::open(&linked),
            Err(MemoryError::StoreFormat(
                StoreFormatError::StoreRootIsNotDirectory(path)
            )) if path == linked
        ));
    }

    #[test]
    fn private_digest_metadata_must_match_the_outer_store_era() {
        let (_parent, root, database) = create_test_database();
        drop(database);
        let keyspace = Config::new(&root).open().unwrap();
        let partition = open_partition(&keyspace, PART_NATIVE_METADATA).unwrap();
        let wrong = NativeMetadata {
            store_era_id: Ulid::new().to_string(),
            digest_key: [7; 32],
        };
        let mut batch = keyspace.batch().durability(Some(PersistMode::SyncAll));
        batch.insert(&partition, NATIVE_METADATA_KEY, encode(&wrong).unwrap());
        batch.commit().unwrap();
        drop(partition);
        drop(keyspace);

        assert!(matches!(
            MemoryDatabase::open(&root),
            Err(MemoryError::StoreEraMismatch { internal, .. }) if internal == wrong.store_era_id
        ));
    }

    #[test]
    fn populated_store_with_missing_native_metadata_fails_closed() {
        let (_parent, root, database) = create_test_database();
        database
            .capture_evidence(
                OperationContext::new(Actor::User),
                NewEvidence {
                    class: EvidenceClass::UserAssertion,
                    scope: Scope::Personal,
                    temporal: TemporalFacts::observed_at(10),
                    lifecycle: EvidenceLifecycle::Direct,
                    text: "metadata must not rotate".into(),
                },
            )
            .unwrap();
        drop(database);

        let keyspace = Config::new(&root).open().unwrap();
        let metadata = open_partition(&keyspace, PART_NATIVE_METADATA).unwrap();
        metadata.remove(NATIVE_METADATA_KEY).unwrap();
        keyspace.persist(PersistMode::SyncAll).unwrap();
        drop(metadata);
        drop(keyspace);

        let error = match MemoryDatabase::open(&root) {
            Ok(_) => panic!("missing metadata in a populated store must be corruption"),
            Err(error) => error,
        };
        assert!(matches!(
            error,
            MemoryError::Corrupt(message) if message.contains("native metadata")
        ));
    }

    #[test]
    fn evidence_preserves_exact_bytes_while_rejecting_blank_input() {
        let (_parent, _root, database) = create_test_database();
        let exact = " \n\t{\"value\":\" exact tool payload \"}\r\n ".to_string();
        let captured = database
            .capture_evidence(
                OperationContext::new(Actor::System),
                NewEvidence {
                    class: EvidenceClass::ToolObservation,
                    scope: Scope::Personal,
                    temporal: TemporalFacts::observed_at(100),
                    lifecycle: EvidenceLifecycle::TerminalTurn {
                        source_event_id: "tool-call-exact-bytes".into(),
                        status: TerminalTurnStatus::Completed,
                    },
                    text: exact.clone(),
                },
            )
            .unwrap();

        assert_eq!(
            database
                .inspect_evidence(captured.id)
                .unwrap()
                .unwrap()
                .text
                .as_deref(),
            Some(exact.as_str())
        );
        assert!(matches!(
            database.capture_evidence(
                OperationContext::new(Actor::User),
                NewEvidence {
                    class: EvidenceClass::UserAssertion,
                    scope: Scope::Personal,
                    temporal: TemporalFacts::observed_at(100),
                    lifecycle: EvidenceLifecycle::Direct,
                    text: " \n\t\r ".into(),
                },
            ),
            Err(MemoryError::InvalidInput(message)) if message.contains("cannot be empty")
        ));
    }

    #[test]
    fn evidence_over_lexical_term_budget_roundtrips_exactly_and_recalls_an_indexed_prefix() {
        let (_parent, _root, database) = create_test_database();
        let mut exact = String::from("lexical_prefix_marker\n");
        for index in 0..(MAX_INDEX_TERMS_PER_RECORD + 64) {
            exact.push_str(&format!("evidence_term_{index:05} "));
        }
        exact.push_str("\nexact trailing bytes  \r\n");
        assert!(tokenize(&exact).len() > MAX_INDEX_TERMS_PER_RECORD);
        assert!(exact.len() < MAX_EVIDENCE_TEXT_BYTES);

        let captured = database
            .capture_evidence(
                OperationContext::new(Actor::System),
                NewEvidence {
                    class: EvidenceClass::ImportedSource,
                    scope: Scope::Personal,
                    temporal: TemporalFacts::observed_at(101),
                    lifecycle: EvidenceLifecycle::Direct,
                    text: exact.clone(),
                },
            )
            .unwrap();
        assert_eq!(
            database
                .inspect_evidence(captured.id)
                .unwrap()
                .unwrap()
                .text
                .as_deref(),
            Some(exact.as_str())
        );
        let recalled = database
            .recall(RecallQuery {
                text: "lexical_prefix_marker".into(),
                scopes: vec![Scope::Personal],
                observed_from_ms: None,
                observed_to_ms: None,
                valid_at_ms: None,
                limit: 10,
            })
            .unwrap();
        assert_eq!(recalled.citations.len(), 1);
        assert_eq!(
            recalled.citations[0].record,
            RecordRef::Evidence(captured.id)
        );
        assert_eq!(recalled.citations[0].text, exact);
    }

    #[test]
    fn evidence_authority_claim_cas_and_operation_replay_are_enforced() {
        let (_parent, _root, database) = create_test_database();
        let evidence_operation = OperationContext::new(Actor::User);
        let evidence_input = NewEvidence {
            class: EvidenceClass::UserAssertion,
            scope: Scope::Personal,
            temporal: TemporalFacts::observed_at(100),
            lifecycle: EvidenceLifecycle::Direct,
            text: "I prefer Rust".into(),
        };
        let first = database
            .capture_evidence(evidence_operation, evidence_input.clone())
            .unwrap();
        let replay = database
            .capture_evidence(evidence_operation, evidence_input)
            .unwrap();
        assert_eq!(replay, first);
        assert!(matches!(
            database.capture_evidence(
                evidence_operation,
                NewEvidence {
                    class: EvidenceClass::UserAssertion,
                    scope: Scope::Personal,
                    temporal: TemporalFacts::observed_at(100),
                    lifecycle: EvidenceLifecycle::Direct,
                    text: "I prefer Go".into(),
                },
            ),
            Err(MemoryError::OperationConflict(id)) if id == evidence_operation.id
        ));

        assert!(matches!(
            database.create_claim(
                OperationContext::new(Actor::System),
                NewClaim {
                    domain: ClaimDomain::ExternalFact,
                    scope: Scope::Personal,
                    temporal: TemporalFacts::observed_at(100),
                    proposition: "Rust is the user's preference".into(),
                    evidence_ids: vec![first.id],
                },
            ),
            Err(MemoryError::InadmissibleSource { evidence_id, .. }) if evidence_id == first.id
        ));

        let claim = database
            .create_claim(
                OperationContext::new(Actor::User),
                NewClaim {
                    domain: ClaimDomain::UserPreference,
                    scope: Scope::Personal,
                    temporal: TemporalFacts::observed_at(100),
                    proposition: "The user prefers Rust".into(),
                    evidence_ids: vec![first.id],
                },
            )
            .unwrap();
        assert_eq!(
            database
                .inspect_claim(claim.id)
                .unwrap()
                .unwrap()
                .proposition
                .as_deref(),
            Some("The user prefers Rust")
        );
        assert!(matches!(
            database.retract(
                OperationContext::new(Actor::User),
                RecordRef::Claim(claim.id),
                2,
            ),
            Err(MemoryError::RevisionConflict {
                expected: 2,
                actual: 1
            })
        ));
        let retracted = database
            .retract(
                OperationContext::new(Actor::User),
                RecordRef::Claim(claim.id),
                1,
            )
            .unwrap();
        assert_eq!(retracted.target.revision, 2);
        assert_eq!(retracted.target.state, RecordState::Retracted);

        let audit_json = serde_json::to_string(&database.audit_events(10).unwrap()).unwrap();
        assert!(!audit_json.contains("prefer Rust"));
        assert!(!audit_json.contains("proposition"));
    }

    #[test]
    fn semantic_relation_identity_is_stable_across_cas_retraction() {
        let (_parent, _root, database) = create_test_database();
        let evidence = database
            .capture_evidence(
                OperationContext::new(Actor::User),
                NewEvidence {
                    class: EvidenceClass::UserAssertion,
                    scope: Scope::Personal,
                    temporal: TemporalFacts::observed_at(100),
                    lifecycle: EvidenceLifecycle::Direct,
                    text: "I prefer Rust".into(),
                },
            )
            .unwrap();
        let claim = database
            .create_claim(
                OperationContext::new(Actor::User),
                NewClaim {
                    domain: ClaimDomain::UserPreference,
                    scope: Scope::Personal,
                    temporal: TemporalFacts::observed_at(100),
                    proposition: "The user prefers Rust".into(),
                    evidence_ids: vec![evidence.id],
                },
            )
            .unwrap();
        let relation = database
            .create_semantic_relation(
                OperationContext::new(Actor::System),
                NewSemanticRelation {
                    from: RecordRef::Evidence(evidence.id),
                    to: RecordRef::Claim(claim.id),
                    kind: RelationKind::Supports,
                    scope: Scope::Personal,
                    evidence_ids: vec![evidence.id],
                    qualifier: Some("direct support".into()),
                },
            )
            .unwrap();
        let before = database
            .inspect_semantic_relation(relation.id)
            .unwrap()
            .unwrap();
        let retracted = database
            .retract(
                OperationContext::new(Actor::System),
                RecordRef::SemanticRelation(relation.id),
                1,
            )
            .unwrap();
        assert_eq!(retracted.target.revision, 2);
        let after = database
            .inspect_semantic_relation(relation.id)
            .unwrap()
            .unwrap();
        assert_eq!(after.header.from, before.header.from);
        assert_eq!(after.header.to, before.header.to);
        assert_eq!(after.header.kind, before.header.kind);
        assert_eq!(after.qualifier.as_deref(), Some("direct support"));
        assert_eq!(after.header.state, RecordState::Retracted);
    }

    #[test]
    fn relation_kinds_enforce_endpoint_shapes_for_direct_and_proposed_writes() {
        let (_parent, _root, database) = create_test_database();
        let source = imported_source(&database, "relation endpoint source");
        let make_claim = |text: &str| {
            database
                .create_claim(
                    OperationContext::new(Actor::System),
                    NewClaim {
                        domain: ClaimDomain::ExternalFact,
                        scope: Scope::Personal,
                        temporal: TemporalFacts::observed_at(500),
                        proposition: text.into(),
                        evidence_ids: vec![source.id],
                    },
                )
                .unwrap()
        };
        let first = make_claim("first claim");
        let second = make_claim("second claim");

        assert!(matches!(
            database.create_semantic_relation(
                OperationContext::new(Actor::System),
                NewSemanticRelation {
                    from: RecordRef::Claim(first.id),
                    to: RecordRef::Evidence(source.id),
                    kind: RelationKind::Supersedes,
                    scope: Scope::Personal,
                    evidence_ids: vec![source.id],
                    qualifier: None,
                },
            ),
            Err(MemoryError::InvalidInput(message)) if message.contains("same-kind")
        ));
        assert!(database
            .create_semantic_relation(
                OperationContext::new(Actor::System),
                NewSemanticRelation {
                    from: RecordRef::Claim(second.id),
                    to: RecordRef::Claim(first.id),
                    kind: RelationKind::Supersedes,
                    scope: Scope::Personal,
                    evidence_ids: vec![source.id],
                    qualifier: None,
                },
            )
            .is_ok());

        let claim_draft = ProposalDraftId::new();
        let entity_draft = ProposalDraftId::new();
        assert!(matches!(
            database.submit_proposal(
                OperationContext::new(Actor::Assistant),
                NewProposalBundle {
                    source_job_id: ProposalSourceJobId::new(),
                    scope: Scope::Personal,
                    source_evidence: vec![EvidenceRevisionPin {
                        id: source.id,
                        revision: 1,
                    }],
                    changes: vec![
                        ProposalChange::CreateClaim {
                            draft_id: claim_draft,
                            domain: ClaimDomain::ExternalFact,
                            temporal: TemporalFacts::observed_at(500),
                            proposition: "draft claim".into(),
                            evidence_ids: vec![source.id],
                        },
                        ProposalChange::CreateEntity {
                            draft_id: entity_draft,
                            kind: EntityKind::Concept,
                            temporal: TemporalFacts::observed_at(500),
                            canonical_name: "draft entity".into(),
                            aliases: Vec::new(),
                            evidence_ids: vec![source.id],
                        },
                        ProposalChange::CreateRelation {
                            draft_id: ProposalDraftId::new(),
                            from: ProposalEndpoint::Draft(claim_draft),
                            to: ProposalEndpoint::Draft(entity_draft),
                            kind: RelationKind::Supersedes,
                            evidence_ids: vec![source.id],
                            qualifier: None,
                        },
                    ],
                },
            ),
            Err(MemoryError::InvalidInput(message)) if message.contains("same-kind")
        ));
    }

    #[test]
    fn unicode_lexical_and_time_recall_returns_revision_citations_and_ids_only_case() {
        let (_parent, _root, database) = create_test_database();
        let earlier = database
            .capture_evidence(
                OperationContext::new(Actor::User),
                NewEvidence {
                    class: EvidenceClass::UserAssertion,
                    scope: Scope::Personal,
                    temporal: TemporalFacts::observed_at(100),
                    lifecycle: EvidenceLifecycle::Direct,
                    text: "我喜欢本地记忆系统".into(),
                },
            )
            .unwrap();
        database
            .capture_evidence(
                OperationContext::new(Actor::User),
                NewEvidence {
                    class: EvidenceClass::UserAssertion,
                    scope: Scope::Personal,
                    temporal: TemporalFacts::observed_at(300),
                    lifecycle: EvidenceLifecycle::Direct,
                    text: "稍后的记忆系统记录".into(),
                },
            )
            .unwrap();
        database
            .capture_evidence(
                OperationContext::new(Actor::User),
                NewEvidence {
                    class: EvidenceClass::UserAssertion,
                    scope: Scope::Personal,
                    temporal: TemporalFacts::observed_at(150),
                    lifecycle: EvidenceLifecycle::Direct,
                    text: "Résumé notes".into(),
                },
            )
            .unwrap();

        let recalled = database
            .recall(RecallQuery {
                text: "记忆系统".into(),
                scopes: vec![Scope::Personal],
                observed_from_ms: Some(0),
                observed_to_ms: Some(200),
                valid_at_ms: None,
                limit: 10,
            })
            .unwrap();
        assert_eq!(recalled.citations.len(), 1);
        assert_eq!(
            recalled.citations[0].record,
            RecordRef::Evidence(earlier.id)
        );
        assert_eq!(recalled.citations[0].revision, 1);
        assert!(recalled.citations[0].matched_term_count > 0);

        let exact = database
            .recall(RecallQuery {
                text: "我喜欢本地记忆系统".into(),
                scopes: vec![Scope::Personal],
                observed_from_ms: None,
                observed_to_ms: None,
                valid_at_ms: None,
                limit: 1,
            })
            .unwrap();
        assert!(exact.citations[0].exact_match);

        let non_ascii_word = database
            .recall(RecallQuery {
                text: "RÉSUMÉ".into(),
                scopes: vec![Scope::Personal],
                observed_from_ms: None,
                observed_to_ms: None,
                valid_at_ms: None,
                limit: 1,
            })
            .unwrap();
        assert_eq!(non_ascii_word.citations[0].text, "Résumé notes");

        let time_only = database
            .recall(RecallQuery {
                text: String::new(),
                scopes: vec![Scope::Personal],
                observed_from_ms: Some(250),
                observed_to_ms: Some(350),
                valid_at_ms: None,
                limit: 10,
            })
            .unwrap();
        assert_eq!(time_only.citations.len(), 1);
        assert_eq!(time_only.citations[0].temporal.observed_at_ms, 300);

        let case = database
            .inspect_recall_case(recalled.case_id)
            .unwrap()
            .unwrap();
        assert_eq!(case.candidates[0].record, RecordRef::Evidence(earlier.id));
        let case_json = serde_json::to_string(&case).unwrap();
        assert!(!case_json.contains("记忆系统"));
        assert!(!case_json.contains("我喜欢"));
        assert!(!case_json.contains("text"));
    }

    #[test]
    fn recall_exposes_typed_evidence_truth_but_never_direct_assistant_utterances() {
        let (_parent, _root, database) = create_test_database();
        let scope = Scope::Session(Ulid::new());
        let raw_source_event_id = "run-018f-private-linkage";
        let outcome = database
            .capture_evidence(
                OperationContext::new(Actor::System),
                NewEvidence {
                    class: EvidenceClass::ActionOutcome,
                    scope: scope.clone(),
                    temporal: TemporalFacts::observed_at(100),
                    lifecycle: EvidenceLifecycle::TerminalTurn {
                        source_event_id: raw_source_event_id.into(),
                        status: TerminalTurnStatus::Failed,
                    },
                    text: "terminal_truth_marker failed safely".into(),
                },
            )
            .unwrap();
        database
            .capture_evidence(
                OperationContext::new(Actor::Assistant),
                NewEvidence {
                    class: EvidenceClass::AssistantUtterance,
                    scope: scope.clone(),
                    temporal: TemporalFacts::observed_at(101),
                    lifecycle: EvidenceLifecycle::Direct,
                    text: "terminal_truth_marker speculative assistant text".into(),
                },
            )
            .unwrap();

        let recalled = database
            .recall(RecallQuery {
                text: "terminal_truth_marker".into(),
                scopes: vec![scope],
                observed_from_ms: None,
                observed_to_ms: None,
                valid_at_ms: None,
                limit: 10,
            })
            .unwrap();
        assert_eq!(recalled.citations.len(), 1);
        let citation = &recalled.citations[0];
        assert_eq!(citation.record, RecordRef::Evidence(outcome.id));
        assert_eq!(citation.evidence.len(), 1);
        assert_eq!(citation.evidence[0].class, EvidenceClass::ActionOutcome);
        let EvidenceLifecycleTruth::TerminalTurn {
            source_event_digest,
            status,
        } = citation.evidence[0].lifecycle
        else {
            panic!("expected terminal lifecycle truth")
        };
        assert_eq!(status, TerminalTurnStatus::Failed);
        assert_ne!(source_event_digest, [0; 32]);
        assert_eq!(
            source_event_digest,
            database
                .source_event_fingerprint(raw_source_event_id)
                .unwrap()
        );
        let stored = database.inspect_evidence(outcome.id).unwrap().unwrap();
        let json = serde_json::to_string(&(stored, citation)).unwrap();
        assert!(!json.contains(raw_source_event_id));
        assert!(json.contains("ActionOutcome"));
        assert!(json.contains("Failed"));
    }

    #[test]
    fn purge_preview_makes_exact_payload_unavailable_and_invalidates_dependency_closure() {
        let (_parent, _root, database) = create_test_database();
        let evidence = database
            .capture_evidence(
                OperationContext::new(Actor::User),
                NewEvidence {
                    class: EvidenceClass::UserAssertion,
                    scope: Scope::Personal,
                    temporal: TemporalFacts::observed_at(100),
                    lifecycle: EvidenceLifecycle::Direct,
                    text: "secret preference for Rust".into(),
                },
            )
            .unwrap();
        let claim = database
            .create_claim(
                OperationContext::new(Actor::User),
                NewClaim {
                    domain: ClaimDomain::UserPreference,
                    scope: Scope::Personal,
                    temporal: TemporalFacts::observed_at(100),
                    proposition: "The user prefers Rust".into(),
                    evidence_ids: vec![evidence.id],
                },
            )
            .unwrap();
        let relation = database
            .create_semantic_relation(
                OperationContext::new(Actor::System),
                NewSemanticRelation {
                    from: RecordRef::Evidence(evidence.id),
                    to: RecordRef::Claim(claim.id),
                    kind: RelationKind::Supports,
                    scope: Scope::Personal,
                    evidence_ids: vec![evidence.id],
                    qualifier: None,
                },
            )
            .unwrap();

        let preview = database
            .preview_purge(Actor::Operator, RecordRef::Evidence(evidence.id))
            .unwrap();
        assert_eq!(preview.payloads_to_make_unavailable, 3);
        assert_eq!(preview.invalidations.len(), 2);
        assert!(preview
            .invalidations
            .iter()
            .any(|entry| entry.record == RecordRef::Claim(claim.id)));
        assert!(preview
            .invalidations
            .iter()
            .any(|entry| { entry.record == RecordRef::SemanticRelation(relation.id) }));
        assert!(matches!(
            database.commit_purge(OperationContext::new(Actor::Assistant), preview.clone()),
            Err(MemoryError::Unauthorized)
        ));

        let operation = OperationContext::new(Actor::Operator);
        let receipt = database.commit_purge(operation, preview.clone()).unwrap();
        assert_eq!(receipt.target.state, RecordState::Purged);
        assert_eq!(receipt.invalidated.len(), 2);
        assert_eq!(receipt.payloads_made_unavailable, 3);
        assert_eq!(
            database
                .inspect_evidence(evidence.id)
                .unwrap()
                .unwrap()
                .text,
            None
        );
        assert_eq!(
            database
                .inspect_claim(claim.id)
                .unwrap()
                .unwrap()
                .header
                .state,
            RecordState::Unsupported
        );
        assert!(database
            .inspect_claim(claim.id)
            .unwrap()
            .unwrap()
            .proposition
            .is_none());
        assert_eq!(
            database
                .inspect_semantic_relation(relation.id)
                .unwrap()
                .unwrap()
                .header
                .state,
            RecordState::Unsupported
        );
        assert!(
            !database
                .inspect_semantic_relation(relation.id)
                .unwrap()
                .unwrap()
                .payload_available
        );
        let recalled = database
            .recall(RecallQuery {
                text: "Rust".into(),
                scopes: vec![Scope::Personal],
                observed_from_ms: None,
                observed_to_ms: None,
                valid_at_ms: None,
                limit: 10,
            })
            .unwrap();
        assert!(recalled.citations.is_empty());

        // A retry returns the original structural receipt even though the
        // target can no longer be previewed as live.
        assert_eq!(database.commit_purge(operation, preview).unwrap(), receipt);
        let audit_json = serde_json::to_string(&database.audit_events(20).unwrap()).unwrap();
        assert!(!audit_json.contains("secret preference"));
    }

    #[test]
    fn purge_after_retraction_removes_every_descendant_payload_without_reinvalidating_it() {
        let (_parent, _root, database) = create_test_database();
        let evidence = database
            .capture_evidence(
                OperationContext::new(Actor::User),
                NewEvidence {
                    class: EvidenceClass::UserAssertion,
                    scope: Scope::Personal,
                    temporal: TemporalFacts::observed_at(100),
                    lifecycle: EvidenceLifecycle::Direct,
                    text: "private source payload".into(),
                },
            )
            .unwrap();
        let claim = database
            .create_claim(
                OperationContext::new(Actor::User),
                NewClaim {
                    domain: ClaimDomain::UserNote,
                    scope: Scope::Personal,
                    temporal: TemporalFacts::observed_at(100),
                    proposition: "private derived payload".into(),
                    evidence_ids: vec![evidence.id],
                },
            )
            .unwrap();
        let relation = database
            .create_semantic_relation(
                OperationContext::new(Actor::System),
                NewSemanticRelation {
                    from: RecordRef::Evidence(evidence.id),
                    to: RecordRef::Claim(claim.id),
                    kind: RelationKind::Supports,
                    scope: Scope::Personal,
                    evidence_ids: vec![evidence.id],
                    qualifier: Some("private qualifier".into()),
                },
            )
            .unwrap();

        let retracted = database
            .retract(
                OperationContext::new(Actor::Operator),
                RecordRef::Evidence(evidence.id),
                1,
            )
            .unwrap();
        assert_eq!(retracted.invalidated.len(), 2);

        let preview = database
            .preview_purge(Actor::Operator, RecordRef::Evidence(evidence.id))
            .unwrap();
        assert_eq!(preview.expected_revision, 2);
        assert_eq!(preview.invalidations.len(), 2);
        assert_eq!(preview.payloads_to_make_unavailable, 3);

        let receipt = database
            .commit_purge(OperationContext::new(Actor::Operator), preview)
            .unwrap();
        assert!(receipt.invalidated.is_empty());
        assert_eq!(receipt.payloads_made_unavailable, 3);
        let claim = database.inspect_claim(claim.id).unwrap().unwrap();
        assert_eq!(claim.header.state, RecordState::Unsupported);
        assert_eq!(claim.header.revision, 2);
        assert!(claim.proposition.is_none());
        let relation = database
            .inspect_semantic_relation(relation.id)
            .unwrap()
            .unwrap();
        assert_eq!(relation.header.state, RecordState::Unsupported);
        assert_eq!(relation.header.revision, 2);
        assert!(!relation.payload_available);
    }

    #[test]
    fn privacy_retraction_and_purge_do_not_refuse_more_than_4096_reverse_edges() {
        const OLD_REFUSAL_THRESHOLD: usize = 4_096;
        let (_parent, _root, database) = create_test_database();
        let source = imported_source(&database, "large privacy fanout source");
        let recall_case = recall_case_for(&database, "large privacy fanout source");
        let mut sample_claims = Vec::new();
        for bundle in 0..33 {
            let changes = (0..MAX_PROPOSAL_CHANGES)
                .map(|change| ProposalChange::CreateClaim {
                    draft_id: ProposalDraftId::new(),
                    domain: ClaimDomain::ExternalFact,
                    temporal: TemporalFacts::observed_at(500),
                    proposition: format!("fanout claim {bundle} {change}"),
                    evidence_ids: vec![source.id],
                })
                .collect();
            let proposal = database
                .submit_proposal(
                    OperationContext::new(Actor::Assistant),
                    NewProposalBundle {
                        source_job_id: ProposalSourceJobId::new(),
                        scope: Scope::Personal,
                        source_evidence: vec![EvidenceRevisionPin {
                            id: source.id,
                            revision: 1,
                        }],
                        changes,
                    },
                )
                .unwrap();
            let review = approve_proposal(&database, &proposal, &recall_case);
            let applied = database
                .adjudicate_proposal(
                    OperationContext::new(Actor::Operator),
                    ProposalAdjudication {
                        proposal_id: proposal.id,
                        expected_proposal_revision: review.proposal.revision,
                        review_case_id: review.review_case_id,
                        expected_review_revision: review.review_revision,
                        decision: ProposalDecision::Accept,
                        authority: AdjudicationAuthority::ExplicitOperator,
                    },
                )
                .unwrap();
            sample_claims.push(applied.draft_mappings[0].record);
        }

        let retraction = database
            .retract(
                OperationContext::new(Actor::Operator),
                RecordRef::Evidence(source.id),
                1,
            )
            .unwrap();
        assert!(retraction.invalidated.len() > OLD_REFUSAL_THRESHOLD);
        let preview = database
            .preview_purge(Actor::Operator, RecordRef::Evidence(source.id))
            .unwrap();
        assert!(preview.invalidations.len() > OLD_REFUSAL_THRESHOLD);
        assert!(preview.payloads_to_make_unavailable > OLD_REFUSAL_THRESHOLD as u32);
        let receipt = database
            .commit_purge(OperationContext::new(Actor::Operator), preview)
            .unwrap();
        assert!(receipt.invalidated.is_empty());
        for sample in [sample_claims[0], sample_claims[32]] {
            let AppliedRecord::Claim(id) = sample else {
                panic!("expected claim mapping")
            };
            assert!(database
                .inspect_claim(id)
                .unwrap()
                .unwrap()
                .proposition
                .is_none());
        }
    }

    #[test]
    fn observed_range_candidate_limit_keeps_the_newest_records() {
        let (_parent, _root, database) = create_test_database();
        let mut ids = Vec::new();
        for observed_at_ms in [100, 200, 300] {
            let receipt = database
                .capture_evidence(
                    OperationContext::new(Actor::User),
                    NewEvidence {
                        class: EvidenceClass::UserAssertion,
                        scope: Scope::Personal,
                        temporal: TemporalFacts::observed_at(observed_at_ms),
                        lifecycle: EvidenceLifecycle::Direct,
                        text: format!("evidence at {observed_at_ms}"),
                    },
                )
                .unwrap();
            ids.push(receipt.id);
        }

        let records = database
            .records_in_observed_range(None, None, 2, &BTreeSet::new())
            .unwrap();

        assert_eq!(
            records,
            vec![RecordRef::Evidence(ids[2]), RecordRef::Evidence(ids[1])]
        );
    }

    #[test]
    fn recall_rejects_an_unbounded_scope_set() {
        let (_parent, _root, database) = create_test_database();
        let scopes = (0..=MAX_RECALL_SCOPES)
            .map(|_| Scope::Session(Ulid::new()))
            .collect();

        let error = database
            .recall(RecallQuery {
                text: "bounded scopes".into(),
                scopes,
                observed_from_ms: None,
                observed_to_ms: None,
                valid_at_ms: None,
                limit: 10,
            })
            .unwrap_err();

        assert!(
            matches!(error, MemoryError::InvalidInput(message) if message.contains("visible scopes"))
        );
    }

    #[test]
    fn oversized_payload_is_rejected_before_any_durable_write() {
        let (_parent, _root, database) = create_test_database();
        let oversized = "x".repeat(MAX_EVIDENCE_TEXT_BYTES + 1);

        let error = database
            .capture_evidence(
                OperationContext::new(Actor::User),
                NewEvidence {
                    class: EvidenceClass::UserAssertion,
                    scope: Scope::Personal,
                    temporal: TemporalFacts::observed_at(100),
                    lifecycle: EvidenceLifecycle::Direct,
                    text: oversized,
                },
            )
            .unwrap_err();

        assert!(
            matches!(error, MemoryError::InvalidInput(message) if message.contains(&MAX_EVIDENCE_TEXT_BYTES.to_string()))
        );
        assert!(database.audit_events(10).unwrap().is_empty());
        assert!(database
            .recall(RecallQuery {
                text: "x".into(),
                scopes: vec![Scope::Personal],
                observed_from_ms: None,
                observed_to_ms: None,
                valid_at_ms: None,
                limit: 10,
            })
            .unwrap()
            .citations
            .is_empty());
    }

    #[test]
    fn claim_source_fanout_is_bounded_before_source_reads() {
        let (_parent, _root, database) = create_test_database();
        let evidence_ids = (0..=MAX_EVIDENCE_SOURCES)
            .map(|_| EvidenceId::new())
            .collect();

        let error = database
            .create_claim(
                OperationContext::new(Actor::Operator),
                NewClaim {
                    domain: ClaimDomain::ExternalFact,
                    scope: Scope::Personal,
                    temporal: TemporalFacts::observed_at(100),
                    proposition: "bounded claim".into(),
                    evidence_ids,
                },
            )
            .unwrap_err();

        assert!(matches!(error, MemoryError::InvalidInput(message) if message.contains("64")));
        assert!(database.audit_events(10).unwrap().is_empty());
    }

    #[test]
    fn utterances_never_support_claims_and_commitments_require_a_completed_terminal_turn() {
        let (_parent, _root, database) = create_test_database();
        assert!(matches!(
            database.capture_evidence(
                OperationContext::new(Actor::Assistant),
                NewEvidence {
                    class: EvidenceClass::UserAssertion,
                    scope: Scope::Personal,
                    temporal: TemporalFacts::observed_at(100),
                    lifecycle: EvidenceLifecycle::Direct,
                    text: "not actually said by the user".into(),
                },
            ),
            Err(MemoryError::Unauthorized)
        ));

        let utterance = database
            .capture_evidence(
                OperationContext::new(Actor::Assistant),
                NewEvidence {
                    class: EvidenceClass::AssistantUtterance,
                    scope: Scope::Session(Ulid::new()),
                    temporal: TemporalFacts::observed_at(100),
                    lifecycle: EvidenceLifecycle::Direct,
                    text: "I might do that later".into(),
                },
            )
            .unwrap();
        let utterance_scope = database
            .inspect_evidence(utterance.id)
            .unwrap()
            .unwrap()
            .header
            .scope;
        assert!(matches!(
            database.create_claim(
                OperationContext::new(Actor::System),
                NewClaim {
                    domain: ClaimDomain::SessionContext,
                    scope: utterance_scope,
                    temporal: TemporalFacts::observed_at(100),
                    proposition: "The assistant committed to doing it".into(),
                    evidence_ids: vec![utterance.id],
                },
            ),
            Err(MemoryError::InadmissibleSource { evidence_id, .. }) if evidence_id == utterance.id
        ));

        assert!(matches!(
            database.capture_evidence(
                OperationContext::new(Actor::System),
                NewEvidence {
                    class: EvidenceClass::AssistantCommitment,
                    scope: Scope::Session(Ulid::new()),
                    temporal: TemporalFacts::observed_at(100),
                    lifecycle: EvidenceLifecycle::Direct,
                    text: "I will send the report".into(),
                },
            ),
            Err(MemoryError::InvalidInput(_))
        ));

        let commitment_scope = Scope::Session(Ulid::new());
        let commitment = database
            .capture_evidence(
                OperationContext::new(Actor::System),
                NewEvidence {
                    class: EvidenceClass::AssistantCommitment,
                    scope: commitment_scope.clone(),
                    temporal: TemporalFacts::observed_at(200),
                    lifecycle: EvidenceLifecycle::TerminalTurn {
                        source_event_id: "018f5f6e-4f1d-7d8a-9a01-completed".into(),
                        status: TerminalTurnStatus::Completed,
                    },
                    text: "I will send the report".into(),
                },
            )
            .unwrap();
        assert!(matches!(
            database.create_claim(
                OperationContext::new(Actor::System),
                NewClaim {
                    domain: ClaimDomain::UserPreference,
                    scope: commitment_scope.clone(),
                    temporal: TemporalFacts::observed_at(200),
                    proposition: "The user prefers receiving reports".into(),
                    evidence_ids: vec![commitment.id],
                },
            ),
            Err(MemoryError::InadmissibleSource { evidence_id, .. })
                if evidence_id == commitment.id
        ));
        let claim = database
            .create_claim(
                OperationContext::new(Actor::System),
                NewClaim {
                    domain: ClaimDomain::AssistantCommitment,
                    scope: commitment_scope,
                    temporal: TemporalFacts::observed_at(200),
                    proposition: "The assistant committed to send the report".into(),
                    evidence_ids: vec![commitment.id],
                },
            )
            .unwrap();
        assert_eq!(claim.state, RecordState::Active);

        for status in [
            TerminalTurnStatus::Failed,
            TerminalTurnStatus::Cancelled,
            TerminalTurnStatus::Interrupted,
        ] {
            assert!(matches!(
                database.capture_evidence(
                    OperationContext::new(Actor::System),
                    NewEvidence {
                        class: EvidenceClass::AssistantCommitment,
                        scope: Scope::Session(Ulid::new()),
                        temporal: TemporalFacts::observed_at(300),
                        lifecycle: EvidenceLifecycle::TerminalTurn {
                            source_event_id: format!("terminal-{status:?}"),
                            status,
                        },
                        text: "this commitment did not complete".into(),
                    },
                ),
                Err(MemoryError::InvalidInput(_))
            ));
        }
        for (class, status) in [
            (EvidenceClass::ToolObservation, TerminalTurnStatus::Failed),
            (EvidenceClass::ActionOutcome, TerminalTurnStatus::Cancelled),
            (
                EvidenceClass::ToolObservation,
                TerminalTurnStatus::Interrupted,
            ),
        ] {
            database
                .capture_evidence(
                    OperationContext::new(Actor::System),
                    NewEvidence {
                        class,
                        scope: Scope::Session(Ulid::new()),
                        temporal: TemporalFacts::observed_at(400),
                        lifecycle: EvidenceLifecycle::TerminalTurn {
                            source_event_id: format!("outcome-{class:?}-{status:?}"),
                            status,
                        },
                        text: "truthful terminal outcome".into(),
                    },
                )
                .unwrap();
        }
        assert!(matches!(
            database.capture_evidence(
                OperationContext::new(Actor::System),
                NewEvidence {
                    class: EvidenceClass::ActionOutcome,
                    scope: Scope::Session(Ulid::new()),
                    temporal: TemporalFacts::observed_at(500),
                    lifecycle: EvidenceLifecycle::TerminalTurn {
                        source_event_id: "x".repeat(MAX_SOURCE_EVENT_ID_BYTES + 1),
                        status: TerminalTurnStatus::Failed,
                    },
                    text: "bounded terminal outcome".into(),
                },
            ),
            Err(MemoryError::InvalidInput(_))
        ));
    }

    #[test]
    fn assistant_cannot_activate_claims_or_semantic_relations() {
        let (_parent, _root, database) = create_test_database();
        let evidence = database
            .capture_evidence(
                OperationContext::new(Actor::User),
                NewEvidence {
                    class: EvidenceClass::UserAssertion,
                    scope: Scope::Personal,
                    temporal: TemporalFacts::observed_at(100),
                    lifecycle: EvidenceLifecycle::Direct,
                    text: "I prefer Rust".into(),
                },
            )
            .unwrap();
        let claim_input = NewClaim {
            domain: ClaimDomain::UserPreference,
            scope: Scope::Personal,
            temporal: TemporalFacts::observed_at(100),
            proposition: "The user prefers Rust".into(),
            evidence_ids: vec![evidence.id],
        };
        assert!(matches!(
            database.create_claim(OperationContext::new(Actor::Assistant), claim_input.clone(),),
            Err(MemoryError::Unauthorized)
        ));
        let claim = database
            .create_claim(OperationContext::new(Actor::User), claim_input)
            .unwrap();
        let relation_input = NewSemanticRelation {
            from: RecordRef::Evidence(evidence.id),
            to: RecordRef::Claim(claim.id),
            kind: RelationKind::Supports,
            scope: Scope::Personal,
            evidence_ids: vec![evidence.id],
            qualifier: None,
        };
        assert!(matches!(
            database.create_semantic_relation(
                OperationContext::new(Actor::Assistant),
                relation_input.clone(),
            ),
            Err(MemoryError::Unauthorized)
        ));
        assert!(database
            .create_semantic_relation(OperationContext::new(Actor::System), relation_input)
            .is_ok());
    }

    #[test]
    fn purge_preview_expires_after_ten_minutes() {
        let (_parent, _root, database) = create_test_database();
        let evidence = database
            .capture_evidence(
                OperationContext::new(Actor::User),
                NewEvidence {
                    class: EvidenceClass::UserAssertion,
                    scope: Scope::Personal,
                    temporal: TemporalFacts::observed_at(100),
                    lifecycle: EvidenceLifecycle::Direct,
                    text: "ephemeral".into(),
                },
            )
            .unwrap();
        let preview = database
            .preview_purge(Actor::Operator, RecordRef::Evidence(evidence.id))
            .unwrap();
        assert_eq!(
            preview.expires_at_ms - preview.issued_at_ms,
            PURGE_PREVIEW_TTL_MS
        );

        let mut expired = preview.clone();
        expired.issued_at_ms = 0;
        expired.expires_at_ms = 1;
        assert!(matches!(
            database.commit_purge(OperationContext::new(Actor::Operator), expired),
            Err(MemoryError::PurgePreviewExpired { .. })
        ));
        assert!(database
            .commit_purge(OperationContext::new(Actor::Operator), preview)
            .is_ok());
    }

    #[test]
    fn begin_turn_captures_raw_evidence_but_recalls_the_pre_turn_view() {
        use std::sync::Arc;
        use std::thread;
        use std::time::Instant;

        let (_parent, _root, database) = create_test_database();
        let database = Arc::new(database);
        let proposal_source = database
            .capture_evidence(
                OperationContext::new(Actor::System),
                NewEvidence {
                    class: EvidenceClass::ImportedSource,
                    scope: Scope::Personal,
                    temporal: TemporalFacts::observed_at(50),
                    lifecycle: EvidenceLifecycle::Direct,
                    text: "source available before the turn".into(),
                },
            )
            .unwrap();
        for index in 0..8 {
            database
                .capture_evidence(
                    OperationContext::new(Actor::System),
                    NewEvidence {
                        class: EvidenceClass::ToolObservation,
                        scope: Scope::Personal,
                        temporal: TemporalFacts::observed_at(60 + index),
                        lifecycle: EvidenceLifecycle::Direct,
                        text: format!("race_marker baseline {index}"),
                    },
                )
                .unwrap();
        }

        let begin_operation = OperationContext::new(Actor::User);
        let begin_database = Arc::clone(&database);
        let begin = thread::spawn(move || {
            begin_database.begin_turn_recall(
                begin_operation,
                NewEvidence {
                    class: EvidenceClass::UserAssertion,
                    scope: Scope::Personal,
                    temporal: TemporalFacts::observed_at(100),
                    lifecycle: EvidenceLifecycle::Direct,
                    text: "race_marker raw current turn".into(),
                },
                RecallQuery {
                    text: "race_marker".into(),
                    scopes: vec![Scope::Personal],
                    observed_from_ms: None,
                    observed_to_ms: None,
                    valid_at_ms: None,
                    limit: 100,
                },
            )
        });

        // The audit header becomes visible immediately after raw capture.  A
        // semantic writer launched from that point either waits on the same
        // gate or starts after recall has completed; it cannot enter the view.
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            let captured = database
                .audit_events(usize::MAX)
                .unwrap()
                .iter()
                .any(|event| {
                    event.operation_id == begin_operation.id
                        && event.action == AuditAction::EvidenceCaptured
                });
            if captured {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "begin-turn capture was not observed"
            );
            thread::yield_now();
        }

        let writer_database = Arc::clone(&database);
        let writer = thread::spawn(move || {
            writer_database.create_claim(
                OperationContext::new(Actor::System),
                NewClaim {
                    domain: ClaimDomain::ExternalFact,
                    scope: Scope::Personal,
                    temporal: TemporalFacts::observed_at(101),
                    proposition: "race_marker CONCURRENT_WRITE".into(),
                    evidence_ids: vec![proposal_source.id],
                },
            )
        });

        let result = begin.join().unwrap().unwrap();
        let concurrent_claim = writer.join().unwrap().unwrap();
        assert_eq!(
            database
                .inspect_evidence(result.evidence.id)
                .unwrap()
                .unwrap()
                .text
                .as_deref(),
            Some("race_marker raw current turn")
        );
        assert!(result
            .recall
            .citations
            .iter()
            .all(|citation| citation.record != RecordRef::Evidence(result.evidence.id)));
        assert!(!result.recall.citations.is_empty());
        assert!(result
            .recall
            .citations
            .iter()
            .all(|citation| !citation.text.contains("CONCURRENT_WRITE")));
        assert_eq!(
            database
                .inspect_claim(concurrent_claim.id)
                .unwrap()
                .unwrap()
                .header
                .state,
            RecordState::Active
        );
    }

    #[test]
    fn generic_retraction_cas_removes_recall_and_invalidates_dependents_once() {
        let (_parent, _root, database) = create_test_database();
        let evidence = database
            .capture_evidence(
                OperationContext::new(Actor::User),
                NewEvidence {
                    class: EvidenceClass::UserAssertion,
                    scope: Scope::Personal,
                    temporal: TemporalFacts::observed_at(100),
                    lifecycle: EvidenceLifecycle::Direct,
                    text: "remember retract_marker".into(),
                },
            )
            .unwrap();
        assert_eq!(
            database
                .inspect_evidence(evidence.id)
                .unwrap()
                .unwrap()
                .header
                .captured_by,
            Actor::User
        );
        let claim = database
            .create_claim(
                OperationContext::new(Actor::User),
                NewClaim {
                    domain: ClaimDomain::UserPreference,
                    scope: Scope::Personal,
                    temporal: TemporalFacts::observed_at(100),
                    proposition: "retract_marker is preferred".into(),
                    evidence_ids: vec![evidence.id],
                },
            )
            .unwrap();
        let relation = database
            .create_semantic_relation(
                OperationContext::new(Actor::System),
                NewSemanticRelation {
                    from: RecordRef::Evidence(evidence.id),
                    to: RecordRef::Claim(claim.id),
                    kind: RelationKind::Supports,
                    scope: Scope::Personal,
                    evidence_ids: vec![evidence.id],
                    qualifier: None,
                },
            )
            .unwrap();

        assert!(matches!(
            database.retract(
                OperationContext::new(Actor::User),
                RecordRef::Evidence(evidence.id),
                2,
            ),
            Err(MemoryError::RevisionConflict {
                expected: 2,
                actual: 1
            })
        ));
        let operation = OperationContext::new(Actor::User);
        let receipt = database
            .retract(operation, RecordRef::Evidence(evidence.id), 1)
            .unwrap();
        assert_eq!(receipt.target.state, RecordState::Retracted);
        assert_eq!(receipt.target.revision, 2);
        assert_eq!(receipt.invalidated.len(), 2);
        assert_eq!(
            database
                .retract(operation, RecordRef::Evidence(evidence.id), 1)
                .unwrap(),
            receipt
        );
        assert_eq!(
            database
                .inspect_claim(claim.id)
                .unwrap()
                .unwrap()
                .header
                .state,
            RecordState::Unsupported
        );
        assert_eq!(
            database
                .inspect_semantic_relation(relation.id)
                .unwrap()
                .unwrap()
                .header
                .state,
            RecordState::Unsupported
        );
        assert!(database
            .recall(RecallQuery {
                text: "retract_marker".into(),
                scopes: vec![Scope::Personal],
                observed_from_ms: None,
                observed_to_ms: None,
                valid_at_ms: None,
                limit: 10,
            })
            .unwrap()
            .citations
            .is_empty());
        let events = database.audit_events(20).unwrap();
        let retraction = events
            .iter()
            .find(|event| event.operation_id == operation.id)
            .unwrap();
        assert_eq!(retraction.action, AuditAction::RecordRetracted);
        assert_eq!(retraction.subjects.len(), 3);
    }

    #[test]
    fn remember_user_claim_commits_assertion_and_user_note_as_one_idempotent_operation() {
        let (_parent, _root, database) = create_test_database();
        let operation = OperationContext::new(Actor::User);
        let input = RememberUserClaim {
            domain: ClaimDomain::UserNote,
            scope: Scope::Personal,
            temporal: TemporalFacts::observed_at(200),
            evidence_text: "Remember note_marker exactly".into(),
            proposition: "note_marker exactly".into(),
        };

        let receipt = database
            .remember_user_claim(operation, input.clone())
            .unwrap();
        assert_eq!(
            database
                .remember_user_claim(operation, input.clone())
                .unwrap(),
            receipt
        );
        let evidence = database
            .inspect_evidence(receipt.evidence.id)
            .unwrap()
            .unwrap();
        assert_eq!(evidence.header.class, EvidenceClass::UserAssertion);
        assert_eq!(evidence.header.captured_by, Actor::User);
        assert_eq!(
            evidence.text.as_deref(),
            Some("Remember note_marker exactly")
        );
        let claim = database.inspect_claim(receipt.claim.id).unwrap().unwrap();
        assert_eq!(claim.header.domain, ClaimDomain::UserNote);
        assert_eq!(claim.header.evidence_ids, vec![receipt.evidence.id]);
        assert_eq!(claim.proposition.as_deref(), Some("note_marker exactly"));

        let operation_events: Vec<_> = database
            .audit_events(20)
            .unwrap()
            .into_iter()
            .filter(|event| event.operation_id == operation.id)
            .collect();
        assert_eq!(operation_events.len(), 1);
        assert_eq!(operation_events[0].action, AuditAction::UserClaimRemembered);
        assert_eq!(
            operation_events[0].subjects,
            vec![
                RecordRef::Evidence(receipt.evidence.id),
                RecordRef::Claim(receipt.claim.id),
            ]
        );
        let mut changed = input;
        changed.proposition = "different".into();
        assert!(matches!(
            database.remember_user_claim(operation, changed),
            Err(MemoryError::OperationConflict(id)) if id == operation.id
        ));

        assert!(matches!(
            database.remember_user_claim(
                OperationContext::new(Actor::User),
                RememberUserClaim {
                    domain: ClaimDomain::UserNote,
                    scope: Scope::Personal,
                    temporal: TemporalFacts::observed_at(201),
                    evidence_text: "uncommitted_marker".into(),
                    proposition: "   ".into(),
                },
            ),
            Err(MemoryError::InvalidInput(_))
        ));
        assert!(database
            .recall(RecallQuery {
                text: "uncommitted_marker".into(),
                scopes: vec![Scope::Personal],
                observed_from_ms: None,
                observed_to_ms: None,
                valid_at_ms: None,
                limit: 10,
            })
            .unwrap()
            .citations
            .is_empty());
    }

    #[test]
    fn correct_user_claim_atomically_supersedes_and_replaces_the_target() {
        let (_parent, _root, database) = create_test_database();
        let remembered = database
            .remember_user_claim(
                OperationContext::new(Actor::User),
                RememberUserClaim {
                    domain: ClaimDomain::UserNote,
                    scope: Scope::Personal,
                    temporal: TemporalFacts::observed_at(100),
                    evidence_text: "old_marker was said".into(),
                    proposition: "old_marker".into(),
                },
            )
            .unwrap();
        let old_relation = database
            .create_semantic_relation(
                OperationContext::new(Actor::System),
                NewSemanticRelation {
                    from: RecordRef::Evidence(remembered.evidence.id),
                    to: RecordRef::Claim(remembered.claim.id),
                    kind: RelationKind::Supports,
                    scope: Scope::Personal,
                    evidence_ids: vec![remembered.evidence.id],
                    qualifier: None,
                },
            )
            .unwrap();

        let operation = OperationContext::new(Actor::User);
        let input = CorrectUserClaim {
            target: remembered.claim.id,
            expected_revision: 1,
            temporal: TemporalFacts::observed_at(200),
            evidence_text: "Correction: new_marker is right".into(),
            replacement_proposition: "new_marker".into(),
        };
        let receipt = database
            .correct_user_claim(operation, input.clone())
            .unwrap();
        assert_eq!(
            database.correct_user_claim(operation, input).unwrap(),
            receipt
        );
        assert_ne!(receipt.replacement.id, remembered.claim.id);
        assert_eq!(receipt.superseded.state, RecordState::Superseded);
        assert_eq!(receipt.superseded.revision, 2);
        assert_eq!(receipt.invalidated.len(), 1);
        assert_eq!(
            receipt.invalidated[0].record,
            RecordRef::SemanticRelation(old_relation.id)
        );

        let correction = database
            .inspect_evidence(receipt.evidence.id)
            .unwrap()
            .unwrap();
        assert_eq!(correction.header.class, EvidenceClass::UserCorrection);
        assert_eq!(correction.header.captured_by, Actor::User);
        let old = database
            .inspect_claim(remembered.claim.id)
            .unwrap()
            .unwrap();
        assert_eq!(old.header.state, RecordState::Superseded);
        let replacement = database
            .inspect_claim(receipt.replacement.id)
            .unwrap()
            .unwrap();
        assert_eq!(replacement.header.domain, ClaimDomain::UserNote);
        assert_eq!(replacement.header.scope, Scope::Personal);
        assert_eq!(replacement.header.evidence_ids, vec![receipt.evidence.id]);
        assert_eq!(replacement.proposition.as_deref(), Some("new_marker"));
        let supersedes = database
            .inspect_semantic_relation(receipt.supersedes_relation.id)
            .unwrap()
            .unwrap();
        assert_eq!(
            supersedes.header.from,
            RecordRef::Claim(receipt.replacement.id)
        );
        assert_eq!(supersedes.header.to, RecordRef::Claim(remembered.claim.id));
        assert_eq!(supersedes.header.kind, RelationKind::Supersedes);
        assert_eq!(supersedes.header.state, RecordState::Active);
        assert_eq!(
            database
                .inspect_semantic_relation(old_relation.id)
                .unwrap()
                .unwrap()
                .header
                .state,
            RecordState::Unsupported
        );
        assert!(database
            .recall(RecallQuery {
                text: "old_marker".into(),
                scopes: vec![Scope::Personal],
                observed_from_ms: None,
                observed_to_ms: None,
                valid_at_ms: None,
                limit: 10,
            })
            .unwrap()
            .citations
            .iter()
            .all(|citation| citation.record != RecordRef::Claim(remembered.claim.id)));

        let operation_events: Vec<_> = database
            .audit_events(20)
            .unwrap()
            .into_iter()
            .filter(|event| event.operation_id == operation.id)
            .collect();
        assert_eq!(operation_events.len(), 1);
        assert_eq!(operation_events[0].action, AuditAction::UserClaimCorrected);
        assert!(matches!(
            database.correct_user_claim(
                OperationContext::new(Actor::User),
                CorrectUserClaim {
                    target: remembered.claim.id,
                    expected_revision: 1,
                    temporal: TemporalFacts::observed_at(201),
                    evidence_text: "another correction".into(),
                    replacement_proposition: "another value".into(),
                },
            ),
            Err(MemoryError::RevisionConflict {
                expected: 1,
                actual: 2
            })
        ));

        let untouched = database
            .remember_user_claim(
                OperationContext::new(Actor::User),
                RememberUserClaim {
                    domain: ClaimDomain::UserNote,
                    scope: Scope::Personal,
                    temporal: TemporalFacts::observed_at(300),
                    evidence_text: "atomic target".into(),
                    proposition: "atomic target".into(),
                },
            )
            .unwrap();
        assert!(matches!(
            database.correct_user_claim(
                OperationContext::new(Actor::User),
                CorrectUserClaim {
                    target: untouched.claim.id,
                    expected_revision: 1,
                    temporal: TemporalFacts::observed_at(301),
                    evidence_text: "uncommitted_correction_marker".into(),
                    replacement_proposition: "   ".into(),
                },
            ),
            Err(MemoryError::InvalidInput(_))
        ));
        assert_eq!(
            database
                .inspect_claim(untouched.claim.id)
                .unwrap()
                .unwrap()
                .header
                .state,
            RecordState::Active
        );
        assert!(database
            .recall(RecallQuery {
                text: "uncommitted_correction_marker".into(),
                scopes: vec![Scope::Personal],
                observed_from_ms: None,
                observed_to_ms: None,
                valid_at_ms: None,
                limit: 10,
            })
            .unwrap()
            .citations
            .is_empty());
    }

    #[test]
    fn generic_inspect_selects_head_or_an_exact_structural_revision() {
        let (_parent, _root, database) = create_test_database();
        let remembered = database
            .remember_user_claim(
                OperationContext::new(Actor::User),
                RememberUserClaim {
                    domain: ClaimDomain::UserNote,
                    scope: Scope::Personal,
                    temporal: TemporalFacts::observed_at(100),
                    evidence_text: "original inspect value".into(),
                    proposition: "original inspect value".into(),
                },
            )
            .unwrap();
        database
            .correct_user_claim(
                OperationContext::new(Actor::User),
                CorrectUserClaim {
                    target: remembered.claim.id,
                    expected_revision: 1,
                    temporal: TemporalFacts::observed_at(200),
                    evidence_text: "corrected inspect value".into(),
                    replacement_proposition: "corrected inspect value".into(),
                },
            )
            .unwrap();

        let historical = database
            .inspect(
                RecordRef::Claim(remembered.claim.id),
                RevisionSelector::Exact(1),
            )
            .unwrap()
            .unwrap();
        let InspectedRecord::Claim(historical) = historical else {
            panic!("expected a claim")
        };
        assert_eq!(historical.header.state, RecordState::Active);
        assert_eq!(
            historical.proposition.as_deref(),
            Some("original inspect value")
        );

        let head = database
            .inspect(
                RecordRef::Claim(remembered.claim.id),
                RevisionSelector::Head,
            )
            .unwrap()
            .unwrap();
        let InspectedRecord::Claim(head) = head else {
            panic!("expected a claim")
        };
        assert_eq!(head.header.state, RecordState::Superseded);
        assert!(database
            .inspect(
                RecordRef::Claim(remembered.claim.id),
                RevisionSelector::Exact(99),
            )
            .unwrap()
            .is_none());
    }

    fn imported_source(database: &MemoryDatabase, text: &str) -> EvidenceReceipt {
        database
            .capture_evidence(
                OperationContext::new(Actor::System),
                NewEvidence {
                    class: EvidenceClass::ImportedSource,
                    scope: Scope::Personal,
                    temporal: TemporalFacts::observed_at(500),
                    lifecycle: EvidenceLifecycle::Direct,
                    text: text.into(),
                },
            )
            .unwrap()
    }

    fn recall_case_for(database: &MemoryDatabase, text: &str) -> RecallCase {
        let recalled = database
            .recall(RecallQuery {
                text: text.into(),
                scopes: vec![Scope::Personal],
                observed_from_ms: None,
                observed_to_ms: None,
                valid_at_ms: None,
                limit: 10,
            })
            .unwrap();
        database
            .inspect_recall_case(recalled.case_id)
            .unwrap()
            .unwrap()
    }

    fn approve_proposal(
        database: &MemoryDatabase,
        proposal: &ProposalReceipt,
        recall_case: &RecallCase,
    ) -> ProposalReviewReceipt {
        database
            .review_proposal(
                OperationContext::new(Actor::Assistant),
                NewProposalReview {
                    proposal_id: proposal.id,
                    proposal_revision: proposal.revision,
                    recall_case_id: recall_case.id,
                    recall_case_revision: recall_case.revision,
                    verdict: ProposalReviewVerdict::Approve,
                    findings: Vec::new(),
                },
            )
            .unwrap()
    }

    #[test]
    fn adjudication_requires_real_authority_and_audits_the_structural_decision() {
        let (_parent, _root, database) = create_test_database();
        let source = imported_source(&database, "audited authority source");
        let recall_case = recall_case_for(&database, "audited authority source");
        let proposal = database
            .submit_proposal(
                OperationContext::new(Actor::Assistant),
                NewProposalBundle {
                    source_job_id: ProposalSourceJobId::new(),
                    scope: Scope::Personal,
                    source_evidence: vec![EvidenceRevisionPin {
                        id: source.id,
                        revision: 1,
                    }],
                    changes: vec![ProposalChange::CreateEntity {
                        draft_id: ProposalDraftId::new(),
                        kind: EntityKind::Concept,
                        temporal: TemporalFacts::observed_at(500),
                        canonical_name: "audited authority entity".into(),
                        aliases: Vec::new(),
                        evidence_ids: vec![source.id],
                    }],
                },
            )
            .unwrap();
        let review = approve_proposal(&database, &proposal, &recall_case);
        let base = ProposalAdjudication {
            proposal_id: proposal.id,
            expected_proposal_revision: review.proposal.revision,
            review_case_id: review.review_case_id,
            expected_review_revision: review.review_revision,
            decision: ProposalDecision::Accept,
            authority: AdjudicationAuthority::ExplicitOperator,
        };
        assert!(matches!(
            database.adjudicate_proposal(OperationContext::new(Actor::System), base.clone()),
            Err(MemoryError::Unauthorized)
        ));

        let operation = OperationContext::new(Actor::Operator);
        database.adjudicate_proposal(operation, base).unwrap();
        let event = database
            .audit_events(usize::MAX)
            .unwrap()
            .into_iter()
            .find(|event| event.operation_id == operation.id)
            .unwrap();
        assert_eq!(
            event.adjudication,
            Some(AdjudicationAudit {
                decision: ProposalDecision::Accept,
                authority: AdjudicationAuthority::ExplicitOperator,
            })
        );
    }

    #[test]
    fn review_findings_accept_only_exact_same_scope_sources_and_approve_fails_closed() {
        let (_parent, _root, database) = create_test_database();
        let source = imported_source(&database, "review_pin_marker proposal source");
        let session_scope = Scope::Session(Ulid::new());
        let cross_scope = database
            .capture_evidence(
                OperationContext::new(Actor::System),
                NewEvidence {
                    class: EvidenceClass::ImportedSource,
                    scope: session_scope.clone(),
                    temporal: TemporalFacts::observed_at(500),
                    lifecycle: EvidenceLifecycle::Direct,
                    text: "review_pin_marker cross scope".into(),
                },
            )
            .unwrap();
        let recalled = database
            .recall(RecallQuery {
                text: "review_pin_marker".into(),
                scopes: vec![Scope::Personal, session_scope],
                observed_from_ms: None,
                observed_to_ms: None,
                valid_at_ms: None,
                limit: 10,
            })
            .unwrap();
        let recall_case = database
            .inspect_recall_case(recalled.case_id)
            .unwrap()
            .unwrap();
        let unrelated = imported_source(&database, "not present in the pinned recall case");
        let proposal = database
            .submit_proposal(
                OperationContext::new(Actor::Assistant),
                NewProposalBundle {
                    source_job_id: ProposalSourceJobId::new(),
                    scope: Scope::Personal,
                    source_evidence: vec![EvidenceRevisionPin {
                        id: source.id,
                        revision: 1,
                    }],
                    changes: vec![ProposalChange::CreateEntity {
                        draft_id: ProposalDraftId::new(),
                        kind: EntityKind::Concept,
                        temporal: TemporalFacts::observed_at(500),
                        canonical_name: "review pin entity".into(),
                        aliases: Vec::new(),
                        evidence_ids: vec![source.id],
                    }],
                },
            )
            .unwrap();
        let review = |verdict, code, pin| NewProposalReview {
            proposal_id: proposal.id,
            proposal_revision: proposal.revision,
            recall_case_id: recall_case.id,
            recall_case_revision: recall_case.revision,
            verdict,
            findings: vec![ProposalReviewFinding {
                code,
                change_index: Some(0),
                pins: vec![pin],
            }],
        };

        assert!(matches!(
            database.review_proposal(
                OperationContext::new(Actor::Assistant),
                review(
                    ProposalReviewVerdict::NeedsUser,
                    ReviewFindingCode::AmbiguousIdentity,
                    RecordRevisionPin {
                        record: RecordRef::Evidence(unrelated.id),
                        revision: 1,
                    },
                ),
            ),
            Err(MemoryError::InvalidInput(message)) if message.contains("proposal evidence or recall case")
        ));
        assert!(matches!(
            database.review_proposal(
                OperationContext::new(Actor::Assistant),
                review(
                    ProposalReviewVerdict::NeedsUser,
                    ReviewFindingCode::ScopeMismatch,
                    RecordRevisionPin {
                        record: RecordRef::Evidence(cross_scope.id),
                        revision: 1,
                    },
                ),
            ),
            Err(MemoryError::ScopeMismatch)
        ));
        assert!(matches!(
            database.review_proposal(
                OperationContext::new(Actor::Assistant),
                review(
                    ProposalReviewVerdict::Approve,
                    ReviewFindingCode::EvidenceInsufficient,
                    RecordRevisionPin {
                        record: RecordRef::Evidence(source.id),
                        revision: 1,
                    },
                ),
            ),
            Err(MemoryError::InvalidInput(message)) if message.contains("approve")
        ));

        let accepted = database
            .review_proposal(
                OperationContext::new(Actor::Assistant),
                NewProposalReview {
                    proposal_id: proposal.id,
                    proposal_revision: proposal.revision,
                    recall_case_id: recall_case.id,
                    recall_case_revision: recall_case.revision,
                    verdict: ProposalReviewVerdict::NeedsUser,
                    findings: vec![ProposalReviewFinding {
                        code: ReviewFindingCode::AmbiguousIdentity,
                        change_index: Some(0),
                        pins: vec![RecordRevisionPin {
                            record: RecordRef::Evidence(source.id),
                            revision: 1,
                        }],
                    }],
                },
            )
            .unwrap();
        assert_eq!(
            database
                .inspect_proposal_review(accepted.review_case_id)
                .unwrap()
                .unwrap()
                .findings
                .unwrap()[0]
                .pins,
            vec![RecordRevisionPin {
                record: RecordRef::Evidence(source.id),
                revision: 1,
            }]
        );
    }

    #[test]
    fn proposal_drafts_are_not_recalled_and_operator_accept_maps_all_drafts_atomically() {
        let (_parent, root, database) = create_test_database();
        let source = imported_source(&database, "Ada wrote proposal_atomic_marker");
        let recall_case = recall_case_for(&database, "proposal_atomic_marker");
        let entity_draft = ProposalDraftId::new();
        let claim_draft = ProposalDraftId::new();
        let relation_draft = ProposalDraftId::new();
        let proposal = database
            .submit_proposal(
                OperationContext::new(Actor::Assistant),
                NewProposalBundle {
                    source_job_id: ProposalSourceJobId::new(),
                    scope: Scope::Personal,
                    source_evidence: vec![EvidenceRevisionPin {
                        id: source.id,
                        revision: 1,
                    }],
                    changes: vec![
                        ProposalChange::CreateEntity {
                            draft_id: entity_draft,
                            kind: EntityKind::Person,
                            temporal: TemporalFacts::observed_at(500),
                            canonical_name: "Ada Lovelace proposal_atomic_marker".into(),
                            aliases: vec!["Ada".into()],
                            evidence_ids: vec![source.id],
                        },
                        ProposalChange::CreateClaim {
                            draft_id: claim_draft,
                            domain: ClaimDomain::ExternalFact,
                            temporal: TemporalFacts::observed_at(500),
                            proposition: "Ada wrote proposal_atomic_marker".into(),
                            evidence_ids: vec![source.id],
                        },
                        ProposalChange::CreateRelation {
                            draft_id: relation_draft,
                            from: ProposalEndpoint::Draft(entity_draft),
                            to: ProposalEndpoint::Draft(claim_draft),
                            kind: RelationKind::About,
                            evidence_ids: vec![source.id],
                            qualifier: None,
                        },
                    ],
                },
            )
            .unwrap();
        assert_eq!(proposal.status, ProposalStatus::PendingReview);
        assert!(database
            .recall(RecallQuery {
                text: "Ada Lovelace".into(),
                scopes: vec![Scope::Personal],
                observed_from_ms: None,
                observed_to_ms: None,
                valid_at_ms: None,
                limit: 10,
            })
            .unwrap()
            .citations
            .iter()
            .all(|citation| !matches!(citation.record, RecordRef::Entity(_))));

        let review = approve_proposal(&database, &proposal, &recall_case);
        let adjudication = ProposalAdjudication {
            proposal_id: proposal.id,
            expected_proposal_revision: review.proposal.revision,
            review_case_id: review.review_case_id,
            expected_review_revision: review.review_revision,
            decision: ProposalDecision::Accept,
            authority: AdjudicationAuthority::ExplicitOperator,
        };
        assert!(matches!(
            database.adjudicate_proposal(
                OperationContext::new(Actor::Assistant),
                adjudication.clone()
            ),
            Err(MemoryError::Unauthorized)
        ));
        assert!(matches!(
            database
                .adjudicate_proposal(OperationContext::new(Actor::System), adjudication.clone()),
            Err(MemoryError::Unauthorized)
        ));
        let operation = OperationContext::new(Actor::Operator);
        let applied = database
            .adjudicate_proposal(operation, adjudication.clone())
            .unwrap();
        assert_eq!(applied.proposal.status, ProposalStatus::Applied);
        assert_eq!(applied.draft_mappings.len(), 3);
        assert_eq!(
            database
                .adjudicate_proposal(operation, adjudication.clone())
                .unwrap(),
            applied
        );
        let mapped = |draft| {
            applied
                .draft_mappings
                .iter()
                .find(|mapping| mapping.draft_id == draft)
                .unwrap()
                .record
        };
        let AppliedRecord::Entity(entity_id) = mapped(entity_draft) else {
            panic!("entity draft was not mapped")
        };
        let AppliedRecord::Claim(claim_id) = mapped(claim_draft) else {
            panic!("claim draft was not mapped")
        };
        let AppliedRecord::SemanticRelation(relation_id) = mapped(relation_draft) else {
            panic!("relation draft was not mapped")
        };
        let relation = database
            .inspect_semantic_relation(relation_id)
            .unwrap()
            .unwrap();
        assert_eq!(relation.header.from, RecordRef::Entity(entity_id));
        assert_eq!(relation.header.to, RecordRef::Claim(claim_id));
        assert!(database
            .recall(RecallQuery {
                text: "Ada Lovelace".into(),
                scopes: vec![Scope::Personal],
                observed_from_ms: None,
                observed_to_ms: None,
                valid_at_ms: None,
                limit: 10,
            })
            .unwrap()
            .citations
            .iter()
            .any(|citation| citation.record == RecordRef::Entity(entity_id)));

        let purge = database
            .preview_purge(Actor::Operator, RecordRef::Evidence(source.id))
            .unwrap();
        assert!(purge
            .invalidations
            .iter()
            .any(|dependency| dependency.record == RecordRef::Entity(entity_id)));
        database
            .commit_purge(OperationContext::new(Actor::Operator), purge)
            .unwrap();
        let unavailable = database.inspect_entity(entity_id).unwrap().unwrap();
        assert_eq!(unavailable.header.state, RecordState::Unsupported);
        assert!(unavailable.canonical_name.is_none());
        assert!(unavailable.aliases.is_empty());

        drop(database);
        let reopened = MemoryDatabase::open(&root).unwrap();
        assert_eq!(
            reopened
                .adjudicate_proposal(operation, adjudication)
                .unwrap(),
            applied
        );
    }

    #[test]
    fn proposal_source_job_and_operation_replay_survive_reopen() {
        let (_parent, root, database) = create_test_database();
        let source = imported_source(&database, "durable proposal source");
        let source_job_id = ProposalSourceJobId::new();
        let operation = OperationContext::new(Actor::Assistant);
        let input = NewProposalBundle {
            source_job_id,
            scope: Scope::Personal,
            source_evidence: vec![EvidenceRevisionPin {
                id: source.id,
                revision: 1,
            }],
            changes: vec![ProposalChange::CreateEntity {
                draft_id: ProposalDraftId::new(),
                kind: EntityKind::Concept,
                temporal: TemporalFacts::observed_at(500),
                canonical_name: "durable proposal entity".into(),
                aliases: Vec::new(),
                evidence_ids: vec![source.id],
            }],
        };
        let first = database.submit_proposal(operation, input.clone()).unwrap();
        assert_eq!(database.submit_proposal(operation, input).unwrap(), first);
        drop(database);

        let reopened = MemoryDatabase::open(&root).unwrap();
        assert_eq!(
            reopened
                .proposal_by_source_job(source_job_id)
                .unwrap()
                .unwrap()
                .header
                .id,
            first.id
        );
    }

    #[test]
    fn proposal_operator_reads_are_bounded_and_latest_review_is_directly_addressable() {
        let (_parent, _root, database) = create_test_database();
        let source = imported_source(&database, "operator proposal listing source");
        let recall_case = recall_case_for(&database, "operator proposal listing source");
        let submit = |name: &str| {
            database
                .submit_proposal(
                    OperationContext::new(Actor::Assistant),
                    NewProposalBundle {
                        source_job_id: ProposalSourceJobId::new(),
                        scope: Scope::Personal,
                        source_evidence: vec![EvidenceRevisionPin {
                            id: source.id,
                            revision: 1,
                        }],
                        changes: vec![ProposalChange::CreateEntity {
                            draft_id: ProposalDraftId::new(),
                            kind: EntityKind::Concept,
                            temporal: TemporalFacts::observed_at(500),
                            canonical_name: name.into(),
                            aliases: Vec::new(),
                            evidence_ids: vec![source.id],
                        }],
                    },
                )
                .unwrap()
        };
        let first = submit("first pending");
        let reviewed = submit("reviewed proposal");
        let third = submit("third pending");
        let review = approve_proposal(&database, &reviewed, &recall_case);

        let listed = database.list_proposals(2).unwrap();
        assert_eq!(listed.len(), 2);
        assert!(listed
            .iter()
            .all(|header| [first.id, reviewed.id, third.id].contains(&header.id)));
        let pending = database
            .list_pending_proposals(MAX_OPERATOR_LIST_LIMIT + 1)
            .unwrap();
        assert_eq!(
            pending
                .iter()
                .map(|header| header.id)
                .collect::<BTreeSet<_>>(),
            BTreeSet::from([first.id, third.id])
        );
        assert_eq!(
            database
                .list_awaiting_adjudication(MAX_OPERATOR_LIST_LIMIT + 1)
                .unwrap()
                .iter()
                .map(|header| header.id)
                .collect::<Vec<_>>(),
            vec![reviewed.id]
        );
        assert_eq!(
            database
                .latest_proposal_review(reviewed.id)
                .unwrap()
                .unwrap()
                .header
                .id,
            review.review_case_id
        );
        assert!(database.latest_proposal_review(first.id).unwrap().is_none());
        database
            .adjudicate_proposal(
                OperationContext::new(Actor::Operator),
                ProposalAdjudication {
                    proposal_id: reviewed.id,
                    expected_proposal_revision: review.proposal.revision,
                    review_case_id: review.review_case_id,
                    expected_review_revision: review.review_revision,
                    decision: ProposalDecision::Reject,
                    authority: AdjudicationAuthority::ExplicitOperator,
                },
            )
            .unwrap();
        assert!(database
            .list_awaiting_adjudication(MAX_OPERATOR_LIST_LIMIT)
            .unwrap()
            .is_empty());
    }

    #[test]
    fn proposal_bundle_has_an_aggregate_encoded_byte_budget() {
        let (_parent, _root, database) = create_test_database();
        let source = imported_source(&database, "aggregate proposal byte source");
        let changes = (0..17)
            .map(|index| ProposalChange::CreateClaim {
                draft_id: ProposalDraftId::new(),
                domain: ClaimDomain::ExternalFact,
                temporal: TemporalFacts::observed_at(500),
                proposition: format!("{index}{}", "x".repeat(MAX_TEXT_BYTES - 2)),
                evidence_ids: vec![source.id],
            })
            .collect();

        let error = database
            .submit_proposal(
                OperationContext::new(Actor::Assistant),
                NewProposalBundle {
                    source_job_id: ProposalSourceJobId::new(),
                    scope: Scope::Personal,
                    source_evidence: vec![EvidenceRevisionPin {
                        id: source.id,
                        revision: 1,
                    }],
                    changes,
                },
            )
            .unwrap_err();

        assert!(
            matches!(error, MemoryError::InvalidInput(message) if message.contains("encoded bytes"))
        );
    }

    #[test]
    fn proposal_bundle_has_an_aggregate_alias_budget() {
        let (_parent, _root, database) = create_test_database();
        let source = imported_source(&database, "aggregate proposal alias source");
        let changes = (0..17)
            .map(|entity| ProposalChange::CreateEntity {
                draft_id: ProposalDraftId::new(),
                kind: EntityKind::Concept,
                temporal: TemporalFacts::observed_at(500),
                canonical_name: format!("entity {entity}"),
                aliases: (0..MAX_ENTITY_ALIASES)
                    .map(|alias| format!("alias {entity} {alias}"))
                    .collect(),
                evidence_ids: vec![source.id],
            })
            .collect();

        let error = database
            .submit_proposal(
                OperationContext::new(Actor::Assistant),
                NewProposalBundle {
                    source_job_id: ProposalSourceJobId::new(),
                    scope: Scope::Personal,
                    source_evidence: vec![EvidenceRevisionPin {
                        id: source.id,
                        revision: 1,
                    }],
                    changes,
                },
            )
            .unwrap_err();

        assert!(
            matches!(error, MemoryError::InvalidInput(message) if message.contains("total aliases"))
        );
    }

    #[test]
    fn proposal_bundle_has_an_aggregate_dependency_edge_budget() {
        let (_parent, _root, database) = create_test_database();
        let sources: Vec<_> = (0..MAX_EVIDENCE_SOURCES)
            .map(|index| imported_source(&database, &format!("edge source {index}")))
            .collect();
        let source_evidence: Vec<_> = sources
            .iter()
            .map(|source| EvidenceRevisionPin {
                id: source.id,
                revision: 1,
            })
            .collect();
        let evidence_ids: Vec<_> = sources.iter().map(|source| source.id).collect();
        let changes = (0..64)
            .map(|index| ProposalChange::CreateClaim {
                draft_id: ProposalDraftId::new(),
                domain: ClaimDomain::ExternalFact,
                temporal: TemporalFacts::observed_at(500),
                proposition: format!("bounded edge claim {index}"),
                evidence_ids: evidence_ids.clone(),
            })
            .collect();

        let error = database
            .submit_proposal(
                OperationContext::new(Actor::Assistant),
                NewProposalBundle {
                    source_job_id: ProposalSourceJobId::new(),
                    scope: Scope::Personal,
                    source_evidence,
                    changes,
                },
            )
            .unwrap_err();

        assert!(
            matches!(error, MemoryError::InvalidInput(message) if message.contains("dependency edges"))
        );
    }

    #[test]
    fn stale_source_pin_prevents_any_proposal_change_from_applying() {
        let (_parent, _root, database) = create_test_database();
        let source = imported_source(&database, "stale proposal source marker");
        let recall_case = recall_case_for(&database, "stale proposal source marker");
        let proposal = database
            .submit_proposal(
                OperationContext::new(Actor::Assistant),
                NewProposalBundle {
                    source_job_id: ProposalSourceJobId::new(),
                    scope: Scope::Personal,
                    source_evidence: vec![EvidenceRevisionPin {
                        id: source.id,
                        revision: 1,
                    }],
                    changes: vec![ProposalChange::CreateEntity {
                        draft_id: ProposalDraftId::new(),
                        kind: EntityKind::Concept,
                        temporal: TemporalFacts::observed_at(500),
                        canonical_name: "must_not_apply_marker".into(),
                        aliases: Vec::new(),
                        evidence_ids: vec![source.id],
                    }],
                },
            )
            .unwrap();
        let review = approve_proposal(&database, &proposal, &recall_case);
        database
            .retract(
                OperationContext::new(Actor::Operator),
                RecordRef::Evidence(source.id),
                1,
            )
            .unwrap();
        let adjudication_operation = OperationContext::new(Actor::Operator);
        let receipt = database
            .adjudicate_proposal(
                adjudication_operation,
                ProposalAdjudication {
                    proposal_id: proposal.id,
                    expected_proposal_revision: review.proposal.revision + 1,
                    review_case_id: review.review_case_id,
                    expected_review_revision: review.review_revision + 1,
                    decision: ProposalDecision::Accept,
                    authority: AdjudicationAuthority::ExplicitOperator,
                },
            )
            .unwrap();
        assert_eq!(receipt.proposal.status, ProposalStatus::Stale);
        assert!(receipt.draft_mappings.is_empty());
        assert_eq!(
            database
                .audit_events(usize::MAX)
                .unwrap()
                .into_iter()
                .find(|event| event.operation_id == adjudication_operation.id)
                .unwrap()
                .adjudication,
            Some(AdjudicationAudit {
                decision: ProposalDecision::Accept,
                authority: AdjudicationAuthority::ExplicitOperator,
            })
        );
        assert!(database
            .recall(RecallQuery {
                text: "must_not_apply_marker".into(),
                scopes: vec![Scope::Personal],
                observed_from_ms: Some(0),
                observed_to_ms: None,
                valid_at_ms: None,
                limit: 10,
            })
            .unwrap()
            .citations
            .is_empty());
    }

    #[test]
    fn stale_recall_candidate_pin_marks_acceptance_stale_without_partial_writes() {
        let (_parent, _root, database) = create_test_database();
        let source = imported_source(&database, "independent proposal source");
        let candidate = imported_source(&database, "lawyer_candidate_marker");
        let recall_case = recall_case_for(&database, "lawyer_candidate_marker");
        assert_eq!(
            recall_case.candidates[0].record,
            RecordRef::Evidence(candidate.id)
        );
        let proposal = database
            .submit_proposal(
                OperationContext::new(Actor::Assistant),
                NewProposalBundle {
                    source_job_id: ProposalSourceJobId::new(),
                    scope: Scope::Personal,
                    source_evidence: vec![EvidenceRevisionPin {
                        id: source.id,
                        revision: 1,
                    }],
                    changes: vec![ProposalChange::CreateEntity {
                        draft_id: ProposalDraftId::new(),
                        kind: EntityKind::Concept,
                        temporal: TemporalFacts::observed_at(500),
                        canonical_name: "candidate_stale_must_not_apply".into(),
                        aliases: Vec::new(),
                        evidence_ids: vec![source.id],
                    }],
                },
            )
            .unwrap();
        let review = approve_proposal(&database, &proposal, &recall_case);
        database
            .retract(
                OperationContext::new(Actor::Operator),
                RecordRef::Evidence(candidate.id),
                1,
            )
            .unwrap();
        let receipt = database
            .adjudicate_proposal(
                OperationContext::new(Actor::Operator),
                ProposalAdjudication {
                    proposal_id: proposal.id,
                    expected_proposal_revision: review.proposal.revision,
                    review_case_id: review.review_case_id,
                    expected_review_revision: review.review_revision + 1,
                    decision: ProposalDecision::Accept,
                    authority: AdjudicationAuthority::ExplicitOperator,
                },
            )
            .unwrap();
        assert_eq!(receipt.proposal.status, ProposalStatus::Stale);
        assert!(receipt.changed_records.is_empty());
        assert!(database
            .recall(RecallQuery {
                text: "candidate_stale_must_not_apply".into(),
                scopes: vec![Scope::Personal],
                observed_from_ms: Some(0),
                observed_to_ms: None,
                valid_at_ms: None,
                limit: 10,
            })
            .unwrap()
            .citations
            .is_empty());
    }

    #[test]
    fn purge_during_review_removes_entity_proposal_and_review_payloads_and_blocks_accept() {
        let (_parent, _root, database) = create_test_database();
        let source = imported_source(&database, "purge_review_marker");
        let recall_case = recall_case_for(&database, "purge_review_marker");
        let proposal = database
            .submit_proposal(
                OperationContext::new(Actor::Assistant),
                NewProposalBundle {
                    source_job_id: ProposalSourceJobId::new(),
                    scope: Scope::Personal,
                    source_evidence: vec![EvidenceRevisionPin {
                        id: source.id,
                        revision: 1,
                    }],
                    changes: vec![ProposalChange::CreateEntity {
                        draft_id: ProposalDraftId::new(),
                        kind: EntityKind::Concept,
                        temporal: TemporalFacts::observed_at(500),
                        canonical_name: "purge_review_entity".into(),
                        aliases: Vec::new(),
                        evidence_ids: vec![source.id],
                    }],
                },
            )
            .unwrap();
        let review = approve_proposal(&database, &proposal, &recall_case);
        let preview = database
            .preview_purge(Actor::Operator, RecordRef::Evidence(source.id))
            .unwrap();
        assert!(preview
            .invalidations
            .iter()
            .any(|entry| entry.record == RecordRef::Proposal(proposal.id)));
        assert!(preview
            .invalidations
            .iter()
            .any(|entry| { entry.record == RecordRef::ProposalReview(review.review_case_id) }));
        database
            .commit_purge(OperationContext::new(Actor::Operator), preview)
            .unwrap();
        assert!(database
            .inspect_proposal(proposal.id)
            .unwrap()
            .unwrap()
            .changes
            .is_none());
        assert!(database
            .inspect_proposal_review(review.review_case_id)
            .unwrap()
            .unwrap()
            .findings
            .is_none());
        assert_eq!(
            database
                .inspect_proposal(proposal.id)
                .unwrap()
                .unwrap()
                .header
                .status,
            ProposalStatus::Stale
        );
        assert!(matches!(
            database.adjudicate_proposal(
                OperationContext::new(Actor::Operator),
                ProposalAdjudication {
                    proposal_id: proposal.id,
                    expected_proposal_revision: review.proposal.revision + 1,
                    review_case_id: review.review_case_id,
                    expected_review_revision: review.review_revision + 1,
                    decision: ProposalDecision::Accept,
                    authority: AdjudicationAuthority::ExplicitOperator,
                }
            ),
            Err(MemoryError::InvalidInput(_))
        ));
    }

    #[test]
    fn recall_feedback_is_structured_idempotent_and_does_not_correct_memory() {
        let (_parent, _root, database) = create_test_database();
        let source = imported_source(&database, "feedback_marker");
        let recall_case = recall_case_for(&database, "feedback_marker");
        let operation = OperationContext::new(Actor::User);
        let input = NewRecallFeedback {
            recall_case_id: recall_case.id,
            recall_case_revision: recall_case.revision,
            candidate: Some(RecordRevisionPin {
                record: RecordRef::Evidence(source.id),
                revision: 1,
            }),
            kind: RecallFeedbackKind::Irrelevant,
        };
        let first = database
            .record_recall_feedback(operation, input.clone())
            .unwrap();
        assert_eq!(
            database.record_recall_feedback(operation, input).unwrap(),
            first
        );
        assert_eq!(
            database.inspect_recall_feedback(first.id).unwrap().unwrap(),
            first
        );
        assert_eq!(
            database
                .inspect_evidence(source.id)
                .unwrap()
                .unwrap()
                .availability
                .state,
            RecordState::Active
        );
    }

    #[test]
    fn recall_feedback_kind_determines_whether_a_candidate_is_allowed() {
        let (_parent, _root, database) = create_test_database();
        let source = imported_source(&database, "feedback candidate semantics");
        let recall_case = recall_case_for(&database, "feedback candidate semantics");
        let candidate = RecordRevisionPin {
            record: RecordRef::Evidence(source.id),
            revision: 1,
        };

        assert!(matches!(
            database.record_recall_feedback(
                OperationContext::new(Actor::User),
                NewRecallFeedback {
                    recall_case_id: recall_case.id,
                    recall_case_revision: recall_case.revision,
                    candidate: Some(candidate),
                    kind: RecallFeedbackKind::MissingExpectedRecord,
                },
            ),
            Err(MemoryError::InvalidInput(message)) if message.contains("must not name a candidate")
        ));
        assert!(matches!(
            database.record_recall_feedback(
                OperationContext::new(Actor::User),
                NewRecallFeedback {
                    recall_case_id: recall_case.id,
                    recall_case_revision: recall_case.revision,
                    candidate: None,
                    kind: RecallFeedbackKind::Relevant,
                },
            ),
            Err(MemoryError::InvalidInput(message)) if message.contains("requires a candidate")
        ));
    }

    #[test]
    fn operational_fingerprint_is_keyed_stable_and_store_specific() {
        let (first_parent, first_root, first) = create_test_database();
        let (_, _, second) = create_test_database();
        let fingerprint = first
            .operational_fingerprint("assistant-memory-ledger", b"same durable input")
            .unwrap();
        assert_eq!(
            fingerprint,
            first
                .operational_fingerprint("assistant-memory-ledger", b"same durable input")
                .unwrap()
        );
        assert_ne!(
            fingerprint,
            first
                .operational_fingerprint("assistant-memory-terminal", b"same durable input")
                .unwrap()
        );
        assert_ne!(
            fingerprint,
            second
                .operational_fingerprint("assistant-memory-ledger", b"same durable input")
                .unwrap()
        );
        drop(first);
        let reopened = MemoryDatabase::open(&first_root).unwrap();
        assert_eq!(
            fingerprint,
            reopened
                .operational_fingerprint("assistant-memory-ledger", b"same durable input")
                .unwrap()
        );
        drop(first_parent);
    }

    fn artifact_snapshot_fixture(
        database: &MemoryDatabase,
        bytes: Vec<u8>,
    ) -> (ArtifactCollectionReceipt, ArtifactSnapshotReceipt) {
        let collection = database
            .create_artifact_collection(
                OperationContext::new(Actor::User),
                NewArtifactCollection {
                    label: "  exact collection label  ".into(),
                },
            )
            .unwrap();
        let snapshot = database
            .import_artifact_snapshot(
                OperationContext::new(Actor::System),
                NewArtifactSnapshot {
                    collection_id: collection.id,
                    expected_collection_revision: 1,
                    temporal: TemporalFacts::observed_at(42),
                    media_type: "application/octet-stream".into(),
                    bytes,
                },
            )
            .unwrap();
        (collection, snapshot)
    }

    #[test]
    fn artifact_snapshot_is_exact_immutable_and_replay_safe() {
        let (_parent, _root, database) = create_test_database();
        let collection = database
            .create_artifact_collection(
                OperationContext::new(Actor::User),
                NewArtifactCollection {
                    label: "  exact collection label  ".into(),
                },
            )
            .unwrap();
        assert_eq!(
            database
                .inspect_artifact_collection(collection.id)
                .unwrap()
                .unwrap()
                .label
                .as_deref(),
            Some("  exact collection label  ")
        );
        let operation = OperationContext::new(Actor::System);
        let bytes = vec![0, 255, 7, 0, 42];
        let input = NewArtifactSnapshot {
            collection_id: collection.id,
            expected_collection_revision: 1,
            temporal: TemporalFacts::observed_at(42),
            media_type: "application/octet-stream".into(),
            bytes: bytes.clone(),
        };
        let first = database
            .import_artifact_snapshot(operation, input.clone())
            .unwrap();
        assert_eq!(
            database
                .import_artifact_snapshot(operation, input.clone())
                .unwrap(),
            first
        );
        let conflicting = NewArtifactSnapshot {
            bytes: vec![1, 2, 3],
            ..input.clone()
        };
        assert!(matches!(
            database.import_artifact_snapshot(operation, conflicting),
            Err(MemoryError::OperationConflict(id)) if id == operation.id
        ));
        let record = database
            .inspect_artifact_snapshot(first.id)
            .unwrap()
            .unwrap();
        assert_eq!(record.header.byte_len, bytes.len() as u64);
        assert_eq!(
            record.media_type.as_deref(),
            Some("application/octet-stream")
        );
        assert_eq!(
            record.content_digest,
            Some(*blake3::hash(&bytes).as_bytes())
        );
        let materialized = database
            .materialize_artifact_snapshot(first.id)
            .unwrap()
            .unwrap();
        assert_eq!(materialized.bytes, bytes);
        assert_eq!(record.header.evidence_id, first.evidence.id);
        assert_eq!(
            database
                .artifact_provenance_for_evidence(first.evidence.id)
                .unwrap(),
            Some(ArtifactEvidenceProvenance::Snapshot {
                collection_id: collection.id,
                snapshot_id: first.id,
                byte_len: bytes.len() as u64,
            })
        );
        assert_eq!(
            database
                .inspect_evidence(first.evidence.id)
                .unwrap()
                .unwrap()
                .header
                .class,
            EvidenceClass::ArtifactSnapshot
        );
    }

    #[test]
    fn artifact_passage_batch_preserves_exact_text_locator_and_lineage() {
        let (_parent, _root, database) = create_test_database();
        let (collection, snapshot) = artifact_snapshot_fixture(&database, vec![1; 100]);
        let operation = OperationContext::new(Actor::System);
        let input = NewArtifactPassageBatch {
            snapshot_id: snapshot.id,
            expected_snapshot_revision: 1,
            passages: vec![
                NewArtifactPassage {
                    locator: ArtifactLocator {
                        ordinal: 0,
                        byte_range: Some(ArtifactRange {
                            start: 0,
                            end_exclusive: 10,
                        }),
                        page_range: Some(ArtifactRange {
                            start: 1,
                            end_exclusive: 2,
                        }),
                        time_range_ms: None,
                    },
                    text: "  exact first passage  ".into(),
                },
                NewArtifactPassage {
                    locator: ArtifactLocator {
                        ordinal: 1,
                        byte_range: Some(ArtifactRange {
                            start: 10,
                            end_exclusive: 20,
                        }),
                        page_range: None,
                        time_range_ms: Some(ArtifactRange {
                            start: 100,
                            end_exclusive: 200,
                        }),
                    },
                    text: "second passage".into(),
                },
            ],
        };
        let receipt = database
            .create_artifact_passages(operation, input.clone())
            .unwrap();
        assert_eq!(
            database.create_artifact_passages(operation, input).unwrap(),
            receipt
        );
        assert_eq!(receipt.passages.len(), 2);
        let first = database
            .inspect_artifact_passage(receipt.passages[0].id)
            .unwrap()
            .unwrap();
        assert_eq!(first.text.as_deref(), Some("  exact first passage  "));
        assert_eq!(first.header.collection_id, collection.id);
        assert_eq!(first.header.snapshot_id, snapshot.id);
        assert_eq!(first.header.evidence_id, receipt.passages[0].evidence.id);
        assert_eq!(first.header.locator.ordinal, 0);
        assert_eq!(
            database
                .artifact_provenance_for_evidence(receipt.passages[0].evidence.id)
                .unwrap(),
            Some(ArtifactEvidenceProvenance::Passage {
                collection_id: collection.id,
                snapshot_id: snapshot.id,
                passage_id: receipt.passages[0].id,
                locator: first.header.locator,
            })
        );
    }

    #[test]
    fn artifact_recall_citations_carry_typed_provenance_and_locator() {
        let (_parent, _root, database) = create_test_database();
        let (collection, snapshot) = artifact_snapshot_fixture(&database, vec![7; 100]);
        let passage = database
            .create_artifact_passages(
                OperationContext::new(Actor::System),
                NewArtifactPassageBatch {
                    snapshot_id: snapshot.id,
                    expected_snapshot_revision: 1,
                    passages: vec![NewArtifactPassage {
                        locator: ArtifactLocator {
                            ordinal: 4,
                            byte_range: Some(ArtifactRange {
                                start: 10,
                                end_exclusive: 20,
                            }),
                            page_range: None,
                            time_range_ms: None,
                        },
                        text: "typed provenance marker".into(),
                    }],
                },
            )
            .unwrap()
            .passages
            .remove(0);
        database
            .create_claim(
                OperationContext::new(Actor::System),
                NewClaim {
                    domain: ClaimDomain::ArtifactContent,
                    scope: Scope::Artifact(collection.id.0),
                    temporal: TemporalFacts::observed_at(43),
                    proposition: "typed provenance derived claim".into(),
                    evidence_ids: vec![passage.evidence.id],
                },
            )
            .unwrap();
        let recall = database
            .recall(RecallQuery {
                text: "typed provenance derived claim".into(),
                scopes: vec![Scope::Artifact(collection.id.0)],
                observed_from_ms: None,
                observed_to_ms: None,
                valid_at_ms: None,
                limit: 10,
            })
            .unwrap();
        let claim = recall
            .citations
            .iter()
            .find(|citation| matches!(citation.record, RecordRef::Claim(_)))
            .unwrap();
        assert_eq!(
            claim.evidence[0].artifact,
            Some(ArtifactEvidenceProvenance::Passage {
                collection_id: collection.id,
                snapshot_id: snapshot.id,
                passage_id: passage.id,
                locator: ArtifactLocator {
                    ordinal: 4,
                    byte_range: Some(ArtifactRange {
                        start: 10,
                        end_exclusive: 20,
                    }),
                    page_range: None,
                    time_range_ms: None,
                },
            })
        );
    }

    #[test]
    fn passage_ordinals_are_unique_across_batches_without_partial_write() {
        let (_parent, root, database) = create_test_database();
        let (_collection, snapshot) = artifact_snapshot_fixture(&database, vec![1; 100]);
        database
            .create_artifact_passages(
                OperationContext::new(Actor::System),
                NewArtifactPassageBatch {
                    snapshot_id: snapshot.id,
                    expected_snapshot_revision: 1,
                    passages: vec![NewArtifactPassage {
                        locator: ArtifactLocator {
                            ordinal: 2,
                            byte_range: None,
                            page_range: None,
                            time_range_ms: None,
                        },
                        text: "existing ordinal".into(),
                    }],
                },
            )
            .unwrap();
        let operation = OperationContext::new(Actor::System);
        let attempted = NewArtifactPassageBatch {
            snapshot_id: snapshot.id,
            expected_snapshot_revision: 1,
            passages: vec![
                NewArtifactPassage {
                    locator: ArtifactLocator {
                        ordinal: 1,
                        byte_range: None,
                        page_range: None,
                        time_range_ms: None,
                    },
                    text: "must not partially insert".into(),
                },
                NewArtifactPassage {
                    locator: ArtifactLocator {
                        ordinal: 2,
                        byte_range: None,
                        page_range: None,
                        time_range_ms: None,
                    },
                    text: "duplicate durable ordinal".into(),
                },
            ],
        };
        assert!(matches!(
            database.create_artifact_passages(operation, attempted),
            Err(MemoryError::InvalidInput(message)) if message.contains("already has passage ordinal 2")
        ));
        let retry = database
            .create_artifact_passages(
                operation,
                NewArtifactPassageBatch {
                    snapshot_id: snapshot.id,
                    expected_snapshot_revision: 1,
                    passages: vec![NewArtifactPassage {
                        locator: ArtifactLocator {
                            ordinal: 1,
                            byte_range: None,
                            page_range: None,
                            time_range_ms: None,
                        },
                        text: "valid retry".into(),
                    }],
                },
            )
            .unwrap();
        assert_eq!(retry.passages.len(), 1);
        drop(database);
        let reopened = MemoryDatabase::open(&root).unwrap();
        assert!(matches!(
            reopened.create_artifact_passages(
                OperationContext::new(Actor::System),
                NewArtifactPassageBatch {
                    snapshot_id: snapshot.id,
                    expected_snapshot_revision: 1,
                    passages: vec![NewArtifactPassage {
                        locator: ArtifactLocator {
                            ordinal: 1,
                            byte_range: None,
                            page_range: None,
                            time_range_ms: None,
                        },
                        text: "duplicate after reopen".into(),
                    }],
                },
            ),
            Err(MemoryError::InvalidInput(message)) if message.contains("already has passage ordinal 1")
        ));
    }

    #[test]
    fn artifact_inputs_are_bounded_before_any_partial_write() {
        let (_parent, _root, database) = create_test_database();
        let (collection, snapshot) = artifact_snapshot_fixture(&database, vec![1; 100]);
        let oversized_snapshot_operation = OperationContext::new(Actor::System);
        assert!(matches!(
            database.import_artifact_snapshot(
                oversized_snapshot_operation,
                NewArtifactSnapshot {
                    collection_id: collection.id,
                    expected_collection_revision: 1,
                    temporal: TemporalFacts::observed_at(1),
                    media_type: "x".into(),
                    bytes: vec![0; MAX_ARTIFACT_SNAPSHOT_BYTES + 1],
                }
            ),
            Err(MemoryError::InvalidInput(_))
        ));
        let invalid_batch_operation = OperationContext::new(Actor::System);
        assert!(matches!(
            database.create_artifact_passages(
                invalid_batch_operation,
                NewArtifactPassageBatch {
                    snapshot_id: snapshot.id,
                    expected_snapshot_revision: 1,
                    passages: vec![
                        NewArtifactPassage {
                            locator: ArtifactLocator { ordinal: 1, byte_range: None, page_range: None, time_range_ms: None },
                            text: "first".into(),
                        },
                        NewArtifactPassage {
                            locator: ArtifactLocator { ordinal: 0, byte_range: None, page_range: None, time_range_ms: None },
                            text: "second".into(),
                        },
                    ],
                }
            ),
            Err(MemoryError::InvalidInput(message)) if message.contains("increasing ordinals")
        ));
        assert!(matches!(
            database.create_artifact_passages(
                OperationContext::new(Actor::System),
                NewArtifactPassageBatch {
                    snapshot_id: snapshot.id,
                    expected_snapshot_revision: 1,
                    passages: (0..=MAX_ARTIFACT_PASSAGE_BATCH)
                        .map(|ordinal| NewArtifactPassage {
                            locator: ArtifactLocator {
                                ordinal: ordinal as u32,
                                byte_range: None,
                                page_range: None,
                                time_range_ms: None,
                            },
                            text: "x".into(),
                        })
                        .collect(),
                }
            ),
            Err(MemoryError::InvalidInput(message)) if message.contains("passages")
        ));
        assert!(matches!(
            database.create_artifact_passages(
                OperationContext::new(Actor::System),
                NewArtifactPassageBatch {
                    snapshot_id: snapshot.id,
                    expected_snapshot_revision: 1,
                    passages: (0..129)
                        .map(|ordinal| NewArtifactPassage {
                            locator: ArtifactLocator {
                                ordinal,
                                byte_range: None,
                                page_range: None,
                                time_range_ms: None,
                            },
                            text: "x".repeat(MAX_TEXT_BYTES),
                        })
                        .collect(),
                }
            ),
            Err(MemoryError::InvalidInput(message)) if message.contains("text bytes")
        ));
        let recovered = database
            .create_artifact_passages(
                invalid_batch_operation,
                NewArtifactPassageBatch {
                    snapshot_id: snapshot.id,
                    expected_snapshot_revision: 1,
                    passages: vec![NewArtifactPassage {
                        locator: ArtifactLocator {
                            ordinal: 0,
                            byte_range: None,
                            page_range: None,
                            time_range_ms: None,
                        },
                        text: "valid retry after rejected batch".into(),
                    }],
                },
            )
            .unwrap();
        assert_eq!(recovered.passages.len(), 1);
        assert_eq!(
            database
                .inspect_artifact_snapshot(snapshot.id)
                .unwrap()
                .unwrap()
                .availability
                .revision,
            1
        );
    }

    #[test]
    fn generic_capture_cannot_forge_artifact_evidence_and_purge_closes_lineage() {
        let (_parent, _root, database) = create_test_database();
        for class in [
            EvidenceClass::ArtifactSnapshot,
            EvidenceClass::ArtifactPassage,
        ] {
            assert!(matches!(
                database.capture_evidence(
                    OperationContext::new(Actor::System),
                    NewEvidence {
                        class,
                        scope: Scope::Artifact(Ulid::new()),
                        temporal: TemporalFacts::observed_at(1),
                        lifecycle: EvidenceLifecycle::Direct,
                        text: "forged".into(),
                    }
                ),
                Err(MemoryError::InvalidInput(message)) if message.contains("first-class artifact API")
            ));
        }
        let (_collection, snapshot) = artifact_snapshot_fixture(&database, vec![9; 100]);
        let passages = database
            .create_artifact_passages(
                OperationContext::new(Actor::System),
                NewArtifactPassageBatch {
                    snapshot_id: snapshot.id,
                    expected_snapshot_revision: 1,
                    passages: vec![NewArtifactPassage {
                        locator: ArtifactLocator {
                            ordinal: 0,
                            byte_range: None,
                            page_range: None,
                            time_range_ms: None,
                        },
                        text: "purge lineage".into(),
                    }],
                },
            )
            .unwrap();
        database
            .retract(
                OperationContext::new(Actor::Operator),
                RecordRef::ArtifactSnapshot(snapshot.id),
                1,
            )
            .unwrap();
        assert_eq!(
            database
                .inspect_artifact_passage(passages.passages[0].id)
                .unwrap()
                .unwrap()
                .availability
                .state,
            RecordState::Unsupported
        );
        let preview = database
            .preview_purge(Actor::Operator, RecordRef::ArtifactSnapshot(snapshot.id))
            .unwrap();
        database
            .commit_purge(OperationContext::new(Actor::Operator), preview)
            .unwrap();
        assert!(
            !database
                .inspect_artifact_snapshot(snapshot.id)
                .unwrap()
                .unwrap()
                .payload_available
        );
        assert!(database
            .materialize_artifact_snapshot(snapshot.id)
            .unwrap()
            .is_none());
        assert!(database
            .inspect_artifact_passage(passages.passages[0].id)
            .unwrap()
            .unwrap()
            .text
            .is_none());
        assert!(database
            .inspect_evidence(passages.passages[0].evidence.id)
            .unwrap()
            .unwrap()
            .text
            .is_none());
    }

    #[test]
    fn artifact_materialization_and_purge_are_linearized_at_the_blob_boundary() {
        let (_parent, _root, database) = create_test_database();
        let database = std::sync::Arc::new(database);
        let (_collection, snapshot) = artifact_snapshot_fixture(&database, vec![7; 128]);
        let preview = database
            .preview_purge(Actor::Operator, RecordRef::ArtifactSnapshot(snapshot.id))
            .unwrap();

        let metadata_observed = std::sync::Arc::new(std::sync::Barrier::new(2));
        let continue_materialization = std::sync::Arc::new(std::sync::Barrier::new(2));
        let materializer = {
            let database = database.clone();
            let metadata_observed = metadata_observed.clone();
            let continue_materialization = continue_materialization.clone();
            std::thread::spawn(move || {
                database.materialize_artifact_snapshot_with_observer(snapshot.id, || {
                    metadata_observed.wait();
                    continue_materialization.wait();
                })
            })
        };

        metadata_observed.wait();
        assert!(database.write_lock.try_lock().is_none());

        let purge_started = std::sync::Arc::new(std::sync::Barrier::new(2));
        let purger = {
            let database = database.clone();
            let purge_started = purge_started.clone();
            std::thread::spawn(move || {
                purge_started.wait();
                database.commit_purge(OperationContext::new(Actor::Operator), preview)
            })
        };
        purge_started.wait();
        continue_materialization.wait();

        let materialized = materializer.join().unwrap().unwrap().unwrap();
        assert_eq!(materialized.bytes, vec![7; 128]);
        purger.join().unwrap().unwrap();
        assert!(database
            .materialize_artifact_snapshot(snapshot.id)
            .unwrap()
            .is_none());
    }

    #[test]
    fn collection_purge_closes_artifact_evidence_and_derived_claim_lineage() {
        let (_parent, _root, database) = create_test_database();
        let (collection, snapshot) = artifact_snapshot_fixture(&database, vec![5; 100]);
        let passage = database
            .create_artifact_passages(
                OperationContext::new(Actor::System),
                NewArtifactPassageBatch {
                    snapshot_id: snapshot.id,
                    expected_snapshot_revision: 1,
                    passages: vec![NewArtifactPassage {
                        locator: ArtifactLocator {
                            ordinal: 0,
                            byte_range: None,
                            page_range: None,
                            time_range_ms: None,
                        },
                        text: "collection purge evidence".into(),
                    }],
                },
            )
            .unwrap()
            .passages
            .remove(0);
        let claim = database
            .create_claim(
                OperationContext::new(Actor::System),
                NewClaim {
                    domain: ClaimDomain::ArtifactContent,
                    scope: Scope::Artifact(collection.id.0),
                    temporal: TemporalFacts::observed_at(44),
                    proposition: "derived artifact claim payload".into(),
                    evidence_ids: vec![passage.evidence.id],
                },
            )
            .unwrap();
        let preview = database
            .preview_purge(
                Actor::Operator,
                RecordRef::ArtifactCollection(collection.id),
            )
            .unwrap();
        assert!(preview
            .invalidations
            .iter()
            .any(|entry| { entry.record == RecordRef::Evidence(passage.evidence.id) }));
        assert!(preview
            .invalidations
            .iter()
            .any(|entry| entry.record == RecordRef::Claim(claim.id)));
        database
            .commit_purge(OperationContext::new(Actor::Operator), preview)
            .unwrap();
        assert!(
            !database
                .inspect_artifact_snapshot(snapshot.id)
                .unwrap()
                .unwrap()
                .payload_available
        );
        assert!(database
            .materialize_artifact_snapshot(snapshot.id)
            .unwrap()
            .is_none());
        assert!(database
            .inspect_artifact_passage(passage.id)
            .unwrap()
            .unwrap()
            .text
            .is_none());
        assert!(database
            .inspect_evidence(passage.evidence.id)
            .unwrap()
            .unwrap()
            .text
            .is_none());
        let claim = database.inspect_claim(claim.id).unwrap().unwrap();
        assert_eq!(claim.header.state, RecordState::Unsupported);
        assert!(claim.proposition.is_none());
    }

    #[test]
    fn artifact_evidence_purge_closes_passage_and_derived_claim_lineage() {
        let (_parent, _root, database) = create_test_database();
        let (collection, snapshot) = artifact_snapshot_fixture(&database, vec![6; 100]);
        let passage = database
            .create_artifact_passages(
                OperationContext::new(Actor::System),
                NewArtifactPassageBatch {
                    snapshot_id: snapshot.id,
                    expected_snapshot_revision: 1,
                    passages: vec![NewArtifactPassage {
                        locator: ArtifactLocator {
                            ordinal: 0,
                            byte_range: None,
                            page_range: None,
                            time_range_ms: None,
                        },
                        text: "evidence-rooted purge".into(),
                    }],
                },
            )
            .unwrap()
            .passages
            .remove(0);
        let claim = database
            .create_claim(
                OperationContext::new(Actor::System),
                NewClaim {
                    domain: ClaimDomain::ArtifactContent,
                    scope: Scope::Artifact(collection.id.0),
                    temporal: TemporalFacts::observed_at(45),
                    proposition: "artifact evidence derived claim".into(),
                    evidence_ids: vec![passage.evidence.id],
                },
            )
            .unwrap();
        let preview = database
            .preview_purge(Actor::Operator, RecordRef::Evidence(passage.evidence.id))
            .unwrap();
        assert!(preview
            .invalidations
            .iter()
            .any(|entry| entry.record == RecordRef::ArtifactPassage(passage.id)));
        assert!(preview
            .invalidations
            .iter()
            .any(|entry| entry.record == RecordRef::Claim(claim.id)));
        database
            .commit_purge(OperationContext::new(Actor::Operator), preview)
            .unwrap();
        assert!(database
            .inspect_artifact_passage(passage.id)
            .unwrap()
            .unwrap()
            .text
            .is_none());
        assert!(database
            .inspect_claim(claim.id)
            .unwrap()
            .unwrap()
            .proposition
            .is_none());
    }

    #[test]
    fn maximum_snapshot_metadata_operations_do_not_materialize_the_blob() {
        let (_parent, _root, database) = create_test_database();
        let (_collection, snapshot) =
            artifact_snapshot_fixture(&database, vec![0xa5; MAX_ARTIFACT_SNAPSHOT_BYTES]);
        let metadata = database
            .inspect_artifact_snapshot(snapshot.id)
            .unwrap()
            .unwrap();
        assert_eq!(metadata.header.byte_len, MAX_ARTIFACT_SNAPSHOT_BYTES as u64);
        assert!(metadata.payload_available);

        // Simulate loss in the separately stored blob. Metadata/state paths
        // must remain usable and must not attempt to load or hash the blob.
        database
            .artifact_snapshot_blobs
            .remove(id_key(snapshot.id.0))
            .unwrap();
        database
            .inspect(
                RecordRef::ArtifactSnapshot(snapshot.id),
                RevisionSelector::Head,
            )
            .unwrap()
            .unwrap();
        database
            .retract(
                OperationContext::new(Actor::Operator),
                RecordRef::ArtifactSnapshot(snapshot.id),
                1,
            )
            .unwrap();
        let preview = database
            .preview_purge(Actor::Operator, RecordRef::ArtifactSnapshot(snapshot.id))
            .unwrap();
        database
            .commit_purge(OperationContext::new(Actor::Operator), preview)
            .unwrap();
    }

    #[test]
    fn relation_evaluator_definitions_are_operator_owned_versioned_and_idempotent() {
        let (_parent, _root, database) = create_test_database();
        let evaluator_id = RelationEvaluatorId::new();
        let input = NewRelationEvaluatorRevision {
            id: evaluator_id,
            expected_revision: None,
            kind: RelationEvaluatorKind::DeterministicRules,
            schema_version: 1,
            dimensions: vec![RelationDimension::Support, RelationDimension::TaskRelevance],
            provenance_digest: [0x11; 32],
        };
        assert!(matches!(
            database.put_relation_evaluator(OperationContext::new(Actor::System), input.clone()),
            Err(MemoryError::Unauthorized)
        ));

        let operation = OperationContext::new(Actor::Operator);
        let created = database
            .put_relation_evaluator(operation, input.clone())
            .unwrap();
        assert_eq!(created.pin.id, evaluator_id);
        assert_eq!(created.pin.revision, 1);
        assert_eq!(created.state, RecordState::Active);
        assert_eq!(
            database
                .put_relation_evaluator(operation, input.clone())
                .unwrap(),
            created
        );

        let mut changed = input.clone();
        changed.schema_version = 2;
        assert!(matches!(
            database.put_relation_evaluator(operation, changed),
            Err(MemoryError::OperationConflict(id)) if id == operation.id
        ));

        let revised = database
            .put_relation_evaluator(
                OperationContext::new(Actor::Operator),
                NewRelationEvaluatorRevision {
                    expected_revision: Some(1),
                    schema_version: 2,
                    provenance_digest: [0x22; 32],
                    ..input
                },
            )
            .unwrap();
        assert_eq!(revised.pin.revision, 2);
        let head = database
            .inspect_relation_evaluator(evaluator_id, RevisionSelector::Head)
            .unwrap()
            .unwrap();
        assert_eq!(head.header.revision, 2);
        assert_eq!(head.header.previous_revision, Some(1));
        assert_eq!(head.header.provenance_digest, [0x22; 32]);
        assert_eq!(
            database
                .inspect_relation_evaluator(evaluator_id, RevisionSelector::Exact(1))
                .unwrap()
                .unwrap()
                .header
                .schema_version,
            1
        );
    }

    #[test]
    fn relation_profiles_pin_an_exact_evaluator_and_reject_duplicate_heads() {
        let (_parent, _root, database) = create_test_database();
        let evaluator_id = RelationEvaluatorId::new();
        let evaluator = database
            .put_relation_evaluator(
                OperationContext::new(Actor::Operator),
                NewRelationEvaluatorRevision {
                    id: evaluator_id,
                    expected_revision: None,
                    kind: RelationEvaluatorKind::DeterministicRules,
                    schema_version: 1,
                    dimensions: vec![RelationDimension::Support, RelationDimension::TaskRelevance],
                    provenance_digest: [0x31; 32],
                },
            )
            .unwrap();
        let profile_id = RelationProfileId::new();
        let input = NewRelationProfileRevision {
            id: profile_id,
            expected_revision: None,
            evaluator: evaluator.pin,
            heads: vec![
                RelationHeadWeight {
                    dimension: RelationDimension::Support,
                    weight_micros: 1_000_000,
                },
                RelationHeadWeight {
                    dimension: RelationDimension::TaskRelevance,
                    weight_micros: 500_000,
                },
            ],
            provenance_digest: [0x32; 32],
        };
        assert!(matches!(
            database.put_relation_profile(OperationContext::new(Actor::System), input.clone()),
            Err(MemoryError::Unauthorized)
        ));
        let profile = database
            .put_relation_profile(OperationContext::new(Actor::Operator), input.clone())
            .unwrap();
        assert_eq!(profile.pin.id, profile_id);
        assert_eq!(profile.pin.revision, 1);
        assert_eq!(profile.availability, RelationProfileAvailability::Available);

        let mut duplicate = input.clone();
        duplicate.id = RelationProfileId::new();
        duplicate.heads.push(RelationHeadWeight {
            dimension: RelationDimension::Support,
            weight_micros: 1,
        });
        assert!(matches!(
            database.put_relation_profile(OperationContext::new(Actor::Operator), duplicate),
            Err(MemoryError::InvalidInput(message)) if message.contains("unique")
        ));

        database
            .put_relation_evaluator(
                OperationContext::new(Actor::Operator),
                NewRelationEvaluatorRevision {
                    id: evaluator_id,
                    expected_revision: Some(1),
                    kind: RelationEvaluatorKind::DeterministicRules,
                    schema_version: 2,
                    dimensions: vec![RelationDimension::Support, RelationDimension::TaskRelevance],
                    provenance_digest: [0x33; 32],
                },
            )
            .unwrap();
        assert_eq!(
            database
                .inspect_relation_profile(profile_id, RevisionSelector::Head)
                .unwrap()
                .unwrap()
                .availability,
            RelationProfileAvailability::StaleEvaluator
        );
    }

    #[test]
    fn relation_signals_are_versioned_fixed_point_sidecars_with_computed_staleness() {
        let (_parent, _root, database) = create_test_database();
        let first = imported_source(&database, "signal source alpha");
        let second = imported_source(&database, "signal source beta");
        let evaluator_id = RelationEvaluatorId::new();
        let evaluator = database
            .put_relation_evaluator(
                OperationContext::new(Actor::Operator),
                NewRelationEvaluatorRevision {
                    id: evaluator_id,
                    expected_revision: None,
                    kind: RelationEvaluatorKind::DeterministicRules,
                    schema_version: 1,
                    dimensions: vec![RelationDimension::Support],
                    provenance_digest: [0x41; 32],
                },
            )
            .unwrap();
        let profile_id = RelationProfileId::new();
        let profile = database
            .put_relation_profile(
                OperationContext::new(Actor::Operator),
                NewRelationProfileRevision {
                    id: profile_id,
                    expected_revision: None,
                    evaluator: evaluator.pin,
                    heads: vec![RelationHeadWeight {
                        dimension: RelationDimension::Support,
                        weight_micros: RELATION_FIXED_POINT_SCALE,
                    }],
                    provenance_digest: [0x42; 32],
                },
            )
            .unwrap();
        let input = NewRelationSignalBatch {
            evaluator: evaluator.pin,
            profile: profile.pin,
            signals: vec![NewRelationSignal {
                from: RecordRevisionPin {
                    record: RecordRef::Evidence(first.id),
                    revision: 1,
                },
                to: RecordRevisionPin {
                    record: RecordRef::Evidence(second.id),
                    revision: 1,
                },
                expected_signal: None,
                scores: vec![RelationDimensionScore {
                    dimension: RelationDimension::Support,
                    score_micros: 750_000,
                }],
                provenance_digest: [0x43; 32],
            }],
        };
        assert!(matches!(
            database.put_relation_signals(OperationContext::new(Actor::Operator), input.clone()),
            Err(MemoryError::Unauthorized)
        ));
        let operation = OperationContext::new(Actor::System);
        let created = database
            .put_relation_signals(operation, input.clone())
            .unwrap();
        assert_eq!(
            database
                .put_relation_signals(operation, input.clone())
                .unwrap(),
            created
        );
        let first_signal = created.signals[0];
        let record = database
            .inspect_relation_signal(first_signal.pin.id, RevisionSelector::Head)
            .unwrap()
            .unwrap();
        assert_eq!(record.availability, RelationSignalAvailability::Available);
        assert_eq!(record.scores.as_ref().unwrap()[0].score_micros, 750_000);

        let profile_v2 = database
            .put_relation_profile(
                OperationContext::new(Actor::Operator),
                NewRelationProfileRevision {
                    id: profile_id,
                    expected_revision: Some(1),
                    evaluator: evaluator.pin,
                    heads: vec![RelationHeadWeight {
                        dimension: RelationDimension::Support,
                        weight_micros: 500_000,
                    }],
                    provenance_digest: [0x44; 32],
                },
            )
            .unwrap();
        assert_eq!(
            database
                .inspect_relation_signal(first_signal.pin.id, RevisionSelector::Head)
                .unwrap()
                .unwrap()
                .availability,
            RelationSignalAvailability::Stale(RelationSignalStaleReason::ProfileAdvanced)
        );

        let revised = database
            .put_relation_signals(
                OperationContext::new(Actor::System),
                NewRelationSignalBatch {
                    evaluator: evaluator.pin,
                    profile: profile_v2.pin,
                    signals: vec![NewRelationSignal {
                        expected_signal: Some(first_signal.pin),
                        ..input.signals[0].clone()
                    }],
                },
            )
            .unwrap();
        assert_eq!(revised.signals[0].pin.id, first_signal.pin.id);
        assert_eq!(revised.signals[0].pin.revision, 2);
        assert_eq!(
            database
                .inspect_relation_signal(first_signal.pin.id, RevisionSelector::Head)
                .unwrap()
                .unwrap()
                .availability,
            RelationSignalAvailability::Available
        );

        database
            .retract(
                OperationContext::new(Actor::System),
                RecordRef::Evidence(first.id),
                1,
            )
            .unwrap();
        assert_eq!(
            database
                .inspect_relation_signal(first_signal.pin.id, RevisionSelector::Head)
                .unwrap()
                .unwrap()
                .availability,
            RelationSignalAvailability::Stale(RelationSignalStaleReason::SourceInactive)
        );
    }

    #[test]
    fn shadow_activation_is_one_hop_deterministic_structural_and_non_authoritative() {
        let (_parent, _root, database) = create_test_database();
        imported_source(&database, "shadow_query_marker common alpha");
        imported_source(&database, "shadow_query_marker common beta");
        let recall_case = recall_case_for(&database, "shadow_query_marker common");
        assert_eq!(recall_case.candidates.len(), 2);
        let baseline = recall_case.clone();
        let candidate_pins: Vec<_> = recall_case
            .candidates
            .iter()
            .map(|candidate| RecordRevisionPin {
                record: candidate.record,
                revision: candidate.revision,
            })
            .collect();

        let evaluator = database
            .put_relation_evaluator(
                OperationContext::new(Actor::Operator),
                NewRelationEvaluatorRevision {
                    id: RelationEvaluatorId::new(),
                    expected_revision: None,
                    kind: RelationEvaluatorKind::DeterministicRules,
                    schema_version: 1,
                    dimensions: vec![RelationDimension::TaskRelevance],
                    provenance_digest: [0x51; 32],
                },
            )
            .unwrap();
        let profile = database
            .put_relation_profile(
                OperationContext::new(Actor::Operator),
                NewRelationProfileRevision {
                    id: RelationProfileId::new(),
                    expected_revision: None,
                    evaluator: evaluator.pin,
                    heads: vec![RelationHeadWeight {
                        dimension: RelationDimension::TaskRelevance,
                        weight_micros: RELATION_FIXED_POINT_SCALE,
                    }],
                    provenance_digest: [0x52; 32],
                },
            )
            .unwrap();
        let signal = database
            .put_relation_signals(
                OperationContext::new(Actor::System),
                NewRelationSignalBatch {
                    evaluator: evaluator.pin,
                    profile: profile.pin,
                    signals: vec![NewRelationSignal {
                        from: candidate_pins[0],
                        to: candidate_pins[1],
                        expected_signal: None,
                        scores: vec![RelationDimensionScore {
                            dimension: RelationDimension::TaskRelevance,
                            score_micros: 800_000,
                        }],
                        provenance_digest: [0x53; 32],
                    }],
                },
            )
            .unwrap()
            .signals[0];

        let request = ShadowActivationRequest {
            recall_case: RecallCasePin {
                id: recall_case.id,
                revision: recall_case.revision,
            },
            evaluator: evaluator.pin,
            profile: profile.pin,
            candidates: candidate_pins.clone(),
        };
        assert!(matches!(
            database.shadow_activate(OperationContext::new(Actor::Operator), request.clone()),
            Err(MemoryError::Unauthorized)
        ));
        let operation = OperationContext::new(Actor::System);
        let trace = database
            .shadow_activate(operation, request.clone())
            .unwrap();
        assert_eq!(database.shadow_activate(operation, request).unwrap(), trace);
        assert_eq!(trace.contributions.len(), 1);
        assert_eq!(trace.contributions[0].signal, signal.pin);
        assert_eq!(trace.contributions[0].weighted_score_micros, 800_000);
        assert_eq!(
            trace
                .candidates
                .iter()
                .find(|candidate| candidate.candidate == candidate_pins[1])
                .unwrap()
                .shadow_rank,
            0
        );
        assert_eq!(
            database
                .inspect_recall_case(recall_case.id)
                .unwrap()
                .unwrap(),
            baseline
        );
        assert_eq!(
            database
                .inspect_activation_trace(trace.id)
                .unwrap()
                .unwrap(),
            trace
        );
        let trace_json = serde_json::to_string(&trace).unwrap();
        let audit_json = serde_json::to_string(&database.audit_events(100).unwrap()).unwrap();
        assert!(!trace_json.contains("shadow_query_marker"));
        assert!(!audit_json.contains("shadow_query_marker"));
    }

    #[test]
    fn canonical_purge_exactly_erases_dependent_relation_signal_payloads() {
        let (_parent, _root, database) = create_test_database();
        let first = imported_source(&database, "purge signal private alpha");
        let second = imported_source(&database, "purge signal private beta");
        let evaluator = database
            .put_relation_evaluator(
                OperationContext::new(Actor::Operator),
                NewRelationEvaluatorRevision {
                    id: RelationEvaluatorId::new(),
                    expected_revision: None,
                    kind: RelationEvaluatorKind::ExternalProjection,
                    schema_version: 1,
                    dimensions: vec![RelationDimension::Support],
                    provenance_digest: [0x61; 32],
                },
            )
            .unwrap();
        let profile = database
            .put_relation_profile(
                OperationContext::new(Actor::Operator),
                NewRelationProfileRevision {
                    id: RelationProfileId::new(),
                    expected_revision: None,
                    evaluator: evaluator.pin,
                    heads: vec![RelationHeadWeight {
                        dimension: RelationDimension::Support,
                        weight_micros: RELATION_FIXED_POINT_SCALE,
                    }],
                    provenance_digest: [0x62; 32],
                },
            )
            .unwrap();
        let signal = database
            .put_relation_signals(
                OperationContext::new(Actor::System),
                NewRelationSignalBatch {
                    evaluator: evaluator.pin,
                    profile: profile.pin,
                    signals: vec![NewRelationSignal {
                        from: RecordRevisionPin {
                            record: RecordRef::Evidence(first.id),
                            revision: 1,
                        },
                        to: RecordRevisionPin {
                            record: RecordRef::Evidence(second.id),
                            revision: 1,
                        },
                        expected_signal: None,
                        scores: vec![RelationDimensionScore {
                            dimension: RelationDimension::Support,
                            score_micros: 900_000,
                        }],
                        provenance_digest: [0x63; 32],
                    }],
                },
            )
            .unwrap()
            .signals[0];

        let recall_case = recall_case_for(&database, "purge signal private");
        let trace = database
            .shadow_activate(
                OperationContext::new(Actor::System),
                ShadowActivationRequest {
                    recall_case: RecallCasePin {
                        id: recall_case.id,
                        revision: recall_case.revision,
                    },
                    evaluator: evaluator.pin,
                    profile: profile.pin,
                    candidates: recall_case
                        .candidates
                        .iter()
                        .map(|candidate| RecordRevisionPin {
                            record: candidate.record,
                            revision: candidate.revision,
                        })
                        .collect(),
                },
            )
            .unwrap();

        let preview = database
            .preview_purge(Actor::Operator, RecordRef::Evidence(first.id))
            .unwrap();
        assert_eq!(
            preview.relation_signal_invalidations,
            vec![RelationSignalPurgeDependency { signal: signal.pin }]
        );
        assert_eq!(preview.activation_trace_invalidations, vec![trace.id]);
        let operation = OperationContext::new(Actor::Operator);
        let receipt = database.commit_purge(operation, preview.clone()).unwrap();
        assert_eq!(receipt.purged_relation_signals.len(), 1);
        assert_eq!(receipt.purged_relation_signals[0].id, signal.pin.id);
        assert_eq!(receipt.purged_relation_signals[0].revision, 2);
        assert_eq!(receipt.purged_activation_traces, vec![trace.id]);
        assert_eq!(database.commit_purge(operation, preview).unwrap(), receipt);
        assert!(database
            .inspect_activation_trace(trace.id)
            .unwrap()
            .is_none());

        let unavailable = database
            .inspect_relation_signal(signal.pin.id, RevisionSelector::Head)
            .unwrap()
            .unwrap();
        assert_eq!(unavailable.header.state, RecordState::Purged);
        assert!(unavailable.scores.is_none());
        assert!(unavailable.provenance_digest.is_none());
        assert_eq!(
            unavailable.availability,
            RelationSignalAvailability::Unavailable(RelationSignalUnavailableReason::PayloadPurged)
        );
        let audit_json = serde_json::to_string(&database.audit_events(100).unwrap()).unwrap();
        assert!(!audit_json.contains("purge signal private"));
    }

    #[test]
    fn definition_retraction_stales_and_definition_purge_erases_dependent_signals() {
        let (_parent, _root, database) = create_test_database();
        let first = imported_source(&database, "definition lifecycle alpha");
        let second = imported_source(&database, "definition lifecycle beta");
        let evaluator = database
            .put_relation_evaluator(
                OperationContext::new(Actor::Operator),
                NewRelationEvaluatorRevision {
                    id: RelationEvaluatorId::new(),
                    expected_revision: None,
                    kind: RelationEvaluatorKind::OfflineLearnedProjection,
                    schema_version: 1,
                    dimensions: vec![RelationDimension::TaskRelevance],
                    provenance_digest: [0x71; 32],
                },
            )
            .unwrap();
        let profile = database
            .put_relation_profile(
                OperationContext::new(Actor::Operator),
                NewRelationProfileRevision {
                    id: RelationProfileId::new(),
                    expected_revision: None,
                    evaluator: evaluator.pin,
                    heads: vec![RelationHeadWeight {
                        dimension: RelationDimension::TaskRelevance,
                        weight_micros: RELATION_FIXED_POINT_SCALE,
                    }],
                    provenance_digest: [0x72; 32],
                },
            )
            .unwrap();
        let signal = database
            .put_relation_signals(
                OperationContext::new(Actor::System),
                NewRelationSignalBatch {
                    evaluator: evaluator.pin,
                    profile: profile.pin,
                    signals: vec![NewRelationSignal {
                        from: RecordRevisionPin {
                            record: RecordRef::Evidence(first.id),
                            revision: 1,
                        },
                        to: RecordRevisionPin {
                            record: RecordRef::Evidence(second.id),
                            revision: 1,
                        },
                        expected_signal: None,
                        scores: vec![RelationDimensionScore {
                            dimension: RelationDimension::TaskRelevance,
                            score_micros: 400_000,
                        }],
                        provenance_digest: [0x73; 32],
                    }],
                },
            )
            .unwrap()
            .signals[0];

        let recall_case = recall_case_for(&database, "definition lifecycle");
        let trace = database
            .shadow_activate(
                OperationContext::new(Actor::System),
                ShadowActivationRequest {
                    recall_case: RecallCasePin {
                        id: recall_case.id,
                        revision: recall_case.revision,
                    },
                    evaluator: evaluator.pin,
                    profile: profile.pin,
                    candidates: recall_case
                        .candidates
                        .iter()
                        .map(|candidate| RecordRevisionPin {
                            record: candidate.record,
                            revision: candidate.revision,
                        })
                        .collect(),
                },
            )
            .unwrap();

        assert!(matches!(
            database.set_relation_profile_state(
                OperationContext::new(Actor::System),
                profile.pin,
                DerivedDefinitionStateChange::Retract
            ),
            Err(MemoryError::Unauthorized)
        ));
        let retracted = database
            .set_relation_profile_state(
                OperationContext::new(Actor::Operator),
                profile.pin,
                DerivedDefinitionStateChange::Retract,
            )
            .unwrap();
        assert_eq!(retracted.profile.state, RecordState::Retracted);
        assert!(retracted.purged_relation_signals.is_empty());
        assert_eq!(
            database
                .inspect_relation_signal(signal.pin.id, RevisionSelector::Head)
                .unwrap()
                .unwrap()
                .availability,
            RelationSignalAvailability::Stale(RelationSignalStaleReason::ProfileInactive)
        );

        let operation = OperationContext::new(Actor::Operator);
        let purged = database
            .set_relation_evaluator_state(
                operation,
                evaluator.pin,
                DerivedDefinitionStateChange::Purge,
            )
            .unwrap();
        assert_eq!(purged.evaluator.state, RecordState::Purged);
        assert_eq!(purged.purged_relation_signals.len(), 1);
        assert_eq!(purged.purged_activation_traces, vec![trace.id]);
        assert_eq!(
            database
                .set_relation_evaluator_state(
                    operation,
                    evaluator.pin,
                    DerivedDefinitionStateChange::Purge,
                )
                .unwrap(),
            purged
        );
        assert!(database
            .inspect_activation_trace(trace.id)
            .unwrap()
            .is_none());
        assert_eq!(
            database
                .inspect_relation_signal(signal.pin.id, RevisionSelector::Head)
                .unwrap()
                .unwrap()
                .availability,
            RelationSignalAvailability::Unavailable(RelationSignalUnavailableReason::PayloadPurged)
        );
    }

    #[test]
    fn relation_signal_owner_indexes_follow_evaluator_changes_exactly() {
        let (_parent, _root, database) = create_test_database();
        let first = imported_source(&database, "evaluator migration alpha");
        let second = imported_source(&database, "evaluator migration beta");
        let evaluator_one = database
            .put_relation_evaluator(
                OperationContext::new(Actor::Operator),
                NewRelationEvaluatorRevision {
                    id: RelationEvaluatorId::new(),
                    expected_revision: None,
                    kind: RelationEvaluatorKind::DeterministicRules,
                    schema_version: 1,
                    dimensions: vec![RelationDimension::Support],
                    provenance_digest: [0x81; 32],
                },
            )
            .unwrap();
        let evaluator_two = database
            .put_relation_evaluator(
                OperationContext::new(Actor::Operator),
                NewRelationEvaluatorRevision {
                    id: RelationEvaluatorId::new(),
                    expected_revision: None,
                    kind: RelationEvaluatorKind::ExternalProjection,
                    schema_version: 1,
                    dimensions: vec![RelationDimension::Support],
                    provenance_digest: [0x82; 32],
                },
            )
            .unwrap();
        let profile_id = RelationProfileId::new();
        let profile_one = database
            .put_relation_profile(
                OperationContext::new(Actor::Operator),
                NewRelationProfileRevision {
                    id: profile_id,
                    expected_revision: None,
                    evaluator: evaluator_one.pin,
                    heads: vec![RelationHeadWeight {
                        dimension: RelationDimension::Support,
                        weight_micros: RELATION_FIXED_POINT_SCALE,
                    }],
                    provenance_digest: [0x83; 32],
                },
            )
            .unwrap();
        let from = RecordRevisionPin {
            record: RecordRef::Evidence(first.id),
            revision: 1,
        };
        let to = RecordRevisionPin {
            record: RecordRef::Evidence(second.id),
            revision: 1,
        };
        let signal_one = database
            .put_relation_signals(
                OperationContext::new(Actor::System),
                NewRelationSignalBatch {
                    evaluator: evaluator_one.pin,
                    profile: profile_one.pin,
                    signals: vec![NewRelationSignal {
                        from,
                        to,
                        expected_signal: None,
                        scores: vec![RelationDimensionScore {
                            dimension: RelationDimension::Support,
                            score_micros: 100_000,
                        }],
                        provenance_digest: [0x84; 32],
                    }],
                },
            )
            .unwrap()
            .signals[0];
        let profile_two = database
            .put_relation_profile(
                OperationContext::new(Actor::Operator),
                NewRelationProfileRevision {
                    id: profile_id,
                    expected_revision: Some(1),
                    evaluator: evaluator_two.pin,
                    heads: vec![RelationHeadWeight {
                        dimension: RelationDimension::Support,
                        weight_micros: RELATION_FIXED_POINT_SCALE,
                    }],
                    provenance_digest: [0x85; 32],
                },
            )
            .unwrap();
        database
            .put_relation_signals(
                OperationContext::new(Actor::System),
                NewRelationSignalBatch {
                    evaluator: evaluator_two.pin,
                    profile: profile_two.pin,
                    signals: vec![NewRelationSignal {
                        from,
                        to,
                        expected_signal: Some(signal_one.pin),
                        scores: vec![RelationDimensionScore {
                            dimension: RelationDimension::Support,
                            score_micros: 200_000,
                        }],
                        provenance_digest: [0x86; 32],
                    }],
                },
            )
            .unwrap();

        let old_purge = database
            .set_relation_evaluator_state(
                OperationContext::new(Actor::Operator),
                evaluator_one.pin,
                DerivedDefinitionStateChange::Purge,
            )
            .unwrap();
        assert!(old_purge.purged_relation_signals.is_empty());
        assert_eq!(
            database
                .inspect_relation_signal(signal_one.pin.id, RevisionSelector::Head)
                .unwrap()
                .unwrap()
                .availability,
            RelationSignalAvailability::Available
        );
    }

    #[test]
    fn relation_signal_and_activation_inputs_enforce_aggregate_bounds_and_uniqueness() {
        let (_parent, _root, database) = create_test_database();
        let first = imported_source(&database, "bound alpha");
        let second = imported_source(&database, "bound beta");
        let evaluator = database
            .put_relation_evaluator(
                OperationContext::new(Actor::Operator),
                NewRelationEvaluatorRevision {
                    id: RelationEvaluatorId::new(),
                    expected_revision: None,
                    kind: RelationEvaluatorKind::DeterministicRules,
                    schema_version: 1,
                    dimensions: vec![RelationDimension::Support],
                    provenance_digest: [0x91; 32],
                },
            )
            .unwrap();
        let profile = database
            .put_relation_profile(
                OperationContext::new(Actor::Operator),
                NewRelationProfileRevision {
                    id: RelationProfileId::new(),
                    expected_revision: None,
                    evaluator: evaluator.pin,
                    heads: vec![RelationHeadWeight {
                        dimension: RelationDimension::Support,
                        weight_micros: RELATION_FIXED_POINT_SCALE,
                    }],
                    provenance_digest: [0x92; 32],
                },
            )
            .unwrap();
        let base = NewRelationSignal {
            from: RecordRevisionPin {
                record: RecordRef::Evidence(first.id),
                revision: 1,
            },
            to: RecordRevisionPin {
                record: RecordRef::Evidence(second.id),
                revision: 1,
            },
            expected_signal: None,
            scores: vec![RelationDimensionScore {
                dimension: RelationDimension::Support,
                score_micros: 1,
            }],
            provenance_digest: [0x93; 32],
        };
        assert!(matches!(
            database.put_relation_signals(
                OperationContext::new(Actor::System),
                NewRelationSignalBatch {
                    evaluator: evaluator.pin,
                    profile: profile.pin,
                    signals: vec![base.clone(), base.clone()],
                }
            ),
            Err(MemoryError::InvalidInput(message)) if message.contains("pairs must be unique")
        ));
        let mut duplicate_dimensions = base.clone();
        duplicate_dimensions
            .scores
            .push(duplicate_dimensions.scores[0]);
        assert!(matches!(
            database.put_relation_signals(
                OperationContext::new(Actor::System),
                NewRelationSignalBatch {
                    evaluator: evaluator.pin,
                    profile: profile.pin,
                    signals: vec![duplicate_dimensions],
                }
            ),
            Err(MemoryError::InvalidInput(message)) if message.contains("dimensions must be unique")
        ));
        let mut out_of_range = base.clone();
        out_of_range.scores[0].score_micros = RELATION_FIXED_POINT_SCALE + 1;
        assert!(matches!(
            database.put_relation_signals(
                OperationContext::new(Actor::System),
                NewRelationSignalBatch {
                    evaluator: evaluator.pin,
                    profile: profile.pin,
                    signals: vec![out_of_range],
                }
            ),
            Err(MemoryError::InvalidInput(message)) if message.contains("scores must be within")
        ));
        assert!(matches!(
            database.put_relation_signals(
                OperationContext::new(Actor::System),
                NewRelationSignalBatch {
                    evaluator: evaluator.pin,
                    profile: profile.pin,
                    signals: vec![base; MAX_RELATION_SIGNAL_BATCH + 1],
                }
            ),
            Err(MemoryError::InvalidInput(message)) if message.contains("signal batch")
        ));
        assert!(matches!(
            database.shadow_activate(
                OperationContext::new(Actor::System),
                ShadowActivationRequest {
                    recall_case: RecallCasePin {
                        id: RecallCaseId::new(),
                        revision: 1,
                    },
                    evaluator: evaluator.pin,
                    profile: profile.pin,
                    candidates: vec![
                        RecordRevisionPin {
                            record: RecordRef::Evidence(first.id),
                            revision: 1,
                        };
                        MAX_ACTIVATION_CANDIDATES + 1
                    ],
                }
            ),
            Err(MemoryError::InvalidInput(message)) if message.contains("candidates")
        ));
    }
}
