//! # mmdb — embedded multi-model memory store for AI agents
//!
//! `mmdb` is a single-process Rust library that bundles:
//!
//! - **LSM key/value store** (via `fjall`) for time-indexed `MemoryNode`s.
//! - **HNSW vector index** (via `hnsw_rs`) for ANN search over embeddings.
//! - **Property graph store** (via `mmdb_graph`) for relations between nodes.
//! - **Optional auto-embedder** so callers can store and query raw text.
//!
//! ## Quickstart
//!
//! ```no_run
//! use mmdb::{Database, DatabaseConfig, Embedder};
//! use mmdb_core::{NodeKind, Result};
//!
//! struct MyEmbedder;
//! impl Embedder for MyEmbedder {
//!     fn embed(&self, _text: &str) -> Result<Vec<f32>> { Ok(vec![0.0; 64]) }
//!     fn model_name(&self) -> &str { "my-model-64" }
//!     fn dim(&self) -> u32 { 64 }
//! }
//!
//! # fn main() -> anyhow::Result<()> {
//! let cfg = DatabaseConfig { tenant: 0, default_model: "my-model-64".into() };
//! let db  = Database::open_with_embedder("/tmp/mmdb", cfg, Box::new(MyEmbedder))?;
//! let _id = db.insert_text(NodeKind::Fact, "the sky is blue")?;
//! let hits = db.search_text("what color is the sky", 3)?;
//! for h in hits { println!("{:.3} {:?}", h.score, h.node.content); }
//! # Ok(()) }
//! ```
//!
//! ## Design notes (single-tenant, Jun 2026)
//!
//! - The user-facing API hides `tenant`; the underlying storage layer still
//!   namespaces every key by `tenant_be(4)` so future MVCC / multi-agent
//!   isolation is non-breaking.
//! - [`Database::open_with`] lets callers pin a default embedding model name;
//!   the simple [`Database::vector_search`] path uses it. Power users that
//!   need multiple models can call [`Database::vector_search_with_model`].

pub use mmdb_blob as blob;
pub use mmdb_catalog as catalog;
pub use mmdb_core as core;
pub use mmdb_graph as graph;
pub use mmdb_query as query;
pub use mmdb_storage as storage;
pub use mmdb_vector as vector;

/// Declare a module's partition schema together with its typed handles.
macro_rules! module_partitions {
    ($parts:ident, $schema:ident, { $($field:ident => $name:expr),+ $(,)? }) => {
        pub(crate) const $schema: &[mmdb_kernel::PartitionSpec] = &[$(mmdb_kernel::PartitionSpec::Inline($name)),+];

        pub(crate) struct $parts {
            $(pub(crate) $field: mmdb_kernel::Partition,)+
        }

        impl $parts {
            pub(crate) fn open(era: &mmdb_kernel::EraPartitions) -> mmdb_kernel::KernelResult<Self> {
                Ok(Self { $($field: era.partition($name)?,)+ })
            }
        }
    };
}

mod access;
mod audit;
mod builder;
pub mod context;
mod convert;
mod db;
mod dream;
mod embedder;
pub mod entity;
mod extraction;
mod lexical;
pub mod native_memory;
mod query_impl;
mod recall;
mod records;
mod runtime;
mod search;
pub mod session;
mod state;
pub mod store;
pub use mmdb_kernel::{store_format, EraId, OperationId};
pub use records::{
    Actor, CheckpointMetadata, CheckpointScope, ContextAccess, ContextError, ContextRef,
    ContextResult, Endpoints, EventKind, InformationKind, LogEvent, MessageStatus,
    PayloadDescriptor, PayloadPage, Provenance, RecordHeader, RecordKind, RecordPin, RecordState,
    RecoveryReport, Scope, StoreModule, TemporalFacts, ToolOutcome, TurnEndReason, TurnError,
    TurnStamp, WriteReceipt, MAX_ACCESS_SCOPES, MAX_CONTEXT_NAME_BYTES, MAX_CONTEXT_SOURCES,
    MAX_PAYLOAD_PAGE_BYTES, MAX_RECORD_BYTES, PAYLOAD_CHUNK_BYTES,
};
pub use session::MessageState;
pub use store::{
    MemoryResetPlan, MemoryResetReceipt, Store, StoreError, StoreResult, STORE_FORMAT_ID,
};
#[cfg(test)]
mod tests;

pub use access::AccessStats;
pub use audit::{AuditAction, AuditContext, AuditFilter, AuditRecord};
pub use builder::{now_ms, NodeBuilder};
pub use db::Database;
pub use dream::{
    DreamEdge, DreamEndpoint, DreamNode, DreamProposal, DreamRun, DreamRunStatus,
    DreamSupersession, MaintenanceTrigger, SupersededSnapshot,
};
pub use embedder::{
    DatabaseConfig, EmbedBatchFuture, EmbedFuture, Embedder, DEFAULT_MODEL, DEFAULT_TENANT,
};
pub use recall::{
    AdjudicatedRecall, CausalAnnotation, ChangeProposal, ChangeProposalStatus, GraphPath,
    GraphSlice, LawyerVerdict, ProposedChange, RecallEvidence, RecallFilter, RecallRequest,
    RecallStatus, VectorEvidence,
};
pub use runtime::{
    AgentClient, AgentRequest, AgentResponse, AgentRole, BlobInput, ClientFuture, ClientRegistry,
    DatabaseBuilder, DreamProfile, EmbeddingClient, EmbeddingDistance, EmbeddingInput,
    EmbeddingOutput, EmbeddingProfile, IngestReport, LawyerFailureMode, LawyerProfile,
    MemoryProfile, ProjectionState, ProjectionStatus, SupportedContent,
};
pub use search::{Hit, HybridOpts, VectorFilter};
pub use state::{NamespacedStateMutation, StateEntry, StateMutation, StatePage, StateStore};
pub use store_format::StoreFormatDescriptor;
