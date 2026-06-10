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

mod builder;
mod convert;
mod db;
mod embedder;
mod query_impl;
mod search;
#[cfg(test)]
mod tests;

pub use builder::{now_ms, NodeBuilder};
pub use db::Database;
pub use embedder::{
    DatabaseConfig, EmbedBatchFuture, EmbedFuture, Embedder, DEFAULT_MODEL, DEFAULT_TENANT,
};
pub use search::{Hit, HybridOpts, VectorFilter};
