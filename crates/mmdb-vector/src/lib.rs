//! mmdb-vector — HNSW-based vector index, optionally per-(tenant, model).
//!
//! P1 scope:
//! - In-memory `Hnsw<f32, DistCosine>` per `(tenant, model)` key
//! - fjall-backed metadata for crash-safe id mapping
//! - insert / search / delete (soft tombstones) / flush
//! - No reranking, no quantization, no online rebuild
pub mod hit;
pub mod index;
pub mod store;

pub use hit::ScoredHit;
pub use index::{IndexKey, VectorIndex, INDEX_DEFAULT_M, INDEX_DEFAULT_EF_CONSTRUCTION};
pub use store::VectorStore;
