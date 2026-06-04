//! mmdb high-level facade.
//!
//! Design notes (single-tenant simplification, Jun 2026):
//! - The user-facing API hides `tenant`; the underlying storage layer still
//!   namespaces every key by `tenant_be(4)` so future MVCC branching / multi-
//!   agent isolation is non-breaking.
//! - `Database::open_with` lets callers pin a default embedding model name;
//!   the simple `vector_search(query, k)` path uses it. Power users that need
//!   multiple models can call `vector_search_with_model`.
use mmdb_core::{Content, Embedding, MemoryNode, NodeKind, Result};
use mmdb_storage::Storage;
use smallvec::SmallVec;
use std::collections::BTreeMap;
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};
use ulid::Ulid;

pub use mmdb_core as core;
pub use mmdb_storage as storage;

/// Default tenant id for single-tenant deployments.
pub const DEFAULT_TENANT: u32 = 0;

/// Default embedding model name when the user does not configure one.
pub const DEFAULT_MODEL: &str = "default";

#[derive(Debug, Clone)]
pub struct DatabaseConfig {
    /// Logical tenant id. Single-tenant users should leave this as DEFAULT_TENANT.
    pub tenant: u32,
    /// Name of the embedding model used by the default `vector_search` path.
    pub default_model: String,
}

impl Default for DatabaseConfig {
    fn default() -> Self {
        Self {
            tenant: DEFAULT_TENANT,
            default_model: DEFAULT_MODEL.to_string(),
        }
    }
}

pub struct Database {
    storage: Storage,
    config: DatabaseConfig,
}

impl Database {
    /// Open with defaults (tenant=0, model="default"). Best for single-user agents.
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        Self::open_with(path, DatabaseConfig::default())
    }

    /// Open with an explicit config.
    pub fn open_with(path: impl AsRef<Path>, config: DatabaseConfig) -> Result<Self> {
        Ok(Self { storage: Storage::open(path)?, config })
    }

    pub fn config(&self) -> &DatabaseConfig {
        &self.config
    }

    pub fn insert(&self, node: MemoryNode) -> Result<Ulid> {
        let mut node = node;
        // Force-stamp the configured tenant so users cannot accidentally cross
        // boundaries via NodeBuilder.
        node.tenant = self.config.tenant;
        let id = node.id;
        self.storage.put_node(&node)?;
        // TODO(P1): for each embedding in node.embeddings, also insert into VectorStore.
        Ok(id)
    }

    pub fn get(&self, id: Ulid) -> Result<Option<MemoryNode>> {
        self.storage.get_node(self.config.tenant, id)
    }

    pub fn scan_by_time(
        &self,
        from_ms: i64,
        to_ms: i64,
        limit: usize,
    ) -> Result<Vec<MemoryNode>> {
        self.storage.scan_by_time(self.config.tenant, from_ms, to_ms, limit)
    }

    pub fn delete(&self, id: Ulid) -> Result<()> {
        self.storage.delete_node(self.config.tenant, id)
    }

    /// Vector search using the database default model.
    ///
    /// **P1 stub** — wiring to `mmdb-vector` lands in the next milestone.
    pub fn vector_search(&self, _query: &[f32], _k: usize) -> Result<Vec<Hit>> {
        // Will dispatch to VectorStore(tenant, default_model).search(...)
        Ok(Vec::new())
    }

    /// Vector search against an explicit model name. Use this only when you
    /// genuinely need multiple embedding spaces (e.g. CLIP + text).
    ///
    /// **P1 stub** — wiring to `mmdb-vector` lands in the next milestone.
    pub fn vector_search_with_model(
        &self,
        _model: &str,
        _query: &[f32],
        _k: usize,
    ) -> Result<Vec<Hit>> {
        Ok(Vec::new())
    }
}

#[derive(Debug, Clone)]
pub struct Hit {
    pub node: MemoryNode,
    pub score: f32,
}

/// NodeBuilder — tenant is no longer a parameter (set by Database on insert).
pub struct NodeBuilder {
    kind: NodeKind,
    content: Option<Content>,
    embeddings: SmallVec<[Embedding; 1]>,
    metadata: BTreeMap<String, serde_json::Value>,
    created_at_ms: Option<i64>,
}

impl NodeBuilder {
    pub fn new(kind: NodeKind) -> Self {
        Self {
            kind,
            content: None,
            embeddings: SmallVec::new(),
            metadata: BTreeMap::new(),
            created_at_ms: None,
        }
    }

    pub fn text(mut self, s: impl Into<String>) -> Self {
        self.content = Some(Content::Text(s.into()));
        self
    }

    pub fn structured(mut self, v: serde_json::Value) -> Self {
        self.content = Some(Content::Structured(v));
        self
    }

    /// Attach an embedding. For the simple single-model path, omit this and
    /// let the writer pipeline fill it in, or pass `DEFAULT_MODEL`.
    pub fn embedding(mut self, model: impl Into<String>, vector: Vec<f32>) -> Self {
        let dim = vector.len() as u32;
        self.embeddings.push(Embedding {
            model: model.into(),
            dim,
            vector: SmallVec::from_vec(vector),
        });
        self
    }

    pub fn metadata(mut self, key: impl Into<String>, value: serde_json::Value) -> Self {
        self.metadata.insert(key.into(), value);
        self
    }

    pub fn created_at(mut self, ts_ms: i64) -> Self {
        self.created_at_ms = Some(ts_ms);
        self
    }

    pub fn build(self) -> MemoryNode {
        let now = self.created_at_ms.unwrap_or_else(now_ms);
        MemoryNode {
            id: Ulid::new(),
            // tenant placeholder; Database::insert will overwrite with its config.
            tenant: DEFAULT_TENANT,
            kind: self.kind,
            created_at_ms: now,
            updated_at_ms: now,
            content: self.content.unwrap_or(Content::Text(String::new())),
            embeddings: self.embeddings,
            metadata: self.metadata,
        }
    }
}

pub fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn insert_get_scan_delete_roundtrip() {
        let dir = tempdir().unwrap();
        let db = Database::open(dir.path()).unwrap();
        let node = NodeBuilder::new(NodeKind::Episode)
            .text("hello world")
            .metadata("source", serde_json::json!("test"))
            .created_at(1000)
            .build();
        let id = db.insert(node).unwrap();

        let got = db.get(id).unwrap().unwrap();
        assert!(matches!(got.content, Content::Text(ref s) if s == "hello world"));
        assert_eq!(got.tenant, DEFAULT_TENANT);

        let scanned = db.scan_by_time(0, 2000, 10).unwrap();
        assert_eq!(scanned.len(), 1);

        db.delete(id).unwrap();
        assert!(db.get(id).unwrap().is_none());
    }

    #[test]
    fn open_with_custom_model_persists_config() {
        let dir = tempdir().unwrap();
        let cfg = DatabaseConfig {
            tenant: DEFAULT_TENANT,
            default_model: "bge-m3".to_string(),
        };
        let db = Database::open_with(dir.path(), cfg).unwrap();
        assert_eq!(db.config().default_model, "bge-m3");

        // vector_search returns empty until P1 vector wiring lands
        let hits = db.vector_search(&[0.1, 0.2, 0.3], 5).unwrap();
        assert!(hits.is_empty());
    }

    #[test]
    fn insert_forces_tenant_from_config() {
        let dir = tempdir().unwrap();
        let db = Database::open(dir.path()).unwrap();
        let mut node = NodeBuilder::new(NodeKind::Fact).text("x").build();
        // Even if a caller tampers with tenant pre-insert, Database normalizes it.
        node.tenant = 999;
        let id = db.insert(node).unwrap();
        let got = db.get(id).unwrap().unwrap();
        assert_eq!(got.tenant, DEFAULT_TENANT);
    }
}
