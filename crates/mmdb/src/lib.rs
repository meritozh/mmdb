//! mmdb high-level facade.
use mmdb_core::{Content, Embedding, MemoryNode, NodeKind, Result};
use mmdb_storage::Storage;
use smallvec::SmallVec;
use std::collections::BTreeMap;
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};
use ulid::Ulid;

pub use mmdb_core as core;
pub use mmdb_storage as storage;

pub struct Database {
    storage: Storage,
}

impl Database {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        Ok(Self { storage: Storage::open(path)? })
    }

    pub fn insert(&self, node: MemoryNode) -> Result<Ulid> {
        let id = node.id;
        self.storage.put_node(&node)?;
        Ok(id)
    }

    pub fn get(&self, tenant: u32, id: Ulid) -> Result<Option<MemoryNode>> {
        self.storage.get_node(tenant, id)
    }

    pub fn scan_by_time(
        &self,
        tenant: u32,
        from_ms: i64,
        to_ms: i64,
        limit: usize,
    ) -> Result<Vec<MemoryNode>> {
        self.storage.scan_by_time(tenant, from_ms, to_ms, limit)
    }

    pub fn delete(&self, tenant: u32, id: Ulid) -> Result<()> {
        self.storage.delete_node(tenant, id)
    }
}

pub struct NodeBuilder {
    tenant: u32,
    kind: NodeKind,
    content: Option<Content>,
    embeddings: SmallVec<[Embedding; 1]>,
    metadata: BTreeMap<String, serde_json::Value>,
    created_at_ms: Option<i64>,
}

impl NodeBuilder {
    pub fn new(tenant: u32, kind: NodeKind) -> Self {
        Self {
            tenant,
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
            tenant: self.tenant,
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
        let node = NodeBuilder::new(1, NodeKind::Episode)
            .text("hello world")
            .metadata("source", serde_json::json!("test"))
            .created_at(1000)
            .build();
        let id = db.insert(node).unwrap();

        let got = db.get(1, id).unwrap().unwrap();
        assert!(matches!(got.content, Content::Text(ref s) if s == "hello world"));

        let scanned = db.scan_by_time(1, 0, 2000, 10).unwrap();
        assert_eq!(scanned.len(), 1);

        db.delete(1, id).unwrap();
        assert!(db.get(1, id).unwrap().is_none());
    }
}
