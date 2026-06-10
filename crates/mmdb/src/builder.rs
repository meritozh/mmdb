use crate::DEFAULT_TENANT;
use mmdb_core::{Content, Embedding, MemoryNode, NodeKind};
use smallvec::SmallVec;
use std::collections::BTreeMap;
use std::time::{SystemTime, UNIX_EPOCH};
use ulid::Ulid;

/// Fluent builder for [`MemoryNode`]. Tenant is set by [`crate::Database::insert`].
pub struct NodeBuilder {
    kind: NodeKind,
    content: Option<Content>,
    embeddings: SmallVec<[Embedding; 1]>,
    metadata: BTreeMap<String, serde_json::Value>,
    created_at_ms: Option<i64>,
}

impl NodeBuilder {
    /// Start a new builder for the given [`NodeKind`].
    pub fn new(kind: NodeKind) -> Self {
        Self {
            kind,
            content: None,
            embeddings: SmallVec::new(),
            metadata: BTreeMap::new(),
            created_at_ms: None,
        }
    }

    /// Set the node body to plain text.
    pub fn text(mut self, s: impl Into<String>) -> Self {
        self.content = Some(Content::Text(s.into()));
        self
    }

    /// Set the node body to a structured JSON value.
    pub fn structured(mut self, v: serde_json::Value) -> Self {
        self.content = Some(Content::Structured(v));
        self
    }

    /// Set the node body to a blob reference already present in the blob store.
    pub fn blob(mut self, hash: [u8; 32], size: u64, mime: impl Into<String>) -> Self {
        self.content = Some(Content::Blob {
            hash,
            size,
            mime: mime.into(),
        });
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

    /// Attach a metadata key/value pair.
    pub fn metadata(mut self, key: impl Into<String>, value: serde_json::Value) -> Self {
        self.metadata.insert(key.into(), value);
        self
    }

    /// Override the created/updated timestamps (epoch ms).
    pub fn created_at(mut self, ts_ms: i64) -> Self {
        self.created_at_ms = Some(ts_ms);
        self
    }

    /// Finalize the builder into a [`MemoryNode`]. Tenant is left at the
    /// default and gets overwritten by [`crate::Database::insert`].
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

/// Current wall-clock time in milliseconds since the UNIX epoch.
pub fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}
