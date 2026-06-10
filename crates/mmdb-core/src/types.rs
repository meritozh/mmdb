use serde::{Deserialize, Serialize};
use smallvec::SmallVec;
use std::collections::BTreeMap;
use ulid::Ulid;

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum NodeKind {
    Episode = 1,
    Fact = 2,
    Entity = 3,
    Artifact = 4,
}

impl NodeKind {
    pub fn as_u8(self) -> u8 {
        self as u8
    }
    pub fn from_u8(v: u8) -> Option<Self> {
        match v {
            1 => Some(Self::Episode),
            2 => Some(Self::Fact),
            3 => Some(Self::Entity),
            4 => Some(Self::Artifact),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Content {
    Text(String),
    Blob {
        hash: [u8; 32],
        size: u64,
        mime: String,
        /// For small blobs (≤ mmdb_blob::INLINE_THRESHOLD, i.e. ≤64 KB)
        /// the payload bytes can be inlined directly inside the node
        /// record so that `get_node()` immediately returns them without
        /// a separate blob-fs lookup. When `None`, bytes must be read
        /// from the blob store via `get_blob_stream(&hash)`.
        inline: Option<Vec<u8>>,
    },
    Structured(serde_json::Value),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Embedding {
    pub model: String,
    pub dim: u32,
    pub vector: SmallVec<[f32; 32]>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemoryNode {
    pub id: Ulid,
    pub tenant: u32,
    pub kind: NodeKind,
    pub created_at_ms: i64,
    pub updated_at_ms: i64,
    pub content: Content,
    pub embeddings: SmallVec<[Embedding; 1]>,
    pub metadata: BTreeMap<String, serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Edge {
    pub src: Ulid,
    pub dst: Ulid,
    pub label: String,
    pub weight: f32,
    pub created_at_ms: i64,
    pub metadata: BTreeMap<String, serde_json::Value>,
}
