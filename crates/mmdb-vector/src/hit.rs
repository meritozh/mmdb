use ulid::Ulid;

#[derive(Debug, Clone, PartialEq)]
pub struct ScoredHit {
    pub node_id: Ulid,
    /// Similarity score in [0, 1] for cosine — larger is better.
    pub score: f32,
    /// Raw HNSW distance — smaller is better.
    pub distance: f32,
}
