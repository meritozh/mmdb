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
use mmdb_core::{Content, Edge, Embedding, MemoryNode, NodeKind, Result};
use mmdb_graph::{Direction, GraphStore};
use mmdb_storage::Storage;
use mmdb_vector::VectorStore;
use smallvec::SmallVec;
use std::collections::BTreeMap;
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};
use ulid::Ulid;

pub use mmdb_core as core;
pub use mmdb_graph as graph;
pub use mmdb_storage as storage;
pub use mmdb_vector as vector;

/// Default tenant id for single-tenant deployments.
pub const DEFAULT_TENANT: u32 = 0;

/// Default embedding model name when the user does not configure one.
pub const DEFAULT_MODEL: &str = "default";

/// Pluggable text-to-vector encoder.
///
/// Provide an implementation via [`Database::open_with_embedder`] to enable
/// auto-embedding: any `Content::Text` node inserted without an embedding
/// matching the configured default model will be embedded on the fly.
///
/// Multi-model setups can still attach explicit `Embedding` entries via
/// `NodeBuilder::embedding` — those are preserved and never overwritten.
pub trait Embedder: Send + Sync {
    /// Encode a single text into a vector. Implementations should return a
    /// vector of constant dimensionality matching `dim()`.
    fn embed(&self, text: &str) -> Result<Vec<f32>>;
    /// Model identity used as the storage key. Should match
    /// `DatabaseConfig::default_model` for the auto-embed path.
    fn model_name(&self) -> &str;
    /// Output dimensionality. Used for sanity checks.
    fn dim(&self) -> u32;
    /// Optional batch path. Default falls back to a loop over `embed`.
    fn embed_batch(&self, texts: &[&str]) -> Result<Vec<Vec<f32>>> {
        texts.iter().map(|t| self.embed(t)).collect()
    }
}


/// Top-level configuration handed to [`Database::open_with`].
#[derive(Debug, Clone)]
pub struct DatabaseConfig {
    /// Logical tenant id. Single-tenant users should leave this as [`DEFAULT_TENANT`].
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

/// High-level handle. See the crate-level docs for a quickstart.
pub struct Database {
    storage: Storage,
    vector_store: VectorStore,
    graph_store: GraphStore,
    config: DatabaseConfig,
    embedder: Option<Box<dyn Embedder>>,
}

impl Database {
    /// Open at `path` with [`DatabaseConfig::default`] (tenant=0, model="default").
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        Self::open_with(path, DatabaseConfig::default())
    }

    /// Open with an explicit config.
    pub fn open_with(path: impl AsRef<Path>, config: DatabaseConfig) -> Result<Self> {
        let storage = Storage::open(path)?;
        let vector_store = VectorStore::open(storage.keyspace.clone())?;
        let graph_store = GraphStore::open(storage.keyspace.clone())?;
        Ok(Self { storage, vector_store, graph_store, config, embedder: None })
    }

    /// Open with an explicit config AND a text embedder. Enables auto-embedding
    /// for `Content::Text` nodes that arrive without an embedding for the
    /// configured `default_model`.
    pub fn open_with_embedder(
        path: impl AsRef<Path>,
        config: DatabaseConfig,
        embedder: Box<dyn Embedder>,
    ) -> Result<Self> {
        debug_assert_eq!(
            embedder.model_name(), config.default_model,
            "embedder.model_name() should match DatabaseConfig.default_model"
        );
        let storage = Storage::open(path)?;
        let vector_store = VectorStore::open(storage.keyspace.clone())?;
        let graph_store = GraphStore::open(storage.keyspace.clone())?;
        Ok(Self { storage, vector_store, graph_store, config, embedder: Some(embedder) })
    }

    /// Returns true if an auto-embedder is wired.
    pub fn has_embedder(&self) -> bool { self.embedder.is_some() }

    /// Borrow the configuration this database was opened with.
    pub fn config(&self) -> &DatabaseConfig {
        &self.config
    }

    /// Insert a node and index every embedding it carries. If an embedder
    /// is configured and the node has no embedding under the embedder's
    /// model, the text body is encoded automatically. Returns the node id.
    pub fn insert(&self, node: MemoryNode) -> Result<Ulid> {
        let mut node = node;
        // Force-stamp the configured tenant so users cannot accidentally cross
        // boundaries via NodeBuilder.
        node.tenant = self.config.tenant;

        // Auto-embed: if an embedder is configured AND the node has no
        // embedding under the embedder's model AND its content is text, embed.
        if let Some(embedder) = self.embedder.as_ref() {
            let model = embedder.model_name();
            let already = node.embeddings.iter().any(|e| e.model == model);
            if !already {
                if let Content::Text(ref t) = node.content {
                    if !t.is_empty() {
                        let v = embedder.embed(t)?;
                        let dim = v.len() as u32;
                        debug_assert_eq!(dim, embedder.dim(),
                            "embedder produced vector of unexpected dim");
                        node.embeddings.push(Embedding {
                            model: model.to_string(),
                            dim,
                            vector: SmallVec::from_vec(v),
                        });
                    }
                }
            }
        }

        let id = node.id;
        self.storage.put_node(&node)?;
        for emb in &node.embeddings {
            self.vector_store
                .insert(self.config.tenant, &emb.model, id, &emb.vector)?;
        }
        Ok(id)
    }

    /// Convenience: insert raw text. Requires an embedder to be configured.
    pub fn insert_text(&self, kind: NodeKind, text: impl Into<String>) -> Result<Ulid> {
        if self.embedder.is_none() {
            return Err(mmdb_core::Error::InvalidArgument(
                "insert_text requires an embedder (use Database::open_with_embedder)".into()
            ));
        }
        let node = NodeBuilder::new(kind).text(text).build();
        self.insert(node)
    }

    /// Convenience: embed a query string and run vector_search.
    pub fn search_text(&self, query: &str, k: usize) -> Result<Vec<Hit>> {
        let embedder = self.embedder.as_ref().ok_or_else(|| {
            mmdb_core::Error::InvalidArgument(
                "search_text requires an embedder (use Database::open_with_embedder)".into()
            )
        })?;
        let q = embedder.embed(query)?;
        self.vector_search_with_model(embedder.model_name(), &q, k)
    }

    /// Fetch a node by id. Returns `Ok(None)` if it does not exist.
    pub fn get(&self, id: Ulid) -> Result<Option<MemoryNode>> {
        self.storage.get_node(self.config.tenant, id)
    }

    /// Time-window scan over `created_at_ms` in `[from_ms, to_ms]`, capped at `limit`.
    pub fn scan_by_time(
        &self,
        from_ms: i64,
        to_ms: i64,
        limit: usize,
    ) -> Result<Vec<MemoryNode>> {
        self.storage.scan_by_time(self.config.tenant, from_ms, to_ms, limit)
    }

    /// Hard-delete a node and all of its embeddings from every index.
    pub fn delete(&self, id: Ulid) -> Result<()> {
        if let Some(node) = self.storage.get_node(self.config.tenant, id)? {
            for emb in &node.embeddings {
                self.vector_store
                    .delete(self.config.tenant, &emb.model, id)?;
            }
        }
        self.storage.delete_node(self.config.tenant, id)
    }

    /// Vector search using the database default model.
    pub fn vector_search(&self, query: &[f32], k: usize) -> Result<Vec<Hit>> {
        let model = self.config.default_model.clone();
        self.vector_search_with_model(&model, query, k)
    }

    /// Vector search with structured post-filter (kind / time window).
    /// Returns at most `k` hits, applying the filter to over-fetched
    /// candidates internally so the result count remains useful.
    pub fn vector_search_filtered(
        &self,
        query: &[f32],
        k: usize,
        filter: VectorFilter,
    ) -> Result<Vec<Hit>> {
        let model = self.config.default_model.clone();
        let tenant = self.config.tenant;
        let storage = &self.storage;
        let f = &filter;
        let pred = move |id: Ulid| -> bool {
            // Fast path: lightweight meta (kind + ts) without full node decode.
            match storage.get_node_meta(tenant, id) {
                Ok(Some(m)) => f.matches_meta(m.kind, m.created_at_ms),
                _ => false,
            }
        };
        let scored = self
            .vector_store
            .search_with_filter(tenant, &model, query, k, Some(&pred))?;
        let mut hits = Vec::with_capacity(scored.len());
        for sh in scored {
            if let Some(node) = self.storage.get_node(tenant, sh.node_id)? {
                hits.push(Hit { node, score: sh.score });
            }
        }
        Ok(hits)
    }

    /// Vector search against an explicit model name. Use this only when you
    /// genuinely need multiple embedding spaces (e.g. CLIP + text).
    pub fn vector_search_with_model(
        &self,
        model: &str,
        query: &[f32],
        k: usize,
    ) -> Result<Vec<Hit>> {
        let scored = self
            .vector_store
            .search(self.config.tenant, model, query, k)?;
        let mut hits = Vec::with_capacity(scored.len());
        for s in scored {
            if let Some(node) = self.storage.get_node(self.config.tenant, s.node_id)? {
                hits.push(Hit { node, score: s.score });
            }
        }
        Ok(hits)
    }
    // -----------------------------------------------------------------------
    // Graph API
    // -----------------------------------------------------------------------

    /// Add an edge between two nodes. The edge is duplicated into a reverse
    /// index so [`Self::neighbours_in`] is also O(neighbours).
    pub fn add_edge(&self, edge: Edge) -> Result<()> {
        self.graph_store.add_edge(self.config.tenant, edge)
    }

    /// Remove an edge identified by `(src, dst, label)`.
    pub fn remove_edge(&self, src: Ulid, dst: Ulid, label: &str) -> Result<()> {
        self.graph_store.remove_edge(self.config.tenant, src, dst, label)
    }

    /// List outgoing edges, optionally filtered by label.
    pub fn neighbours_out(&self, node: Ulid, label: Option<&str>) -> Result<Vec<Edge>> {
        self.graph_store.neighbours_out(self.config.tenant, node, label)
    }

    /// List incoming edges, optionally filtered by label.
    pub fn neighbours_in(&self, node: Ulid, label: Option<&str>) -> Result<Vec<Edge>> {
        self.graph_store.neighbours_in(self.config.tenant, node, label)
    }

    // -----------------------------------------------------------------------
    // Hybrid (vector + graph) ranker
    // -----------------------------------------------------------------------

    /// Vector recall then BFS expansion then blended score reranking.
    ///
    /// Returns at most `opts.k` hits ordered by blended score:
    ///
    /// ```text
    /// score(n) = alpha * vector_score(n) + (1 - alpha) * neighbour_signal(n)
    /// neighbour_signal(n) = max(vector_score(seed)) over edges seed -> n
    ///                        * decay ^ hop_distance
    /// ```
    pub fn hybrid_search(&self, query: &[f32], opts: HybridOpts) -> Result<Vec<Hit>> {
        let seeds = self.vector_search(query, opts.seed_k.max(opts.k))?;
        if seeds.is_empty() { return Ok(Vec::new()); }

        let mut scores: std::collections::HashMap<Ulid, f32> = std::collections::HashMap::new();
        for h in &seeds {
            let v = opts.alpha * h.score;
            scores.entry(h.node.id).and_modify(|s| if v > *s { *s = v }).or_insert(v);
        }

        if opts.expand_hops > 0 && opts.alpha < 1.0 {
            for seed in &seeds {
                let mut frontier: Vec<(Ulid, usize)> = vec![(seed.node.id, 0)];
                let mut local_visited: std::collections::HashSet<Ulid> = std::collections::HashSet::new();
                local_visited.insert(seed.node.id);
                while let Some((node, hop)) = frontier.pop() {
                    if hop >= opts.expand_hops { continue; }
                    let edges = match opts.direction {
                        Direction::Out => self.graph_store.neighbours_out(self.config.tenant, node, opts.label.as_deref())?,
                        Direction::In  => self.graph_store.neighbours_in(self.config.tenant, node, opts.label.as_deref())?,
                        Direction::Both => {
                            let mut e = self.graph_store.neighbours_out(self.config.tenant, node, opts.label.as_deref())?;
                            e.extend(self.graph_store.neighbours_in(self.config.tenant, node, opts.label.as_deref())?);
                            e
                        }
                    };
                    for e in edges {
                        let next_id = if e.src == node { e.dst } else { e.src };
                        if !local_visited.insert(next_id) { continue; }
                        let neighbour_signal = seed.score * opts.decay.powi(hop as i32 + 1);
                        let contrib = (1.0 - opts.alpha) * neighbour_signal;
                        scores.entry(next_id).and_modify(|s| *s += contrib).or_insert(contrib);
                        frontier.push((next_id, hop + 1));
                    }
                }
            }
        }

        let mut ranked: Vec<(Ulid, f32)> = scores.into_iter().collect();
        ranked.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        ranked.truncate(opts.k);
        let mut hits = Vec::with_capacity(ranked.len());
        for (id, score) in ranked {
            if let Some(node) = self.storage.get_node(self.config.tenant, id)? {
                hits.push(Hit { node, score });
            }
        }
        Ok(hits)
    }

}

/// One ranked result of a vector search.
#[derive(Debug, Clone)]
pub struct Hit {
    /// The retrieved node.
    pub node: MemoryNode,
    /// Similarity score in `[0.0, 1.0]` (1.0 = identical).
    pub score: f32,
}

/// Post-filter for `vector_search_filtered`. All set fields are AND-ed.
#[derive(Debug, Clone, Default)]
pub struct VectorFilter {
    /// Require this kind.
    pub kind: Option<NodeKind>,
    /// Inclusive lower bound on `created_at_ms`.
    pub after_ms: Option<i64>,
    /// Inclusive upper bound on `created_at_ms`.
    pub before_ms: Option<i64>,
}

impl VectorFilter {
    /// Empty filter (matches everything).
    pub fn new() -> Self { Self::default() }
    /// Require this `NodeKind`.
    pub fn kind(mut self, k: NodeKind) -> Self { self.kind = Some(k); self }
    /// Require `created_at_ms >= t`.
    pub fn after_ms(mut self, t: i64) -> Self { self.after_ms = Some(t); self }
    /// Require `created_at_ms <= t`.
    pub fn before_ms(mut self, t: i64) -> Self { self.before_ms = Some(t); self }
    /// Test the filter against a fully-decoded node.
    pub fn matches(&self, n: &MemoryNode) -> bool {
        self.matches_meta(n.kind.as_u8(), n.created_at_ms)
    }

    /// Predicate against pre-decoded meta. Faster path used by
    /// `vector_search_filtered` when only kind+ts are needed.
    pub fn matches_meta(&self, kind_u8: u8, created_at_ms: i64) -> bool {
        if let Some(k) = self.kind { if kind_u8 != k.as_u8() { return false; } }
        if let Some(a) = self.after_ms  { if created_at_ms < a { return false; } }
        if let Some(b) = self.before_ms { if created_at_ms > b { return false; } }
        true
    }
}


/// Options for [`Database::hybrid_search`].
#[derive(Debug, Clone)]
pub struct HybridOpts {
    /// Final hit count returned to the caller.
    pub k: usize,
    /// Seed pool size pulled from pure vector search. Should be `>= k`.
    pub seed_k: usize,
    /// BFS expansion depth around each seed. `0` disables graph rerank.
    pub expand_hops: usize,
    /// Direction of edges to follow during expansion.
    pub direction: Direction,
    /// Optional edge-label filter.
    pub label: Option<String>,
    /// Score-blend coefficient. `1.0` = pure vector, `0.0` = pure graph.
    pub alpha: f32,
    /// Per-hop multiplicative decay applied to neighbour contributions.
    pub decay: f32,
}

impl Default for HybridOpts {
    fn default() -> Self {
        Self {
            k: 10,
            seed_k: 20,
            expand_hops: 1,
            direction: Direction::Both,
            label: None,
            alpha: 0.7,
            decay: 0.5,
        }
    }
}

/// Fluent builder for [`MemoryNode`]. Tenant is set by [`Database::insert`].
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
    /// default and gets overwritten by [`Database::insert`].
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

        // No nodes inserted -> empty result
        let hits = db.vector_search(&[0.1, 0.2, 0.3], 5).unwrap();
        assert!(hits.is_empty());
    }

    fn norm(v: Vec<f32>) -> Vec<f32> {
        let n: f32 = v.iter().map(|x| x * x).sum::<f32>().sqrt();
        v.into_iter().map(|x| x / n).collect()
    }

    #[test]
    fn vector_search_returns_inserted_nodes_ranked() {
        let dir = tempdir().unwrap();
        let db = Database::open(dir.path()).unwrap();
        let mk = |v: Vec<f32>, label: &str| {
            NodeBuilder::new(NodeKind::Fact)
                .text(label)
                .embedding(DEFAULT_MODEL, norm(v))
                .build()
        };
        let n1 = mk(vec![1.0, 0.0, 0.0, 0.0], "axis-x");
        let n2 = mk(vec![0.0, 1.0, 0.0, 0.0], "axis-y");
        let n3 = mk(vec![0.95, 0.05, 0.0, 0.0], "near-x");
        let id1 = db.insert(n1).unwrap();
        let _id2 = db.insert(n2).unwrap();
        let id3 = db.insert(n3).unwrap();

        let q = norm(vec![1.0, 0.0, 0.0, 0.0]);
        let hits = db.vector_search(&q, 2).unwrap();
        assert_eq!(hits.len(), 2, "got {:?}", hits.iter().map(|h| &h.node.id).collect::<Vec<_>>());
        assert_eq!(hits[0].node.id, id1);
        assert_eq!(hits[1].node.id, id3);
        assert!(hits[0].score >= hits[1].score);
    }

    #[test]
    fn vector_search_filtered_by_kind_and_time() {
        let dir = tempdir().unwrap();
        let db = Database::open(dir.path()).unwrap();
        let v = norm(vec![1.0, 0.0, 0.0, 0.0]);
        let fact_id = db.insert(
            NodeBuilder::new(NodeKind::Fact)
                .text("fact").created_at(1_000).embedding(DEFAULT_MODEL, v.clone()).build()
        ).unwrap();
        let ep_id = db.insert(
            NodeBuilder::new(NodeKind::Episode)
                .text("episode").created_at(2_000).embedding(DEFAULT_MODEL, v.clone()).build()
        ).unwrap();
        // kind filter — only Fact survives
        let hits = db.vector_search_filtered(&v, 5, VectorFilter::new().kind(NodeKind::Fact)).unwrap();
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].node.id, fact_id);
        // time-window — only Episode survives
        let hits = db.vector_search_filtered(&v, 5, VectorFilter::new().after_ms(1_500)).unwrap();
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].node.id, ep_id);
        // both — empty
        let hits = db.vector_search_filtered(
            &v, 5,
            VectorFilter::new().kind(NodeKind::Fact).after_ms(1_500),
        ).unwrap();
        assert!(hits.is_empty());
    }

    #[test]
    fn delete_removes_from_vector_search() {
        let dir = tempdir().unwrap();
        let db = Database::open(dir.path()).unwrap();
        let node = NodeBuilder::new(NodeKind::Fact)
            .text("x")
            .embedding(DEFAULT_MODEL, norm(vec![1.0, 0.0, 0.0]))
            .build();
        let id = db.insert(node).unwrap();
        let q = norm(vec![1.0, 0.0, 0.0]);
        assert_eq!(db.vector_search(&q, 5).unwrap().len(), 1);
        db.delete(id).unwrap();
        assert_eq!(db.vector_search(&q, 5).unwrap().len(), 0);
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


    /// Toy embedder: tokenize on whitespace + FNV1a hash into a fixed-dim bucket.
    /// Deterministic & content-discriminating enough for unit tests.
    struct HashEmbedder { dim: u32, name: String }
    impl HashEmbedder {
        fn new(name: &str, dim: u32) -> Self { Self { dim, name: name.to_string() } }
        fn fnv1a(s: &str) -> u32 {
            let mut h: u32 = 0x811c9dc5;
            for b in s.as_bytes() { h ^= *b as u32; h = h.wrapping_mul(0x01000193); }
            h
        }
    }
    impl Embedder for HashEmbedder {
        fn embed(&self, text: &str) -> Result<Vec<f32>> {
            let mut v = vec![0.0f32; self.dim as usize];
            for tok in text.split_whitespace() {
                let h = Self::fnv1a(tok) as usize;
                v[h % self.dim as usize] += 1.0;
            }
            // L2 normalize so cosine works.
            let n: f32 = v.iter().map(|x| x*x).sum::<f32>().sqrt();
            if n > 0.0 { for x in v.iter_mut() { *x /= n; } }
            Ok(v)
        }
        fn model_name(&self) -> &str { &self.name }
        fn dim(&self) -> u32 { self.dim }
    }

    #[test]
    fn auto_embeds_text_on_insert() {
        let dir = tempdir().unwrap();
        let cfg = DatabaseConfig { tenant: DEFAULT_TENANT, default_model: "hash-32".into() };
        let db = Database::open_with_embedder(
            dir.path(), cfg, Box::new(HashEmbedder::new("hash-32", 32)),
        ).unwrap();
        assert!(db.has_embedder());

        let id = db.insert_text(NodeKind::Fact, "the quick brown fox").unwrap();
        let got = db.get(id).unwrap().unwrap();
        assert_eq!(got.embeddings.len(), 1);
        assert_eq!(got.embeddings[0].model, "hash-32");
        assert_eq!(got.embeddings[0].dim, 32);

        // search_text should round-trip the same string back as the top hit.
        let hits = db.search_text("the quick brown fox", 3).unwrap();
        assert!(!hits.is_empty());
        assert_eq!(hits[0].node.id, id);
    }

    #[test]
    fn explicit_embedding_overrides_auto() {
        let dir = tempdir().unwrap();
        let cfg = DatabaseConfig { tenant: DEFAULT_TENANT, default_model: "hash-32".into() };
        let db = Database::open_with_embedder(
            dir.path(), cfg, Box::new(HashEmbedder::new("hash-32", 32)),
        ).unwrap();
        // Pre-attach an embedding under the embedder's model -> auto-embed skipped.
        let mut v = vec![0.0f32; 32]; v[0] = 1.0;
        let node = NodeBuilder::new(NodeKind::Fact)
            .text("ignored for embedding purposes")
            .embedding("hash-32", v.clone())
            .build();
        let id = db.insert(node).unwrap();
        let got = db.get(id).unwrap().unwrap();
        assert_eq!(got.embeddings.len(), 1);
        assert_eq!(got.embeddings[0].vector.as_slice(), v.as_slice());
    }

    #[test]
    fn insert_text_without_embedder_errors() {
        let dir = tempdir().unwrap();
        let db = Database::open(dir.path()).unwrap();
        let err = db.insert_text(NodeKind::Fact, "x").unwrap_err();
        assert!(matches!(err, mmdb_core::Error::InvalidArgument(_)));
    }

    #[test]
    fn hybrid_search_promotes_neighbour_via_graph() {
        let dir = tempdir().unwrap();
        let db = Database::open(dir.path()).unwrap();

        // Three facts: query is closest to A; B is mid; C is far.
        let a = db.insert(
            NodeBuilder::new(NodeKind::Fact).text("A")
                .embedding(DEFAULT_MODEL, norm(vec![1.0, 0.0, 0.0, 0.0])).build()
        ).unwrap();
        let b = db.insert(
            NodeBuilder::new(NodeKind::Fact).text("B")
                .embedding(DEFAULT_MODEL, norm(vec![0.6, 0.8, 0.0, 0.0])).build()
        ).unwrap();
        let c = db.insert(
            NodeBuilder::new(NodeKind::Fact).text("C")
                .embedding(DEFAULT_MODEL, norm(vec![0.0, 0.0, 1.0, 0.0])).build()
        ).unwrap();

        // Wire C as a related neighbour of A.
        use mmdb_core::Edge;
        use std::collections::BTreeMap;
        db.add_edge(Edge {
            src: a, dst: c, label: "related".into(),
            weight: 1.0, created_at_ms: 0, metadata: BTreeMap::new(),
        }).unwrap();

        let q = norm(vec![1.0, 0.0, 0.0, 0.0]);

        // Pure vector: C is ranked below B because it's orthogonal to the query.
        let pure = db.vector_search(&q, 3).unwrap();
        let pure_order: Vec<_> = pure.iter().map(|h| h.node.id).collect();
        assert_eq!(pure_order[0], a);
        // B should beat C in pure vector ranking.
        assert!(pure_order.iter().position(|x| *x == b) < pure_order.iter().position(|x| *x == c));

        // Hybrid: C gets a neighbour bump from A and may rank above B.
        let opts = HybridOpts {
            k: 3, seed_k: 5, expand_hops: 1,
            direction: graph::Direction::Out,
            label: Some("related".into()),
            alpha: 0.3, decay: 1.0,
        };
        let hyb = db.hybrid_search(&q, opts).unwrap();
        let pos_b = hyb.iter().position(|h| h.node.id == b);
        let pos_c = hyb.iter().position(|h| h.node.id == c);
        assert!(pos_c.is_some(), "C must appear in hybrid result");
        // With alpha=0.3 and decay=1.0, C inherits 0.7 * a.score which dominates B.
        assert!(pos_c < pos_b || pos_b.is_none(), "C should be promoted above B; got order {:?}",
                hyb.iter().map(|h| (h.node.id, h.score)).collect::<Vec<_>>());
    }

    #[test]
    fn hybrid_search_alpha_one_equals_vector_only() {
        let dir = tempdir().unwrap();
        let db = Database::open(dir.path()).unwrap();
        let v = norm(vec![1.0, 0.0, 0.0, 0.0]);
        let id = db.insert(
            NodeBuilder::new(NodeKind::Fact).text("x")
                .embedding(DEFAULT_MODEL, v.clone()).build()
        ).unwrap();
        let opts = HybridOpts { alpha: 1.0, expand_hops: 0, ..Default::default() };
        let hits = db.hybrid_search(&v, opts).unwrap();
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].node.id, id);
    }

}
