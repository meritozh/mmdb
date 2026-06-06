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
use mmdb_catalog::Catalog;
use mmdb_core::{Content, Edge, Embedding, MemoryNode, NodeKind, Result};
use mmdb_graph::{Direction, GraphStore};
use mmdb_storage::Storage;
use mmdb_vector::VectorStore;
use smallvec::SmallVec;
use std::collections::{BTreeMap, HashSet};
use std::future::Future;
use std::io::Read;
use std::path::Path;
use std::pin::Pin;
use std::sync::{Arc, Mutex, RwLock};
use std::task::{Context, Poll, Waker};
use std::time::{SystemTime, UNIX_EPOCH};
use ulid::Ulid;

pub use mmdb_blob as blob;
pub use mmdb_catalog as catalog;
pub use mmdb_core as core;
pub use mmdb_graph as graph;
pub use mmdb_query as query;
pub use mmdb_storage as storage;
pub use mmdb_vector as vector;

/// Default tenant id for single-tenant deployments.
pub const DEFAULT_TENANT: u32 = 0;

/// Default embedding model name when the user does not configure one.
pub const DEFAULT_MODEL: &str = "default";
const QUERY_BATCH_SIZE: usize = 1024;

pub type EmbedFuture<'a> = Pin<Box<dyn Future<Output = Result<Vec<f32>>> + Send + 'a>>;
pub type EmbedBatchFuture<'a> = Pin<Box<dyn Future<Output = Result<Vec<Vec<f32>>>> + Send + 'a>>;
type QueryUdfFn = mmdb_query::UdfFn;

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
    /// Optional async path for remote embedding providers.
    ///
    /// The default delegates to the synchronous method, so existing embedders
    /// keep working. Remote implementations can override this to perform real
    /// async I/O without blocking the caller's executor.
    fn embed_async<'a>(&'a self, text: &'a str) -> EmbedFuture<'a> {
        Box::pin(async move { self.embed(text) })
    }
    /// Optional async batch path. Default awaits `embed_async` in order.
    fn embed_batch_async<'a>(&'a self, texts: &'a [&'a str]) -> EmbedBatchFuture<'a> {
        Box::pin(async move {
            let mut out = Vec::with_capacity(texts.len());
            for text in texts {
                out.push(self.embed_async(text).await?);
            }
            Ok(out)
        })
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
    storage: Arc<Storage>,
    vector_store: Arc<VectorStore>,
    graph_store: Arc<GraphStore>,
    blob_store: Arc<blob::BlobStore>,
    catalog: Arc<Catalog>,
    query_udfs: RwLock<BTreeMap<String, Arc<QueryUdfFn>>>,
    config: DatabaseConfig,
    embedder: Option<Arc<dyn Embedder>>,
}

impl Database {
    /// Open at `path` with [`DatabaseConfig::default`] (tenant=0, model="default").
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        Self::open_with(path, DatabaseConfig::default())
    }

    /// Open with an explicit config.
    pub fn open_with(path: impl AsRef<Path>, config: DatabaseConfig) -> Result<Self> {
        let path = path.as_ref();
        let storage = Arc::new(Storage::open(path)?);
        let vector_store = Arc::new(VectorStore::open(storage.keyspace.clone())?);
        let graph_store = Arc::new(GraphStore::open(storage.keyspace.clone())?);
        let blob_store = Arc::new(blob::BlobStore::open(path)?);
        let catalog = rebuild_catalog(&storage, config.tenant)?;
        Ok(Self {
            storage,
            vector_store,
            graph_store,
            blob_store,
            catalog: Arc::new(catalog),
            query_udfs: RwLock::new(BTreeMap::new()),
            config,
            embedder: None,
        })
    }

    /// Open with an explicit config AND a text embedder. Enables auto-embedding
    /// for `Content::Text` nodes that arrive without an embedding for the
    /// configured `default_model`.
    pub fn open_with_embedder(
        path: impl AsRef<Path>,
        config: DatabaseConfig,
        embedder: Box<dyn Embedder>,
    ) -> Result<Self> {
        if embedder.model_name() != config.default_model {
            return Err(mmdb_core::Error::InvalidArgument(format!(
                "embedder model `{}` does not match DatabaseConfig.default_model `{}`",
                embedder.model_name(),
                config.default_model
            )));
        }
        let path = path.as_ref();
        let storage = Arc::new(Storage::open(path)?);
        let vector_store = Arc::new(VectorStore::open(storage.keyspace.clone())?);
        let graph_store = Arc::new(GraphStore::open(storage.keyspace.clone())?);
        let blob_store = Arc::new(blob::BlobStore::open(path)?);
        let catalog = rebuild_catalog(&storage, config.tenant)?;
        Ok(Self {
            storage,
            vector_store,
            graph_store,
            blob_store,
            catalog: Arc::new(catalog),
            query_udfs: RwLock::new(BTreeMap::new()),
            config,
            embedder: Some(Arc::from(embedder)),
        })
    }

    /// Returns true if an auto-embedder is wired.
    pub fn has_embedder(&self) -> bool {
        self.embedder.is_some()
    }

    /// Borrow the configuration this database was opened with.
    pub fn config(&self) -> &DatabaseConfig {
        &self.config
    }

    /// Register a facade-local query UDF used by [`Self::execute_query`].
    ///
    /// This closure binding is intentionally lightweight; `mmdb-udf` remains
    /// the Wasmtime-backed sandbox/runtime layer.
    pub fn register_query_udf(
        &self,
        name: impl Into<String>,
        udf: impl Fn(&mmdb_query::Record, &[mmdb_query::Expr]) -> f32 + Send + Sync + 'static,
    ) {
        if let Ok(mut udfs) = self.query_udfs.write() {
            udfs.insert(name.into(), Arc::new(udf));
        }
    }

    /// Insert a node and index every embedding it carries. If an embedder
    /// is configured and the node has no embedding under the embedder's
    /// model, the text body is encoded automatically. Returns the node id.
    pub fn insert(&self, node: MemoryNode) -> Result<Ulid> {
        self.insert_inner(node, false)
    }

    fn insert_inner(&self, node: MemoryNode, blob_ref_already_acquired: bool) -> Result<Ulid> {
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
                        debug_assert_eq!(
                            dim,
                            embedder.dim(),
                            "embedder produced vector of unexpected dim"
                        );
                        node.embeddings.push(Embedding {
                            model: model.to_string(),
                            dim,
                            vector: SmallVec::from_vec(v),
                        });
                    }
                }
            }
        }

        self.validate_node_embeddings(&node)?;

        let id = node.id;
        let previous = self.storage.get_node(self.config.tenant, id)?;
        let previous_blob_hash = previous.as_ref().and_then(|node| blob_hash(&node.content));
        let next_blob_hash = blob_hash(&node.content);
        let acquired_next_blob =
            if !blob_ref_already_acquired && next_blob_hash != previous_blob_hash {
                if let Some(hash) = next_blob_hash {
                    self.blob_store.inc_ref(&hash)?;
                    true
                } else {
                    false
                }
            } else {
                false
            };

        if let Err(err) = self.storage.put_node(&node) {
            if acquired_next_blob {
                if let Some(hash) = next_blob_hash {
                    let _ = self.blob_store.dec_ref(&hash);
                }
            }
            return Err(err);
        }
        if let Some(previous) = previous {
            for emb in &previous.embeddings {
                self.vector_store
                    .delete(self.config.tenant, &emb.model, id)?;
            }
            self.release_previous_blob_if_replaced(&previous.content, &node.content)?;
            if previous.kind != node.kind {
                self.catalog
                    .record_node_delete(self.config.tenant, previous.kind);
                self.catalog
                    .record_node_insert(self.config.tenant, node.kind);
            }
        } else {
            self.catalog
                .record_node_insert(self.config.tenant, node.kind);
        }
        for emb in &node.embeddings {
            self.vector_store
                .insert(self.config.tenant, &emb.model, id, &emb.vector)?;
        }
        Ok(id)
    }

    fn validate_node_embeddings(&self, node: &MemoryNode) -> Result<()> {
        let mut seen_models = BTreeMap::new();
        for emb in &node.embeddings {
            if emb.dim as usize != emb.vector.len() {
                return Err(mmdb_core::Error::InvalidArgument(format!(
                    "embedding `{}` declares dim {}, got {} values",
                    emb.model,
                    emb.dim,
                    emb.vector.len()
                )));
            }
            if seen_models.insert(emb.model.clone(), emb.dim).is_some() {
                return Err(mmdb_core::Error::InvalidArgument(format!(
                    "duplicate embedding model `{}` on node {}",
                    emb.model, node.id
                )));
            }
            self.vector_store.validate_insert(
                self.config.tenant,
                &emb.model,
                emb.vector.as_slice(),
            )?;
        }
        Ok(())
    }

    /// Convenience: insert raw text. Requires an embedder to be configured.
    pub fn insert_text(&self, kind: NodeKind, text: impl Into<String>) -> Result<Ulid> {
        if self.embedder.is_none() {
            return Err(mmdb_core::Error::InvalidArgument(
                "insert_text requires an embedder (use Database::open_with_embedder)".into(),
            ));
        }
        let node = NodeBuilder::new(kind).text(text).build();
        self.insert(node)
    }

    /// Convenience: embed a query string and run vector_search.
    pub fn search_text(&self, query: &str, k: usize) -> Result<Vec<Hit>> {
        let embedder = self.embedder.as_ref().ok_or_else(|| {
            mmdb_core::Error::InvalidArgument(
                "search_text requires an embedder (use Database::open_with_embedder)".into(),
            )
        })?;
        let q = embedder.embed(query)?;
        self.vector_search_with_model(embedder.model_name(), &q, k)
    }

    /// Async variant of [`Self::insert_text`] for remote embedding providers.
    pub async fn insert_text_async(&self, kind: NodeKind, text: impl Into<String>) -> Result<Ulid> {
        let embedder = self.embedder.as_ref().ok_or_else(|| {
            mmdb_core::Error::InvalidArgument(
                "insert_text_async requires an embedder (use Database::open_with_embedder)".into(),
            )
        })?;
        let text = text.into();
        let vector = embedder.embed_async(&text).await?;
        debug_assert_eq!(
            vector.len() as u32,
            embedder.dim(),
            "embedder produced vector of unexpected dim"
        );
        let node = NodeBuilder::new(kind)
            .text(text)
            .embedding(embedder.model_name(), vector)
            .build();
        self.insert(node)
    }

    /// Async variant of [`Self::search_text`] for remote embedding providers.
    pub async fn search_text_async(&self, query: &str, k: usize) -> Result<Vec<Hit>> {
        let embedder = self.embedder.as_ref().ok_or_else(|| {
            mmdb_core::Error::InvalidArgument(
                "search_text_async requires an embedder (use Database::open_with_embedder)".into(),
            )
        })?;
        let q = embedder.embed_async(query).await?;
        self.vector_search_with_model(embedder.model_name(), &q, k)
    }

    /// Fetch a node by id. Returns `Ok(None)` if it does not exist.
    pub fn get(&self, id: Ulid) -> Result<Option<MemoryNode>> {
        self.storage.get_node(self.config.tenant, id)
    }

    /// Time-window scan over `created_at_ms` in `[from_ms, to_ms]`, capped at `limit`.
    pub fn scan_by_time(&self, from_ms: i64, to_ms: i64, limit: usize) -> Result<Vec<MemoryNode>> {
        self.storage
            .scan_by_time(self.config.tenant, from_ms, to_ms, limit)
    }

    /// Hard-delete a node and all of its embeddings from every index.
    pub fn delete(&self, id: Ulid) -> Result<()> {
        if let Some(node) = self.storage.get_node(self.config.tenant, id)? {
            for emb in &node.embeddings {
                self.vector_store
                    .delete(self.config.tenant, &emb.model, id)?;
            }
            self.release_blob_ref(&node.content)?;
            self.storage.delete_node(self.config.tenant, id)?;
            self.catalog
                .record_node_delete(self.config.tenant, node.kind);
            Ok(())
        } else {
            self.storage.delete_node(self.config.tenant, id)
        }
    }

    /// Store bytes in the content-addressed blob store and insert a blob-backed node.
    pub fn insert_blob(
        &self,
        kind: NodeKind,
        reader: impl Read,
        mime: impl Into<String>,
    ) -> Result<Ulid> {
        let blob_ref = self.blob_store.put_stream(reader)?;
        let node = NodeBuilder::new(kind)
            .blob(blob_ref.hash, blob_ref.size, mime)
            .build();
        match self.insert_inner(node, true) {
            Ok(id) => Ok(id),
            Err(err) => {
                let _ = self.blob_store.dec_ref(&blob_ref.hash);
                Err(err)
            }
        }
    }

    /// Read blob bytes by content hash.
    pub fn get_blob_stream(&self, hash: &[u8; 32]) -> Result<Box<dyn Read + Send>> {
        self.blob_store.get_stream(hash)
    }

    /// Return the persisted blob refcount, if present.
    pub fn blob_refcount(&self, hash: &[u8; 32]) -> Result<Option<u64>> {
        self.blob_store.refcount(hash)
    }

    /// Physically remove blobs whose refcount reached zero.
    pub fn gc_blobs(&self) -> Result<usize> {
        self.blob_store.gc()
    }

    fn release_previous_blob_if_replaced(&self, previous: &Content, next: &Content) -> Result<()> {
        let previous_hash = blob_hash(previous);
        if previous_hash.is_some() && previous_hash != blob_hash(next) {
            self.release_blob_ref(previous)?;
        }
        Ok(())
    }

    fn release_blob_ref(&self, content: &Content) -> Result<()> {
        if let Content::Blob { hash, .. } = content {
            match self.blob_store.dec_ref(hash) {
                Ok(()) | Err(mmdb_core::Error::NotFound) => {}
                Err(err) => return Err(err),
            }
        }
        Ok(())
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
        let allowed_by_metadata = if filter.metadata.is_empty() {
            None
        } else {
            Some(metadata_candidate_set(storage, tenant, &filter.metadata)?)
        };
        let f = &filter;
        let pred = move |id: Ulid| -> bool {
            if let Some(allowed) = &allowed_by_metadata {
                if !allowed.contains(&id) {
                    return false;
                }
            }
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
                hits.push(Hit {
                    node,
                    score: sh.score,
                });
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
                hits.push(Hit {
                    node,
                    score: s.score,
                });
            }
        }
        Ok(hits)
    }

    /// Execute a [`mmdb_query::LogicalPlan`] against this database's real
    /// storage/vector/graph stores and return lightweight query records.
    ///
    /// This is intentionally a facade-level bridge: `mmdb-query` stays free of
    /// storage dependencies, while the user-facing `mmdb` crate can bind IR
    /// leaves to persisted sources.
    pub fn execute_query(&self, plan: &mmdb_query::LogicalPlan) -> Result<Vec<mmdb_query::Record>> {
        self.execute_query_physical(plan)
    }

    /// Execute a [`mmdb_query::LogicalPlan`] through the source-backed physical
    /// executor, including facade-local UDFs registered with
    /// [`Self::register_query_udf`].
    pub fn execute_query_physical(
        &self,
        plan: &mmdb_query::LogicalPlan,
    ) -> Result<Vec<mmdb_query::Record>> {
        let executor = self.source_executor_with_udfs()?;
        let mut op = executor.compile(plan, QUERY_BATCH_SIZE)?;
        collect_query_operator(&mut *op)
    }

    fn source_executor_with_udfs(&self) -> Result<mmdb_query::SourceExecutor<'_, Self>> {
        let udfs = self.query_udfs_snapshot()?;
        let mut executor = mmdb_query::SourceExecutor::new(self);
        for (name, udf) in udfs.iter() {
            executor = executor.with_udf(name.clone(), udf.clone());
        }
        Ok(executor)
    }

    fn query_udfs_snapshot(&self) -> Result<BTreeMap<String, Arc<QueryUdfFn>>> {
        self.query_udfs
            .read()
            .map(|udfs| {
                udfs.iter()
                    .map(|(name, udf)| (name.clone(), udf.clone()))
                    .collect()
            })
            .map_err(|_| mmdb_core::Error::Storage("query UDF registry read lock poisoned".into()))
    }

    /// Return optimizer stats derived from the facade catalog for the current
    /// tenant. Reopen rebuilds this catalog from persisted nodes, so callers can
    /// feed stats into `mmdb-query` planning without rescanning at plan time.
    pub fn query_optimizer_stats(&self) -> mmdb_query::Stats {
        query_stats_from_catalog(self.catalog.tenant_stats(self.config.tenant))
    }

    /// Async counterpart to [`Self::execute_query`].
    ///
    /// The storage/vector/graph crates expose synchronous APIs, so this future
    /// offloads source-backed physical execution to a worker thread. Polling the
    /// future starts the worker and returns `Pending`; the worker wakes the
    /// caller when the real store execution path finishes.
    pub fn execute_query_async(
        &self,
        plan: &mmdb_query::LogicalPlan,
    ) -> impl Future<Output = Result<Vec<mmdb_query::Record>>> + Send + 'static {
        AsyncQueryFuture::new(AsyncQueryRequest {
            source: QuerySourceHandle {
                storage: self.storage.clone(),
                vector_store: self.vector_store.clone(),
                graph_store: self.graph_store.clone(),
                embedder: self.embedder.clone(),
                config: self.config.clone(),
            },
            udfs: self.query_udfs_snapshot(),
            plan: plan.clone(),
        })
    }

    fn graph_expand_query_rows(
        &self,
        seeds: Vec<mmdb_query::Record>,
        relation: Option<&str>,
        depth: u8,
    ) -> Result<Vec<mmdb_query::Record>> {
        graph_expand_query_rows_from(
            &self.storage,
            &self.graph_store,
            self.config.tenant,
            seeds,
            relation,
            depth,
        )
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
        self.graph_store
            .remove_edge(self.config.tenant, src, dst, label)
    }

    /// List outgoing edges, optionally filtered by label.
    pub fn neighbours_out(&self, node: Ulid, label: Option<&str>) -> Result<Vec<Edge>> {
        self.graph_store
            .neighbours_out(self.config.tenant, node, label)
    }

    /// List incoming edges, optionally filtered by label.
    pub fn neighbours_in(&self, node: Ulid, label: Option<&str>) -> Result<Vec<Edge>> {
        self.graph_store
            .neighbours_in(self.config.tenant, node, label)
    }

    /// Enumerate graph edge labels observed for this database tenant.
    pub fn edge_labels(&self) -> Result<Vec<String>> {
        self.graph_store.labels(self.config.tenant)
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
        if seeds.is_empty() {
            return Ok(Vec::new());
        }

        let mut scores: std::collections::HashMap<Ulid, f32> = std::collections::HashMap::new();
        for h in &seeds {
            let v = opts.alpha * h.score;
            scores
                .entry(h.node.id)
                .and_modify(|s| {
                    if v > *s {
                        *s = v
                    }
                })
                .or_insert(v);
        }

        if opts.expand_hops > 0 && opts.alpha < 1.0 {
            for seed in &seeds {
                let mut frontier: Vec<(Ulid, usize)> = vec![(seed.node.id, 0)];
                let mut local_visited: std::collections::HashSet<Ulid> =
                    std::collections::HashSet::new();
                local_visited.insert(seed.node.id);
                while let Some((node, hop)) = frontier.pop() {
                    if hop >= opts.expand_hops {
                        continue;
                    }
                    let edges = match opts.direction {
                        Direction::Out => self.graph_store.neighbours_out(
                            self.config.tenant,
                            node,
                            opts.label.as_deref(),
                        )?,
                        Direction::In => self.graph_store.neighbours_in(
                            self.config.tenant,
                            node,
                            opts.label.as_deref(),
                        )?,
                        Direction::Both => {
                            let mut e = self.graph_store.neighbours_out(
                                self.config.tenant,
                                node,
                                opts.label.as_deref(),
                            )?;
                            e.extend(self.graph_store.neighbours_in(
                                self.config.tenant,
                                node,
                                opts.label.as_deref(),
                            )?);
                            e
                        }
                    };
                    for e in edges {
                        let next_id = if e.src == node { e.dst } else { e.src };
                        if !local_visited.insert(next_id) {
                            continue;
                        }
                        let neighbour_signal = seed.score * opts.decay.powi(hop as i32 + 1);
                        let contrib = (1.0 - opts.alpha) * neighbour_signal;
                        scores
                            .entry(next_id)
                            .and_modify(|s| *s += contrib)
                            .or_insert(contrib);
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

#[derive(Clone)]
struct QuerySourceHandle {
    storage: Arc<Storage>,
    vector_store: Arc<VectorStore>,
    graph_store: Arc<GraphStore>,
    embedder: Option<Arc<dyn Embedder>>,
    config: DatabaseConfig,
}

struct AsyncQueryRequest {
    source: QuerySourceHandle,
    udfs: Result<BTreeMap<String, Arc<QueryUdfFn>>>,
    plan: mmdb_query::LogicalPlan,
}

struct AsyncQueryFuture {
    request: Option<AsyncQueryRequest>,
    state: Arc<Mutex<AsyncQueryState>>,
}

struct AsyncQueryState {
    result: Option<Result<Vec<mmdb_query::Record>>>,
    waker: Option<Waker>,
}

impl AsyncQueryFuture {
    fn new(request: AsyncQueryRequest) -> Self {
        Self {
            request: Some(request),
            state: Arc::new(Mutex::new(AsyncQueryState {
                result: None,
                waker: None,
            })),
        }
    }
}

impl Future for AsyncQueryFuture {
    type Output = Result<Vec<mmdb_query::Record>>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        {
            let mut state = self.state.lock().expect("async query state mutex poisoned");
            if let Some(result) = state.result.take() {
                return Poll::Ready(result);
            }
            state.waker = Some(cx.waker().clone());
        }

        if let Some(request) = self.request.take() {
            let state = self.state.clone();
            std::thread::spawn(move || {
                let result = execute_async_query_request(request);
                let waker = {
                    let mut state = state.lock().expect("async query state mutex poisoned");
                    state.result = Some(result);
                    state.waker.take()
                };
                if let Some(waker) = waker {
                    waker.wake();
                }
            });
        }

        Poll::Pending
    }
}

fn execute_async_query_request(request: AsyncQueryRequest) -> Result<Vec<mmdb_query::Record>> {
    let udfs = request.udfs?;
    let mut executor = mmdb_query::SourceExecutor::new(&request.source);
    for (name, udf) in udfs {
        executor = executor.with_udf(name, udf);
    }
    let mut op = executor.compile(&request.plan, QUERY_BATCH_SIZE)?;
    collect_query_operator(&mut *op)
}

impl mmdb_query::QuerySource for QuerySourceHandle {
    fn range_scan(
        &self,
        table: &mmdb_query::TableId,
        filter: Option<&mmdb_query::Predicate>,
    ) -> Result<Vec<mmdb_query::Record>> {
        if table != &mmdb_query::TableId::Nodes {
            return Err(mmdb_core::Error::InvalidArgument(format!(
                "database query source does not support scanning {table:?}"
            )));
        }
        Ok(self
            .storage
            .scan_by_time(self.config.tenant, 0, i64::MAX, usize::MAX)?
            .into_iter()
            .map(node_to_query_record)
            .filter(|record| {
                filter
                    .map(|pred| query_predicate_matches(record, pred))
                    .unwrap_or(true)
            })
            .collect())
    }

    fn hnsw_search(
        &self,
        query: &mmdb_query::VectorRef,
        model: &mmdb_query::ModelId,
        k: usize,
        filter: Option<&mmdb_query::Predicate>,
    ) -> Result<Vec<mmdb_query::Record>> {
        let vector = resolve_query_vector(query, model, self.embedder.as_deref())?;
        let hits = self
            .vector_store
            .search(self.config.tenant, &model.0, &vector, k)?;
        let mut rows = Vec::with_capacity(hits.len());
        for hit in hits {
            let Some(node) = self.storage.get_node(self.config.tenant, hit.node_id)? else {
                continue;
            };
            let record = node_to_query_record(node).with_score(hit.score);
            if filter
                .map(|pred| query_predicate_matches(&record, pred))
                .unwrap_or(true)
            {
                rows.push(record);
            }
        }
        Ok(rows)
    }

    fn graph_expand(
        &self,
        seeds: Vec<mmdb_query::Record>,
        relation: Option<&str>,
        depth: u8,
    ) -> Result<Vec<mmdb_query::Record>> {
        graph_expand_query_rows_from(
            &self.storage,
            &self.graph_store,
            self.config.tenant,
            seeds,
            relation,
            depth,
        )
    }
}

impl mmdb_query::QuerySource for Database {
    fn range_scan(
        &self,
        table: &mmdb_query::TableId,
        filter: Option<&mmdb_query::Predicate>,
    ) -> Result<Vec<mmdb_query::Record>> {
        if table != &mmdb_query::TableId::Nodes {
            return Err(mmdb_core::Error::InvalidArgument(format!(
                "database query source does not support scanning {table:?}"
            )));
        }
        Ok(self
            .storage
            .scan_by_time(self.config.tenant, 0, i64::MAX, usize::MAX)?
            .into_iter()
            .map(node_to_query_record)
            .filter(|record| {
                filter
                    .map(|pred| query_predicate_matches(record, pred))
                    .unwrap_or(true)
            })
            .collect())
    }

    fn hnsw_search(
        &self,
        query: &mmdb_query::VectorRef,
        model: &mmdb_query::ModelId,
        k: usize,
        filter: Option<&mmdb_query::Predicate>,
    ) -> Result<Vec<mmdb_query::Record>> {
        let vector = resolve_query_vector(query, model, self.embedder.as_deref())?;
        let hits = self
            .vector_store
            .search(self.config.tenant, &model.0, &vector, k)?;
        let mut rows = Vec::with_capacity(hits.len());
        for hit in hits {
            let Some(node) = self.storage.get_node(self.config.tenant, hit.node_id)? else {
                continue;
            };
            let record = node_to_query_record(node).with_score(hit.score);
            if filter
                .map(|pred| query_predicate_matches(&record, pred))
                .unwrap_or(true)
            {
                rows.push(record);
            }
        }
        Ok(rows)
    }

    fn graph_expand(
        &self,
        seeds: Vec<mmdb_query::Record>,
        relation: Option<&str>,
        depth: u8,
    ) -> Result<Vec<mmdb_query::Record>> {
        self.graph_expand_query_rows(seeds, relation, depth)
    }
}

fn graph_expand_query_rows_from(
    storage: &Storage,
    graph_store: &GraphStore,
    tenant: u32,
    seeds: Vec<mmdb_query::Record>,
    relation: Option<&str>,
    depth: u8,
) -> Result<Vec<mmdb_query::Record>> {
    if depth == 0 {
        return Ok(seeds);
    }

    let mut out = Vec::new();
    let mut emitted = HashSet::new();
    for seed in seeds {
        if emitted.insert(seed.node_id.clone()) {
            out.push(seed.clone());
        }
        let Ok(seed_id) = seed.node_id.parse::<Ulid>() else {
            continue;
        };
        for id in graph_store.bfs(tenant, seed_id, depth as usize, Direction::Out, relation)? {
            let id_string = id.to_string();
            if !emitted.insert(id_string) {
                continue;
            }
            if let Some(node) = storage.get_node(tenant, id)? {
                out.push(node_to_query_record(node));
            }
        }
    }
    Ok(out)
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
    /// Exact metadata predicates. All entries are AND-ed.
    pub metadata: BTreeMap<String, serde_json::Value>,
}

impl VectorFilter {
    /// Empty filter (matches everything).
    pub fn new() -> Self {
        Self::default()
    }
    /// Require this `NodeKind`.
    pub fn kind(mut self, k: NodeKind) -> Self {
        self.kind = Some(k);
        self
    }
    /// Require `created_at_ms >= t`.
    pub fn after_ms(mut self, t: i64) -> Self {
        self.after_ms = Some(t);
        self
    }
    /// Require `created_at_ms <= t`.
    pub fn before_ms(mut self, t: i64) -> Self {
        self.before_ms = Some(t);
        self
    }
    /// Require exact equality on a metadata key/value pair.
    pub fn metadata_eq(mut self, key: impl Into<String>, value: serde_json::Value) -> Self {
        self.metadata.insert(key.into(), value);
        self
    }
    /// Test the filter against a fully-decoded node.
    pub fn matches(&self, n: &MemoryNode) -> bool {
        self.matches_meta(n.kind.as_u8(), n.created_at_ms)
            && self
                .metadata
                .iter()
                .all(|(k, v)| n.metadata.get(k) == Some(v))
    }

    /// Predicate against pre-decoded meta. Faster path used by
    /// `vector_search_filtered` when only kind+ts are needed.
    pub fn matches_meta(&self, kind_u8: u8, created_at_ms: i64) -> bool {
        if let Some(k) = self.kind {
            if kind_u8 != k.as_u8() {
                return false;
            }
        }
        if let Some(a) = self.after_ms {
            if created_at_ms < a {
                return false;
            }
        }
        if let Some(b) = self.before_ms {
            if created_at_ms > b {
                return false;
            }
        }
        true
    }
}

fn metadata_candidate_set(
    storage: &Storage,
    tenant: u32,
    filters: &BTreeMap<String, serde_json::Value>,
) -> Result<HashSet<Ulid>> {
    let mut iter = filters.iter();
    let Some((first_key, first_value)) = iter.next() else {
        return Ok(HashSet::new());
    };
    let mut allowed = storage.node_ids_by_metadata(tenant, first_key, first_value)?;
    for (key, value) in iter {
        let next = storage.node_ids_by_metadata(tenant, key, value)?;
        allowed.retain(|id| next.contains(id));
        if allowed.is_empty() {
            break;
        }
    }
    Ok(allowed)
}

fn resolve_query_vector(
    query: &mmdb_query::VectorRef,
    model: &mmdb_query::ModelId,
    embedder: Option<&dyn Embedder>,
) -> Result<Vec<f32>> {
    match query {
        mmdb_query::VectorRef::Vector(vector) => Ok(vector.clone()),
        mmdb_query::VectorRef::Text(text) => {
            let embedder = embedder.ok_or_else(|| {
                mmdb_core::Error::InvalidArgument(
                    "text vector query requires an embedder (use Database::open_with_embedder)"
                        .into(),
                )
            })?;
            if embedder.model_name() != model.0 {
                return Err(mmdb_core::Error::InvalidArgument(format!(
                    "text vector query requested model `{}`, but configured embedder is `{}`",
                    model.0,
                    embedder.model_name()
                )));
            }
            embedder.embed(text)
        }
    }
}

fn node_to_query_record(node: MemoryNode) -> mmdb_query::Record {
    let mut record = mmdb_query::Record::new(
        node.id.to_string(),
        node.tenant,
        node.kind,
        node.created_at_ms,
    )
    .with_updated_at_ms(node.updated_at_ms);
    if let Some(literal) = content_to_query_literal(&node.content) {
        record = record.with_field("content", literal);
    }
    for (key, value) in node.metadata {
        if let Some(literal) = json_to_query_literal(value) {
            record = record.with_field(key, literal);
        }
    }
    record
}

fn content_to_query_literal(content: &Content) -> Option<mmdb_query::Literal> {
    match content {
        Content::Text(value) => Some(mmdb_query::Literal::String(value.clone())),
        Content::Structured(value) => json_to_query_literal(value.clone())
            .or_else(|| Some(mmdb_query::Literal::String(value.to_string()))),
        Content::Blob { size, mime, .. } => {
            Some(mmdb_query::Literal::String(format!("blob:{mime}:{size}")))
        }
    }
}

fn json_to_query_literal(value: serde_json::Value) -> Option<mmdb_query::Literal> {
    match value {
        serde_json::Value::String(value) => Some(mmdb_query::Literal::String(value)),
        serde_json::Value::Bool(value) => Some(mmdb_query::Literal::Bool(value)),
        serde_json::Value::Number(value) => {
            value.as_i64().map(mmdb_query::Literal::I64).or_else(|| {
                value
                    .as_u64()
                    .and_then(|v| u32::try_from(v).ok())
                    .map(mmdb_query::Literal::U32)
            })
        }
        serde_json::Value::Null | serde_json::Value::Array(_) | serde_json::Value::Object(_) => {
            None
        }
    }
}

fn query_predicate_matches(record: &mmdb_query::Record, pred: &mmdb_query::Predicate) -> bool {
    match pred {
        mmdb_query::Predicate::Eq(field, literal) => {
            query_field_literal(record, field).as_ref() == Some(literal)
        }
        mmdb_query::Predicate::Gt(field, literal) => {
            query_compare_i64(record, field, literal, |a, b| a > b)
        }
        mmdb_query::Predicate::Gte(field, literal) => {
            query_compare_i64(record, field, literal, |a, b| a >= b)
        }
        mmdb_query::Predicate::Lt(field, literal) => {
            query_compare_i64(record, field, literal, |a, b| a < b)
        }
        mmdb_query::Predicate::Lte(field, literal) => {
            query_compare_i64(record, field, literal, |a, b| a <= b)
        }
        mmdb_query::Predicate::In(field, literals) => query_field_literal(record, field)
            .map(|value| literals.contains(&value))
            .unwrap_or(false),
        mmdb_query::Predicate::And(preds) => preds
            .iter()
            .all(|pred| query_predicate_matches(record, pred)),
        mmdb_query::Predicate::Or(preds) => preds
            .iter()
            .any(|pred| query_predicate_matches(record, pred)),
        mmdb_query::Predicate::Not(pred) => !query_predicate_matches(record, pred),
    }
}

fn query_field_literal(
    record: &mmdb_query::Record,
    field: &mmdb_query::FieldRef,
) -> Option<mmdb_query::Literal> {
    match field {
        mmdb_query::FieldRef::Tenant => Some(mmdb_query::Literal::U32(record.tenant)),
        mmdb_query::FieldRef::Kind => Some(mmdb_query::Literal::NodeKind(record.kind)),
        mmdb_query::FieldRef::CreatedAtMs => Some(mmdb_query::Literal::I64(record.created_at_ms)),
        mmdb_query::FieldRef::Score => Some(mmdb_query::Literal::F32(mmdb_query::OrderedF32(
            record.score,
        ))),
        mmdb_query::FieldRef::NodeId => Some(mmdb_query::Literal::String(record.node_id.clone())),
        mmdb_query::FieldRef::UpdatedAtMs => Some(mmdb_query::Literal::I64(record.updated_at_ms)),
        mmdb_query::FieldRef::Content => record.fields.get("content").cloned(),
        mmdb_query::FieldRef::Metadata(key) => record.fields.get(key).cloned(),
    }
}

fn query_compare_i64(
    record: &mmdb_query::Record,
    field: &mmdb_query::FieldRef,
    literal: &mmdb_query::Literal,
    cmp: impl FnOnce(i64, i64) -> bool,
) -> bool {
    if let (mmdb_query::FieldRef::Score, mmdb_query::Literal::F32(rhs)) = (field, literal) {
        return cmp_f32(record.score, rhs.0, cmp);
    }
    let Some(mmdb_query::Literal::I64(lhs)) = query_field_literal(record, field) else {
        return false;
    };
    let mmdb_query::Literal::I64(rhs) = literal else {
        return false;
    };
    cmp(lhs, *rhs)
}

fn cmp_f32(lhs: f32, rhs: f32, cmp: impl FnOnce(i64, i64) -> bool) -> bool {
    match lhs.total_cmp(&rhs) {
        std::cmp::Ordering::Less => cmp(0, 1),
        std::cmp::Ordering::Equal => cmp(0, 0),
        std::cmp::Ordering::Greater => cmp(1, 0),
    }
}

fn collect_query_operator(
    op: &mut dyn mmdb_query::PhysicalOperator,
) -> Result<Vec<mmdb_query::Record>> {
    let mut rows = Vec::new();
    while let Some(batch) = op.next_batch()? {
        rows.extend(batch.rows);
    }
    Ok(rows)
}

fn blob_hash(content: &Content) -> Option<[u8; 32]> {
    match content {
        Content::Blob { hash, .. } => Some(*hash),
        _ => None,
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

fn rebuild_catalog(storage: &Storage, tenant: u32) -> Result<Catalog> {
    let catalog = Catalog::default();
    for node in storage.scan_by_time(tenant, 0, i64::MAX, usize::MAX)? {
        catalog.record_node_insert(tenant, node.kind);
    }
    Ok(catalog)
}

fn query_stats_from_catalog(stats: mmdb_catalog::TenantStats) -> mmdb_query::Stats {
    let mut histograms = BTreeMap::new();
    histograms.insert(
        mmdb_query::FieldRef::Kind,
        mmdb_query::FieldHistogram::from_counts(
            stats
                .nodes_by_kind
                .into_iter()
                .map(|(kind, count)| (mmdb_query::Literal::NodeKind(kind), count)),
        ),
    );
    mmdb_query::Stats {
        node_rows: stats.total_nodes.min(usize::MAX as u64) as usize,
        estimated_filter_selectivity: 1.0,
        histograms,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mmdb_query::{
        AggregateExpr, FieldRef, Literal, LogicalPlan, ModelId, Predicate, SortKey, SourceExecutor,
        TableId, VectorRef,
    };
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
        assert_eq!(
            hits.len(),
            2,
            "got {:?}",
            hits.iter().map(|h| &h.node.id).collect::<Vec<_>>()
        );
        assert_eq!(hits[0].node.id, id1);
        assert_eq!(hits[1].node.id, id3);
        assert!(hits[0].score >= hits[1].score);
    }

    #[test]
    fn vector_search_filtered_by_kind_and_time() {
        let dir = tempdir().unwrap();
        let db = Database::open(dir.path()).unwrap();
        let v = norm(vec![1.0, 0.0, 0.0, 0.0]);
        let fact_id = db
            .insert(
                NodeBuilder::new(NodeKind::Fact)
                    .text("fact")
                    .created_at(1_000)
                    .embedding(DEFAULT_MODEL, v.clone())
                    .build(),
            )
            .unwrap();
        let ep_id = db
            .insert(
                NodeBuilder::new(NodeKind::Episode)
                    .text("episode")
                    .created_at(2_000)
                    .embedding(DEFAULT_MODEL, v.clone())
                    .build(),
            )
            .unwrap();
        // kind filter — only Fact survives
        let hits = db
            .vector_search_filtered(&v, 5, VectorFilter::new().kind(NodeKind::Fact))
            .unwrap();
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].node.id, fact_id);
        // time-window — only Episode survives
        let hits = db
            .vector_search_filtered(&v, 5, VectorFilter::new().after_ms(1_500))
            .unwrap();
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].node.id, ep_id);
        // both — empty
        let hits = db
            .vector_search_filtered(
                &v,
                5,
                VectorFilter::new().kind(NodeKind::Fact).after_ms(1_500),
            )
            .unwrap();
        assert!(hits.is_empty());
    }

    #[test]
    fn vector_search_filtered_by_metadata_value() {
        let dir = tempdir().unwrap();
        let db = Database::open(dir.path()).unwrap();
        let v = norm(vec![1.0, 0.0, 0.0, 0.0]);

        let keep = db
            .insert(
                NodeBuilder::new(NodeKind::Fact)
                    .text("keep")
                    .metadata("project", serde_json::json!("alpha"))
                    .embedding(DEFAULT_MODEL, v.clone())
                    .build(),
            )
            .unwrap();
        db.insert(
            NodeBuilder::new(NodeKind::Fact)
                .text("drop")
                .metadata("project", serde_json::json!("beta"))
                .embedding(DEFAULT_MODEL, v.clone())
                .build(),
        )
        .unwrap();

        let hits = db
            .vector_search_filtered(
                &v,
                5,
                VectorFilter::new().metadata_eq("project", serde_json::json!("alpha")),
            )
            .unwrap();
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].node.id, keep);

        let mut updated = db.get(keep).unwrap().unwrap();
        updated
            .metadata
            .insert("project".into(), serde_json::json!("gamma"));
        db.insert(updated).unwrap();
        let hits = db
            .vector_search_filtered(
                &v,
                5,
                VectorFilter::new().metadata_eq("project", serde_json::json!("alpha")),
            )
            .unwrap();
        assert!(hits.is_empty());
        let hits = db
            .vector_search_filtered(
                &v,
                5,
                VectorFilter::new().metadata_eq("project", serde_json::json!("gamma")),
            )
            .unwrap();
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].node.id, keep);

        db.delete(keep).unwrap();
        let hits = db
            .vector_search_filtered(
                &v,
                5,
                VectorFilter::new().metadata_eq("project", serde_json::json!("gamma")),
            )
            .unwrap();
        assert!(hits.is_empty());
    }

    #[test]
    fn execute_query_scans_persisted_nodes_with_filter() {
        let dir = tempdir().unwrap();
        let db = Database::open(dir.path()).unwrap();
        let keep = db
            .insert(
                NodeBuilder::new(NodeKind::Fact)
                    .text("alpha fact")
                    .created_at(1_000)
                    .metadata("project", serde_json::json!("alpha"))
                    .build(),
            )
            .unwrap();
        db.insert(
            NodeBuilder::new(NodeKind::Episode)
                .text("alpha episode")
                .created_at(1_100)
                .metadata("project", serde_json::json!("alpha"))
                .build(),
        )
        .unwrap();
        db.insert(
            NodeBuilder::new(NodeKind::Fact)
                .text("beta fact")
                .created_at(1_200)
                .metadata("project", serde_json::json!("beta"))
                .build(),
        )
        .unwrap();

        let rows = db
            .execute_query(&LogicalPlan::Scan {
                table: TableId::Nodes,
                filter: Some(Predicate::and([
                    Predicate::kind_eq(NodeKind::Fact),
                    Predicate::eq(
                        FieldRef::Metadata("project".to_string()),
                        Literal::String("alpha".to_string()),
                    ),
                ])),
            })
            .unwrap();

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].node_id, keep.to_string());
        assert_eq!(
            rows[0].fields.get("project"),
            Some(&Literal::String("alpha".to_string()))
        );
    }

    #[test]
    fn execute_query_projects_content_and_metadata_fields() {
        let dir = tempdir().unwrap();
        let db = Database::open(dir.path()).unwrap();
        let id = db
            .insert(
                NodeBuilder::new(NodeKind::Fact)
                    .text("project me")
                    .metadata("project", serde_json::json!("alpha"))
                    .build(),
            )
            .unwrap();

        let rows = db
            .execute_query(&LogicalPlan::Project {
                input: Box::new(LogicalPlan::Scan {
                    table: TableId::Nodes,
                    filter: Some(Predicate::kind_eq(NodeKind::Fact)),
                }),
                fields: vec![
                    FieldRef::NodeId,
                    FieldRef::Content,
                    FieldRef::Metadata("project".to_string()),
                    FieldRef::Score,
                ],
            })
            .unwrap();

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].node_id, id.to_string());
        assert_eq!(
            rows[0].fields,
            BTreeMap::from([
                ("node_id".to_string(), Literal::String(id.to_string())),
                (
                    "content".to_string(),
                    Literal::String("project me".to_string())
                ),
                ("project".to_string(), Literal::String("alpha".to_string())),
                (
                    "score".to_string(),
                    Literal::F32(mmdb_query::OrderedF32(0.0))
                ),
            ])
        );
    }

    #[test]
    fn execute_query_projects_vector_score_field() {
        let dir = tempdir().unwrap();
        let db = Database::open(dir.path()).unwrap();
        let q = norm(vec![1.0, 0.0, 0.0]);
        let id = db
            .insert(
                NodeBuilder::new(NodeKind::Fact)
                    .text("scored")
                    .embedding(DEFAULT_MODEL, q.clone())
                    .build(),
            )
            .unwrap();

        let rows = db
            .execute_query(&LogicalPlan::Project {
                input: Box::new(LogicalPlan::VectorSearch {
                    query: VectorRef::Vector(q),
                    k: 1,
                    filter: None,
                    model: ModelId::from(DEFAULT_MODEL),
                }),
                fields: vec![FieldRef::NodeId, FieldRef::Score],
            })
            .unwrap();

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].node_id, id.to_string());
        let Some(Literal::F32(score)) = rows[0].fields.get("score") else {
            panic!("expected projected score");
        };
        assert!(score.0 > 0.99);
    }

    #[test]
    fn execute_query_filters_updated_at_field() {
        let dir = tempdir().unwrap();
        let db = Database::open(dir.path()).unwrap();
        let mut old = NodeBuilder::new(NodeKind::Fact)
            .text("old")
            .created_at(100)
            .build();
        old.updated_at_ms = 200;
        db.insert(old).unwrap();
        let mut fresh = NodeBuilder::new(NodeKind::Fact)
            .text("fresh")
            .created_at(100)
            .build();
        fresh.updated_at_ms = 900;
        let fresh_id = fresh.id;
        db.insert(fresh).unwrap();

        let rows = db
            .execute_query(&LogicalPlan::Scan {
                table: TableId::Nodes,
                filter: Some(Predicate::Gte(FieldRef::UpdatedAtMs, Literal::I64(800))),
            })
            .unwrap();

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].node_id, fresh_id.to_string());
    }

    #[test]
    fn execute_query_uses_vector_and_graph_stores() {
        let dir = tempdir().unwrap();
        let db = Database::open(dir.path()).unwrap();
        let q = norm(vec![1.0, 0.0, 0.0, 0.0]);
        let seed = db
            .insert(
                NodeBuilder::new(NodeKind::Fact)
                    .text("seed")
                    .created_at(1_000)
                    .embedding(DEFAULT_MODEL, q.clone())
                    .build(),
            )
            .unwrap();
        let related = db
            .insert(
                NodeBuilder::new(NodeKind::Fact)
                    .text("related")
                    .created_at(1_100)
                    .build(),
            )
            .unwrap();
        db.insert(
            NodeBuilder::new(NodeKind::Fact)
                .text("far")
                .created_at(1_200)
                .embedding(DEFAULT_MODEL, norm(vec![0.0, 1.0, 0.0, 0.0]))
                .build(),
        )
        .unwrap();
        db.add_edge(Edge {
            src: seed,
            dst: related,
            label: "related".to_string(),
            weight: 1.0,
            created_at_ms: 1_300,
            metadata: BTreeMap::new(),
        })
        .unwrap();

        let rows = db
            .execute_query(&LogicalPlan::TopK {
                input: Box::new(LogicalPlan::GraphExpand {
                    from: Box::new(LogicalPlan::VectorSearch {
                        query: VectorRef::Vector(q),
                        k: 1,
                        filter: None,
                        model: ModelId::from(DEFAULT_MODEL),
                    }),
                    relation: Some("related".to_string()),
                    depth: 1,
                }),
                k: 2,
                by: SortKey::ScoreDesc,
            })
            .unwrap();

        let ids = rows
            .iter()
            .map(|row| row.node_id.as_str())
            .collect::<Vec<_>>();
        assert!(ids.contains(&seed.to_string().as_str()));
        assert!(ids.contains(&related.to_string().as_str()));
    }

    #[test]
    fn execute_query_embeds_text_vector_ref_with_configured_embedder() {
        let dir = tempdir().unwrap();
        let cfg = DatabaseConfig {
            tenant: DEFAULT_TENANT,
            default_model: "hash-32".into(),
        };
        let db = Database::open_with_embedder(
            dir.path(),
            cfg,
            Box::new(HashEmbedder::new("hash-32", 32)),
        )
        .unwrap();
        let keep = db
            .insert_text(NodeKind::Fact, "quarterly revenue memo")
            .unwrap();
        db.insert_text(NodeKind::Fact, "garden planning note")
            .unwrap();

        let rows = db
            .execute_query(&LogicalPlan::VectorSearch {
                query: VectorRef::Text("quarterly revenue".to_string()),
                k: 1,
                filter: Some(Predicate::kind_eq(NodeKind::Fact)),
                model: ModelId::from("hash-32"),
            })
            .unwrap();

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].node_id, keep.to_string());
    }

    #[test]
    fn source_executor_runs_against_database_stores() {
        let dir = tempdir().unwrap();
        let db = Database::open(dir.path()).unwrap();
        let q = norm(vec![1.0, 0.0, 0.0, 0.0]);
        let seed = db
            .insert(
                NodeBuilder::new(NodeKind::Fact)
                    .text("seed")
                    .created_at(1_000)
                    .embedding(DEFAULT_MODEL, q.clone())
                    .build(),
            )
            .unwrap();
        let related = db
            .insert(
                NodeBuilder::new(NodeKind::Episode)
                    .text("related")
                    .created_at(1_100)
                    .build(),
            )
            .unwrap();
        db.add_edge(Edge {
            src: seed,
            dst: related,
            label: "related".to_string(),
            weight: 1.0,
            created_at_ms: 1_200,
            metadata: BTreeMap::new(),
        })
        .unwrap();

        let plan = LogicalPlan::TopK {
            input: Box::new(LogicalPlan::GraphExpand {
                from: Box::new(LogicalPlan::VectorSearch {
                    query: VectorRef::Vector(q),
                    k: 1,
                    filter: None,
                    model: ModelId::from(DEFAULT_MODEL),
                }),
                relation: Some("related".to_string()),
                depth: 1,
            }),
            k: 2,
            by: SortKey::ScoreDesc,
        };

        let mut op = SourceExecutor::new(&db).compile(&plan, 1).unwrap();
        let mut rows = Vec::new();
        while let Some(batch) = op.next_batch().unwrap() {
            rows.extend(batch.rows);
        }

        let ids = rows
            .iter()
            .map(|row| row.node_id.as_str())
            .collect::<Vec<_>>();
        assert!(ids.contains(&seed.to_string().as_str()));
        assert!(ids.contains(&related.to_string().as_str()));

        let explain = SourceExecutor::new(&db)
            .explain(&plan, &db.query_optimizer_stats(), 2)
            .unwrap();
        assert_eq!(explain.operator, "TopKOp");
        assert_eq!(explain.actual_rows, Some(2));
        assert_eq!(explain.children[0].operator, "GraphExpandOp");
        assert_eq!(explain.children[0].actual_rows, Some(2));
        assert_eq!(explain.children[0].children[0].operator, "HnswSearchOp");
        assert_eq!(explain.children[0].children[0].actual_rows, Some(1));
    }

    #[test]
    fn execute_query_physical_matches_facade_for_udf_free_plan() {
        let dir = tempdir().unwrap();
        let db = Database::open(dir.path()).unwrap();
        let q = norm(vec![1.0, 0.0, 0.0, 0.0]);
        let seed = db
            .insert(
                NodeBuilder::new(NodeKind::Fact)
                    .text("seed")
                    .created_at(1_000)
                    .embedding(DEFAULT_MODEL, q.clone())
                    .build(),
            )
            .unwrap();
        let related = db
            .insert(
                NodeBuilder::new(NodeKind::Fact)
                    .text("related")
                    .created_at(1_100)
                    .build(),
            )
            .unwrap();
        db.add_edge(Edge {
            src: seed,
            dst: related,
            label: "related".to_string(),
            weight: 1.0,
            created_at_ms: 1_200,
            metadata: BTreeMap::new(),
        })
        .unwrap();
        let plan = LogicalPlan::TopK {
            input: Box::new(LogicalPlan::GraphExpand {
                from: Box::new(LogicalPlan::VectorSearch {
                    query: VectorRef::Vector(q),
                    k: 1,
                    filter: None,
                    model: ModelId::from(DEFAULT_MODEL),
                }),
                relation: Some("related".to_string()),
                depth: 1,
            }),
            k: 2,
            by: SortKey::ScoreDesc,
        };

        let recursive_rows = db.execute_query(&plan).unwrap();
        let physical_rows = db.execute_query_physical(&plan).unwrap();

        assert_eq!(physical_rows, recursive_rows);
    }

    #[test]
    fn execute_query_counts_rows_grouped_by_kind() {
        let dir = tempdir().unwrap();
        let db = Database::open(dir.path()).unwrap();
        db.insert(NodeBuilder::new(NodeKind::Fact).text("fact one").build())
            .unwrap();
        db.insert(NodeBuilder::new(NodeKind::Fact).text("fact two").build())
            .unwrap();
        db.insert(NodeBuilder::new(NodeKind::Episode).text("episode").build())
            .unwrap();

        let rows = db
            .execute_query(&LogicalPlan::Aggregate {
                input: Box::new(LogicalPlan::Scan {
                    table: TableId::Nodes,
                    filter: None,
                }),
                group_by: vec![FieldRef::Kind],
                aggregate: AggregateExpr::Count,
            })
            .unwrap();

        assert_eq!(
            rows.iter()
                .find(|row| row.fields.get("kind") == Some(&Literal::NodeKind(NodeKind::Fact)))
                .and_then(|row| row.fields.get("count")),
            Some(&Literal::I64(2))
        );
        assert_eq!(
            rows.iter()
                .find(|row| row.fields.get("kind") == Some(&Literal::NodeKind(NodeKind::Episode)))
                .and_then(|row| row.fields.get("count")),
            Some(&Literal::I64(1))
        );
    }

    #[test]
    fn execute_query_applies_registered_udf_score() {
        let dir = tempdir().unwrap();
        let db = Database::open(dir.path()).unwrap();
        db.insert(NodeBuilder::new(NodeKind::Fact).text("low").build())
            .unwrap();
        let boosted = db
            .insert(NodeBuilder::new(NodeKind::Episode).text("boosted").build())
            .unwrap();
        db.register_query_udf("boost_episode", |record, _args| {
            if record.kind == NodeKind::Episode {
                10.0
            } else {
                1.0
            }
        });

        let rows = db
            .execute_query(&LogicalPlan::TopK {
                input: Box::new(LogicalPlan::Udf {
                    input: Box::new(LogicalPlan::Scan {
                        table: TableId::Nodes,
                        filter: None,
                    }),
                    name: "boost_episode".to_string(),
                    args: vec![],
                }),
                k: 1,
                by: SortKey::ScoreDesc,
            })
            .unwrap();

        assert_eq!(rows[0].node_id, boosted.to_string());
        assert_eq!(rows[0].score, 10.0);
    }

    #[test]
    fn execute_query_physical_applies_registered_udf_score() {
        let dir = tempdir().unwrap();
        let db = Database::open(dir.path()).unwrap();
        db.insert(NodeBuilder::new(NodeKind::Fact).text("low").build())
            .unwrap();
        let boosted = db
            .insert(NodeBuilder::new(NodeKind::Episode).text("boosted").build())
            .unwrap();
        db.register_query_udf("boost_episode", |record, _args| {
            if record.kind == NodeKind::Episode {
                10.0
            } else {
                1.0
            }
        });

        let rows = db
            .execute_query_physical(&LogicalPlan::TopK {
                input: Box::new(LogicalPlan::Udf {
                    input: Box::new(LogicalPlan::Scan {
                        table: TableId::Nodes,
                        filter: None,
                    }),
                    name: "boost_episode".to_string(),
                    args: vec![],
                }),
                k: 1,
                by: SortKey::ScoreDesc,
            })
            .unwrap();

        assert_eq!(rows[0].node_id, boosted.to_string());
        assert_eq!(rows[0].score, 10.0);
    }

    #[test]
    fn execute_query_async_matches_sync_facade() {
        let dir = tempdir().unwrap();
        let db = Database::open(dir.path()).unwrap();
        db.insert(
            NodeBuilder::new(NodeKind::Fact)
                .text("async query")
                .created_at(1_000)
                .build(),
        )
        .unwrap();
        let plan = LogicalPlan::Scan {
            table: TableId::Nodes,
            filter: Some(Predicate::kind_eq(NodeKind::Fact)),
        };

        let sync_rows = db.execute_query(&plan).unwrap();
        let async_rows = block_on(db.execute_query_async(&plan)).unwrap();

        assert_eq!(async_rows, sync_rows);
    }

    #[test]
    fn execute_query_async_returns_pending_before_worker_finishes() {
        let dir = tempdir().unwrap();
        let db = Database::open(dir.path()).unwrap();
        db.insert(NodeBuilder::new(NodeKind::Fact).text("async yield").build())
            .unwrap();
        let plan = LogicalPlan::Scan {
            table: TableId::Nodes,
            filter: Some(Predicate::kind_eq(NodeKind::Fact)),
        };

        let waker = noop_waker();
        let mut cx = std::task::Context::from_waker(&waker);
        let mut future = Box::pin(db.execute_query_async(&plan));

        assert!(matches!(
            std::future::Future::poll(future.as_mut(), &mut cx),
            std::task::Poll::Pending
        ));
        let started = std::time::Instant::now();
        loop {
            match std::future::Future::poll(future.as_mut(), &mut cx) {
                std::task::Poll::Ready(Ok(rows)) => {
                    assert_eq!(rows.len(), 1);
                    break;
                }
                std::task::Poll::Ready(Err(err)) => panic!("async query failed: {err}"),
                std::task::Poll::Pending => {
                    assert!(
                        started.elapsed() < std::time::Duration::from_secs(2),
                        "async query worker did not finish"
                    );
                    std::thread::yield_now();
                }
            }
        }
    }

    #[test]
    fn execute_query_async_does_not_block_polling_thread_on_sync_work() {
        let dir = tempdir().unwrap();
        let db = Database::open(dir.path()).unwrap();
        db.insert(
            NodeBuilder::new(NodeKind::Fact)
                .text("async offload")
                .build(),
        )
        .unwrap();
        db.register_query_udf("slow_boost", |record, _args| {
            std::thread::sleep(std::time::Duration::from_millis(200));
            record.score + 1.0
        });
        let plan = LogicalPlan::Udf {
            input: Box::new(LogicalPlan::Scan {
                table: TableId::Nodes,
                filter: Some(Predicate::kind_eq(NodeKind::Fact)),
            }),
            name: "slow_boost".to_string(),
            args: Vec::new(),
        };

        let waker = noop_waker();
        let mut cx = std::task::Context::from_waker(&waker);
        let mut future = Box::pin(db.execute_query_async(&plan));

        assert!(matches!(
            std::future::Future::poll(future.as_mut(), &mut cx),
            std::task::Poll::Pending
        ));
        let started = std::time::Instant::now();
        let second_poll = std::future::Future::poll(future.as_mut(), &mut cx);

        assert!(
            started.elapsed() < std::time::Duration::from_millis(50),
            "polling thread was blocked by synchronous query work"
        );
        assert!(matches!(second_poll, std::task::Poll::Pending));

        std::thread::sleep(std::time::Duration::from_millis(250));
        let ready = std::future::Future::poll(future.as_mut(), &mut cx);
        assert!(matches!(ready, std::task::Poll::Ready(Ok(_))));
    }

    #[test]
    fn query_optimizer_stats_are_rebuilt_from_persisted_nodes() {
        let dir = tempdir().unwrap();
        {
            let db = Database::open(dir.path()).unwrap();
            db.insert(NodeBuilder::new(NodeKind::Fact).text("fact one").build())
                .unwrap();
            db.insert(NodeBuilder::new(NodeKind::Fact).text("fact two").build())
                .unwrap();
            db.insert(
                NodeBuilder::new(NodeKind::Episode)
                    .text("episode one")
                    .build(),
            )
            .unwrap();
        }

        let db = Database::open(dir.path()).unwrap();
        let stats = db.query_optimizer_stats();
        let kind_histogram = stats.histograms.get(&FieldRef::Kind).unwrap();

        assert_eq!(stats.node_rows, 3);
        assert_eq!(kind_histogram.total_count(), 3);
        assert_eq!(kind_histogram.count(&Literal::NodeKind(NodeKind::Fact)), 2);
        assert_eq!(
            kind_histogram.count(&Literal::NodeKind(NodeKind::Episode)),
            1
        );
        assert_eq!(
            stats.estimate_selectivity(&Predicate::kind_eq(NodeKind::Fact)),
            2.0 / 3.0
        );
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

    #[test]
    fn insert_rejects_vector_dim_mismatch_without_persisting_node() {
        let dir = tempdir().unwrap();
        let db = Database::open(dir.path()).unwrap();
        let seed = db
            .insert(
                NodeBuilder::new(NodeKind::Fact)
                    .text("seed")
                    .embedding(DEFAULT_MODEL, norm(vec![1.0, 0.0, 0.0]))
                    .build(),
            )
            .unwrap();
        let bad = NodeBuilder::new(NodeKind::Fact)
            .text("bad")
            .embedding(DEFAULT_MODEL, vec![1.0, 0.0])
            .build();
        let bad_id = bad.id;

        let err = db.insert(bad).unwrap_err();

        assert!(matches!(err, mmdb_core::Error::InvalidArgument(_)));
        assert!(db.get(bad_id).unwrap().is_none());
        let hits = db.vector_search(&norm(vec![1.0, 0.0, 0.0]), 5).unwrap();
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].node.id, seed);
    }

    /// Toy embedder: tokenize on whitespace + FNV1a hash into a fixed-dim bucket.
    /// Deterministic & content-discriminating enough for unit tests.
    struct HashEmbedder {
        dim: u32,
        name: String,
    }
    impl HashEmbedder {
        fn new(name: &str, dim: u32) -> Self {
            Self {
                dim,
                name: name.to_string(),
            }
        }
        fn fnv1a(s: &str) -> u32 {
            let mut h: u32 = 0x811c9dc5;
            for b in s.as_bytes() {
                h ^= *b as u32;
                h = h.wrapping_mul(0x01000193);
            }
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
            let n: f32 = v.iter().map(|x| x * x).sum::<f32>().sqrt();
            if n > 0.0 {
                for x in v.iter_mut() {
                    *x /= n;
                }
            }
            Ok(v)
        }
        fn model_name(&self) -> &str {
            &self.name
        }
        fn dim(&self) -> u32 {
            self.dim
        }
    }

    #[test]
    fn auto_embeds_text_on_insert() {
        let dir = tempdir().unwrap();
        let cfg = DatabaseConfig {
            tenant: DEFAULT_TENANT,
            default_model: "hash-32".into(),
        };
        let db = Database::open_with_embedder(
            dir.path(),
            cfg,
            Box::new(HashEmbedder::new("hash-32", 32)),
        )
        .unwrap();
        assert!(db.has_embedder());

        let id = db
            .insert_text(NodeKind::Fact, "the quick brown fox")
            .unwrap();
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
        let cfg = DatabaseConfig {
            tenant: DEFAULT_TENANT,
            default_model: "hash-32".into(),
        };
        let db = Database::open_with_embedder(
            dir.path(),
            cfg,
            Box::new(HashEmbedder::new("hash-32", 32)),
        )
        .unwrap();
        // Pre-attach an embedding under the embedder's model -> auto-embed skipped.
        let mut v = vec![0.0f32; 32];
        v[0] = 1.0;
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
    fn open_with_embedder_rejects_model_mismatch() {
        let dir = tempdir().unwrap();
        let cfg = DatabaseConfig {
            tenant: DEFAULT_TENANT,
            default_model: "configured".into(),
        };

        let result = Database::open_with_embedder(
            dir.path(),
            cfg,
            Box::new(HashEmbedder::new("actual", 32)),
        );
        let err = match result {
            Ok(_) => panic!("expected model mismatch to be rejected"),
            Err(err) => err,
        };

        assert!(format!("{err}").contains("does not match"));
    }

    struct AsyncOnlyEmbedder;
    impl Embedder for AsyncOnlyEmbedder {
        fn embed(&self, _text: &str) -> Result<Vec<f32>> {
            Err(mmdb_core::Error::InvalidArgument(
                "sync embed should not run".into(),
            ))
        }
        fn model_name(&self) -> &str {
            "async-4"
        }
        fn dim(&self) -> u32 {
            4
        }
        fn embed_async<'a>(&'a self, _text: &'a str) -> EmbedFuture<'a> {
            Box::pin(async move { Ok(vec![1.0, 0.0, 0.0, 0.0]) })
        }
    }

    fn block_on<F: std::future::Future>(future: F) -> F::Output {
        let waker = noop_waker();
        let mut cx = std::task::Context::from_waker(&waker);
        let mut future = Box::pin(future);
        loop {
            match std::future::Future::poll(future.as_mut(), &mut cx) {
                std::task::Poll::Ready(value) => return value,
                std::task::Poll::Pending => std::thread::yield_now(),
            }
        }
    }

    fn noop_waker() -> std::task::Waker {
        fn raw_waker() -> std::task::RawWaker {
            fn clone(_: *const ()) -> std::task::RawWaker {
                raw_waker()
            }
            fn wake(_: *const ()) {}
            fn wake_by_ref(_: *const ()) {}
            fn drop(_: *const ()) {}
            std::task::RawWaker::new(
                std::ptr::null(),
                &std::task::RawWakerVTable::new(clone, wake, wake_by_ref, drop),
            )
        }

        unsafe { std::task::Waker::from_raw(raw_waker()) }
    }

    #[test]
    fn async_text_paths_use_async_embedder() {
        let dir = tempdir().unwrap();
        let cfg = DatabaseConfig {
            tenant: DEFAULT_TENANT,
            default_model: "async-4".into(),
        };
        let db =
            Database::open_with_embedder(dir.path(), cfg, Box::new(AsyncOnlyEmbedder)).unwrap();

        let id = block_on(db.insert_text_async(NodeKind::Fact, "async memory")).unwrap();
        let got = db.get(id).unwrap().unwrap();
        assert_eq!(got.embeddings[0].model, "async-4");

        let hits = block_on(db.search_text_async("async memory", 1)).unwrap();
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].node.id, id);
    }

    #[test]
    fn hybrid_search_promotes_neighbour_via_graph() {
        let dir = tempdir().unwrap();
        let db = Database::open(dir.path()).unwrap();

        // Three facts: query is closest to A; B is mid; C is far.
        let a = db
            .insert(
                NodeBuilder::new(NodeKind::Fact)
                    .text("A")
                    .embedding(DEFAULT_MODEL, norm(vec![1.0, 0.0, 0.0, 0.0]))
                    .build(),
            )
            .unwrap();
        let b = db
            .insert(
                NodeBuilder::new(NodeKind::Fact)
                    .text("B")
                    .embedding(DEFAULT_MODEL, norm(vec![0.6, 0.8, 0.0, 0.0]))
                    .build(),
            )
            .unwrap();
        let c = db
            .insert(
                NodeBuilder::new(NodeKind::Fact)
                    .text("C")
                    .embedding(DEFAULT_MODEL, norm(vec![0.0, 0.0, 1.0, 0.0]))
                    .build(),
            )
            .unwrap();

        // Wire C as a related neighbour of A.
        use mmdb_core::Edge;
        use std::collections::BTreeMap;
        db.add_edge(Edge {
            src: a,
            dst: c,
            label: "related".into(),
            weight: 1.0,
            created_at_ms: 0,
            metadata: BTreeMap::new(),
        })
        .unwrap();

        let q = norm(vec![1.0, 0.0, 0.0, 0.0]);

        // Pure vector: C is ranked below B because it's orthogonal to the query.
        let pure = db.vector_search(&q, 3).unwrap();
        let pure_order: Vec<_> = pure.iter().map(|h| h.node.id).collect();
        assert_eq!(pure_order[0], a);
        // B should beat C in pure vector ranking.
        assert!(pure_order.iter().position(|x| *x == b) < pure_order.iter().position(|x| *x == c));

        // Hybrid: C gets a neighbour bump from A and may rank above B.
        let opts = HybridOpts {
            k: 3,
            seed_k: 5,
            expand_hops: 1,
            direction: graph::Direction::Out,
            label: Some("related".into()),
            alpha: 0.3,
            decay: 1.0,
        };
        let hyb = db.hybrid_search(&q, opts).unwrap();
        let pos_b = hyb.iter().position(|h| h.node.id == b);
        let pos_c = hyb.iter().position(|h| h.node.id == c);
        assert!(pos_c.is_some(), "C must appear in hybrid result");
        // With alpha=0.3 and decay=1.0, C inherits 0.7 * a.score which dominates B.
        assert!(
            pos_c < pos_b || pos_b.is_none(),
            "C should be promoted above B; got order {:?}",
            hyb.iter().map(|h| (h.node.id, h.score)).collect::<Vec<_>>()
        );
    }

    #[test]
    fn hybrid_search_alpha_one_equals_vector_only() {
        let dir = tempdir().unwrap();
        let db = Database::open(dir.path()).unwrap();
        let v = norm(vec![1.0, 0.0, 0.0, 0.0]);
        let id = db
            .insert(
                NodeBuilder::new(NodeKind::Fact)
                    .text("x")
                    .embedding(DEFAULT_MODEL, v.clone())
                    .build(),
            )
            .unwrap();
        let opts = HybridOpts {
            alpha: 1.0,
            expand_hops: 0,
            ..Default::default()
        };
        let hits = db.hybrid_search(&v, opts).unwrap();
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].node.id, id);
    }

    #[test]
    fn edge_labels_are_available_from_facade() {
        let dir = tempdir().unwrap();
        let db = Database::open(dir.path()).unwrap();
        let a = Ulid::new();
        let b = Ulid::new();
        db.add_edge(Edge {
            src: a,
            dst: b,
            label: "mentions".into(),
            weight: 1.0,
            created_at_ms: 0,
            metadata: BTreeMap::new(),
        })
        .unwrap();

        assert_eq!(db.edge_labels().unwrap(), vec!["mentions".to_string()]);
    }

    #[test]
    fn insert_blob_stores_artifact_and_reads_stream() {
        use std::io::{Cursor, Read};

        let dir = tempdir().unwrap();
        let db = Database::open(dir.path()).unwrap();
        let id = db
            .insert_blob(
                NodeKind::Artifact,
                Cursor::new(b"blob payload".to_vec()),
                "text/plain",
            )
            .unwrap();

        let node = db.get(id).unwrap().unwrap();
        let Content::Blob { hash, size, mime } = node.content else {
            panic!("expected blob content");
        };
        assert_eq!(size, 12);
        assert_eq!(mime, "text/plain");
        assert_eq!(db.blob_refcount(&hash).unwrap(), Some(1));

        let mut bytes = Vec::new();
        db.get_blob_stream(&hash)
            .unwrap()
            .read_to_end(&mut bytes)
            .unwrap();
        assert_eq!(bytes, b"blob payload");
    }

    #[test]
    fn deleting_blob_node_releases_ref_and_gc_removes_bytes() {
        use std::io::Cursor;

        let dir = tempdir().unwrap();
        let db = Database::open(dir.path()).unwrap();
        let id = db
            .insert_blob(
                NodeKind::Artifact,
                Cursor::new(b"temporary payload".to_vec()),
                "text/plain",
            )
            .unwrap();
        let hash = match db.get(id).unwrap().unwrap().content {
            Content::Blob { hash, .. } => hash,
            _ => panic!("expected blob content"),
        };

        db.delete(id).unwrap();
        assert_eq!(db.blob_refcount(&hash).unwrap(), Some(0));
        assert_eq!(db.gc_blobs().unwrap(), 1);
        assert_eq!(db.blob_refcount(&hash).unwrap(), None);
        assert!(db.get_blob_stream(&hash).is_err());
    }

    #[test]
    fn inserting_node_with_existing_blob_reference_increments_refcount() {
        use std::io::Cursor;

        let dir = tempdir().unwrap();
        let db = Database::open(dir.path()).unwrap();
        let first = db
            .insert_blob(
                NodeKind::Artifact,
                Cursor::new(b"shared payload".to_vec()),
                "text/plain",
            )
            .unwrap();
        let (hash, size, mime) = match db.get(first).unwrap().unwrap().content {
            Content::Blob { hash, size, mime } => (hash, size, mime),
            _ => panic!("expected blob content"),
        };

        let second = db
            .insert(
                NodeBuilder::new(NodeKind::Artifact)
                    .blob(hash, size, mime)
                    .build(),
            )
            .unwrap();

        assert_eq!(db.blob_refcount(&hash).unwrap(), Some(2));
        db.delete(first).unwrap();
        assert_eq!(db.blob_refcount(&hash).unwrap(), Some(1));
        assert_eq!(db.gc_blobs().unwrap(), 0);
        assert!(db.get_blob_stream(&hash).is_ok());
        db.delete(second).unwrap();
        assert_eq!(db.blob_refcount(&hash).unwrap(), Some(0));
    }
}
