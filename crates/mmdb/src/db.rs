use crate::builder::NodeBuilder;
use crate::embedder::{DatabaseConfig, Embedder};
use crate::query_impl::{
    collect_query_operator, query_stats_from_catalog, rebuild_catalog, AsyncQueryFuture,
    AsyncQueryRequest, QuerySourceHandle, QueryUdfFn, QUERY_BATCH_SIZE,
};
use mmdb_blob::BlobStore;
use mmdb_catalog::Catalog;
use mmdb_core::{Content, Edge, Embedding, MemoryNode, NodeKind, Result};
use mmdb_graph::GraphStore;
use mmdb_storage::Storage;
use mmdb_vector::VectorStore;
use smallvec::SmallVec;
use std::collections::BTreeMap;
use std::future::Future;
use std::io::Read;
use std::path::Path;
use std::sync::{Arc, RwLock};
use ulid::Ulid;

/// High-level handle. See the crate-level docs for a quickstart.
pub struct Database {
    pub(crate) storage: Arc<Storage>,
    pub(crate) vector_store: Arc<VectorStore>,
    pub(crate) graph_store: Arc<GraphStore>,
    pub(crate) blob_store: Arc<BlobStore>,
    pub(crate) catalog: Arc<Catalog>,
    pub(crate) query_udfs: RwLock<BTreeMap<String, Arc<QueryUdfFn>>>,
    pub(crate) config: DatabaseConfig,
    pub(crate) embedder: Option<Arc<dyn Embedder>>,
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
        let blob_store = Arc::new(BlobStore::open(path)?);
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
        let blob_store = Arc::new(BlobStore::open(path)?);
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
    // Query execution
    // -----------------------------------------------------------------------

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

    pub(crate) fn graph_expand_query_rows(
        &self,
        seeds: Vec<mmdb_query::Record>,
        relation: Option<&str>,
        depth: u8,
    ) -> Result<Vec<mmdb_query::Record>> {
        crate::query_impl::graph_expand_query_rows_from(
            &self.storage,
            &self.graph_store,
            self.config.tenant,
            seeds,
            relation,
            depth,
        )
    }
}

fn blob_hash(content: &Content) -> Option<[u8; 32]> {
    match content {
        Content::Blob { hash, .. } => Some(*hash),
        _ => None,
    }
}
