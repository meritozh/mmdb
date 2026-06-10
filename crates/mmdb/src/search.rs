use crate::Database;
use mmdb_core::{MemoryNode, NodeKind, Result};
use mmdb_graph::Direction;
use mmdb_storage::Storage;
use std::collections::{BTreeMap, HashSet};
use ulid::Ulid;

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

pub(crate) fn metadata_candidate_set(
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

impl Database {
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
