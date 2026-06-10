use super::keys::*;
use super::{EXACT_SEARCH_MAX_ROWS, HitFilter, VectorStore};
use crate::{IndexKey, ScoredHit, VectorIndex};
use fjall::PersistMode;
use mmdb_core::{Error, Result};
use std::sync::Arc;
use ulid::Ulid;

impl VectorStore {
    pub fn insert(&self, tenant: u32, model: &str, node_id: Ulid, vector: &[f32]) -> Result<()> {
        self.insert_batch(tenant, model, &[(node_id, vector.to_vec())])
    }

    /// Validate that a vector can be inserted into this `(tenant, model)`
    /// space without mutating graph points or persisted meta.
    pub fn validate_insert(&self, tenant: u32, model: &str, vector: &[f32]) -> Result<()> {
        if vector.is_empty() {
            return Err(Error::InvalidArgument("empty vector".into()));
        }
        let key = IndexKey::new(tenant, model);
        if let Some(idx) = self.resolve_index(&key) {
            if idx.dim as usize != vector.len() {
                return Err(Error::InvalidArgument(format!(
                    "dim mismatch: index expects {}, got {}",
                    idx.dim,
                    vector.len()
                )));
            }
        }
        Ok(())
    }

    /// Batched insert. All entries land in a single fjall batch + persist.
    pub fn insert_batch(
        &self,
        tenant: u32,
        model: &str,
        items: &[(Ulid, Vec<f32>)],
    ) -> Result<()> {
        if items.is_empty() {
            return Ok(());
        }
        let dim = items[0].1.len();
        if dim == 0 {
            return Err(Error::InvalidArgument("empty vector".into()));
        }
        for (_, v) in items {
            if v.len() != dim {
                return Err(Error::InvalidArgument(
                    "vectors in a batch must share dim".into(),
                ));
            }
        }
        let key = IndexKey::new(tenant, model);
        let idx = self.get_or_create_index(&key, dim as u32);
        if idx.dim as usize != dim {
            return Err(Error::InvalidArgument(format!(
                "dim mismatch: index expects {}, got {}",
                idx.dim, dim
            )));
        }

        // Allocate internal ids up-front so we can write graph + meta together.
        let mut assigned: Vec<(Vec<f32>, u64)> = Vec::with_capacity(items.len());
        let mut node_to_id: Vec<(Ulid, u64)> = Vec::with_capacity(items.len());
        for (node_id, v) in items {
            let id = id_alloc(&idx);
            assigned.push((v.clone(), id));
            node_to_id.push((*node_id, id));
        }
        idx.insert_batch(&assigned);

        // Persist meta + rev in one fjall batch.
        let mut batch = self.keyspace.batch();
        for ((node_id, internal_id), (vec, _)) in node_to_id.iter().zip(assigned.iter()) {
            let meta_key = meta_key_bytes(tenant, model, *node_id);
            let mut meta_val = Vec::with_capacity(8 + 4 + vec.len() * 4);
            meta_val.extend_from_slice(&internal_id.to_be_bytes());
            meta_val.extend_from_slice(&(vec.len() as u32).to_be_bytes());
            for f in vec {
                meta_val.extend_from_slice(&f.to_le_bytes());
            }
            let rev_key = rev_key_bytes(tenant, model, *internal_id);
            batch.insert(&self.meta, meta_key, meta_val);
            batch.insert(&self.rev, rev_key, node_id.0.to_be_bytes());
        }
        batch.commit().map_err(|e| Error::Storage(e.to_string()))?;
        self.keyspace
            .persist(PersistMode::SyncAll)
            .map_err(|e| Error::Storage(e.to_string()))?;
        Ok(())
    }

    pub fn search(
        &self,
        tenant: u32,
        model: &str,
        query: &[f32],
        k: usize,
    ) -> Result<Vec<ScoredHit>> {
        self.search_with_filter(tenant, model, query, k, None)
    }

    /// Vector search with optional post-filter on the resolved `Ulid`.
    /// Implementation widens HNSW's k to `k * over_fetch` (capped) so the
    /// filter has room to drop candidates without returning fewer than k.
    pub fn search_with_filter<'a>(
        &self,
        tenant: u32,
        model: &str,
        query: &[f32],
        k: usize,
        filter: Option<&HitFilter<'a>>,
    ) -> Result<Vec<ScoredHit>> {
        let key = IndexKey::new(tenant, model);
        let Some(idx) = self.resolve_index(&key) else {
            return Ok(Vec::new());
        };
        if idx.dim as usize != query.len() {
            return Err(Error::InvalidArgument(format!(
                "dim mismatch: index expects {}, got {}",
                idx.dim,
                query.len()
            )));
        }

        if self.meta_row_count(tenant, model, EXACT_SEARCH_MAX_ROWS + 1)? <= EXACT_SEARCH_MAX_ROWS {
            return self.exact_search_from_meta(tenant, model, query, k, filter);
        }

        let over_fetch = if filter.is_some() { 4 } else { 1 };
        let widened = (k * over_fetch).max(k);
        let ef = (widened * 4).max(32);
        let raw = idx.search(query, widened, ef);

        let mut out = Vec::with_capacity(k);
        for (internal_id, dist) in raw {
            let rev_key = rev_key_bytes(tenant, model, internal_id);
            let v = self
                .rev
                .get(&rev_key)
                .map_err(|e| Error::Storage(e.to_string()))?;
            let Some(v) = v else { continue };
            if v.len() != 16 {
                continue;
            }
            let mut buf = [0u8; 16];
            buf.copy_from_slice(&v);
            let node_id = Ulid(u128::from_be_bytes(buf));
            if let Some(f) = filter {
                if !f(node_id) {
                    continue;
                }
            }
            let score = (1.0 - dist / 2.0).clamp(0.0, 1.0);
            out.push(ScoredHit {
                node_id,
                score,
                distance: dist,
            });
            if out.len() >= k {
                break;
            }
        }
        if out.len() < k {
            return self.exact_search_from_meta(tenant, model, query, k, filter);
        }
        Ok(out)
    }

    pub fn delete(&self, tenant: u32, model: &str, node_id: Ulid) -> Result<()> {
        let key = IndexKey::new(tenant, model);
        let meta_key = meta_key_bytes(tenant, model, node_id);
        let Some(v) = self
            .meta
            .get(&meta_key)
            .map_err(|e| Error::Storage(e.to_string()))?
        else {
            return Ok(());
        };
        if v.len() < 8 {
            return Err(Error::Storage("corrupt meta value".into()));
        }
        let mut buf = [0u8; 8];
        buf.copy_from_slice(&v[0..8]);
        let internal_id = u64::from_be_bytes(buf);

        if let Some(idx) = self.resolve_index(&key) {
            idx.mark_deleted(internal_id);
        }

        let rev_key = rev_key_bytes(tenant, model, internal_id);
        let tomb_key = tomb_key_bytes(tenant, model, internal_id);
        let mut batch = self.keyspace.batch();
        batch.remove(&self.meta, meta_key);
        batch.remove(&self.rev, rev_key);
        batch.insert(&self.tomb, tomb_key, []);
        batch.commit().map_err(|e| Error::Storage(e.to_string()))?;
        self.record_tombstone_high_watermark((tenant, model_hash(model)), internal_id);
        self.keyspace
            .persist(PersistMode::SyncAll)
            .map_err(|e| Error::Storage(e.to_string()))?;
        Ok(())
    }

    pub(super) fn record_tombstone_high_watermark(&self, key: (u32, u32), internal_id: u64) {
        self.tombstone_high_watermarks
            .entry(key)
            .and_modify(|max_id| *max_id = (*max_id).max(internal_id))
            .or_insert(internal_id);
    }

    fn get_or_create_index(&self, key: &IndexKey, dim: u32) -> Arc<VectorIndex> {
        if let Some(entry) = self.indices.get(key) {
            return entry.clone();
        }
        // Maybe a rebuilt placeholder exists for this hash — promote it.
        let mh = model_hash(&key.model);
        let placeholder = IndexKey::new(key.tenant, format!("__h::{:08x}", mh));
        if let Some((_, idx)) = self.indices.remove(&placeholder) {
            self.indices.insert(key.clone(), idx.clone());
            self.model_names.insert((key.tenant, mh), key.model.clone());
            return idx;
        }
        let idx = Arc::new(VectorIndex::new(dim));
        if let Some(max_id) = self
            .tombstone_high_watermarks
            .get(&(key.tenant, mh))
            .map(|entry| *entry.value())
        {
            idx.set_next_id_at_least(max_id);
        }
        self.indices.insert(key.clone(), idx.clone());
        self.model_names.insert((key.tenant, mh), key.model.clone());
        idx
    }

    fn resolve_index(&self, key: &IndexKey) -> Option<Arc<VectorIndex>> {
        if let Some(e) = self.indices.get(key) {
            return Some(e.clone());
        }
        let mh = model_hash(&key.model);
        let placeholder = IndexKey::new(key.tenant, format!("__h::{:08x}", mh));
        if let Some((_, idx)) = self.indices.remove(&placeholder) {
            self.indices.insert(key.clone(), idx.clone());
            self.model_names.insert((key.tenant, mh), key.model.clone());
            return Some(idx);
        }
        None
    }

    fn meta_row_count(&self, tenant: u32, model: &str, stop_after: usize) -> Result<usize> {
        let (lo, hi) = meta_range_bytes(tenant, model);
        let mut count = 0;
        for kv in self.meta.range(lo..hi) {
            kv.map_err(|e| Error::Storage(e.to_string()))?;
            count += 1;
            if count >= stop_after {
                break;
            }
        }
        Ok(count)
    }

    fn exact_search_from_meta<'a>(
        &self,
        tenant: u32,
        model: &str,
        query: &[f32],
        k: usize,
        filter: Option<&HitFilter<'a>>,
    ) -> Result<Vec<ScoredHit>> {
        let (lo, hi) = meta_range_bytes(tenant, model);
        let mut scored = Vec::new();
        for kv in self.meta.range(lo..hi) {
            let (key, value) = kv.map_err(|e| Error::Storage(e.to_string()))?;
            let Some(node_id) = node_id_from_meta_key(&key) else {
                continue;
            };
            if let Some(f) = filter {
                if !f(node_id) {
                    continue;
                }
            }
            let Some((internal_id, vector)) = decode_meta_value(&value) else {
                continue;
            };
            if vector.len() != query.len() {
                continue;
            }
            let tomb_key = tomb_key_bytes(tenant, model, internal_id);
            if self
                .tomb
                .get(&tomb_key)
                .map_err(|e| Error::Storage(e.to_string()))?
                .is_some()
            {
                continue;
            }
            let distance = cosine_distance(query, &vector);
            let score = (1.0 - distance / 2.0).clamp(0.0, 1.0);
            scored.push(ScoredHit {
                node_id,
                score,
                distance,
            });
        }
        scored.sort_by(|a, b| {
            a.distance
                .partial_cmp(&b.distance)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        scored.truncate(k);
        Ok(scored)
    }
}

/// Helper: atomically allocate a fresh internal_id from the index's counter
/// without inserting yet. We rebuild the counter using `set_next_id_at_least`
/// after each call so concurrent callers always get distinct ids.
fn id_alloc(idx: &VectorIndex) -> u64 {
    // Trick: use a sentinel zero-len vector? No — insert_with_id requires a
    // real graph entry. Instead, pull from a fresh `insert` slot by leveraging
    // the existing atomic via a public helper. We add one here using the
    // single-shot path: insert a placeholder vector? That would pollute the
    // graph. The cleanest fix is to expose the counter; see VectorIndex.
    use std::sync::atomic::Ordering;
    idx.next_internal_id_load_and_inc(Ordering::SeqCst)
}

fn cosine_distance(a: &[f32], b: &[f32]) -> f32 {
    let mut dot = 0.0_f64;
    let mut na = 0.0_f64;
    let mut nb = 0.0_f64;
    for (x, y) in a.iter().zip(b.iter()) {
        dot += (*x as f64) * (*y as f64);
        na += (*x as f64) * (*x as f64);
        nb += (*y as f64) * (*y as f64);
    }
    if na == 0.0 || nb == 0.0 {
        return 0.0;
    }
    (1.0 - dot / (na * nb).sqrt()).max(0.0) as f32
}
