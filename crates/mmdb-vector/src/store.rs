//! VectorStore — top-level facade that owns multiple per-(tenant, model) indices.
//!
//! Persistence model (P1 + P1.5):
//! - `vector_meta`  key=[tenant|mh|ulid]            val=[internal_id_be(8)|dim_be(4)|f32 vector]
//! - `vector_rev`   key=[tenant|mh|internal_id]     val=ulid bytes
//! - `vector_tomb`  key=[tenant|mh|internal_id]     val=[]   (presence = tombstoned)
//!
//! On `open()` we scan `vector_meta` per (tenant, model) prefix and rebuild
//! the in-memory HNSW graph via `insert_batch`; then we replay `vector_tomb`
//! to restore soft-delete state. This means the on-disk format is the source
//! of truth — the graph itself is never persisted.
use crate::{IndexKey, ScoredHit, VectorIndex};
use dashmap::DashMap;
use fjall::{Keyspace, PartitionCreateOptions, PartitionHandle, PersistMode};
use mmdb_core::{Error, Result};
use roaring::RoaringBitmap;
use std::collections::HashMap;
use std::sync::Arc;
use ulid::Ulid;

const PART_META: &str = "vector_meta";
const PART_REV: &str = "vector_rev";
const PART_TOMB: &str = "vector_tomb";

pub struct VectorStore {
    keyspace: Keyspace,
    meta: PartitionHandle,
    rev: PartitionHandle,
    tomb: PartitionHandle,
    indices: DashMap<IndexKey, Arc<VectorIndex>>,
    /// model_hash -> original model name, populated on open() so rebuilt
    /// indices can be re-keyed. New inserts also register here.
    model_names: DashMap<(u32, u32), String>,
}

/// Predicate passed to `search_with_filter`. Receives the raw `Ulid` of each
/// candidate; return `true` to keep it.
pub type HitFilter<'a> = dyn Fn(Ulid) -> bool + Send + Sync + 'a;

impl VectorStore {
    pub fn open(keyspace: Keyspace) -> Result<Self> {
        let meta = keyspace
            .open_partition(PART_META, PartitionCreateOptions::default())
            .map_err(|e| Error::Storage(e.to_string()))?;
        let rev = keyspace
            .open_partition(PART_REV, PartitionCreateOptions::default())
            .map_err(|e| Error::Storage(e.to_string()))?;
        let tomb = keyspace
            .open_partition(PART_TOMB, PartitionCreateOptions::default())
            .map_err(|e| Error::Storage(e.to_string()))?;

        let s = Self {
            keyspace,
            meta,
            rev,
            tomb,
            indices: DashMap::new(),
            model_names: DashMap::new(),
        };
        s.rebuild()?;
        Ok(s)
    }

    /// Scan persisted meta + tomb partitions and rebuild every (tenant, model)
    /// index that has at least one live row.
    fn rebuild(&self) -> Result<()> {
        // Group meta rows by (tenant, model_hash) so we can do bulk inserts.
        let mut grouped: HashMap<(u32, u32), (u32, Vec<(Vec<f32>, u64)>, u64)> = HashMap::new();
        //                                  dim, items,                 max_id

        for kv in self.meta.iter() {
            let (k, v) = kv.map_err(|e| Error::Storage(e.to_string()))?;
            if k.len() != 4 + 4 + 16 || v.len() < 8 + 4 {
                continue;
            }
            let tenant = u32::from_be_bytes(k[0..4].try_into().unwrap());
            let mh = u32::from_be_bytes(k[4..8].try_into().unwrap());

            let internal_id = u64::from_be_bytes(v[0..8].try_into().unwrap());
            let dim = u32::from_be_bytes(v[8..12].try_into().unwrap());
            let expected = 12 + (dim as usize) * 4;
            if v.len() != expected {
                continue;
            }
            let mut vec = Vec::with_capacity(dim as usize);
            for chunk in v[12..].chunks_exact(4) {
                vec.push(f32::from_le_bytes(chunk.try_into().unwrap()));
            }

            let entry = grouped
                .entry((tenant, mh))
                .or_insert((dim, Vec::new(), 0));
            // sanity: dim must match within a group
            if entry.0 != dim {
                continue;
            }
            entry.1.push((vec, internal_id));
            if internal_id > entry.2 {
                entry.2 = internal_id;
            }
        }

        // Rebuild each index. Note: model NAME is lost on disk (we only stored
        // its hash), so we discover names lazily — when a future insert/search
        // arrives with the same hash, it registers the name. Until then the
        // rebuilt index lives keyed by `__rebuilt::<hash>` so search via an
        // unknown model still works as soon as the caller provides the name.
        for ((tenant, mh), (dim, items, max_id)) in grouped {
            let idx = Arc::new(VectorIndex::new(dim));
            if !items.is_empty() {
                idx.insert_batch(&items);
            }
            idx.set_next_id_at_least(max_id);
            // Bookkeeping: store under a placeholder model name keyed by hash.
            // When the first insert/search with the real name arrives we will
            // alias both keys (see `get_or_create_index`).
            let placeholder = format!("__h::{:08x}", mh);
            self.indices
                .insert(IndexKey::new(tenant, placeholder), idx);
        }

        // Replay tombstones into whichever index already exists.
        let mut tomb_groups: HashMap<(u32, u32), RoaringBitmap> = HashMap::new();
        for kv in self.tomb.iter() {
            let (k, _) = kv.map_err(|e| Error::Storage(e.to_string()))?;
            if k.len() != 4 + 4 + 8 {
                continue;
            }
            let tenant = u32::from_be_bytes(k[0..4].try_into().unwrap());
            let mh = u32::from_be_bytes(k[4..8].try_into().unwrap());
            let internal_id = u64::from_be_bytes(k[8..16].try_into().unwrap());
            tomb_groups
                .entry((tenant, mh))
                .or_default()
                .insert(internal_id as u32);
        }
        for ((tenant, mh), bm) in tomb_groups {
            let placeholder = format!("__h::{:08x}", mh);
            if let Some(idx) = self.indices.get(&IndexKey::new(tenant, placeholder)) {
                idx.load_tombstones(bm);
            }
        }

        Ok(())
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

    pub fn insert(
        &self,
        tenant: u32,
        model: &str,
        node_id: Ulid,
        vector: &[f32],
    ) -> Result<()> {
        self.insert_batch(tenant, model, &[(node_id, vector.to_vec())])
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
            // Reserve a fresh id by inserting+rewinding is wasteful; instead
            // bump the atomic directly via insert with id semantics.
            let id = {
                // Single-shot allocation using set_next_id_at_least + load
                let next = idx.tombstone_snapshot();
                let _ = next; // discard, only used for noop
                // Use insert_with_id with a freshly allocated id from the index
                // via internal counter:
                let alloc = id_alloc(&idx);
                alloc
            };
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
            out.push(ScoredHit { node_id, score, distance: dist });
            if out.len() >= k {
                break;
            }
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
        self.keyspace
            .persist(PersistMode::SyncAll)
            .map_err(|e| Error::Storage(e.to_string()))?;
        Ok(())
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

fn model_hash(model: &str) -> u32 {
    let mut h: u32 = 0x811C9DC5;
    for b in model.as_bytes() {
        h ^= *b as u32;
        h = h.wrapping_mul(0x0100_0193);
    }
    h
}

fn meta_key_bytes(tenant: u32, model: &str, node_id: Ulid) -> Vec<u8> {
    let mh = model_hash(model);
    let mut k = Vec::with_capacity(4 + 4 + 16);
    k.extend_from_slice(&tenant.to_be_bytes());
    k.extend_from_slice(&mh.to_be_bytes());
    k.extend_from_slice(&node_id.0.to_be_bytes());
    k
}

fn rev_key_bytes(tenant: u32, model: &str, internal_id: u64) -> Vec<u8> {
    let mh = model_hash(model);
    let mut k = Vec::with_capacity(4 + 4 + 8);
    k.extend_from_slice(&tenant.to_be_bytes());
    k.extend_from_slice(&mh.to_be_bytes());
    k.extend_from_slice(&internal_id.to_be_bytes());
    k
}

fn tomb_key_bytes(tenant: u32, model: &str, internal_id: u64) -> Vec<u8> {
    rev_key_bytes(tenant, model, internal_id)
}

#[cfg(test)]
mod tests {
    use super::*;
    use fjall::Config;
    use tempfile::tempdir;

    fn make_store_at(path: &std::path::Path) -> VectorStore {
        let ks = Config::new(path).open().unwrap();
        VectorStore::open(ks).unwrap()
    }

    fn norm(v: Vec<f32>) -> Vec<f32> {
        let n: f32 = v.iter().map(|x| x * x).sum::<f32>().sqrt();
        v.into_iter().map(|x| x / n).collect()
    }

    #[test]
    fn insert_then_search_returns_inserted_id() {
        let dir = tempdir().unwrap();
        let s = make_store_at(dir.path());
        let id1 = Ulid::new();
        let id2 = Ulid::new();
        s.insert(0, "m", id1, &norm(vec![1.0, 0.0, 0.0, 0.0])).unwrap();
        s.insert(0, "m", id2, &norm(vec![0.0, 1.0, 0.0, 0.0])).unwrap();
        let hits = s.search(0, "m", &norm(vec![1.0, 0.0, 0.0, 0.0]), 1).unwrap();
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].node_id, id1);
        assert!(hits[0].score > 0.99);
    }

    #[test]
    fn delete_excludes_from_search() {
        let dir = tempdir().unwrap();
        let s = make_store_at(dir.path());
        let id = Ulid::new();
        s.insert(0, "m", id, &norm(vec![1.0, 0.0, 0.0])).unwrap();
        s.delete(0, "m", id).unwrap();
        let hits = s.search(0, "m", &norm(vec![1.0, 0.0, 0.0]), 5).unwrap();
        assert!(hits.iter().all(|h| h.node_id != id));
    }

    #[test]
    fn dim_mismatch_is_rejected() {
        let dir = tempdir().unwrap();
        let s = make_store_at(dir.path());
        s.insert(0, "m", Ulid::new(), &[1.0, 0.0, 0.0]).unwrap();
        let err = s.insert(0, "m", Ulid::new(), &[1.0, 0.0]).unwrap_err();
        assert!(format!("{err}").contains("dim mismatch"));
    }

    #[test]
    fn empty_index_search_returns_empty() {
        let dir = tempdir().unwrap();
        let s = make_store_at(dir.path());
        let hits = s.search(0, "absent", &[1.0, 0.0, 0.0], 5).unwrap();
        assert!(hits.is_empty());
    }

    #[test]
    fn top_k_orders_by_similarity() {
        let dir = tempdir().unwrap();
        let s = make_store_at(dir.path());
        let near = Ulid::new();
        let far = Ulid::new();
        s.insert(0, "m", near, &norm(vec![1.0, 0.01, 0.0, 0.0])).unwrap();
        s.insert(0, "m", far,  &norm(vec![0.0, 0.0, 1.0, 0.0])).unwrap();
        let hits = s.search(0, "m", &norm(vec![1.0, 0.0, 0.0, 0.0]), 2).unwrap();
        assert_eq!(hits.len(), 2);
        assert_eq!(hits[0].node_id, near);
        assert!(hits[0].score > hits[1].score);
    }

    #[test]
    fn rebuild_after_reopen() {
        let dir = tempdir().unwrap();
        let id1 = Ulid::new();
        let id2 = Ulid::new();
        {
            let s = make_store_at(dir.path());
            s.insert(0, "m", id1, &norm(vec![1.0, 0.0, 0.0, 0.0])).unwrap();
            s.insert(0, "m", id2, &norm(vec![0.0, 1.0, 0.0, 0.0])).unwrap();
        }
        // Reopen: HNSW graph must be rebuilt from meta.
        let s = make_store_at(dir.path());
        let hits = s.search(0, "m", &norm(vec![1.0, 0.0, 0.0, 0.0]), 1).unwrap();
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].node_id, id1);
    }

    #[test]
    fn tombstones_survive_reopen() {
        let dir = tempdir().unwrap();
        let id = Ulid::new();
        {
            let s = make_store_at(dir.path());
            s.insert(0, "m", id, &norm(vec![1.0, 0.0, 0.0])).unwrap();
            s.delete(0, "m", id).unwrap();
        }
        let s = make_store_at(dir.path());
        let hits = s.search(0, "m", &norm(vec![1.0, 0.0, 0.0]), 5).unwrap();
        assert!(hits.iter().all(|h| h.node_id != id));
    }

    #[test]
    fn insert_batch_works() {
        let dir = tempdir().unwrap();
        let s = make_store_at(dir.path());
        let items: Vec<_> = (0..50)
            .map(|i| {
                let mut v = vec![0.0_f32; 8];
                v[i % 8] = 1.0;
                (Ulid::new(), norm(v))
            })
            .collect();
        s.insert_batch(0, "m", &items).unwrap();
        let hits = s.search(0, "m", &norm(vec![1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0]), 3).unwrap();
        assert!(!hits.is_empty());
    }

    #[test]
    fn search_with_filter_keeps_only_matching() {
        let dir = tempdir().unwrap();
        let s = make_store_at(dir.path());
        let keep = Ulid::new();
        let drop = Ulid::new();
        s.insert(0, "m", keep, &norm(vec![1.0, 0.0, 0.0, 0.0])).unwrap();
        s.insert(0, "m", drop, &norm(vec![0.99, 0.01, 0.0, 0.0])).unwrap();
        let f: Box<HitFilter> = Box::new(move |u| u == keep);
        let hits = s
            .search_with_filter(0, "m", &norm(vec![1.0, 0.0, 0.0, 0.0]), 5, Some(&*f))
            .unwrap();
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].node_id, keep);
    }
}
