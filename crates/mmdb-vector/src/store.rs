//! VectorStore — top-level facade that owns multiple per-(tenant, model) indices.
//!
//! Persistence model (P1 + P1.5):
//! - `vector_meta`  key=[tenant|mh|ulid]            val=[internal_id_be(8)|dim_be(4)|f32 vector]
//! - `vector_rev`   key=[tenant|mh|internal_id]     val=ulid bytes
//! - `vector_tomb`  key=[tenant|mh|internal_id]     val=[]   (presence = tombstoned)
//!
//! `vector_meta` remains the source of truth. `flush_snapshots()` can persist
//! native HNSW dump files plus a small manifest; `open()` uses those files as a
//! cold-start optimization only when the manifest covers all live and tombstoned
//! internal ids, then replays `vector_tomb` to restore soft-delete state.
use crate::{IndexKey, ScoredHit, VectorIndex};
use dashmap::DashMap;
use fjall::{Keyspace, PartitionCreateOptions, PartitionHandle, PersistMode};
use mmdb_core::{Error, Result};
use roaring::RoaringBitmap;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::path::{Path, PathBuf};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use ulid::Ulid;

const PART_META: &str = "vector_meta";
const PART_REV: &str = "vector_rev";
const PART_TOMB: &str = "vector_tomb";
const EXACT_SEARCH_MAX_ROWS: usize = 1_024;
const SNAPSHOT_DIR_NAME: &str = "vector_hnsw_snapshots";
const SNAPSHOT_MANIFEST_VERSION: u32 = 1;

pub struct VectorStore {
    keyspace: Keyspace,
    meta: PartitionHandle,
    rev: PartitionHandle,
    tomb: PartitionHandle,
    snapshot_dir: PathBuf,
    indices: DashMap<IndexKey, Arc<VectorIndex>>,
    snapshot_reloads: AtomicUsize,
    /// model_hash -> original model name, populated on open() so rebuilt
    /// indices can be re-keyed. New inserts also register here.
    model_names: DashMap<(u32, u32), String>,
    tombstone_high_watermarks: DashMap<(u32, u32), u64>,
}

/// Predicate passed to `search_with_filter`. Receives the raw `Ulid` of each
/// candidate; return `true` to keep it.
pub type HitFilter<'a> = dyn Fn(Ulid) -> bool + Send + Sync + 'a;

type RebuildGroup = (u32, Vec<(Vec<f32>, u64)>, u64);

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SnapshotManifest {
    version: u32,
    tenant: u32,
    model_hash: u32,
    dim: u32,
    basename: String,
    max_internal_id: u64,
    point_count: usize,
}

impl VectorStore {
    pub fn open(keyspace: Keyspace) -> Result<Self> {
        Self::open_with_snapshot_dir(keyspace, None::<PathBuf>)
    }

    pub fn open_with_snapshot_dir(
        keyspace: Keyspace,
        snapshot_dir: impl Into<Option<PathBuf>>,
    ) -> Result<Self> {
        let meta = keyspace
            .open_partition(PART_META, PartitionCreateOptions::default())
            .map_err(|e| Error::Storage(e.to_string()))?;
        let rev = keyspace
            .open_partition(PART_REV, PartitionCreateOptions::default())
            .map_err(|e| Error::Storage(e.to_string()))?;
        let tomb = keyspace
            .open_partition(PART_TOMB, PartitionCreateOptions::default())
            .map_err(|e| Error::Storage(e.to_string()))?;
        let snapshot_dir = snapshot_dir
            .into()
            .unwrap_or_else(|| default_snapshot_dir(&meta));

        let s = Self {
            keyspace,
            meta,
            rev,
            tomb,
            snapshot_dir,
            indices: DashMap::new(),
            snapshot_reloads: AtomicUsize::new(0),
            model_names: DashMap::new(),
            tombstone_high_watermarks: DashMap::new(),
        };
        s.rebuild()?;
        Ok(s)
    }

    /// Scan persisted meta + tomb partitions and rebuild every (tenant, model)
    /// index that has at least one live row.
    fn rebuild(&self) -> Result<()> {
        // Group meta rows by (tenant, model_hash) so we can do bulk inserts.
        let mut grouped: HashMap<(u32, u32), RebuildGroup> = HashMap::new();
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

            let entry = grouped.entry((tenant, mh)).or_insert((dim, Vec::new(), 0));
            // sanity: dim must match within a group
            if entry.0 != dim {
                continue;
            }
            entry.1.push((vec, internal_id));
            if internal_id > entry.2 {
                entry.2 = internal_id;
            }
        }

        let tomb_groups = self.load_tombstone_groups()?;
        for (&key, bm) in &tomb_groups {
            if let Some(max_id) = bm.iter().max().map(u64::from) {
                self.record_tombstone_high_watermark(key, max_id);
            }
        }

        // Rebuild each index. Note: model NAME is lost on disk (we only stored
        // its hash), so we discover names lazily — when a future insert/search
        // arrives with the same hash, it registers the name. Until then the
        // rebuilt index lives keyed by `__rebuilt::<hash>` so search via an
        // unknown model still works as soon as the caller provides the name.
        for ((tenant, mh), (dim, items, max_id)) in grouped {
            let tomb_max_id = tomb_groups
                .get(&(tenant, mh))
                .and_then(|bm| bm.iter().max())
                .map(u64::from)
                .unwrap_or(0);
            let covered_max_id = max_id.max(tomb_max_id);
            let idx = if let Some(idx) =
                self.load_snapshot_index(tenant, mh, dim, covered_max_id, items.len())?
            {
                self.snapshot_reloads.fetch_add(1, Ordering::SeqCst);
                Arc::new(idx)
            } else {
                let idx = Arc::new(VectorIndex::new(dim));
                if !items.is_empty() {
                    idx.insert_batch(&items);
                }
                idx.set_next_id_at_least(covered_max_id);
                idx
            };
            // Bookkeeping: store under a placeholder model name keyed by hash.
            // When the first insert/search with the real name arrives we will
            // alias both keys (see `get_or_create_index`).
            let placeholder = format!("__h::{:08x}", mh);
            self.indices.insert(IndexKey::new(tenant, placeholder), idx);
        }

        // Replay tombstones into whichever index already exists.
        for ((tenant, mh), bm) in tomb_groups {
            let placeholder = format!("__h::{:08x}", mh);
            if let Some(idx) = self.indices.get(&IndexKey::new(tenant, placeholder)) {
                idx.load_tombstones(bm);
            }
        }

        Ok(())
    }

    fn load_tombstone_groups(&self) -> Result<HashMap<(u32, u32), RoaringBitmap>> {
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
        Ok(tomb_groups)
    }

    fn load_snapshot_index(
        &self,
        tenant: u32,
        mh: u32,
        dim: u32,
        covered_max_id: u64,
        live_count: usize,
    ) -> Result<Option<VectorIndex>> {
        let Some(manifest) = self.read_snapshot_manifest(tenant, mh)? else {
            return Ok(None);
        };
        if manifest.version != SNAPSHOT_MANIFEST_VERSION
            || manifest.tenant != tenant
            || manifest.model_hash != mh
            || manifest.dim != dim
            || manifest.max_internal_id < covered_max_id
            || manifest.point_count < live_count
        {
            return Ok(None);
        }
        if !self
            .snapshot_dir
            .join(format!("{}.hnsw.graph", manifest.basename))
            .exists()
            || !self
                .snapshot_dir
                .join(format!("{}.hnsw.data", manifest.basename))
                .exists()
        {
            return Ok(None);
        }

        let idx = match catch_unwind(AssertUnwindSafe(|| {
            VectorIndex::load_snapshot_from_dir(dim, &self.snapshot_dir, &manifest.basename)
        })) {
            Ok(Ok(idx)) => idx,
            Ok(Err(_)) | Err(_) => return Ok(None),
        };
        if idx.point_count() != manifest.point_count {
            return Ok(None);
        }
        idx.set_next_id_at_least(covered_max_id);
        Ok(Some(idx))
    }

    fn read_snapshot_manifest(&self, tenant: u32, mh: u32) -> Result<Option<SnapshotManifest>> {
        let path = manifest_path(&self.snapshot_dir, tenant, mh);
        match std::fs::read(&path) {
            Ok(bytes) => serde_json::from_slice(&bytes)
                .map(Some)
                .map_err(|e| Error::Storage(e.to_string())),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(err) => Err(Error::Storage(err.to_string())),
        }
    }

    fn write_snapshot_manifest(&self, manifest: &SnapshotManifest) -> Result<()> {
        std::fs::create_dir_all(&self.snapshot_dir)?;
        let path = manifest_path(&self.snapshot_dir, manifest.tenant, manifest.model_hash);
        let tmp = path.with_extension("manifest.json.tmp");
        let bytes =
            serde_json::to_vec_pretty(manifest).map_err(|e| Error::Storage(e.to_string()))?;
        std::fs::write(&tmp, bytes)?;
        std::fs::rename(tmp, path)?;
        Ok(())
    }

    pub fn flush_snapshots(&self) -> Result<usize> {
        std::fs::create_dir_all(&self.snapshot_dir)?;
        let mut written = 0;
        for entry in self.indices.iter() {
            let key = entry.key();
            let idx = entry.value();
            let mh = model_hash_from_index_key(key);
            if idx.point_count() == 0 {
                continue;
            }

            let manifest_path = manifest_path(&self.snapshot_dir, key.tenant, mh);
            if !idx.is_dirty() && manifest_path.exists() {
                continue;
            }

            let max_internal_id = self.max_internal_id_for_hash(key.tenant, mh)?;
            let basename =
                idx.snapshot_to_dir(&self.snapshot_dir, &snapshot_basename(key.tenant, mh))?;
            let manifest = SnapshotManifest {
                version: SNAPSHOT_MANIFEST_VERSION,
                tenant: key.tenant,
                model_hash: mh,
                dim: idx.dim,
                basename,
                max_internal_id,
                point_count: idx.point_count(),
            };
            self.write_snapshot_manifest(&manifest)?;
            written += 1;
        }
        Ok(written)
    }

    pub fn snapshot_reload_count(&self) -> usize {
        self.snapshot_reloads.load(Ordering::SeqCst)
    }

    fn max_internal_id_for_hash(&self, tenant: u32, mh: u32) -> Result<u64> {
        let mut max_internal_id = 0;
        let (lo, hi) = meta_range_bytes_for_hash(tenant, mh);
        for kv in self.meta.range(lo..hi) {
            let (_, value) = kv.map_err(|e| Error::Storage(e.to_string()))?;
            if value.len() < 8 {
                continue;
            }
            let internal_id = u64::from_be_bytes(value[0..8].try_into().unwrap());
            max_internal_id = max_internal_id.max(internal_id);
        }

        let (lo, hi) = tomb_range_bytes_for_hash(tenant, mh);
        for kv in self.tomb.range(lo..hi) {
            let (key, _) = kv.map_err(|e| Error::Storage(e.to_string()))?;
            if key.len() != 4 + 4 + 8 {
                continue;
            }
            let internal_id = u64::from_be_bytes(key[8..16].try_into().unwrap());
            max_internal_id = max_internal_id.max(internal_id);
        }
        Ok(max_internal_id)
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
    pub fn insert_batch(&self, tenant: u32, model: &str, items: &[(Ulid, Vec<f32>)]) -> Result<()> {
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

    fn record_tombstone_high_watermark(&self, key: (u32, u32), internal_id: u64) {
        self.tombstone_high_watermarks
            .entry(key)
            .and_modify(|max_id| *max_id = (*max_id).max(internal_id))
            .or_insert(internal_id);
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

fn model_hash(model: &str) -> u32 {
    let mut h: u32 = 0x811C9DC5;
    for b in model.as_bytes() {
        h ^= *b as u32;
        h = h.wrapping_mul(0x0100_0193);
    }
    h
}

fn model_hash_from_index_key(key: &IndexKey) -> u32 {
    key.model
        .strip_prefix("__h::")
        .and_then(|hex| u32::from_str_radix(hex, 16).ok())
        .unwrap_or_else(|| model_hash(&key.model))
}

fn default_snapshot_dir(meta: &PartitionHandle) -> PathBuf {
    meta.path()
        .parent()
        .and_then(Path::parent)
        .unwrap_or_else(|| meta.path())
        .join(SNAPSHOT_DIR_NAME)
}

fn snapshot_basename(tenant: u32, mh: u32) -> String {
    format!("tenant{tenant}-model{mh:08x}")
}

fn manifest_path(dir: &Path, tenant: u32, mh: u32) -> PathBuf {
    dir.join(format!("{}.manifest.json", snapshot_basename(tenant, mh)))
}

fn meta_key_bytes(tenant: u32, model: &str, node_id: Ulid) -> Vec<u8> {
    meta_key_bytes_for_hash(tenant, model_hash(model), node_id)
}

fn meta_key_bytes_for_hash(tenant: u32, mh: u32, node_id: Ulid) -> Vec<u8> {
    let mut k = Vec::with_capacity(4 + 4 + 16);
    k.extend_from_slice(&tenant.to_be_bytes());
    k.extend_from_slice(&mh.to_be_bytes());
    k.extend_from_slice(&node_id.0.to_be_bytes());
    k
}

fn meta_range_bytes(tenant: u32, model: &str) -> (Vec<u8>, Vec<u8>) {
    meta_range_bytes_for_hash(tenant, model_hash(model))
}

fn meta_range_bytes_for_hash(tenant: u32, mh: u32) -> (Vec<u8>, Vec<u8>) {
    let mut lo = Vec::with_capacity(4 + 4);
    lo.extend_from_slice(&tenant.to_be_bytes());
    lo.extend_from_slice(&mh.to_be_bytes());
    let mut hi = lo.clone();
    hi.extend_from_slice(&[0xff; 16]);
    (lo, hi)
}

fn node_id_from_meta_key(key: &[u8]) -> Option<Ulid> {
    if key.len() != 4 + 4 + 16 {
        return None;
    }
    let mut buf = [0u8; 16];
    buf.copy_from_slice(&key[8..24]);
    Some(Ulid(u128::from_be_bytes(buf)))
}

fn decode_meta_value(value: &[u8]) -> Option<(u64, Vec<f32>)> {
    if value.len() < 12 {
        return None;
    }
    let internal_id = u64::from_be_bytes(value[0..8].try_into().ok()?);
    let dim = u32::from_be_bytes(value[8..12].try_into().ok()?);
    let expected = 12 + dim as usize * 4;
    if value.len() != expected {
        return None;
    }
    let vector = value[12..]
        .chunks_exact(4)
        .map(|chunk| Some(f32::from_le_bytes(chunk.try_into().ok()?)))
        .collect::<Option<Vec<_>>>()?;
    Some((internal_id, vector))
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

fn rev_key_bytes(tenant: u32, model: &str, internal_id: u64) -> Vec<u8> {
    rev_key_bytes_for_hash(tenant, model_hash(model), internal_id)
}

fn rev_key_bytes_for_hash(tenant: u32, mh: u32, internal_id: u64) -> Vec<u8> {
    let mut k = Vec::with_capacity(4 + 4 + 8);
    k.extend_from_slice(&tenant.to_be_bytes());
    k.extend_from_slice(&mh.to_be_bytes());
    k.extend_from_slice(&internal_id.to_be_bytes());
    k
}

fn tomb_key_bytes(tenant: u32, model: &str, internal_id: u64) -> Vec<u8> {
    rev_key_bytes(tenant, model, internal_id)
}

fn tomb_range_bytes_for_hash(tenant: u32, mh: u32) -> (Vec<u8>, Vec<u8>) {
    let mut lo = Vec::with_capacity(4 + 4);
    lo.extend_from_slice(&tenant.to_be_bytes());
    lo.extend_from_slice(&mh.to_be_bytes());
    let mut hi = lo.clone();
    hi.extend_from_slice(&[0xff; 8]);
    (lo, hi)
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
        s.insert(0, "m", id1, &norm(vec![1.0, 0.0, 0.0, 0.0]))
            .unwrap();
        s.insert(0, "m", id2, &norm(vec![0.0, 1.0, 0.0, 0.0]))
            .unwrap();
        let hits = s
            .search(0, "m", &norm(vec![1.0, 0.0, 0.0, 0.0]), 1)
            .unwrap();
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
        s.insert(0, "m", near, &norm(vec![1.0, 0.01, 0.0, 0.0]))
            .unwrap();
        s.insert(0, "m", far, &norm(vec![0.0, 0.0, 1.0, 0.0]))
            .unwrap();
        let hits = s
            .search(0, "m", &norm(vec![1.0, 0.0, 0.0, 0.0]), 2)
            .unwrap();
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
            s.insert(0, "m", id1, &norm(vec![1.0, 0.0, 0.0, 0.0]))
                .unwrap();
            s.insert(0, "m", id2, &norm(vec![0.0, 1.0, 0.0, 0.0]))
                .unwrap();
        }
        // Reopen: HNSW graph must be rebuilt from meta.
        let s = make_store_at(dir.path());
        let hits = s
            .search(0, "m", &norm(vec![1.0, 0.0, 0.0, 0.0]), 1)
            .unwrap();
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].node_id, id1);
    }

    #[test]
    fn open_reloads_checkpointed_hnsw_snapshot() {
        let dir = tempdir().unwrap();
        let id1 = Ulid::new();
        let id2 = Ulid::new();
        {
            let s = make_store_at(dir.path());
            s.insert(0, "m", id1, &norm(vec![1.0, 0.0, 0.0, 0.0]))
                .unwrap();
            s.insert(0, "m", id2, &norm(vec![0.0, 1.0, 0.0, 0.0]))
                .unwrap();
            s.flush_snapshots().unwrap();
        }

        let s = make_store_at(dir.path());

        assert_eq!(s.snapshot_reload_count(), 1);
        let hits = s
            .search(0, "m", &norm(vec![1.0, 0.0, 0.0, 0.0]), 1)
            .unwrap();
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].node_id, id1);
    }

    #[test]
    fn open_falls_back_to_rebuild_when_hnsw_snapshot_is_corrupt() {
        let dir = tempdir().unwrap();
        let id = Ulid::new();
        let snapshot_dir = {
            let s = make_store_at(dir.path());
            s.insert(0, "m", id, &norm(vec![1.0, 0.0, 0.0, 0.0]))
                .unwrap();
            assert_eq!(s.flush_snapshots().unwrap(), 1);
            s.snapshot_dir.clone()
        };

        let mh = model_hash("m");
        let manifest: SnapshotManifest =
            serde_json::from_slice(&std::fs::read(manifest_path(&snapshot_dir, 0, mh)).unwrap())
                .unwrap();
        std::fs::write(
            snapshot_dir.join(format!("{}.hnsw.graph", manifest.basename)),
            b"not a valid graph",
        )
        .unwrap();

        let s = make_store_at(dir.path());

        assert_eq!(s.snapshot_reload_count(), 0);
        let hits = s
            .search(0, "m", &norm(vec![1.0, 0.0, 0.0, 0.0]), 1)
            .unwrap();
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].node_id, id);
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
    fn insert_after_all_vectors_deleted_and_reopened_is_searchable() {
        let dir = tempdir().unwrap();
        let deleted = Ulid::new();
        {
            let s = make_store_at(dir.path());
            s.insert(0, "m", deleted, &norm(vec![1.0, 0.0, 0.0]))
                .unwrap();
            s.delete(0, "m", deleted).unwrap();
        }

        let s = make_store_at(dir.path());
        let fresh = Ulid::new();
        s.insert(0, "m", fresh, &norm(vec![1.0, 0.0, 0.0])).unwrap();

        let hits = s.search(0, "m", &norm(vec![1.0, 0.0, 0.0]), 1).unwrap();
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].node_id, fresh);
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
        let hits = s
            .search(
                0,
                "m",
                &norm(vec![1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0]),
                3,
            )
            .unwrap();
        assert!(!hits.is_empty());
    }

    #[test]
    fn search_with_filter_keeps_only_matching() {
        let dir = tempdir().unwrap();
        let s = make_store_at(dir.path());
        let keep = Ulid::new();
        let drop = Ulid::new();
        s.insert(0, "m", keep, &norm(vec![1.0, 0.0, 0.0, 0.0]))
            .unwrap();
        s.insert(0, "m", drop, &norm(vec![0.99, 0.01, 0.0, 0.0]))
            .unwrap();
        let f: Box<HitFilter> = Box::new(move |u| u == keep);
        let hits = s
            .search_with_filter(0, "m", &norm(vec![1.0, 0.0, 0.0, 0.0]), 5, Some(&*f))
            .unwrap();
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].node_id, keep);
    }
}
