//! Per-(tenant, model) HNSW index instance with soft-delete tombstones.
use hnsw_rs::api::AnnT;
use hnsw_rs::hnswio::{HnswIo, ReloadOptions};
use hnsw_rs::prelude::{DistCosine, Hnsw};
use mmdb_core::{Error, Result};
use parking_lot::RwLock;
use roaring::RoaringBitmap;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};

pub const INDEX_DEFAULT_M: usize = 16;
pub const INDEX_DEFAULT_EF_CONSTRUCTION: usize = 200;
pub const INDEX_DEFAULT_MAX_ELEMENTS: usize = 1_000_000;
pub const INDEX_DEFAULT_MAX_LAYER: usize = 16;

#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub struct IndexKey {
    pub tenant: u32,
    pub model: String,
}

impl IndexKey {
    pub fn new(tenant: u32, model: impl Into<String>) -> Self {
        Self {
            tenant,
            model: model.into(),
        }
    }
}

pub struct VectorIndex {
    pub dim: u32,
    inner: Hnsw<'static, f32, DistCosine>,
    next_internal_id: AtomicU64,
    tombstones: RwLock<RoaringBitmap>,
    dirty: parking_lot::Mutex<bool>,
}

impl VectorIndex {
    pub fn new(dim: u32) -> Self {
        let inner = Hnsw::<f32, DistCosine>::new(
            INDEX_DEFAULT_M,
            INDEX_DEFAULT_MAX_ELEMENTS,
            INDEX_DEFAULT_MAX_LAYER,
            INDEX_DEFAULT_EF_CONSTRUCTION,
            DistCosine,
        );
        Self {
            dim,
            inner,
            next_internal_id: AtomicU64::new(1),
            tombstones: RwLock::new(RoaringBitmap::new()),
            dirty: parking_lot::Mutex::new(false),
        }
    }

    pub fn load_snapshot_from_dir(dim: u32, dir: impl AsRef<Path>, basename: &str) -> Result<Self> {
        let mut loader =
            HnswIo::new_with_options(dir.as_ref(), basename, ReloadOptions::new(false));
        let loaded: Hnsw<'_, f32, DistCosine> = loader
            .load_hnsw::<f32, DistCosine>()
            .map_err(|e| Error::Storage(e.to_string()))?;

        // SAFETY: ReloadOptions::new(false) disables mmap, so hnsw_rs reloads
        // each point into owned Vec<f32> storage instead of borrowing DataMap
        // memory owned by HnswIo. The public load_hnsw signature still ties the
        // Hnsw lifetime to the loader for the mmap-capable path; with mmap off,
        // the returned graph has no loader-backed references.
        let inner = unsafe {
            std::mem::transmute::<Hnsw<'_, f32, DistCosine>, Hnsw<'static, f32, DistCosine>>(loaded)
        };

        Ok(Self {
            dim,
            inner,
            next_internal_id: AtomicU64::new(1),
            tombstones: RwLock::new(RoaringBitmap::new()),
            dirty: parking_lot::Mutex::new(false),
        })
    }

    /// Returns the internal id assigned to this insertion.
    pub fn insert(&self, vector: &[f32]) -> u64 {
        let id = self.next_internal_id.fetch_add(1, Ordering::SeqCst);
        self.inner.insert((vector, id as usize));
        *self.dirty.lock() = true;
        id
    }

    /// Insert with an externally-supplied internal id. Used by
    /// `VectorStore::open` to rebuild the in-memory graph from persisted
    /// metadata after a restart. The caller must guarantee uniqueness.
    pub fn insert_with_id(&self, vector: &[f32], internal_id: u64) {
        self.inner.insert((vector, internal_id as usize));
        // do not mark dirty: rebuilding from disk is not a new mutation
    }

    /// After bulk rebuild, advance the id counter past every id we just
    /// reinserted, so future `insert()` calls don't collide.
    pub fn set_next_id_at_least(&self, candidate: u64) {
        let mut cur = self.next_internal_id.load(Ordering::SeqCst);
        while candidate >= cur {
            match self.next_internal_id.compare_exchange(
                cur,
                candidate + 1,
                Ordering::SeqCst,
                Ordering::SeqCst,
            ) {
                Ok(_) => break,
                Err(observed) => cur = observed,
            }
        }
    }

    /// Replace tombstone bitmap wholesale (used during open-time rebuild).
    pub fn load_tombstones(&self, bm: RoaringBitmap) {
        *self.tombstones.write() = bm;
    }

    /// Read a snapshot of the current tombstone bitmap.
    pub fn tombstone_snapshot(&self) -> RoaringBitmap {
        self.tombstones.read().clone()
    }

    pub fn mark_deleted(&self, internal_id: u64) {
        let mut g = self.tombstones.write();
        g.insert(internal_id as u32);
        *self.dirty.lock() = true;
    }

    pub fn is_tombstoned(&self, internal_id: u64) -> bool {
        self.tombstones.read().contains(internal_id as u32)
    }

    /// Batch insert. Each entry is `(vector_slice, assigned_internal_id)`.
    /// IDs are caller-allocated so the storage layer can persist mapping
    /// in the same fjall batch.
    pub fn insert_batch(&self, items: &[(Vec<f32>, u64)]) {
        let refs: Vec<(&Vec<f32>, usize)> = items.iter().map(|(v, id)| (v, *id as usize)).collect();
        self.inner.parallel_insert(&refs);
        *self.dirty.lock() = true;
    }

    /// Returns (internal_id, distance) pairs, with tombstoned entries filtered.
    pub fn search(&self, query: &[f32], k: usize, ef: usize) -> Vec<(u64, f32)> {
        let raw = self.inner.search(query, k * 2, ef.max(k));
        let mut out = Vec::with_capacity(k);
        for n in raw {
            let id = n.d_id as u64;
            if self.is_tombstoned(id) {
                continue;
            }
            out.push((id, n.distance));
            if out.len() >= k {
                break;
            }
        }
        out
    }

    pub fn point_count(&self) -> usize {
        self.inner.get_nb_point()
    }

    /// Reserve and return a fresh internal id without inserting into the
    /// HNSW graph. Used by `VectorStore::insert_batch` when it wants to
    /// persist mapping and bulk-insert in one shot.
    pub fn next_internal_id_load_and_inc(&self, ord: Ordering) -> u64 {
        self.next_internal_id.fetch_add(1, ord)
    }

    pub fn is_dirty(&self) -> bool {
        *self.dirty.lock()
    }

    pub fn clear_dirty(&self) {
        *self.dirty.lock() = false;
    }

    /// Dump the native HNSW graph/data files to `dir` using `basename`.
    ///
    /// This is the first half of the cold-start acceleration path. The
    /// persisted vector metadata remains the source of truth; callers can use
    /// this snapshot as an optimization when a compatible reload path is
    /// available.
    pub fn snapshot_to_dir(&self, dir: impl AsRef<Path>, basename: &str) -> Result<String> {
        std::fs::create_dir_all(dir.as_ref())?;
        let dumped = self
            .inner
            .file_dump(dir.as_ref(), basename)
            .map_err(|e| Error::Storage(e.to_string()))?;
        self.clear_dirty();
        Ok(dumped)
    }

    /// Load a native HNSW dump for the duration of this search and query it.
    ///
    /// `hnsw_rs` ties the loaded graph lifetime to `HnswIo`, so the safe API
    /// keeps the loader inside this function. `VectorStore::open` can still use
    /// metadata rebuild as its source-of-truth fallback while callers validate
    /// snapshot compatibility without self-referential ownership.
    pub fn search_snapshot(
        dir: impl AsRef<Path>,
        basename: &str,
        query: &[f32],
        k: usize,
        ef: usize,
    ) -> Result<Vec<(u64, f32)>> {
        let mut loader = HnswIo::new(dir.as_ref(), basename);
        let hnsw: Hnsw<'_, f32, DistCosine> = loader
            .load_hnsw::<f32, DistCosine>()
            .map_err(|e| Error::Storage(e.to_string()))?;
        Ok(hnsw
            .search(query, k, ef.max(k))
            .into_iter()
            .map(|hit| (hit.d_id as u64, hit.distance))
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn snapshot_writes_hnsw_dump_files() {
        let dir = tempdir().unwrap();
        let idx = VectorIndex::new(3);
        idx.insert(&[1.0, 0.0, 0.0]);

        let basename = idx.snapshot_to_dir(dir.path(), "tenant0-model").unwrap();

        assert!(dir.path().join(format!("{basename}.hnsw.graph")).exists());
        assert!(dir.path().join(format!("{basename}.hnsw.data")).exists());
        assert!(!idx.is_dirty());
    }

    #[test]
    fn snapshot_reload_searches_dump_without_rebuild() {
        let dir = tempdir().unwrap();
        let idx = VectorIndex::new(3);
        let id = idx.insert(&[1.0, 0.0, 0.0]);
        let basename = idx.snapshot_to_dir(dir.path(), "tenant0-model").unwrap();

        let hits =
            VectorIndex::search_snapshot(dir.path(), &basename, &[1.0, 0.0, 0.0], 1, 32).unwrap();

        assert_eq!(hits[0].0, id);
    }
}
