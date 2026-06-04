//! Per-(tenant, model) HNSW index instance with soft-delete tombstones.
use hnsw_rs::prelude::{DistCosine, Hnsw};
use parking_lot::RwLock;
use roaring::RoaringBitmap;
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
        Self { tenant, model: model.into() }
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

    /// Returns the internal id assigned to this insertion.
    pub fn insert(&self, vector: &[f32]) -> u64 {
        let id = self.next_internal_id.fetch_add(1, Ordering::SeqCst);
        self.inner.insert((vector, id as usize));
        *self.dirty.lock() = true;
        id
    }

    pub fn mark_deleted(&self, internal_id: u64) {
        let mut g = self.tombstones.write();
        g.insert(internal_id as u32);
        *self.dirty.lock() = true;
    }

    pub fn is_tombstoned(&self, internal_id: u64) -> bool {
        self.tombstones.read().contains(internal_id as u32)
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

    pub fn is_dirty(&self) -> bool {
        *self.dirty.lock()
    }

    pub fn clear_dirty(&self) {
        *self.dirty.lock() = false;
    }
}
