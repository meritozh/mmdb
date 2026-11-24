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
use crate::{IndexKey, VectorIndex};
use dashmap::DashMap;
use fjall::{Database as FjallDatabase, Keyspace, KeyspaceCreateOptions};
use mmdb_core::{Error, Result};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use std::sync::{atomic::AtomicUsize, Arc};
use ulid::Ulid;

// ---- partition & tuning constants ----

pub(crate) const PART_META: &str = "vector_meta";
pub(crate) const PART_REV: &str = "vector_rev";
pub(crate) const PART_TOMB: &str = "vector_tomb";
pub(crate) const EXACT_SEARCH_MAX_ROWS: usize = 1_024;
pub(crate) const MAX_SEARCH_RESULTS: usize = 100_000;
pub(crate) const MAX_HNSW_EF: usize = 400_000;
pub(crate) const SNAPSHOT_DIR_NAME: &str = "vector_hnsw_snapshots";
pub(crate) const SNAPSHOT_MANIFEST_VERSION: u32 = 1;

// ---- submodules ----

mod keys;
mod ops;
mod snapshot;

#[cfg(test)]
mod tests;

// ---- public API types ----

/// Predicate passed to `search_with_filter`. Receives the raw `Ulid` of each
/// candidate; return `true` to keep it.
pub type HitFilter<'a> = dyn Fn(Ulid) -> bool + Send + Sync + 'a;

pub struct VectorStore {
    database: FjallDatabase,
    meta: Keyspace,
    rev: Keyspace,
    tomb: Keyspace,
    snapshot_dir: PathBuf,
    indices: DashMap<IndexKey, Arc<VectorIndex>>,
    snapshot_reloads: AtomicUsize,
    /// model_hash -> original model name, populated on open() so rebuilt
    /// indices can be re-keyed. New inserts also register here.
    model_names: DashMap<(u32, u32), String>,
    tombstone_high_watermarks: DashMap<(u32, u32), u64>,
}

pub(crate) type RebuildGroup = (u32, Vec<(Vec<f32>, u64)>, u64);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct SnapshotManifest {
    version: u32,
    tenant: u32,
    model_hash: u32,
    dim: u32,
    basename: String,
    max_internal_id: u64,
    point_count: usize,
}

impl VectorStore {
    /// Keep HNSW snapshots in `vector_hnsw_snapshots/` under the MMDB root.
    pub fn open(database: FjallDatabase, root: impl AsRef<Path>) -> Result<Self> {
        Self::open_with_snapshot_dir(database, root.as_ref().join(SNAPSHOT_DIR_NAME))
    }

    /// Read and write this store's HNSW snapshots in the supplied directory.
    pub fn open_with_snapshot_dir(
        database: FjallDatabase,
        snapshot_dir: impl Into<PathBuf>,
    ) -> Result<Self> {
        let meta = database
            .keyspace(PART_META, KeyspaceCreateOptions::default)
            .map_err(|e| Error::Storage(e.to_string()))?;
        let rev = database
            .keyspace(PART_REV, KeyspaceCreateOptions::default)
            .map_err(|e| Error::Storage(e.to_string()))?;
        let tomb = database
            .keyspace(PART_TOMB, KeyspaceCreateOptions::default)
            .map_err(|e| Error::Storage(e.to_string()))?;
        let s = Self {
            database,
            meta,
            rev,
            tomb,
            snapshot_dir: snapshot_dir.into(),
            indices: DashMap::new(),
            snapshot_reloads: AtomicUsize::new(0),
            model_names: DashMap::new(),
            tombstone_high_watermarks: DashMap::new(),
        };
        s.rebuild()?;
        Ok(s)
    }
}
