use super::keys::*;
use super::{RebuildGroup, SnapshotManifest, VectorStore, SNAPSHOT_DIR_NAME, SNAPSHOT_MANIFEST_VERSION};
use crate::{IndexKey, VectorIndex};
use fjall::PartitionHandle;
use mmdb_core::{Error, Result};
use roaring::RoaringBitmap;
use std::collections::HashMap;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::path::{Path, PathBuf};
use std::sync::{
    atomic::Ordering,
    Arc,
};

impl VectorStore {
    /// Scan persisted meta + tomb partitions and rebuild every (tenant, model)
    /// index that has at least one live row.
    pub(super) fn rebuild(&self) -> Result<()> {
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
}

pub(super) fn default_snapshot_dir(meta: &PartitionHandle) -> PathBuf {
    meta.path()
        .parent()
        .and_then(Path::parent)
        .unwrap_or_else(|| meta.path())
        .join(SNAPSHOT_DIR_NAME)
}

pub(super) fn snapshot_basename(tenant: u32, mh: u32) -> String {
    format!("tenant{tenant}-model{mh:08x}")
}

pub(super) fn manifest_path(dir: &Path, tenant: u32, mh: u32) -> PathBuf {
    dir.join(format!("{}.manifest.json", snapshot_basename(tenant, mh)))
}
