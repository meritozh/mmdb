//! VectorStore — top-level facade that owns multiple per-(tenant, model) indices.
//!
//! Persistence model (P1):
//! - id mapping `internal_id -> Ulid` lives in fjall partition `vector_rev`
//! - id mapping `Ulid -> internal_id` lives in fjall partition `vector_meta`
//! - HNSW graph stays in memory; on crash, the index is rebuilt by replaying
//!   `vector_meta` (P1 doesn't yet ship file dumps — that's P1.5).
use crate::{IndexKey, ScoredHit, VectorIndex};
use dashmap::DashMap;
use fjall::{Keyspace, PartitionCreateOptions, PartitionHandle, PersistMode};
use mmdb_core::{Error, Result};
use std::sync::Arc;
use ulid::Ulid;

const PART_META: &str = "vector_meta";
const PART_REV: &str = "vector_rev";

pub struct VectorStore {
    keyspace: Keyspace,
    meta: PartitionHandle,
    rev: PartitionHandle,
    indices: DashMap<IndexKey, Arc<VectorIndex>>,
}

impl VectorStore {
    pub fn open(keyspace: Keyspace) -> Result<Self> {
        let meta = keyspace
            .open_partition(PART_META, PartitionCreateOptions::default())
            .map_err(|e| Error::Storage(e.to_string()))?;
        let rev = keyspace
            .open_partition(PART_REV, PartitionCreateOptions::default())
            .map_err(|e| Error::Storage(e.to_string()))?;
        Ok(Self { keyspace, meta, rev, indices: DashMap::new() })
    }

    fn get_or_create_index(&self, key: &IndexKey, dim: u32) -> Arc<VectorIndex> {
        if let Some(entry) = self.indices.get(key) {
            return entry.clone();
        }
        let idx = Arc::new(VectorIndex::new(dim));
        self.indices.insert(key.clone(), idx.clone());
        idx
    }

    pub fn insert(
        &self,
        tenant: u32,
        model: &str,
        node_id: Ulid,
        vector: &[f32],
    ) -> Result<()> {
        if vector.is_empty() {
            return Err(Error::InvalidArgument("empty vector".into()));
        }
        let key = IndexKey::new(tenant, model);
        let idx = self.get_or_create_index(&key, vector.len() as u32);
        if idx.dim as usize != vector.len() {
            return Err(Error::InvalidArgument(format!(
                "dim mismatch: index expects {}, got {}",
                idx.dim,
                vector.len()
            )));
        }

        let internal_id = idx.insert(vector);

        // persist mapping
        let meta_key = meta_key_bytes(tenant, model, node_id);
        let rev_key = rev_key_bytes(tenant, model, internal_id);
        let mut batch = self.keyspace.batch();
        batch.insert(&self.meta, meta_key, internal_id.to_be_bytes());
        batch.insert(&self.rev, rev_key, node_id.0.to_be_bytes());
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
        let key = IndexKey::new(tenant, model);
        let Some(idx) = self.indices.get(&key) else {
            return Ok(Vec::new());
        };
        if idx.dim as usize != query.len() {
            return Err(Error::InvalidArgument(format!(
                "dim mismatch: index expects {}, got {}",
                idx.dim,
                query.len()
            )));
        }

        let ef = (k * 4).max(32);
        let raw = idx.search(query, k, ef);
        let mut out = Vec::with_capacity(raw.len());
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
            // Cosine distance from hnsw_rs is in [0, 2]; similarity = 1 - dist/2
            let score = (1.0 - dist / 2.0).clamp(0.0, 1.0);
            out.push(ScoredHit { node_id, score, distance: dist });
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
        if v.len() != 8 {
            return Err(Error::Storage("corrupt meta value".into()));
        }
        let mut buf = [0u8; 8];
        buf.copy_from_slice(&v);
        let internal_id = u64::from_be_bytes(buf);

        if let Some(idx) = self.indices.get(&key) {
            idx.mark_deleted(internal_id);
        }

        let rev_key = rev_key_bytes(tenant, model, internal_id);
        let mut batch = self.keyspace.batch();
        batch.remove(&self.meta, meta_key);
        batch.remove(&self.rev, rev_key);
        batch.commit().map_err(|e| Error::Storage(e.to_string()))?;
        self.keyspace
            .persist(PersistMode::SyncAll)
            .map_err(|e| Error::Storage(e.to_string()))?;
        Ok(())
    }
}

fn model_hash(model: &str) -> u32 {
    // Cheap stable hash; collisions are tolerable because model name is also
    // part of the key in practice via per-(tenant, model) DashMap lookup.
    // For storage prefix we use a 32-bit FNV-1a.
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

#[cfg(test)]
mod tests {
    use super::*;
    use fjall::Config;
    use tempfile::tempdir;

    fn make_store() -> (tempfile::TempDir, VectorStore) {
        let dir = tempdir().unwrap();
        let ks = Config::new(dir.path()).open().unwrap();
        let s = VectorStore::open(ks).unwrap();
        (dir, s)
    }

    fn norm(v: Vec<f32>) -> Vec<f32> {
        let n: f32 = v.iter().map(|x| x * x).sum::<f32>().sqrt();
        v.into_iter().map(|x| x / n).collect()
    }

    #[test]
    fn insert_then_search_returns_inserted_id() {
        let (_dir, s) = make_store();
        let id1 = Ulid::new();
        let id2 = Ulid::new();
        s.insert(0, "m", id1, &norm(vec![1.0, 0.0, 0.0, 0.0])).unwrap();
        s.insert(0, "m", id2, &norm(vec![0.0, 1.0, 0.0, 0.0])).unwrap();

        let hits = s.search(0, "m", &norm(vec![1.0, 0.0, 0.0, 0.0]), 1).unwrap();
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].node_id, id1);
        assert!(hits[0].score > 0.99, "self-cosine should be ~1, got {}", hits[0].score);
    }

    #[test]
    fn delete_excludes_from_search() {
        let (_dir, s) = make_store();
        let id = Ulid::new();
        s.insert(0, "m", id, &norm(vec![1.0, 0.0, 0.0])).unwrap();
        s.delete(0, "m", id).unwrap();
        let hits = s.search(0, "m", &norm(vec![1.0, 0.0, 0.0]), 5).unwrap();
        assert!(hits.iter().all(|h| h.node_id != id));
    }

    #[test]
    fn dim_mismatch_is_rejected() {
        let (_dir, s) = make_store();
        s.insert(0, "m", Ulid::new(), &[1.0, 0.0, 0.0]).unwrap();
        let err = s.insert(0, "m", Ulid::new(), &[1.0, 0.0]).unwrap_err();
        assert!(format!("{err}").contains("dim mismatch"));
    }

    #[test]
    fn empty_index_search_returns_empty() {
        let (_dir, s) = make_store();
        let hits = s.search(0, "absent", &[1.0, 0.0, 0.0], 5).unwrap();
        assert!(hits.is_empty());
    }

    #[test]
    fn top_k_orders_by_similarity() {
        let (_dir, s) = make_store();
        let near = Ulid::new();
        let far = Ulid::new();
        s.insert(0, "m", near, &norm(vec![1.0, 0.01, 0.0, 0.0])).unwrap();
        s.insert(0, "m", far,  &norm(vec![0.0, 0.0, 1.0, 0.0])).unwrap();

        let hits = s.search(0, "m", &norm(vec![1.0, 0.0, 0.0, 0.0]), 2).unwrap();
        assert_eq!(hits.len(), 2);
        assert_eq!(hits[0].node_id, near);
        assert!(hits[0].score > hits[1].score);
    }
}
