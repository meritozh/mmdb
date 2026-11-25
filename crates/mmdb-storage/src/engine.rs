use crate::{codec, keys, partitions};
use fjall::Readable;
use fjall::{Database as FjallDatabase, Keyspace, KeyspaceCreateOptions, PersistMode};
use mmdb_core::{Error, MemoryNode, Result};
use std::collections::HashSet;
use std::path::Path;
use ulid::Ulid;

pub struct Storage {
    pub database: FjallDatabase,
    pub nodes: Keyspace,
    pub nodes_by_time: Keyspace,
    pub nodes_by_kind: Keyspace,
    pub nodes_meta: Keyspace,
    pub meta_index: Keyspace,
}

impl Storage {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        Self::from_database(Self::open_database(path)?)
    }

    /// Open the primary engine before initializing any MMDB keyspaces.
    pub fn open_database(path: impl AsRef<Path>) -> Result<FjallDatabase> {
        mmdb_kernel::engine_format::inspect(path.as_ref(), &[])?;
        FjallDatabase::builder(path)
            .open()
            .map_err(|e| Error::Storage(e.to_string()))
    }

    /// Bind the node store to an open primary engine.
    pub fn from_database(ks: FjallDatabase) -> Result<Self> {
        let nodes = ks
            .keyspace(partitions::NODES, KeyspaceCreateOptions::default)
            .map_err(|e| Error::Storage(e.to_string()))?;
        let nodes_by_time = ks
            .keyspace(partitions::NODES_BY_TIME, KeyspaceCreateOptions::default)
            .map_err(|e| Error::Storage(e.to_string()))?;
        let nodes_by_kind = ks
            .keyspace(partitions::NODES_BY_KIND, KeyspaceCreateOptions::default)
            .map_err(|e| Error::Storage(e.to_string()))?;
        let nodes_meta = ks
            .keyspace(partitions::NODES_META, KeyspaceCreateOptions::default)
            .map_err(|e| Error::Storage(e.to_string()))?;
        let meta_index = ks
            .keyspace(partitions::META_INDEX, KeyspaceCreateOptions::default)
            .map_err(|e| Error::Storage(e.to_string()))?;
        Ok(Self {
            database: ks,
            nodes,
            nodes_by_time,
            nodes_by_kind,
            nodes_meta,
            meta_index,
        })
    }

    pub fn put_node(&self, n: &MemoryNode) -> Result<()> {
        let old = self.get_node(n.tenant, n.id)?;
        let bytes = codec::encode_node(n)?;
        let nk = keys::node_key(n.tenant, n.id);
        let tk = keys::time_key(n.tenant, n.created_at_ms, n.id);
        let kk = keys::kind_key(n.tenant, n.kind.as_u8(), n.created_at_ms, n.id);

        let mut batch = self.database.batch().durability(Some(PersistMode::SyncAll));
        if let Some(old) = old {
            let old_tk = keys::time_key(old.tenant, old.created_at_ms, old.id);
            let old_kk = keys::kind_key(old.tenant, old.kind.as_u8(), old.created_at_ms, old.id);
            batch.remove(&self.nodes_by_time, old_tk);
            batch.remove(&self.nodes_by_kind, old_kk);
            for key in meta_index_keys(&old) {
                batch.remove(&self.meta_index, key);
            }
        }
        batch.insert(&self.nodes, nk.clone(), bytes);
        batch.insert(&self.nodes_by_time, tk, []);
        batch.insert(&self.nodes_by_kind, kk, []);
        batch.insert(&self.nodes_meta, nk, encode_meta(n));
        for (key, value) in meta_index_entries(n)? {
            batch.insert(&self.meta_index, key, value);
        }
        batch.commit().map_err(|e| Error::Storage(e.to_string()))?;
        Ok(())
    }

    pub fn get_node(&self, tenant: u32, id: Ulid) -> Result<Option<MemoryNode>> {
        let nk = keys::node_key(tenant, id);
        match self
            .nodes
            .get(&nk)
            .map_err(|e| Error::Storage(e.to_string()))?
        {
            Some(v) => Ok(Some(codec::decode_node(&v)?)),
            None => Ok(None),
        }
    }

    pub fn scan_by_time(
        &self,
        tenant: u32,
        from_ms: i64,
        to_ms: i64,
        limit: usize,
    ) -> Result<Vec<MemoryNode>> {
        let (lo, hi) = keys::time_range(tenant, from_ms, to_ms);
        if limit == 0 {
            return Ok(Vec::new());
        }
        let view = self.database.snapshot();
        self.nodes_from_time_index(
            tenant,
            limit,
            &view,
            view.range(&self.nodes_by_time, lo..hi),
        )
    }

    fn nodes_from_time_index(
        &self,
        tenant: u32,
        limit: usize,
        view: &fjall::Snapshot,
        index_rows: impl Iterator<Item = fjall::Guard>,
    ) -> Result<Vec<MemoryNode>> {
        let mut out = Vec::new();
        for kv in index_rows {
            let k = kv.key().map_err(|e| Error::Storage(e.to_string()))?;
            if let Some(id) = keys::id_from_time_key(&k) {
                if let Some(n) = view
                    .get(&self.nodes, keys::node_key(tenant, id))
                    .map_err(|e| Error::Storage(e.to_string()))?
                {
                    out.push(codec::decode_node(&n)?);
                    if out.len() >= limit {
                        break;
                    }
                }
            }
        }
        Ok(out)
    }

    pub fn delete_node(&self, tenant: u32, id: Ulid) -> Result<()> {
        if let Some(n) = self.get_node(tenant, id)? {
            let nk = keys::node_key(tenant, id);
            let tk = keys::time_key(tenant, n.created_at_ms, id);
            let kk = keys::kind_key(tenant, n.kind.as_u8(), n.created_at_ms, id);
            let mut batch = self.database.batch().durability(Some(PersistMode::SyncAll));
            batch.remove(&self.nodes, nk.clone());
            batch.remove(&self.nodes_by_time, tk);
            batch.remove(&self.nodes_by_kind, kk);
            batch.remove(&self.nodes_meta, nk);
            for key in meta_index_keys(&n) {
                batch.remove(&self.meta_index, key);
            }
            batch.commit().map_err(|e| Error::Storage(e.to_string()))?;
        }
        Ok(())
    }
}

/// Lightweight per-node metadata: just enough to do kind/time post-filtering
/// without deserialising the full node payload.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct NodeMeta {
    pub kind: u8,
    pub created_at_ms: i64,
    pub updated_at_ms: i64,
}

fn encode_meta(n: &MemoryNode) -> Vec<u8> {
    let mut v = Vec::with_capacity(1 + 8 + 8);
    v.push(n.kind.as_u8());
    v.extend_from_slice(&n.created_at_ms.to_be_bytes());
    v.extend_from_slice(&n.updated_at_ms.to_be_bytes());
    v
}

pub fn decode_meta(b: &[u8]) -> Option<NodeMeta> {
    if b.len() != 1 + 8 + 8 {
        return None;
    }
    let kind = b[0];
    let mut c = [0u8; 8];
    c.copy_from_slice(&b[1..9]);
    let created = i64::from_be_bytes(c);
    let mut u = [0u8; 8];
    u.copy_from_slice(&b[9..17]);
    let updated = i64::from_be_bytes(u);
    Some(NodeMeta {
        kind,
        created_at_ms: created,
        updated_at_ms: updated,
    })
}

impl Storage {
    /// Look up the cheap meta record for a node. Returns None if absent.
    pub fn get_node_meta(&self, tenant: u32, id: Ulid) -> Result<Option<NodeMeta>> {
        let nk = keys::node_key(tenant, id);
        match self
            .nodes_meta
            .get(&nk)
            .map_err(|e| Error::Storage(e.to_string()))?
        {
            Some(v) => Ok(decode_meta(&v)),
            None => Ok(None),
        }
    }

    /// Return node ids whose metadata contains exactly `field = value`.
    ///
    /// The index key uses stable hashes for compact range lookup while the
    /// value stores the original `(field, value)` pair so rare hash collisions
    /// can be rejected during scan.
    pub fn node_ids_by_metadata(
        &self,
        tenant: u32,
        field: &str,
        value: &serde_json::Value,
    ) -> Result<HashSet<Ulid>> {
        self.metadata_matches(tenant, field, value, &self.database.snapshot())
            .map(|ids| ids.into_iter().collect())
    }

    fn metadata_matches(
        &self,
        tenant: u32,
        field: &str,
        value: &serde_json::Value,
        index: &fjall::Snapshot,
    ) -> Result<Vec<Ulid>> {
        let (lo, hi) = meta_index_range(tenant, field, value)?;
        let mut out = Vec::new();
        for kv in index.range(&self.meta_index, lo..hi) {
            let (k, v) = kv.into_inner().map_err(|e| Error::Storage(e.to_string()))?;
            if !meta_index_value_matches(&v, field, value) {
                continue;
            }
            if let Some(id) = id_from_meta_index_key(&k) {
                out.push(id);
            }
        }
        Ok(out)
    }
    /// Both halves of the index join observe the same committed batch.
    pub fn nodes_by_metadata(
        &self,
        tenant: u32,
        field: &str,
        value: &serde_json::Value,
    ) -> Result<Vec<MemoryNode>> {
        let view = self.database.snapshot();
        self.metadata_matches(tenant, field, value, &view)?
            .into_iter()
            .map(|id| {
                let bytes = view
                    .get(&self.nodes, keys::node_key(tenant, id))
                    .map_err(|e| Error::Storage(e.to_string()))?
                    .ok_or_else(|| {
                        Error::Storage(format!("metadata index refers to missing node {id}"))
                    })?;
                codec::decode_node(&bytes)
            })
            .collect()
    }
}

fn meta_index_entries(n: &MemoryNode) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
    let mut out = Vec::with_capacity(n.metadata.len());
    for (field, value) in &n.metadata {
        out.push((
            meta_index_key(n.tenant, field, value, n.id)?,
            serde_json::to_vec(&(field, value))?,
        ));
    }
    Ok(out)
}

fn meta_index_keys(n: &MemoryNode) -> Vec<Vec<u8>> {
    n.metadata
        .iter()
        .filter_map(|(field, value)| meta_index_key(n.tenant, field, value, n.id).ok())
        .collect()
}

fn meta_index_key(
    tenant: u32,
    field: &str,
    value: &serde_json::Value,
    id: Ulid,
) -> Result<Vec<u8>> {
    let mut key = meta_index_prefix(tenant, field, value)?;
    key.extend_from_slice(&id.0.to_be_bytes());
    Ok(key)
}

fn meta_index_range(
    tenant: u32,
    field: &str,
    value: &serde_json::Value,
) -> Result<(Vec<u8>, Vec<u8>)> {
    let lo = meta_index_prefix(tenant, field, value)?;
    let mut hi = lo.clone();
    hi.extend_from_slice(&[0xff; 16]);
    Ok((lo, hi))
}

fn meta_index_prefix(tenant: u32, field: &str, value: &serde_json::Value) -> Result<Vec<u8>> {
    let value_bytes = serde_json::to_vec(value)?;
    let mut key = Vec::with_capacity(4 + 4 + 8);
    key.extend_from_slice(&tenant.to_be_bytes());
    key.extend_from_slice(&fnv1a32(field.as_bytes()).to_be_bytes());
    key.extend_from_slice(&fnv1a64(&value_bytes).to_be_bytes());
    Ok(key)
}

fn id_from_meta_index_key(k: &[u8]) -> Option<Ulid> {
    if k.len() != 4 + 4 + 8 + 16 {
        return None;
    }
    let mut buf = [0u8; 16];
    buf.copy_from_slice(&k[16..32]);
    Some(Ulid(u128::from_be_bytes(buf)))
}

fn meta_index_value_matches(v: &[u8], field: &str, value: &serde_json::Value) -> bool {
    serde_json::from_slice::<(String, serde_json::Value)>(v)
        .map(|(stored_field, stored_value)| stored_field == field && stored_value == *value)
        .unwrap_or(false)
}

fn fnv1a32(bytes: &[u8]) -> u32 {
    let mut h: u32 = 0x811c_9dc5;
    for b in bytes {
        h ^= *b as u32;
        h = h.wrapping_mul(0x0100_0193);
    }
    h
}

fn fnv1a64(bytes: &[u8]) -> u64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for b in bytes {
        h ^= *b as u64;
        h = h.wrapping_mul(0x0000_0100_0000_01b3);
    }
    h
}

#[cfg(test)]
mod tests {
    use super::*;
    use mmdb_core::{Content, MemoryState, NodeKind};

    #[test]
    fn time_join_uses_the_index_version_after_replacement() {
        let dir = tempfile::tempdir().unwrap();
        let store = Storage::open(dir.path()).unwrap();
        let mut node = MemoryNode {
            id: Ulid::new(),
            tenant: 7,
            kind: NodeKind::Fact,
            created_at_ms: 10,
            updated_at_ms: 10,
            content: Content::Text("old".into()),
            metadata: Default::default(),
            embeddings: Default::default(),
            revision: 1,
            state: MemoryState::Active,
            valid_from_ms: None,
            valid_to_ms: None,
        };
        store.put_node(&node).unwrap();
        let view = store.database.snapshot();
        let (lo, hi) = keys::time_range(7, 10, 11);
        let rows = view.range(&store.nodes_by_time, lo..hi).inspect(|_| {
            node.created_at_ms = 20;
            node.revision = 2;
            store.put_node(&node).unwrap();
        });
        let old = store.nodes_from_time_index(7, 1, &view, rows).unwrap();
        assert_eq!((old[0].created_at_ms, old[0].revision), (10, 1));
        assert!(store.scan_by_time(7, 10, 11, 1).unwrap().is_empty());
        assert!(store.scan_by_time(7, 20, 21, 0).unwrap().is_empty());
        assert_eq!(store.scan_by_time(7, 20, 21, 1).unwrap()[0].revision, 2);
    }
}
