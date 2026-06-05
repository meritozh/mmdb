use crate::{codec, keys, partitions};
use fjall::{Config, Keyspace, PartitionCreateOptions, PartitionHandle, PersistMode};
use mmdb_core::{Error, MemoryNode, Result};
use std::path::Path;
use ulid::Ulid;

pub struct Storage {
    pub keyspace: Keyspace,
    pub nodes: PartitionHandle,
    pub nodes_by_time: PartitionHandle,
    pub nodes_by_kind: PartitionHandle,
    pub nodes_meta: PartitionHandle,
}

impl Storage {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let ks = Config::new(path)
            .open()
            .map_err(|e| Error::Storage(e.to_string()))?;
        let nodes = ks
            .open_partition(partitions::NODES, PartitionCreateOptions::default())
            .map_err(|e| Error::Storage(e.to_string()))?;
        let nodes_by_time = ks
            .open_partition(partitions::NODES_BY_TIME, PartitionCreateOptions::default())
            .map_err(|e| Error::Storage(e.to_string()))?;
        let nodes_by_kind = ks
            .open_partition(partitions::NODES_BY_KIND, PartitionCreateOptions::default())
            .map_err(|e| Error::Storage(e.to_string()))?;
        let nodes_meta = ks
            .open_partition(partitions::NODES_META, PartitionCreateOptions::default())
            .map_err(|e| Error::Storage(e.to_string()))?;
        Ok(Self { keyspace: ks, nodes, nodes_by_time, nodes_by_kind, nodes_meta })
    }

    pub fn put_node(&self, n: &MemoryNode) -> Result<()> {
        let bytes = codec::encode_node(n)?;
        let nk = keys::node_key(n.tenant, n.id);
        let tk = keys::time_key(n.tenant, n.created_at_ms, n.id);
        let kk = keys::kind_key(n.tenant, n.kind.as_u8(), n.created_at_ms, n.id);

        let mut batch = self.keyspace.batch();
        batch.insert(&self.nodes, nk.clone(), bytes);
        batch.insert(&self.nodes_by_time, tk, []);
        batch.insert(&self.nodes_by_kind, kk, []);
        batch.insert(&self.nodes_meta, nk, encode_meta(n));
        batch.commit().map_err(|e| Error::Storage(e.to_string()))?;
        self.keyspace
            .persist(PersistMode::SyncAll)
            .map_err(|e| Error::Storage(e.to_string()))?;
        Ok(())
    }

    pub fn get_node(&self, tenant: u32, id: Ulid) -> Result<Option<MemoryNode>> {
        let nk = keys::node_key(tenant, id);
        match self.nodes.get(&nk).map_err(|e| Error::Storage(e.to_string()))? {
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
        let mut out = Vec::new();
        for kv in self.nodes_by_time.range(lo..hi) {
            let (k, _) = kv.map_err(|e| Error::Storage(e.to_string()))?;
            if let Some(id) = keys::id_from_time_key(&k) {
                if let Some(n) = self.get_node(tenant, id)? {
                    out.push(n);
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
            let mut batch = self.keyspace.batch();
            batch.remove(&self.nodes, nk.clone());
            batch.remove(&self.nodes_by_time, tk);
            batch.remove(&self.nodes_by_kind, kk);
            batch.remove(&self.nodes_meta, nk);
            batch.commit().map_err(|e| Error::Storage(e.to_string()))?;
            self.keyspace
                .persist(PersistMode::SyncAll)
                .map_err(|e| Error::Storage(e.to_string()))?;
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
    if b.len() != 1 + 8 + 8 { return None; }
    let kind = b[0];
    let mut c = [0u8; 8]; c.copy_from_slice(&b[1..9]);
    let created = i64::from_be_bytes(c);
    let mut u = [0u8; 8]; u.copy_from_slice(&b[9..17]);
    let updated = i64::from_be_bytes(u);
    Some(NodeMeta { kind, created_at_ms: created, updated_at_ms: updated })
}

impl Storage {
    /// Look up the cheap meta record for a node. Returns None if absent.
    pub fn get_node_meta(&self, tenant: u32, id: Ulid) -> Result<Option<NodeMeta>> {
        let nk = keys::node_key(tenant, id);
        match self.nodes_meta.get(&nk).map_err(|e| Error::Storage(e.to_string()))? {
            Some(v) => Ok(decode_meta(&v)),
            None => Ok(None),
        }
    }
}
