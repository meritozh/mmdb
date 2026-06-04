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
        Ok(Self { keyspace: ks, nodes, nodes_by_time, nodes_by_kind })
    }

    pub fn put_node(&self, n: &MemoryNode) -> Result<()> {
        let bytes = codec::encode_node(n)?;
        let nk = keys::node_key(n.tenant, n.id);
        let tk = keys::time_key(n.tenant, n.created_at_ms, n.id);
        let kk = keys::kind_key(n.tenant, n.kind.as_u8(), n.created_at_ms, n.id);

        let mut batch = self.keyspace.batch();
        batch.insert(&self.nodes, nk, bytes);
        batch.insert(&self.nodes_by_time, tk, []);
        batch.insert(&self.nodes_by_kind, kk, []);
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
            batch.remove(&self.nodes, nk);
            batch.remove(&self.nodes_by_time, tk);
            batch.remove(&self.nodes_by_kind, kk);
            batch.commit().map_err(|e| Error::Storage(e.to_string()))?;
            self.keyspace
                .persist(PersistMode::SyncAll)
                .map_err(|e| Error::Storage(e.to_string()))?;
        }
        Ok(())
    }
}
