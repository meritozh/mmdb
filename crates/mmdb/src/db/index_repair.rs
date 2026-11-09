//! A tenant has one node mutation writer. Before crossing stores, persist
//! the node identity and all model indexes either revision could have touched.
//! The base node (including absence) decides the outcome on replay.
use super::Database;
use fjall::{Keyspace, PartitionCreateOptions, PartitionHandle, PersistMode};
use mmdb_core::{Error, MemoryNode, Result};
use mmdb_storage::Storage;
use std::collections::BTreeSet;
use ulid::Ulid;

#[derive(serde::Serialize, serde::Deserialize)]
struct Pending {
    id: Ulid,
    models: BTreeSet<String>,
}
pub(super) struct IndexRepairs {
    ks: Keyspace,
    part: PartitionHandle,
    key: [u8; 4],
}
fn storage_error(e: impl std::fmt::Display) -> Error {
    Error::Storage(e.to_string())
}

impl IndexRepairs {
    pub(super) fn open(storage: &Storage, tenant: u32) -> Result<Self> {
        let ks = storage.keyspace.clone();
        let part = ks
            .open_partition("node_index_repair_v1", PartitionCreateOptions::default())
            .map_err(storage_error)?;
        Ok(Self {
            ks,
            part,
            key: tenant.to_be_bytes(),
        })
    }
    pub(super) fn begin(
        &self,
        id: Ulid,
        old: Option<&MemoryNode>,
        new: Option<&MemoryNode>,
    ) -> Result<()> {
        let models = old
            .into_iter()
            .chain(new)
            .flat_map(|n| n.embeddings.iter().map(|e| e.model.clone()))
            .collect();
        let mut batch = self.ks.batch().durability(Some(PersistMode::SyncAll));
        batch.insert(
            &self.part,
            self.key,
            serde_json::to_vec(&Pending { id, models })?,
        );
        batch.commit().map_err(storage_error)
    }
    pub(super) fn finish(&self) -> Result<()> {
        let mut batch = self.ks.batch().durability(Some(PersistMode::SyncAll));
        batch.remove(&self.part, self.key);
        batch.commit().map_err(storage_error)
    }
}
impl Database {
    /// Called before publishing an opened Database, or under the node write lock.
    pub(super) fn recover_node_indexes(&self) -> Result<()> {
        let r = &self.index_repairs;
        let Some(bytes) = r.part.get(r.key).map_err(storage_error)? else {
            return Ok(());
        };
        let pending: Pending = serde_json::from_slice(&bytes)?;
        for model in pending.models {
            self.vector_store
                .delete(self.config.tenant, &model, pending.id)?;
        }
        if let Some(node) = self.get(pending.id)? {
            self.reconcile_node_indexes_unlocked(&node)?;
        } else {
            self.lexical_index.delete(self.config.tenant, pending.id)?;
            self.access_store.delete(self.config.tenant, pending.id)?;
        }
        r.finish()
    }
}
