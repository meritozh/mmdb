use crate::audit::{AuditAction, AuditContext};
use crate::Database;
use fjall::{Keyspace, PartitionCreateOptions, PartitionHandle, PersistMode};
use mmdb_core::{Error, Result};
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use serde_json::json;
use ulid::Ulid;

const PART_ACCESS: &str = "memory_access_v1";

/// Durable retrieval-usage counters kept separately from semantic node revisions.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AccessStats {
    pub node_id: Ulid,
    pub access_count: u64,
    pub last_accessed_at_ms: i64,
}

pub(crate) struct AccessStore {
    keyspace: Keyspace,
    records: PartitionHandle,
    write_lock: Mutex<()>,
}

impl AccessStore {
    pub(crate) fn open(keyspace: Keyspace) -> Result<Self> {
        let records = keyspace
            .open_partition(PART_ACCESS, PartitionCreateOptions::default())
            .map_err(|error| Error::Storage(error.to_string()))?;
        Ok(Self {
            keyspace,
            records,
            write_lock: Mutex::new(()),
        })
    }

    pub(crate) fn get(&self, tenant: u32, node_id: Ulid) -> Result<Option<AccessStats>> {
        self.records
            .get(access_key(tenant, node_id))
            .map_err(|error| Error::Storage(error.to_string()))?
            .map(|value| serde_json::from_slice(&value).map_err(Error::from))
            .transpose()
    }

    pub(crate) fn record(
        &self,
        tenant: u32,
        node_ids: &[Ulid],
        accessed_at_ms: i64,
    ) -> Result<Vec<AccessStats>> {
        if node_ids.is_empty() {
            return Ok(Vec::new());
        }

        let _guard = self.write_lock.lock();
        let mut updated = Vec::with_capacity(node_ids.len());
        let mut batch = self.keyspace.batch().durability(Some(PersistMode::SyncAll));
        for node_id in node_ids {
            let current = self.get(tenant, *node_id)?;
            let stats = AccessStats {
                node_id: *node_id,
                access_count: current
                    .as_ref()
                    .map_or(1, |stats| stats.access_count.saturating_add(1)),
                last_accessed_at_ms: current.map_or(accessed_at_ms, |stats| {
                    stats.last_accessed_at_ms.max(accessed_at_ms)
                }),
            };
            batch.insert(
                &self.records,
                access_key(tenant, *node_id),
                serde_json::to_vec(&stats)?,
            );
            updated.push(stats);
        }
        batch
            .commit()
            .map_err(|error| Error::Storage(error.to_string()))?;
        Ok(updated)
    }

    pub(crate) fn delete(&self, tenant: u32, node_id: Ulid) -> Result<()> {
        let _guard = self.write_lock.lock();
        let mut batch = self.keyspace.batch().durability(Some(PersistMode::SyncAll));
        batch.remove(&self.records, access_key(tenant, node_id));
        batch
            .commit()
            .map_err(|error| Error::Storage(error.to_string()))
    }

    pub(crate) fn merge(
        &self,
        tenant: u32,
        target: Ulid,
        sources: &[Ulid],
    ) -> Result<Option<AccessStats>> {
        let _guard = self.write_lock.lock();
        let mut node_ids = sources.to_vec();
        node_ids.sort_unstable();
        node_ids.dedup();
        node_ids.retain(|node_id| *node_id != target);

        let target_stats = self.get(tenant, target)?;
        let mut access_count = target_stats.as_ref().map_or(0, |stats| stats.access_count);
        let mut last_accessed_at_ms = target_stats.as_ref().map(|stats| stats.last_accessed_at_ms);
        let mut found = target_stats.is_some();
        let mut populated_sources = Vec::new();
        for source in node_ids {
            let Some(stats) = self.get(tenant, source)? else {
                continue;
            };
            found = true;
            access_count = access_count.saturating_add(stats.access_count);
            last_accessed_at_ms = Some(
                last_accessed_at_ms.map_or(stats.last_accessed_at_ms, |current| {
                    current.max(stats.last_accessed_at_ms)
                }),
            );
            populated_sources.push(source);
        }
        if !found {
            return Ok(None);
        }

        let merged = AccessStats {
            node_id: target,
            access_count,
            last_accessed_at_ms: last_accessed_at_ms.unwrap_or_else(crate::now_ms),
        };
        let mut batch = self.keyspace.batch().durability(Some(PersistMode::SyncAll));
        batch.insert(
            &self.records,
            access_key(tenant, target),
            serde_json::to_vec(&merged)?,
        );
        for source in populated_sources {
            batch.remove(&self.records, access_key(tenant, source));
        }
        batch
            .commit()
            .map_err(|error| Error::Storage(error.to_string()))?;
        Ok(Some(merged))
    }
}

impl Database {
    /// Return durable retrieval-usage counters for `node_id`, if it has been accessed.
    pub fn access_stats(&self, node_id: Ulid) -> Result<Option<AccessStats>> {
        self.access_store.get(self.config.tenant, node_id)
    }

    /// Record one retrieval-use event for each distinct node in `node_ids`.
    ///
    /// Counters live in a sidecar partition so recording an access does not change
    /// a memory's semantic revision or rebuild its lexical/vector projections.
    pub fn record_access(
        &self,
        node_ids: impl AsRef<[Ulid]>,
        context: AuditContext,
    ) -> Result<Vec<AccessStats>> {
        let operation_id = Ulid::new();
        let mut node_ids = node_ids.as_ref().to_vec();
        node_ids.sort_unstable();
        node_ids.dedup();
        let result = {
            let _guard = self.node_mutation_lock.lock();
            (|| {
                for node_id in &node_ids {
                    if self.get(*node_id)?.is_none() {
                        return Err(Error::InvalidArgument(format!(
                            "cannot record access for unknown node {node_id}"
                        )));
                    }
                }
                self.access_store
                    .record(self.config.tenant, &node_ids, crate::now_ms())
            })()
        };
        self.append_audit(
            operation_id,
            AuditAction::Mutation,
            "record_access",
            result.is_ok(),
            context,
            json!({
                "node_ids": node_ids,
                "stats": result.as_ref().ok(),
            }),
            result.as_ref().err().map(ToString::to_string),
        )?;
        result
    }

    /// Atomically fold access counters from `sources` into `target`.
    ///
    /// Source counters are removed after transfer, making retries idempotent.
    /// Semantic nodes and their revisions are not changed.
    pub fn merge_access_stats(
        &self,
        target: Ulid,
        sources: impl AsRef<[Ulid]>,
        context: AuditContext,
    ) -> Result<Option<AccessStats>> {
        let operation_id = Ulid::new();
        let mut sources = sources.as_ref().to_vec();
        sources.sort_unstable();
        sources.dedup();
        sources.retain(|node_id| *node_id != target);
        let result = {
            let _guard = self.node_mutation_lock.lock();
            (|| {
                if self.get(target)?.is_none() {
                    return Err(Error::InvalidArgument(format!(
                        "cannot merge access into unknown node {target}"
                    )));
                }
                for source in &sources {
                    if self.get(*source)?.is_none() {
                        return Err(Error::InvalidArgument(format!(
                            "cannot merge access from unknown node {source}"
                        )));
                    }
                }
                self.access_store
                    .merge(self.config.tenant, target, &sources)
            })()
        };
        self.append_audit(
            operation_id,
            AuditAction::Mutation,
            "merge_access_stats",
            result.is_ok(),
            context,
            json!({
                "target": target,
                "sources": sources,
                "stats": result.as_ref().ok(),
            }),
            result.as_ref().err().map(ToString::to_string),
        )?;
        result
    }
}

fn access_key(tenant: u32, node_id: Ulid) -> Vec<u8> {
    let mut key = Vec::with_capacity(20);
    key.extend_from_slice(&tenant.to_be_bytes());
    key.extend_from_slice(&node_id.0.to_be_bytes());
    key
}
