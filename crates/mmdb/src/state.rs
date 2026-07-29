use fjall::{Keyspace, PartitionCreateOptions, PartitionHandle, PersistMode};
use mmdb_core::{Error, Result};
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashSet;

const PART_STATE: &str = "app_state_v1";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct StateEntry {
    pub key: Vec<u8>,
    pub value: Value,
    pub revision: u64,
    pub updated_at_ms: i64,
}

#[derive(Debug, Clone)]
pub enum StateMutation {
    Put {
        key: Vec<u8>,
        value: Value,
        expected_revision: Option<u64>,
    },
    Delete {
        key: Vec<u8>,
        expected_revision: Option<u64>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct StoredValue {
    value: Value,
    revision: u64,
    updated_at_ms: i64,
    #[serde(default)]
    deleted: bool,
}

pub struct StateStore {
    tenant: u32,
    keyspace: Keyspace,
    partition: PartitionHandle,
    write_lock: Mutex<()>,
}

impl StateStore {
    pub(crate) fn open(keyspace: Keyspace, tenant: u32) -> Result<Self> {
        let partition = keyspace
            .open_partition(PART_STATE, PartitionCreateOptions::default())
            .map_err(|error| Error::Storage(error.to_string()))?;
        Ok(Self {
            tenant,
            keyspace,
            partition,
            write_lock: Mutex::new(()),
        })
    }

    pub fn get(&self, namespace: &str, key: impl AsRef<[u8]>) -> Result<Option<StateEntry>> {
        let storage_key = state_key(self.tenant, namespace, key.as_ref())?;
        self.get_stored(&storage_key).map(|entry| {
            entry
                .filter(|stored| !stored.deleted)
                .map(|stored| StateEntry {
                    key: key.as_ref().to_vec(),
                    value: stored.value,
                    revision: stored.revision,
                    updated_at_ms: stored.updated_at_ms,
                })
        })
    }

    pub fn put(&self, namespace: &str, key: impl AsRef<[u8]>, value: Value) -> Result<u64> {
        let _guard = self.write_lock.lock();
        let storage_key = state_key(self.tenant, namespace, key.as_ref())?;
        let revision = self
            .get_stored(&storage_key)?
            .map_or(1, |stored| stored.revision.saturating_add(1));
        let mut batch = self.keyspace.batch().durability(Some(PersistMode::SyncAll));
        batch.insert(
            &self.partition,
            storage_key,
            serde_json::to_vec(&StoredValue {
                value,
                revision,
                updated_at_ms: crate::now_ms(),
                deleted: false,
            })?,
        );
        batch
            .commit()
            .map_err(|error| Error::Storage(error.to_string()))?;
        Ok(revision)
    }

    pub fn apply(&self, namespace: &str, mutations: Vec<StateMutation>) -> Result<()> {
        let _guard = self.write_lock.lock();
        self.apply_locked(namespace, mutations)
    }

    pub fn scan_prefix(
        &self,
        namespace: &str,
        prefix: impl AsRef<[u8]>,
        limit: usize,
    ) -> Result<Vec<StateEntry>> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let namespace_prefix = namespace_prefix(self.tenant, namespace)?;
        let storage_prefix = state_key(self.tenant, namespace, prefix.as_ref())?;

        let mut entries = Vec::new();
        for item in self.partition.prefix(storage_prefix) {
            let (key, value) = item.map_err(|error| Error::Storage(error.to_string()))?;
            let stored: StoredValue = serde_json::from_slice(&value)?;
            if stored.deleted {
                continue;
            }
            entries.push(StateEntry {
                key: key[namespace_prefix.len()..].to_vec(),
                value: stored.value,
                revision: stored.revision,
                updated_at_ms: stored.updated_at_ms,
            });
            if entries.len() >= limit {
                break;
            }
        }
        Ok(entries)
    }

    pub fn replace_prefix(
        &self,
        namespace: &str,
        prefix: impl AsRef<[u8]>,
        entries: Vec<(Vec<u8>, Value)>,
    ) -> Result<()> {
        let prefix = prefix.as_ref();
        let mut replacement_keys = HashSet::with_capacity(entries.len());
        for (key, _) in &entries {
            if !key.starts_with(prefix) {
                return Err(Error::InvalidArgument(
                    "replacement state key is outside the requested prefix".into(),
                ));
            }
            if !replacement_keys.insert(key.clone()) {
                return Err(Error::InvalidArgument(
                    "duplicate replacement state key".into(),
                ));
            }
        }
        let _guard = self.write_lock.lock();
        let existing = self.scan_prefix(namespace, prefix, usize::MAX)?;
        let mut mutations = existing
            .into_iter()
            .filter(|entry| !replacement_keys.contains(&entry.key))
            .map(|entry| StateMutation::Delete {
                key: entry.key,
                expected_revision: None,
            })
            .collect::<Vec<_>>();
        mutations.extend(entries.into_iter().map(|(key, value)| StateMutation::Put {
            key,
            value,
            expected_revision: None,
        }));
        self.apply_locked(namespace, mutations)
    }

    fn apply_locked(&self, namespace: &str, mutations: Vec<StateMutation>) -> Result<()> {
        let mut prepared = Vec::with_capacity(mutations.len());
        let mut seen = HashSet::with_capacity(mutations.len());
        for mutation in mutations {
            let (key, expected_revision) = match &mutation {
                StateMutation::Put {
                    key,
                    expected_revision,
                    ..
                }
                | StateMutation::Delete {
                    key,
                    expected_revision,
                } => (key, expected_revision),
            };
            let storage_key = state_key(self.tenant, namespace, key)?;
            if !seen.insert(storage_key.clone()) {
                return Err(Error::InvalidArgument(
                    "duplicate state key in batch".into(),
                ));
            }
            let current = self.get_stored(&storage_key)?;
            if let Some(expected) = expected_revision {
                let actual = current.as_ref().map(|entry| entry.revision).unwrap_or(0);
                if actual != *expected {
                    return Err(Error::InvalidArgument(format!(
                        "state revision conflict: expected {expected}, found {actual}"
                    )));
                }
            }
            prepared.push((storage_key, current, mutation));
        }

        let now = crate::now_ms();
        let mut batch = self.keyspace.batch().durability(Some(PersistMode::SyncAll));
        for (storage_key, current, mutation) in prepared {
            match mutation {
                StateMutation::Put { value, .. } => {
                    let revision = current.map_or(1, |entry| entry.revision.saturating_add(1));
                    batch.insert(
                        &self.partition,
                        storage_key,
                        serde_json::to_vec(&StoredValue {
                            value,
                            revision,
                            updated_at_ms: now,
                            deleted: false,
                        })?,
                    );
                }
                StateMutation::Delete { .. } => {
                    let revision = current.map_or(1, |entry| entry.revision.saturating_add(1));
                    batch.insert(
                        &self.partition,
                        storage_key,
                        serde_json::to_vec(&StoredValue {
                            value: Value::Null,
                            revision,
                            updated_at_ms: now,
                            deleted: true,
                        })?,
                    );
                }
            }
        }
        batch
            .commit()
            .map_err(|error| Error::Storage(error.to_string()))
    }

    fn get_stored(&self, key: &[u8]) -> Result<Option<StoredValue>> {
        match self
            .partition
            .get(key)
            .map_err(|error| Error::Storage(error.to_string()))?
        {
            Some(value) => Ok(Some(serde_json::from_slice(&value)?)),
            None => Ok(None),
        }
    }
}

fn namespace_prefix(tenant: u32, namespace: &str) -> Result<Vec<u8>> {
    let len = u16::try_from(namespace.len())
        .map_err(|_| Error::InvalidArgument("state namespace is too long".into()))?;
    let mut prefix = Vec::with_capacity(6 + namespace.len());
    prefix.extend_from_slice(&tenant.to_be_bytes());
    prefix.extend_from_slice(&len.to_be_bytes());
    prefix.extend_from_slice(namespace.as_bytes());
    Ok(prefix)
}

fn state_key(tenant: u32, namespace: &str, key: &[u8]) -> Result<Vec<u8>> {
    let mut output = namespace_prefix(tenant, namespace)?;
    let encoded_len = output
        .len()
        .checked_add(key.len())
        .ok_or_else(|| Error::InvalidArgument("state key is too long".into()))?;
    if encoded_len > u16::MAX as usize {
        return Err(Error::InvalidArgument(
            "encoded state key exceeds 65535 bytes".into(),
        ));
    }
    output.reserve(key.len());
    output.extend_from_slice(key);
    Ok(output)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Database;
    use serde_json::json;

    #[test]
    fn state_round_trip_batch_and_replace_prefix() {
        let dir = tempfile::tempdir().unwrap();
        let db = Database::open(dir.path()).unwrap();
        let state = db.state();

        assert_eq!(
            state
                .put("chat", b"session/a", json!({"title": "A"}))
                .unwrap(),
            1
        );
        assert_eq!(
            state
                .put("chat", b"session/a", json!({"title": "B"}))
                .unwrap(),
            2
        );
        assert_eq!(
            state.get("chat", b"session/a").unwrap().unwrap().value,
            json!({"title": "B"})
        );

        state
            .apply(
                "chat",
                vec![
                    StateMutation::Put {
                        key: b"message/a/0001".to_vec(),
                        value: json!("one"),
                        expected_revision: Some(0),
                    },
                    StateMutation::Put {
                        key: b"message/a/0002".to_vec(),
                        value: json!("two"),
                        expected_revision: Some(0),
                    },
                ],
            )
            .unwrap();
        let messages = state
            .scan_prefix("chat", b"message/a/", usize::MAX)
            .unwrap();
        assert_eq!(messages.len(), 2);
        assert_eq!(messages[0].value, json!("one"));

        state
            .replace_prefix(
                "chat",
                b"message/a/",
                vec![(b"message/a/0003".to_vec(), json!("three"))],
            )
            .unwrap();
        let messages = state
            .scan_prefix("chat", b"message/a/", usize::MAX)
            .unwrap();
        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].value, json!("three"));
    }

    #[test]
    fn state_revision_conflict_leaves_batch_unchanged() {
        let dir = tempfile::tempdir().unwrap();
        let db = Database::open(dir.path()).unwrap();
        let state = db.state();
        state.put("workflow", b"run/1", json!("running")).unwrap();

        let error = state
            .apply(
                "workflow",
                vec![
                    StateMutation::Put {
                        key: b"run/1".to_vec(),
                        value: json!("done"),
                        expected_revision: Some(9),
                    },
                    StateMutation::Put {
                        key: b"run/2".to_vec(),
                        value: json!("running"),
                        expected_revision: None,
                    },
                ],
            )
            .unwrap_err();
        assert!(error.to_string().contains("revision conflict"));
        assert_eq!(
            state.get("workflow", b"run/1").unwrap().unwrap().value,
            json!("running")
        );
        assert!(state.get("workflow", b"run/2").unwrap().is_none());
    }

    #[test]
    fn state_is_tenant_scoped() {
        let dir = tempfile::tempdir().unwrap();
        {
            let db = Database::open_with(
                dir.path(),
                crate::DatabaseConfig {
                    tenant: 1,
                    ..Default::default()
                },
            )
            .unwrap();
            db.state().put("chat", b"session", json!("one")).unwrap();
        }
        {
            let db = Database::open_with(
                dir.path(),
                crate::DatabaseConfig {
                    tenant: 2,
                    ..Default::default()
                },
            )
            .unwrap();
            assert!(db.state().get("chat", b"session").unwrap().is_none());
        }
        let db = Database::open_with(
            dir.path(),
            crate::DatabaseConfig {
                tenant: 1,
                ..Default::default()
            },
        )
        .unwrap();
        assert_eq!(
            db.state().get("chat", b"session").unwrap().unwrap().value,
            json!("one")
        );
    }

    #[test]
    fn state_prefix_scan_handles_long_ff_suffix() {
        let dir = tempfile::tempdir().unwrap();
        let db = Database::open(dir.path()).unwrap();
        let state = db.state();
        let key = vec![0xff; 512];

        state.put("binary", &key, json!("value")).unwrap();
        assert_eq!(
            state.scan_prefix("binary", [], usize::MAX).unwrap().len(),
            1
        );

        state.replace_prefix("binary", [], Vec::new()).unwrap();
        assert!(state.get("binary", &key).unwrap().is_none());
    }

    #[test]
    fn replace_prefix_rejects_entries_outside_prefix() {
        let dir = tempfile::tempdir().unwrap();
        let db = Database::open(dir.path()).unwrap();
        let state = db.state();
        state.put("chat", b"target/old", json!("old")).unwrap();

        let error = state
            .replace_prefix(
                "chat",
                b"target/",
                vec![(b"other".to_vec(), json!("invalid"))],
            )
            .unwrap_err();
        assert!(error.to_string().contains("outside the requested prefix"));
        assert_eq!(
            state.get("chat", b"target/old").unwrap().unwrap().value,
            json!("old")
        );
        assert!(state.get("chat", b"other").unwrap().is_none());
    }

    #[test]
    fn state_rejects_oversized_encoded_keys() {
        let dir = tempfile::tempdir().unwrap();
        let db = Database::open(dir.path()).unwrap();
        let state = db.state();
        let oversized_key = vec![0; 65_529];

        let error = state.put("n", oversized_key, json!("value")).unwrap_err();
        assert!(error.to_string().contains("exceeds 65535 bytes"));
    }

    #[test]
    fn delete_then_recreate_rejects_stale_revision() {
        let dir = tempfile::tempdir().unwrap();
        let db = Database::open(dir.path()).unwrap();
        let state = db.state();

        assert_eq!(state.put("cas", b"key", json!("first")).unwrap(), 1);
        state
            .apply(
                "cas",
                vec![StateMutation::Delete {
                    key: b"key".to_vec(),
                    expected_revision: Some(1),
                }],
            )
            .unwrap();
        assert!(state.get("cas", b"key").unwrap().is_none());
        assert_eq!(state.put("cas", b"key", json!("second")).unwrap(), 3);

        let error = state
            .apply(
                "cas",
                vec![StateMutation::Put {
                    key: b"key".to_vec(),
                    value: json!("stale"),
                    expected_revision: Some(1),
                }],
            )
            .unwrap_err();
        assert!(error.to_string().contains("revision conflict"));
        assert_eq!(
            state.get("cas", b"key").unwrap().unwrap().value,
            json!("second")
        );
    }

    #[test]
    fn duplicate_batch_keys_are_rejected_without_writes() {
        let dir = tempfile::tempdir().unwrap();
        let db = Database::open(dir.path()).unwrap();
        let state = db.state();

        let error = state
            .apply(
                "batch",
                vec![
                    StateMutation::Put {
                        key: b"key".to_vec(),
                        value: json!("first"),
                        expected_revision: Some(0),
                    },
                    StateMutation::Put {
                        key: b"key".to_vec(),
                        value: json!("second"),
                        expected_revision: Some(0),
                    },
                ],
            )
            .unwrap_err();
        assert!(error.to_string().contains("duplicate state key"));
        assert!(state.get("batch", b"key").unwrap().is_none());
    }
}
