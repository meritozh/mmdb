use crate::store_format::{
    inspect_store_root, require_managed_store, StoreFormatDescriptor, StoreLease, StoreRootState,
};
use fjall::{Config, Keyspace, PartitionCreateOptions, PartitionHandle, PersistMode};
use mmdb_core::{Error, Result};
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashSet;
use std::fs;
use std::io;
use std::path::Path;

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

/// One state mutation together with its namespace.
///
/// A namespaced batch is committed as one fjall transaction, which lets the
/// runtime make a terminal conversation update and its memory-outbox marker
/// durable without a crash gap.
#[derive(Debug, Clone)]
pub struct NamespacedStateMutation {
    namespace: String,
    mutation: StateMutation,
}

impl NamespacedStateMutation {
    pub fn new(namespace: impl Into<String>, mutation: StateMutation) -> Self {
        Self {
            namespace: namespace.into(),
            mutation,
        }
    }

    pub fn namespace(&self) -> &str {
        &self.namespace
    }

    pub fn mutation(&self) -> &StateMutation {
        &self.mutation
    }
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
    _lease: Option<StoreLease>,
}

impl StateStore {
    /// Open an operational state store in its own physical root.
    ///
    /// Keeping runtime records outside the semantic-memory keyspace lets
    /// memory rebuilds and erasure operate without risking conversations,
    /// runs, workflows, or checkpoints.
    pub fn open_path(
        path: impl AsRef<Path>,
        tenant: u32,
        format: &StoreFormatDescriptor,
    ) -> Result<Self> {
        let path = path.as_ref();
        let lease = StoreLease::acquire(path).map_err(state_store_format_error)?;
        match fs::symlink_metadata(path) {
            Ok(_) => {
                let inspected = inspect_store_root(path).map_err(state_store_format_error)?;
                if matches!(inspected.state(), StoreRootState::Empty) {
                    format
                        .new_manifest()
                        .map_err(state_store_format_error)?
                        .write_new(path)
                        .map_err(state_store_format_error)?;
                }
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                fs::create_dir(path)?;
                format
                    .new_manifest()
                    .map_err(state_store_format_error)?
                    .write_new(path)
                    .map_err(state_store_format_error)?;
            }
            Err(error) => return Err(Error::Io(error)),
        }
        let managed =
            require_managed_store(path, format.format_id()).map_err(state_store_format_error)?;
        let keyspace = Config::new(managed.canonical_root())
            .open()
            .map_err(|error| Error::Storage(error.to_string()))?;
        Self::open_with_lease(keyspace, tenant, Some(lease))
    }

    pub(crate) fn open(keyspace: Keyspace, tenant: u32) -> Result<Self> {
        Self::open_with_lease(keyspace, tenant, None)
    }

    fn open_with_lease(keyspace: Keyspace, tenant: u32, lease: Option<StoreLease>) -> Result<Self> {
        let partition = keyspace
            .open_partition(PART_STATE, PartitionCreateOptions::default())
            .map_err(|error| Error::Storage(error.to_string()))?;
        Ok(Self {
            tenant,
            keyspace,
            partition,
            write_lock: Mutex::new(()),
            _lease: lease,
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
        self.apply_namespaced_locked(
            mutations
                .into_iter()
                .map(|mutation| NamespacedStateMutation::new(namespace, mutation))
                .collect(),
        )
    }

    /// Atomically apply mutations spanning multiple namespaces.
    ///
    /// Every compare-and-swap precondition is checked before the batch is
    /// written. A conflict in any namespace leaves the entire batch unchanged.
    pub fn apply_namespaced(&self, mutations: Vec<NamespacedStateMutation>) -> Result<()> {
        let _guard = self.write_lock.lock();
        self.apply_namespaced_locked(mutations)
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

    /// Read a bounded page in key order after an optional exclusive cursor.
    ///
    /// The cursor is a caller-visible key and must belong to `prefix`. The
    /// storage iterator seeks directly to it; deleted entries do not consume
    /// the live-entry limit.
    pub fn scan_prefix_after(
        &self,
        namespace: &str,
        prefix: impl AsRef<[u8]>,
        after_exclusive: Option<&[u8]>,
        limit: usize,
    ) -> Result<Vec<StateEntry>> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let prefix = prefix.as_ref();
        if after_exclusive.is_some_and(|cursor| !cursor.starts_with(prefix)) {
            return Err(Error::InvalidArgument(
                "state scan cursor is outside the requested prefix".into(),
            ));
        }
        let namespace_prefix = namespace_prefix(self.tenant, namespace)?;
        let storage_prefix = state_key(self.tenant, namespace, prefix)?;
        let after_storage = after_exclusive
            .map(|cursor| state_key(self.tenant, namespace, cursor))
            .transpose()?;
        let start = after_storage
            .as_ref()
            .cloned()
            .unwrap_or_else(|| storage_prefix.clone());

        let mut entries = Vec::new();
        for item in self.partition.range(start..) {
            let (key, value) = item.map_err(|error| Error::Storage(error.to_string()))?;
            if !key.starts_with(&storage_prefix) {
                break;
            }
            if after_storage
                .as_ref()
                .is_some_and(|cursor| key.as_ref() <= cursor.as_slice())
            {
                continue;
            }
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
        self.apply_namespaced_locked(
            mutations
                .into_iter()
                .map(|mutation| NamespacedStateMutation::new(namespace, mutation))
                .collect(),
        )
    }

    fn apply_namespaced_locked(&self, mutations: Vec<NamespacedStateMutation>) -> Result<()> {
        let mut prepared = Vec::with_capacity(mutations.len());
        let mut seen = HashSet::with_capacity(mutations.len());
        for NamespacedStateMutation {
            namespace,
            mutation,
        } in mutations
        {
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
            let storage_key = state_key(self.tenant, &namespace, key)?;
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

fn state_store_format_error(error: crate::store_format::StoreFormatError) -> Error {
    Error::Storage(error.to_string())
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
    use crate::{
        native_memory::MemoryDatabase,
        store_format::{require_managed_store, StoreFormatDescriptor, STORE_MANIFEST_FILE},
        Database,
    };
    use serde_json::json;

    #[test]
    fn standalone_state_store_reopens_without_semantic_database() {
        let container = tempfile::tempdir().unwrap();
        let root = container.path().join("runtime");
        let format = StoreFormatDescriptor::new("example.runtime-state-v1").unwrap();
        {
            let state = StateStore::open_path(&root, 7, &format).unwrap();
            let managed = require_managed_store(&root, format.format_id()).unwrap();
            assert_eq!(managed.manifest().format_id(), format.format_id());
            state
                .put("chat", b"session/a", json!({"title": "Separated"}))
                .unwrap();
        }

        let reopened = StateStore::open_path(&root, 7, &format).unwrap();
        let entry = reopened
            .get("chat", b"session/a")
            .unwrap()
            .expect("persisted runtime state");
        assert_eq!(entry.value, json!({"title": "Separated"}));
    }

    #[test]
    fn standalone_state_store_holds_an_exclusive_root_lease() {
        let container = tempfile::tempdir().unwrap();
        let root = container.path().join("runtime");
        let format = StoreFormatDescriptor::new("example.runtime-state-v1").unwrap();
        let first = StateStore::open_path(&root, 0, &format).unwrap();

        let error = match StateStore::open_path(&root, 0, &format) {
            Ok(_) => panic!("a second state-store handle must not open the same root"),
            Err(error) => error,
        };
        assert!(error.to_string().contains("already open"));

        drop(first);
        StateStore::open_path(&root, 0, &format).expect("dropping the owner releases the lease");
    }

    #[test]
    fn standalone_state_store_never_adopts_another_applications_root() {
        let container = tempfile::tempdir().unwrap();
        let root = container.path().join("runtime");
        let alpha = StoreFormatDescriptor::new("alpha.runtime-state-v1").unwrap();
        let beta = StoreFormatDescriptor::new("beta.runtime-state-v1").unwrap();
        drop(StateStore::open_path(&root, 0, &alpha).unwrap());

        let error = match StateStore::open_path(&root, 0, &beta) {
            Ok(_) => panic!("a second application must not adopt the managed root"),
            Err(error) => error,
        };

        assert!(error.to_string().contains(alpha.format_id()));
        assert!(error.to_string().contains(beta.format_id()));
        let managed = require_managed_store(&root, alpha.format_id()).unwrap();
        assert_eq!(managed.manifest().format_id(), alpha.format_id());
    }

    #[test]
    fn standalone_state_store_refuses_a_semantic_memory_root() {
        let parent = tempfile::tempdir().unwrap();
        let root = parent.path().join("memory");
        let format = StoreFormatDescriptor::new("example.runtime-state-v1").unwrap();
        let memory = MemoryDatabase::create(&root).unwrap();
        drop(memory);

        let error = match StateStore::open_path(&root, 0, &format) {
            Ok(_) => panic!("runtime state must not open a semantic-memory store"),
            Err(error) => error,
        };
        assert!(error.to_string().contains("expected exact format"));
        assert!(error.to_string().contains(format.format_id()));
    }

    #[test]
    fn standalone_state_store_refuses_unrecognized_non_empty_roots_without_adopting_them() {
        let container = tempfile::tempdir().unwrap();
        let root = container.path().join("runtime");
        let format = StoreFormatDescriptor::new("example.runtime-state-v1").unwrap();
        std::fs::create_dir(&root).unwrap();
        let foreign = root.join("foreign.data");
        std::fs::write(&foreign, b"untouched").unwrap();

        let error = match StateStore::open_path(&root, 0, &format) {
            Ok(_) => panic!("unrecognized contents must not become runtime state"),
            Err(error) => error,
        };
        assert!(error.to_string().contains("no managed marker"));
        assert_eq!(std::fs::read(foreign).unwrap(), b"untouched");
        assert!(!root.join(STORE_MANIFEST_FILE).exists());
    }

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
    fn prefix_cursor_reads_only_live_entries_after_the_exclusive_key() {
        let dir = tempfile::tempdir().unwrap();
        let db = Database::open(dir.path()).unwrap();
        let state = db.state();
        for key in [
            b"job/001".as_slice(),
            b"job/002".as_slice(),
            b"job/003".as_slice(),
            b"job/004".as_slice(),
            b"job/005".as_slice(),
            b"other/001".as_slice(),
        ] {
            state.put("scheduler", key, json!(key.len())).unwrap();
        }
        state
            .apply(
                "scheduler",
                vec![StateMutation::Delete {
                    key: b"job/003".to_vec(),
                    expected_revision: None,
                }],
            )
            .unwrap();

        let head = state
            .scan_prefix_after("scheduler", b"job/", None, 2)
            .unwrap();
        assert_eq!(
            head.into_iter().map(|entry| entry.key).collect::<Vec<_>>(),
            [b"job/001".to_vec(), b"job/002".to_vec()]
        );
        let page = state
            .scan_prefix_after("scheduler", b"job/", Some(b"job/001"), 2)
            .unwrap();
        assert_eq!(
            page.into_iter().map(|entry| entry.key).collect::<Vec<_>>(),
            [b"job/002".to_vec(), b"job/004".to_vec()]
        );
        let tail = state
            .scan_prefix_after("scheduler", b"job/", Some(b"job/004"), 2)
            .unwrap();
        assert_eq!(
            tail.into_iter().map(|entry| entry.key).collect::<Vec<_>>(),
            [b"job/005".to_vec()]
        );
        assert!(state
            .scan_prefix_after("scheduler", b"job/", Some(b"job/005"), 2)
            .unwrap()
            .is_empty());
        assert!(state
            .scan_prefix_after("scheduler", b"job/", Some(b"other/001"), 2)
            .unwrap_err()
            .to_string()
            .contains("cursor"));
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
    fn namespaced_batch_commits_terminal_state_and_outbox_atomically() {
        let container = tempfile::tempdir().unwrap();
        let root = container.path().join("runtime");
        let format = StoreFormatDescriptor::new("example.runtime-state-v1").unwrap();
        let state = StateStore::open_path(&root, 0, &format).unwrap();
        state
            .put(
                "runtime.conversation_turns",
                b"run/1",
                json!({"status": "running"}),
            )
            .unwrap();

        state
            .apply_namespaced(vec![
                NamespacedStateMutation::new(
                    "runtime.conversation_turns",
                    StateMutation::Put {
                        key: b"run/1".to_vec(),
                        value: json!({"status": "completed"}),
                        expected_revision: Some(1),
                    },
                ),
                NamespacedStateMutation::new(
                    "memory.outbox",
                    StateMutation::Put {
                        key: b"run/1".to_vec(),
                        value: json!({"state": "pending"}),
                        expected_revision: Some(0),
                    },
                ),
            ])
            .unwrap();

        assert_eq!(
            state
                .get("runtime.conversation_turns", b"run/1")
                .unwrap()
                .unwrap()
                .value,
            json!({"status": "completed"})
        );
        assert_eq!(
            state.get("memory.outbox", b"run/1").unwrap().unwrap().value,
            json!({"state": "pending"})
        );

        let conflict = state
            .apply_namespaced(vec![
                NamespacedStateMutation::new(
                    "runtime.conversation_turns",
                    StateMutation::Put {
                        key: b"run/1".to_vec(),
                        value: json!({"status": "failed"}),
                        expected_revision: Some(1),
                    },
                ),
                NamespacedStateMutation::new(
                    "memory.outbox",
                    StateMutation::Put {
                        key: b"run/2".to_vec(),
                        value: json!({"state": "pending"}),
                        expected_revision: Some(0),
                    },
                ),
            ])
            .unwrap_err();
        assert!(conflict.to_string().contains("revision conflict"));
        assert!(state.get("memory.outbox", b"run/2").unwrap().is_none());
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
