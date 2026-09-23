//! Feature A2 — Entity Identity MVP.
//!
//! Provides a globally-unique identity index over
//! `(namespace, entity_type, external_key)` triples, independent of graph
//! edges. The index is a dedicated secondary partition mirroring the existing
//! `names` / `action_names` indexes: reads are point-lookups, and every write
//! (node + identity entry, alias, or merge redirect) is assembled into a single
//! durable batch while holding the context write lock, so a failure never
//! leaves a half-created entity.
//!
//! Feature A4 also lives here: the [`ProcessRecord`] sum type that lets callers
//! tag a record's payload with event / decision / state-transition semantics
//! without introducing a new on-disk format.

use crate::context::{
    ContextError, ContextRef, ContextResult, ContextStore, InformationKind, Properties,
    RecordHeader, RecordKind, RecordPin, TypeKind,
};
use crate::native_memory::{OperationId, RecordState, Scope, TemporalFacts};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::BTreeMap;
use ulid::Ulid;

/// A caller-supplied, globally-unique handle for an entity.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EntityRef {
    pub namespace: String,
    pub entity_type: String,
    pub external_key: String,
}

impl EntityRef {
    pub fn new(
        namespace: impl Into<String>,
        entity_type: impl Into<String>,
        external_key: impl Into<String>,
    ) -> Self {
        Self {
            namespace: namespace.into(),
            entity_type: entity_type.into(),
            external_key: external_key.into(),
        }
    }
}

/// The resolved identity of an entity row.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EntityIdentity {
    pub entity_ref: EntityRef,
    pub node_id: Ulid,
    /// When this handle is an alias, the primary entity it redirects to.
    pub alias_of: Option<Ulid>,
    pub created_at_ms: i64,
    pub source_operation: Option<Ulid>,
}

/// On-disk row for the entity-identity secondary index.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct StoredEntityIdentity {
    namespace: String,
    entity_type: String,
    external_key: String,
    node_id: Ulid,
    alias_of: Option<Ulid>,
    created_at_ms: i64,
    source_operation: Option<Ulid>,
}

impl From<StoredEntityIdentity> for EntityIdentity {
    fn from(stored: StoredEntityIdentity) -> Self {
        Self {
            entity_ref: EntityRef {
                namespace: stored.namespace,
                entity_type: stored.entity_type,
                external_key: stored.external_key,
            },
            node_id: stored.node_id,
            alias_of: stored.alias_of,
            created_at_ms: stored.created_at_ms,
            source_operation: stored.source_operation,
        }
    }
}

/// Maximum entities returned by a single paged listing.
pub const MAX_ENTITY_LIST: usize = 256;

// ---------------------------------------------------------------------------
// Process semantics (Feature A4.3)
// ---------------------------------------------------------------------------

/// Minimal process vocabulary: an *event* happened, a *decision* was made, or an
/// entity's state transitioned. These are payload markers, not new storage
/// formats — callers embed one as an object record's property / metadata.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "process_kind", rename_all = "snake_case")]
pub enum ProcessRecord {
    Event {
        event_type: String,
        occurred_at_ms: i64,
        observed_at_ms: i64,
        payload: Value,
    },
    Decision {
        decision_id: Ulid,
        decided_at_ms: i64,
        rationale: String,
        alternatives: Vec<String>,
        chosen: String,
        source_operation: Option<Ulid>,
    },
    StateTransition {
        entity_ref: EntityRef,
        from_state: String,
        to_state: String,
        transitioned_at_ms: i64,
        /// Optional back-reference to the event or decision that caused this
        /// transition.
        cause: Option<Ulid>,
    },
}

// ---------------------------------------------------------------------------
// Key encoding
// ---------------------------------------------------------------------------

use crate::context::storage::{decode, encode, owner_key, segment};

fn identity_key(owner: &str, entity_ref: &EntityRef) -> Vec<u8> {
    let mut key = owner_key(owner);
    segment(&mut key, entity_ref.namespace.as_bytes());
    segment(&mut key, entity_ref.entity_type.as_bytes());
    segment(&mut key, entity_ref.external_key.as_bytes());
    key
}

/// Prefix covering every identity row for `(namespace, entity_type)`.
fn identity_prefix(owner: &str, namespace: &str, entity_type: &str) -> Vec<u8> {
    let mut key = owner_key(owner);
    segment(&mut key, namespace.as_bytes());
    segment(&mut key, entity_type.as_bytes());
    key
}

fn decode_identity(bytes: &[u8]) -> ContextResult<StoredEntityIdentity> {
    decode(bytes).map_err(|_| ContextError::Corrupt("corrupt entity identity row".into()))
}

impl ContextStore<'_> {
    /// Atomically resolve `entity_ref` to a node id, creating a fresh Object node
    /// when the triple has never been seen. The identity index entry and the node
    /// header are written in one durable batch, so a crash cannot leave an index
    /// row pointing at a missing node (or vice versa).
    pub async fn resolve_or_create_entity(
        &self,
        op: OperationId,
        entity_ref: EntityRef,
    ) -> ContextResult<EntityIdentity> {
        self.resolve_or_create_entity_locked(op, entity_ref)
    }

    fn resolve_or_create_entity_locked(
        &self,
        op: OperationId,
        entity_ref: EntityRef,
    ) -> ContextResult<EntityIdentity> {
        let _guard = self.db.lock_write();
        self.check_entity_ref(&entity_ref)?;
        if let Some(existing) = self.identity_locked(&entity_ref)? {
            return Ok(existing);
        }
        let digest = self.digest("entity-resolve", &entity_ref)?;
        if let Some(stored) = self.replay(op, digest)? {
            return self.identity_from_pin(&stored.pin);
        }
        let type_pin = self.ensure_entity_type(&entity_ref.entity_type)?;
        let now = crate::now_ms();
        let pin = self.fresh_pin(RecordKind::Object);
        let mut properties: Properties = BTreeMap::new();
        properties.insert("namespace".into(), json!(entity_ref.namespace));
        properties.insert("entity_type".into(), json!(entity_ref.entity_type));
        properties.insert("external_key".into(), json!(entity_ref.external_key));
        let bytes = encode(&properties)?;
        let header = RecordHeader {
            pin: pin.clone(),
            scope: Scope::Personal,
            state: RecordState::Active,
            type_pin: Some(type_pin),
            sources: Vec::new(),
            provenance: self.provenance(),
            information: InformationKind::Observation,
            temporal: TemporalFacts::observed_at(now),
            recorded_at_ms: now,
            payload: crate::context::PayloadDescriptor {
                revision: pin.revision,
                bytes: bytes.len() as u64,
                digest: self.content_digest(&bytes),
            },
            endpoints: None,
            history: None,
            checkpoint: None,
        };
        let stored = StoredEntityIdentity {
            namespace: entity_ref.namespace.clone(),
            entity_type: entity_ref.entity_type.clone(),
            external_key: entity_ref.external_key.clone(),
            node_id: pin.record.id,
            alias_of: None,
            created_at_ms: now,
            source_operation: Some(op.0),
        };
        let mut batch = self.db.durable_batch();
        self.put_payload(&mut batch, &pin, &bytes);
        self.put_header(&mut batch, &header)?;
        self.replace_postings(&mut batch, &header, Some(&properties))?;
        self.replace_adjacency(&mut batch, None, &header)?;
        batch.insert(
            &self.parts.entity_identity,
            identity_key(&self.access.owner, &entity_ref),
            encode(&stored)?,
        );
        self.commit_operation(
            batch,
            op,
            digest,
            crate::context::WriteReceipt {
                pin,
                history_sequence: None,
            },
        )?;
        Ok(stored.into())
    }

    /// Look up an entity without creating it.
    pub fn get_entity(&self, entity_ref: &EntityRef) -> ContextResult<Option<EntityIdentity>> {
        let _guard = self.db.lock_write();
        self.identity_locked(entity_ref)
    }

    /// Attach an alias `alias` that redirects to the primary entity. The alias
    /// triple is itself unique.
    pub fn add_entity_alias(
        &self,
        op: OperationId,
        primary: &EntityRef,
        alias: EntityRef,
    ) -> ContextResult<EntityIdentity> {
        let _guard = self.db.lock_write();
        self.check_entity_ref(primary)?;
        self.check_entity_ref(&alias)?;
        if primary == &alias {
            return Err(ContextError::InvalidInput(
                "alias must differ from the primary reference".into(),
            ));
        }
        let primary_row = self.identity_locked(primary)?.ok_or_else(|| {
            ContextError::NotFound(ContextRef {
                era: self.db.era_id(),
                owner: self.access.owner.clone(),
                kind: RecordKind::Object,
                id: Ulid::nil(),
            })
        })?;
        if self.identity_locked(&alias)?.is_some() {
            return Err(ContextError::IdempotencyConflict);
        }
        let now = crate::now_ms();
        let stored = StoredEntityIdentity {
            namespace: alias.namespace.clone(),
            entity_type: alias.entity_type.clone(),
            external_key: alias.external_key.clone(),
            node_id: primary_row.node_id,
            alias_of: Some(primary_row.node_id),
            created_at_ms: now,
            source_operation: Some(op.0),
        };
        let mut batch = self.db.durable_batch();
        batch.insert(
            &self.parts.entity_identity,
            identity_key(&self.access.owner, &alias),
            encode(&stored)?,
        );
        self.commit_operation(
            batch,
            op,
            self.digest("entity-alias", &(primary, &alias))?,
            crate::context::WriteReceipt {
                pin: self.fresh_pin(RecordKind::Object),
                history_sequence: None,
            },
        )?;
        Ok(stored.into())
    }

    /// Merge `source` into `target`: every identity row (including aliases)
    /// that pointed at the source node is rewritten to point at the target.
    /// Idempotent: merging an already-merged alias is a no-op.
    pub fn merge_entities(
        &self,
        op: OperationId,
        source: &EntityRef,
        target: &EntityRef,
    ) -> ContextResult<()> {
        let _guard = self.db.lock_write();
        self.check_entity_ref(source)?;
        self.check_entity_ref(target)?;
        let source_row = self.identity_locked(source)?.ok_or_else(|| {
            ContextError::NotFound(ContextRef {
                era: self.db.era_id(),
                owner: self.access.owner.clone(),
                kind: RecordKind::Object,
                id: Ulid::nil(),
            })
        })?;
        let target_row = self.identity_locked(target)?.ok_or_else(|| {
            ContextError::NotFound(ContextRef {
                era: self.db.era_id(),
                owner: self.access.owner.clone(),
                kind: RecordKind::Object,
                id: Ulid::nil(),
            })
        })?;
        if source_row.entity_ref.entity_type != target_row.entity_ref.entity_type {
            return Err(ContextError::TypeConflict(format!(
                "cannot merge entities of different types: {} vs {}",
                source_row.entity_ref.entity_type, target_row.entity_ref.entity_type
            )));
        }
        // Already the same identity, or source already redirects to target.
        if source_row.node_id == target_row.node_id
            || source_row.alias_of == Some(target_row.node_id)
        {
            return Ok(());
        }
        let mut batch = self.db.durable_batch();
        let mut rewritten = 0usize;
        let prefix = owner_key(&self.access.owner);
        for entry in self.parts.entity_identity.range(prefix.clone()..) {
            let (key, value) = entry?;
            if !key.starts_with(&prefix) {
                break;
            }
            let mut row = decode_identity(&value)?;
            if row.node_id == source_row.node_id || row.alias_of == Some(source_row.node_id) {
                row.node_id = target_row.node_id;
                row.alias_of = Some(target_row.node_id);
                batch.insert(&self.parts.entity_identity, key, encode(&row)?);
                rewritten += 1;
            }
        }
        if rewritten == 0 {
            return Ok(());
        }
        self.commit_operation(
            batch,
            op,
            self.digest("entity-merge", &(source, target))?,
            crate::context::WriteReceipt {
                pin: self.fresh_pin(RecordKind::Object),
                history_sequence: None,
            },
        )
        .map(|_| ())
    }

    /// List entity identities under `(namespace, entity_type)`, optionally
    /// resuming after `after_external_key`.
    pub fn list_entities(
        &self,
        namespace: &str,
        entity_type: &str,
        limit: usize,
        after: Option<&str>,
    ) -> ContextResult<Vec<EntityIdentity>> {
        if limit == 0 || limit > MAX_ENTITY_LIST {
            return Err(ContextError::InvalidInput(format!(
                "entity list limit must be 1..={MAX_ENTITY_LIST}"
            )));
        }
        let _guard = self.db.lock_write();
        let prefix = identity_prefix(&self.access.owner, namespace, entity_type);
        let start = match after {
            Some(key) => {
                let mut start = prefix.clone();
                segment(&mut start, key.as_bytes());
                start
            }
            None => prefix.clone(),
        };
        let mut out = Vec::new();
        for entry in self.parts.entity_identity.range(start.clone()..) {
            let (row_key, value) = entry?;
            if !row_key.starts_with(&prefix) {
                break;
            }
            if out.len() >= limit {
                break;
            }
            if after.is_some() && row_key == start {
                continue;
            }
            out.push(decode_identity(&value)?.into());
        }
        Ok(out)
    }

    // -----------------------------------------------------------------------
    // Internal helpers
    // -----------------------------------------------------------------------

    fn check_entity_ref(&self, entity_ref: &EntityRef) -> ContextResult<()> {
        if entity_ref.namespace.is_empty()
            || entity_ref.entity_type.is_empty()
            || entity_ref.external_key.is_empty()
        {
            return Err(ContextError::InvalidInput(
                "entity namespace, type and external_key must all be non-empty".into(),
            ));
        }
        Ok(())
    }

    fn identity_locked(&self, entity_ref: &EntityRef) -> ContextResult<Option<EntityIdentity>> {
        let Some(bytes) = self
            .parts
            .entity_identity
            .get(identity_key(&self.access.owner, entity_ref))?
        else {
            return Ok(None);
        };
        Ok(Some(decode_identity(&bytes)?.into()))
    }

    fn identity_from_pin(&self, pin: &RecordPin) -> ContextResult<EntityIdentity> {
        // Replay path: locate the identity row that points at this node.
        let prefix = owner_key(&self.access.owner);
        for entry in self.parts.entity_identity.range(prefix.clone()..) {
            let (key, value) = entry?;
            if !key.starts_with(&prefix) {
                break;
            }
            let row = decode_identity(&value)?;
            if row.node_id == pin.record.id {
                return Ok(row.into());
            }
        }
        Err(ContextError::NotFound(pin.record.clone()))
    }

    /// Find (or create) a minimal object type named after `entity_type`. Runs
    /// under the already-held write lock.
    fn ensure_entity_type(&self, entity_type: &str) -> ContextResult<RecordPin> {
        self.ensure_type_locked(entity_type, TypeKind::Object)
    }
}
