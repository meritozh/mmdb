//! Feature A3 — Extraction Transaction.
//!
//! A single operation that batches object/relation creation and revision into
//! one durable, atomic commit. Callers supply a stable `operation_id`:
//!
//! * A completed transaction is memoised by `operation_id`, so re-submitting the
//!   same id returns the cached receipt (`replayed = true`) with no duplicate
//!   rows.
//! * A transaction that crashed after writing its `Incomplete` marker but before
//!   committing leaves no partial rows (fjall batches commit atomically). The
//!   next submission notices the marker, clears it, and re-executes.

use crate::context::storage::{decode, encode, owner_key, segment};
use crate::context::{
    ContextError, ContextRef, ContextResult, ContextStore, Endpoints, InformationKind, Properties,
    RecordHeader, RecordKind, RecordPin, TypeKind,
};
use crate::native_memory::{OperationId, RecordState, Scope, TemporalFacts};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use ulid::Ulid;

/// Hard cap on the number of object + relation operations in one transaction.
pub const MAX_BATCH_SIZE: usize = 256;

/// One unit of work inside an extraction transaction.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum ExtractionOp {
    CreateObject {
        type_ref: RecordPin,
        properties: Properties,
        sources: Vec<RecordPin>,
    },
    ReviseObject {
        expected: RecordPin,
        properties: Properties,
        sources: Vec<RecordPin>,
    },
    CreateRelation {
        relation_type: String,
        from: ContextRef,
        to: ContextRef,
        weight: f32,
        evidence: Vec<Ulid>,
        sources: Vec<RecordPin>,
    },
    ReviseRelation {
        expected: RecordPin,
        weight: Option<f32>,
        evidence: Option<Vec<Ulid>>,
        sources: Vec<RecordPin>,
    },
}

/// Outcome of an extraction transaction.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExtractionReceipt {
    pub operation_id: Ulid,
    pub created_objects: Vec<RecordPin>,
    pub revised_objects: Vec<RecordPin>,
    pub created_relations: Vec<RecordPin>,
    pub revised_relations: Vec<RecordPin>,
    /// True when this result was served from the memoised completed receipt.
    pub replayed: bool,
}

fn extraction_key(owner: &str, operation_id: Ulid) -> Vec<u8> {
    let mut key = owner_key(owner);
    segment(&mut key, operation_id.0.to_be_bytes().as_slice());
    key
}

impl ContextStore<'_> {
    /// Run a batch extraction atomically, memoised by `operation_id`.
    pub async fn extraction_transaction(
        &self,
        operation_id: Ulid,
        ops: Vec<ExtractionOp>,
    ) -> ContextResult<ExtractionReceipt> {
        if ops.is_empty() {
            return Err(ContextError::InvalidInput(
                "extraction transaction needs at least one operation".into(),
            ));
        }
        if ops.len() > MAX_BATCH_SIZE {
            return Err(ContextError::InvalidInput(format!(
                "extraction batch exceeds {MAX_BATCH_SIZE} operations"
            )));
        }
        let _guard = self.db.lock_write();
        let op_key = extraction_key(&self.access.owner, operation_id);

        // Completed memo -> replay.
        if let Some(bytes) = self.parts.extraction_receipts.get(op_key.clone())? {
            let mut receipt: ExtractionReceipt = decode(&bytes)?;
            let _ = self.parts.extraction_incomplete.remove(op_key.clone())?;
            receipt.replayed = true;
            return Ok(receipt);
        }
        // Crash recovery: a leftover Incomplete marker means the previous attempt
        // died before committing. Because batches commit atomically, no partial
        // rows exist; just clear the marker and re-execute.
        if self
            .parts
            .extraction_incomplete
            .get(op_key.clone())?
            .is_some()
        {
            self.parts.extraction_incomplete.remove(op_key.clone())?;
        }

        // Persist the Incomplete marker in its own durable commit first.
        let mut marker = self.db.durable_batch();
        marker.insert(&self.parts.extraction_incomplete, op_key.clone(), b"1");
        marker.commit()?;

        let result = self.build_extraction_batch(operation_id, &ops);
        match result {
            Ok((receipt, mut batch)) => {
                batch.insert(
                    &self.parts.extraction_receipts,
                    op_key.clone(),
                    encode(&receipt)?,
                );
                batch.remove(&self.parts.extraction_incomplete, op_key.clone());
                match batch.commit() {
                    Ok(()) => Ok(receipt),
                    Err(error) => {
                        let _ = self.parts.extraction_incomplete.remove(op_key)?;
                        Err(ContextError::from(error))
                    }
                }
            }
            Err(error) => Err(error),
        }
    }

    fn build_extraction_batch(
        &self,
        operation_id: Ulid,
        ops: &[ExtractionOp],
    ) -> ContextResult<(ExtractionReceipt, fjall::Batch)> {
        let mut batch = self.db.durable_batch();
        let mut receipt = ExtractionReceipt {
            operation_id,
            created_objects: Vec::new(),
            revised_objects: Vec::new(),
            created_relations: Vec::new(),
            revised_relations: Vec::new(),
            replayed: false,
        };
        for op in ops {
            match op {
                ExtractionOp::CreateObject {
                    type_ref,
                    properties,
                    sources,
                } => {
                    let type_def = self.type_definition_locked(type_ref)?;
                    self.validate_properties(&type_def, properties, &type_def.scope)?;
                    if !sources.is_empty() {
                        self.validate_sources(
                            &type_def.scope,
                            sources,
                            InformationKind::Observation,
                            None,
                        )?;
                    }
                    let pin = self.fresh_pin(RecordKind::Object);
                    self.write_record(
                        &mut batch,
                        &pin,
                        type_def.scope.clone(),
                        Some(type_ref.clone()),
                        properties,
                        sources.clone(),
                        None,
                    )?;
                    receipt.created_objects.push(pin);
                }
                ExtractionOp::ReviseObject {
                    expected,
                    properties,
                    sources,
                } => {
                    let (previous, pin) = self.next_revision(expected)?;
                    let type_ref = previous.type_pin.clone().ok_or_else(|| {
                        ContextError::InvalidInput("revised record has no type".into())
                    })?;
                    let type_def = self.type_definition_locked(&type_ref)?;
                    self.validate_properties(&type_def, properties, &previous.scope)?;
                    if !sources.is_empty() {
                        self.validate_sources(
                            &previous.scope,
                            sources,
                            InformationKind::Observation,
                            Some(&expected.record),
                        )?;
                    }
                    self.write_record(
                        &mut batch,
                        &pin,
                        previous.scope.clone(),
                        Some(type_ref),
                        properties,
                        sources.clone(),
                        None,
                    )?;
                    receipt.revised_objects.push(pin);
                }
                ExtractionOp::CreateRelation {
                    relation_type,
                    from,
                    to,
                    weight,
                    evidence: _,
                    sources,
                } => {
                    let type_ref = self.ensure_relation_type(relation_type)?;
                    let type_def = self.type_definition_locked(&type_ref)?;
                    let endpoints = Endpoints {
                        from: from.clone(),
                        to: to.clone(),
                    };
                    let mut properties: Properties = BTreeMap::new();
                    properties.insert("weight".into(), serde_json::json!(*weight));
                    if !sources.is_empty() {
                        self.validate_sources(
                            &type_def.scope,
                            sources,
                            InformationKind::Observation,
                            None,
                        )?;
                    }
                    let pin = self.fresh_pin(RecordKind::Relation);
                    self.write_record(
                        &mut batch,
                        &pin,
                        type_def.scope.clone(),
                        Some(type_ref),
                        &properties,
                        sources.clone(),
                        Some(endpoints),
                    )?;
                    receipt.created_relations.push(pin);
                }
                ExtractionOp::ReviseRelation {
                    expected,
                    weight,
                    evidence: _,
                    sources,
                } => {
                    let (previous, pin) = self.next_revision(expected)?;
                    let type_ref = previous.type_pin.clone().ok_or_else(|| {
                        ContextError::InvalidInput("revised relation has no type".into())
                    })?;
                    let mut properties: Properties = BTreeMap::new();
                    if let Some(weight) = weight {
                        properties.insert("weight".into(), serde_json::json!(*weight));
                    }
                    if !sources.is_empty() {
                        self.validate_sources(
                            &previous.scope,
                            sources,
                            InformationKind::Observation,
                            Some(&expected.record),
                        )?;
                    }
                    self.write_record(
                        &mut batch,
                        &pin,
                        previous.scope.clone(),
                        Some(type_ref),
                        &properties,
                        sources.clone(),
                        previous.endpoints.clone(),
                    )?;
                    receipt.revised_relations.push(pin);
                }
            }
        }
        Ok((receipt, batch))
    }

    /// Low-level record writer shared by object/relation extraction ops.
    #[allow(clippy::too_many_arguments)]
    fn write_record(
        &self,
        batch: &mut fjall::Batch,
        pin: &RecordPin,
        scope: Scope,
        type_pin: Option<RecordPin>,
        properties: &Properties,
        sources: Vec<RecordPin>,
        endpoints: Option<Endpoints>,
    ) -> ContextResult<()> {
        let now = crate::now_ms();
        let bytes = encode(properties)?;
        let header = RecordHeader {
            pin: pin.clone(),
            scope,
            state: RecordState::Active,
            type_pin,
            sources,
            provenance: self.provenance(),
            information: InformationKind::Observation,
            temporal: TemporalFacts::observed_at(now),
            recorded_at_ms: now,
            payload: crate::context::PayloadDescriptor {
                revision: pin.revision,
                bytes: bytes.len() as u64,
                digest: self.content_digest(&bytes),
            },
            endpoints,
            history: None,
            checkpoint: None,
        };
        self.put_payload(batch, pin, &bytes);
        self.put_header(batch, &header)?;
        self.replace_postings(batch, &header, Some(properties))?;
        self.replace_adjacency(batch, None, &header)?;
        Ok(())
    }

    fn ensure_relation_type(&self, relation_type: &str) -> ContextResult<RecordPin> {
        self.ensure_type_locked(
            relation_type,
            TypeKind::Relation {
                from_types: Vec::new(),
                to_types: Vec::new(),
            },
        )
    }
}
