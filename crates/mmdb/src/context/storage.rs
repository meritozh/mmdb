use super::super::{Actor, OperationId, RecordState, Scope};
use super::*;
use serde::{de::DeserializeOwned, Serialize};
use std::collections::BTreeSet;
use ulid::Ulid;

pub(super) fn validate_name(value: &str, label: &str) -> ContextResult<()> {
    if value.trim().is_empty()
        || value.len() > MAX_CONTEXT_NAME_BYTES
        || value.chars().any(char::is_control)
    {
        return Err(ContextError::InvalidInput(format!(
            "{label} must have 1..={MAX_CONTEXT_NAME_BYTES} bytes without control characters"
        )));
    }
    Ok(())
}

pub(super) fn segment(key: &mut Vec<u8>, value: &[u8]) {
    key.extend_from_slice(&(value.len() as u32).to_be_bytes());
    key.extend_from_slice(value);
}

pub(super) fn owner_key(owner: &str) -> Vec<u8> {
    let mut key = Vec::new();
    segment(&mut key, owner.as_bytes());
    key
}

pub(super) fn kind_byte(kind: RecordKind) -> u8 {
    match kind {
        RecordKind::Type => 0,
        RecordKind::Object => 1,
        RecordKind::Relation => 2,
        RecordKind::History => 3,
        RecordKind::Checkpoint => 4,
        RecordKind::Action => 5,
        RecordKind::ActionExecution => 6,
    }
}

pub(super) fn record_key(record: &ContextRef) -> Vec<u8> {
    let mut key = owner_key(&record.owner);
    key.push(kind_byte(record.kind));
    key.extend_from_slice(&record.id.0.to_be_bytes());
    key
}

pub(super) fn revision_key(pin: &RecordPin) -> Vec<u8> {
    let mut key = record_key(&pin.record);
    key.extend_from_slice(&pin.revision.to_be_bytes());
    key
}

pub(super) fn payload_key(pin: &RecordPin, chunk: u64) -> Vec<u8> {
    let mut key = revision_key(pin);
    key.extend_from_slice(&chunk.to_be_bytes());
    key
}

pub(super) fn operation_key(owner: &str, operation: OperationId) -> Vec<u8> {
    let mut key = owner_key(owner);
    key.extend_from_slice(&operation.0 .0.to_be_bytes());
    key
}

pub(super) fn session_key(owner: &str, session: &str) -> Vec<u8> {
    let mut key = owner_key(owner);
    segment(&mut key, session.as_bytes());
    key
}

pub(super) fn decode<T: DeserializeOwned>(bytes: &[u8]) -> ContextResult<T> {
    serde_json::from_slice(bytes)
        .map_err(|_| ContextError::Corrupt("invalid persisted record".into()))
}

pub(super) fn encode<T: Serialize>(value: &T) -> ContextResult<Vec<u8>> {
    Ok(serde_json::to_vec(value)?)
}

impl ContextStore<'_> {
    pub(super) fn check_scope(&self, scope: &Scope) -> ContextResult<()> {
        if !self.access.scopes.contains(scope) {
            return Err(ContextError::AccessDenied);
        }
        Ok(())
    }

    pub(super) fn check_ref(&self, record: &ContextRef) -> ContextResult<()> {
        if record.owner != self.access.owner || record.era != self.db.era_id {
            return Err(ContextError::AccessDenied);
        }
        Ok(())
    }

    pub(super) fn fresh_pin(&self, kind: RecordKind) -> RecordPin {
        RecordPin {
            record: ContextRef {
                era: self.db.era_id,
                owner: self.access.owner.clone(),
                kind,
                id: Ulid::new(),
            },
            revision: 1,
        }
    }

    pub(super) fn provenance(&self) -> Provenance {
        Provenance {
            actor: self.access.actor,
            agent: self.access.agent.clone(),
            session: self.access.session.clone(),
            run: self.access.run.clone(),
            parent_run: self.access.parent_run.clone(),
        }
    }

    pub(super) fn head_locked(&self, record: &ContextRef) -> ContextResult<RecordHeader> {
        self.check_ref(record)?;
        let revision = self
            .parts
            .heads
            .get(record_key(record))?
            .ok_or_else(|| ContextError::NotFound(record.clone()))?;
        let revision: [u8; 8] = revision
            .as_ref()
            .try_into()
            .map_err(|_| ContextError::Corrupt("invalid head revision".into()))?;
        self.header_locked(&RecordPin {
            record: record.clone(),
            revision: u64::from_be_bytes(revision),
        })
    }

    pub(super) fn header_locked(&self, pin: &RecordPin) -> ContextResult<RecordHeader> {
        self.check_ref(&pin.record)?;
        if pin.revision == 0 {
            return Err(ContextError::InvalidInput(
                "revision must be positive".into(),
            ));
        }
        let bytes = self
            .parts
            .headers
            .get(revision_key(pin))?
            .ok_or_else(|| ContextError::NotFound(pin.record.clone()))?;
        let header: RecordHeader = decode(&bytes)?;
        if header.pin != *pin {
            return Err(ContextError::Corrupt(
                "record key and identity disagree".into(),
            ));
        }
        self.check_scope(&header.scope)?;
        Ok(header)
    }

    pub fn head(&self, record: &ContextRef) -> ContextResult<RecordHeader> {
        let _guard = self.db.write_lock.lock();
        self.head_locked(record)
    }

    pub fn inspect_header(&self, pin: &RecordPin) -> ContextResult<RecordHeader> {
        let _guard = self.db.write_lock.lock();
        self.header_locked(pin)
    }

    pub(super) fn available_locked(&self, pin: &RecordPin, active: bool) -> ContextResult<bool> {
        self.available_walk(pin, active, None, 0, &mut 0, &mut BTreeSet::new())
    }

    fn available_walk(
        &self,
        pin: &RecordPin,
        active: bool,
        excluded: Option<&ContextRef>,
        depth: usize,
        count: &mut usize,
        path: &mut BTreeSet<RecordPin>,
    ) -> ContextResult<bool> {
        *count += 1;
        if depth > 32 || *count > 4096 {
            return Err(ContextError::BudgetExceeded);
        }
        if excluded == Some(&pin.record) || !path.insert(pin.clone()) {
            return Ok(false);
        }
        let header = match self.header_locked(pin) {
            Ok(header) => header,
            Err(ContextError::NotFound(_) | ContextError::AccessDenied) => {
                path.remove(pin);
                return Ok(false);
            }
            Err(error) => return Err(error),
        };
        let head = self.head_locked(&pin.record)?;
        if header
            .checkpoint
            .as_ref()
            .map(|checkpoint| {
                if active {
                    self.availability_epoch_locked()
                        .map(|epoch| epoch != checkpoint.availability_epoch)
                } else {
                    self.checkpoint_redaction_epoch_locked()
                        .map(|epoch| checkpoint.availability_epoch < epoch)
                }
            })
            .transpose()?
            .unwrap_or(false)
        {
            path.remove(pin);
            return Ok(false);
        }
        if let Some(history) = &header.history {
            if !self.message_available_locked(history)? {
                path.remove(pin);
                return Ok(false);
            }
        }
        if head.state == RecordState::Purged
            || header.state == RecordState::Purged
            || (active
                && (head.state != RecordState::Active
                    || head.pin.revision != pin.revision
                    || header.state != RecordState::Active))
        {
            path.remove(pin);
            return Ok(false);
        }
        for source in &header.sources {
            if !self.available_walk(source, active, excluded, depth + 1, count, path)? {
                path.remove(pin);
                return Ok(false);
            }
        }
        path.remove(pin);
        Ok(true)
    }

    pub(super) fn validate_sources(
        &self,
        scope: &Scope,
        sources: &[RecordPin],
        information: InformationKind,
        revising: Option<&ContextRef>,
    ) -> ContextResult<()> {
        self.check_scope(scope)?;
        if sources.is_empty() || sources.len() > MAX_CONTEXT_SOURCES {
            return Err(ContextError::InvalidInput(
                "context requires 1..=64 source references".into(),
            ));
        }
        if information == InformationKind::Instruction
            && !matches!(self.access.actor, Actor::User | Actor::Operator)
        {
            return Err(ContextError::AccessDenied);
        }
        let mut count = 1;
        let mut path = BTreeSet::new();
        for source in sources {
            if source.record.kind == RecordKind::Type {
                return Err(ContextError::InvalidInput(
                    "type definitions are not observation sources".into(),
                ));
            }
            let header = self.header_locked(source)?;
            if header.scope != *scope && header.scope != Scope::Personal {
                return Err(ContextError::AccessDenied);
            }
            if !self.available_walk(source, true, revising, 1, &mut count, &mut path)? {
                if revising.is_some() {
                    return Err(ContextError::InvalidInput("revision sources must be available and cannot depend on the revised record".into()));
                }
                return Err(ContextError::Unavailable(source.record.clone()));
            }
        }
        Ok(())
    }

    /// Original snapshots may copy a historical version or a type descriptor.
    /// They retain its exact pin; purge still invalidates every copied payload.
    pub(super) fn validate_history_sources(
        &self,
        scope: &Scope,
        sources: &[RecordPin],
    ) -> ContextResult<()> {
        self.check_scope(scope)?;
        if sources.len() > MAX_CONTEXT_SOURCES {
            return Err(ContextError::InvalidInput(
                "history has too many source references".into(),
            ));
        }
        let mut count = 1;
        let mut path = BTreeSet::new();
        for source in sources {
            let header = self.header_locked(source)?;
            if header.scope != *scope && header.scope != Scope::Personal {
                return Err(ContextError::AccessDenied);
            }
            if !self.available_walk(source, false, None, 1, &mut count, &mut path)? {
                return Err(ContextError::Unavailable(source.record.clone()));
            }
        }
        Ok(())
    }

    pub(super) fn digest<T: Serialize>(&self, domain: &str, input: &T) -> ContextResult<[u8; 32]> {
        let mut h = blake3::Hasher::new_keyed(&self.db.digest_key);
        h.update(b"mmdb-context-v1\0");
        h.update(domain.as_bytes());
        h.update(&encode(&(&self.access.owner, self.provenance(), input))?);
        Ok(*h.finalize().as_bytes())
    }

    pub(super) fn content_digest(&self, bytes: &[u8]) -> [u8; 32] {
        *blake3::keyed_hash(&self.db.digest_key, bytes).as_bytes()
    }

    pub(super) fn stored_operation(
        &self,
        operation: OperationId,
    ) -> ContextResult<Option<StoredOperation>> {
        self.parts
            .operations
            .get(operation_key(&self.access.owner, operation))?
            .map(|bytes| decode(&bytes))
            .transpose()
    }

    pub fn operation_receipt(&self, operation: OperationId) -> ContextResult<Option<WriteReceipt>> {
        let _guard = self.db.write_lock.lock();
        let stored = self.stored_operation(operation)?;
        if let Some(stored) = &stored {
            self.header_locked(&stored.receipt.pin)?;
        }
        Ok(stored.map(|stored| stored.receipt))
    }

    pub(super) fn replay(
        &self,
        operation: OperationId,
        digest: [u8; 32],
    ) -> ContextResult<Option<WriteReceipt>> {
        let Some(stored) = self.stored_operation(operation)? else {
            return Ok(None);
        };
        if stored.digest != digest {
            return Err(ContextError::IdempotencyConflict);
        }
        self.header_locked(&stored.receipt.pin)?;
        Ok(Some(stored.receipt))
    }

    pub(super) fn commit_operation(
        &self,
        mut batch: fjall::Batch,
        operation: OperationId,
        digest: [u8; 32],
        receipt: WriteReceipt,
    ) -> ContextResult<WriteReceipt> {
        batch.insert(
            &self.parts.operations,
            operation_key(&self.access.owner, operation),
            encode(&StoredOperation {
                digest,
                receipt: receipt.clone(),
            })?,
        );
        batch
            .commit()
            .map_err(|_| ContextError::CommitUnknown(operation))?;
        #[cfg(test)]
        if self
            .parts
            .fail_commit_ack
            .swap(false, std::sync::atomic::Ordering::SeqCst)
        {
            return Err(ContextError::CommitUnknown(operation));
        }
        Ok(receipt)
    }

    pub(super) fn put_header(
        &self,
        batch: &mut fjall::Batch,
        header: &RecordHeader,
    ) -> ContextResult<()> {
        batch.insert(
            &self.parts.headers,
            revision_key(&header.pin),
            encode(header)?,
        );
        batch.insert(
            &self.parts.heads,
            record_key(&header.pin.record),
            header.pin.revision.to_be_bytes(),
        );
        Ok(())
    }

    pub(super) fn put_payload(&self, batch: &mut fjall::Batch, pin: &RecordPin, bytes: &[u8]) {
        for (chunk, bytes) in bytes.chunks(PAYLOAD_CHUNK_BYTES).enumerate() {
            let mut value = Vec::with_capacity(32 + bytes.len());
            value.extend_from_slice(&self.content_digest(bytes));
            value.extend_from_slice(bytes);
            batch.insert(&self.parts.payloads, payload_key(pin, chunk as u64), value);
        }
    }

    pub(super) fn payload_slice_locked(
        &self,
        header: &RecordHeader,
        offset: u64,
        limit: usize,
    ) -> ContextResult<PayloadPage> {
        if limit == 0 || limit > MAX_PAYLOAD_PAGE_BYTES || offset > header.payload.bytes {
            return Err(ContextError::InvalidInput(
                "invalid bounded payload range".into(),
            ));
        }
        let end = header
            .payload
            .bytes
            .min(offset.saturating_add(limit as u64));
        let payload_pin = RecordPin {
            record: header.pin.record.clone(),
            revision: header.payload.revision,
        };
        let mut bytes = Vec::with_capacity((end - offset) as usize);
        let mut at = offset;
        while at < end {
            let chunk_index = at / PAYLOAD_CHUNK_BYTES as u64;
            let value = self
                .parts
                .payloads
                .get(payload_key(&payload_pin, chunk_index))?
                .ok_or_else(|| ContextError::Corrupt("missing committed payload chunk".into()))?;
            if value.len() < 32 || value.len() > PAYLOAD_CHUNK_BYTES + 32 {
                return Err(ContextError::Corrupt("invalid payload chunk length".into()));
            }
            let (digest, chunk) = value.split_at(32);
            if digest != self.content_digest(chunk) {
                return Err(ContextError::Corrupt(
                    "payload chunk checksum mismatch".into(),
                ));
            }
            let chunk_offset = (at % PAYLOAD_CHUNK_BYTES as u64) as usize;
            let count = ((end - at) as usize).min(chunk.len().saturating_sub(chunk_offset));
            if count == 0 {
                return Err(ContextError::Corrupt("short payload chunk".into()));
            }
            bytes.extend_from_slice(&chunk[chunk_offset..chunk_offset + count]);
            at += count as u64;
        }
        Ok(PayloadPage {
            bytes,
            next_offset: (end < header.payload.bytes).then_some(end),
            total_bytes: header.payload.bytes,
        })
    }

    pub fn read_payload(
        &self,
        pin: &RecordPin,
        offset: u64,
        limit: usize,
    ) -> ContextResult<PayloadPage> {
        let _guard = self.db.write_lock.lock();
        let header = self.header_locked(pin)?;
        if !self.available_locked(pin, false)? {
            return Err(ContextError::Unavailable(pin.record.clone()));
        }
        self.payload_slice_locked(&header, offset, limit)
    }

    pub(super) fn small_payload_locked(&self, header: &RecordHeader) -> ContextResult<Vec<u8>> {
        if header.payload.bytes > MAX_RECORD_BYTES as u64 {
            return Err(ContextError::Corrupt("oversize structured payload".into()));
        }
        let page = self.payload_slice_locked(header, 0, MAX_RECORD_BYTES)?;
        if page.next_offset.is_some() || self.content_digest(&page.bytes) != header.payload.digest {
            return Err(ContextError::Corrupt(
                "structured payload checksum mismatch".into(),
            ));
        }
        Ok(page.bytes)
    }

    pub(super) fn read_locked(&self, pin: &RecordPin) -> ContextResult<ContextRecord> {
        let header = self.header_locked(pin)?;
        if !self.available_locked(pin, false)? {
            return Err(ContextError::Unavailable(pin.record.clone()));
        }
        let body = match pin.record.kind {
            RecordKind::Type => RecordBody::Type(decode(&self.small_payload_locked(&header)?)?),
            RecordKind::Object => RecordBody::Object(decode(&self.small_payload_locked(&header)?)?),
            RecordKind::Relation => {
                RecordBody::Relation(decode(&self.small_payload_locked(&header)?)?)
            }
            RecordKind::History => RecordBody::History(
                header
                    .history
                    .clone()
                    .ok_or_else(|| ContextError::Corrupt("history metadata missing".into()))?,
            ),
            RecordKind::Action => RecordBody::Action(decode(&self.small_payload_locked(&header)?)?),
            RecordKind::ActionExecution => {
                RecordBody::ActionExecution(decode(&self.small_payload_locked(&header)?)?)
            }
            RecordKind::Checkpoint => {
                RecordBody::Checkpoint(decode(&self.small_payload_locked(&header)?)?)
            }
        };
        let current_state = self.head_locked(&pin.record)?.state;
        Ok(ContextRecord {
            header,
            current_state,
            body,
        })
    }

    pub fn read(&self, pin: &RecordPin) -> ContextResult<ContextRecord> {
        let _guard = self.db.write_lock.lock();
        self.read_locked(pin)
    }

    pub(super) fn next_revision(
        &self,
        expected: &RecordPin,
    ) -> ContextResult<(RecordHeader, RecordPin)> {
        let head = self.head_locked(&expected.record)?;
        if head.pin.revision != expected.revision {
            return Err(ContextError::RevisionConflict {
                expected: expected.revision,
                actual: head.pin.revision,
            });
        }
        if head.state == RecordState::Purged {
            return Err(ContextError::Unavailable(expected.record.clone()));
        }
        let revision = expected
            .revision
            .checked_add(1)
            .ok_or_else(|| ContextError::Corrupt("revision overflow".into()))?;
        Ok((
            head,
            RecordPin {
                record: expected.record.clone(),
                revision,
            },
        ))
    }
}
