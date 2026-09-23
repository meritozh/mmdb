use super::super::{OperationId, RecordState, TemporalFacts};
use super::storage::*;
use super::*;

#[derive(serde::Serialize, serde::Deserialize)]
struct CheckpointVisibility {
    availability_epoch: u64,
    redaction_epoch: u64,
}

impl ContextStore<'_> {
    pub fn availability_epoch(&self) -> ContextResult<u64> {
        let _guard = self.db.write_lock.lock();
        self.availability_epoch_locked()
    }
    pub(crate) fn availability_epoch_locked(&self) -> ContextResult<u64> {
        self.parts
            .lifecycle
            .get(owner_key(&self.access.owner))?
            .map(|bytes| decode(&bytes))
            .transpose()
            .map(|epoch| epoch.unwrap_or(0))
    }

    fn checkpoint_redaction_key(&self) -> Vec<u8> {
        let mut key = owner_key(&self.access.owner);
        key.extend_from_slice(b"checkpoint-redaction");
        key
    }

    /// Old stores did not distinguish corrections from erasure. Their current
    /// epoch is the conservative baseline: never revive an older summary whose
    /// unlisted input may already have been purged by an older writer.
    pub(crate) fn checkpoint_redaction_epoch_locked(&self) -> ContextResult<u64> {
        let current = self.availability_epoch_locked()?;
        let tracked = self
            .parts
            .lifecycle
            .get(self.checkpoint_redaction_key())?
            .map(|bytes| decode::<CheckpointVisibility>(&bytes))
            .transpose()?;
        match tracked {
            Some(tracked) if tracked.availability_epoch == current => {
                if tracked.redaction_epoch > current {
                    return Err(ContextError::Corrupt(
                        "checkpoint redaction epoch exceeds current epoch".into(),
                    ));
                }
                Ok(tracked.redaction_epoch)
            }
            // An older writer can advance the original epoch without updating
            // this marker. Treat that gap as possible erasure, never as a safe
            // correction merely because a marker already exists.
            _ => Ok(current),
        }
    }

    pub(crate) fn advance_availability_epoch(
        &self,
        batch: &mut fjall::Batch,
    ) -> ContextResult<u64> {
        let current = self.availability_epoch_locked()?;
        let next = current.checked_add(1).ok_or(ContextError::BudgetExceeded)?;
        let redaction_epoch = self.checkpoint_redaction_epoch_locked()?;
        batch.insert(
            &self.parts.lifecycle,
            self.checkpoint_redaction_key(),
            encode(&CheckpointVisibility {
                availability_epoch: next,
                redaction_epoch,
            })?,
        );
        batch.insert(
            &self.parts.lifecycle,
            owner_key(&self.access.owner),
            encode(&next)?,
        );
        Ok(next)
    }

    pub(crate) fn invalidate_checkpoint_payloads(
        &self,
        batch: &mut fjall::Batch,
        epoch: u64,
    ) -> ContextResult<()> {
        batch.insert(
            &self.parts.lifecycle,
            self.checkpoint_redaction_key(),
            encode(&CheckpointVisibility {
                availability_epoch: epoch,
                redaction_epoch: epoch,
            })?,
        );
        Ok(())
    }

    /// Checkpoints are immutable recovery records, fixed to committed history.
    /// A correction invalidates older checkpoints for current recovery while
    /// exact historical reads remain available. Purge or history withdrawal
    /// also hides older payloads, including unlisted inputs to a summary.
    pub fn save_checkpoint(
        &self,
        operation: OperationId,
        input: CheckpointInput,
    ) -> ContextResult<WriteReceipt> {
        validate_name(&input.session, "checkpoint session")?;
        validate_name(&input.run, "checkpoint run")?;
        if self
            .access
            .session
            .as_ref()
            .is_some_and(|session| *session != input.session)
            || self
                .access
                .run
                .as_ref()
                .is_some_and(|run| *run != input.run)
        {
            return Err(ContextError::AccessDenied);
        }
        if self.access.parent_run.is_some() && input.scope != CheckpointScope::Run {
            return Err(ContextError::AccessDenied);
        }
        let bytes = encode(&input.payload)?;
        if bytes.len() > MAX_RECORD_BYTES {
            return Err(ContextError::InvalidInput(
                "checkpoint exceeds 128 KiB".into(),
            ));
        }
        let _guard = self.db.write_lock.lock();
        let digest = self.digest("save-checkpoint", &input)?;
        if let Some(receipt) = self.replay(operation, digest)? {
            return Ok(receipt);
        }
        if self.availability_epoch_locked()? != input.expected_availability_epoch {
            return Err(ContextError::InvalidInput(
                "context changed while checkpoint was being generated".into(),
            ));
        }
        let key = checkpoint_key(&self.access.owner, &input.session, input.scope, &input.run);
        let previous: Option<RecordPin> = self
            .parts
            .checkpoints
            .get(&key)?
            .map(|bytes| decode(&bytes))
            .transpose()?;
        if previous != input.expected_previous {
            return Err(ContextError::InvalidInput(
                "checkpoint parent changed".into(),
            ));
        }
        let window = if let Some(previous) = &previous {
            self.header_locked(previous)?
                .checkpoint
                .ok_or_else(|| ContextError::Corrupt("checkpoint metadata missing".into()))?
                .window
                .checked_add(1)
                .ok_or(ContextError::BudgetExceeded)?
        } else {
            1
        };
        let anchor = self.history_anchor_locked(&input.session, input.history_sequence)?;
        let mut sources = input.sources;
        if !sources.contains(&anchor.pin) {
            sources.push(anchor.pin);
        }
        self.validate_sources(&anchor.scope, &sources, InformationKind::Inference, None)?;
        let pin = self.fresh_pin(RecordKind::Checkpoint);
        let now = super::super::now_ms();
        let header = RecordHeader {
            pin: pin.clone(),
            scope: anchor.scope,
            state: RecordState::Active,
            type_pin: None,
            sources,
            provenance: self.provenance(),
            information: InformationKind::Inference,
            temporal: TemporalFacts::observed_at(now),
            recorded_at_ms: now,
            payload: PayloadDescriptor {
                revision: 1,
                bytes: bytes.len() as u64,
                digest: self.content_digest(&bytes),
            },
            endpoints: None,
            history: None,
            checkpoint: Some(CheckpointMetadata {
                scope: input.scope,
                session: input.session,
                run: input.run,
                window,
                history_sequence: input.history_sequence,
                previous,
                availability_epoch: self.availability_epoch_locked()?,
            }),
        };
        let mut batch = self.db.durable_batch();
        self.put_payload(&mut batch, &pin, &bytes);
        self.put_header(&mut batch, &header)?;
        batch.insert(&self.parts.checkpoints, key, encode(&pin)?);
        self.commit_operation(
            batch,
            operation,
            digest,
            WriteReceipt {
                pin,
                history_sequence: Some(input.history_sequence),
            },
        )
    }

    pub fn latest_checkpoint(&self, session: &str) -> ContextResult<Option<RecordHeader>> {
        validate_name(session, "session")?;
        let _guard = self.db.write_lock.lock();
        self.parts
            .checkpoints
            .get(session_key(&self.access.owner, session))?
            .map(|bytes| self.header_locked(&decode(&bytes)?))
            .transpose()
    }

    /// A run-local summary cannot advance another model window's session cursor.
    pub fn latest_run_checkpoint(
        &self,
        session: &str,
        run: &str,
    ) -> ContextResult<Option<RecordHeader>> {
        validate_name(session, "session")?;
        validate_name(run, "run")?;
        let _guard = self.db.write_lock.lock();
        self.parts
            .checkpoints
            .get(checkpoint_key(
                &self.access.owner,
                session,
                CheckpointScope::Run,
                run,
            ))?
            .map(|bytes| self.header_locked(&decode(&bytes)?))
            .transpose()
    }
}

fn checkpoint_key(owner: &str, session: &str, scope: CheckpointScope, run: &str) -> Vec<u8> {
    let mut key = session_key(owner, session);
    if scope == CheckpointScope::Run {
        segment(&mut key, run.as_bytes());
    }
    key
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::native_memory::{Actor, Scope};

    #[test]
    fn missing_and_stale_visibility_markers_do_not_restore_old_checkpoint_payloads() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("context");
        let db = MemoryDatabase::create_context(&path).unwrap();
        let store = db
            .context(ContextAccess::new("owner", Actor::System))
            .unwrap();
        let mut batch = db.durable_batch();
        store.advance_availability_epoch(&mut batch).unwrap();
        batch.commit().unwrap();
        let anchor = store
            .append_history(
                OperationId::new(),
                HistoryInput {
                    sources: vec![],
                    session: "session".into(),
                    event_id: "source".into(),
                    scope: Scope::Personal,
                    kind: HistoryKind::UserMessage,
                    media_type: "text/plain".into(),
                    occurred_at_ms: 1,
                    expected_sequence: None,
                },
                b"public input".as_slice(),
            )
            .unwrap();
        let checkpoint = store
            .save_checkpoint(
                OperationId::new(),
                CheckpointInput {
                    scope: CheckpointScope::Session,
                    session: "session".into(),
                    run: "run".into(),
                    history_sequence: 1,
                    expected_previous: None,
                    expected_availability_epoch: 1,
                    sources: vec![anchor.pin],
                    payload: serde_json::json!({"summary":"an older snapshot"}),
                },
            )
            .unwrap()
            .pin;
        assert!(store.read(&checkpoint).is_ok());
        // Simulate an older binary advancing only the original lifecycle key.
        store
            .parts
            .lifecycle
            .insert(owner_key("owner"), encode(&2u64).unwrap())
            .unwrap();
        assert!(matches!(
            store.read(&checkpoint),
            Err(ContextError::Unavailable(_))
        ));
        store
            .parts
            .lifecycle
            .remove(store.checkpoint_redaction_key())
            .unwrap();
        assert!(matches!(
            store.read(&checkpoint),
            Err(ContextError::Unavailable(_))
        ));
        let mut batch = db.durable_batch();
        assert_eq!(store.advance_availability_epoch(&mut batch).unwrap(), 3);
        batch.commit().unwrap();
        assert_eq!(store.checkpoint_redaction_epoch_locked().unwrap(), 2);
        drop(store);
        drop(db);
        let db = MemoryDatabase::open_context(path).unwrap();
        let store = db
            .context(ContextAccess::new("owner", Actor::System))
            .unwrap();
        assert!(matches!(
            store.read(&checkpoint),
            Err(ContextError::Unavailable(_))
        ));
        assert_eq!(store.checkpoint_redaction_epoch_locked().unwrap(), 2);
    }
}
