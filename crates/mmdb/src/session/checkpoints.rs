use super::*;
use crate::records::codec::*;
use crate::{OperationId, RecordState, TemporalFacts};

impl SessionLog<'_> {
    pub fn read_checkpoint(&self, pin: &RecordPin) -> ContextResult<CheckpointRecord> {
        if pin.record.kind != RecordKind::Checkpoint {
            return Err(ContextError::InvalidInput("expected a checkpoint".into()));
        }
        let _guard = self.scoped.lock();
        let header = self.scoped.readable_header_locked(pin)?;
        let payload = decode(&self.scoped.small_payload_locked(&header)?)?;
        Ok(CheckpointRecord { header, payload })
    }

    /// Checkpoints are immutable recovery records, fixed to the committed session log.
    /// A correction invalidates older checkpoints for current recovery while
    /// exact historical reads remain available. Purge or event withdrawal
    /// also hides older payloads, including unlisted inputs to a summary.
    pub fn save_checkpoint(
        &self,
        operation: OperationId,
        input: CheckpointInput,
    ) -> ContextResult<WriteReceipt> {
        validate_name(&input.session, "checkpoint session")?;
        validate_name(&input.run, "checkpoint run")?;
        if self
            .scoped
            .access()
            .session
            .as_ref()
            .is_some_and(|session| *session != input.session)
            || self
                .scoped
                .access()
                .run
                .as_ref()
                .is_some_and(|run| *run != input.run)
        {
            return Err(ContextError::AccessDenied);
        }
        if self.scoped.access().parent_run.is_some() && input.scope != CheckpointScope::Run {
            return Err(ContextError::AccessDenied);
        }
        let bytes = encode(&input.payload)?;
        if bytes.len() > MAX_RECORD_BYTES {
            return Err(ContextError::InvalidInput(
                "checkpoint exceeds 128 KiB".into(),
            ));
        }
        let _guard = self.scoped.lock();
        let digest = self.scoped.digest("save-checkpoint", &input)?;
        if let Some(receipt) = self.scoped.replay(operation, digest)? {
            return Ok(receipt);
        }
        if self.scoped.availability_epoch_locked()? != input.expected_availability_epoch {
            return Err(ContextError::InvalidInput(
                "context changed while checkpoint was being generated".into(),
            ));
        }
        let key = checkpoint_key(
            &self.scoped.access().owner,
            &input.session,
            input.scope,
            &input.run,
        );
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
            self.scoped
                .header_locked(previous)?
                .checkpoint
                .ok_or_else(|| ContextError::Corrupt("checkpoint metadata missing".into()))?
                .window
                .checked_add(1)
                .ok_or(ContextError::BudgetExceeded)?
        } else {
            1
        };
        let anchor = self.event_anchor_locked(&input.session, input.sequence)?;
        let mut sources = input.sources;
        if !sources.contains(&anchor.pin) {
            sources.push(anchor.pin);
        }
        let source_scopes = self.scoped.validate_sources(
            &anchor.scope,
            &sources,
            InformationKind::Inference,
            None,
        )?;
        let pin = self.scoped.fresh_pin(RecordKind::Checkpoint);
        let now = super::super::now_ms();
        let mut batch = self.scoped.batch();
        let header = RecordHeader {
            pin: pin.clone(),
            scope: anchor.scope,
            state: RecordState::Active,
            type_pin: None,
            sources,
            source_scopes,
            provenance: self.scoped.provenance(),
            information: InformationKind::Inference,
            temporal: TemporalFacts::observed_at(now),
            recorded_at_ms: now,
            payload: self.scoped.put_payload(&mut batch, &pin, &bytes),
            endpoints: None,
            event: None,
            checkpoint: Some(CheckpointMetadata {
                scope: input.scope,
                session: input.session,
                run: input.run,
                window,
                sequence: input.sequence,
                previous,
                availability_epoch: self.scoped.availability_epoch_locked()?,
            }),
        };
        self.scoped.put_header(&mut batch, &header)?;
        batch.insert(&self.parts.checkpoints, key, encode(&pin)?);
        self.scoped.commit_operation(
            batch,
            operation,
            digest,
            WriteReceipt {
                pin,
                sequence: Some(input.sequence),
            },
        )
    }

    pub fn latest_checkpoint(&self, session: &str) -> ContextResult<Option<RecordHeader>> {
        validate_name(session, "session")?;
        let _guard = self.scoped.lock();
        self.parts
            .checkpoints
            .get(session_key(&self.scoped.access().owner, session))?
            .map(|bytes| self.scoped.header_locked(&decode(&bytes)?))
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
        let _guard = self.scoped.lock();
        self.parts
            .checkpoints
            .get(checkpoint_key(
                &self.scoped.access().owner,
                session,
                CheckpointScope::Run,
                run,
            ))?
            .map(|bytes| self.scoped.header_locked(&decode(&bytes)?))
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
    use crate::Store;
    use crate::{Actor, Scope};

    #[test]
    fn missing_and_stale_visibility_markers_do_not_restore_old_checkpoint_payloads() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("context");
        let db = Store::create(&path).unwrap();
        let store = db
            .session_log(ContextAccess::new("owner", Actor::System))
            .unwrap();
        let mut batch = store.scoped.batch();
        store.scoped.advance_epoch(&mut batch).unwrap();
        batch.commit().unwrap();
        let anchor = store
            .append(
                OperationId::new(),
                EventInput {
                    sources: vec![],
                    session: "session".into(),
                    event_id: "source".into(),
                    scope: Scope::Personal,
                    kind: EventKind::UserMessage,
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
                    sequence: 1,
                    expected_previous: None,
                    expected_availability_epoch: 1,
                    sources: vec![anchor.pin],
                    payload: serde_json::json!({"summary":"an older snapshot"}),
                },
            )
            .unwrap()
            .pin;
        assert!(store.read_checkpoint(&checkpoint).is_ok());
        // Simulate an older binary advancing only the original lifecycle key.
        let mut batch = store.scoped.batch();
        batch.insert(
            store.scoped.test_lifecycle(),
            owner_key("owner"),
            encode(&2u64).unwrap(),
        );
        batch.commit().unwrap();
        assert!(matches!(
            store.read_checkpoint(&checkpoint),
            Err(ContextError::Unavailable(_))
        ));
        let mut batch = store.scoped.batch();
        batch.remove(
            store.scoped.test_lifecycle(),
            store.scoped.checkpoint_redaction_key(),
        );
        batch.commit().unwrap();
        assert!(matches!(
            store.read_checkpoint(&checkpoint),
            Err(ContextError::Unavailable(_))
        ));
        let mut batch = store.scoped.batch();
        assert_eq!(store.scoped.advance_epoch(&mut batch).unwrap(), 3);
        batch.commit().unwrap();
        assert_eq!(store.scoped.checkpoint_redaction_epoch_locked().unwrap(), 2);
        drop(store);
        drop(db);
        let db = Store::open(path).unwrap();
        let store = db
            .session_log(ContextAccess::new("owner", Actor::System))
            .unwrap();
        assert!(matches!(
            store.read_checkpoint(&checkpoint),
            Err(ContextError::Unavailable(_))
        ));
        assert_eq!(store.scoped.checkpoint_redaction_epoch_locked().unwrap(), 2);
    }
}
