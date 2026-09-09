use super::super::{OperationId, RecordState, TemporalFacts};
use super::storage::*;
use super::*;

impl ContextStore<'_> {
    pub fn availability_epoch(&self) -> ContextResult<u64> {
        let _guard = self.db.write_lock.lock();
        self.availability_epoch_locked()
    }
    pub(super) fn availability_epoch_locked(&self) -> ContextResult<u64> {
        self.parts
            .lifecycle
            .get(owner_key(&self.access.owner))?
            .map(|bytes| decode(&bytes))
            .transpose()
            .map(|epoch| epoch.unwrap_or(0))
    }

    pub(super) fn advance_availability_epoch(&self, batch: &mut fjall::Batch) -> ContextResult<()> {
        let next = self
            .availability_epoch_locked()?
            .checked_add(1)
            .ok_or(ContextError::BudgetExceeded)?;
        batch.insert(
            &self.parts.lifecycle,
            owner_key(&self.access.owner),
            encode(&next)?,
        );
        Ok(())
    }

    /// Checkpoints are immutable recovery records, fixed to committed history.
    /// A correction, retraction or purge conservatively invalidates older
    /// owner checkpoints, including summaries of streamed history fragments.
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
