use super::codec::*;
use super::*;
use crate::OperationId;
use mmdb_kernel::Batch;

// Owner keys begin with a four-byte length. This store-wide key is disjoint
// from every valid owner prefix.
const MEMORY_RESETS: &[u8] = b"memory-resets";

impl Records {
    fn counter(&self, key: impl AsRef<[u8]>) -> ContextResult<u64> {
        self.lifecycle
            .get(key)?
            .map(|bytes| decode(&bytes))
            .transpose()
            .map(Option::unwrap_or_default)
    }

    pub(crate) fn count_memory_reset(&self, batch: &mut Batch) -> ContextResult<()> {
        let resets = self
            .counter(MEMORY_RESETS)?
            .checked_add(1)
            .ok_or(ContextError::BudgetExceeded)?;
        batch.insert(&self.lifecycle, MEMORY_RESETS, encode(&resets)?);
        Ok(())
    }
}

impl Scoped<'_> {
    pub(crate) fn availability_locked(&self) -> ContextResult<Availability> {
        let epoch = self.availability_epoch_locked()?;
        let redaction_epoch = self.records.counter(self.checkpoint_redaction_key())?;
        if redaction_epoch > epoch {
            return Err(ContextError::Corrupt(
                "checkpoint redaction epoch exceeds current epoch".into(),
            ));
        }
        Ok(Availability {
            epoch,
            redaction_epoch,
        })
    }

    pub(crate) fn availability_epoch_locked(&self) -> ContextResult<u64> {
        let local = self.records.counter(owner_key(&self.access().owner))?;
        self.effective_epoch(local)
    }

    fn effective_epoch(&self, local: u64) -> ContextResult<u64> {
        local
            .checked_add(self.records.counter(MEMORY_RESETS)?)
            .ok_or(ContextError::BudgetExceeded)
    }

    fn checkpoint_redaction_key(&self) -> Vec<u8> {
        let mut key = owner_key(&self.access().owner);
        key.extend_from_slice(b"checkpoint-redaction");
        key
    }

    pub(crate) fn advance_epoch(&self, batch: &mut Batch) -> ContextResult<u64> {
        let key = owner_key(&self.access().owner);
        let local = self
            .records
            .counter(&key)?
            .checked_add(1)
            .ok_or(ContextError::BudgetExceeded)?;
        let epoch = self.effective_epoch(local)?;
        batch.insert(&self.records.lifecycle, key, encode(&local)?);
        Ok(epoch)
    }

    pub(crate) fn invalidate_checkpoint_payloads(
        &self,
        batch: &mut Batch,
        epoch: u64,
    ) -> ContextResult<()> {
        batch.insert(
            &self.records.lifecycle,
            self.checkpoint_redaction_key(),
            encode(&epoch)?,
        );
        Ok(())
    }

    pub(crate) fn commit_state_change(
        &self,
        operation: OperationId,
        expected: RecordPin,
        state: RecordState,
        redacts: bool,
        update_indexes: impl FnOnce(&mut Batch, &RecordHeader, &RecordHeader) -> ContextResult<()>,
    ) -> ContextResult<WriteReceipt> {
        let _guard = self.lock();
        let digest = self.digest("set-context-state", &(&expected, state))?;
        if let Some(receipt) = self.replay(operation, digest)? {
            return Ok(receipt);
        }
        let mut batch = self.batch();
        let (previous, header) =
            self.state_change(&mut batch, operation, &expected, state, redacts)?;
        update_indexes(&mut batch, &previous, &header)?;
        self.commit_operation(
            batch,
            operation,
            digest,
            WriteReceipt {
                pin: header.pin,
                sequence: header.event.as_ref().map(|event| event.sequence),
            },
        )
    }

    pub(crate) fn state_change(
        &self,
        batch: &mut Batch,
        operation: OperationId,
        expected: &RecordPin,
        state: RecordState,
        redacts: bool,
    ) -> ContextResult<(RecordHeader, RecordHeader)> {
        let (previous, pin) = self.next_revision(expected)?;
        if state == RecordState::Retracted
            && previous.information == InformationKind::Instruction
            && !matches!(self.access().actor, Actor::User | Actor::Operator)
        {
            return Err(ContextError::AccessDenied);
        }
        let mut header = previous.clone();
        header.pin = pin;
        header.state = state;
        header.provenance = self.provenance();
        header.recorded_at_ms = crate::now_ms();
        let epoch = self.advance_epoch(batch)?;
        if redacts {
            self.invalidate_checkpoint_payloads(batch, epoch)?;
        }
        self.put_header(batch, &header)?;
        if state == RecordState::Purged {
            batch.insert(
                &self.table(expected.record.kind.module()).pending,
                record_key(&header.pin.record),
                encode(&StagedUpload {
                    pin: header.pin.clone(),
                    operation,
                    purge: true,
                })?,
            );
        }
        Ok((previous, header))
    }
}
