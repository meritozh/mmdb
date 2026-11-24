use super::codec::*;
use super::*;
use crate::OperationId;
use mmdb_kernel::Batch;

#[derive(serde::Serialize, serde::Deserialize)]
struct CheckpointVisibility {
    availability_epoch: u64,
    redaction_epoch: u64,
}

impl Scoped<'_> {
    pub(crate) fn availability_locked(&self) -> ContextResult<Availability> {
        let epoch = self.availability_epoch_locked()?;
        Ok(Availability {
            epoch,
            redaction_epoch: self.redaction_epoch_at(epoch)?,
        })
    }
    pub(crate) fn availability_epoch_locked(&self) -> ContextResult<u64> {
        self.records
            .lifecycle
            .get(owner_key(&self.access().owner))?
            .map(|bytes| decode(&bytes))
            .transpose()
            .map(|epoch| epoch.unwrap_or(0))
    }

    pub(crate) fn checkpoint_redaction_key(&self) -> Vec<u8> {
        let mut key = owner_key(&self.access().owner);
        key.extend_from_slice(b"checkpoint-redaction");
        key
    }

    /// Old stores did not distinguish corrections from erasure. Their current
    /// epoch is the conservative baseline: never revive an older summary whose
    /// unlisted input may already have been purged by an older writer.
    pub(crate) fn checkpoint_redaction_epoch_locked(&self) -> ContextResult<u64> {
        self.redaction_epoch_at(self.availability_epoch_locked()?)
    }

    fn redaction_epoch_at(&self, current: u64) -> ContextResult<u64> {
        let tracked = self
            .records
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

    pub(crate) fn advance_epoch(&self, batch: &mut mmdb_kernel::Batch) -> ContextResult<u64> {
        let current = self.availability_epoch_locked()?;
        let next = current.checked_add(1).ok_or(ContextError::BudgetExceeded)?;
        let redaction_epoch = self.checkpoint_redaction_epoch_locked()?;
        batch.insert(
            &self.records.lifecycle,
            self.checkpoint_redaction_key(),
            encode(&CheckpointVisibility {
                availability_epoch: next,
                redaction_epoch,
            })?,
        );
        batch.insert(
            &self.records.lifecycle,
            owner_key(&self.access().owner),
            encode(&next)?,
        );
        Ok(next)
    }

    pub(crate) fn invalidate_checkpoint_payloads(
        &self,
        batch: &mut mmdb_kernel::Batch,
        epoch: u64,
    ) -> ContextResult<()> {
        batch.insert(
            &self.records.lifecycle,
            self.checkpoint_redaction_key(),
            encode(&CheckpointVisibility {
                availability_epoch: epoch,
                redaction_epoch: epoch,
            })?,
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
