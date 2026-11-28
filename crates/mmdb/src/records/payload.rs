use super::chunks::ChunkSpace;
use super::codec::*;
use super::*;
use crate::OperationId;
use mmdb_kernel::Batch;
use std::io::Read;

impl StreamedPayload {
    pub(crate) fn publish(&self, batch: &mut Batch) {
        if let Some((pin, pending)) = &self.upload {
            batch.remove(pending, record_key(&pin.record));
        }
    }
}

impl Scoped<'_> {
    fn payload_space(&self, pin: &RecordPin) -> ChunkSpace<'_> {
        ChunkSpace {
            partition: &self.table(pin.record.kind.module()).payloads,
            prefix: revision_key(pin),
        }
    }
    pub(crate) fn put_payload(
        &self,
        batch: &mut mmdb_kernel::Batch,
        pin: &RecordPin,
        bytes: &[u8],
    ) -> PayloadDescriptor {
        let chunks = self.put_chunks(batch, &self.payload_space(pin), bytes);
        PayloadDescriptor {
            revision: pin.revision,
            bytes: chunks.bytes,
            digest: chunks.digest,
        }
    }

    pub(crate) fn payload_slice_locked(
        &self,
        header: &RecordHeader,
        offset: u64,
        limit: usize,
    ) -> ContextResult<PayloadPage> {
        let payload_pin = RecordPin {
            record: header.pin.record.clone(),
            revision: header.payload.revision,
        };
        self.read_chunks(
            &self.payload_space(&payload_pin),
            header.payload.bytes,
            offset,
            limit,
        )
    }

    pub(crate) fn small_payload_locked(&self, header: &RecordHeader) -> ContextResult<Vec<u8>> {
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

    pub(crate) fn stream_payload<R: Read>(
        &self,
        payload: R,
        upload: Option<(&RecordPin, OperationId)>,
    ) -> ContextResult<StreamedPayload> {
        if let Some((pin, operation)) = upload {
            let mut batch = self.records.kernel.batch();
            batch.insert(
                &self.table(pin.record.kind.module()).pending,
                record_key(&pin.record),
                encode(&StagedUpload {
                    pin: pin.clone(),
                    operation,
                    purge: false,
                })?,
            );
            batch.commit()?;
        }
        let space = upload.map(|(pin, _)| self.payload_space(pin));
        let chunks = self.write_chunks(space.as_ref(), payload)?;
        Ok(StreamedPayload {
            descriptor: PayloadDescriptor {
                revision: upload.map_or(1, |(pin, _)| pin.revision),
                bytes: chunks.bytes,
                digest: chunks.digest,
            },
            upload: upload.map(|(pin, _)| {
                (
                    pin.clone(),
                    self.table(pin.record.kind.module()).pending.clone(),
                )
            }),
        })
    }

    pub(crate) fn recover_payloads(
        &self,
        module: StoreModule,
        max_chunks: usize,
        max_uploads: usize,
    ) -> ContextResult<RecoveryReport> {
        if max_chunks == 0 || max_chunks > 4096 || max_uploads == 0 || max_uploads > 100 {
            return Err(ContextError::InvalidInput("invalid cleanup budget".into()));
        }
        let _guard = self.records.kernel.write_lock();
        let mut report = RecoveryReport {
            chunks_removed: 0,
            uploads_removed: 0,
            more: false,
        };
        let owner = owner_key(&self.access().owner);
        let table = self.table(module);
        for entry in table.pending.prefix(owner) {
            let (key, bytes) = entry?;
            if report.chunks_removed == max_chunks || report.uploads_removed == max_uploads {
                report.more = true;
                break;
            }
            let staged: StagedUpload = decode(&bytes)?;
            if staged.pin.record.kind.module() != module {
                return Err(ContextError::Corrupt(
                    "pending upload belongs to another module".into(),
                ));
            }
            self.check_ref(&staged.pin.record)?;
            let head = self.head_locked(&staged.pin.record);
            match (staged.purge, head) {
                (true, Ok(header)) if header.state == RecordState::Purged => {}
                (false, Err(ContextError::NotFound(_))) => {}
                (_, Err(error)) => return Err(error),
                _ => {
                    return Err(ContextError::Corrupt(
                        "cleanup entry points at committed live data".into(),
                    ))
                }
            }
            let prefix = if staged.purge {
                record_key(&staged.pin.record)
            } else {
                revision_key(&staged.pin)
            };
            let remaining = max_chunks - report.chunks_removed;
            let mut batch = self.records.kernel.batch();
            let chunks = self.remove_chunks(
                &mut batch,
                &ChunkSpace {
                    partition: &table.payloads,
                    prefix,
                },
                remaining,
            )?;
            report.chunks_removed += chunks.removed;
            if chunks.complete {
                batch.remove(&table.pending, key);
                report.uploads_removed += 1;
            }
            batch.commit()?;
            if !chunks.complete {
                report.more = true;
                break;
            }
        }
        Ok(report)
    }
}
