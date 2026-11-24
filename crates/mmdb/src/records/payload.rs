use super::codec::*;
use super::*;
use crate::OperationId;
use mmdb_kernel::Batch;
use std::io::{ErrorKind, Read};

impl StreamedPayload {
    pub(crate) fn publish(&self, batch: &mut Batch) {
        if let Some((pin, pending)) = &self.upload {
            batch.remove(pending, record_key(&pin.record));
        }
    }
}

impl Scoped<'_> {
    pub(crate) fn content_digest(&self, bytes: &[u8]) -> [u8; 32] {
        *self
            .records
            .kernel
            .keyed_hasher("context-content")
            .update(bytes)
            .finalize()
            .as_bytes()
    }

    pub(crate) fn put_payload(
        &self,
        batch: &mut mmdb_kernel::Batch,
        pin: &RecordPin,
        bytes: &[u8],
    ) -> PayloadDescriptor {
        for (chunk, bytes) in bytes.chunks(PAYLOAD_CHUNK_BYTES).enumerate() {
            self.put_chunk(batch, pin, chunk as u64, bytes);
        }
        PayloadDescriptor {
            revision: pin.revision,
            bytes: bytes.len() as u64,
            digest: self.content_digest(bytes),
        }
    }

    fn put_chunk(&self, batch: &mut Batch, pin: &RecordPin, chunk: u64, bytes: &[u8]) {
        let mut value = Vec::with_capacity(32 + bytes.len());
        value.extend_from_slice(&self.content_digest(bytes));
        value.extend_from_slice(bytes);
        batch.insert(
            &self.table(pin.record.kind.module()).payloads,
            payload_key(pin, chunk),
            value,
        );
    }

    pub(crate) fn payload_slice_locked(
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
                .table(payload_pin.record.kind.module())
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
        mut payload: R,
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
        let mut buffer = vec![0; PAYLOAD_CHUNK_BYTES];
        let mut hasher = self.records.kernel.keyed_hasher("context-history-payload");
        let mut length = 0_u64;
        let mut chunk = 0_u64;
        loop {
            let mut used = 0;
            while used < buffer.len() {
                match payload.read(&mut buffer[used..]) {
                    Ok(0) => break,
                    Ok(count) => used += count,
                    Err(error) if error.kind() == ErrorKind::Interrupted => continue,
                    Err(error) => return Err(error.into()),
                }
            }
            if used == 0 {
                break;
            }
            hasher.update(&buffer[..used]);
            length = length
                .checked_add(used as u64)
                .ok_or(ContextError::BudgetExceeded)?;
            if let Some((pin, _)) = upload {
                let mut batch = self.batch();
                self.put_chunk(&mut batch, pin, chunk, &buffer[..used]);
                batch.commit()?;
            }
            chunk += 1;
        }
        let payload_digest = *hasher.finalize().as_bytes();
        Ok(StreamedPayload {
            descriptor: PayloadDescriptor {
                revision: upload.map_or(1, |(pin, _)| pin.revision),
                bytes: length,
                digest: payload_digest,
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
            let mut complete = true;
            for (seen, chunk) in table.payloads.prefix(prefix).enumerate() {
                if seen == remaining {
                    complete = false;
                    break;
                }
                let (chunk_key, _) = chunk?;
                batch.remove(&table.payloads, chunk_key);
                report.chunks_removed += 1;
            }
            if complete {
                batch.remove(&table.pending, key);
                report.uploads_removed += 1;
            }
            batch.commit()?;
            if !complete {
                report.more = true;
                break;
            }
        }
        Ok(report)
    }
}
