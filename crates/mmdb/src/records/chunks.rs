use super::*;
use mmdb_kernel::{Batch, Partition};
use std::io::{ErrorKind, Read};

pub(crate) struct ChunkSpace<'a> {
    pub partition: &'a Partition,
    pub prefix: Vec<u8>,
}

impl ChunkSpace<'_> {
    fn key(&self, index: u64) -> Vec<u8> {
        let mut key = self.prefix.clone();
        key.extend_from_slice(&index.to_be_bytes());
        key
    }
}

pub(crate) struct ChunkedPayload {
    pub bytes: u64,
    pub digest: [u8; 32],
}

pub(crate) struct RemovedChunks {
    pub removed: usize,
    pub complete: bool,
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

    fn put_chunk(&self, batch: &mut Batch, space: &ChunkSpace<'_>, chunk: u64, bytes: &[u8]) {
        let mut value = Vec::with_capacity(32 + bytes.len());
        value.extend_from_slice(&self.content_digest(bytes));
        value.extend_from_slice(bytes);
        batch.insert(space.partition, space.key(chunk), value);
    }

    pub(crate) fn put_chunks(
        &self,
        batch: &mut Batch,
        space: &ChunkSpace<'_>,
        bytes: &[u8],
    ) -> ChunkedPayload {
        for (index, chunk) in bytes.chunks(PAYLOAD_CHUNK_BYTES).enumerate() {
            self.put_chunk(batch, space, index as u64, chunk);
        }
        ChunkedPayload {
            bytes: bytes.len() as u64,
            digest: self.content_digest(bytes),
        }
    }

    pub(crate) fn write_chunks<R: Read>(
        &self,
        space: Option<&ChunkSpace<'_>>,
        mut payload: R,
    ) -> ContextResult<ChunkedPayload> {
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
            if let Some(space) = space {
                let mut batch = self.batch();
                self.put_chunk(&mut batch, space, chunk, &buffer[..used]);
                batch.commit()?;
            }
            chunk += 1;
        }
        Ok(ChunkedPayload {
            bytes: length,
            digest: *hasher.finalize().as_bytes(),
        })
    }

    pub(crate) fn read_chunks(
        &self,
        space: &ChunkSpace<'_>,
        total: u64,
        offset: u64,
        limit: usize,
    ) -> ContextResult<PayloadPage> {
        if limit == 0 || limit > MAX_PAYLOAD_PAGE_BYTES || offset > total {
            return Err(ContextError::InvalidInput(
                "invalid bounded payload range".into(),
            ));
        }
        let end = total.min(offset.saturating_add(limit as u64));
        let mut bytes = Vec::with_capacity((end - offset) as usize);
        let mut at = offset;
        while at < end {
            let chunk_index = at / PAYLOAD_CHUNK_BYTES as u64;
            let value = space
                .partition
                .get(space.key(chunk_index))?
                .ok_or_else(|| ContextError::Corrupt("missing committed payload chunk".into()))?;
            let expected =
                (total - chunk_index * PAYLOAD_CHUNK_BYTES as u64).min(PAYLOAD_CHUNK_BYTES as u64);
            if value.len() != expected as usize + 32 {
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
            next_offset: (end < total).then_some(end),
            total_bytes: total,
        })
    }

    pub(crate) fn remove_chunks(
        &self,
        batch: &mut Batch,
        space: &ChunkSpace<'_>,
        max: usize,
    ) -> ContextResult<RemovedChunks> {
        let mut removed = 0;
        for key in space.partition.prefix_keys(&space.prefix) {
            let key = key?;
            if removed == max {
                return Ok(RemovedChunks {
                    removed,
                    complete: false,
                });
            }
            batch.remove(space.partition, key);
            removed += 1;
        }
        Ok(RemovedChunks {
            removed,
            complete: true,
        })
    }
}
