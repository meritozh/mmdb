use super::super::{Actor, OperationId, RecordState, Scope, TemporalFacts};
use super::storage::*;
use super::*;
use serde::{Deserialize, Serialize};
use std::io::{ErrorKind, Read};

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SessionHead {
    sequence: u64,
    scope: Scope,
}

fn identity_key(owner: &str, session: &str, identity: &str) -> Vec<u8> {
    let mut key = session_key(owner, session);
    segment(&mut key, identity.as_bytes());
    key
}

impl ContextStore<'_> {
    /// Enumerate native history without relying on an application projection.
    /// This recovery catalog requires trusted harness authority.
    pub fn history_sessions(
        &self,
        limit: usize,
        after: Option<&str>,
    ) -> ContextResult<HistorySessionPage> {
        if !matches!(self.access.actor, Actor::System | Actor::Operator) {
            return Err(ContextError::AccessDenied);
        }
        if limit == 0 || limit > 100 {
            return Err(ContextError::InvalidInput(
                "session page limit must be 1..=100".into(),
            ));
        }
        if let Some(after) = after {
            validate_name(after, "session cursor")?;
        }
        let _guard = self.db.write_lock.lock();
        let prefix = owner_key(&self.access.owner);
        let start = after.map_or_else(
            || prefix.clone(),
            |session| session_key(&self.access.owner, session),
        );
        let mut sessions = Vec::new();
        let mut next = None;
        for (scanned, entry) in self.parts.history_heads.range(start.clone()..).enumerate() {
            let (key, value) = entry?;
            if !key.starts_with(&prefix) {
                break;
            }
            if after.is_some() && key.as_ref() == start {
                continue;
            }
            if sessions.len() == limit || scanned >= 4096 {
                return Ok(HistorySessionPage { sessions, next });
            }
            let tail = &key[prefix.len()..];
            if tail.len() < 4 {
                return Err(ContextError::Corrupt("invalid session index".into()));
            }
            let length = u32::from_be_bytes(
                tail[..4]
                    .try_into()
                    .map_err(|_| ContextError::Corrupt("invalid session length".into()))?,
            ) as usize;
            if length != tail.len() - 4 {
                return Err(ContextError::Corrupt("invalid session key length".into()));
            }
            let session = String::from_utf8(tail[4..].to_vec())
                .map_err(|_| ContextError::Corrupt("invalid session identity".into()))?;
            let head: SessionHead = decode(&value)?;
            next = Some(session.clone());
            if self.check_scope(&head.scope).is_ok() {
                sessions.push(session);
            }
        }
        Ok(HistorySessionPage {
            sessions,
            next: None,
        })
    }

    /// Appends an exact payload using bounded buffers. No event becomes visible
    /// until its complete payload, history position and receipt are committed.
    pub fn append_history<R: Read>(
        &self,
        operation: OperationId,
        input: HistoryInput,
        mut payload: R,
    ) -> ContextResult<WriteReceipt> {
        self.validate_history_input(&input)?;
        let _guard = self.db.write_lock.lock();
        let event_key = identity_key(&self.access.owner, &input.session, &input.event_id);
        let previous_operation = self.stored_operation(operation)?;
        let previous_event: Option<StoredEvent> = self
            .parts
            .events
            .get(&event_key)?
            .map(|bytes| decode(&bytes))
            .transpose()?;
        let session_key = session_key(&self.access.owner, &input.session);
        let head: Option<SessionHead> = self
            .parts
            .history_heads
            .get(&session_key)?
            .map(|bytes| decode(&bytes))
            .transpose()?;
        if let Some(head) = &head {
            self.check_scope(&head.scope)?;
            if head.scope != input.scope {
                return Err(ContextError::InvalidInput(
                    "a session's history scope cannot change".into(),
                ));
            }
        }
        let sequence = head.map_or(0, |head| head.sequence);
        let writing = previous_operation.is_none() && previous_event.is_none();
        let call_sources = if writing {
            if let Some(expected) = input.expected_sequence {
                if expected != sequence {
                    return Err(ContextError::HistoryConflict {
                        expected,
                        actual: sequence,
                    });
                }
            }
            let mut sources = self.validate_call_link(&input)?;
            sources.extend(input.sources.clone());
            self.validate_history_sources(&input.scope, &sources)?;
            sources
        } else {
            Vec::new()
        };
        let message_transition = if writing {
            self.message_transition(&input)?
        } else {
            None
        };
        let pin = self.fresh_pin(RecordKind::History);
        if writing {
            let mut batch = self.db.durable_batch();
            batch.insert(
                &self.parts.pending,
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
        let mut hasher = blake3::Hasher::new_keyed(&self.db.digest_key);
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
            if writing {
                let mut value = Vec::with_capacity(32 + used);
                value.extend_from_slice(&self.content_digest(&buffer[..used]));
                value.extend_from_slice(&buffer[..used]);
                let mut batch = self.db.durable_batch();
                batch.insert(&self.parts.payloads, payload_key(&pin, chunk), value);
                batch.commit()?;
            }
            chunk += 1;
        }
        let payload_digest = *hasher.finalize().as_bytes();
        let digest = self.digest("append-history", &(&input, length, payload_digest))?;
        if let Some(receipt) = self.replay(operation, digest)? {
            return Ok(receipt);
        }
        let mut event_input = input.clone();
        event_input.expected_sequence = None;
        let event_digest = self.digest("history-event", &(&event_input, length, payload_digest))?;
        if let Some(previous) = previous_event {
            if previous.digest != event_digest {
                return Err(ContextError::IdempotencyConflict);
            }
            self.header_locked(&previous.receipt.pin)?;
            return self.commit_operation(
                self.db.durable_batch(),
                operation,
                digest,
                previous.receipt,
            );
        }
        if !writing {
            return Err(ContextError::IdempotencyConflict);
        }
        let next_sequence = sequence
            .checked_add(1)
            .ok_or(ContextError::BudgetExceeded)?;
        let history = HistoryEntry {
            session: input.session.clone(),
            event_id: input.event_id,
            sequence: next_sequence,
            kind: input.kind.clone(),
            media_type: input.media_type,
        };
        let mut provenance = self.provenance();
        provenance.session = Some(input.session.clone());
        let header = RecordHeader {
            pin: pin.clone(),
            scope: input.scope.clone(),
            state: RecordState::Active,
            type_pin: None,
            sources: call_sources,
            provenance,
            information: InformationKind::Observation,
            temporal: TemporalFacts::observed_at(input.occurred_at_ms),
            recorded_at_ms: super::super::now_ms(),
            payload: PayloadDescriptor {
                revision: pin.revision,
                bytes: length,
                digest: payload_digest,
            },
            endpoints: None,
            history: Some(history),
            checkpoint: None,
        };
        let receipt = WriteReceipt {
            pin: pin.clone(),
            history_sequence: Some(next_sequence),
        };
        let mut batch = self.db.durable_batch();
        if let Some((key, state)) = message_transition {
            batch.insert(&self.parts.messages, key, encode(&state)?);
        }
        self.put_header(&mut batch, &header)?;
        let mut order_key = session_key.clone();
        order_key.extend_from_slice(&next_sequence.to_be_bytes());
        batch.insert(&self.parts.history_order, order_key, encode(&pin.record)?);
        batch.insert(
            &self.parts.history_heads,
            session_key,
            encode(&SessionHead {
                sequence: next_sequence,
                scope: input.scope,
            })?,
        );
        batch.insert(
            &self.parts.events,
            event_key,
            encode(&StoredEvent {
                digest: event_digest,
                receipt: receipt.clone(),
            })?,
        );
        match input.kind {
            HistoryKind::ToolCall { call_id, .. } => batch.insert(
                &self.parts.calls,
                identity_key(&self.access.owner, &input.session, &call_id),
                encode(&pin.record)?,
            ),
            HistoryKind::ToolResult { call_id, .. } => batch.insert(
                &self.parts.results,
                identity_key(&self.access.owner, &input.session, &call_id),
                encode(&pin.record)?,
            ),
            _ => {}
        }
        batch.remove(&self.parts.pending, record_key(&pin.record));
        self.commit_operation(batch, operation, digest, receipt)
    }

    fn validate_history_input(&self, input: &HistoryInput) -> ContextResult<()> {
        self.check_scope(&input.scope)?;
        validate_name(&input.session, "session")?;
        validate_name(&input.event_id, "event identity")?;
        validate_name(&input.media_type, "media type")?;
        if self
            .access
            .session
            .as_ref()
            .is_some_and(|session| session != &input.session)
        {
            return Err(ContextError::AccessDenied);
        }
        if input.kind == HistoryKind::UserMessage
            && !matches!(
                self.access.actor,
                Actor::User | Actor::System | Actor::Operator
            )
        {
            return Err(ContextError::AccessDenied);
        }
        match &input.kind {
            HistoryKind::AssistantStarted { message_id }
            | HistoryKind::AssistantFragment { message_id, .. }
            | HistoryKind::AssistantTerminal { message_id, .. } => {
                validate_name(message_id, "assistant message identity")?
            }
            HistoryKind::ToolCall {
                call_id,
                tool_name,
                message_id,
            } => {
                validate_name(call_id, "tool call identity")?;
                validate_name(tool_name, "tool name")?;
                if let Some(message_id) = message_id {
                    validate_name(message_id, "assistant message identity")?;
                }
            }
            HistoryKind::ToolResult { call_id, .. } => {
                validate_name(call_id, "tool call identity")?
            }
            _ => {}
        }
        Ok(())
    }

    fn message_transition(
        &self,
        input: &HistoryInput,
    ) -> ContextResult<Option<(Vec<u8>, MessageState)>> {
        let message_id = match &input.kind {
            HistoryKind::AssistantStarted { message_id }
            | HistoryKind::AssistantFragment { message_id, .. }
            | HistoryKind::AssistantTerminal { message_id, .. } => message_id,
            _ => return Ok(None),
        };
        let key = identity_key(&self.access.owner, &input.session, message_id);
        let previous: Option<MessageState> = self
            .parts
            .messages
            .get(&key)?
            .map(|bytes| decode(&bytes))
            .transpose()?;
        let state = match (&input.kind, previous) {
            (HistoryKind::AssistantStarted { .. }, None) => MessageState {
                next_fragment: 0,
                terminal: None,
                unavailable: false,
            },
            (HistoryKind::AssistantFragment { ordinal, .. }, Some(mut state))
                if state.terminal.is_none() && *ordinal == state.next_fragment =>
            {
                state.next_fragment = state
                    .next_fragment
                    .checked_add(1)
                    .ok_or(ContextError::BudgetExceeded)?;
                state
            }
            (HistoryKind::AssistantTerminal { status, .. }, Some(mut state))
                if state.terminal.is_none() =>
            {
                state.terminal = Some(*status);
                state
            }
            _ => {
                return Err(ContextError::InvalidInput(
                    "assistant stream identity, fragment order or terminal state conflict".into(),
                ))
            }
        };
        Ok(Some((key, state)))
    }

    pub fn message_state(
        &self,
        session: &str,
        message_id: &str,
    ) -> ContextResult<Option<MessageState>> {
        validate_name(session, "session")?;
        validate_name(message_id, "assistant message identity")?;
        let _guard = self.db.write_lock.lock();
        let head: Option<SessionHead> = self
            .parts
            .history_heads
            .get(session_key(&self.access.owner, session))?
            .map(|bytes| decode(&bytes))
            .transpose()?;
        if let Some(head) = head {
            self.check_scope(&head.scope)?;
        }
        self.parts
            .messages
            .get(identity_key(&self.access.owner, session, message_id))?
            .map(|bytes| decode(&bytes))
            .transpose()
    }

    pub(super) fn history_anchor_locked(
        &self,
        session: &str,
        sequence: u64,
    ) -> ContextResult<RecordHeader> {
        let mut key = session_key(&self.access.owner, session);
        key.extend_from_slice(&sequence.to_be_bytes());
        let record = self.parts.history_order.get(key)?.ok_or_else(|| {
            ContextError::InvalidInput("checkpoint history position has not been committed".into())
        })?;
        self.head_locked(&decode(&record)?)
    }

    pub(super) fn message_available_locked(&self, history: &HistoryEntry) -> ContextResult<bool> {
        let id = match &history.kind {
            HistoryKind::AssistantStarted { message_id }
            | HistoryKind::AssistantFragment { message_id, .. }
            | HistoryKind::AssistantTerminal { message_id, .. }
            | HistoryKind::ToolCall {
                message_id: Some(message_id),
                ..
            } => message_id,
            _ => return Ok(true),
        };
        let state: MessageState = self
            .parts
            .messages
            .get(identity_key(&self.access.owner, &history.session, id))?
            .map(|bytes| decode(&bytes))
            .transpose()?
            .ok_or_else(|| ContextError::Corrupt("missing message lifecycle".into()))?;
        Ok(!state.unavailable)
    }

    pub(super) fn invalidate_message(
        &self,
        batch: &mut fjall::Batch,
        history: &HistoryEntry,
    ) -> ContextResult<()> {
        let id = match &history.kind {
            HistoryKind::AssistantStarted { message_id }
            | HistoryKind::AssistantFragment { message_id, .. }
            | HistoryKind::AssistantTerminal { message_id, .. }
            | HistoryKind::ToolCall {
                message_id: Some(message_id),
                ..
            } => message_id,
            _ => return Ok(()),
        };
        let key = identity_key(&self.access.owner, &history.session, id);
        let mut state: MessageState = self
            .parts
            .messages
            .get(&key)?
            .map(|bytes| decode(&bytes))
            .transpose()?
            .ok_or_else(|| ContextError::Corrupt("missing message lifecycle".into()))?;
        state.unavailable = true;
        batch.insert(&self.parts.messages, key, encode(&state)?);
        Ok(())
    }

    fn validate_call_link(&self, input: &HistoryInput) -> ContextResult<Vec<RecordPin>> {
        match &input.kind {
            HistoryKind::ToolCall {
                call_id,
                message_id,
                ..
            } => {
                if let Some(message_id) = message_id {
                    let state: MessageState = self
                        .parts
                        .messages
                        .get(identity_key(&self.access.owner, &input.session, message_id))?
                        .map(|bytes| decode(&bytes))
                        .transpose()?
                        .ok_or_else(|| {
                            ContextError::InvalidInput(
                                "tool references an unknown assistant message".into(),
                            )
                        })?;
                    if state.unavailable {
                        return Err(ContextError::InvalidInput(
                            "tool references an unavailable assistant message".into(),
                        ));
                    }
                }
                if self.parts.calls.contains_key(identity_key(
                    &self.access.owner,
                    &input.session,
                    call_id,
                ))? {
                    return Err(ContextError::IdempotencyConflict);
                }
            }
            HistoryKind::ToolResult { call_id, .. } => {
                let key = identity_key(&self.access.owner, &input.session, call_id);
                let call = self.parts.calls.get(&key)?.ok_or_else(|| {
                    ContextError::InvalidInput(
                        "tool result has no preceding call in this session".into(),
                    )
                })?;
                let reference = decode(&call)?;
                let header = self.head_locked(&reference)?;
                // A completed operation may have revised a source used by its
                // call. Pair immutable originals using historical readability;
                // purge and message unavailability still invalidate the link.
                if !self.available_locked(&header.pin, false)? {
                    return Err(ContextError::Unavailable(reference));
                }
                if self.parts.results.contains_key(key)? {
                    return Err(ContextError::IdempotencyConflict);
                }
                return Ok(vec![header.pin]);
            }
            _ => {}
        }
        Ok(Vec::new())
    }

    pub fn history_position(&self, session: &str) -> ContextResult<u64> {
        validate_name(session, "session")?;
        let _guard = self.db.write_lock.lock();
        let head: Option<SessionHead> = self
            .parts
            .history_heads
            .get(session_key(&self.access.owner, session))?
            .map(|bytes| decode(&bytes))
            .transpose()?;
        if let Some(head) = head {
            self.check_scope(&head.scope)?;
            Ok(head.sequence)
        } else {
            Ok(0)
        }
    }

    /// Exact original headers for one call/result pair, including tombstones.
    /// Payload availability is still checked separately when reading bytes.
    pub fn tool_history(
        &self,
        session: &str,
        call_id: &str,
    ) -> ContextResult<(Option<RecordHeader>, Option<RecordHeader>)> {
        validate_name(session, "session")?;
        validate_name(call_id, "tool call identity")?;
        let _guard = self.db.write_lock.lock();
        let key = identity_key(&self.access.owner, session, call_id);
        let call = self
            .parts
            .calls
            .get(&key)?
            .map(|bytes| self.head_locked(&decode(&bytes)?))
            .transpose()?;
        let result = self
            .parts
            .results
            .get(&key)?
            .map(|bytes| self.head_locked(&decode(&bytes)?))
            .transpose()?;
        Ok((call, result))
    }

    pub fn history(
        &self,
        session: &str,
        after_sequence: u64,
        limit: usize,
    ) -> ContextResult<HistoryPage> {
        validate_name(session, "session")?;
        if limit == 0 || limit > 100 {
            return Err(ContextError::InvalidInput(
                "history page limit must be 1..=100".into(),
            ));
        }
        let _guard = self.db.write_lock.lock();
        let prefix = session_key(&self.access.owner, session);
        let mut start = prefix.clone();
        start.extend_from_slice(&after_sequence.to_be_bytes());
        let mut entries = Vec::new();
        for entry in self.parts.history_order.range(start.clone()..) {
            let (key, reference) = entry?;
            if !key.starts_with(&prefix) {
                break;
            }
            if key.as_ref() == start {
                continue;
            }
            if entries.len() == limit {
                let next_sequence = entries.last().and_then(|header: &RecordHeader| {
                    header.history.as_ref().map(|history| history.sequence)
                });
                return Ok(HistoryPage {
                    entries,
                    next_sequence,
                });
            }
            let header = self.head_locked(&decode(&reference)?)?;
            entries.push(header);
        }
        Ok(HistoryPage {
            entries,
            next_sequence: None,
        })
    }

    /// Cleans abandoned uploads and purged payloads, never live committed data.
    /// The chunk and upload counts bound work even for very large originals.
    pub fn recover_payloads(
        &self,
        max_chunks: usize,
        max_uploads: usize,
    ) -> ContextResult<RecoveryReport> {
        if max_chunks == 0 || max_chunks > 4096 || max_uploads == 0 || max_uploads > 100 {
            return Err(ContextError::InvalidInput("invalid cleanup budget".into()));
        }
        let _guard = self.db.write_lock.lock();
        let mut report = RecoveryReport {
            chunks_removed: 0,
            uploads_removed: 0,
            more: false,
        };
        for entry in self.parts.pending.prefix(owner_key(&self.access.owner)) {
            let (key, bytes) = entry?;
            if report.chunks_removed == max_chunks || report.uploads_removed == max_uploads {
                report.more = true;
                break;
            }
            let staged: StagedUpload = decode(&bytes)?;
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
            let mut batch = self.db.durable_batch();
            let mut complete = true;
            for (seen, chunk) in self.parts.payloads.prefix(prefix).enumerate() {
                if seen == remaining {
                    complete = false;
                    break;
                }
                let (chunk_key, _) = chunk?;
                batch.remove(&self.parts.payloads, chunk_key);
                report.chunks_removed += 1;
            }
            if complete {
                batch.remove(&self.parts.pending, key);
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
