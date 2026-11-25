use super::*;
use crate::records::codec::*;
use crate::{Actor, OperationId, RecordState, Scope, TemporalFacts};
use serde::{Deserialize, Serialize};
use std::io::Read;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SessionHead {
    sequence: u64,
    scope: Scope,
}

/// One event ready to write into the session's batch.
struct NewEvent {
    session: String,
    event_id: String,
    kind: EventKind,
    media_type: String,
    scope: Scope,
    occurred_at_ms: i64,
    provenance: Provenance,
    sources: Vec<RecordPin>,
    source_scopes: Vec<Scope>,
    pin: RecordPin,
    payload: PayloadDescriptor,
    message: Option<MessageState>,
}

impl NewEvent {
    fn identity_input(&self) -> EventInput {
        // A result's first source is its implicit call link. Only the caller's
        // explicit sources participate in the persisted event identity.
        let sources = match self.kind {
            EventKind::ToolResult { .. } => &self.sources[1..],
            _ => &self.sources[..],
        };
        EventInput {
            sources: sources.to_vec(),
            session: self.session.clone(),
            event_id: self.event_id.clone(),
            scope: self.scope.clone(),
            kind: self.kind.clone(),
            media_type: self.media_type.clone(),
            occurred_at_ms: self.occurred_at_ms,
            expected_sequence: None,
        }
    }
}

#[derive(Serialize, Deserialize)]
struct ToolCallIndex {
    record: ContextRef,
    message_id: Option<String>,
    purged: bool,
}

fn identity_key(owner: &str, session: &str, identity: &str) -> Vec<u8> {
    let mut key = session_key(owner, session);
    segment(&mut key, identity.as_bytes());
    key
}

fn assistant_message_id(kind: &EventKind) -> Option<&str> {
    match kind {
        EventKind::AssistantStarted { message_id }
        | EventKind::AssistantFragment { message_id, .. }
        | EventKind::AssistantTerminal { message_id, .. } => Some(message_id),
        _ => None,
    }
}

pub(crate) struct LogGate {
    calls: mmdb_kernel::Partition,
    messages: mmdb_kernel::Partition,
}

impl LogGate {
    pub(crate) fn open(session: &mmdb_kernel::EraPartitions) -> mmdb_kernel::KernelResult<Self> {
        Ok(Self {
            calls: session.partition(CALLS)?,
            messages: session.partition(MESSAGES)?,
        })
    }
}

impl crate::records::Gate for LogGate {
    fn admits(
        &self,
        owner: &str,
        header: &RecordHeader,
        active: bool,
        now: &crate::records::Availability,
    ) -> ContextResult<bool> {
        if let Some(checkpoint) = &header.checkpoint {
            return Ok(if active {
                checkpoint.availability_epoch == now.epoch
            } else {
                checkpoint.availability_epoch >= now.redaction_epoch
            });
        }
        if let Some(event) = &header.event {
            return self.message_available_locked(owner, event);
        }
        Ok(true)
    }
}

impl LogGate {
    fn message_available_locked(&self, owner: &str, event: &LogEvent) -> ContextResult<bool> {
        let call;
        let id = match &event.kind {
            EventKind::AssistantStarted { message_id }
            | EventKind::AssistantFragment { message_id, .. }
            | EventKind::AssistantTerminal { message_id, .. } => message_id,
            EventKind::ToolCall { call_id, .. } | EventKind::ToolResult { call_id, .. } => {
                call = load_call(&self.calls, owner, &event.session, call_id)?
                    .ok_or_else(|| ContextError::Corrupt("missing tool call index".into()))?;
                if call.purged {
                    return Ok(false);
                }
                let Some(id) = &call.message_id else {
                    return Ok(true);
                };
                id
            }
            _ => return Ok(true),
        };
        let state: MessageState = self
            .messages
            .get(identity_key(owner, &event.session, id))?
            .map(|bytes| decode(&bytes))
            .transpose()?
            .ok_or_else(|| ContextError::Corrupt("missing message lifecycle".into()))?;
        Ok(!state.unavailable)
    }
}

fn load_call(
    calls: &mmdb_kernel::Partition,
    owner: &str,
    session: &str,
    call_id: &str,
) -> ContextResult<Option<ToolCallIndex>> {
    calls
        .get(identity_key(owner, session, call_id))?
        .map(|bytes| decode(&bytes))
        .transpose()
}

impl SessionLog<'_> {
    /// Enumerate sessions without relying on an application projection.
    /// This recovery catalog requires trusted harness authority.
    pub fn sessions(&self, limit: usize, after: Option<&str>) -> ContextResult<SessionPage> {
        if !matches!(self.scoped.access().actor, Actor::System | Actor::Operator) {
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
        let _guard = self.scoped.lock();
        let prefix = owner_key(&self.scoped.access().owner);
        let start = after.map_or_else(
            || prefix.clone(),
            |session| session_key(&self.scoped.access().owner, session),
        );
        let mut sessions = Vec::new();
        let mut next = None;
        for (scanned, entry) in self.parts.heads.range(start.clone()..).enumerate() {
            let (key, value) = entry?;
            if !key.starts_with(&prefix) {
                break;
            }
            if after.is_some() && key.as_ref() == start {
                continue;
            }
            if sessions.len() == limit || scanned >= 4096 {
                return Ok(SessionPage { sessions, next });
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
            if self.scoped.check_scope(&head.scope).is_ok() {
                sessions.push(session);
            }
        }
        Ok(SessionPage {
            sessions,
            next: None,
        })
    }

    /// Appends an exact payload using bounded buffers. No event becomes visible
    /// until its complete payload, session log position and receipt are committed.
    pub fn append<R: Read>(
        &self,
        operation: OperationId,
        input: EventInput,
        payload: R,
    ) -> ContextResult<WriteReceipt> {
        self.validate_event_input(&input)?;
        let _guard = self.scoped.lock();
        let event_key = identity_key(&self.scoped.access().owner, &input.session, &input.event_id);
        let previous_operation = self.scoped.has_operation(operation)?;
        let previous_event: Option<StoredEvent> = self
            .parts
            .events
            .get(&event_key)?
            .map(|bytes| decode(&bytes))
            .transpose()?;
        let session_key = session_key(&self.scoped.access().owner, &input.session);
        let head: Option<SessionHead> = self
            .parts
            .heads
            .get(&session_key)?
            .map(|bytes| decode(&bytes))
            .transpose()?;
        if let Some(head) = &head {
            self.scoped.check_scope(&head.scope)?;
            if head.scope != input.scope {
                return Err(ContextError::InvalidInput(
                    "a session's log scope cannot change".into(),
                ));
            }
        }
        let mut head = head.unwrap_or_else(|| SessionHead {
            sequence: 0,
            scope: input.scope.clone(),
        });
        let writing = !previous_operation && previous_event.is_none();
        let (sources, source_scopes) = if writing {
            if let Some(expected) = input.expected_sequence {
                if expected != head.sequence {
                    return Err(ContextError::PositionConflict {
                        expected,
                        actual: head.sequence,
                    });
                }
            }
            let mut sources = self.validate_call_link(&input)?;
            sources.extend(input.sources.clone());
            let source_scopes = self.scoped.validate_copy_sources(&input.scope, &sources)?;
            (sources, source_scopes)
        } else {
            (Vec::new(), Vec::new())
        };
        let message_transition = if writing {
            self.message_transition(&input)?
        } else {
            None
        };
        let pin = self.scoped.fresh_pin(RecordKind::Event);
        let streamed = self
            .scoped
            .stream_payload(payload, writing.then_some((&pin, operation)))?;
        let length = streamed.descriptor.bytes;
        let payload_digest = streamed.descriptor.digest;
        let digest = self
            .scoped
            .digest("append-history", &(&input, length, payload_digest))?;
        if let Some(receipt) = self.scoped.replay(operation, digest)? {
            return Ok(receipt);
        }
        if let Some(previous) = previous_event {
            let event_input = EventInput {
                expected_sequence: None,
                ..input
            };
            if previous.digest != self.event_digest(&event_input, &streamed.descriptor)? {
                return Err(ContextError::IdempotencyConflict);
            }
            self.scoped.header_locked(&previous.receipt.pin)?;
            return self.scoped.commit_operation(
                self.scoped.batch(),
                operation,
                digest,
                previous.receipt,
            );
        }
        if !writing {
            return Err(ContextError::IdempotencyConflict);
        }
        let mut provenance = self.scoped.provenance();
        provenance.session = Some(input.session.clone());
        let event = NewEvent {
            session: input.session,
            event_id: input.event_id,
            kind: input.kind,
            media_type: input.media_type,
            scope: input.scope,
            occurred_at_ms: input.occurred_at_ms,
            provenance,
            sources,
            source_scopes,
            pin,
            payload: streamed.descriptor.clone(),
            message: message_transition,
        };
        let mut batch = self.scoped.batch();
        let receipt = self.put_event(&mut batch, &mut head, event)?;
        batch.insert(&self.parts.heads, session_key, encode(&head)?);
        streamed.publish(&mut batch);
        self.scoped
            .commit_operation(batch, operation, digest, receipt)
    }

    fn event_digest(
        &self,
        input: &EventInput,
        payload: &PayloadDescriptor,
    ) -> ContextResult<[u8; 32]> {
        self.scoped
            .digest("history-event", &(input, payload.bytes, payload.digest))
    }

    fn put_event(
        &self,
        batch: &mut mmdb_kernel::Batch,
        head: &mut SessionHead,
        event: NewEvent,
    ) -> ContextResult<WriteReceipt> {
        let sequence = head
            .sequence
            .checked_add(1)
            .ok_or(ContextError::BudgetExceeded)?;
        let digest = self.event_digest(&event.identity_input(), &event.payload)?;
        let owner = &self.scoped.access().owner;
        let receipt = WriteReceipt {
            pin: event.pin.clone(),
            sequence: Some(sequence),
        };
        let mut order_key = session_key(owner, &event.session);
        order_key.extend_from_slice(&sequence.to_be_bytes());
        batch.insert(&self.parts.order, order_key, encode(&event.pin.record)?);
        batch.insert(
            &self.parts.events,
            identity_key(owner, &event.session, &event.event_id),
            encode(&StoredEvent {
                digest,
                receipt: receipt.clone(),
            })?,
        );
        if let Some((id, state)) = assistant_message_id(&event.kind).zip(event.message) {
            let key = identity_key(owner, &event.session, id);
            batch.insert(&self.parts.messages, key, encode(&state)?);
        }
        match &event.kind {
            EventKind::ToolCall {
                call_id,
                message_id,
                ..
            } => batch.insert(
                &self.parts.calls,
                identity_key(owner, &event.session, call_id),
                encode(&ToolCallIndex {
                    record: event.pin.record.clone(),
                    message_id: message_id.clone(),
                    purged: false,
                })?,
            ),
            EventKind::ToolResult { call_id, .. } => batch.insert(
                &self.parts.results,
                identity_key(owner, &event.session, call_id),
                encode(&event.pin.record)?,
            ),
            _ => {}
        }
        let header = RecordHeader {
            pin: event.pin,
            scope: event.scope,
            state: RecordState::Active,
            type_pin: None,
            sources: event.sources,
            source_scopes: event.source_scopes,
            provenance: event.provenance,
            information: InformationKind::Observation,
            temporal: TemporalFacts::observed_at(event.occurred_at_ms),
            recorded_at_ms: super::super::now_ms(),
            payload: event.payload,
            endpoints: None,
            event: Some(LogEvent {
                session: event.session,
                event_id: event.event_id,
                sequence,
                kind: event.kind,
                media_type: event.media_type,
            }),
            checkpoint: None,
        };
        self.scoped.put_header(batch, &header)?;
        head.sequence = sequence;
        Ok(receipt)
    }

    fn validate_event_input(&self, input: &EventInput) -> ContextResult<()> {
        self.scoped.check_scope(&input.scope)?;
        validate_name(&input.session, "session")?;
        validate_name(&input.event_id, "event identity")?;
        validate_name(&input.media_type, "media type")?;
        if self
            .scoped
            .access()
            .session
            .as_ref()
            .is_some_and(|session| session != &input.session)
        {
            return Err(ContextError::AccessDenied);
        }
        if input.kind == EventKind::UserMessage
            && !matches!(
                self.scoped.access().actor,
                Actor::User | Actor::System | Actor::Operator
            )
        {
            return Err(ContextError::AccessDenied);
        }
        match &input.kind {
            EventKind::AssistantStarted { message_id }
            | EventKind::AssistantFragment { message_id, .. }
            | EventKind::AssistantTerminal { message_id, .. } => {
                validate_name(message_id, "assistant message identity")?
            }
            EventKind::ToolCall {
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
            EventKind::ToolResult { call_id, .. } => validate_name(call_id, "tool call identity")?,
            _ => {}
        }
        Ok(())
    }

    fn message_transition(&self, input: &EventInput) -> ContextResult<Option<MessageState>> {
        let Some(message_id) = assistant_message_id(&input.kind) else {
            return Ok(None);
        };
        let key = identity_key(&self.scoped.access().owner, &input.session, message_id);
        let previous: Option<MessageState> = self
            .parts
            .messages
            .get(&key)?
            .map(|bytes| decode(&bytes))
            .transpose()?;
        let state = match (&input.kind, previous) {
            (EventKind::AssistantStarted { .. }, None) => MessageState {
                next_fragment: 0,
                terminal: None,
                unavailable: false,
            },
            (EventKind::AssistantFragment { ordinal, .. }, Some(mut state))
                if state.terminal.is_none() && *ordinal == state.next_fragment =>
            {
                state.next_fragment = state
                    .next_fragment
                    .checked_add(1)
                    .ok_or(ContextError::BudgetExceeded)?;
                state
            }
            (EventKind::AssistantTerminal { status, .. }, Some(mut state))
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
        Ok(Some(state))
    }

    pub fn message_state(
        &self,
        session: &str,
        message_id: &str,
    ) -> ContextResult<Option<MessageState>> {
        validate_name(session, "session")?;
        validate_name(message_id, "assistant message identity")?;
        let _guard = self.scoped.lock();
        let head: Option<SessionHead> = self
            .parts
            .heads
            .get(session_key(&self.scoped.access().owner, session))?
            .map(|bytes| decode(&bytes))
            .transpose()?;
        if let Some(head) = head {
            self.scoped.check_scope(&head.scope)?;
        }
        self.parts
            .messages
            .get(identity_key(
                &self.scoped.access().owner,
                session,
                message_id,
            ))?
            .map(|bytes| decode(&bytes))
            .transpose()
    }

    pub(crate) fn event_anchor_locked(
        &self,
        session: &str,
        sequence: u64,
    ) -> ContextResult<RecordHeader> {
        let mut key = session_key(&self.scoped.access().owner, session);
        key.extend_from_slice(&sequence.to_be_bytes());
        let record = self.parts.order.get(key)?.ok_or_else(|| {
            ContextError::InvalidInput(
                "checkpoint session log position has not been committed".into(),
            )
        })?;
        self.scoped.head_locked(&decode(&record)?)
    }

    fn call_index(&self, session: &str, call_id: &str) -> ContextResult<Option<ToolCallIndex>> {
        load_call(
            &self.parts.calls,
            &self.scoped.access().owner,
            session,
            call_id,
        )
    }

    fn indexed_call(&self, session: &str, call_id: &str) -> ContextResult<ToolCallIndex> {
        self.call_index(session, call_id)?
            .ok_or_else(|| ContextError::Corrupt("missing tool call index".into()))
    }

    pub(crate) fn withdraw_event(
        &self,
        batch: &mut mmdb_kernel::Batch,
        event: &LogEvent,
    ) -> ContextResult<()> {
        let id = match &event.kind {
            EventKind::AssistantStarted { message_id }
            | EventKind::AssistantFragment { message_id, .. }
            | EventKind::AssistantTerminal { message_id, .. } => message_id,
            EventKind::ToolCall {
                call_id,
                message_id,
                ..
            } => {
                let mut call = self.indexed_call(&event.session, call_id)?;
                call.purged = true;
                let key = identity_key(&self.scoped.access().owner, &event.session, call_id);
                batch.insert(&self.parts.calls, key, encode(&call)?);
                let Some(id) = message_id else { return Ok(()) };
                id
            }
            _ => return Ok(()),
        };
        let key = identity_key(&self.scoped.access().owner, &event.session, id);
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

    fn validate_call_link(&self, input: &EventInput) -> ContextResult<Vec<RecordPin>> {
        match &input.kind {
            EventKind::ToolCall {
                call_id,
                message_id,
                ..
            } => {
                if let Some(message_id) = message_id {
                    let state: MessageState = self
                        .parts
                        .messages
                        .get(identity_key(
                            &self.scoped.access().owner,
                            &input.session,
                            message_id,
                        ))?
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
                    &self.scoped.access().owner,
                    &input.session,
                    call_id,
                ))? {
                    return Err(ContextError::IdempotencyConflict);
                }
            }
            EventKind::ToolResult { call_id, .. } => {
                let key = identity_key(&self.scoped.access().owner, &input.session, call_id);
                let call = self.call_index(&input.session, call_id)?.ok_or_else(|| {
                    ContextError::InvalidInput(
                        "tool result has no preceding call in this session".into(),
                    )
                })?;
                let header = self.scoped.head_locked(&call.record)?;
                // A completed operation may have revised a source used by its
                // call. Pair immutable originals using historical readability;
                // purge and message unavailability still invalidate the link.
                if !self.scoped.available_locked(&header.pin, false)? {
                    return Err(ContextError::Unavailable(call.record));
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

    pub fn position(&self, session: &str) -> ContextResult<u64> {
        validate_name(session, "session")?;
        let _guard = self.scoped.lock();
        let head: Option<SessionHead> = self
            .parts
            .heads
            .get(session_key(&self.scoped.access().owner, session))?
            .map(|bytes| decode(&bytes))
            .transpose()?;
        if let Some(head) = head {
            self.scoped.check_scope(&head.scope)?;
            Ok(head.sequence)
        } else {
            Ok(0)
        }
    }

    /// Exact original headers for one call/result pair, including tombstones.
    /// Payload availability is still checked separately when reading bytes.
    pub fn tool_pair(
        &self,
        session: &str,
        call_id: &str,
    ) -> ContextResult<(Option<RecordHeader>, Option<RecordHeader>)> {
        validate_name(session, "session")?;
        validate_name(call_id, "tool call identity")?;
        let _guard = self.scoped.lock();
        let key = identity_key(&self.scoped.access().owner, session, call_id);
        let call = self
            .call_index(session, call_id)?
            .map(|call| self.scoped.head_locked(&call.record))
            .transpose()?;
        let result = self
            .parts
            .results
            .get(&key)?
            .map(|bytes| self.scoped.head_locked(&decode(&bytes)?))
            .transpose()?;
        Ok((call, result))
    }

    pub fn events(
        &self,
        session: &str,
        after_sequence: u64,
        limit: usize,
    ) -> ContextResult<EventPage> {
        validate_name(session, "session")?;
        if limit == 0 || limit > 100 {
            return Err(ContextError::InvalidInput(
                "event page limit must be 1..=100".into(),
            ));
        }
        let _guard = self.scoped.lock();
        let prefix = session_key(&self.scoped.access().owner, session);
        let mut start = prefix.clone();
        start.extend_from_slice(&after_sequence.to_be_bytes());
        let mut entries = Vec::new();
        for entry in self.parts.order.range(start.clone()..) {
            let (key, reference) = entry?;
            if !key.starts_with(&prefix) {
                break;
            }
            if key.as_ref() == start {
                continue;
            }
            if entries.len() == limit {
                let next_sequence = entries.last().and_then(|header: &RecordHeader| {
                    header.event.as_ref().map(|event| event.sequence)
                });
                return Ok(EventPage {
                    entries,
                    next_sequence,
                });
            }
            let header = self.scoped.head_locked(&decode(&reference)?)?;
            entries.push(header);
        }
        Ok(EventPage {
            entries,
            next_sequence: None,
        })
    }
}
