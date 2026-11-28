use super::turns::{self, Change, Numbers, OpenTurn, Owed, TurnHolder, TurnState};
use super::view::{identity_key, OrderEntry, ResultIndex, ToolCallIndex, View};
use super::*;
use crate::records::codec::*;
use crate::{Actor, OperationId, RecordState, Scope, TemporalFacts};
use std::io::Read;

use super::catalog::{Changes, SessionHead};

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
    turn: Option<TurnStamp>,
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

impl Owed {
    const TERMINAL: u8 = 0;
    const RESULT: u8 = 1;

    fn key(&self, session_key: &[u8]) -> Vec<u8> {
        match self {
            Self::Terminal { message_id, .. } => {
                Self::row_key(session_key, Self::TERMINAL, message_id)
            }
            Self::Result { call_id, .. } => Self::row_key(session_key, Self::RESULT, call_id),
        }
    }

    // The identity is the last key component, so byte order is identity order.
    fn row_key(session_key: &[u8], kind: u8, id: &str) -> Vec<u8> {
        let mut key = session_key.to_vec();
        key.push(kind);
        key.extend_from_slice(id.as_bytes());
        key
    }

    fn from_row(suffix: &[u8], value: &[u8]) -> ContextResult<Self> {
        let invalid = || ContextError::Corrupt("invalid owed item".into());
        let (&kind, id) = suffix.split_first().ok_or_else(invalid)?;
        let id = std::str::from_utf8(id).map_err(|_| invalid())?.to_owned();
        match kind {
            Self::TERMINAL => Ok(Self::Terminal {
                message_id: id,
                step: decode(value)?,
            }),
            Self::RESULT => Ok(Self::Result {
                call_id: id,
                step: decode(value)?,
            }),
            _ => Err(invalid()),
        }
    }
}

pub(crate) struct LogGate {
    parts: SessionParts,
}

impl LogGate {
    pub(crate) fn open(session: &mmdb_kernel::EraPartitions) -> mmdb_kernel::KernelResult<Self> {
        Ok(Self {
            parts: SessionParts::open(session)?,
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
        let view = View::load(&self.parts, owner, &event.session)?;
        let call;
        let id = match &event.kind {
            EventKind::AssistantStarted { message_id }
            | EventKind::AssistantFragment { message_id, .. }
            | EventKind::AssistantTerminal { message_id, .. } => message_id,
            EventKind::ToolCall { call_id, .. } | EventKind::ToolResult { call_id, .. } => {
                call = view
                    .call(call_id)?
                    .map(|found| found.row)
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
        let state: MessageState = view
            .message(id)?
            .map(|found| found.row)
            .ok_or_else(|| ContextError::Corrupt("missing message lifecycle".into()))?;
        Ok(!state.unavailable)
    }
}

/// End every turn left open by the previous lease holder in one batch.
/// Store::open calls this while it still has exclusive access to the store.
pub(crate) fn end_interrupted_turns(
    records: &Records,
    parts: &SessionParts,
) -> ContextResult<usize> {
    let mut batch = None;
    let mut ended = 0;
    let mut changes = Changes::default();
    let occurred_at_ms = crate::now_ms();
    for entry in parts.open_turns.iter() {
        let (key, value) = entry?;
        let turn: OpenTurn = decode(&value)?;
        if key.as_ref() != session_key(&turn.owner, &turn.session) {
            return Err(ContextError::Corrupt(
                "open turn identity disagrees with key".into(),
            ));
        }
        let value = parts
            .heads
            .get(&key)?
            .ok_or_else(|| ContextError::Corrupt("open turn has no session head".into()))?;
        let mut head: SessionHead = decode(&value)?;
        let access = ContextAccess {
            session: Some(turn.session.clone()),
            run: Some(turn.run.clone()),
            scopes: vec![head.scope.clone()],
            ..ContextAccess::new(&turn.owner, Actor::System)
        };
        let log = SessionLog {
            scoped: records.scoped(access),
            parts,
        };
        let batch = batch.get_or_insert_with(|| log.scoped.batch());
        log.put_turn_end(
            batch,
            &mut head,
            &key,
            &turn,
            &log.owed_items(&key)?,
            TurnEndReason::Interrupted,
            occurred_at_ms,
            &log.view(&turn.session)?,
            None,
        )?;
        changes.put_head(&log, batch, &turn.session, &mut head, None)?;
        ended += 1;
    }
    if let Some(batch) = batch {
        batch.commit()?;
    }
    Ok(ended)
}

impl SessionLog<'_> {
    pub(super) fn view<'a>(&'a self, session: &'a str) -> ContextResult<View<'a>> {
        View::load(self.parts, &self.scoped.access().owner, session)
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
        let previous_operation = self.scoped.has_operation(operation)?;
        let previous_event: Option<super::view::Found<StoredEvent>> =
            self.view(&input.session)?.event(&input.event_id)?;
        let session_key = session_key(&self.scoped.access().owner, &input.session);
        let mut head = self.session_head(&input.session)?;
        self.scoped.check_scope(&head.scope)?;
        if head.scope != input.scope {
            return Err(ContextError::InvalidInput(
                "a session's log scope cannot change".into(),
            ));
        }
        if previous_operation || previous_event.is_some() {
            return self.retry_append(operation, input, payload, previous_event);
        }
        if let Some(expected) = input.expected_sequence {
            if expected != head.sequence {
                return Err(ContextError::PositionConflict {
                    expected,
                    actual: head.sequence,
                });
            }
        }
        let (message_step, answers) = self.validate_call_link(&input)?;
        let state = self.turn_state(&session_key, head.numbers)?;
        let admitted = turns::admit(
            &state,
            turns::Write {
                kind: &input.kind,
                run: self.scoped.access().run.as_deref(),
                message_step,
                owed: self.owed_to_settle(&session_key, &input.kind)?,
            },
        )
        .map_err(ContextError::Turn)?;
        let mut batch = self.scoped.batch();
        let next = head
            .sequence
            .checked_add(1)
            .ok_or(ContextError::BudgetExceeded)?;
        let open_start = match &admitted.change {
            Change::StartTurn { .. } => Some(next),
            Change::EndTurn(_) => None,
            _ => state.open.as_ref().map(|turn| turn.start),
        };
        self.put_turn_change(
            &mut batch,
            &mut head,
            &session_key,
            &input,
            state,
            admitted.change,
        )?;
        let reserve = usize::from(matches!(input.kind, EventKind::ToolCall { .. }));
        let source_scopes = self.scoped.validate_copy_sources(
            &input.scope,
            &input.sources,
            answers.as_ref(),
            reserve,
        )?;
        let message = self.message_transition(&input.session, &input.kind, admitted.stamp, next)?;
        let pin = self.scoped.fresh_pin(RecordKind::Event);
        let streamed = self
            .scoped
            .stream_payload(payload, Some((&pin, operation)))?;
        let digest = self.append_digest(&input, &streamed.descriptor)?;
        let sources = answers
            .into_iter()
            .map(|call| call.pin)
            .chain(input.sources)
            .collect();
        let mut provenance = self.scoped.provenance();
        provenance.session = Some(input.session.clone());
        let event = NewEvent {
            session: input.session.clone(),
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
            message,
            turn: admitted.stamp,
        };
        let receipt = self.put_event(&mut batch, &mut head, event, open_start)?;
        Changes::default().put_head(self, &mut batch, &input.session, &mut head, None)?;
        streamed.publish(&mut batch);
        self.scoped
            .commit_operation(batch, operation, digest, receipt)
    }

    fn retry_append<R: Read>(
        &self,
        operation: OperationId,
        input: EventInput,
        payload: R,
        previous_event: Option<super::view::Found<StoredEvent>>,
    ) -> ContextResult<WriteReceipt> {
        let streamed = self.scoped.stream_payload(payload, None)?;
        let digest = self.append_digest(&input, &streamed.descriptor)?;
        if let Some(receipt) = self.scoped.replay(operation, digest)? {
            return Ok(receipt);
        }
        let previous = previous_event.ok_or(ContextError::IdempotencyConflict)?;
        let mut provenance = self.scoped.provenance();
        if provenance.session.as_ref() == Some(&input.session) {
            provenance.session = Some(previous.session.clone());
        }
        let event_input = EventInput {
            session: previous.session,
            expected_sequence: None,
            ..input
        };
        let previous = previous.row;
        if previous.digest
            != self.scoped.digest_with_provenance(
                "history-event",
                provenance,
                &(
                    &event_input,
                    streamed.descriptor.bytes,
                    streamed.descriptor.digest,
                ),
            )?
        {
            return Err(ContextError::IdempotencyConflict);
        }
        self.scoped.header_locked(&previous.receipt.pin)?;
        self.scoped
            .commit_operation(self.scoped.batch(), operation, digest, previous.receipt)
    }

    fn append_digest(
        &self,
        input: &EventInput,
        payload: &PayloadDescriptor,
    ) -> ContextResult<[u8; 32]> {
        self.scoped
            .digest("append-history", &(input, payload.bytes, payload.digest))
    }

    fn turn_state(&self, key: &[u8], numbers: Numbers) -> ContextResult<TurnState> {
        let open = self
            .parts
            .open_turns
            .get(key)?
            .map(|bytes| decode(&bytes))
            .transpose()?;
        Ok(TurnState { open, numbers })
    }

    fn owed_to_settle(&self, session_key: &[u8], kind: &EventKind) -> ContextResult<Option<Owed>> {
        let key = match kind {
            EventKind::AssistantTerminal { message_id, .. } => {
                Owed::row_key(session_key, Owed::TERMINAL, message_id)
            }
            EventKind::ToolResult { call_id, .. } => {
                Owed::row_key(session_key, Owed::RESULT, call_id)
            }
            _ => return Ok(None),
        };
        self.parts
            .owed
            .get(&key)?
            .map(|bytes| Owed::from_row(&key[session_key.len()..], &bytes))
            .transpose()
    }

    fn owed_items(&self, key: &[u8]) -> ContextResult<Vec<Owed>> {
        self.parts
            .owed
            .prefix(key)
            .map(|row| {
                let (entry, value) = row?;
                Owed::from_row(&entry[key.len()..], &value)
            })
            .collect()
    }

    fn put_turn_change(
        &self,
        batch: &mut mmdb_kernel::Batch,
        head: &mut SessionHead,
        key: &[u8],
        input: &EventInput,
        state: TurnState,
        change: Change,
    ) -> ContextResult<()> {
        match &change {
            Change::Nothing => return Ok(()),
            Change::EndTurn(reason) => {
                let turn = state
                    .open
                    .as_ref()
                    .ok_or_else(|| ContextError::Corrupt("turn end without open turn".into()))?;
                return self.put_turn_end(
                    batch,
                    head,
                    key,
                    turn,
                    &self.owed_items(key)?,
                    *reason,
                    input.occurred_at_ms,
                    &self.view(&turn.session)?,
                    None,
                );
            }
            Change::Owe(item) => {
                batch.insert(&self.parts.owed, item.key(key), encode(&item.step())?)
            }
            Change::Settle(item) => batch.remove(&self.parts.owed, item.key(key)),
            _ => {}
        }
        let holder = TurnHolder {
            owner: &self.scoped.access().owner,
            session: &input.session,
        };
        let position = head
            .sequence
            .checked_add(1)
            .ok_or(ContextError::BudgetExceeded)?;
        let state = turns::apply(state, &change, holder, &input.event_id, position)?;
        head.numbers = state.numbers;
        let turn = state
            .open
            .ok_or_else(|| ContextError::Corrupt("turn change without open turn".into()))?;
        batch.insert(&self.parts.open_turns, key, encode(&turn)?);
        Ok(())
    }

    pub(super) fn put_turn_end(
        &self,
        batch: &mut mmdb_kernel::Batch,
        head: &mut SessionHead,
        key: &[u8],
        turn: &OpenTurn,
        owed: &[Owed],
        reason: TurnEndReason,
        occurred_at_ms: i64,
        view: &View<'_>,
        mut replayed_messages: Option<&mut std::collections::BTreeMap<String, MessageState>>,
    ) -> ContextResult<()> {
        let closing = turns::close(turn, owed, reason)?;
        batch.remove(&self.parts.open_turns, key);
        for item in owed {
            batch.remove(&self.parts.owed, item.key(key));
        }
        if closing.is_empty() {
            return Ok(());
        }
        let payload = self
            .scoped
            .stream_payload(std::io::empty(), None)?
            .descriptor;
        for event in closing {
            let (sources, source_scopes) = match &event.kind {
                EventKind::ToolResult { call_id, .. } => {
                    // The call reserved this result's budget and recorded its source scopes.
                    let call = view
                        .call(call_id)?
                        .ok_or_else(|| ContextError::Corrupt("missing owed call".into()))?
                        .row;
                    let header = self.scoped.head_locked(&call.record)?;
                    (vec![header.pin], header.source_scopes)
                }
                _ => (Vec::new(), Vec::new()),
            };
            let stamp = Some(event.stamp);
            let message = if let Some(id) = turns::assistant_message_id(&event.kind) {
                let previous = match replayed_messages.as_deref_mut() {
                    Some(messages) => messages.remove(id),
                    None => view.message(id)?.map(|found| found.row),
                };
                Some(turns::message_transition(
                    &event.kind,
                    previous,
                    stamp,
                    head.sequence
                        .checked_add(1)
                        .ok_or(ContextError::BudgetExceeded)?,
                )?)
            } else {
                None
            };
            let new_event = NewEvent {
                session: turn.session.clone(),
                event_id: event.event_id,
                kind: event.kind,
                media_type: "application/octet-stream".into(),
                scope: head.scope.clone(),
                occurred_at_ms,
                provenance: Provenance {
                    actor: Actor::System,
                    agent: None,
                    session: Some(turn.session.clone()),
                    run: Some(turn.run.clone()),
                    parent_run: None,
                },
                sources,
                source_scopes,
                pin: self.scoped.fresh_pin(RecordKind::Event),
                payload: payload.clone(),
                message,
                turn: stamp,
            };
            let open_start =
                (!matches!(new_event.kind, EventKind::TurnEnded { .. })).then_some(turn.start);
            self.put_event(batch, head, new_event, open_start)?;
        }
        Ok(())
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
        open_turn: Option<u64>,
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
        batch.insert(
            &self.parts.order,
            order_key,
            encode(&OrderEntry {
                record: event.pin.record.clone(),
                numbers: head.numbers,
                open_turn,
            })?,
        );
        batch.insert(
            &self.parts.events,
            identity_key(owner, &event.session, &event.event_id),
            encode(&StoredEvent {
                digest,
                receipt: receipt.clone(),
            })?,
        );
        if let Some((id, state)) = turns::assistant_message_id(&event.kind).zip(event.message) {
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
                    sequence,
                    record: event.pin.record.clone(),
                    message_id: message_id.clone(),
                    purged: false,
                })?,
            ),
            EventKind::ToolResult { call_id, .. } => batch.insert(
                &self.parts.results,
                identity_key(owner, &event.session, call_id),
                encode(&ResultIndex {
                    record: event.pin.record.clone(),
                    sequence,
                })?,
            ),
            _ => {}
        }
        let header = RecordHeader {
            session: None,
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
                session: event.session.clone(),
                event_id: event.event_id,
                sequence,
                kind: event.kind,
                media_type: event.media_type,
                turn: event.turn,
            }),
            checkpoint: None,
        };
        self.scoped.put_header(batch, &header)?;
        super::coverage::Coverage::new(self.parts, owner).record_scopes(
            batch,
            &event.session,
            sequence,
            &header.source_scopes,
        )?;
        head.sequence = sequence;
        Ok(receipt)
    }

    fn validate_event_input(&self, input: &EventInput) -> ContextResult<()> {
        self.scoped.check_scope(&input.scope)?;
        validate_name(&input.session, "session")?;
        validate_name(&input.event_id, "event identity")?;
        if input.event_id.starts_with("mmdb:") {
            return Err(ContextError::InvalidInput(
                "mmdb: event identities are reserved".into(),
            ));
        }
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
        if matches!(
            input.kind,
            EventKind::TurnStarted
                | EventKind::TurnEnded { .. }
                | EventKind::StepStarted
                | EventKind::StepEnded { .. }
        ) && !matches!(self.scoped.access().actor, Actor::System | Actor::Operator)
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
            EventKind::TurnEnded { start_event_id, .. } => {
                validate_name(start_event_id, "turn start event identity")?
            }
            EventKind::StepEnded { start_event_id } => {
                validate_name(start_event_id, "step start event identity")?
            }
            _ => {}
        }
        Ok(())
    }

    fn message_transition(
        &self,
        session: &str,
        kind: &EventKind,
        stamp: Option<TurnStamp>,
        position: u64,
    ) -> ContextResult<Option<MessageState>> {
        let Some(message_id) = turns::assistant_message_id(kind) else {
            return Ok(None);
        };
        let previous = self
            .view(session)?
            .message(message_id)?
            .map(|found| found.row);
        turns::message_transition(kind, previous, stamp, position).map(Some)
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
        Ok(self
            .view(session)?
            .message(message_id)?
            .map(|found| found.row))
    }

    pub(crate) fn event_anchor_locked(
        &self,
        session: &str,
        sequence: u64,
    ) -> ContextResult<RecordHeader> {
        let entry = self.view(session)?.entry(sequence)?.ok_or_else(|| {
            ContextError::InvalidInput(
                "checkpoint session log position has not been committed".into(),
            )
        })?;
        self.scoped.head_locked(&entry.record)
    }

    fn call_index(&self, session: &str, call_id: &str) -> ContextResult<Option<ToolCallIndex>> {
        Ok(self.view(session)?.call(call_id)?.map(|found| found.row))
    }

    fn indexed_call(&self, session: &str, call_id: &str) -> ContextResult<ToolCallIndex> {
        self.call_index(session, call_id)?
            .ok_or_else(|| ContextError::Corrupt("missing tool call index".into()))
    }

    pub(crate) fn withdraw_event(
        &self,
        batch: &mut mmdb_kernel::Batch,
        event: &LogEvent,
        epoch: u64,
    ) -> ContextResult<()> {
        let coverage = super::coverage::Coverage::new(self.parts, &self.scoped.access().owner);
        let id = match &event.kind {
            EventKind::AssistantStarted { message_id }
            | EventKind::AssistantFragment { message_id, .. }
            | EventKind::AssistantTerminal { message_id, .. } => Some(message_id),
            EventKind::ToolCall {
                call_id,
                message_id,
                ..
            } => {
                let mut call = self.indexed_call(&event.session, call_id)?;
                call.purged = true;
                let key = identity_key(&self.scoped.access().owner, &event.session, call_id);
                batch.insert(&self.parts.calls, key, encode(&call)?);
                message_id.as_ref()
            }
            _ => None,
        };
        let Some(id) = id else {
            coverage.record_withdrawal(batch, &event.session, epoch, event.sequence);
            return Ok(());
        };
        let view = self.view(&event.session)?;
        let overlay = view
            .message(id)?
            .ok_or_else(|| ContextError::Corrupt("missing message lifecycle".into()))?;
        let holder = view.holder(overlay.row.started);
        coverage.record_withdrawal(batch, holder, epoch, overlay.row.started);
        let key = identity_key(&self.scoped.access().owner, holder, id);
        let mut state: MessageState = self
            .view(holder)?
            .message(id)?
            .map(|found| found.row)
            .ok_or_else(|| ContextError::Corrupt("missing message lifecycle".into()))?;
        state.unavailable = true;
        batch.insert(&self.parts.messages, key, encode(&state)?);
        Ok(())
    }

    fn validate_call_link(
        &self,
        input: &EventInput,
    ) -> ContextResult<(Option<u64>, Option<RecordHeader>)> {
        match &input.kind {
            EventKind::ToolCall {
                call_id,
                message_id,
                ..
            } => {
                let step = if let Some(message_id) = message_id {
                    let state: MessageState = self
                        .view(&input.session)?
                        .message(message_id)?
                        .map(|found| found.row)
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
                    Some(state.step)
                } else {
                    None
                };
                if self.call_index(&input.session, call_id)?.is_some() {
                    return Err(ContextError::IdempotencyConflict);
                }
                Ok((step, None))
            }
            EventKind::ToolResult { call_id, .. } => {
                let call = self.call_index(&input.session, call_id)?.ok_or_else(|| {
                    ContextError::InvalidInput(
                        "tool result has no preceding call in this session".into(),
                    )
                })?;
                Ok((None, Some(self.scoped.head_locked(&call.record)?)))
            }
            _ => Ok((None, None)),
        }
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
        let call = self
            .call_index(session, call_id)?
            .map(|call| self.scoped.head_locked(&call.record))
            .transpose()?;
        let result = self
            .view(session)?
            .result(call_id)?
            .map(|result| self.scoped.head_locked(&result.row.record))
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
        let mut entries = Vec::new();
        for row in self.view(session)?.entries_after(after_sequence) {
            let (_, entry) = row?;
            if entries.len() == limit {
                let next_sequence = entries.last().and_then(|header: &RecordHeader| {
                    header.event.as_ref().map(|event| event.sequence)
                });
                return Ok(EventPage {
                    entries,
                    next_sequence,
                });
            }
            let header = self.scoped.head_locked(&entry.record)?;
            entries.push(header);
        }
        Ok(EventPage {
            entries,
            next_sequence: None,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn replay_matches_live_turn_state() {
        let dir = tempfile::tempdir().unwrap();
        let store = crate::Store::create(dir.path().join("store")).unwrap();
        let log = store
            .session_log(ContextAccess {
                run: Some("run".into()),
                ..ContextAccess::new("owner", Actor::System)
            })
            .unwrap();
        log.create_session(
            OperationId::new(),
            crate::session::NewSession {
                session: "session".into(),
                kind: "chat".into(),
                parent: None,
                metadata: serde_json::Value::Null,
            },
            Scope::Personal,
        )
        .unwrap();
        let holder = TurnHolder {
            owner: "owner",
            session: "session",
        };
        let key = session_key(holder.owner, holder.session);
        let mut headers = Vec::new();
        for (id, kind) in [
            ("first", EventKind::TurnStarted),
            ("first-step", EventKind::StepStarted),
            (
                "end-step",
                EventKind::StepEnded {
                    start_event_id: "first-step".into(),
                },
            ),
            (
                "end",
                EventKind::TurnEnded {
                    reason: TurnEndReason::Completed,
                    start_event_id: "first".into(),
                },
            ),
            ("second", EventKind::TurnStarted),
            (
                "call",
                EventKind::ToolCall {
                    call_id: "direct".into(),
                    tool_name: "tool".into(),
                    message_id: None,
                },
            ),
            ("step", EventKind::StepStarted),
            (
                "start",
                EventKind::AssistantStarted {
                    message_id: "msg".into(),
                },
            ),
            (
                "frag",
                EventKind::AssistantFragment {
                    message_id: "msg".into(),
                    ordinal: 0,
                },
            ),
            (
                "result",
                EventKind::ToolResult {
                    call_id: "direct".into(),
                    outcome: ToolOutcome::Succeeded,
                },
            ),
            (
                "terminal",
                EventKind::AssistantTerminal {
                    message_id: "msg".into(),
                    status: MessageStatus::Completed,
                },
            ),
            (
                "linked",
                EventKind::ToolCall {
                    call_id: "linked".into(),
                    tool_name: "tool".into(),
                    message_id: Some("msg".into()),
                },
            ),
            (
                "answer",
                EventKind::ToolResult {
                    call_id: "linked".into(),
                    outcome: ToolOutcome::Succeeded,
                },
            ),
            (
                "step-end",
                EventKind::StepEnded {
                    start_event_id: "step".into(),
                },
            ),
        ] {
            let input = EventInput {
                sources: vec![],
                session: holder.session.into(),
                event_id: id.into(),
                scope: Scope::Personal,
                kind,
                media_type: "text/plain".into(),
                occurred_at_ms: 0,
                expected_sequence: None,
            };
            let receipt = log.append(OperationId::new(), input, &[][..]).unwrap();
            let header = log.inspect_header(&receipt.pin).unwrap();
            if matches!(
                header.event.as_ref().unwrap().kind,
                EventKind::TurnEnded { .. }
            ) {
                headers.clear();
                assert!(log.parts.open_turns.get(&key).unwrap().is_none());
                continue;
            }
            headers.push(header);
            let replay = turns::replay(
                holder,
                headers.iter().map(|h| {
                    let e = h.event.as_ref().unwrap();
                    turns::Replayed {
                        position: e.sequence,
                        event_id: &e.event_id,
                        kind: &e.kind,
                        stamp: e.turn,
                        run: h.provenance.run.as_deref(),
                    }
                }),
            )
            .unwrap();
            let live: OpenTurn = decode(&log.parts.open_turns.get(&key).unwrap().unwrap()).unwrap();
            assert_eq!(replay.turn, live);
            assert_eq!(replay.owed, log.owed_items(&key).unwrap());
            assert_eq!(
                replay.open_messages.get("msg"),
                log.message_state(holder.session, "msg")
                    .unwrap()
                    .filter(|m| m.terminal.is_none())
                    .as_ref()
            );
        }
    }
}
