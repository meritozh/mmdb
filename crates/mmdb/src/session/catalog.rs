use super::turns::{Numbers, OpenTurn};
use super::*;
use crate::records::codec::*;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
/// Fields a harness chooses when it creates or forks a session.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NewSession {
    pub session: String,
    pub kind: String,
    pub parent: Option<SessionParent>,
    pub metadata: serde_json::Value,
}

/// A session header with the counters MMDB maintains for it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionState {
    /// Current header revision; pass it to `update_session`.
    pub pin: RecordPin,
    pub scope: Scope,
    pub header: SessionHeader,
    pub position: u64,
    pub turns: u64,
    pub open_run: Option<String>,
    pub changed_at_ms: i64,
    pub change: u64,
}

/// Select sessions in descending change-number order.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionQuery {
    pub kind: Option<String>,
    pub parent: Option<String>,
    /// Continue below this change number.
    pub before: Option<u64>,
    pub limit: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionPage {
    pub sessions: Vec<SessionState>,
    pub next: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) struct SessionHead {
    pub header: ContextRef,
    pub scope: Scope,
    pub creation: [u8; 32],
    pub created: u64,
    pub sequence: u64,
    pub numbers: Numbers,
    pub change: u64,
    pub changed_at_ms: i64,
}

#[derive(Serialize, Deserialize)]
pub(super) struct ChangeRow {
    pub session: String,
    pub kind: String,
    pub parent: Option<String>,
}

#[derive(Default)]
pub(super) struct Changes(BTreeMap<String, u64>);

impl Changes {
    pub(super) fn put_head(
        &mut self,
        log: &SessionLog<'_>,
        batch: &mut mmdb_kernel::Batch,
        session: &str,
        head: &mut SessionHead,
        creation: Option<ChangeRow>,
    ) -> ContextResult<()> {
        let owner = &log.scoped.access().owner;
        let row = if let Some(row) = creation {
            encode(&row)?
        } else {
            let key = change_key(owner, head.change);
            let row = log
                .parts
                .changes
                .get(&key)?
                .ok_or_else(|| ContextError::Corrupt("missing change row".into()))?;
            batch.remove(&log.parts.changes, key);
            row.to_vec()
        };
        let last = match self.0.get(owner) {
            Some(last) => *last,
            None => match log
                .parts
                .changes
                .prefix_keys(owner_key(owner))
                .next_back()
                .transpose()?
            {
                None => 0,
                Some(key) => u64::from_be_bytes(
                    key[owner_key(owner).len()..]
                        .try_into()
                        .map_err(|_| ContextError::Corrupt("invalid change number".into()))?,
                ),
            },
        };
        head.change = last.checked_add(1).ok_or(ContextError::BudgetExceeded)?;
        self.0.insert(owner.clone(), head.change);
        head.changed_at_ms = crate::now_ms();
        batch.insert(&log.parts.changes, change_key(owner, head.change), row);
        batch.insert(&log.parts.heads, session_key(owner, session), encode(head)?);
        Ok(())
    }
}

fn change_key(owner: &str, change: u64) -> Vec<u8> {
    let mut key = owner_key(owner);
    key.extend(change.to_be_bytes());
    key
}

fn validate_metadata(metadata: &serde_json::Value) -> ContextResult<()> {
    if encode(metadata)?.len() > 4096 {
        return Err(ContextError::InvalidInput(
            "session metadata exceeds 4 KiB".into(),
        ));
    }
    Ok(())
}

impl SessionLog<'_> {
    pub(super) fn session_head(&self, session: &str) -> ContextResult<SessionHead> {
        let bytes = self
            .parts
            .heads
            .get(session_key(&self.scoped.access().owner, session))?
            .ok_or_else(|| ContextError::UnknownSession(session.to_owned()))?;
        decode(&bytes)
    }

    pub fn session(&self, session: &str) -> ContextResult<SessionState> {
        validate_name(session, "session")?;
        let _guard = self.scoped.lock();
        let head = self.session_head(session)?;
        self.session_state(session, head)
    }

    fn session_state(&self, session: &str, head: SessionHead) -> ContextResult<SessionState> {
        let header = self.scoped.head_locked(&head.header)?;
        let turn: Option<OpenTurn> = self
            .parts
            .open_turns
            .get(session_key(&self.scoped.access().owner, session))?
            .map(|v| decode(&v))
            .transpose()?;
        Ok(SessionState {
            pin: header.pin,
            scope: head.scope,
            header: header
                .session
                .ok_or_else(|| ContextError::Corrupt("missing session header".into()))?,
            position: head.sequence,
            turns: head.numbers.turns,
            open_run: turn.map(|t| t.run),
            changed_at_ms: head.changed_at_ms,
            change: head.change,
        })
    }

    pub fn sessions(&self, query: SessionQuery) -> ContextResult<SessionPage> {
        use std::ops::Bound::{Excluded, Included};

        let access = self.scoped.access();
        if !matches!(access.actor, Actor::System | Actor::Operator) {
            return Err(ContextError::AccessDenied);
        }
        if !(1..=100).contains(&query.limit) {
            return Err(ContextError::InvalidInput(
                "session page limit must be 1..=100".into(),
            ));
        }
        if let Some(kind) = &query.kind {
            validate_name(kind, "session kind")?;
        }
        if let Some(parent) = &query.parent {
            validate_name(parent, "parent session")?;
        }
        let _guard = self.scoped.lock();
        let prefix = owner_key(&access.owner);
        let upper = match query.before {
            Some(before) => Excluded(change_key(&access.owner, before)),
            None => Included(change_key(&access.owner, u64::MAX)),
        };
        let mut sessions = Vec::new();
        let mut next = None;
        let range = (Included(prefix.clone()), upper);
        for (scanned, item) in self.parts.changes.range(range).rev().take(4096).enumerate() {
            let (key, value) = item?;
            let change = u64::from_be_bytes(
                key[prefix.len()..]
                    .try_into()
                    .map_err(|_| ContextError::Corrupt("invalid change number".into()))?,
            );
            let row: ChangeRow = decode(&value)?;
            if query.kind.as_ref().is_none_or(|kind| kind == &row.kind)
                && query
                    .parent
                    .as_ref()
                    .is_none_or(|parent| Some(parent) == row.parent.as_ref())
            {
                let head = self.session_head(&row.session)?;
                if access.scopes.contains(&head.scope) {
                    sessions.push(self.session_state(&row.session, head)?);
                }
            }
            if sessions.len() == query.limit || scanned + 1 == 4096 {
                if self
                    .parts
                    .changes
                    .range_keys(prefix.clone()..key.to_vec())
                    .next_back()
                    .transpose()?
                    .is_some()
                {
                    next = Some(change);
                }
                break;
            }
        }
        Ok(SessionPage { sessions, next })
    }

    pub fn update_session(
        &self,
        operation: OperationId,
        expected: RecordPin,
        metadata: serde_json::Value,
    ) -> ContextResult<WriteReceipt> {
        if !matches!(self.scoped.access().actor, Actor::System | Actor::Operator) {
            return Err(ContextError::AccessDenied);
        }
        if expected.record.kind != RecordKind::Session {
            return Err(ContextError::InvalidInput(
                "expected a session header".into(),
            ));
        }
        validate_metadata(&metadata)?;
        let _guard = self.scoped.lock();
        let digest = self
            .scoped
            .digest("update-session", &(&expected, &metadata))?;
        if let Some(receipt) = self.scoped.replay(operation, digest)? {
            return Ok(receipt);
        }
        let (mut header, pin) = self.scoped.next_revision(&expected)?;
        let session = header
            .session
            .as_mut()
            .ok_or_else(|| ContextError::Corrupt("missing session header".into()))?;
        session.metadata = metadata;
        let name = session.session.clone();
        let mut head = self.session_head(&name)?;
        header.pin = pin.clone();
        header.provenance = self.scoped.provenance();
        header.recorded_at_ms = crate::now_ms();
        let mut batch = self.scoped.batch();
        self.scoped.put_header(&mut batch, &header)?;
        Changes::default().put_head(self, &mut batch, &name, &mut head, None)?;
        self.scoped.commit_operation(
            batch,
            operation,
            digest,
            WriteReceipt {
                pin,
                sequence: Some(head.sequence),
            },
        )
    }

    pub fn create_session(
        &self,
        operation: OperationId,
        session: NewSession,
        scope: Scope,
    ) -> ContextResult<WriteReceipt> {
        let access = self.scoped.access();
        if !matches!(access.actor, Actor::Operator | Actor::System)
            || access
                .session
                .as_ref()
                .is_some_and(|s| s != &session.session)
        {
            return Err(ContextError::AccessDenied);
        }
        validate_name(&session.session, "session")?;
        validate_name(&session.kind, "session kind")?;
        validate_metadata(&session.metadata)?;
        self.scoped.check_scope(&scope)?;
        let _guard = self.scoped.lock();
        if let Some(parent) = &session.parent {
            validate_name(&parent.session, "parent session")?;
            self.scoped
                .check_scope(&self.session_head(&parent.session)?.scope)?;
        }
        let origin: Option<SessionOrigin> = None;
        let content = (&session, &scope, &origin);
        let digest = self.scoped.digest("create-session", &content)?;
        let creation = self.scoped.content_digest(&encode(&content)?);
        if let Some(receipt) = self.scoped.replay(operation, digest)? {
            return Ok(receipt);
        }
        let mut batch = self.scoped.batch();
        let key = session_key(&access.owner, &session.session);
        if let Some(bytes) = self.parts.heads.get(key)? {
            let head: SessionHead = decode(&bytes)?;
            self.scoped.check_scope(&head.scope)?;
            if head.creation != creation {
                return Err(ContextError::IdempotencyConflict);
            }
            return self.scoped.commit_operation(
                batch,
                operation,
                digest,
                WriteReceipt {
                    pin: RecordPin {
                        record: head.header,
                        revision: 1,
                    },
                    sequence: Some(head.created),
                },
            );
        }
        let pin = self.scoped.fresh_pin(RecordKind::Session);
        let now = crate::now_ms();
        let row = ChangeRow {
            session: session.session.clone(),
            kind: session.kind.clone(),
            parent: session.parent.as_ref().map(|p| p.session.clone()),
        };
        let header = RecordHeader {
            session: Some(SessionHeader {
                session: session.session.clone(),
                kind: session.kind,
                parent: session.parent,
                origin,
                created_at_ms: now,
                metadata: session.metadata,
            }),
            pin: pin.clone(),
            scope: scope.clone(),
            state: RecordState::Active,
            type_pin: None,
            sources: vec![],
            source_scopes: vec![],
            provenance: self.scoped.provenance(),
            information: InformationKind::Observation,
            temporal: TemporalFacts::observed_at(now),
            recorded_at_ms: now,
            payload: self.scoped.put_payload(&mut batch, &pin, &[]),
            endpoints: None,
            event: None,
            checkpoint: None,
        };
        self.scoped.put_header(&mut batch, &header)?;
        let mut head = SessionHead {
            header: pin.record.clone(),
            scope,
            creation,
            created: 0,
            sequence: 0,
            numbers: Numbers::default(),
            change: 0,
            changed_at_ms: now,
        };
        Changes::default().put_head(self, &mut batch, &session.session, &mut head, Some(row))?;
        self.scoped.commit_operation(
            batch,
            operation,
            digest,
            WriteReceipt {
                pin,
                sequence: Some(0),
            },
        )
    }
}
