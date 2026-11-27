use super::catalog::SessionHead;
use super::turns::{Replayed, TurnHolder};
use super::*;
use crate::records::codec::{encode, validate_name};

impl SessionLog<'_> {
    /// Create a session sharing the source log through the given position.
    pub fn fork_session(
        &self,
        operation: OperationId,
        session: NewSession,
        origin: SessionOrigin,
    ) -> ContextResult<WriteReceipt> {
        if !matches!(self.scoped.access().actor, Actor::System | Actor::Operator) {
            return Err(ContextError::AccessDenied);
        }
        validate_name(&origin.session, "source session")?;
        let _guard = self.scoped.lock();
        let source = self.session_head(&origin.session)?;
        self.scoped.check_scope(&source.scope)?;
        if origin.position == 0 {
            return Err(ContextError::InvalidInput(
                "fork position must be positive".into(),
            ));
        }
        if origin.position > source.sequence {
            return Err(ContextError::PositionConflict {
                expected: origin.position,
                actual: source.sequence,
            });
        }
        self.create_locked(operation, session, source.scope, Some(origin))
    }

    pub(super) fn put_lineage(
        &self,
        batch: &mut mmdb_kernel::Batch,
        session: &str,
        origin: &SessionOrigin,
        head: &mut SessionHead,
    ) -> ContextResult<()> {
        let view = self.view(&origin.session)?;
        let entry = view
            .entry(origin.position)?
            .ok_or_else(|| ContextError::Corrupt("missing fork position".into()))?;
        let fork = view.fork(session, origin.position)?;
        let owner = &self.scoped.access().owner;
        let key = session_key(owner, session);
        batch.insert(&self.parts.lineage, &key, encode(&fork.ancestors())?);
        head.sequence = origin.position;
        head.numbers = entry.numbers;
        let Some(start) = entry.open_turn else {
            return Ok(());
        };
        let mut events = Vec::new();
        for position in start..=origin.position {
            let row = view
                .entry(position)?
                .ok_or_else(|| ContextError::Corrupt("missing turn event".into()))?;
            let header = self.scoped.head_locked(&row.record)?;
            let event = header
                .event
                .ok_or_else(|| ContextError::Corrupt("missing event header".into()))?;
            events.push((position, event, header.provenance.run));
        }
        let mut replay = turns::replay(
            TurnHolder { owner, session },
            events.iter().map(|(position, event, run)| Replayed {
                position: *position,
                event_id: &event.event_id,
                kind: &event.kind,
                stamp: event.turn,
                run: run.as_deref(),
            }),
        )?;
        self.put_turn_end(
            batch,
            head,
            &key,
            &replay.turn,
            &replay.owed,
            TurnEndReason::Forked,
            crate::now_ms(),
            &fork,
            Some(&mut replay.open_messages),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Scope, Store};

    fn new(session: &str) -> NewSession {
        NewSession {
            session: session.into(),
            kind: "chat".into(),
            parent: None,
            metadata: serde_json::Value::Null,
        }
    }

    fn origin() -> SessionOrigin {
        SessionOrigin {
            session: "source".into(),
            position: 3,
        }
    }

    fn source(store: &Store) -> SessionLog<'_> {
        let log = store
            .session_log(ContextAccess {
                run: Some("run".into()),
                ..ContextAccess::new("owner", Actor::System)
            })
            .unwrap();
        log.create_session(OperationId::new(), new("source"), Scope::Personal)
            .unwrap();
        for (id, kind) in [
            ("prefix", EventKind::Notice),
            ("turn", EventKind::TurnStarted),
            ("step", EventKind::StepStarted),
            ("suffix", EventKind::Notice),
        ] {
            log.append(
                OperationId::new(),
                EventInput {
                    session: "source".into(),
                    event_id: id.into(),
                    kind,
                    scope: Scope::Personal,
                    sources: Vec::new(),
                    expected_sequence: None,
                    media_type: "text/plain".into(),
                    occurred_at_ms: 1,
                },
                &[][..],
            )
            .unwrap();
        }
        log
    }

    #[test]
    fn owner_search_rejects_invalid_order_records() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::create(dir.path().join("store")).unwrap();
        let log = source(&store);
        let mut key = session_key("owner", "source");
        key.extend(1u64.to_be_bytes());
        let mut entry = log.view("source").unwrap().entry(1).unwrap().unwrap();
        for (owner, kind) in [("owner", RecordKind::Session), ("other", RecordKind::Event)] {
            entry.record.owner = owner.into();
            entry.record.kind = kind;
            let mut batch = log.scoped.batch();
            batch.insert(&log.parts.order, &key, encode(&entry).unwrap());
            batch.commit().unwrap();
            assert!(matches!(
                log.search(LogQuery {
                    text: "x".into(),
                    session: None,
                    cursor: None,
                    limit: 1,
                    max_events: 1,
                    max_bytes: 10,
                }),
                Err(ContextError::Corrupt(_))
            ));
        }
    }

    #[test]
    fn forking_reads_only_the_cut_turn() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::create(dir.path().join("store")).unwrap();
        let log = source(&store);
        let mut batch = log.scoped.batch();
        // Reading either entry would now fail decoding.
        for position in [1_u64, 4] {
            let mut key = session_key("owner", "source");
            key.extend(position.to_be_bytes());
            batch.insert(&log.parts.order, key, b"invalid");
        }
        batch.commit().unwrap();
        assert_eq!(
            log.fork_session(OperationId::new(), new("fork"), origin())
                .unwrap()
                .sequence,
            Some(5)
        );
    }

    #[test]
    fn a_lost_fork_commit_leaves_no_session() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("store");
        let operation = OperationId::new();
        {
            let store = Store::create(&path).unwrap();
            let log = source(&store);
            let before = log.session("source").unwrap();
            log.scoped.drop_next_commit(&log.parts.lineage);
            assert!(
                matches!(log.fork_session(operation,new("fork"),origin()),Err(ContextError::CommitUnknown(op)) if op==operation)
            );
            assert_eq!(log.session("source").unwrap(), before);
        }
        let store = Store::open(&path).unwrap();
        let log = store
            .session_log(ContextAccess::new("owner", Actor::System))
            .unwrap();
        assert!(matches!(
            log.session("fork"),
            Err(ContextError::UnknownSession(_))
        ));
        assert!(log.operation_receipt(operation).unwrap().is_none());
        let key = session_key("owner", "fork");
        assert!(log.parts.lineage.get(&key).unwrap().is_none());
        assert!(log.parts.order.prefix(&key).next().is_none());
        assert_eq!(
            log.fork_session(operation, new("fork"), origin())
                .unwrap()
                .sequence,
            Some(5)
        );
    }
}
