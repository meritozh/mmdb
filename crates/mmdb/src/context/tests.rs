use super::*;
use crate::session::{CheckpointInput, EventInput, SessionLog};
use crate::Store;
use crate::{Actor, OperationId, RecordState, Scope, TemporalFacts};
use serde_json::json;
use std::collections::BTreeMap;
use ulid::Ulid;

#[test]
fn current_sources_guard_keeps_retraction_after_the_protected_commit() {
    use std::sync::{mpsc, Arc};
    use std::time::Duration;

    let dir = tempfile::tempdir().unwrap();
    let db = Arc::new(Store::create(dir.path().join("context")).unwrap());
    let access = ContextAccess::new("owner", Actor::User);
    let context = db.context(access.clone()).unwrap();
    let log = db.session_log(access.clone()).unwrap();
    let pin = event_source(&log, "session", "request");
    let guard = context
        .validate_current_guard(std::slice::from_ref(&pin), None, Some(0))
        .unwrap();
    assert_eq!(guard.availability_epoch(), 0);

    let (started_tx, started_rx) = mpsc::channel();
    let (done_tx, done_rx) = mpsc::channel();
    let other_db = db.clone();
    let other_pin = pin.clone();
    let thread = std::thread::spawn(move || {
        let log = other_db.session_log(access).unwrap();
        started_tx.send(()).unwrap();
        let result = log.retract(OperationId::new(), other_pin);
        done_tx.send(result).unwrap();
    });
    started_rx.recv().unwrap();
    assert!(matches!(
        done_rx.recv_timeout(Duration::from_millis(50)),
        Err(mpsc::RecvTimeoutError::Timeout)
    ));

    // A real commit in a separate state root can complete under the guard.
    let state = crate::StateStore::open_path(
        dir.path().join("state"),
        0,
        &crate::StoreFormatDescriptor::new("guard-test-v1").unwrap(),
    )
    .unwrap();
    state
        .put("approval", "accepted", serde_json::json!({ "source": pin }))
        .unwrap();
    drop(guard);
    done_rx
        .recv_timeout(Duration::from_secs(5))
        .unwrap()
        .unwrap();
    thread.join().unwrap();
    assert!(state.get("approval", "accepted").unwrap().is_some());
    assert!(matches!(
        context.validate_current_guard(std::slice::from_ref(&pin), None, None),
        Err(ContextError::Unavailable(_))
    ));
    assert!(matches!(
        context.validate_current_guard(&[], None, Some(0)),
        Err(ContextError::AvailabilityConflict {
            expected: 0,
            actual: 1
        })
    ));
}

#[test]
fn current_sources_guard_rejects_stale_and_out_of_scope_sources_without_writing() {
    let dir = tempfile::tempdir().unwrap();
    let db = Store::create(dir.path().join("context")).unwrap();
    let access = ContextAccess::new("owner", Actor::User);
    let context = db.context(access.clone()).unwrap();
    let log = db.session_log(access).unwrap();
    let pin = event_source(&log, "session", "request");
    let other = db
        .context(ContextAccess::new("another-owner", Actor::System))
        .unwrap();
    assert!(matches!(
        other.validate_current_guard(std::slice::from_ref(&pin), None, None),
        Err(ContextError::AccessDenied)
    ));
    log.retract(OperationId::new(), pin.clone()).unwrap();
    assert!(matches!(
        context.validate_current_guard(std::slice::from_ref(&pin), None, None),
        Err(ContextError::Unavailable(_))
    ));
    assert!(matches!(
        context.validate_current_guard(&vec![pin; MAX_CONTEXT_SOURCES + 1], None, None),
        Err(ContextError::BudgetExceeded)
    ));
}

#[test]
fn committed_history_survives_a_lost_acknowledgement_without_duplication() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("context");
    let operation = OperationId::new();
    let profile = |actor| ContextAccess {
        run: Some("run".into()),
        ..ContextAccess::new("owner", actor)
    };
    let input = EventInput {
        sources: Vec::new(),
        session: "continuous-session".into(),
        event_id: "message-1".into(),
        scope: Scope::Personal,
        kind: EventKind::AssistantMessage,
        media_type: "text/plain".into(),
        occurred_at_ms: 1,
        expected_sequence: Some(2),
    };
    let expected = {
        let db = Store::create(&root).unwrap();
        let store = db.session_log(profile(Actor::Assistant)).unwrap();
        let system = db.session_log(profile(Actor::System)).unwrap();
        for (id, kind) in [
            ("turn", EventKind::TurnStarted),
            ("step", EventKind::StepStarted),
        ] {
            let boundary = EventInput {
                event_id: id.into(),
                kind,
                expected_sequence: None,
                ..input.clone()
            };
            system
                .append(OperationId::new(), boundary, &[][..])
                .unwrap();
        }
        store.scoped.lose_next_commit_ack();
        let error = store
            .append(operation, input.clone(), b"durable answer".as_slice())
            .unwrap_err();
        assert!(matches!(error, ContextError::CommitUnknown(id) if id == operation));
        assert_eq!(store.position("continuous-session").unwrap(), 3);
        store.operation_receipt(operation).unwrap().unwrap()
    };
    let db = Store::open(&root).unwrap();
    let store = db.session_log(profile(Actor::Assistant)).unwrap();
    assert_eq!(
        store.operation_receipt(operation).unwrap(),
        Some(expected.clone())
    );
    let reopened_position = store.position("continuous-session").unwrap();
    assert_eq!(
        store
            .append(operation, input, b"durable answer".as_slice())
            .unwrap(),
        expected
    );
    assert_eq!(
        store.position("continuous-session").unwrap(),
        reopened_position
    );
    assert_eq!(
        store
            .events("continuous-session", 0, 10)
            .unwrap()
            .entries
            .iter()
            .filter(|header| header.event.as_ref().unwrap().kind == EventKind::AssistantMessage)
            .count(),
        1
    );
    assert_eq!(
        store.read_payload(&expected.pin, 0, 32).unwrap().bytes,
        b"durable answer"
    );
    assert_eq!(
        store.head(&expected.pin.record).unwrap().state,
        RecordState::Active
    );
    assert_eq!(store.recover_payloads(1, 1).unwrap().uploads_removed, 0);
}

#[test]
fn a_cancel_with_a_lost_acknowledgement_published_all_its_closing_events() {
    let dir = tempfile::tempdir().unwrap();
    let db = Store::create(dir.path().join("context")).unwrap();
    let log = db
        .session_log(ContextAccess {
            run: Some("run".into()),
            ..ContextAccess::new("owner", Actor::System)
        })
        .unwrap();
    let event = |id: &str, kind| EventInput {
        session: "session".into(),
        event_id: id.into(),
        scope: Scope::Personal,
        kind,
        media_type: "text/plain".into(),
        occurred_at_ms: 1,
        sources: vec![],
        expected_sequence: None,
    };
    for input in [
        event("turn", EventKind::TurnStarted),
        event("step", EventKind::StepStarted),
        event(
            "message",
            EventKind::AssistantStarted {
                message_id: "m".into(),
            },
        ),
        event(
            "call",
            EventKind::ToolCall {
                call_id: "c".into(),
                tool_name: "read".into(),
                message_id: Some("m".into()),
            },
        ),
    ] {
        log.append(OperationId::new(), input, &[][..]).unwrap();
    }
    let cancel = EventKind::TurnEnded {
        start_event_id: "turn".into(),
        reason: crate::TurnEndReason::Cancelled,
    };
    let before = log.position("session").unwrap();
    let operation = OperationId::new();
    log.scoped.lose_next_commit_ack();
    let error = log
        .append(
            operation,
            event("cancel", cancel.clone()),
            &b"cancelled"[..],
        )
        .unwrap_err();
    assert!(matches!(error, ContextError::CommitUnknown(id) if id == operation));
    assert_eq!(log.position("session").unwrap(), before + 4);
    let events = log.events("session", before, 10).unwrap().entries;
    let kinds: Vec<_> = events
        .iter()
        .map(|header| header.event.as_ref().unwrap().kind.clone())
        .collect();
    assert_eq!(
        kinds,
        [
            EventKind::AssistantTerminal {
                message_id: "m".into(),
                status: MessageStatus::Interrupted
            },
            EventKind::ToolResult {
                call_id: "c".into(),
                outcome: ToolOutcome::EffectUnknown
            },
            EventKind::StepEnded {
                start_event_id: "step".into(),
            },
            cancel,
        ]
    );
    let receipt = log.operation_receipt(operation).unwrap().unwrap();
    assert_eq!(receipt.pin, events[3].pin);
    assert_eq!(receipt.sequence, Some(before + 4));
    log.append(
        OperationId::new(),
        event("next", EventKind::TurnStarted),
        &[][..],
    )
    .unwrap();
}

#[test]
fn checksum_failure_is_reported_instead_of_returning_modified_source_bytes() {
    let dir = tempfile::tempdir().unwrap();
    let db = Store::create(dir.path().join("context")).unwrap();
    let store = db
        .session_log(ContextAccess::new("owner", Actor::System))
        .unwrap();
    let receipt = store
        .append(
            OperationId::new(),
            EventInput {
                sources: Vec::new(),
                session: "session".into(),
                event_id: "message".into(),
                scope: Scope::Personal,
                kind: EventKind::UserMessage,
                media_type: "text/plain".into(),
                occurred_at_ms: 1,
                expected_sequence: None,
            },
            b"original".as_slice(),
        )
        .unwrap();
    let key = storage::payload_key(&receipt.pin, 0);
    let mut bytes = store
        .scoped
        .test_payloads(receipt.pin.record.kind)
        .get(&key)
        .unwrap()
        .unwrap()
        .to_vec();
    bytes[32] ^= 1;
    let mut batch = store.scoped.batch();
    batch.insert(
        store.scoped.test_payloads(receipt.pin.record.kind),
        key,
        bytes,
    );
    batch.commit().unwrap();
    assert!(matches!(
        store.read_payload(&receipt.pin, 0, 32),
        Err(ContextError::Corrupt(_))
    ));
}

#[test]
fn assistant_cannot_retract_instruction_records_but_may_retract_others() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("context");

    let owner = Store::create(&root).unwrap();
    let user = owner
        .context(ContextAccess::new("owner", Actor::User))
        .unwrap();
    let type_pin = user
        .define_type(
            OperationId::new(),
            TypeDefinition {
                name: "note".into(),
                scope: Scope::Personal,
                kind: TypeKind::Object,
                properties: BTreeMap::from([(
                    "text".into(),
                    PropertyDefinition {
                        value_type: PropertyType::Text,
                        required: true,
                    },
                )]),
            },
        )
        .unwrap()
        .pin;
    let source = owner
        .session_log(ContextAccess::new("owner", Actor::User))
        .unwrap()
        .append(
            OperationId::new(),
            EventInput {
                sources: Vec::new(),
                session: "s".into(),
                event_id: "e".into(),
                scope: Scope::Personal,
                kind: EventKind::UserMessage,
                media_type: "text/plain".into(),
                occurred_at_ms: 1,
                expected_sequence: None,
            },
            b"source".as_slice(),
        )
        .unwrap()
        .pin;

    // A User authors an Instruction record.
    let instruction = user
        .save_object(
            OperationId::new(),
            ObjectInput {
                type_pin: type_pin.clone(),
                scope: Scope::Personal,
                properties: BTreeMap::from([("text".into(), json!("do the thing"))]),
                sources: vec![source.clone()],
                information: InformationKind::Instruction,
                temporal: TemporalFacts::observed_at(10),
            },
        )
        .unwrap()
        .pin;

    // An Assistant must NOT be able to retract the Instruction it could not
    // have authored.
    let assistant = owner
        .context(ContextAccess::new("owner", Actor::Assistant))
        .unwrap();
    assert!(matches!(
        assistant.retract(OperationId::new(), instruction.clone()),
        Err(ContextError::AccessDenied)
    ));
    assert_eq!(
        user.head(&instruction.record).unwrap().state,
        RecordState::Active
    );

    // The User can retract it.
    user.retract(OperationId::new(), instruction.clone())
        .unwrap();
    assert_eq!(
        user.head(&instruction.record).unwrap().state,
        RecordState::Retracted
    );

    // Assistant retraction stays allowed for non-Instruction records.
    let inference = assistant
        .save_object(
            OperationId::new(),
            ObjectInput {
                type_pin,
                scope: Scope::Personal,
                properties: BTreeMap::from([("text".into(), json!("an inference"))]),
                sources: vec![source],
                information: InformationKind::Inference,
                temporal: TemporalFacts::observed_at(10),
            },
        )
        .unwrap()
        .pin;
    assistant
        .retract(OperationId::new(), inference.clone())
        .unwrap();
    assert_eq!(
        assistant.head(&inference.record).unwrap().state,
        RecordState::Retracted
    );
}

// ---------------------------------------------------------------------------
// Feature A4 — temporal semantics & process records
// ---------------------------------------------------------------------------

use crate::entity::{EntityRef, ProcessRecord};
use crate::extraction::{ExtractionOp, MAX_BATCH_SIZE};

fn block_on<F: std::future::Future>(future: F) -> F::Output {
    use std::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};
    fn raw() -> RawWaker {
        fn clone(_: *const ()) -> RawWaker {
            raw()
        }
        fn wake(_: *const ()) {}
        fn wake_by_ref(_: *const ()) {}
        fn drop(_: *const ()) {}
        RawWaker::new(
            std::ptr::null(),
            &RawWakerVTable::new(clone, wake, wake_by_ref, drop),
        )
    }
    let waker = unsafe { Waker::from_raw(raw()) };
    let mut cx = Context::from_waker(&waker);
    let mut future = Box::pin(future);
    loop {
        match std::future::Future::poll(future.as_mut(), &mut cx) {
            Poll::Ready(v) => return v,
            Poll::Pending => std::thread::yield_now(),
        }
    }
}

fn fresh_store() -> (
    tempfile::TempDir,
    ContextStore<'static>,
    SessionLog<'static>,
) {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("context");
    let db = Box::leak(Box::new(Store::create(&root).unwrap()));
    let access = ContextAccess::new("owner", Actor::User);
    let context = db.context(access.clone()).unwrap();
    let log = db.session_log(access).unwrap();
    (dir, context, log)
}

fn object_type(store: &ContextStore, name: &str) -> RecordPin {
    store
        .define_type(
            OperationId::new(),
            TypeDefinition {
                name: name.into(),
                scope: Scope::Personal,
                kind: TypeKind::Object,
                properties: BTreeMap::from([(
                    "text".into(),
                    PropertyDefinition {
                        value_type: PropertyType::Text,
                        required: false,
                    },
                )]),
            },
        )
        .unwrap()
        .pin
}

fn event_source(store: &SessionLog, session: &str, event: &str) -> RecordPin {
    store
        .append(
            OperationId::new(),
            EventInput {
                sources: Vec::new(),
                session: session.into(),
                event_id: event.into(),
                scope: Scope::Personal,
                kind: EventKind::UserMessage,
                media_type: "text/plain".into(),
                occurred_at_ms: 1,
                expected_sequence: None,
            },
            b"source".as_slice(),
        )
        .unwrap()
        .pin
}

#[test]
fn temporal_fields_distinguishable() {
    let (_dir, store, log) = fresh_store();
    let typ = object_type(&store, "note");
    let source = event_source(&log, "s", "e");
    let temporal = TemporalFacts {
        observed_at_ms: 1000,
        source_at_ms: Some(500),
        valid_from_ms: Some(2000),
        valid_to_ms: Some(9000),
    };
    let receipt = store
        .save_object(
            OperationId::new(),
            ObjectInput {
                type_pin: typ,
                scope: Scope::Personal,
                properties: BTreeMap::from([("text".into(), json!("t"))]),
                sources: vec![source],
                information: InformationKind::Observation,
                temporal,
            },
        )
        .unwrap();
    let header = store.head(&receipt.pin.record).unwrap();
    // recorded_at_ms (system write time) must differ from observed_at_ms.
    assert_ne!(header.recorded_at_ms, header.temporal.observed_at_ms);
    assert_eq!(header.temporal.observed_at_ms, 1000);
    assert_eq!(header.temporal.valid_from_ms, Some(2000));
    assert_eq!(header.temporal.valid_to_ms, Some(9000));
    // is_valid_at reasons only about the valid window + state.
    assert!(!header.is_valid_at(1500));
    assert!(header.is_valid_at(5000));
    assert!(!header.is_valid_at(9000));
}

#[test]
fn query_current_valid_filters_by_time_and_state() {
    let (_dir, store, log) = fresh_store();
    let typ = object_type(&store, "note");
    let source = event_source(&log, "s", "e");

    // Active, valid at now.
    let active = store
        .save_object(
            OperationId::new(),
            ObjectInput {
                type_pin: typ.clone(),
                scope: Scope::Personal,
                properties: BTreeMap::from([("text".into(), json!("active"))]),
                sources: vec![source.clone()],
                information: InformationKind::Observation,
                temporal: TemporalFacts::observed_at(1),
            },
        )
        .unwrap();
    // Not yet valid (valid window starts far in the future).
    let future = store
        .save_object(
            OperationId::new(),
            ObjectInput {
                type_pin: typ.clone(),
                scope: Scope::Personal,
                properties: BTreeMap::from([("text".into(), json!("future"))]),
                sources: vec![source.clone()],
                information: InformationKind::Observation,
                temporal: TemporalFacts {
                    observed_at_ms: 1,
                    source_at_ms: None,
                    valid_from_ms: Some(i64::MAX),
                    valid_to_ms: None,
                },
            },
        )
        .unwrap();
    // Retracted.
    let retracted = store
        .save_object(
            OperationId::new(),
            ObjectInput {
                type_pin: typ.clone(),
                scope: Scope::Personal,
                properties: BTreeMap::from([("text".into(), json!("retracted"))]),
                sources: vec![source],
                information: InformationKind::Observation,
                temporal: TemporalFacts::observed_at(1),
            },
        )
        .unwrap();
    store
        .retract(OperationId::new(), retracted.pin.clone())
        .unwrap();

    let current = store.query_current_valid(None, None, 100).unwrap();
    let ids: Vec<_> = current.iter().map(|r| r.header.pin.record.id).collect();
    assert!(ids.contains(&active.pin.record.id));
    assert!(!ids.contains(&future.pin.record.id));
    assert!(!ids.contains(&retracted.pin.record.id));
}

#[test]
fn process_record_event_decision_transition_serialization() {
    let event = ProcessRecord::Event {
        event_type: "login".into(),
        occurred_at_ms: 1,
        observed_at_ms: 2,
        payload: json!({"user": "u"}),
    };
    let decision = ProcessRecord::Decision {
        decision_id: Ulid::new(),
        decided_at_ms: 3,
        rationale: "r".into(),
        alternatives: vec!["a".into(), "b".into()],
        chosen: "a".into(),
        source_operation: None,
    };
    let transition = ProcessRecord::StateTransition {
        entity_ref: EntityRef::new("ns", "widget", "w1"),
        from_state: "off".into(),
        to_state: "on".into(),
        transitioned_at_ms: 4,
        cause: None,
    };
    for record in [event, decision, transition] {
        let bytes = serde_json::to_vec(&record).unwrap();
        let back: ProcessRecord = serde_json::from_slice(&bytes).unwrap();
        match (record, back) {
            (
                ProcessRecord::Event { event_type, .. },
                ProcessRecord::Event { event_type: e2, .. },
            ) => {
                assert_eq!(event_type, e2);
            }
            (
                ProcessRecord::Decision { decision_id, .. },
                ProcessRecord::Decision {
                    decision_id: d2, ..
                },
            ) => {
                assert_eq!(decision_id, d2);
            }
            (
                ProcessRecord::StateTransition { entity_ref, .. },
                ProcessRecord::StateTransition { entity_ref: r2, .. },
            ) => {
                assert_eq!(entity_ref, r2);
            }
            _ => panic!("process record tag mismatch"),
        }
    }
}

#[test]
fn state_transition_references_cause() {
    let cause = Ulid::new();
    let transition = ProcessRecord::StateTransition {
        entity_ref: EntityRef::new("ns", "widget", "w1"),
        from_state: "off".into(),
        to_state: "on".into(),
        transitioned_at_ms: 4,
        cause: Some(cause),
    };
    let bytes = serde_json::to_vec(&transition).unwrap();
    let back: ProcessRecord = serde_json::from_slice(&bytes).unwrap();
    match back {
        ProcessRecord::StateTransition { cause: Some(c), .. } => assert_eq!(c, cause),
        _ => panic!("expected cause to round-trip"),
    }
}

// ---------------------------------------------------------------------------
// Feature A2 — entity identity MVP
// ---------------------------------------------------------------------------

#[test]
fn entity_resolve_or_create_is_atomic_and_idempotent() {
    let (_dir, store, _) = fresh_store();
    let r = EntityRef::new("ns", "person", "alice");
    let first = block_on(store.resolve_or_create_entity(OperationId::new(), r.clone())).unwrap();
    let second = block_on(store.resolve_or_create_entity(OperationId::new(), r.clone())).unwrap();
    assert_eq!(first.node_id, second.node_id);
    assert!(first.alias_of.is_none());
    let again = store.get_entity(&r).unwrap().unwrap();
    assert_eq!(again.node_id, first.node_id);
}

#[test]
fn entity_identity_unique_constraint() {
    let (_dir, store, _) = fresh_store();
    let primary = EntityRef::new("ns", "person", "bob");
    block_on(store.resolve_or_create_entity(OperationId::new(), primary.clone())).unwrap();
    // A different entity already owns this key.
    let taken = EntityRef::new("ns", "person", "dave");
    block_on(store.resolve_or_create_entity(OperationId::new(), taken.clone())).unwrap();
    // Aliasing onto an already-occupied key must be rejected.
    let err = store
        .add_entity_alias(OperationId::new(), &primary, taken)
        .unwrap_err();
    assert!(matches!(err, ContextError::IdempotencyConflict));
}

#[test]
fn entity_alias_resolves_to_primary() {
    let (_dir, store, _) = fresh_store();
    let primary = EntityRef::new("ns", "person", "carol");
    let created =
        block_on(store.resolve_or_create_entity(OperationId::new(), primary.clone())).unwrap();
    let alias = EntityRef::new("ns", "person", "carol-alias");
    let aliased = store
        .add_entity_alias(OperationId::new(), &primary, alias.clone())
        .unwrap();
    assert_eq!(aliased.node_id, created.node_id);
    assert_eq!(aliased.alias_of, Some(created.node_id));
    let via_alias = store.get_entity(&alias).unwrap().unwrap();
    assert_eq!(via_alias.node_id, created.node_id);
}

#[test]
fn entity_merge_redirects_identities() {
    let (_dir, store, _) = fresh_store();
    let source = EntityRef::new("ns", "person", "old");
    let target = EntityRef::new("ns", "person", "new");
    let s = block_on(store.resolve_or_create_entity(OperationId::new(), source.clone())).unwrap();
    let t = block_on(store.resolve_or_create_entity(OperationId::new(), target.clone())).unwrap();
    store
        .merge_entities(OperationId::new(), &source, &target)
        .unwrap();
    let redirected = store.get_entity(&source).unwrap().unwrap();
    assert_eq!(redirected.node_id, t.node_id);
    assert_eq!(redirected.alias_of, Some(t.node_id));
    assert_ne!(s.node_id, t.node_id);
}

#[test]
fn entity_merge_conflict() {
    let (_dir, store, _) = fresh_store();
    let target = EntityRef::new("ns", "person", "real");
    block_on(store.resolve_or_create_entity(OperationId::new(), target.clone())).unwrap();
    // source does not exist -> NotFound.
    let missing = EntityRef::new("ns", "person", "ghost");
    assert!(store
        .merge_entities(OperationId::new(), &missing, &target)
        .is_err());
    // different entity_type -> error.
    let other = EntityRef::new("ns", "product", "widget");
    block_on(store.resolve_or_create_entity(OperationId::new(), other.clone())).unwrap();
    let wrong = store.merge_entities(OperationId::new(), &other, &target);
    assert!(wrong.is_err());
}

#[test]
fn entity_merge_idempotent() {
    let (_dir, store, _) = fresh_store();
    let source = EntityRef::new("ns", "person", "a");
    let target = EntityRef::new("ns", "person", "b");
    block_on(store.resolve_or_create_entity(OperationId::new(), source.clone())).unwrap();
    block_on(store.resolve_or_create_entity(OperationId::new(), target.clone())).unwrap();
    store
        .merge_entities(OperationId::new(), &source, &target)
        .unwrap();
    // Repeating the merge has no observable side effects.
    store
        .merge_entities(OperationId::new(), &source, &target)
        .unwrap();
    let redirected = store.get_entity(&source).unwrap().unwrap();
    assert_eq!(
        redirected.node_id,
        store.get_entity(&target).unwrap().unwrap().node_id
    );
}

#[test]
fn entity_writes_refuse_an_operation_id_a_session_event_holds() {
    let (_dir, store, log) = fresh_store();
    let operation = OperationId::new();
    let input = EventInput {
        sources: Vec::new(),
        session: "entity-session".into(),
        event_id: "event-1".into(),
        scope: Scope::Personal,
        kind: EventKind::UserMessage,
        media_type: "text/plain".into(),
        occurred_at_ms: 1,
        expected_sequence: None,
    };
    let receipt = log
        .append(operation, input.clone(), b"hello".as_slice())
        .unwrap();
    let primary = EntityRef::new("ns", "person", "erin");
    let other = EntityRef::new("ns", "person", "frank");
    block_on(store.resolve_or_create_entity(OperationId::new(), primary.clone())).unwrap();
    block_on(store.resolve_or_create_entity(OperationId::new(), other.clone())).unwrap();
    let alias = EntityRef::new("ns", "person", "erin-alias");
    assert!(matches!(
        store.add_entity_alias(operation, &primary, alias.clone()),
        Err(ContextError::IdempotencyConflict)
    ));
    assert!(matches!(
        store.merge_entities(operation, &other, &primary),
        Err(ContextError::IdempotencyConflict)
    ));
    assert_eq!(log.operation_receipt(operation).unwrap(), Some(receipt.clone()));
    assert_eq!(
        log.append(operation, input, b"hello".as_slice()).unwrap(),
        receipt
    );

    // An identity write retried under its own operation ID replays.
    let alias_operation = OperationId::new();
    let aliased = store
        .add_entity_alias(alias_operation, &primary, alias.clone())
        .unwrap();
    assert_eq!(
        store
            .add_entity_alias(alias_operation, &primary, alias)
            .unwrap(),
        aliased
    );
    assert!(store.operation_receipt(alias_operation).unwrap().is_some());
}

// ---------------------------------------------------------------------------
// Feature A3 — extraction transaction
// ---------------------------------------------------------------------------

#[test]
fn revising_a_type_or_extracting_advances_the_availability_epoch() {
    let (_dir, store, log) = fresh_store();
    let anchor = event_source(&log, "session", "anchor");
    let original = object_type(&store, "note");
    let checkpoint = |previous| {
        log.save_checkpoint(
            OperationId::new(),
            CheckpointInput {
                scope: CheckpointScope::Session,
                session: "session".into(),
                run: "run".into(),
                sequence: 1,
                expected_previous: previous,
                expected_availability_epoch: store.availability_epoch().unwrap(),
                sources: vec![anchor.clone()],
                payload: json!("summary"),
            },
        )
        .unwrap()
        .pin
    };
    let assert_stale = |pin: &RecordPin| {
        assert!(matches!(
            store.validate_current(std::slice::from_ref(pin), None),
            Err(ContextError::Unavailable(record)) if record == pin.record
        ));
        assert!(log.read_checkpoint(pin).is_ok());
    };

    let first = checkpoint(None);
    let (_, mut definition) = store.find_type(&Scope::Personal, "note").unwrap().unwrap();
    definition.properties.clear();
    let operation = OperationId::new();
    let revise = |id| store.revise_type(id, original.clone(), definition.clone());
    let revised = revise(operation).unwrap().pin;
    assert_stale(&first);
    assert_eq!(revise(operation).unwrap().pin, revised);
    assert!(matches!(
        revise(OperationId::new()),
        Err(ContextError::RevisionConflict { .. })
    ));
    assert_eq!(store.availability_epoch().unwrap(), 1);

    let second = checkpoint(Some(first));
    let create = ExtractionOp::CreateObject {
        type_ref: revised,
        properties: BTreeMap::new(),
        sources: vec![],
    };
    let created = block_on(store.extraction_transaction(Ulid::new(), vec![create; 2])).unwrap();
    assert_eq!(store.availability_epoch().unwrap(), 1);
    store
        .validate_current(std::slice::from_ref(&second), None)
        .unwrap();
    let revisions: Vec<_> = created
        .created_objects
        .into_iter()
        .map(|expected| ExtractionOp::ReviseObject {
            expected,
            properties: BTreeMap::new(),
            sources: vec![],
        })
        .collect();
    let extract = |id| block_on(store.extraction_transaction(id, revisions.clone()));
    let operation = Ulid::new();
    assert_eq!(extract(operation).unwrap().revised_objects.len(), 2);
    assert_stale(&second);
    assert!(extract(operation).unwrap().replayed);
    assert!(matches!(
        extract(Ulid::new()),
        Err(ContextError::RevisionConflict { .. })
    ));
    assert_eq!(store.availability_epoch().unwrap(), 2);
}

#[test]
fn extraction_batch_creates_objects_and_relations_atomically() {
    let (_dir, store, log) = fresh_store();
    let obj_type = object_type(&store, "thing");
    let a = store
        .save_object(
            OperationId::new(),
            ObjectInput {
                type_pin: obj_type.clone(),
                scope: Scope::Personal,
                properties: BTreeMap::from([("text".into(), json!("a"))]),
                sources: vec![event_source(&log, "s", "e1")],
                information: InformationKind::Observation,
                temporal: TemporalFacts::observed_at(1),
            },
        )
        .unwrap()
        .pin;
    let b = store
        .save_object(
            OperationId::new(),
            ObjectInput {
                type_pin: obj_type.clone(),
                scope: Scope::Personal,
                properties: BTreeMap::from([("text".into(), json!("b"))]),
                sources: vec![event_source(&log, "s", "e2")],
                information: InformationKind::Observation,
                temporal: TemporalFacts::observed_at(1),
            },
        )
        .unwrap()
        .pin;
    let op = Ulid::new();
    let receipt = block_on(store.extraction_transaction(
        op,
        vec![
            ExtractionOp::CreateObject {
                type_ref: obj_type.clone(),
                properties: BTreeMap::new(),
                sources: vec![],
            },
            ExtractionOp::CreateObject {
                type_ref: obj_type.clone(),
                properties: BTreeMap::new(),
                sources: vec![],
            },
            ExtractionOp::CreateRelation {
                relation_type: "rel".into(),
                from: a.record.clone(),
                to: b.record.clone(),
                weight: 0.5,
                evidence: vec![],
                sources: vec![],
            },
        ],
    ))
    .unwrap();
    assert_eq!(receipt.created_objects.len(), 2);
    assert_eq!(receipt.created_relations.len(), 1);
    assert!(!receipt.replayed);
}

#[test]
fn extraction_batch_failure_is_atomic() {
    let (_dir, store, _) = fresh_store();
    let bogus = RecordPin {
        record: ContextRef {
            era: store.scoped.era(StoreModule::Memory),
            owner: "owner".into(),
            kind: RecordKind::Object,
            id: Ulid::new(),
        },
        revision: 1,
    };
    let before = store.query_current_valid(None, None, 100).unwrap().len();
    let result = block_on(store.extraction_transaction(
        Ulid::new(),
        vec![
            ExtractionOp::CreateObject {
                type_ref: bogus.clone(),
                properties: BTreeMap::new(),
                sources: vec![],
            },
            ExtractionOp::CreateObject {
                type_ref: bogus,
                properties: BTreeMap::new(),
                sources: vec![],
            },
        ],
    ));
    assert!(result.is_err());
    let after = store.query_current_valid(None, None, 100).unwrap().len();
    assert_eq!(
        before, after,
        "failed extraction must not commit any records"
    );
}

#[test]
fn extraction_idempotent_replay() {
    let (_dir, store, _) = fresh_store();
    let obj_type = object_type(&store, "thing");
    let op = Ulid::new();
    let ops = vec![
        ExtractionOp::CreateObject {
            type_ref: obj_type.clone(),
            properties: BTreeMap::new(),
            sources: vec![],
        },
        ExtractionOp::CreateObject {
            type_ref: obj_type,
            properties: BTreeMap::new(),
            sources: vec![],
        },
    ];
    let first = block_on(store.extraction_transaction(op, ops.clone())).unwrap();
    let second = block_on(store.extraction_transaction(op, ops)).unwrap();
    assert!(second.replayed);
    assert_eq!(first.created_objects, second.created_objects);
}

#[test]
fn extraction_batch_limit_enforced() {
    let (_dir, store, _) = fresh_store();
    let obj_type = object_type(&store, "thing");
    let ops = vec![
        ExtractionOp::CreateObject {
            type_ref: obj_type,
            properties: BTreeMap::new(),
            sources: vec![]
        };
        MAX_BATCH_SIZE + 1
    ];
    assert!(block_on(store.extraction_transaction(Ulid::new(), ops)).is_err());
}

#[test]
fn extraction_revise_conflict() {
    let (_dir, store, _) = fresh_store();
    let obj_type = object_type(&store, "thing");
    let created = block_on(store.extraction_transaction(
        Ulid::new(),
        vec![ExtractionOp::CreateObject {
            type_ref: obj_type.clone(),
            properties: BTreeMap::new(),
            sources: vec![],
        }],
    ))
    .unwrap();
    let bad_pin = RecordPin {
        record: created.created_objects[0].record.clone(),
        revision: 999,
    };
    let result = block_on(store.extraction_transaction(
        Ulid::new(),
        vec![ExtractionOp::ReviseObject {
            expected: bad_pin,
            properties: BTreeMap::new(),
            sources: vec![],
        }],
    ));
    assert!(result.is_err());
}

#[test]
fn extraction_crash_recovery() {
    let (_dir, store, _) = fresh_store();
    let obj_type = object_type(&store, "thing");
    let op = Ulid::new();
    // Simulate a previous crash: an Incomplete marker exists but no receipt.
    let mut key = crate::context::storage::owner_key("owner");
    crate::context::storage::segment(&mut key, op.0.to_be_bytes().as_slice());
    let mut batch = store.scoped.batch();
    batch.insert(&store.parts.extraction_incomplete, key, b"1");
    batch.commit().unwrap();

    let receipt = block_on(store.extraction_transaction(
        op,
        vec![ExtractionOp::CreateObject {
            type_ref: obj_type,
            properties: BTreeMap::new(),
            sources: vec![],
        }],
    ))
    .unwrap();
    assert!(!receipt.replayed);
    assert_eq!(receipt.created_objects.len(), 1);
    // The marker must have been cleared.
    let mut key = crate::context::storage::owner_key("owner");
    crate::context::storage::segment(&mut key, op.0.to_be_bytes().as_slice());
    assert!(store
        .parts
        .extraction_incomplete
        .get(key)
        .unwrap()
        .is_none());
}

#[test]
fn extraction_source_pins_preserved() {
    let (_dir, store, log) = fresh_store();
    let obj_type = object_type(&store, "thing");
    let source = event_source(&log, "s", "e");
    let receipt = block_on(store.extraction_transaction(
        Ulid::new(),
        vec![ExtractionOp::CreateObject {
            type_ref: obj_type,
            properties: BTreeMap::new(),
            sources: vec![source.clone()],
        }],
    ))
    .unwrap();
    let record = store.read(&receipt.created_objects[0]).unwrap();
    assert_eq!(record.header.sources, vec![source]);
}

#[test]
fn a_memory_purge_commits_its_head_epoch_and_receipt_together() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("store");
    let access = ContextAccess::new("owner", Actor::User);
    let operation = OperationId::new();
    let (object, epoch) = {
        let store = Store::create(&path).unwrap();
        let context = store.context(access.clone()).unwrap();
        let source = event_source(
            &store.session_log(access.clone()).unwrap(),
            "session",
            "input",
        );
        let object = context
            .save_object(
                OperationId::new(),
                ObjectInput {
                    type_pin: object_type(&context, "note"),
                    scope: Scope::Personal,
                    properties: BTreeMap::from([("text".into(), json!("private"))]),
                    sources: vec![source],
                    information: InformationKind::Inference,
                    temporal: TemporalFacts::observed_at(10),
                },
            )
            .unwrap()
            .pin;
        let epoch = context.availability_epoch().unwrap();
        assert!(context.operation_receipt(operation).unwrap().is_none());
        context.scoped.lose_next_commit_ack();
        assert!(
            matches!(context.purge(operation, object.clone()), Err(ContextError::CommitUnknown(id)) if id == operation)
        );
        (object, epoch)
    };

    let store = Store::open(&path).unwrap();
    let log = store.session_log(access.clone()).unwrap();
    let context = store.context(access).unwrap();
    let head = context.head(&object.record).unwrap();
    assert_eq!(head.state, RecordState::Purged);
    assert_eq!(head.pin.revision, object.revision + 1);
    let receipt = context.operation_receipt(operation).unwrap().unwrap();
    assert_eq!(receipt.pin, head.pin);
    assert_eq!(context.availability_epoch().unwrap(), epoch + 1);
    assert_eq!(context.purge(operation, object).unwrap(), receipt);
    assert_eq!(context.availability_epoch().unwrap(), epoch + 1);
    assert_eq!(log.recover_payloads(1, 1).unwrap().uploads_removed, 0);
    assert_eq!(context.recover_payloads(1, 1).unwrap().uploads_removed, 1);
}

#[test]
fn a_streaming_append_blocks_memory_writes_until_it_ends() {
    use std::io::{self, Read};
    use std::sync::mpsc;
    use std::time::Duration;

    struct PausedInput {
        entered: Option<mpsc::Sender<()>>,
        resume: mpsc::Receiver<()>,
    }

    impl Read for PausedInput {
        fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
            let Some(entered) = self.entered.take() else {
                return Ok(0);
            };
            entered.send(()).unwrap();
            self.resume.recv_timeout(Duration::from_secs(5)).unwrap();
            buffer[0] = b'x';
            Ok(1)
        }
    }

    let dir = tempfile::tempdir().unwrap();
    let store = Store::create(dir.path().join("store")).unwrap();
    let (entered_tx, entered_rx) = mpsc::channel();
    let (resume_tx, resume_rx) = mpsc::channel();
    let (started_tx, started_rx) = mpsc::channel();
    let (written_tx, written_rx) = mpsc::channel();
    let access = ContextAccess::new("owner", Actor::User);
    let log = store.session_log(access.clone()).unwrap();
    let context = store.context(access).unwrap();
    std::thread::scope(|threads| {
        let append = threads.spawn(move || {
            log.append(
                OperationId::new(),
                EventInput {
                    sources: vec![],
                    session: "s".into(),
                    event_id: "event".into(),
                    scope: Scope::Personal,
                    kind: EventKind::UserMessage,
                    media_type: "text/plain".into(),
                    occurred_at_ms: 1,
                    expected_sequence: Some(0),
                },
                PausedInput {
                    entered: Some(entered_tx),
                    resume: resume_rx,
                },
            )
            .unwrap()
        });
        entered_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        threads.spawn(move || {
            started_tx.send(()).unwrap();
            written_tx
                .send(
                    context.define_type(OperationId::new(), TypeDefinition::note(Scope::Personal)),
                )
                .unwrap();
        });
        started_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        let while_streaming = written_rx.recv_timeout(Duration::from_millis(100));
        resume_tx.send(()).unwrap();
        assert!(matches!(
            while_streaming,
            Err(mpsc::RecvTimeoutError::Timeout)
        ));
        assert_eq!(append.join().unwrap().sequence, Some(1));
        assert!(written_rx
            .recv_timeout(Duration::from_secs(5))
            .unwrap()
            .is_ok());
    });
}
