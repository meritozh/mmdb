use super::super::{Actor, RecordState, Scope};
use super::*;
use std::sync::atomic::Ordering;

#[test]
fn current_sources_guard_keeps_retraction_after_the_protected_commit() {
    use std::sync::{mpsc, Arc};
    use std::time::Duration;

    let dir = tempfile::tempdir().unwrap();
    let db = Arc::new(MemoryDatabase::create_context(dir.path().join("context")).unwrap());
    let access = ContextAccess::new("owner", Actor::User);
    let context = db.context(access.clone()).unwrap();
    let pin = context
        .append_history(
            OperationId::new(),
            HistoryInput {
                sources: vec![],
                session: "session".into(),
                event_id: "request".into(),
                scope: Scope::Personal,
                kind: HistoryKind::UserMessage,
                media_type: "text/plain".into(),
                occurred_at_ms: 1,
                expected_sequence: None,
            },
            b"source".as_slice(),
        )
        .unwrap()
        .pin;
    let guard = context
        .validate_current_guard(std::slice::from_ref(&pin), None, Some(0))
        .unwrap();
    assert_eq!(guard.availability_epoch(), 0);

    let (started_tx, started_rx) = mpsc::channel();
    let (done_tx, done_rx) = mpsc::channel();
    let other_db = db.clone();
    let other_pin = pin.clone();
    let thread = std::thread::spawn(move || {
        let context = other_db.context(access).unwrap();
        started_tx.send(()).unwrap();
        let result = context.retract(OperationId::new(), other_pin);
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
    let db = MemoryDatabase::create_context(dir.path().join("context")).unwrap();
    let context = db
        .context(ContextAccess::new("owner", Actor::User))
        .unwrap();
    let pin = context
        .append_history(
            OperationId::new(),
            HistoryInput {
                sources: vec![],
                session: "session".into(),
                event_id: "request".into(),
                scope: Scope::Personal,
                kind: HistoryKind::UserMessage,
                media_type: "text/plain".into(),
                occurred_at_ms: 1,
                expected_sequence: None,
            },
            b"source".as_slice(),
        )
        .unwrap()
        .pin;
    let other = db
        .context(ContextAccess::new("another-owner", Actor::System))
        .unwrap();
    assert!(matches!(
        other.validate_current_guard(std::slice::from_ref(&pin), None, None),
        Err(ContextError::AccessDenied)
    ));
    context.retract(OperationId::new(), pin.clone()).unwrap();
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
    let input = HistoryInput {
        sources: Vec::new(),
        session: "continuous-session".into(),
        event_id: "message-1".into(),
        scope: Scope::Personal,
        kind: HistoryKind::AssistantMessage,
        media_type: "text/plain".into(),
        occurred_at_ms: 1,
        expected_sequence: Some(0),
    };
    let expected = {
        let db = MemoryDatabase::create_context(&root).unwrap();
        let store = db
            .context(ContextAccess::new("owner", Actor::Assistant))
            .unwrap();
        store.parts.fail_commit_ack.store(true, Ordering::SeqCst);
        let error = store
            .append_history(operation, input.clone(), b"durable answer".as_slice())
            .unwrap_err();
        assert!(matches!(error, ContextError::CommitUnknown(id) if id == operation));
        store.operation_receipt(operation).unwrap().unwrap()
    };
    let db = MemoryDatabase::open_context(&root).unwrap();
    let store = db
        .context(ContextAccess::new("owner", Actor::Assistant))
        .unwrap();
    assert_eq!(
        store.operation_receipt(operation).unwrap(),
        Some(expected.clone())
    );
    assert_eq!(
        store
            .append_history(operation, input, b"durable answer".as_slice())
            .unwrap(),
        expected
    );
    assert_eq!(store.history_position("continuous-session").unwrap(), 1);
    assert_eq!(
        store
            .history("continuous-session", 0, 10)
            .unwrap()
            .entries
            .len(),
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
fn checksum_failure_is_reported_instead_of_returning_modified_source_bytes() {
    let dir = tempfile::tempdir().unwrap();
    let db = MemoryDatabase::create_context(dir.path().join("context")).unwrap();
    let store = db
        .context(ContextAccess::new("owner", Actor::System))
        .unwrap();
    let receipt = store
        .append_history(
            OperationId::new(),
            HistoryInput {
                sources: Vec::new(),
                session: "session".into(),
                event_id: "message".into(),
                scope: Scope::Personal,
                kind: HistoryKind::UserMessage,
                media_type: "text/plain".into(),
                occurred_at_ms: 1,
                expected_sequence: None,
            },
            b"original".as_slice(),
        )
        .unwrap();
    let key = storage::payload_key(&receipt.pin, 0);
    let mut bytes = store.parts.payloads.get(&key).unwrap().unwrap().to_vec();
    bytes[32] ^= 1;
    store.parts.payloads.insert(key, bytes).unwrap();
    assert!(matches!(
        store.read_payload(&receipt.pin, 0, 32),
        Err(ContextError::Corrupt(_))
    ));
}
