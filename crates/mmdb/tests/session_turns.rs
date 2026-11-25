use mmdb::session::{EventInput, SessionLog};
use mmdb::{
    Actor, ContextAccess, ContextError, EventKind, MessageStatus, OperationId, Scope, Store,
    ToolOutcome, TurnEndReason, TurnError, TurnStamp, WriteReceipt,
};

const SESSION: &str = "chat";

fn fixture() -> (tempfile::TempDir, Store) {
    let directory = tempfile::tempdir().unwrap();
    let store = Store::create(directory.path().join("store")).unwrap();
    (directory, store)
}

fn writer<'a>(store: &'a Store, actor: Actor, run: Option<&str>) -> SessionLog<'a> {
    store
        .session_log(ContextAccess {
            run: run.map(str::to_owned),
            ..ContextAccess::new("owner", actor)
        })
        .unwrap()
}

fn system(store: &Store) -> SessionLog<'_> {
    writer(store, Actor::System, Some("run"))
}

fn input(id: &str, kind: EventKind) -> EventInput {
    EventInput {
        session: SESSION.into(),
        event_id: id.into(),
        kind,
        sources: vec![],
        scope: Scope::Personal,
        media_type: "text/plain".into(),
        occurred_at_ms: 1,
        expected_sequence: None,
    }
}

fn write(log: &SessionLog<'_>, id: &str, kind: EventKind) -> WriteReceipt {
    log.append(OperationId::new(), input(id, kind), &[][..])
        .unwrap()
}

fn rejected(log: &SessionLog<'_>, event: EventInput) -> ContextError {
    let before = log.position(&event.session).unwrap();
    let session = event.session.clone();
    let op = OperationId::new();
    let error = log.append(op, event, &[][..]).unwrap_err();
    assert_eq!(log.position(&session).unwrap(), before);
    assert!(log.operation_receipt(op).unwrap().is_none());
    error
}

fn refused(log: &SessionLog<'_>, id: &str, kind: EventKind, expected: TurnError) {
    let error = rejected(log, input(id, kind));
    assert!(
        matches!(error, ContextError::Turn(actual) if actual == expected),
        "expected {expected:?}, got {error:?}"
    );
}

fn end(id: &str, reason: TurnEndReason) -> EventKind {
    EventKind::TurnEnded {
        reason,
        start_event_id: id.into(),
    }
}

fn completed(id: &str) -> EventKind {
    end(id, TurnEndReason::Completed)
}

fn started(message: &str) -> EventKind {
    EventKind::AssistantStarted {
        message_id: message.into(),
    }
}

fn fragment(message: &str) -> EventKind {
    EventKind::AssistantFragment {
        message_id: message.into(),
        ordinal: 0,
    }
}

fn terminal(message: &str) -> EventKind {
    EventKind::AssistantTerminal {
        message_id: message.into(),
        status: MessageStatus::Completed,
    }
}

fn call(id: &str, message: Option<&str>) -> EventKind {
    EventKind::ToolCall {
        call_id: id.into(),
        tool_name: "read".into(),
        message_id: message.map(str::to_owned),
    }
}

fn result(id: &str) -> EventKind {
    EventKind::ToolResult {
        call_id: id.into(),
        outcome: ToolOutcome::Succeeded,
    }
}

fn open_step(log: &SessionLog<'_>) {
    write(log, "turn", EventKind::TurnStarted);
    write(log, "step", EventKind::StepStarted);
}

fn stamp(number: u64, step: Option<u64>) -> Option<TurnStamp> {
    Some(TurnStamp { number, step })
}

#[test]
fn turn_and_step_numbers_increase_across_the_session() {
    let (dir, db) = fixture();
    let log = system(&db);
    let user = writer(&db, Actor::User, None);
    let input_run = writer(&db, Actor::System, Some("input-run"));
    for (handle, id, kind) in [
        (&user, "user", EventKind::UserMessage),
        (&log, "t1", EventKind::TurnStarted),
        (&input_run, "notice", EventKind::Notice),
        (&log, "s1", EventKind::StepStarted),
        (&user, "interrupt", EventKind::UserMessage),
        (&log, "s1-end", EventKind::StepEnded),
        (&log, "s2", EventKind::StepStarted),
        (&log, "s2-end", EventKind::StepEnded),
        (&log, "t1-end", completed("t1")),
        (&log, "between", EventKind::Notice),
    ] {
        write(handle, id, kind);
    }
    // Continue the counters across a reopen with no unfinished work.
    drop(db);
    let db = Store::open(dir.path().join("store")).unwrap();
    let log = system(&db);
    write(&log, "t2", EventKind::TurnStarted);
    write(&log, "s3", EventKind::StepStarted);
    let expected = [
        None,
        stamp(1, None),
        stamp(1, None),
        stamp(1, Some(1)),
        stamp(1, Some(1)),
        stamp(1, Some(1)),
        stamp(1, Some(2)),
        stamp(1, Some(2)),
        stamp(1, None),
        None,
        stamp(2, None),
        stamp(2, Some(3)),
    ];
    let events = log.events(SESSION, 0, 100).unwrap().entries;
    assert_eq!(events.len(), expected.len());
    for (index, (header, expected)) in events.into_iter().zip(expected).enumerate() {
        let event = header.event.unwrap();
        assert_eq!(event.sequence, index as u64 + 1);
        assert_eq!(event.turn, expected, "{}", event.event_id);
    }
}

#[test]
fn only_system_or_operator_write_turn_and_step_events() {
    let (_dir, db) = fixture();
    let sys = system(&db);
    let operator = writer(&db, Actor::Operator, Some("run"));
    for actor in [Actor::Assistant, Actor::User] {
        let denied = writer(&db, actor, Some("run"));
        for kind in [
            EventKind::TurnStarted,
            EventKind::StepStarted,
            EventKind::StepEnded,
            completed("turn"),
        ] {
            assert!(matches!(
                rejected(&denied, input("denied", kind)),
                ContextError::AccessDenied
            ));
        }
    }
    for (index, handle) in [&sys, &operator].into_iter().enumerate() {
        let t = format!("t{index}");
        write(handle, &t, EventKind::TurnStarted);
        write(handle, &format!("s{index}"), EventKind::StepStarted);
        write(handle, &format!("s{index}-end"), EventKind::StepEnded);
        write(handle, &format!("t{index}-end"), completed(&t));
    }
}

#[test]
fn a_turn_needs_a_run_and_a_session_holds_one_open_turn() {
    let (_dir, db) = fixture();
    let log = system(&db);
    let no_run = writer(&db, Actor::System, None);
    refused(
        &no_run,
        "turn",
        EventKind::TurnStarted,
        TurnError::RunRequired,
    );
    open_step(&log);
    refused(
        &no_run,
        "second",
        EventKind::TurnStarted,
        TurnError::RunRequired,
    );
    refused(&log, "second", EventKind::TurnStarted, TurnError::TurnOpen);
    refused(
        &log,
        "another-step",
        EventKind::StepStarted,
        TurnError::StepOpen,
    );
    let event = EventInput {
        session: "another-session".into(),
        ..input("turn", EventKind::TurnStarted)
    };
    let receipt = log.append(OperationId::new(), event, &[][..]).unwrap();
    assert_eq!(
        log.inspect_header(&receipt.pin)
            .unwrap()
            .event
            .unwrap()
            .turn,
        stamp(1, None)
    );
}

#[test]
fn a_harness_cannot_write_interrupted_or_forked() {
    let (_dir, db) = fixture();
    let log = system(&db);
    for reason in [TurnEndReason::Interrupted, TurnEndReason::Forked] {
        refused(&log, "end", end("wrong", reason), TurnError::ReservedReason);
    }
    open_step(&log);
    let other = writer(&db, Actor::Operator, Some("other"));
    for reason in [TurnEndReason::Interrupted, TurnEndReason::Forked] {
        refused(
            &other,
            "end",
            end("wrong", reason),
            TurnError::ReservedReason,
        );
    }
}

#[test]
fn event_ids_with_the_mmdb_prefix_are_reserved() {
    let (_dir, db) = fixture();
    let log = system(&db);
    for kind in [
        EventKind::Notice,
        EventKind::TurnStarted,
        EventKind::AssistantMessage,
    ] {
        assert!(matches!(
            rejected(&log, input("mmdb:turn-1:end", kind)),
            ContextError::InvalidInput(_)
        ));
    }
    write(&log, "turn", EventKind::TurnStarted);
    assert!(matches!(
        rejected(&log, input("end", completed(" "))),
        ContextError::InvalidInput(_)
    ));
}

#[test]
fn retrying_a_turn_event_after_the_turn_moved_on_returns_its_receipt() {
    let (_dir, db) = fixture();
    let log = system(&db);
    let op = OperationId::new();
    let event = EventInput {
        expected_sequence: Some(0),
        ..input("turn", EventKind::TurnStarted)
    };
    let original = log.append(op, event.clone(), &[][..]).unwrap();
    let ended = write(&log, "end", completed("turn"));
    write(&log, "next", EventKind::TurnStarted);
    let before = log.position(SESSION).unwrap();
    assert_eq!(log.append(op, event.clone(), &[][..]).unwrap(), original);
    assert_eq!(
        log.append(OperationId::new(), event, &[][..]).unwrap(),
        original
    );
    assert_eq!(write(&log, "end", completed("turn")), ended);
    assert!(matches!(
        rejected(&log, input("turn", EventKind::StepStarted)),
        ContextError::IdempotencyConflict
    ));
    assert_eq!(log.position(SESSION).unwrap(), before);

    write(&log, "step", EventKind::StepStarted);
    write(&log, "call", call("c", None));
    let operation = OperationId::new();
    let cancel = EventInput {
        expected_sequence: Some(log.position(SESSION).unwrap()),
        ..input("cancel", end("next", TurnEndReason::Cancelled))
    };
    let receipt = log.append(operation, cancel.clone(), &[][..]).unwrap();
    assert_eq!(
        receipt.sequence,
        Some(cancel.expected_sequence.unwrap() + 3)
    );
    write(&log, "later", EventKind::TurnStarted);
    let before_retry = log.position(SESSION).unwrap();
    for retry in [operation, OperationId::new()] {
        assert_eq!(log.append(retry, cancel.clone(), &[][..]).unwrap(), receipt);
    }
    assert_eq!(log.position(SESSION).unwrap(), before_retry);
}

#[test]
fn withdrawn_turn_events_keep_numbers_and_turn_state() {
    let (_dir, db) = fixture();
    let log = writer(&db, Actor::Operator, Some("run"));
    let t = write(&log, "turn", EventKind::TurnStarted);
    let s = write(&log, "step", EventKind::StepStarted);
    log.retract(OperationId::new(), t.pin).unwrap();
    log.purge(OperationId::new(), s.pin).unwrap();
    let m = write(&log, "message", EventKind::AssistantMessage);
    let se = write(&log, "step-end", EventKind::StepEnded);
    log.retract(OperationId::new(), se.pin).unwrap();
    refused(
        &log,
        "late",
        EventKind::AssistantMessage,
        TurnError::OutsideStep,
    );
    let te = write(&log, "turn-end", completed("turn"));
    log.purge(OperationId::new(), te.pin).unwrap();
    refused(&log, "late", EventKind::StepStarted, TurnError::OutsideTurn);
    let next = write(&log, "next", EventKind::TurnStarted);
    assert_eq!(
        log.inspect_header(&m.pin).unwrap().event.unwrap().turn,
        stamp(1, Some(1))
    );
    assert_eq!(
        log.inspect_header(&next.pin).unwrap().event.unwrap().turn,
        stamp(2, None)
    );
    let stamps: Vec<_> = log
        .events(SESSION, 0, 10)
        .unwrap()
        .entries
        .into_iter()
        .map(|header| header.event.unwrap().turn)
        .collect();
    assert_eq!(
        stamps,
        [
            stamp(1, None),
            stamp(1, Some(1)),
            stamp(1, Some(1)),
            stamp(1, Some(1)),
            stamp(1, None),
            stamp(2, None)
        ]
    );
}

#[test]
fn assistant_events_and_linked_calls_need_an_open_step() {
    let (_dir, db) = fixture();
    let log = system(&db);
    write(&log, "turn", EventKind::TurnStarted);
    for kind in [
        EventKind::AssistantMessage,
        started("m"),
        fragment("m"),
        terminal("m"),
        EventKind::StepEnded,
    ] {
        refused(&log, "outside", kind, TurnError::OutsideStep);
    }
    write(&log, "step", EventKind::StepStarted);
    write(&log, "m-start", started("m"));
    write(&log, "m-end", terminal("m"));
    write(&log, "step-end", EventKind::StepEnded);
    refused(
        &log,
        "outside",
        call("c", Some("m")),
        TurnError::OutsideStep,
    );
}

#[test]
fn a_linked_call_stays_in_its_message_step() {
    let (_dir, db) = fixture();
    let log = system(&db);
    open_step(&log);
    write(&log, "m-start", started("m"));
    write(&log, "m-end", terminal("m"));
    assert_eq!(log.message_state(SESSION, "m").unwrap().unwrap().step, 1);
    write(&log, "c", call("c", Some("m")));
    write(&log, "r", result("c"));
    write(&log, "s-end", EventKind::StepEnded);
    write(&log, "s2", EventKind::StepStarted);
    refused(
        &log,
        "later",
        call("later", Some("m")),
        TurnError::OutsideStep,
    );
    write(&log, "s2-end", EventKind::StepEnded);
    write(&log, "t-end", completed("turn"));
    write(&log, "t2", EventKind::TurnStarted);
    write(&log, "s3", EventKind::StepStarted);
    refused(
        &log,
        "later",
        call("later", Some("m")),
        TurnError::OutsideStep,
    );
}

#[test]
fn direct_calls_inside_a_turn_need_no_step() {
    let (_dir, db) = fixture();
    let log = system(&db);
    refused(&log, "direct", call("direct", None), TurnError::OutsideTurn);
    write(&log, "turn", EventKind::TurnStarted);
    let c = write(&log, "direct", call("direct", None));
    write(&log, "step", EventKind::StepStarted);
    write(&log, "step-end", EventKind::StepEnded);
    let r = write(&log, "result", result("direct"));
    write(&log, "end", completed("turn"));
    for receipt in [c, r] {
        assert_eq!(
            log.inspect_header(&receipt.pin)
                .unwrap()
                .event
                .unwrap()
                .turn,
            stamp(1, None)
        );
    }
}

#[test]
fn only_the_opening_run_writes_inside_a_turn() {
    let (_dir, db) = fixture();
    let log = system(&db);
    let other = writer(&db, Actor::Operator, Some("other"));
    open_step(&log);
    write(&log, "m", started("m"));
    write(&log, "c", call("c", Some("m")));
    for kind in [
        EventKind::StepStarted,
        EventKind::StepEnded,
        EventKind::AssistantMessage,
        started("other-message"),
        fragment("m"),
        terminal("m"),
        call("other-call", None),
        call("linked", Some("m")),
        result("c"),
        completed("turn"),
    ] {
        refused(&other, "other-event", kind, TurnError::OtherRun);
    }
    write(&log, "r", result("c"));
    write(&log, "m-end", terminal("m"));
    write(&log, "s-end", EventKind::StepEnded);
    write(&log, "end", completed("turn"));
}

#[test]
fn a_turn_end_names_the_turn_it_ends() {
    let (_dir, db) = fixture();
    let log = system(&db);
    let other = writer(&db, Actor::Operator, Some("other"));
    write(&log, "first", EventKind::TurnStarted);
    refused(&log, "end", completed("wrong"), TurnError::OtherTurn);
    // This end request has no receipt. A later delivery must name the same turn.
    write(&log, "step", EventKind::StepStarted);
    let op = OperationId::new();
    let stale = input("stale-end", completed("first"));
    let attempt = |expected: TurnError| {
        let position = log.position(SESSION).unwrap();
        assert!(
            matches!(log.append(op, stale.clone(), &[][..]), Err(ContextError::Turn(e)) if e == expected)
        );
        assert_eq!(log.position(SESSION).unwrap(), position);
        assert!(log.operation_receipt(op).unwrap().is_none());
    };
    attempt(TurnError::TurnUnfinished);
    write(&log, "se", EventKind::StepEnded);
    write(&log, "end", completed("first"));
    write(&other, "foreign", EventKind::TurnStarted);
    attempt(TurnError::OtherRun);
    write(&other, "foreign-end", completed("foreign"));
    write(&log, "next", EventKind::TurnStarted);
    attempt(TurnError::OtherTurn);
}

#[test]
fn work_started_by_the_turn_run_writes_under_that_run() {
    let (_dir, db) = fixture();
    let log = system(&db);
    open_step(&log);
    let delegated = db
        .session_log(ContextAccess {
            run: Some("run".into()),
            agent: Some("worker".into()),
            ..ContextAccess::new("owner", Actor::Assistant)
        })
        .unwrap();
    let child = db
        .session_log(ContextAccess {
            run: Some("child".into()),
            parent_run: Some("run".into()),
            ..ContextAccess::new("owner", Actor::Assistant)
        })
        .unwrap();
    refused(&child, "c", call("c", None), TurnError::OtherRun);
    let receipt = write(&delegated, "c", call("c", None));
    let header = log.inspect_header(&receipt.pin).unwrap();
    assert_eq!(header.provenance.run.as_deref(), Some("run"));
    assert_eq!(header.provenance.agent.as_deref(), Some("worker"));
    assert_eq!(header.event.unwrap().turn, stamp(1, Some(1)));
    write(&delegated, "r", result("c"));
    write(&log, "se", EventKind::StepEnded);
    write(&log, "end", completed("turn"));
}

#[test]
fn a_foreign_caller_waits_for_the_open_turn() {
    let (_dir, db) = fixture();
    let log = system(&db);
    let other = writer(&db, Actor::Operator, Some("api"));
    write(&log, "first", EventKind::TurnStarted);
    refused(
        &other,
        "api-call",
        call("api-call", None),
        TurnError::OtherRun,
    );
    refused(
        &other,
        "api-turn",
        EventKind::TurnStarted,
        TurnError::TurnOpen,
    );
    write(&log, "first-end", completed("first"));
    write(&other, "api-turn", EventKind::TurnStarted);
    write(&other, "api-call", call("api-call", None));
    write(&other, "api-result", result("api-call"));
    write(&other, "api-end", completed("api-turn"));
}

#[test]
fn a_step_ends_only_when_it_owes_nothing() {
    let (_dir, db) = fixture();
    let log = system(&db);
    open_step(&log);
    write(&log, "m-start", started("m"));
    refused(&log, "end", EventKind::StepEnded, TurnError::StepUnfinished);
    write(&log, "m-fragment", fragment("m"));
    write(&log, "c", call("c", Some("m")));
    write(&log, "direct", call("direct", None));
    write(&log, "m-end", terminal("m"));
    refused(&log, "end", EventKind::StepEnded, TurnError::StepUnfinished);
    write(&log, "r", result("c"));
    refused(&log, "end", EventKind::StepEnded, TurnError::StepUnfinished);
    write(&log, "r-direct", result("direct"));
    write(&log, "end", EventKind::StepEnded);
    write(&log, "t-end", completed("turn"));
}

#[test]
fn completed_needs_no_open_step_and_no_unanswered_call() {
    let (_dir, db) = fixture();
    let log = system(&db);
    open_step(&log);
    refused(&log, "end", completed("turn"), TurnError::TurnUnfinished);
    write(&log, "s-end", EventKind::StepEnded);
    write(&log, "c", call("c", None));
    refused(&log, "end", completed("turn"), TurnError::TurnUnfinished);
    write(&log, "r", result("c"));
    write(&log, "end", completed("turn"));
}

fn check_unfinished_turn(reason: TurnEndReason) {
    let (_dir, db) = fixture();
    let log = writer(&db, Actor::Operator, Some("run"));
    open_step(&log);
    for id in ["z", "alpha"] {
        write(&log, &format!("start-{id}"), started(id));
        write(&log, &format!("call-{id}"), call(id, Some(id)));
    }
    let before = log.position(SESSION).unwrap();
    let operation = OperationId::new();
    let finish = EventInput {
        occurred_at_ms: 987,
        expected_sequence: Some(before),
        ..input("end", end("turn", reason))
    };
    let receipt = log.append(operation, finish, &b"details"[..]).unwrap();
    let events = log.events(SESSION, before, 10).unwrap().entries;
    assert_eq!(events.len(), 6);
    for (i, header) in events[..5].iter().enumerate() {
        let event = header.event.as_ref().unwrap();
        assert_eq!(event.sequence, before + i as u64 + 1);
        assert_eq!(event.turn, stamp(1, Some(1)));
        assert_eq!(event.media_type, "application/octet-stream");
        assert_eq!(header.scope, Scope::Personal);
        assert_eq!(header.temporal.observed_at_ms, 987);
        assert_eq!(header.provenance.actor, Actor::System);
        assert_eq!(header.provenance.run.as_deref(), Some("run"));
        assert_eq!(header.provenance.session.as_deref(), Some(SESSION));
        assert!(log
            .read_payload(&header.pin, 0, 32)
            .unwrap()
            .bytes
            .is_empty());
    }
    for (i, id) in ["alpha", "z"].into_iter().enumerate() {
        let hash = blake3::hash(id.as_bytes());
        let terminal = events[i].event.as_ref().unwrap();
        assert_eq!(
            terminal.kind,
            EventKind::AssistantTerminal {
                message_id: id.into(),
                status: MessageStatus::Interrupted,
            }
        );
        assert_eq!(terminal.event_id, format!("mmdb:turn-1:terminal-{hash}"));
        let message = log.message_state(SESSION, id).unwrap().unwrap();
        assert_eq!(message.terminal, Some(MessageStatus::Interrupted));
        let result = &events[i + 2];
        let event = result.event.as_ref().unwrap();
        assert_eq!(
            event.kind,
            EventKind::ToolResult {
                call_id: id.into(),
                outcome: ToolOutcome::EffectUnknown,
            }
        );
        assert_eq!(event.event_id, format!("mmdb:turn-1:result-{hash}"));
        let (call, paired) = log.tool_pair(SESSION, id).unwrap();
        assert_eq!(result.sources, [call.unwrap().pin]);
        assert_eq!(paired.as_ref(), Some(result));
    }
    let step = events[4].event.as_ref().unwrap();
    assert_eq!(step.kind, EventKind::StepEnded);
    assert_eq!(step.event_id, "mmdb:turn-1:step-end");
    let last = &events[5];
    let event = last.event.as_ref().unwrap();
    assert_eq!(event.kind, end("turn", reason));
    assert_eq!(event.event_id, "end");
    assert_eq!(event.turn, stamp(1, None));
    assert_eq!(last.provenance.actor, Actor::Operator);
    assert_eq!(receipt.sequence, Some(before + 6));
    assert_eq!(receipt.pin, last.pin);
    assert_eq!(
        log.read_payload(&receipt.pin, 0, 32).unwrap().bytes,
        b"details"
    );
    assert_eq!(log.operation_receipt(operation).unwrap(), Some(receipt));
    write(&log, "later", EventKind::TurnStarted);
    write(&log, "later-end", completed("later"));
}

#[test]
fn cancelling_a_turn_closes_what_it_owes_in_one_batch() {
    check_unfinished_turn(TurnEndReason::Cancelled);
}

#[test]
fn failed_turns_close_like_cancelled_ones() {
    check_unfinished_turn(TurnEndReason::Failed);
}

#[test]
fn results_written_before_the_end_are_kept() {
    let (_dir, db) = fixture();
    let log = system(&db);
    open_step(&log);
    write(&log, "call", call("c", None));
    let rejected = EventKind::ToolResult {
        call_id: "c".into(),
        outcome: ToolOutcome::RejectedBeforeDispatch,
    };
    let known = log
        .append(
            OperationId::new(),
            input("known", rejected.clone()),
            &b"offline"[..],
        )
        .unwrap();
    let before = log.position(SESSION).unwrap();
    write(&log, "end", end("turn", TurnEndReason::Cancelled));
    assert_eq!(log.position(SESSION).unwrap(), before + 2);
    let result = log.tool_pair(SESSION, "c").unwrap().1.unwrap();
    assert_eq!(result.pin, known.pin);
    assert_eq!(result.event.unwrap().kind, rejected);
    assert_eq!(
        log.read_payload(&known.pin, 0, 32).unwrap().bytes,
        b"offline"
    );
}

#[test]
fn a_late_result_after_cancel_is_refused() {
    let (_dir, db) = fixture();
    let log = writer(&db, Actor::Operator, Some("run"));
    let other = writer(&db, Actor::System, Some("other"));
    write(&log, "turn", EventKind::TurnStarted);
    let request = write(&log, "call", call("c", None));
    write(&log, "cancel", end("turn", TurnEndReason::Cancelled));
    refused(&log, "late", result("c"), TurnError::Answered);
    write(&other, "other", EventKind::TurnStarted);
    refused(&log, "late", result("c"), TurnError::Answered);
    write(&other, "other-end", completed("other"));
    write(&log, "later", EventKind::TurnStarted);
    refused(&log, "late", result("c"), TurnError::Answered);
    log.purge(OperationId::new(), request.pin).unwrap();
    refused(&log, "late", result("c"), TurnError::Answered);
}

#[test]
fn a_closing_result_fits_the_reserved_budget() {
    let (_dir, db) = fixture();
    let workspace = Scope::Workspace(ulid::Ulid::new());
    let access = ContextAccess {
        run: Some("run".into()),
        scopes: vec![Scope::Personal, workspace.clone()],
        ..ContextAccess::new("owner", Actor::System)
    };
    let log = db.session_log(access.clone()).unwrap();
    let mut source = write(&log, "depth-0", EventKind::Notice).pin;
    for depth in 1..=30 {
        let event = EventInput {
            sources: vec![source],
            ..input(&format!("depth-{depth}"), EventKind::Notice)
        };
        source = log.append(OperationId::new(), event, &[][..]).unwrap().pin;
    }
    let event = |id, kind| EventInput {
        session: "workspace-chat".into(),
        scope: workspace.clone(),
        ..input(id, kind)
    };
    log.append(
        OperationId::new(),
        event("turn", EventKind::TurnStarted),
        &[][..],
    )
    .unwrap();
    let mut request = event("call", call("c", None));
    request.sources.push(source);
    let call = log.append(OperationId::new(), request, &[][..]).unwrap();
    // The end writer can read the call's header, but cannot walk its Personal source.
    let closer = db
        .session_log(ContextAccess {
            scopes: vec![workspace.clone()],
            ..access.clone()
        })
        .unwrap();
    closer
        .append(
            OperationId::new(),
            event("cancel", end("turn", TurnEndReason::Cancelled)),
            &[][..],
        )
        .unwrap();
    let result = log.tool_pair("workspace-chat", "c").unwrap().1.unwrap();
    assert_eq!(result.scope, workspace);
    assert_eq!(result.source_scopes, [Scope::Personal]);
    assert_eq!(result.sources, [call.pin]);
    assert_eq!(result.event.unwrap().turn, stamp(1, None));
    db.context(access)
        .unwrap()
        .validate_current(&[result.pin], None)
        .unwrap();
}

#[test]
fn an_owed_call_gets_its_result_after_it_becomes_unreadable() {
    let (_dir, db) = fixture();
    let workspace = Scope::Workspace(ulid::Ulid::new());
    let profile = ContextAccess {
        run: Some("run".into()),
        scopes: vec![Scope::Personal, workspace.clone()],
        ..ContextAccess::new("owner", Actor::Operator)
    };
    let log = db.session_log(profile).unwrap();
    let personal = EventInput {
        session: "private".into(),
        ..input("source", EventKind::Notice)
    };
    let personal = log
        .append(OperationId::new(), personal, &b"original"[..])
        .unwrap()
        .pin;
    for mode in ["purge", "retract", "fragment"] {
        let event = |id: &str, kind| EventInput {
            session: mode.into(),
            scope: workspace.clone(),
            ..input(id, kind)
        };
        let append = |event| {
            log.append(OperationId::new(), event, &b"payload"[..])
                .unwrap()
        };
        append(event("turn", EventKind::TurnStarted));
        append(event("step", EventKind::StepStarted));
        append(event("m", started("m")));
        let fragment = append(event("fragment", fragment("m")));
        let mut request = event("c", call("c", Some("m")));
        request.sources.push(personal.clone());
        let call = append(request);
        let scopes = log.inspect_header(&call.pin).unwrap().source_scopes;
        assert_eq!(scopes, [Scope::Personal]);
        match mode {
            "purge" => log.purge(OperationId::new(), call.pin.clone()).unwrap(),
            "retract" => log.retract(OperationId::new(), call.pin.clone()).unwrap(),
            _ => log.retract(OperationId::new(), fragment.pin).unwrap(),
        };
        assert!(matches!(
            log.read_payload(&call.pin, 0, 100),
            Err(ContextError::Unavailable(_))
        ));
        let mut explicit = event("r", result("c"));
        explicit.sources.push(call.pin.clone());
        assert!(matches!(
            rejected(&log, explicit),
            ContextError::Unavailable(_)
        ));
        let r = append(event("r", result("c")));
        let header = log.inspect_header(&r.pin).unwrap();
        assert_eq!(header.sources.len(), 1);
        assert_eq!(header.sources[0].record, call.pin.record);
        assert_eq!(header.source_scopes, scopes);
        assert!(matches!(
            log.read_payload(&r.pin, 0, 100),
            Err(ContextError::Unavailable(_))
        ));
        append(event("m-end", terminal("m")));
        append(event("se", EventKind::StepEnded));
        append(event("end", completed("turn")));
    }
}

#[test]
fn a_second_result_for_a_call_is_answered() {
    let (_dir, db) = fixture();
    let log = system(&db);
    write(&log, "turn", EventKind::TurnStarted);
    write(&log, "c", call("c", None));
    write(&log, "r", result("c"));
    refused(&log, "again", result("c"), TurnError::Answered);
    write(&log, "end", completed("turn"));
    refused(&log, "again", result("c"), TurnError::Answered);
    let other = writer(&db, Actor::Operator, Some("other"));
    write(&other, "next", EventKind::TurnStarted);
    refused(&log, "again", result("c"), TurnError::Answered);
    refused(&other, "again", result("c"), TurnError::Answered);
}

#[test]
fn turn_errors_follow_their_precedence() {
    let (_dir, db) = fixture();
    let log = system(&db);
    let other = writer(&db, Actor::Operator, Some("other"));
    for kind in [EventKind::AssistantMessage, started("m")] {
        refused(&log, "empty", kind, TurnError::OutsideTurn);
    }
    let missing = || input("call", call("c", Some("missing")));
    assert!(matches!(
        rejected(&log, missing()),
        ContextError::InvalidInput(_)
    ));
    write(&log, "turn", EventKind::TurnStarted);
    refused(
        &other,
        "other",
        EventKind::AssistantMessage,
        TurnError::OtherRun,
    );
    write(&log, "step", EventKind::StepStarted);
    assert!(matches!(
        rejected(&log, missing()),
        ContextError::InvalidInput(_)
    ));
    write(&log, "start", started("m"));
    write(&log, "terminal", terminal("m"));
    write(&log, "step-end", EventKind::StepEnded);
    write(&log, "turn-end", completed("turn"));
    refused(&log, "late", call("c", Some("m")), TurnError::OutsideTurn);
}
