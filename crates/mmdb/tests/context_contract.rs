use mmdb::context::*;
use mmdb::session::*;
use mmdb::{Actor, OperationId, RecordState, Scope, TemporalFacts};
use mmdb::{Store, StoreError, STORE_FORMAT_ID};
use serde_json::json;
use std::collections::{BTreeMap, BTreeSet};
use std::io::{self, Read};
use std::sync::Arc;

fn access(owner: &str) -> ContextAccess {
    ContextAccess::new(owner, Actor::System)
}

fn event_input(session: &str, event: &str) -> EventInput {
    EventInput {
        sources: Vec::new(),
        session: session.into(),
        event_id: event.into(),
        scope: Scope::Personal,
        kind: EventKind::UserMessage,
        media_type: "text/plain; charset=utf-8".into(),
        occurred_at_ms: 10,
        expected_sequence: None,
    }
}

fn source(log: &SessionLog<'_>, event: &str, text: &str) -> RecordPin {
    log.append(
        OperationId::new(),
        event_input("session-a", event),
        text.as_bytes(),
    )
    .unwrap()
    .pin
}

fn definition(name: &str) -> TypeDefinition {
    TypeDefinition {
        name: name.into(),
        scope: Scope::Personal,
        kind: TypeKind::Object,
        properties: BTreeMap::from([(
            "text".into(),
            PropertyDefinition {
                value_type: PropertyType::Text,
                required: true,
            },
        )]),
    }
}

fn object(type_pin: &RecordPin, source: &RecordPin, text: &str) -> ObjectInput {
    ObjectInput {
        type_pin: type_pin.clone(),
        scope: Scope::Personal,
        properties: BTreeMap::from([("text".into(), json!(text))]),
        sources: vec![source.clone()],
        information: InformationKind::Inference,
        temporal: TemporalFacts::observed_at(10),
    }
}

fn query(text: &str) -> ContextQuery {
    ContextQuery {
        text: text.into(),
        seeds: Vec::new(),
        valid_at_ms: None,
        budget: RecallBudget::default(),
    }
}

fn relation(
    type_pin: &RecordPin,
    from: &RecordPin,
    to: &RecordPin,
    evidence: &RecordPin,
) -> RelationInput {
    RelationInput {
        type_pin: type_pin.clone(),
        scope: Scope::Personal,
        from: from.record.clone(),
        to: to.record.clone(),
        properties: BTreeMap::new(),
        sources: vec![evidence.clone()],
        information: InformationKind::Inference,
        temporal: TemporalFacts::observed_at(10),
    }
}

fn relation_type(context: &ContextStore<'_>, object_type: &RecordPin) -> RecordPin {
    context
        .define_type(
            OperationId::new(),
            TypeDefinition {
                name: "research.supports".into(),
                scope: Scope::Personal,
                kind: TypeKind::Relation {
                    from_types: vec![object_type.record.clone()],
                    to_types: vec![object_type.record.clone()],
                },
                properties: BTreeMap::new(),
            },
        )
        .unwrap()
        .pin
}

#[test]
fn independent_harnesses_share_types_history_and_relations_after_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("context");
    let (paper, decision, link, type_pin, raw) = {
        let db = Store::create(&root).unwrap();
        let research_log = db.session_log(access("owner")).unwrap();
        let research = db.context(access("owner")).unwrap();
        let raw = source(
            &research_log,
            "original",
            "Research found a capacity constraint that affects the decision.",
        );
        let type_pin = research
            .define_type(OperationId::new(), definition("research.observation"))
            .unwrap()
            .pin;
        let paper = research
            .save_object(
                OperationId::new(),
                object(&type_pin, &raw, "capacity observation"),
            )
            .unwrap()
            .pin;
        let decision = research
            .save_object(
                OperationId::new(),
                object(&type_pin, &raw, "reconsider the target allocation"),
            )
            .unwrap()
            .pin;
        let edge_type = relation_type(&research, &type_pin);
        let link = research
            .save_relation(
                OperationId::new(),
                relation(&edge_type, &paper, &decision, &raw),
            )
            .unwrap()
            .pin;
        (paper, decision, link, type_pin, raw)
    };
    let db = Store::open(&root).unwrap();
    let mut trader_access = access("owner");
    trader_access.actor = Actor::Assistant;
    trader_access.agent = Some("trading".into());
    trader_access.session = Some("session-b".into());
    let trading_log = db.session_log(trader_access.clone()).unwrap();
    let trading = db.context(trader_access).unwrap();
    assert_eq!(
        trading
            .find_type(&Scope::Personal, "research.observation")
            .unwrap()
            .unwrap()
            .0,
        type_pin
    );
    let recalled = trading.recall(query("capacity")).unwrap();
    assert!(recalled
        .hits
        .iter()
        .any(|hit| hit.record.header.pin == paper && hit.matched_terms > 0));
    let indirect = recalled
        .hits
        .iter()
        .find(|hit| hit.record.header.pin == decision)
        .unwrap();
    assert_eq!(indirect.path, vec![link]);
    assert_eq!(
        indirect.record.header.information,
        InformationKind::Inference
    );
    assert_eq!(indirect.record.header.sources, vec![raw.clone()]);
    let bytes = trading_log.read_payload(&raw, 0, 256).unwrap().bytes;
    assert_eq!(
        String::from_utf8(bytes).unwrap(),
        "Research found a capacity constraint that affects the decision."
    );
    assert_eq!(trading_log.position("session-a").unwrap(), 1);
    assert_eq!(trading_log.position("session-b").unwrap(), 0);
}

#[test]
fn type_versions_are_reused_and_old_instances_keep_their_schema() {
    let dir = tempfile::tempdir().unwrap();
    let db = Store::create(dir.path().join("context")).unwrap();
    let log = db.session_log(access("owner")).unwrap();
    let context = db.context(access("owner")).unwrap();
    let raw = source(&log, "raw", "A bounded observation");
    let original = definition("observation");
    let first = context
        .define_type(OperationId::new(), original.clone())
        .unwrap()
        .pin;
    assert_eq!(
        context
            .define_type(OperationId::new(), original.clone())
            .unwrap()
            .pin,
        first
    );
    let existing = context
        .save_object(OperationId::new(), object(&first, &raw, "old shape"))
        .unwrap()
        .pin;
    let mut second = original;
    second.properties.insert(
        "verified".into(),
        PropertyDefinition {
            value_type: PropertyType::Boolean,
            required: true,
        },
    );
    assert!(matches!(
        context.define_type(OperationId::new(), second.clone()),
        Err(ContextError::TypeConflict(_))
    ));
    let revised = context
        .revise_type(OperationId::new(), first.clone(), second.clone())
        .unwrap()
        .pin;
    assert_eq!(revised.revision, 2);
    assert!(matches!(
        context.revise_type(OperationId::new(), first.clone(), second),
        Err(ContextError::RevisionConflict {
            expected: 1,
            actual: 2
        })
    ));
    assert!(context.read(&existing).is_ok());
    assert!(matches!(
        context.save_object(OperationId::new(), object(&revised, &raw, "missing field")),
        Err(ContextError::InvalidInput(_))
    ));
    let mut new = object(&revised, &raw, "new shape");
    new.properties.insert("verified".into(), json!(true));
    assert!(context.save_object(OperationId::new(), new).is_ok());
    let page = context.list_types(1, None).unwrap();
    assert_eq!(page.definitions[0].0, revised);
    assert!(page.next.is_none());
}

#[test]
fn source_and_property_validation_reject_cross_owner_or_wrong_type() {
    let dir = tempfile::tempdir().unwrap();
    let db = Store::create(dir.path().join("context")).unwrap();
    let alice_log = db.session_log(access("alice")).unwrap();
    let alice = db.context(access("alice")).unwrap();
    let bob_log = db.session_log(access("bob")).unwrap();
    let bob = db.context(access("bob")).unwrap();
    let raw = source(&alice_log, "a", "Alice's evidence");
    let alice_type = alice
        .define_type(OperationId::new(), definition("note"))
        .unwrap()
        .pin;
    let bob_type = bob
        .define_type(OperationId::new(), definition("note"))
        .unwrap()
        .pin;
    assert!(matches!(
        bob_log.read_payload(&raw, 0, 100),
        Err(ContextError::AccessDenied)
    ));
    assert!(matches!(
        bob.save_object(OperationId::new(), object(&bob_type, &raw, "copied")),
        Err(ContextError::AccessDenied)
    ));
    let mut bad = object(&alice_type, &raw, "test");
    bad.properties.insert("text".into(), json!(42));
    assert!(matches!(
        alice.save_object(OperationId::new(), bad),
        Err(ContextError::InvalidInput(_))
    ));
    let mut bad = object(&alice_type, &raw, "test");
    bad.sources.clear();
    assert!(matches!(
        alice.save_object(OperationId::new(), bad),
        Err(ContextError::InvalidInput(_))
    ));
    let mut bad = object(&alice_type, &raw, "test");
    bad.properties
        .insert("undeclared".into(), json!("surprise"));
    assert!(matches!(
        alice.save_object(OperationId::new(), bad),
        Err(ContextError::InvalidInput(_))
    ));
    assert!(bob.recall(query("Alice")).unwrap().hits.is_empty());
}

#[test]
fn private_sources_do_not_become_public_through_objects_or_relations() {
    let dir = tempfile::tempdir().unwrap();
    let db = Store::create(dir.path().join("context")).unwrap();
    let private = Scope::Session(ulid::Ulid::new());
    let mut permitted = access("owner");
    permitted.scopes.push(private.clone());
    let all_log = db.session_log(permitted.clone()).unwrap();
    let all = db.context(permitted).unwrap();
    let mut raw_input = event_input("private", "raw");
    raw_input.scope = private.clone();
    let raw = all_log
        .append(OperationId::new(), raw_input, b"private fact".as_slice())
        .unwrap()
        .pin;
    let type_pin = all
        .define_type(OperationId::new(), definition("note"))
        .unwrap()
        .pin;
    assert!(matches!(
        all.save_object(OperationId::new(), object(&type_pin, &raw, "leaked")),
        Err(ContextError::AccessDenied)
    ));
    let mut private_note = object(&type_pin, &raw, "private fact");
    private_note.scope = private;
    let note = all
        .save_object(OperationId::new(), private_note)
        .unwrap()
        .pin;
    let public = db.context(access("owner")).unwrap();
    assert!(matches!(
        public.read(&note),
        Err(ContextError::AccessDenied)
    ));
    assert!(public.recall(query("private")).unwrap().hits.is_empty());
}

#[test]
fn complete_large_history_is_searchable_across_chunks_and_query_pages() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("context");
    let mut bytes = vec![b'x'; 2 * 1024 * 1024 + 31];
    let needle = "遗漏证据";
    let at = 64 * 1024 - 2;
    bytes[at..at + needle.len()].copy_from_slice(needle.as_bytes());
    let tail = b"TAIL_WITHOUT_CURATED_NOTE";
    let tail_at = bytes.len() - tail.len();
    bytes[tail_at..].copy_from_slice(tail);
    let pin = {
        let db = Store::create(&root).unwrap();
        db.session_log(access("owner"))
            .unwrap()
            .append(
                OperationId::new(),
                event_input("long", "large"),
                bytes.as_slice(),
            )
            .unwrap()
            .pin
    };
    let db = Store::open(&root).unwrap();
    let log = db.session_log(access("owner")).unwrap();
    let mut restored = Vec::new();
    let mut offset = 0;
    loop {
        let page = log.read_payload(&pin, offset, 65_537).unwrap();
        restored.extend_from_slice(&page.bytes);
        match page.next_offset {
            Some(next) => offset = next,
            None => break,
        }
    }
    assert_eq!(restored, bytes);
    for (text, expected_at) in [(needle, at), ("tail_without_curated_note", tail_at)] {
        let mut cursor = None;
        let mut found = Vec::new();
        for _ in 0..100 {
            let result = log
                .search(LogQuery {
                    text: text.into(),
                    session: None,
                    cursor,
                    max_events: 10,
                    max_bytes: 64 * 1024,
                    limit: 10,
                })
                .unwrap();
            assert!(result.bytes_examined <= 64 * 1024);
            found.extend(result.matches);
            cursor = result.cursor;
            if cursor.is_none() {
                break;
            }
        }
        assert!(cursor.is_none());
        assert_eq!(found.len(), 1);
        assert_eq!(found[0].header.pin, pin);
        assert_eq!(found[0].byte_offset, expected_at as u64);
    }
}

#[test]
fn append_retries_preserve_identity_and_order_even_after_later_events() {
    let dir = tempfile::tempdir().unwrap();
    let db = Store::create(dir.path().join("context")).unwrap();
    let log = db.session_log(access("owner")).unwrap();
    let operation = OperationId::new();
    let mut input = event_input("session", "first");
    input.expected_sequence = Some(0);
    let first = log
        .append(operation, input.clone(), b"first".as_slice())
        .unwrap();
    log.append(
        OperationId::new(),
        event_input("session", "second"),
        b"second".as_slice(),
    )
    .unwrap();
    assert_eq!(
        log.append(operation, input.clone(), b"first".as_slice())
            .unwrap(),
        first
    );
    input.expected_sequence = Some(2);
    assert_eq!(
        log.append(OperationId::new(), input.clone(), b"first".as_slice())
            .unwrap(),
        first
    );
    assert!(matches!(
        log.append(OperationId::new(), input, b"different".as_slice()),
        Err(ContextError::IdempotencyConflict)
    ));
    let page = log.events("session", 0, 1).unwrap();
    assert_eq!(page.entries[0].pin, first.pin);
    assert_eq!(page.next_sequence, Some(1));
    let second = log.events("session", 1, 1).unwrap();
    assert_eq!(second.entries[0].event.as_ref().unwrap().sequence, 2);
    assert!(second.next_sequence.is_none());
    assert_eq!(log.operation_receipt(operation).unwrap(), Some(first));
}

#[test]
fn concurrent_appends_allocate_one_session_order() {
    let dir = tempfile::tempdir().unwrap();
    let db = Arc::new(Store::create(dir.path().join("context")).unwrap());
    let jobs: Vec<_> = (0..8)
        .map(|index| {
            let db = db.clone();
            std::thread::spawn(move || {
                db.session_log(access("owner"))
                    .unwrap()
                    .append(
                        OperationId::new(),
                        event_input("shared", &format!("event-{index}")),
                        format!("item-{index}").as_bytes(),
                    )
                    .unwrap()
            })
        })
        .collect();
    let receipts: Vec<_> = jobs.into_iter().map(|job| job.join().unwrap()).collect();
    let positions: BTreeSet<_> = receipts
        .iter()
        .map(|receipt| receipt.sequence.unwrap())
        .collect();
    assert_eq!(positions, (1..=8).collect());
    let log = db.session_log(access("owner")).unwrap();
    let page = log.events("shared", 0, 100).unwrap();
    assert_eq!(page.entries.len(), 8);
    let mut stale = event_input("shared", "stale");
    stale.expected_sequence = Some(0);
    assert!(matches!(
        log.append(OperationId::new(), stale, b"stale".as_slice()),
        Err(ContextError::PositionConflict {
            expected: 0,
            actual: 8
        })
    ));
}

#[test]
fn tool_results_require_their_call_and_follow_its_purge_state() {
    let dir = tempfile::tempdir().unwrap();
    let db = Store::create(dir.path().join("context")).unwrap();
    let log = db
        .session_log(ContextAccess::new("owner", Actor::Operator))
        .unwrap();
    let mut result = event_input("session", "result");
    result.kind = EventKind::ToolResult {
        call_id: "call-1".into(),
        outcome: ToolOutcome::Succeeded,
    };
    assert!(matches!(
        log.append(OperationId::new(), result.clone(), b"response".as_slice()),
        Err(ContextError::InvalidInput(_))
    ));
    let mut call = event_input("session", "call");
    call.kind = EventKind::ToolCall {
        message_id: None,
        call_id: "call-1".into(),
        tool_name: "research".into(),
    };
    let call = log
        .append(OperationId::new(), call, b"arguments".as_slice())
        .unwrap();
    let result = log
        .append(OperationId::new(), result, b"response".as_slice())
        .unwrap();
    assert_eq!(
        log.inspect_header(&result.pin).unwrap().sources,
        vec![call.pin.clone()]
    );
    log.purge(OperationId::new(), call.pin).unwrap();
    assert!(matches!(
        log.read_payload(&result.pin, 0, 10),
        Err(ContextError::Unavailable(_))
    ));
}

#[test]
fn purged_sources_stay_unavailable_after_reopen_recovery_and_replay() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("context");
    let operation = OperationId::new();
    let (raw, note, input) = {
        let db = Store::create(&root).unwrap();
        let log = db.session_log(access("owner")).unwrap();
        let context = db.context(access("owner")).unwrap();
        let raw = source(&log, "raw", "sensitive observation");
        let type_pin = context
            .define_type(OperationId::new(), definition("note"))
            .unwrap()
            .pin;
        let input = object(&type_pin, &raw, "sensitive inference");
        let note = context.save_object(operation, input.clone()).unwrap().pin;
        let operator_log = db
            .session_log(ContextAccess::new("owner", Actor::Operator))
            .unwrap();
        operator_log.purge(OperationId::new(), raw.clone()).unwrap();
        assert!(matches!(
            context.read(&note),
            Err(ContextError::Unavailable(_))
        ));
        (raw, note, input)
    };
    let db = Store::open(&root).unwrap();
    let log = db.session_log(access("owner")).unwrap();
    let context = db.context(access("owner")).unwrap();
    assert_eq!(context.save_object(operation, input).unwrap().pin, note);
    assert!(context.recall(query("sensitive")).unwrap().hits.is_empty());
    assert!(matches!(
        log.read_payload(&raw, 0, 10),
        Err(ContextError::Unavailable(_))
    ));
    let report = log.recover_payloads(1, 1).unwrap();
    assert_eq!(report.chunks_removed, 1);
    assert!(!report.more);
    assert!(matches!(
        context.read(&note),
        Err(ContextError::Unavailable(_))
    ));
}

#[test]
fn correction_replaces_postings_and_retains_explicit_history() {
    let dir = tempfile::tempdir().unwrap();
    let db = Store::create(dir.path().join("context")).unwrap();
    let log = db.session_log(access("owner")).unwrap();
    let context = db.context(access("owner")).unwrap();
    let raw = source(&log, "raw", "A correction source");
    let type_pin = context
        .define_type(OperationId::new(), definition("note"))
        .unwrap()
        .pin;
    let old = context
        .save_object(
            OperationId::new(),
            object(&type_pin, &raw, "obsolete claim"),
        )
        .unwrap()
        .pin;
    let new = context
        .revise_object(
            OperationId::new(),
            old.clone(),
            object(&type_pin, &raw, "corrected claim"),
        )
        .unwrap()
        .pin;
    assert!(context.recall(query("obsolete")).unwrap().hits.is_empty());
    assert_eq!(
        context.recall(query("corrected")).unwrap().hits[0]
            .record
            .header
            .pin,
        new
    );
    assert!(
        matches!(context.read(&old).unwrap().body, RecordBody::Object(ref properties) if properties["text"] == "obsolete claim")
    );
    assert!(matches!(
        context.revise_object(OperationId::new(), old, object(&type_pin, &raw, "race")),
        Err(ContextError::RevisionConflict { .. })
    ));
    context.retract(OperationId::new(), new.clone()).unwrap();
    assert!(context.recall(query("corrected")).unwrap().hits.is_empty());
    assert_eq!(
        context.read(&new).unwrap().current_state,
        RecordState::Retracted
    );
}

#[test]
fn revisions_cannot_introduce_self_dependent_source_chains() {
    let dir = tempfile::tempdir().unwrap();
    let db = Store::create(dir.path().join("context")).unwrap();
    let log = db.session_log(access("owner")).unwrap();
    let store = db.context(access("owner")).unwrap();
    let raw = source(&log, "raw", "observation");
    let note = store
        .define_type(OperationId::new(), TypeDefinition::note(Scope::Personal))
        .unwrap()
        .pin;
    let a = store
        .save_object(OperationId::new(), object(&note, &raw, "first inference"))
        .unwrap()
        .pin;
    let b = store
        .save_object(OperationId::new(), object(&note, &a, "dependent inference"))
        .unwrap()
        .pin;
    for dependency in [&a, &b] {
        let operation = OperationId::new();
        assert!(matches!(
            store.revise_object(
                operation,
                a.clone(),
                object(&note, dependency, "circular revision")
            ),
            Err(ContextError::InvalidInput(_))
        ));
        assert!(store.operation_receipt(operation).unwrap().is_none());
        assert_eq!(store.head(&a.record).unwrap().pin, a);
    }
    let corrected = store
        .revise_object(
            OperationId::new(),
            a.clone(),
            object(&note, &raw, "corrected inference"),
        )
        .unwrap()
        .pin;
    let recalled = store.recall(query("inference")).unwrap();
    assert_eq!(recalled.hits.len(), 1);
    assert_eq!(recalled.hits[0].record.header.pin, corrected);
    assert_eq!(store.read(&b).unwrap().header.sources, vec![a]);
}

#[test]
fn relationship_cycles_are_bounded_and_inactive_endpoints_are_excluded() {
    let dir = tempfile::tempdir().unwrap();
    let db = Store::create(dir.path().join("context")).unwrap();
    let log = db.session_log(access("owner")).unwrap();
    let context = db.context(access("owner")).unwrap();
    let raw = source(&log, "raw", "source");
    let type_pin = context
        .define_type(OperationId::new(), definition("note"))
        .unwrap()
        .pin;
    let a = context
        .save_object(OperationId::new(), object(&type_pin, &raw, "alpha"))
        .unwrap()
        .pin;
    let b = context
        .save_object(OperationId::new(), object(&type_pin, &raw, "beta"))
        .unwrap()
        .pin;
    let relation_type = relation_type(&context, &type_pin);
    context
        .save_relation(OperationId::new(), relation(&relation_type, &a, &b, &raw))
        .unwrap();
    context
        .save_relation(OperationId::new(), relation(&relation_type, &b, &a, &raw))
        .unwrap();
    let result = context.recall(query("alpha")).unwrap();
    assert_eq!(
        result
            .hits
            .iter()
            .filter(|hit| hit.record.header.pin == b)
            .count(),
        1
    );
    assert!(result.edges_examined <= 512);
    let mut bounded = query("alpha");
    bounded.budget.max_candidates = 1;
    let result = context.recall(bounded).unwrap();
    assert!(result.truncated);
    assert_eq!(result.hits.len(), 1);
    context.retract(OperationId::new(), b).unwrap();
    assert_eq!(context.recall(query("alpha")).unwrap().hits.len(), 1);
}

#[test]
fn relation_pages_apply_direction_scope_time_and_type_filters_without_stalling() {
    let dir = tempfile::tempdir().unwrap();
    let db = Store::create(dir.path().join("context")).unwrap();
    let private = Scope::Session(ulid::Ulid::new());
    let mut permitted = access("owner");
    permitted.scopes.push(private.clone());
    let all_log = db.session_log(permitted.clone()).unwrap();
    let all = db.context(permitted).unwrap();
    let raw = source(&all_log, "raw", "evidence");
    let type_pin = all
        .define_type(OperationId::new(), TypeDefinition::note(Scope::Personal))
        .unwrap()
        .pin;
    assert_eq!(all.list_types(1, None).unwrap().definitions[0].0, type_pin);
    assert_eq!(
        all.find_type(&Scope::Personal, "mmdb.note")
            .unwrap()
            .unwrap()
            .0,
        type_pin
    );
    let a = all
        .save_object(OperationId::new(), object(&type_pin, &raw, "alpha"))
        .unwrap()
        .pin;
    let b = all
        .save_object(OperationId::new(), object(&type_pin, &raw, "beta"))
        .unwrap()
        .pin;
    let mut timed_input = object(&type_pin, &raw, "expired endpoint");
    timed_input.temporal.valid_to_ms = Some(20);
    let timed = all
        .save_object(OperationId::new(), timed_input)
        .unwrap()
        .pin;
    let supports = relation_type(&all, &type_pin);
    let opposes = all
        .define_type(
            OperationId::new(),
            TypeDefinition {
                name: "research.opposes".into(),
                scope: Scope::Personal,
                kind: TypeKind::Relation {
                    from_types: vec![type_pin.record.clone()],
                    to_types: vec![type_pin.record.clone()],
                },
                properties: BTreeMap::new(),
            },
        )
        .unwrap()
        .pin;
    let out = all
        .save_relation(OperationId::new(), relation(&supports, &a, &b, &raw))
        .unwrap()
        .pin;
    let incoming = all
        .save_relation(OperationId::new(), relation(&supports, &b, &a, &raw))
        .unwrap()
        .pin;
    all.save_relation(OperationId::new(), relation(&opposes, &a, &b, &raw))
        .unwrap();
    all.save_relation(OperationId::new(), relation(&supports, &a, &timed, &raw))
        .unwrap();
    let mut private_link = relation(&supports, &a, &b, &raw);
    private_link.scope = private;
    all.save_relation(OperationId::new(), private_link).unwrap();
    let public = db.context(access("owner")).unwrap();
    let mut request = RelationQuery {
        endpoint: a.record.clone(),
        direction: RelationDirection::Both,
        type_filter: Some(supports.record),
        valid_at_ms: Some(20),
        cursor: None,
        max_edges: 1,
        limit: 1,
    };
    let mut found = BTreeSet::new();
    let mut pages = 0;
    let mut first_cursor = None;
    loop {
        let page = public.relations(request.clone()).unwrap();
        assert!(page.edges_examined <= 1);
        found.extend(
            page.relations
                .into_iter()
                .map(|relation| relation.header.pin),
        );
        pages += 1;
        if pages == 1 {
            first_cursor = page.cursor.clone();
        }
        assert!(pages <= 5);
        match page.cursor {
            Some(cursor) => {
                request.cursor =
                    Some(serde_json::from_value(serde_json::to_value(cursor).unwrap()).unwrap())
            }
            None => break,
        }
    }
    assert_eq!(found, BTreeSet::from([out.clone(), incoming.clone()]));
    request.cursor = None;
    request.max_edges = 10;
    request.direction = RelationDirection::Outgoing;
    let page = public.relations(request.clone()).unwrap();
    assert_eq!(page.relations[0].header.pin, out);
    request.cursor = first_cursor;
    request.direction = RelationDirection::Incoming;
    assert!(matches!(
        public.relations(request.clone()),
        Err(ContextError::InvalidInput(_))
    ));
    request.cursor = None;
    assert_eq!(
        public.relations(request.clone()).unwrap().relations[0]
            .header
            .pin,
        incoming
    );
    all.retract(OperationId::new(), b).unwrap();
    assert!(public.relations(request).unwrap().relations.is_empty());
    let mut timed_query = query("alpha");
    timed_query.valid_at_ms = Some(20);
    assert_eq!(public.recall(timed_query).unwrap().hits.len(), 1);
}

#[test]
fn reset_creates_a_new_context_era_without_importing_previous_history() {
    use mmdb::store_format::{commit_reset, ResetPlan, ResetSafety, StoreEraId};
    use std::time::{Duration, SystemTime};
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().canonicalize().unwrap().join("context");
    let old = {
        let db = Store::create(&root).unwrap();
        source(
            &db.session_log(access("owner")).unwrap(),
            "old",
            "old source",
        )
    };
    let safety = ResetSafety::for_current_process(Vec::new()).unwrap();
    let plan = ResetPlan::build(
        std::slice::from_ref(&root),
        STORE_FORMAT_ID,
        STORE_FORMAT_ID,
        SystemTime::now(),
        Duration::from_secs(60),
        &safety,
    )
    .unwrap();
    let receipt = commit_reset(&plan, plan.digest().as_str(), StoreEraId::new(), &[]).unwrap();
    let db = Store::open(&root).unwrap();
    let fresh_log = db.session_log(access("owner")).unwrap();
    assert_eq!(fresh_log.position("session-a").unwrap(), 0);
    assert!(matches!(
        fresh_log.read_payload(&old, 0, 100),
        Err(ContextError::AccessDenied)
    ));
    let previous = Store::open(receipt.quarantine_path()).unwrap();
    assert_eq!(
        previous
            .session_log(access("owner"))
            .unwrap()
            .read_payload(&old, 0, 32)
            .unwrap()
            .bytes,
        b"old source"
    );
}

#[test]
fn chinese_terms_find_curated_context_without_embeddings() {
    let dir = tempfile::tempdir().unwrap();
    let db = Store::create(dir.path().join("context")).unwrap();
    let log = db.session_log(access("owner")).unwrap();
    let context = db.context(access("owner")).unwrap();
    let raw = source(&log, "raw", "项目需要考虑交易日窗口。");
    let type_pin = context
        .define_type(OperationId::new(), definition("决策"))
        .unwrap()
        .pin;
    let saved = context
        .save_object(
            OperationId::new(),
            object(&type_pin, &raw, "默认使用交易日窗口"),
        )
        .unwrap()
        .pin;
    assert_eq!(
        context.recall(query("交易日")).unwrap().hits[0]
            .record
            .header
            .pin,
        saved
    );
}

struct FailingReader {
    remaining: usize,
}
impl Read for FailingReader {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        if self.remaining == 0 {
            return Err(io::Error::other("simulated source interruption"));
        }
        let count = buffer.len().min(self.remaining);
        buffer[..count].fill(b'x');
        self.remaining -= count;
        Ok(count)
    }
}

#[test]
fn incomplete_uploads_never_advance_history_and_can_be_cleaned() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("context");
    let operation = OperationId::new();
    {
        let db = Store::create(&root).unwrap();
        let log = db.session_log(access("owner")).unwrap();
        assert!(matches!(
            log.append(
                operation,
                event_input("session", "interrupted"),
                FailingReader {
                    remaining: PAYLOAD_CHUNK_BYTES + 5
                }
            ),
            Err(ContextError::Io(_))
        ));
        assert_eq!(log.position("session").unwrap(), 0);
        assert!(log.operation_receipt(operation).unwrap().is_none());
    }
    let db = Store::open(&root).unwrap();
    let log = db.session_log(access("owner")).unwrap();
    assert!(log.events("session", 0, 10).unwrap().entries.is_empty());
    let context = db.context(access("owner")).unwrap();
    assert_eq!(context.recover_payloads(10, 10).unwrap().chunks_removed, 0);
    let report = log.recover_payloads(10, 10).unwrap();
    assert_eq!(report.chunks_removed, 1);
    assert_eq!(report.uploads_removed, 1);
    assert_eq!(
        log.append(
            operation,
            event_input("session", "interrupted"),
            b"complete retry".as_slice()
        )
        .unwrap()
        .sequence,
        Some(1)
    );
}

#[test]
fn history_crash_child() {
    let Some(root) = std::env::var_os("MMDB_CONTEXT_TEST_CRASH_ROOT") else {
        return;
    };
    struct CrashReader(bool);
    impl Read for CrashReader {
        fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
            if self.0 {
                std::process::exit(77);
            }
            self.0 = true;
            buffer.fill(b'x');
            Ok(buffer.len())
        }
    }
    let db = Store::create(root).unwrap();
    db.session_log(access("owner"))
        .unwrap()
        .append(
            OperationId::new(),
            event_input("session", "crashed"),
            CrashReader(false),
        )
        .unwrap();
    panic!("crash reader must terminate its process");
}

#[test]
fn actual_process_interruption_leaves_no_partial_visible_history() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("context");
    let status = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "history_crash_child", "--nocapture"])
        .env("MMDB_CONTEXT_TEST_CRASH_ROOT", &root)
        .status()
        .unwrap();
    assert_eq!(status.code(), Some(77));
    let db = Store::open(&root).unwrap();
    let log = db.session_log(access("owner")).unwrap();
    assert_eq!(log.position("session").unwrap(), 0);
    assert_eq!(log.recover_payloads(10, 10).unwrap().chunks_removed, 1);
    assert_eq!(
        log.append(
            OperationId::new(),
            event_input("session", "after-crash"),
            b"resumed".as_slice()
        )
        .unwrap()
        .sequence,
        Some(1)
    );
}

#[test]
fn format_and_store_identity_prevent_accidental_adoption() {
    let dir = tempfile::tempdir().unwrap();
    use mmdb::store_format::{StoreEraId, StoreFormatError, StoreManifest};
    use mmdb_kernel::KernelError;
    for legacy_format in [
        "mmdb-context-v1",
        "mmdb-native-memory-v1",
        "mmdb-context-dev7",
        "mmdb-native-memory-dev7",
    ] {
        let legacy = dir.path().join(legacy_format);
        std::fs::create_dir(&legacy).unwrap();
        StoreManifest::new(legacy_format, StoreEraId::new())
            .unwrap()
            .write_new(&legacy)
            .unwrap();
        assert!(matches!(
            Store::open(&legacy),
            Err(StoreError::Kernel(KernelError::Format(StoreFormatError::FormatMismatch { expected, found, .. })))
                if expected == STORE_FORMAT_ID && found == legacy_format
        ));
    }
    let first = Store::create(dir.path().join("first")).unwrap();
    let log = first.session_log(access("owner")).unwrap();
    let context = first.context(access("owner")).unwrap();
    let pin = source(&log, "raw", "one era");
    let type_pin = context
        .define_type(OperationId::new(), definition("note"))
        .unwrap()
        .pin;
    assert_eq!(pin.record.era, first.era(StoreModule::Session));
    assert_eq!(type_pin.record.era, first.era(StoreModule::Memory));
    assert_ne!(pin.record.era, type_pin.record.era);
    assert_eq!(first.memory().era_id(), type_pin.record.era);
    let second = Store::create(dir.path().join("second")).unwrap();
    let foreign_log = second.session_log(access("owner")).unwrap();
    let foreign = second.context(access("owner")).unwrap();
    assert!(matches!(
        foreign_log.read_payload(&pin, 0, 32),
        Err(ContextError::AccessDenied)
    ));
    assert!(matches!(
        foreign.read(&type_pin),
        Err(ContextError::AccessDenied)
    ));
}

#[test]
fn source_validation_reserves_the_new_root_depth_and_aggregate_budget() {
    let tmp = tempfile::tempdir().unwrap();
    let db = Store::create(tmp.path().join("context")).unwrap();
    let log = db.session_log(access("owner")).unwrap();
    let store = db.context(access("owner")).unwrap();
    let root = source(&log, "source", "evidence");
    let kind = store
        .define_type(OperationId::new(), definition("test.depth"))
        .unwrap()
        .pin;
    let mut previous = root.clone();
    for _ in 0..32 {
        previous = store
            .save_object(OperationId::new(), object(&kind, &previous, "depth"))
            .unwrap()
            .pin;
    }
    assert!(store.read(&previous).is_ok());
    let failed = OperationId::new();
    assert!(matches!(
        store.save_object(failed, object(&kind, &previous, "too deep")),
        Err(ContextError::BudgetExceeded)
    ));
    assert!(store.operation_receipt(failed).unwrap().is_none());
    let mut wide = object(&kind, &root, "wide");
    wide.sources = vec![root; 64];
    let wide = store.save_object(OperationId::new(), wide).unwrap().pin;
    let mut too_wide = object(&kind, &wide, "too wide");
    too_wide.sources = vec![wide; 64];
    assert!(matches!(
        store.save_object(OperationId::new(), too_wide),
        Err(ContextError::BudgetExceeded)
    ));
    assert!(!store.recall(query("depth")).unwrap().hits.is_empty());
}

#[test]
fn action_discovery_and_preparation_share_the_same_target_scope() {
    let d = tempfile::tempdir().unwrap();
    let db = Store::create(d.path().join("db")).unwrap();
    let a = Scope::Session(ulid::Ulid::new());
    let b = Scope::Session(ulid::Ulid::new());
    let mut access = access("owner");
    access.scopes.extend([a.clone(), b.clone()]);
    let host_log = db.session_log(access.clone()).unwrap();
    let host = db.context(access.clone()).unwrap();
    let other = db.context(access).unwrap();
    let raw = source(&host_log, "scope-evidence", "bounded");
    let t = host
        .define_type(OperationId::new(), definition("example.task"))
        .unwrap()
        .pin;
    let mut target = object(&t, &raw, "target");
    target.scope = b.clone();
    let target = host.save_object(OperationId::new(), target).unwrap().pin;
    for scope in [a, b.clone(), Scope::Personal] {
        let allowed = scope == b || scope == Scope::Personal;
        let pin = host
            .define_action(
                OperationId::new(),
                ActionDefinition {
                    name: "example.apply".into(),
                    description: "Apply the change".into(),
                    scope,
                    target_type: t.clone(),
                    inputs: BTreeMap::new(),
                    preconditions: vec![],
                    sources: vec![raw.clone()],
                },
            )
            .unwrap()
            .pin;
        let offered = other
            .actions_for(&target, 100, None)
            .unwrap()
            .actions
            .iter()
            .any(|v| v.pin == pin);
        let prepared = other.prepare_action(
            OperationId::new(),
            ActionInvocation {
                action: pin,
                target: target.clone(),
                inputs: BTreeMap::new(),
                sources: vec![],
            },
        );
        assert_eq!(offered, allowed);
        assert_eq!(prepared.is_ok(), allowed);
    }
}

#[test]
fn action_contract_enforces_authority_versions_conditions_and_one_preparation() {
    let tmp = tempfile::tempdir().unwrap();
    let db = Arc::new(Store::create(tmp.path().join("context")).unwrap());
    let log = db.session_log(access("owner")).unwrap();
    let store = db.context(access("owner")).unwrap();
    let evidence = source(&log, "source", "milestone instruction");
    let kind = store
        .define_type(OperationId::new(), definition("pmo.milestone"))
        .unwrap()
        .pin;
    let target = store
        .save_object(OperationId::new(), object(&kind, &evidence, "pending"))
        .unwrap()
        .pin;
    let action = ActionDefinition {
        name: "pmo.complete".into(),
        description: "Complete a pending milestone".into(),
        scope: Scope::Personal,
        target_type: kind.clone(),
        inputs: BTreeMap::from([(
            "text".into(),
            PropertyDefinition {
                value_type: PropertyType::Text,
                required: true,
            },
        )]),
        preconditions: vec![ActionCondition::Equals {
            property: "text".into(),
            value: json!("pending"),
        }],
        sources: vec![evidence.clone()],
    };
    let assistant = db
        .context(ContextAccess::new("owner", Actor::Assistant))
        .unwrap();
    let mut invalid = action.clone();
    invalid.preconditions = vec![ActionCondition::Equals {
        property: "text".into(),
        value: json!(false),
    }];
    assert!(matches!(
        assistant.define_action(OperationId::new(), invalid),
        Err(ContextError::InvalidInput(_))
    ));
    let action_pin = assistant
        .define_action(OperationId::new(), action.clone())
        .unwrap()
        .pin;
    let offers = assistant.actions_for(&target, 10, None).unwrap();
    assert_eq!(offers.actions.len(), 1);
    assert!(offers.actions[0].preconditions_met);
    let invocation = ActionInvocation {
        action: action_pin.clone(),
        target: target.clone(),
        inputs: BTreeMap::from([("text".into(), json!("completed"))]),
        sources: Vec::new(),
    };
    assert!(matches!(
        assistant.prepare_action(OperationId::new(), invocation.clone()),
        Err(ContextError::AccessDenied)
    ));
    let operation = OperationId::new();
    let handles = (0..2)
        .map(|_| {
            let db = db.clone();
            let invocation = invocation.clone();
            std::thread::spawn(move || {
                db.context(access("owner"))
                    .unwrap()
                    .prepare_action_once(operation, invocation)
                    .unwrap()
            })
        })
        .collect::<Vec<_>>();
    let prepared = handles
        .into_iter()
        .map(|handle| handle.join().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(prepared[0].receipt, prepared[1].receipt);
    assert_eq!(
        prepared
            .iter()
            .filter(|result| !result.already_present)
            .count(),
        1
    );
    let preparation = prepared[0].receipt.pin.clone();
    assert!(matches!(
        assistant.retract(OperationId::new(), preparation.clone()),
        Err(ContextError::AccessDenied)
    ));
    let mut expired = object(&kind, &evidence, "pending");
    expired.temporal.valid_from_ms = Some(0);
    expired.temporal.valid_to_ms = Some(1);
    let expired = store.save_object(OperationId::new(), expired).unwrap().pin;
    assert!(matches!(
        store.prepare_action(
            OperationId::new(),
            ActionInvocation {
                target: expired,
                ..invocation.clone()
            }
        ),
        Err(ContextError::Unavailable(_))
    ));
    assert!(matches!(
        assistant.finish_action(
            OperationId::new(),
            preparation.clone(),
            ActionStatus::Succeeded,
            json!({}),
            vec![evidence.clone()]
        ),
        Err(ContextError::AccessDenied)
    ));
    let updated = store
        .revise_object(OperationId::new(), target, object(&kind, &evidence, "done"))
        .unwrap()
        .pin;
    let outcome = store
        .finish_action(
            OperationId::new(),
            preparation,
            ActionStatus::Succeeded,
            json!({"actual":"done"}),
            vec![evidence.clone()],
        )
        .unwrap();
    assert!(matches!(
        store.read(&outcome.pin).unwrap().body,
        RecordBody::ActionExecution(ActionExecution {
            status: ActionStatus::Succeeded,
            ..
        })
    ));
    assert!(store
        .prepare_action(OperationId::new(), invocation.clone())
        .is_err());
    let mut changed = invocation;
    changed.target = updated;
    assert!(store.prepare_action(OperationId::new(), changed).is_err());
    assert!(!store
        .actions_for(
            &store.head(&outcome.pin.record).unwrap().sources[1],
            10,
            None
        )
        .is_ok());
    let revised = assistant
        .revise_action(
            OperationId::new(),
            action_pin.clone(),
            ActionDefinition {
                description: "A revised action".into(),
                ..action
            },
        )
        .unwrap();
    assert_eq!(revised.pin.revision, 2);
    assert!(store.read(&action_pin).is_ok());
}

#[test]
fn copied_history_tracks_source_availability_and_session_catalog_pages() {
    let tmp = tempfile::tempdir().unwrap();
    let db = Store::create(tmp.path().join("context")).unwrap();
    let log = db.session_log(access("owner")).unwrap();
    let store = db.context(access("owner")).unwrap();
    let original = source(&log, "source", "withdrawable-secret");
    let mut copied = event_input("model-session", "model-request");
    copied.kind = EventKind::Notice;
    copied.sources = vec![original.clone()];
    let copy = log
        .append(
            OperationId::new(),
            copied,
            b"withdrawable-secret copied".as_slice(),
        )
        .unwrap();
    assert!(log.read_payload(&copy.pin, 0, 100).is_ok());
    db.session_log(ContextAccess::new("owner", Actor::User))
        .unwrap()
        .purge(OperationId::new(), original)
        .unwrap();
    assert_eq!(
        log.read_payload(&copy.pin, 0, 100).unwrap().bytes,
        b"withdrawable-secret copied"
    );
    assert!(matches!(
        store.validate_current(&[copy.pin], None),
        Err(ContextError::Unavailable(_))
    ));
    for index in 0..7 {
        log.append(
            OperationId::new(),
            event_input(&format!("session-{index}"), "input"),
            b"original".as_slice(),
        )
        .unwrap();
    }
    let mut sessions = BTreeSet::new();
    let mut after = None;
    loop {
        let page = log.sessions(2, after.as_deref()).unwrap();
        for session in page.sessions {
            assert!(sessions.insert(session));
        }
        match page.next {
            Some(next) => after = Some(next),
            None => break,
        }
    }
    assert_eq!(sessions.len(), 9);
    assert!(db
        .session_log(access("another-owner"))
        .unwrap()
        .sessions(2, None)
        .unwrap()
        .sessions
        .is_empty());
    assert!(matches!(
        db.session_log(ContextAccess::new("owner", Actor::Assistant))
            .unwrap()
            .sessions(2, None),
        Err(ContextError::AccessDenied)
    ));
}

#[test]
fn current_validation_rejects_changed_dependencies_without_breaking_historical_reads() {
    let tmp = tempfile::tempdir().unwrap();
    let db = Store::create(tmp.path().join("context")).unwrap();
    let log = db.session_log(access("owner")).unwrap();
    let store = db.context(access("owner")).unwrap();
    let root = source(&log, "original", "original instruction");
    let kind = store
        .define_type(OperationId::new(), definition("test.current"))
        .unwrap()
        .pin;
    let upstream = store
        .save_object(OperationId::new(), object(&kind, &root, "old upstream"))
        .unwrap()
        .pin;
    let dependent = store
        .save_object(
            OperationId::new(),
            object(&kind, &upstream, "derived context"),
        )
        .unwrap()
        .pin;
    assert!(store
        .validate_current(std::slice::from_ref(&dependent), None)
        .is_ok());
    store
        .revise_object(
            OperationId::new(),
            upstream,
            object(&kind, &root, "new upstream"),
        )
        .unwrap();
    assert!(store.read(&dependent).is_ok());
    assert!(matches!(
        store.validate_current(&[dependent], None),
        Err(ContextError::Unavailable(_))
    ));
}

#[test]
fn checkpoint_parent_and_epoch_are_compared_atomically_and_survive_reopen() {
    let tmp = tempfile::tempdir().unwrap();
    let path = tmp.path().join("context");
    let db = Store::create(&path).unwrap();
    let log = db.session_log(access("owner")).unwrap();
    let store = db.context(access("owner")).unwrap();
    let anchor = source(&log, "event", "original");
    let input = CheckpointInput {
        scope: CheckpointScope::Session,
        session: "session-a".into(),
        run: "run".into(),
        sequence: 1,
        expected_previous: None,
        expected_availability_epoch: store.availability_epoch().unwrap(),
        sources: vec![anchor.clone()],
        payload: json!({"goal":"continue"}),
    };
    let first = log
        .save_checkpoint(OperationId::new(), input.clone())
        .unwrap();
    assert!(log.save_checkpoint(OperationId::new(), input).is_err());
    let tail = source(&log, "tail", "new message");
    assert_eq!(log.events("session-a", 1, 10).unwrap().entries[0].pin, tail);
    let old_epoch = store.availability_epoch().unwrap();
    db.session_log(ContextAccess::new("owner", Actor::User))
        .unwrap()
        .purge(OperationId::new(), anchor)
        .unwrap();
    assert!(matches!(
        log.read_checkpoint(&first.pin),
        Err(ContextError::Unavailable(_))
    ));
    assert!(log
        .save_checkpoint(
            OperationId::new(),
            CheckpointInput {
                scope: CheckpointScope::Session,
                session: "session-a".into(),
                run: "run".into(),
                sequence: 2,
                expected_previous: Some(first.pin.clone()),
                expected_availability_epoch: old_epoch,
                sources: vec![tail],
                payload: json!({})
            }
        )
        .is_err());
    drop(store);
    drop(db);
    let reopened = Store::open(path).unwrap();
    let log = reopened.session_log(access("owner")).unwrap();
    assert_eq!(
        log.latest_checkpoint("session-a").unwrap().unwrap().pin,
        first.pin
    );
    assert!(matches!(
        log.read_checkpoint(&first.pin),
        Err(ContextError::Unavailable(_))
    ));
    assert_eq!(log.position("session-a").unwrap(), 2);
}

#[test]
fn live_revision_keeps_checkpoint_derived_originals_and_pairs_the_completed_tool() {
    let tmp = tempfile::tempdir().unwrap();
    let path = tmp.path().join("context");
    let db = Store::create(&path).unwrap();
    let log = db.session_log(access("owner")).unwrap();
    let store = db.context(access("owner")).unwrap();
    let raw = source(&log, "original", "SQLite source text");
    let kind = store
        .define_type(OperationId::new(), definition("note"))
        .unwrap()
        .pin;
    let note = store
        .save_object(
            OperationId::new(),
            object(&kind, &raw, "initial conclusion"),
        )
        .unwrap()
        .pin;
    let checkpoint = log
        .save_checkpoint(
            OperationId::new(),
            CheckpointInput {
                scope: CheckpointScope::Session,
                session: "session-a".into(),
                run: "run".into(),
                sequence: 1,
                expected_previous: None,
                expected_availability_epoch: store.availability_epoch().unwrap(),
                sources: vec![raw.clone(), note.clone()],
                payload: json!({"goal":"continue the study"}),
            },
        )
        .unwrap()
        .pin;
    let derived = store
        .save_object(
            OperationId::new(),
            object(&kind, &checkpoint, "checkpoint-derived detail"),
        )
        .unwrap()
        .pin;
    let mut call_input = event_input("session-a", "revise-call");
    call_input.kind = EventKind::ToolCall {
        call_id: "revise-call".into(),
        tool_name: "context".into(),
        message_id: None,
    };
    call_input.sources = vec![derived.clone()];
    let call = log
        .append(
            OperationId::new(),
            call_input,
            b"revise the original note".as_slice(),
        )
        .unwrap()
        .pin;
    let changed = store
        .revise_object(
            OperationId::new(),
            note.clone(),
            object(&kind, &raw, "revised conclusion"),
        )
        .unwrap()
        .pin;
    assert!(
        store
            .validate_current(std::slice::from_ref(&checkpoint), None)
            .is_err(),
        "old checkpoint cannot be current recovery state"
    );
    assert!(
        store
            .validate_current(std::slice::from_ref(&derived), None)
            .is_err(),
        "derived knowledge must be revalidated before current recall"
    );
    assert!(
        log.read_checkpoint(&checkpoint).is_ok(),
        "a correction must preserve the exact historical snapshot"
    );
    assert!(
        store.read(&derived).is_ok(),
        "a correction must preserve derived original payloads"
    );
    assert!(
        log.read_payload(&call, 0, 100).is_ok(),
        "the already dispatched call must remain readable"
    );
    let mut result_input = event_input("session-a", "revise-result");
    result_input.kind = EventKind::ToolResult {
        call_id: "revise-call".into(),
        outcome: ToolOutcome::Succeeded,
    };
    result_input.sources = vec![changed.clone()];
    let result = log
        .append(
            OperationId::new(),
            result_input,
            b"revision 2 committed".as_slice(),
        )
        .unwrap()
        .pin;
    assert_eq!(
        log.tool_pair("session-a", "revise-call")
            .unwrap()
            .1
            .unwrap()
            .pin,
        result
    );
    drop(store);
    drop(db);
    let db = Store::open(&path).unwrap();
    let log = db.session_log(access("owner")).unwrap();
    let store = db.context(access("owner")).unwrap();
    assert!(log.read_checkpoint(&checkpoint).is_ok());
    assert!(store.read(&derived).is_ok());
    for pin in [&call, &result] {
        assert!(log.read_payload(pin, 0, 100).is_ok());
    }
    db.session_log(ContextAccess::new("owner", Actor::User))
        .unwrap()
        .purge(OperationId::new(), raw)
        .unwrap();
    assert!(matches!(
        log.read_checkpoint(&checkpoint),
        Err(ContextError::Unavailable(_))
    ));
    assert!(matches!(
        store.read(&derived),
        Err(ContextError::Unavailable(_))
    ));
    for (pin, payload) in [
        (&call, b"revise the original note".as_slice()),
        (&result, b"revision 2 committed".as_slice()),
    ] {
        assert_eq!(log.read_payload(pin, 0, 100).unwrap().bytes, payload);
        assert!(matches!(
            store.validate_current(std::slice::from_ref(pin), None),
            Err(ContextError::Unavailable(_))
        ));
    }
}

#[test]
fn checkpoint_history_redaction_remains_conservative_for_unlisted_summary_input() {
    let tmp = tempfile::tempdir().unwrap();
    let db = Store::create(tmp.path().join("context")).unwrap();
    let log = db.session_log(access("owner")).unwrap();
    let store = db.context(access("owner")).unwrap();
    let omitted = source(
        &log,
        "earlier",
        "detail summarized but not individually pinned",
    );
    let anchor = source(&log, "anchor", "last event");
    let checkpoint = log
        .save_checkpoint(
            OperationId::new(),
            CheckpointInput {
                scope: CheckpointScope::Session,
                session: "session-a".into(),
                run: "run".into(),
                sequence: 2,
                expected_previous: None,
                expected_availability_epoch: store.availability_epoch().unwrap(),
                sources: vec![anchor],
                payload: json!({"summary":"detail summarized but not individually pinned"}),
            },
        )
        .unwrap()
        .pin;
    db.session_log(ContextAccess::new("owner", Actor::User))
        .unwrap()
        .retract(OperationId::new(), omitted)
        .unwrap();
    assert!(matches!(
        log.read_checkpoint(&checkpoint),
        Err(ContextError::Unavailable(_))
    ));
}

#[test]
fn session_and_child_run_checkpoints_keep_independent_cas_chains_across_reopen() {
    let tmp = tempfile::tempdir().unwrap();
    let path = tmp.path().join("context");
    let mut parent = ContextAccess::new("owner", Actor::Assistant);
    parent.session = Some("session-a".into());
    parent.run = Some("parent-run".into());
    let mut child = parent.clone();
    child.run = Some("child-run".into());
    child.parent_run = parent.run.clone();
    let mut sibling = child.clone();
    sibling.run = Some("sibling-run".into());
    let cases = [
        (CheckpointScope::Session, parent),
        (CheckpointScope::Run, child),
        (CheckpointScope::Run, sibling),
    ];
    let latest = |log: &SessionLog<'_>, scope, run: &str| {
        match scope {
            CheckpointScope::Session => log.latest_checkpoint("session-a"),
            CheckpointScope::Run => log.latest_run_checkpoint("session-a", run),
        }
        .unwrap()
    };

    let (base, first) = {
        let db = Store::create(&path).unwrap();
        let log = db.session_log(access("owner")).unwrap();
        let store = db.context(access("owner")).unwrap();
        let anchor = source(&log, "anchor", "committed parent history");
        let base = CheckpointInput {
            scope: CheckpointScope::Session,
            session: "session-a".into(),
            run: "parent-run".into(),
            sequence: 1,
            expected_previous: None,
            expected_availability_epoch: store.availability_epoch().unwrap(),
            sources: vec![anchor],
            payload: json!({"goal": "resume committed work"}),
        };
        let mut first = Vec::new();
        for (scope, profile) in &cases {
            let profile_log = db.session_log(profile.clone()).unwrap();
            let run = profile.run.as_deref().unwrap();
            assert!(latest(&profile_log, *scope, run).is_none());
            let pin = profile_log
                .save_checkpoint(
                    OperationId::new(),
                    CheckpointInput {
                        scope: *scope,
                        run: run.into(),
                        ..base.clone()
                    },
                )
                .unwrap()
                .pin;
            first.push(pin.clone());
            assert_eq!(latest(&profile_log, *scope, run).unwrap().pin, pin);
            assert_eq!(
                log.latest_checkpoint("session-a").unwrap().unwrap().pin,
                first[0]
            );
        }
        (base, first)
    };

    // Every handle and the database have dropped before acquiring a new lease.
    let second = {
        let db = Store::open(&path).unwrap();
        for ((scope, profile), pin) in cases.iter().zip(&first) {
            let log = db.session_log(profile.clone()).unwrap();
            let run = profile.run.as_deref().unwrap();
            let header = latest(&log, *scope, run).unwrap();
            assert_eq!(&header.pin, pin);
            assert_eq!(
                header.checkpoint,
                Some(CheckpointMetadata {
                    scope: *scope,
                    session: "session-a".into(),
                    run: run.into(),
                    window: 1,
                    sequence: 1,
                    previous: None,
                    availability_epoch: base.expected_availability_epoch,
                })
            );
            assert_eq!(header.provenance.run, profile.run);
            assert_eq!(header.provenance.parent_run, profile.parent_run);
            if profile.parent_run.is_some() {
                let operation = OperationId::new();
                assert!(matches!(
                    log.save_checkpoint(
                        operation,
                        CheckpointInput {
                            run: run.into(),
                            expected_previous: Some(first[0].clone()),
                            ..base.clone()
                        }
                    ),
                    Err(ContextError::AccessDenied)
                ));
                assert!(log.operation_receipt(operation).unwrap().is_none());
                assert_eq!(
                    log.latest_checkpoint("session-a").unwrap().unwrap().pin,
                    first[0]
                );
                assert_eq!(&latest(&log, *scope, run).unwrap().pin, pin);
            }
        }

        let mut second = Vec::new();
        for (index, (scope, profile)) in cases.iter().enumerate() {
            let log = db.session_log(profile.clone()).unwrap();
            let run = profile.run.as_deref().unwrap();
            let input = CheckpointInput {
                scope: *scope,
                run: run.into(),
                expected_previous: Some(first[index].clone()),
                ..base.clone()
            };
            for wrong_previous in [None, Some(first[(index + 1) % cases.len()].clone())] {
                let operation = OperationId::new();
                assert!(matches!(
                    log.save_checkpoint(
                        operation,
                        CheckpointInput {
                            expected_previous: wrong_previous,
                            ..input.clone()
                        }
                    ),
                    Err(ContextError::InvalidInput(_))
                ));
                assert!(log.operation_receipt(operation).unwrap().is_none());
                assert_eq!(latest(&log, *scope, run).unwrap().pin, first[index]);
            }
            let next = log
                .save_checkpoint(OperationId::new(), input.clone())
                .unwrap()
                .pin;
            let stale = OperationId::new();
            assert!(matches!(
                log.save_checkpoint(stale, input),
                Err(ContextError::InvalidInput(_))
            ));
            assert!(log.operation_receipt(stale).unwrap().is_none());
            assert_eq!(latest(&log, *scope, run).unwrap().pin, next);
            second.push(next);
        }
        second
    };

    let db = Store::open(&path).unwrap();
    for (index, (scope, profile)) in cases.iter().enumerate() {
        let log = db.session_log(profile.clone()).unwrap();
        let header = latest(&log, *scope, profile.run.as_deref().unwrap()).unwrap();
        assert_eq!(header.pin, second[index]);
        let metadata = header.checkpoint.unwrap();
        assert_eq!(metadata.scope, *scope);
        assert_eq!(metadata.window, 2);
        assert_eq!(metadata.previous, Some(first[index].clone()));
        assert_eq!(metadata.sequence, 1);
        assert_eq!(header.provenance.parent_run, profile.parent_run);
        assert_eq!(
            log.read_checkpoint(&second[index]).unwrap().payload,
            base.payload
        );
        assert_eq!(
            log.read_checkpoint(&first[index])
                .unwrap()
                .header
                .checkpoint
                .unwrap()
                .window,
            1
        );
        assert_eq!(log.position("session-a").unwrap(), 1);
    }
}

#[test]
fn log_copies_stay_readable_after_their_sources_change() {
    let tmp = tempfile::tempdir().unwrap();
    let db = Store::create(tmp.path().join("context")).unwrap();
    let log = db.session_log(access("owner")).unwrap();
    let store = db.context(access("owner")).unwrap();
    let raw = source(&log, "evidence", "original observation");
    let old_type = store
        .define_type(OperationId::new(), definition("test.history-snapshot"))
        .unwrap()
        .pin;
    let old_object = store
        .save_object(OperationId::new(), object(&old_type, &raw, "old claim"))
        .unwrap()
        .pin;
    let current_object = store
        .revise_object(
            OperationId::new(),
            old_object.clone(),
            object(&old_type, &raw, "corrected claim"),
        )
        .unwrap()
        .pin;
    let mut revised_type = definition("test.history-snapshot");
    revised_type.properties.insert(
        "verified".into(),
        PropertyDefinition {
            value_type: PropertyType::Boolean,
            required: false,
        },
    );
    let current_type = store
        .revise_type(OperationId::new(), old_type.clone(), revised_type)
        .unwrap()
        .pin;
    assert_eq!(current_object.revision, 2);
    assert_eq!(current_type.revision, 2);

    let mut copies = Vec::new();
    for (index, original) in [&old_object, &old_type].into_iter().enumerate() {
        assert!(matches!(
            store.validate_current(std::slice::from_ref(original), None),
            Err(ContextError::Unavailable(_))
        ));
        let payload = store
            .read_payload(original, 0, MAX_RECORD_BYTES)
            .unwrap()
            .bytes;
        assert!(!payload.is_empty());
        let mut input = event_input("snapshot-session", &format!("copy-{index}"));
        input.kind = EventKind::Notice;
        input.sources = vec![original.clone()];
        let copy = log
            .append(OperationId::new(), input, payload.as_slice())
            .unwrap();
        assert_eq!(copy.sequence, Some(index as u64 + 1));
        assert_eq!(
            log.inspect_header(&copy.pin).unwrap().sources,
            vec![original.clone()]
        );
        assert_eq!(
            log.read_payload(&copy.pin, 0, MAX_RECORD_BYTES)
                .unwrap()
                .bytes,
            payload
        );
        copies.push((copy.pin, payload));
    }

    db.context(ContextAccess::new("owner", Actor::User))
        .unwrap()
        .purge(OperationId::new(), current_object)
        .unwrap();
    assert!(matches!(
        store.read(&old_object),
        Err(ContextError::Unavailable(_))
    ));
    for (pin, payload) in &copies {
        assert_eq!(
            log.read_payload(pin, 0, MAX_RECORD_BYTES).unwrap().bytes,
            *payload
        );
        assert!(matches!(
            store.validate_current(std::slice::from_ref(pin), None),
            Err(ContextError::Unavailable(_))
        ));
    }

    let mut blocked = event_input("snapshot-session", "copy-after-purge");
    blocked.kind = EventKind::Notice;
    blocked.sources = vec![old_object.clone(), old_type];
    blocked.expected_sequence = Some(2);
    let operation = OperationId::new();
    assert!(matches!(
        log.append(operation, blocked, copies[0].1.as_slice()),
        Err(ContextError::Unavailable(record)) if record == old_object.record
    ));
    assert!(log.operation_receipt(operation).unwrap().is_none());
    assert_eq!(log.position("snapshot-session").unwrap(), 2);
    assert!(log
        .events("snapshot-session", 2, 10)
        .unwrap()
        .entries
        .is_empty());
}

#[test]
fn history_copy_reserves_new_root_depth_before_committing_an_unreadable_record() {
    let tmp = tempfile::tempdir().unwrap();
    let db = Store::create(tmp.path().join("context")).unwrap();
    let log = db.session_log(access("owner")).unwrap();
    let mut chain = vec![source(&log, "root", "original evidence")];
    for depth in 1..=32 {
        let mut input = event_input("depth-session", &format!("depth-{depth}"));
        input.kind = EventKind::Notice;
        input.sources = vec![chain.last().unwrap().clone()];
        input.expected_sequence = Some(depth - 1);
        let copy = log
            .append(OperationId::new(), input, b"copied evidence".as_slice())
            .unwrap();
        assert_eq!(copy.sequence, Some(depth));
        chain.push(copy.pin);
    }
    assert_eq!(
        log.inspect_header(&chain[32]).unwrap().sources,
        vec![chain[31].clone()]
    );
    assert_eq!(
        log.read_payload(&chain[32], 0, 100).unwrap().bytes,
        b"copied evidence"
    );

    let mut input = event_input("depth-session", "overflow");
    input.kind = EventKind::Notice;
    input.sources = vec![chain[32].clone()];
    input.expected_sequence = Some(32);
    let operation = OperationId::new();
    assert!(matches!(
        log.append(operation, input.clone(), b"too deep".as_slice()),
        Err(ContextError::BudgetExceeded)
    ));
    assert!(log.operation_receipt(operation).unwrap().is_none());
    assert_eq!(log.position("depth-session").unwrap(), 32);
    assert!(log
        .events("depth-session", 32, 10)
        .unwrap()
        .entries
        .is_empty());

    // A rejected copy consumes neither its event identity nor its operation.
    input.sources = vec![chain[31].clone()];
    let accepted = log
        .append(operation, input, b"within limit".as_slice())
        .unwrap();
    assert_eq!(accepted.sequence, Some(33));
    assert_eq!(
        log.operation_receipt(operation).unwrap(),
        Some(accepted.clone())
    );
    assert_eq!(
        log.read_payload(&accepted.pin, 0, 100).unwrap().bytes,
        b"within limit"
    );
}

#[test]
fn tool_results_stay_readable_after_their_call_source_is_purged() {
    let temp = tempfile::tempdir().unwrap();
    let db = Store::create(temp.path().join("context")).unwrap();
    let log = db.session_log(access("owner")).unwrap();
    let store = db.context(access("owner")).unwrap();
    let raw = source(&log, "source", "old observation");
    let kind = store
        .define_type(OperationId::new(), definition("test.revision-result"))
        .unwrap()
        .pin;
    let old = store
        .save_object(OperationId::new(), object(&kind, &raw, "old value"))
        .unwrap()
        .pin;
    let mut call = event_input("session-a", "update-call");
    call.kind = EventKind::ToolCall {
        message_id: None,
        call_id: "update-object".into(),
        tool_name: "context".into(),
    };
    call.sources = vec![old.clone()];
    let call = log
        .append(
            OperationId::new(),
            call,
            b"update to a new revision".as_slice(),
        )
        .unwrap();
    let current = store
        .revise_object(OperationId::new(), old, object(&kind, &raw, "new value"))
        .unwrap()
        .pin;
    assert!(log.read_payload(&call.pin, 0, 100).is_ok());
    let mut result = event_input("session-a", "update-result");
    result.kind = EventKind::ToolResult {
        call_id: "update-object".into(),
        outcome: ToolOutcome::Succeeded,
    };
    let result = log
        .append(OperationId::new(), result, b"revision committed".as_slice())
        .unwrap();
    assert_eq!(
        log.read_payload(&result.pin, 0, 100).unwrap().bytes,
        b"revision committed"
    );
    let user_log = db
        .session_log(ContextAccess::new("owner", Actor::User))
        .unwrap();
    let user = db
        .context(ContextAccess::new("owner", Actor::User))
        .unwrap();
    user.purge(OperationId::new(), current).unwrap();
    for (pin, payload) in [
        (&call.pin, b"update to a new revision".as_slice()),
        (&result.pin, b"revision committed".as_slice()),
    ] {
        assert_eq!(log.read_payload(pin, 0, 100).unwrap().bytes, payload);
        assert!(matches!(
            store.validate_current(std::slice::from_ref(pin), None),
            Err(ContextError::Unavailable(_))
        ));
    }
    user_log.purge(OperationId::new(), call.pin).unwrap();
    assert!(matches!(
        log.read_payload(&result.pin, 0, 100),
        Err(ContextError::Unavailable(_))
    ));
}

#[test]
fn history_copy_shares_the_aggregate_source_budget_including_its_new_root() {
    let tmp = tempfile::tempdir().unwrap();
    let db = Store::create(tmp.path().join("context")).unwrap();
    let log = db.session_log(access("owner")).unwrap();
    let raw = source(&log, "root", "original evidence");
    let mut wide = event_input("budget-session", "wide");
    wide.kind = EventKind::Notice;
    wide.sources = vec![raw.clone(); 64];
    let wide = log
        .append(OperationId::new(), wide, b"wide snapshot".as_slice())
        .unwrap()
        .pin;

    // Each branch visits 65 records; 1 + 63 * 65 is exactly 4096.
    let mut boundary = event_input("budget-session", "boundary");
    boundary.kind = EventKind::Notice;
    boundary.sources = vec![wide.clone(); 63];
    let boundary = log
        .append(OperationId::new(), boundary, b"at limit".as_slice())
        .unwrap();
    assert_eq!(
        log.read_payload(&boundary.pin, 0, 100).unwrap().bytes,
        b"at limit"
    );

    let mut overflow = event_input("budget-session", "overflow");
    overflow.kind = EventKind::Notice;
    overflow.sources = vec![wide; 63];
    overflow.sources.push(raw);
    overflow.expected_sequence = Some(2);
    let operation = OperationId::new();
    assert!(matches!(
        log.append(operation, overflow, b"over limit".as_slice()),
        Err(ContextError::BudgetExceeded)
    ));
    assert!(log.operation_receipt(operation).unwrap().is_none());
    assert_eq!(log.position("budget-session").unwrap(), 2);
    assert!(log
        .events("budget-session", 2, 10)
        .unwrap()
        .entries
        .is_empty());
}

#[test]
fn a_log_copy_needs_the_scopes_of_its_sources() {
    let tmp = tempfile::tempdir().unwrap();
    let db = Store::create(tmp.path().join("context")).unwrap();
    let workspace = Scope::Workspace(ulid::Ulid::new());
    let mut authorized = access("owner");
    authorized.scopes.push(workspace.clone());
    let log = db.session_log(authorized.clone()).unwrap();
    let store = db.context(authorized).unwrap();
    let raw = source(&log, "raw", "personal detail");
    let mut input = event_input("work", "copy");
    input.scope = workspace.clone();
    input.sources = vec![raw];
    let copy = log
        .append(OperationId::new(), input.clone(), b"first".as_slice())
        .unwrap()
        .pin;
    input.event_id = "second".into();
    input.sources = vec![copy];
    let nested = log
        .append(OperationId::new(), input, b"second".as_slice())
        .unwrap()
        .pin;
    let checkpoint = log
        .save_checkpoint(
            OperationId::new(),
            CheckpointInput {
                session: "work".into(),
                run: "run".into(),
                scope: CheckpointScope::Session,
                sequence: 2,
                expected_previous: None,
                expected_availability_epoch: store.availability_epoch().unwrap(),
                sources: vec![],
                payload: json!("summary"),
            },
        )
        .unwrap()
        .pin;
    let mut restricted = access("owner");
    restricted.scopes = vec![workspace];
    let restricted_log = db.session_log(restricted.clone()).unwrap();
    let restricted = db.context(restricted).unwrap();
    for pin in [&nested, &checkpoint] {
        assert_eq!(
            log.inspect_header(pin).unwrap().source_scopes,
            vec![Scope::Personal]
        );
        assert!(matches!(
            restricted_log.read_payload(pin, 0, 100),
            Err(ContextError::Unavailable(_))
        ));
        assert!(matches!(
            restricted.validate_current(std::slice::from_ref(pin), None),
            Err(ContextError::Unavailable(_))
        ));
    }
}

#[test]
fn purging_a_log_event_hides_what_was_derived_from_it() {
    let tmp = tempfile::tempdir().unwrap();
    let db = Store::create(tmp.path().join("context")).unwrap();
    let log = db
        .session_log(ContextAccess::new("owner", Actor::Operator))
        .unwrap();
    let store = db
        .context(ContextAccess::new("owner", Actor::Operator))
        .unwrap();
    let raw = source(&log, "raw", "original");
    let independent = source(&log, "other", "another observation");
    let kind = store
        .define_type(OperationId::new(), definition("note"))
        .unwrap()
        .pin;
    let a = store
        .save_object(OperationId::new(), object(&kind, &raw, "derived"))
        .unwrap()
        .pin;
    let b = store
        .save_object(
            OperationId::new(),
            object(&kind, &independent, "independent"),
        )
        .unwrap()
        .pin;
    let link = store
        .save_relation(
            OperationId::new(),
            relation(&relation_type(&store, &kind), &a, &b, &raw),
        )
        .unwrap()
        .pin;
    let checkpoint = log
        .save_checkpoint(
            OperationId::new(),
            CheckpointInput {
                scope: CheckpointScope::Session,
                session: "session-a".into(),
                run: "run".into(),
                sequence: 2,
                expected_previous: None,
                expected_availability_epoch: store.availability_epoch().unwrap(),
                sources: vec![raw.clone()],
                payload: json!("summary"),
            },
        )
        .unwrap()
        .pin;
    log.purge(OperationId::new(), raw).unwrap();
    for pin in [&a, &link] {
        assert!(matches!(
            store.read_payload(pin, 0, 100),
            Err(ContextError::Unavailable(_))
        ));
    }
    assert!(matches!(
        log.read_checkpoint(&checkpoint),
        Err(ContextError::Unavailable(_))
    ));
    assert!(store.read(&b).is_ok());
}

#[test]
fn a_withdrawn_fragment_hides_its_whole_message() {
    let tmp = tempfile::tempdir().unwrap();
    let db = Store::create(tmp.path().join("context")).unwrap();
    let log = db
        .session_log(ContextAccess::new("owner", Actor::Operator))
        .unwrap();
    let message_id = "message".to_string();
    let kinds = [
        EventKind::AssistantStarted {
            message_id: message_id.clone(),
        },
        EventKind::AssistantFragment {
            message_id: message_id.clone(),
            ordinal: 0,
        },
        EventKind::AssistantFragment {
            message_id: message_id.clone(),
            ordinal: 1,
        },
        EventKind::ToolCall {
            message_id: Some(message_id.clone()),
            call_id: "call".into(),
            tool_name: "read".into(),
        },
        EventKind::ToolResult {
            call_id: "call".into(),
            outcome: ToolOutcome::Succeeded,
        },
        EventKind::AssistantTerminal {
            message_id,
            status: MessageStatus::Completed,
        },
    ];
    let mut pins = Vec::new();
    for (index, kind) in kinds.into_iter().enumerate() {
        let mut input = event_input("session-a", &index.to_string());
        input.kind = kind;
        pins.push(
            log.append(OperationId::new(), input, b"event".as_slice())
                .unwrap()
                .pin,
        );
    }
    log.retract(OperationId::new(), pins[1].clone()).unwrap();
    for pin in pins {
        assert!(matches!(
            log.read_payload(&pin, 0, 100),
            Err(ContextError::Unavailable(_))
        ));
    }
}

#[test]
fn a_legacy_directory_resets_into_an_empty_store() {
    use mmdb::store_format::{commit_reset, ResetPlan, ResetSafety, StoreEraId, StoreManifest};
    use std::time::{Duration, SystemTime};
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().canonicalize().unwrap().join("legacy");
    std::fs::create_dir(&root).unwrap();
    let legacy = "mmdb-context-v1";
    StoreManifest::new(legacy, StoreEraId::new())
        .unwrap()
        .write_new(&root)
        .unwrap();
    std::fs::write(root.join("old-data"), b"previous contents").unwrap();
    let safety = ResetSafety::for_current_process(Vec::new()).unwrap();
    let plan = ResetPlan::build(
        std::slice::from_ref(&root),
        legacy,
        STORE_FORMAT_ID,
        SystemTime::now(),
        Duration::from_secs(60),
        &safety,
    )
    .unwrap();
    let receipt = commit_reset(&plan, plan.digest().as_str(), StoreEraId::new(), &[]).unwrap();
    let store = Store::open(&root).unwrap();
    let log = store.session_log(access("owner")).unwrap();
    let context = store.context(access("owner")).unwrap();
    assert!(log.sessions(10, None).unwrap().sessions.is_empty());
    assert!(context
        .query_current_valid(None, None, 10)
        .unwrap()
        .is_empty());
    assert_eq!(context.availability_epoch().unwrap(), 0);
    assert!(!root.join("old-data").exists());
    assert_eq!(
        std::fs::read(receipt.quarantine_path().join("old-data")).unwrap(),
        b"previous contents"
    );
}

#[test]
fn session_log_and_context_reject_the_same_invalid_access() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::create(dir.path().join("store")).unwrap();
    let mut empty_scopes = access("owner");
    empty_scopes.scopes.clear();
    let mut bad_parent = access("owner");
    bad_parent.parent_run = Some("without-run".into());
    for invalid in [access(" "), empty_scopes, bad_parent] {
        let (Err(ContextError::InvalidInput(context)), Err(ContextError::InvalidInput(log))) =
            (store.context(invalid.clone()), store.session_log(invalid))
        else {
            panic!("both handles must reject invalid access");
        };
        assert_eq!(context, log);
    }
}

#[test]
fn access_bound_to_a_session_appends_only_to_it() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::create(dir.path().join("store")).unwrap();
    let mut profile = access("owner");
    profile.session = Some("bound".into());
    let log = store.session_log(profile).unwrap();
    let id = OperationId::new();
    assert!(matches!(
        log.append(id, event_input("other", "event"), &b"text"[..]),
        Err(ContextError::AccessDenied)
    ));
    assert_eq!(log.position("other").unwrap(), 0);
    assert!(log.operation_receipt(id).unwrap().is_none());
    let receipt = log
        .append(id, event_input("bound", "event"), &b"text"[..])
        .unwrap();
    assert_eq!(receipt.sequence, Some(1));
}

#[test]
fn log_permissions_follow_the_actor() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::create(dir.path().join("store")).unwrap();
    for actor in [
        Actor::User,
        Actor::Assistant,
        Actor::System,
        Actor::Operator,
    ] {
        let owner = format!("{actor:?}");
        let log = store
            .session_log(ContextAccess::new(&owner, actor))
            .unwrap();
        let message = log.append(OperationId::new(), event_input("s", "user"), &b"said"[..]);
        if actor == Actor::Assistant {
            assert!(matches!(message, Err(ContextError::AccessDenied)));
        } else {
            message.unwrap();
        }
        let sessions = log.sessions(5, None);
        if matches!(actor, Actor::System | Actor::Operator) {
            assert_eq!(sessions.unwrap().sessions, ["s"]);
        } else {
            assert!(matches!(sessions, Err(ContextError::AccessDenied)));
        }
        let writer = store.session_log(access(&owner)).unwrap();
        let first = source(&writer, "retract", "first");
        let second = source(&writer, "purge", "second");
        for result in [
            log.retract(OperationId::new(), first),
            log.purge(OperationId::new(), second),
        ] {
            if matches!(actor, Actor::User | Actor::Operator) {
                result.unwrap();
            } else {
                assert!(matches!(result, Err(ContextError::AccessDenied)));
            }
        }
    }
}

fn log_query() -> LogQuery {
    LogQuery {
        text: "hit".into(),
        session: None,
        cursor: None,
        max_events: 4096,
        max_bytes: 16 * 1024 * 1024,
        limit: 100,
    }
}

#[test]
fn log_reads_and_searches_refuse_budgets_over_their_limits() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::create(dir.path().join("store")).unwrap();
    let log = store.session_log(access("owner")).unwrap();
    let pin = source(&log, "event", "hit");
    assert_eq!(
        log.read_payload(&pin, 0, MAX_PAYLOAD_PAGE_BYTES)
            .unwrap()
            .bytes,
        b"hit"
    );
    assert!(matches!(
        log.read_payload(&pin, 0, MAX_PAYLOAD_PAGE_BYTES + 1),
        Err(ContextError::InvalidInput(_))
    ));
    assert_eq!(log.search(log_query()).unwrap().matches.len(), 1);
    for invalid in [
        LogQuery {
            max_events: 4097,
            ..log_query()
        },
        LogQuery {
            max_bytes: 16 * 1024 * 1024 + 1,
            ..log_query()
        },
        LogQuery {
            limit: 101,
            ..log_query()
        },
    ] {
        assert!(matches!(
            log.search(invalid),
            Err(ContextError::InvalidInput(_))
        ));
    }
}

#[test]
fn search_walks_sessions_in_key_order_and_events_in_sequence() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::create(dir.path().join("store")).unwrap();
    let log = store.session_log(access("owner")).unwrap();
    for session in ["bb", "z", "aa", "z", "bb", "aa"] {
        let seq = log.position(session).unwrap() + 1;
        log.append(
            OperationId::new(),
            event_input(session, &seq.to_string()),
            &b"hit"[..],
        )
        .unwrap();
    }
    let mut query = LogQuery {
        limit: 1,
        ..log_query()
    };
    let mut positions = Vec::new();
    loop {
        let page = log.search(query.clone()).unwrap();
        assert_eq!(page.matches.len(), 1);
        let entry = page
            .matches
            .into_iter()
            .next()
            .unwrap()
            .header
            .event
            .unwrap();
        positions.push((entry.session, entry.sequence));
        if page.cursor.is_none() {
            break;
        }
        query.cursor = page.cursor;
        assert!(positions.len() < 6);
    }
    // Session keys contain a length prefix before the session bytes.
    assert_eq!(
        positions,
        ["z", "aa", "bb"]
            .into_iter()
            .flat_map(|s| [(s.into(), 1), (s.into(), 2)])
            .collect::<Vec<_>>()
    );
}

#[test]
fn recall_reaches_through_log_events_but_returns_memory_records() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::create(dir.path().join("store")).unwrap();
    let log = store.session_log(access("owner")).unwrap();
    let context = store.context(access("owner")).unwrap();
    let event = source(&log, "event", "raw");
    let ty = context
        .define_type(OperationId::new(), definition("note"))
        .unwrap()
        .pin;
    let a = context
        .save_object(OperationId::new(), object(&ty, &event, "alpha"))
        .unwrap()
        .pin;
    let b = context
        .save_object(OperationId::new(), object(&ty, &event, "beta"))
        .unwrap()
        .pin;
    let edge_type = context
        .define_type(
            OperationId::new(),
            TypeDefinition {
                name: "link".into(),
                scope: Scope::Personal,
                kind: TypeKind::Relation {
                    from_types: vec![],
                    to_types: vec![],
                },
                properties: BTreeMap::new(),
            },
        )
        .unwrap()
        .pin;
    let edge = |from, to| {
        context
            .save_relation(OperationId::new(), relation(&edge_type, from, to, &event))
            .unwrap()
            .pin
    };
    let first = edge(&a, &event);
    let second = edge(&event, &b);
    for (seed, path) in [(&a, vec![first, second.clone()]), (&event, vec![second])] {
        let found = context
            .recall(ContextQuery {
                seeds: vec![seed.record.clone()],
                ..query("")
            })
            .unwrap();
        assert!(found
            .hits
            .iter()
            .all(|hit| hit.record.header.pin.record.kind.module() == StoreModule::Memory));
        assert_eq!(
            found
                .hits
                .iter()
                .find(|hit| hit.record.header.pin == b)
                .unwrap()
                .path,
            path
        );
    }
}

#[test]
fn one_operation_id_names_one_write_across_the_log_and_context() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::create(dir.path().join("store")).unwrap();
    let log = store.session_log(access("owner")).unwrap();
    let context = store.context(access("owner")).unwrap();
    let id = OperationId::new();
    let receipt = log
        .append(id, event_input("s", "first"), &b"text"[..])
        .unwrap();
    assert!(matches!(
        context.define_type(id, definition("note")),
        Err(ContextError::IdempotencyConflict)
    ));
    assert_eq!(context.operation_receipt(id).unwrap(), Some(receipt));
    let id = OperationId::new();
    let receipt = context.define_type(id, definition("note")).unwrap();
    assert!(matches!(
        log.append(id, event_input("s", "second"), &b"text"[..]),
        Err(ContextError::IdempotencyConflict)
    ));
    assert_eq!(log.operation_receipt(id).unwrap(), Some(receipt));
    assert_eq!(log.position("s").unwrap(), 1);
}

#[test]
fn record_handles_reject_another_kind_before_checking_access() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::create(dir.path().join("store")).unwrap();
    let log = store.session_log(access("owner")).unwrap();
    let context = store.context(access("owner")).unwrap();
    let event = source(&log, "event", "text");
    let memory = context
        .define_type(OperationId::new(), definition("note"))
        .unwrap()
        .pin;
    let foreign = store.context(access("another-owner")).unwrap();
    let foreign_log = store.session_log(access("another-owner")).unwrap();
    for rejected in [
        foreign.head(&event.record).map(|_| ()),
        foreign.inspect_header(&event).map(|_| ()),
        foreign.read(&event).map(|_| ()),
        foreign.read_payload(&event, 0, 32).map(|_| ()),
        foreign
            .retract(OperationId::new(), event.clone())
            .map(|_| ()),
        foreign.purge(OperationId::new(), event.clone()).map(|_| ()),
        foreign_log.head(&memory.record).map(|_| ()),
        foreign_log.inspect_header(&memory).map(|_| ()),
        foreign_log.read_payload(&memory, 0, 32).map(|_| ()),
        foreign_log.read_checkpoint(&event).map(|_| ()),
        foreign_log.read_checkpoint(&memory).map(|_| ()),
        foreign_log
            .retract(OperationId::new(), memory.clone())
            .map(|_| ()),
        foreign_log
            .purge(OperationId::new(), memory.clone())
            .map(|_| ()),
    ] {
        assert!(matches!(rejected, Err(ContextError::InvalidInput(_))));
    }
}
