use mmdb::context::*;
use mmdb::native_memory::{Actor, MemoryDatabase, OperationId, RecordState, Scope, TemporalFacts};
use serde_json::json;
use std::collections::{BTreeMap, BTreeSet};
use std::io::{self, Read};
use std::sync::Arc;

fn access(owner: &str) -> ContextAccess {
    ContextAccess::new(owner, Actor::System)
}

fn history_input(session: &str, event: &str) -> HistoryInput {
    HistoryInput {
        sources: Vec::new(),
        session: session.into(),
        event_id: event.into(),
        scope: Scope::Personal,
        kind: HistoryKind::UserMessage,
        media_type: "text/plain; charset=utf-8".into(),
        occurred_at_ms: 10,
        expected_sequence: None,
    }
}

fn source(context: &ContextStore<'_>, event: &str, text: &str) -> RecordPin {
    context
        .append_history(
            OperationId::new(),
            history_input("session-a", event),
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
        let db = MemoryDatabase::create_context(&root).unwrap();
        let research = db.context(access("owner")).unwrap();
        let raw = source(
            &research,
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
    let db = MemoryDatabase::open_context(&root).unwrap();
    let mut trader_access = access("owner");
    trader_access.actor = Actor::Assistant;
    trader_access.agent = Some("trading".into());
    trader_access.session = Some("session-b".into());
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
    let bytes = trading.read_payload(&raw, 0, 256).unwrap().bytes;
    assert_eq!(
        String::from_utf8(bytes).unwrap(),
        "Research found a capacity constraint that affects the decision."
    );
    assert_eq!(trading.history_position("session-a").unwrap(), 1);
    assert_eq!(trading.history_position("session-b").unwrap(), 0);
}

#[test]
fn type_versions_are_reused_and_old_instances_keep_their_schema() {
    let dir = tempfile::tempdir().unwrap();
    let db = MemoryDatabase::create_context(dir.path().join("context")).unwrap();
    let context = db.context(access("owner")).unwrap();
    let raw = source(&context, "raw", "A bounded observation");
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
    let db = MemoryDatabase::create_context(dir.path().join("context")).unwrap();
    let alice = db.context(access("alice")).unwrap();
    let bob = db.context(access("bob")).unwrap();
    let raw = source(&alice, "a", "Alice's evidence");
    let alice_type = alice
        .define_type(OperationId::new(), definition("note"))
        .unwrap()
        .pin;
    let bob_type = bob
        .define_type(OperationId::new(), definition("note"))
        .unwrap()
        .pin;
    assert!(matches!(bob.read(&raw), Err(ContextError::AccessDenied)));
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
    let db = MemoryDatabase::create_context(dir.path().join("context")).unwrap();
    let private = Scope::Session(ulid::Ulid::new());
    let mut permitted = access("owner");
    permitted.scopes.push(private.clone());
    let all = db.context(permitted).unwrap();
    let mut raw_input = history_input("private", "raw");
    raw_input.scope = private.clone();
    let raw = all
        .append_history(OperationId::new(), raw_input, b"private fact".as_slice())
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
        let db = MemoryDatabase::create_context(&root).unwrap();
        db.context(access("owner"))
            .unwrap()
            .append_history(
                OperationId::new(),
                history_input("long", "large"),
                bytes.as_slice(),
            )
            .unwrap()
            .pin
    };
    let db = MemoryDatabase::open_context(&root).unwrap();
    let context = db.context(access("owner")).unwrap();
    let mut restored = Vec::new();
    let mut offset = 0;
    loop {
        let page = context.read_payload(&pin, offset, 65_537).unwrap();
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
            let result = context
                .search_history(HistoryQuery {
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
    let db = MemoryDatabase::create_context(dir.path().join("context")).unwrap();
    let context = db.context(access("owner")).unwrap();
    let operation = OperationId::new();
    let mut input = history_input("session", "first");
    input.expected_sequence = Some(0);
    let first = context
        .append_history(operation, input.clone(), b"first".as_slice())
        .unwrap();
    context
        .append_history(
            OperationId::new(),
            history_input("session", "second"),
            b"second".as_slice(),
        )
        .unwrap();
    assert_eq!(
        context
            .append_history(operation, input.clone(), b"first".as_slice())
            .unwrap(),
        first
    );
    input.expected_sequence = Some(2);
    assert_eq!(
        context
            .append_history(OperationId::new(), input.clone(), b"first".as_slice())
            .unwrap(),
        first
    );
    assert!(matches!(
        context.append_history(OperationId::new(), input, b"different".as_slice()),
        Err(ContextError::IdempotencyConflict)
    ));
    let page = context.history("session", 0, 1).unwrap();
    assert_eq!(page.entries[0].pin, first.pin);
    assert_eq!(page.next_sequence, Some(1));
    let second = context.history("session", 1, 1).unwrap();
    assert_eq!(second.entries[0].history.as_ref().unwrap().sequence, 2);
    assert!(second.next_sequence.is_none());
    assert_eq!(context.operation_receipt(operation).unwrap(), Some(first));
}

#[test]
fn concurrent_appends_allocate_one_session_order() {
    let dir = tempfile::tempdir().unwrap();
    let db = Arc::new(MemoryDatabase::create_context(dir.path().join("context")).unwrap());
    let jobs: Vec<_> = (0..8)
        .map(|index| {
            let db = db.clone();
            std::thread::spawn(move || {
                db.context(access("owner"))
                    .unwrap()
                    .append_history(
                        OperationId::new(),
                        history_input("shared", &format!("event-{index}")),
                        format!("item-{index}").as_bytes(),
                    )
                    .unwrap()
            })
        })
        .collect();
    let receipts: Vec<_> = jobs.into_iter().map(|job| job.join().unwrap()).collect();
    let positions: BTreeSet<_> = receipts
        .iter()
        .map(|receipt| receipt.history_sequence.unwrap())
        .collect();
    assert_eq!(positions, (1..=8).collect());
    let context = db.context(access("owner")).unwrap();
    let history = context.history("shared", 0, 100).unwrap();
    assert_eq!(history.entries.len(), 8);
    let mut stale = history_input("shared", "stale");
    stale.expected_sequence = Some(0);
    assert!(matches!(
        context.append_history(OperationId::new(), stale, b"stale".as_slice()),
        Err(ContextError::HistoryConflict {
            expected: 0,
            actual: 8
        })
    ));
}

#[test]
fn tool_results_require_their_call_and_follow_its_purge_state() {
    let dir = tempfile::tempdir().unwrap();
    let db = MemoryDatabase::create_context(dir.path().join("context")).unwrap();
    let context = db
        .context(ContextAccess::new("owner", Actor::Operator))
        .unwrap();
    let mut result = history_input("session", "result");
    result.kind = HistoryKind::ToolResult {
        call_id: "call-1".into(),
        outcome: ToolOutcome::Succeeded,
    };
    assert!(matches!(
        context.append_history(OperationId::new(), result.clone(), b"response".as_slice()),
        Err(ContextError::InvalidInput(_))
    ));
    let mut call = history_input("session", "call");
    call.kind = HistoryKind::ToolCall {
        message_id: None,
        call_id: "call-1".into(),
        tool_name: "research".into(),
    };
    let call = context
        .append_history(OperationId::new(), call, b"arguments".as_slice())
        .unwrap();
    let result = context
        .append_history(OperationId::new(), result, b"response".as_slice())
        .unwrap();
    assert_eq!(
        context.read(&result.pin).unwrap().header.sources,
        vec![call.pin.clone()]
    );
    context.purge(OperationId::new(), call.pin).unwrap();
    assert!(matches!(
        context.read_payload(&result.pin, 0, 10),
        Err(ContextError::Unavailable(_))
    ));
}

#[test]
fn purged_sources_stay_unavailable_after_reopen_recovery_and_replay() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("context");
    let operation = OperationId::new();
    let (raw, note, input) = {
        let db = MemoryDatabase::create_context(&root).unwrap();
        let context = db.context(access("owner")).unwrap();
        let raw = source(&context, "raw", "sensitive observation");
        let type_pin = context
            .define_type(OperationId::new(), definition("note"))
            .unwrap()
            .pin;
        let input = object(&type_pin, &raw, "sensitive inference");
        let note = context.save_object(operation, input.clone()).unwrap().pin;
        let operator = db
            .context(ContextAccess::new("owner", Actor::Operator))
            .unwrap();
        operator.purge(OperationId::new(), raw.clone()).unwrap();
        assert!(matches!(
            context.read(&note),
            Err(ContextError::Unavailable(_))
        ));
        (raw, note, input)
    };
    let db = MemoryDatabase::open_context(&root).unwrap();
    let context = db.context(access("owner")).unwrap();
    assert_eq!(context.save_object(operation, input).unwrap().pin, note);
    assert!(context.recall(query("sensitive")).unwrap().hits.is_empty());
    assert!(matches!(
        context.read_payload(&raw, 0, 10),
        Err(ContextError::Unavailable(_))
    ));
    let report = context.recover_payloads(1, 1).unwrap();
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
    let db = MemoryDatabase::create_context(dir.path().join("context")).unwrap();
    let context = db.context(access("owner")).unwrap();
    let raw = source(&context, "raw", "A correction source");
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
    let db = MemoryDatabase::create_context(dir.path().join("context")).unwrap();
    let store = db.context(access("owner")).unwrap();
    let raw = source(&store, "raw", "observation");
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
    let db = MemoryDatabase::create_context(dir.path().join("context")).unwrap();
    let context = db.context(access("owner")).unwrap();
    let raw = source(&context, "raw", "source");
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
    let db = MemoryDatabase::create_context(dir.path().join("context")).unwrap();
    let private = Scope::Session(ulid::Ulid::new());
    let mut permitted = access("owner");
    permitted.scopes.push(private.clone());
    let all = db.context(permitted).unwrap();
    let raw = source(&all, "raw", "evidence");
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
        let db = MemoryDatabase::create_context(&root).unwrap();
        source(&db.context(access("owner")).unwrap(), "old", "old source")
    };
    let safety = ResetSafety::for_current_process(Vec::new()).unwrap();
    let plan = ResetPlan::build(
        std::slice::from_ref(&root),
        CONTEXT_STORE_FORMAT_ID,
        SystemTime::now(),
        Duration::from_secs(60),
        &safety,
    )
    .unwrap();
    let receipt = commit_reset(&plan, plan.digest().as_str(), StoreEraId::new(), &[]).unwrap();
    let db = MemoryDatabase::open_context(&root).unwrap();
    let fresh = db.context(access("owner")).unwrap();
    assert_eq!(fresh.history_position("session-a").unwrap(), 0);
    assert!(matches!(fresh.read(&old), Err(ContextError::AccessDenied)));
    let previous = MemoryDatabase::open_context(receipt.quarantine_path()).unwrap();
    assert_eq!(
        previous
            .context(access("owner"))
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
    let db = MemoryDatabase::create_context(dir.path().join("context")).unwrap();
    let context = db.context(access("owner")).unwrap();
    let raw = source(&context, "raw", "项目需要考虑交易日窗口。");
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
        let db = MemoryDatabase::create_context(&root).unwrap();
        let context = db.context(access("owner")).unwrap();
        assert!(matches!(
            context.append_history(
                operation,
                history_input("session", "interrupted"),
                FailingReader {
                    remaining: PAYLOAD_CHUNK_BYTES + 5
                }
            ),
            Err(ContextError::Io(_))
        ));
        assert_eq!(context.history_position("session").unwrap(), 0);
        assert!(context.operation_receipt(operation).unwrap().is_none());
    }
    let db = MemoryDatabase::open_context(&root).unwrap();
    let context = db.context(access("owner")).unwrap();
    assert!(context
        .history("session", 0, 10)
        .unwrap()
        .entries
        .is_empty());
    let report = context.recover_payloads(10, 10).unwrap();
    assert_eq!(report.chunks_removed, 1);
    assert_eq!(report.uploads_removed, 1);
    assert_eq!(
        context
            .append_history(
                operation,
                history_input("session", "interrupted"),
                b"complete retry".as_slice()
            )
            .unwrap()
            .history_sequence,
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
    let db = MemoryDatabase::create_context(root).unwrap();
    db.context(access("owner"))
        .unwrap()
        .append_history(
            OperationId::new(),
            history_input("session", "crashed"),
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
    let db = MemoryDatabase::open_context(&root).unwrap();
    let context = db.context(access("owner")).unwrap();
    assert_eq!(context.history_position("session").unwrap(), 0);
    assert_eq!(context.recover_payloads(10, 10).unwrap().chunks_removed, 1);
    assert_eq!(
        context
            .append_history(
                OperationId::new(),
                history_input("session", "after-crash"),
                b"resumed".as_slice()
            )
            .unwrap()
            .history_sequence,
        Some(1)
    );
}

#[test]
fn format_and_store_identity_prevent_accidental_adoption() {
    let dir = tempfile::tempdir().unwrap();
    let legacy = dir.path().join("legacy");
    {
        let db = MemoryDatabase::create(&legacy).unwrap();
        assert!(matches!(
            db.context(access("owner")),
            Err(ContextError::UnsupportedFormat)
        ));
    }
    assert!(MemoryDatabase::open_context(&legacy).is_err());
    let first = MemoryDatabase::create_context(dir.path().join("first")).unwrap();
    let pin = source(&first.context(access("owner")).unwrap(), "raw", "one era");
    let second = MemoryDatabase::create_context(dir.path().join("second")).unwrap();
    assert!(matches!(
        second.context(access("owner")).unwrap().read(&pin),
        Err(ContextError::AccessDenied)
    ));
}

#[test]
fn source_validation_reserves_the_new_root_depth_and_aggregate_budget() {
    let tmp = tempfile::tempdir().unwrap();
    let db = MemoryDatabase::create_context(tmp.path().join("context")).unwrap();
    let store = db.context(access("owner")).unwrap();
    let root = source(&store, "source", "evidence");
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
fn action_contract_enforces_authority_versions_conditions_and_one_preparation() {
    let tmp = tempfile::tempdir().unwrap();
    let db = Arc::new(MemoryDatabase::create_context(tmp.path().join("context")).unwrap());
    let store = db.context(access("owner")).unwrap();
    let evidence = source(&store, "source", "milestone instruction");
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
    let db = MemoryDatabase::create_context(tmp.path().join("context")).unwrap();
    let store = db.context(access("owner")).unwrap();
    let original = source(&store, "source", "withdrawable-secret");
    let mut copied = history_input("model-session", "model-request");
    copied.kind = HistoryKind::Notice;
    copied.sources = vec![original.clone()];
    let copy = store
        .append_history(
            OperationId::new(),
            copied,
            b"withdrawable-secret copied".as_slice(),
        )
        .unwrap();
    assert!(store.read_payload(&copy.pin, 0, 100).is_ok());
    db.context(ContextAccess::new("owner", Actor::User))
        .unwrap()
        .purge(OperationId::new(), original)
        .unwrap();
    assert!(matches!(
        store.read_payload(&copy.pin, 0, 100),
        Err(ContextError::Unavailable(_))
    ));
    for index in 0..7 {
        store
            .append_history(
                OperationId::new(),
                history_input(&format!("session-{index}"), "input"),
                b"original".as_slice(),
            )
            .unwrap();
    }
    let mut sessions = BTreeSet::new();
    let mut after = None;
    loop {
        let page = store.history_sessions(2, after.as_deref()).unwrap();
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
        .context(access("another-owner"))
        .unwrap()
        .history_sessions(2, None)
        .unwrap()
        .sessions
        .is_empty());
    assert!(matches!(
        db.context(ContextAccess::new("owner", Actor::Assistant))
            .unwrap()
            .history_sessions(2, None),
        Err(ContextError::AccessDenied)
    ));
}

#[test]
fn current_validation_rejects_changed_dependencies_without_breaking_historical_reads() {
    let tmp = tempfile::tempdir().unwrap();
    let db = MemoryDatabase::create_context(tmp.path().join("context")).unwrap();
    let store = db.context(access("owner")).unwrap();
    let root = source(&store, "original", "original instruction");
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
    let db = MemoryDatabase::create_context(&path).unwrap();
    let store = db.context(access("owner")).unwrap();
    let anchor = source(&store, "event", "original");
    let input = CheckpointInput {
        scope: CheckpointScope::Session,
        session: "session-a".into(),
        run: "run".into(),
        history_sequence: 1,
        expected_previous: None,
        expected_availability_epoch: store.availability_epoch().unwrap(),
        sources: vec![anchor.clone()],
        payload: json!({"goal":"continue"}),
    };
    let first = store
        .save_checkpoint(OperationId::new(), input.clone())
        .unwrap();
    assert!(store.save_checkpoint(OperationId::new(), input).is_err());
    let tail = source(&store, "tail", "new message");
    assert_eq!(
        store.history("session-a", 1, 10).unwrap().entries[0].pin,
        tail
    );
    let old_epoch = store.availability_epoch().unwrap();
    db.context(ContextAccess::new("owner", Actor::User))
        .unwrap()
        .purge(OperationId::new(), anchor)
        .unwrap();
    assert!(matches!(
        store.read(&first.pin),
        Err(ContextError::Unavailable(_))
    ));
    assert!(store
        .save_checkpoint(
            OperationId::new(),
            CheckpointInput {
                scope: CheckpointScope::Session,
                session: "session-a".into(),
                run: "run".into(),
                history_sequence: 2,
                expected_previous: Some(first.pin.clone()),
                expected_availability_epoch: old_epoch,
                sources: vec![tail],
                payload: json!({})
            }
        )
        .is_err());
    drop(store);
    drop(db);
    let reopened = MemoryDatabase::open_context(path).unwrap();
    let store = reopened.context(access("owner")).unwrap();
    assert_eq!(
        store.latest_checkpoint("session-a").unwrap().unwrap().pin,
        first.pin
    );
    assert!(matches!(
        store.read(&first.pin),
        Err(ContextError::Unavailable(_))
    ));
    assert_eq!(store.history_position("session-a").unwrap(), 2);
}

#[test]
fn live_revision_keeps_checkpoint_derived_originals_and_pairs_the_completed_tool() {
    let tmp = tempfile::tempdir().unwrap();
    let path = tmp.path().join("context");
    let db = MemoryDatabase::create_context(&path).unwrap();
    let store = db.context(access("owner")).unwrap();
    let raw = source(&store, "original", "SQLite source text");
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
    let checkpoint = store
        .save_checkpoint(
            OperationId::new(),
            CheckpointInput {
                scope: CheckpointScope::Session,
                session: "session-a".into(),
                run: "run".into(),
                history_sequence: 1,
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
    let mut call_input = history_input("session-a", "revise-call");
    call_input.kind = HistoryKind::ToolCall {
        call_id: "revise-call".into(),
        tool_name: "context".into(),
        message_id: None,
    };
    call_input.sources = vec![derived.clone()];
    let call = store
        .append_history(
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
        store.read(&checkpoint).is_ok(),
        "a correction must preserve the exact historical snapshot"
    );
    assert!(
        store.read(&derived).is_ok(),
        "a correction must preserve derived original payloads"
    );
    assert!(
        store.read(&call).is_ok(),
        "the already dispatched call must remain readable"
    );
    let mut result_input = history_input("session-a", "revise-result");
    result_input.kind = HistoryKind::ToolResult {
        call_id: "revise-call".into(),
        outcome: ToolOutcome::Succeeded,
    };
    result_input.sources = vec![changed.clone()];
    let result = store
        .append_history(
            OperationId::new(),
            result_input,
            b"revision 2 committed".as_slice(),
        )
        .unwrap()
        .pin;
    assert_eq!(
        store
            .tool_history("session-a", "revise-call")
            .unwrap()
            .1
            .unwrap()
            .pin,
        result
    );
    drop(store);
    drop(db);
    let db = MemoryDatabase::open_context(&path).unwrap();
    let store = db.context(access("owner")).unwrap();
    for pin in [&checkpoint, &derived, &call, &result] {
        assert!(store.read(pin).is_ok());
    }
    db.context(ContextAccess::new("owner", Actor::User))
        .unwrap()
        .purge(OperationId::new(), raw)
        .unwrap();
    for pin in [&checkpoint, &derived, &call, &result] {
        assert!(
            matches!(store.read(pin), Err(ContextError::Unavailable(_))),
            "purge must still hide every copied original"
        );
    }
}

#[test]
fn checkpoint_history_redaction_remains_conservative_for_unlisted_summary_input() {
    let tmp = tempfile::tempdir().unwrap();
    let db = MemoryDatabase::create_context(tmp.path().join("context")).unwrap();
    let store = db.context(access("owner")).unwrap();
    let omitted = source(
        &store,
        "earlier",
        "detail summarized but not individually pinned",
    );
    let anchor = source(&store, "anchor", "last event");
    let checkpoint = store
        .save_checkpoint(
            OperationId::new(),
            CheckpointInput {
                scope: CheckpointScope::Session,
                session: "session-a".into(),
                run: "run".into(),
                history_sequence: 2,
                expected_previous: None,
                expected_availability_epoch: store.availability_epoch().unwrap(),
                sources: vec![anchor],
                payload: json!({"summary":"detail summarized but not individually pinned"}),
            },
        )
        .unwrap()
        .pin;
    db.context(ContextAccess::new("owner", Actor::User))
        .unwrap()
        .retract(OperationId::new(), omitted)
        .unwrap();
    assert!(matches!(
        store.read(&checkpoint),
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
    let latest = |store: &ContextStore<'_>, scope, run: &str| {
        match scope {
            CheckpointScope::Session => store.latest_checkpoint("session-a"),
            CheckpointScope::Run => store.latest_run_checkpoint("session-a", run),
        }
        .unwrap()
    };

    let (base, first) = {
        let db = MemoryDatabase::create_context(&path).unwrap();
        let store = db.context(access("owner")).unwrap();
        let anchor = source(&store, "anchor", "committed parent history");
        let base = CheckpointInput {
            scope: CheckpointScope::Session,
            session: "session-a".into(),
            run: "parent-run".into(),
            history_sequence: 1,
            expected_previous: None,
            expected_availability_epoch: store.availability_epoch().unwrap(),
            sources: vec![anchor],
            payload: json!({"goal": "resume committed work"}),
        };
        let mut first = Vec::new();
        for (scope, profile) in &cases {
            let context = db.context(profile.clone()).unwrap();
            let run = profile.run.as_deref().unwrap();
            assert!(latest(&context, *scope, run).is_none());
            let pin = context
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
            assert_eq!(latest(&context, *scope, run).unwrap().pin, pin);
            assert_eq!(
                store.latest_checkpoint("session-a").unwrap().unwrap().pin,
                first[0]
            );
        }
        (base, first)
    };

    // Every handle and the database have dropped before acquiring a new lease.
    let second = {
        let db = MemoryDatabase::open_context(&path).unwrap();
        for ((scope, profile), pin) in cases.iter().zip(&first) {
            let store = db.context(profile.clone()).unwrap();
            let run = profile.run.as_deref().unwrap();
            let header = latest(&store, *scope, run).unwrap();
            assert_eq!(&header.pin, pin);
            assert_eq!(
                header.checkpoint,
                Some(CheckpointMetadata {
                    scope: *scope,
                    session: "session-a".into(),
                    run: run.into(),
                    window: 1,
                    history_sequence: 1,
                    previous: None,
                    availability_epoch: base.expected_availability_epoch,
                })
            );
            assert_eq!(header.provenance.run, profile.run);
            assert_eq!(header.provenance.parent_run, profile.parent_run);
            if profile.parent_run.is_some() {
                let operation = OperationId::new();
                assert!(matches!(
                    store.save_checkpoint(
                        operation,
                        CheckpointInput {
                            run: run.into(),
                            expected_previous: Some(first[0].clone()),
                            ..base.clone()
                        }
                    ),
                    Err(ContextError::AccessDenied)
                ));
                assert!(store.operation_receipt(operation).unwrap().is_none());
                assert_eq!(
                    store.latest_checkpoint("session-a").unwrap().unwrap().pin,
                    first[0]
                );
                assert_eq!(&latest(&store, *scope, run).unwrap().pin, pin);
            }
        }

        let mut second = Vec::new();
        for (index, (scope, profile)) in cases.iter().enumerate() {
            let store = db.context(profile.clone()).unwrap();
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
                    store.save_checkpoint(
                        operation,
                        CheckpointInput {
                            expected_previous: wrong_previous,
                            ..input.clone()
                        }
                    ),
                    Err(ContextError::InvalidInput(_))
                ));
                assert!(store.operation_receipt(operation).unwrap().is_none());
                assert_eq!(latest(&store, *scope, run).unwrap().pin, first[index]);
            }
            let next = store
                .save_checkpoint(OperationId::new(), input.clone())
                .unwrap()
                .pin;
            let stale = OperationId::new();
            assert!(matches!(
                store.save_checkpoint(stale, input),
                Err(ContextError::InvalidInput(_))
            ));
            assert!(store.operation_receipt(stale).unwrap().is_none());
            assert_eq!(latest(&store, *scope, run).unwrap().pin, next);
            second.push(next);
        }
        second
    };

    let db = MemoryDatabase::open_context(&path).unwrap();
    for (index, (scope, profile)) in cases.iter().enumerate() {
        let store = db.context(profile.clone()).unwrap();
        let header = latest(&store, *scope, profile.run.as_deref().unwrap()).unwrap();
        assert_eq!(header.pin, second[index]);
        let metadata = header.checkpoint.unwrap();
        assert_eq!(metadata.scope, *scope);
        assert_eq!(metadata.window, 2);
        assert_eq!(metadata.previous, Some(first[index].clone()));
        assert_eq!(metadata.history_sequence, 1);
        assert_eq!(header.provenance.parent_run, profile.parent_run);
        assert_eq!(
            store.read(&second[index]).unwrap().body,
            RecordBody::Checkpoint(base.payload.clone())
        );
        assert_eq!(
            store
                .read(&first[index])
                .unwrap()
                .header
                .checkpoint
                .unwrap()
                .window,
            1
        );
        assert_eq!(store.history_position("session-a").unwrap(), 1);
    }
}

#[test]
fn history_copies_old_object_and_type_pins_but_purge_blocks_object_copies() {
    let tmp = tempfile::tempdir().unwrap();
    let db = MemoryDatabase::create_context(tmp.path().join("context")).unwrap();
    let store = db.context(access("owner")).unwrap();
    let raw = source(&store, "evidence", "original observation");
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
        let mut input = history_input("snapshot-session", &format!("copy-{index}"));
        input.kind = HistoryKind::Notice;
        input.sources = vec![original.clone()];
        let copy = store
            .append_history(OperationId::new(), input, payload.as_slice())
            .unwrap();
        assert_eq!(copy.history_sequence, Some(index as u64 + 1));
        assert_eq!(
            store.read(&copy.pin).unwrap().header.sources,
            vec![original.clone()]
        );
        assert_eq!(
            store
                .read_payload(&copy.pin, 0, MAX_RECORD_BYTES)
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
    assert!(matches!(
        store.read(&copies[0].0),
        Err(ContextError::Unavailable(_))
    ));
    assert!(matches!(
        store.read_payload(&copies[0].0, 0, MAX_RECORD_BYTES),
        Err(ContextError::Unavailable(_))
    ));
    assert_eq!(
        store
            .read_payload(&copies[1].0, 0, MAX_RECORD_BYTES)
            .unwrap()
            .bytes,
        copies[1].1
    );

    let mut blocked = history_input("snapshot-session", "copy-after-purge");
    blocked.kind = HistoryKind::Notice;
    blocked.sources = vec![old_object.clone(), old_type];
    blocked.expected_sequence = Some(2);
    let operation = OperationId::new();
    assert!(matches!(
        store.append_history(operation, blocked, copies[0].1.as_slice()),
        Err(ContextError::Unavailable(record)) if record == old_object.record
    ));
    assert!(store.operation_receipt(operation).unwrap().is_none());
    assert_eq!(store.history_position("snapshot-session").unwrap(), 2);
    assert!(store
        .history("snapshot-session", 2, 10)
        .unwrap()
        .entries
        .is_empty());
}

#[test]
fn history_copy_reserves_new_root_depth_before_committing_an_unreadable_record() {
    let tmp = tempfile::tempdir().unwrap();
    let db = MemoryDatabase::create_context(tmp.path().join("context")).unwrap();
    let store = db.context(access("owner")).unwrap();
    let mut chain = vec![source(&store, "root", "original evidence")];
    for depth in 1..=32 {
        let mut input = history_input("depth-session", &format!("depth-{depth}"));
        input.kind = HistoryKind::Notice;
        input.sources = vec![chain.last().unwrap().clone()];
        input.expected_sequence = Some(depth - 1);
        let copy = store
            .append_history(OperationId::new(), input, b"copied evidence".as_slice())
            .unwrap();
        assert_eq!(copy.history_sequence, Some(depth));
        chain.push(copy.pin);
    }
    assert_eq!(
        store.read(&chain[32]).unwrap().header.sources,
        vec![chain[31].clone()]
    );
    assert_eq!(
        store.read_payload(&chain[32], 0, 100).unwrap().bytes,
        b"copied evidence"
    );

    let mut input = history_input("depth-session", "overflow");
    input.kind = HistoryKind::Notice;
    input.sources = vec![chain[32].clone()];
    input.expected_sequence = Some(32);
    let operation = OperationId::new();
    assert!(matches!(
        store.append_history(operation, input.clone(), b"too deep".as_slice()),
        Err(ContextError::BudgetExceeded)
    ));
    assert!(store.operation_receipt(operation).unwrap().is_none());
    assert_eq!(store.history_position("depth-session").unwrap(), 32);
    assert!(store
        .history("depth-session", 32, 10)
        .unwrap()
        .entries
        .is_empty());

    // A rejected copy consumes neither its event identity nor its operation.
    input.sources = vec![chain[31].clone()];
    let accepted = store
        .append_history(operation, input, b"within limit".as_slice())
        .unwrap();
    assert_eq!(accepted.history_sequence, Some(33));
    assert_eq!(
        store.operation_receipt(operation).unwrap(),
        Some(accepted.clone())
    );
    assert_eq!(
        store.read_payload(&accepted.pin, 0, 100).unwrap().bytes,
        b"within limit"
    );
}

#[test]
fn tool_results_link_historical_call_sources_and_still_follow_purge() {
    let temp = tempfile::tempdir().unwrap();
    let db = MemoryDatabase::create_context(temp.path().join("context")).unwrap();
    let store = db.context(access("owner")).unwrap();
    let raw = source(&store, "source", "old observation");
    let kind = store
        .define_type(OperationId::new(), definition("test.revision-result"))
        .unwrap()
        .pin;
    let old = store
        .save_object(OperationId::new(), object(&kind, &raw, "old value"))
        .unwrap()
        .pin;
    let mut call = history_input("session-a", "update-call");
    call.kind = HistoryKind::ToolCall {
        message_id: None,
        call_id: "update-object".into(),
        tool_name: "context".into(),
    };
    call.sources = vec![old.clone()];
    let call = store
        .append_history(
            OperationId::new(),
            call,
            b"update to a new revision".as_slice(),
        )
        .unwrap();
    let current = store
        .revise_object(OperationId::new(), old, object(&kind, &raw, "new value"))
        .unwrap()
        .pin;
    assert!(store.read(&call.pin).is_ok());
    let mut result = history_input("session-a", "update-result");
    result.kind = HistoryKind::ToolResult {
        call_id: "update-object".into(),
        outcome: ToolOutcome::Succeeded,
    };
    let result = store
        .append_history(OperationId::new(), result, b"revision committed".as_slice())
        .unwrap();
    assert_eq!(
        store.read_payload(&result.pin, 0, 100).unwrap().bytes,
        b"revision committed"
    );
    db.context(ContextAccess::new("owner", Actor::User))
        .unwrap()
        .purge(OperationId::new(), current)
        .unwrap();
    assert!(matches!(
        store.read(&result.pin),
        Err(ContextError::Unavailable(_))
    ));
}

#[test]
fn history_copy_shares_the_aggregate_source_budget_including_its_new_root() {
    let tmp = tempfile::tempdir().unwrap();
    let db = MemoryDatabase::create_context(tmp.path().join("context")).unwrap();
    let store = db.context(access("owner")).unwrap();
    let raw = source(&store, "root", "original evidence");
    let mut wide = history_input("budget-session", "wide");
    wide.kind = HistoryKind::Notice;
    wide.sources = vec![raw.clone(); 64];
    let wide = store
        .append_history(OperationId::new(), wide, b"wide snapshot".as_slice())
        .unwrap()
        .pin;

    // Each branch visits 65 records; 1 + 63 * 65 is exactly 4096.
    let mut boundary = history_input("budget-session", "boundary");
    boundary.kind = HistoryKind::Notice;
    boundary.sources = vec![wide.clone(); 63];
    let boundary = store
        .append_history(OperationId::new(), boundary, b"at limit".as_slice())
        .unwrap();
    assert_eq!(
        store.read_payload(&boundary.pin, 0, 100).unwrap().bytes,
        b"at limit"
    );

    let mut overflow = history_input("budget-session", "overflow");
    overflow.kind = HistoryKind::Notice;
    overflow.sources = vec![wide; 63];
    overflow.sources.push(raw);
    overflow.expected_sequence = Some(2);
    let operation = OperationId::new();
    assert!(matches!(
        store.append_history(operation, overflow, b"over limit".as_slice()),
        Err(ContextError::BudgetExceeded)
    ));
    assert!(store.operation_receipt(operation).unwrap().is_none());
    assert_eq!(store.history_position("budget-session").unwrap(), 2);
    assert!(store
        .history("budget-session", 2, 10)
        .unwrap()
        .entries
        .is_empty());
}
