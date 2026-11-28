use mmdb::{session::*, *};
use std::time::{Duration, UNIX_EPOCH};
fn access(actor: Actor) -> ContextAccess {
    ContextAccess {
        run: Some("run".into()),
        scopes: vec![Scope::System, Scope::Personal],
        ..ContextAccess::new("owner", actor)
    }
}
fn new(name: &str) -> NewSession {
    NewSession {
        session: name.into(),
        kind: "chat".into(),
        parent: None,
        metadata: serde_json::Value::Null,
    }
}
fn event(
    log: &SessionLog<'_>,
    session: &str,
    id: &str,
    kind: EventKind,
    sources: Vec<RecordPin>,
) -> RecordPin {
    log.append(
        OperationId::new(),
        EventInput {
            session: session.into(),
            event_id: id.into(),
            kind,
            scope: if session == "source" {
                Scope::Personal
            } else {
                Scope::System
            },
            sources,
            expected_sequence: None,
            media_type: "text/plain".into(),
            occurred_at_ms: 1,
        },
        id.as_bytes(),
    )
    .unwrap()
    .pin
}
fn input(name: &str, position: u64) -> ConversationCacheInput {
    ConversationCacheInput {
        session: "chat".into(),
        derivation: name.into(),
        position,
        media_type: "text/plain".into(),
        expected_availability_epoch: 0,
    }
}
struct Fixture {
    store: Store,
    dir: tempfile::TempDir,
}
impl Fixture {
    fn new() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::create(dir.path().join("db")).unwrap();
        let f = Self { store, dir };
        let log = f.log();
        log.create_session(OperationId::new(), new("chat"), Scope::System)
            .unwrap();
        event(&log, "chat", "A", EventKind::Notice, vec![]);
        f
    }
    fn log(&self) -> SessionLog<'_> {
        self.store.session_log(access(Actor::Operator)).unwrap()
    }
    fn restricted(&self) -> SessionLog<'_> {
        self.store
            .session_log(ContextAccess {
                scopes: vec![Scope::System],
                ..access(Actor::Operator)
            })
            .unwrap()
    }
    fn add_source(&self) {
        let l = self.log();
        l.create_session(OperationId::new(), new("source"), Scope::Personal)
            .unwrap();
        let pin = event(&l, "source", "private", EventKind::Notice, vec![]);
        event(&l, "chat", "copy", EventKind::Notice, vec![pin]);
    }
    fn save(&self, name: &str, n: u64) -> ConversationCache {
        self.log()
            .save_conversation_cache(input(name, n), name.as_bytes())
            .unwrap()
    }
    fn fork(&self) {
        self.log()
            .fork_session(
                OperationId::new(),
                new("fork"),
                SessionOrigin {
                    session: "chat".into(),
                    position: 1,
                },
            )
            .unwrap();
    }
    fn first(&self) -> RecordPin {
        self.log()
            .events("chat", 0, 1)
            .unwrap()
            .entries
            .remove(0)
            .pin
    }
}

#[test]
fn only_system_or_operator_save_or_remove_caches() {
    let f = Fixture::new();
    for actor in [
        Actor::System,
        Actor::Operator,
        Actor::User,
        Actor::Assistant,
    ] {
        let l = f.store.session_log(access(actor)).unwrap();
        let save = l.save_conversation_cache(input("x", 1), &[][..]);
        let remove = l.remove_conversation_cache("chat", "x");
        if matches!(actor, Actor::System | Actor::Operator) {
            save.unwrap();
            assert!(remove.unwrap());
        } else {
            assert!(matches!(save, Err(ContextError::AccessDenied)));
            assert!(matches!(remove, Err(ContextError::AccessDenied)));
        }
    }
}

#[test]
fn saving_needs_every_covered_source_scope() {
    let f = Fixture::new();
    f.add_source();
    assert!(matches!(
        f.restricted()
            .save_conversation_cache(input("x", 2), &[][..]),
        Err(ContextError::AccessDenied)
    ));
    assert!(f.log().conversation_cache("chat", "x").unwrap().is_none());
}

#[test]
fn reading_a_cache_needs_every_covered_source_scope() {
    let f = Fixture::new();
    f.add_source();
    let c = f.save("x", 2);
    let l = f.restricted();
    assert!(matches!(
        l.conversation_cache("chat", "x"),
        Err(ContextError::AccessDenied)
    ));
    assert!(matches!(
        l.read_conversation_cache(&c, 0, 1),
        Err(ContextError::AccessDenied)
    ));
}

#[test]
fn listing_skips_unreadable_caches_without_naming_them() {
    let f = Fixture::new();
    f.add_source();
    f.save("aa", 1);
    f.save("z", 2);
    let l = f.restricted();
    let page = l.conversation_caches("chat", None, 1).unwrap();
    assert_eq!(page.caches[0].derivation, "aa");
    assert_eq!(page.next, None);
    f.save("b", 2);
    f.save("c", 1);
    f.save("d", 2);
    f.save("e", 1);
    let p = l.conversation_caches("chat", None, 2).unwrap();
    assert_eq!(p.next.as_deref(), Some("c"));
    assert_eq!(
        p.caches
            .iter()
            .map(|c| c.derivation.as_str())
            .collect::<Vec<_>>(),
        ["aa", "c"]
    );
    assert_eq!(
        l.conversation_caches("chat", p.next.as_deref(), 2)
            .unwrap()
            .caches[0]
            .derivation,
        "e"
    );
}

#[test]
fn a_session_bound_access_reaches_only_its_session_caches() {
    let f = Fixture::new();
    let c = f.save("x", 1);
    let l = f
        .store
        .session_log(ContextAccess {
            session: Some("fork".into()),
            ..access(Actor::System)
        })
        .unwrap();
    assert!(matches!(
        l.conversation_cache("chat", "x"),
        Err(ContextError::AccessDenied)
    ));
    assert!(matches!(
        l.conversation_caches("chat", None, 2),
        Err(ContextError::AccessDenied)
    ));
    assert!(matches!(
        l.remove_conversation_cache("chat", "x"),
        Err(ContextError::AccessDenied)
    ));
    assert!(matches!(
        l.read_conversation_cache(&c, 0, 1),
        Err(ContextError::AccessDenied)
    ));
    assert!(matches!(
        l.save_conversation_cache(input("x", 1), &[][..]),
        Err(ContextError::AccessDenied)
    ));
}

#[test]
fn each_derivation_keeps_only_its_latest_cache() {
    let f = Fixture::new();
    event(&f.log(), "chat", "B", EventKind::Notice, vec![]);
    let old = f.save("x", 2);
    let y = f.save("y", 2);
    let x = f.save("x", 1);
    assert_ne!(old.version, x.version);
    assert_eq!(
        f.log()
            .conversation_caches("chat", None, 10)
            .unwrap()
            .caches,
        [x, y]
    );
}

#[test]
fn repeating_a_save_replaces_the_cache_again() {
    let f = Fixture::new();
    let a = f.save("x", 1);
    let b = f.save("x", 1);
    assert_ne!(a.version, b.version);
    assert_eq!(a.digest, b.digest);
}

#[test]
fn a_cache_may_end_inside_a_forked_prefix_and_forks_start_without_caches() {
    let f = Fixture::new();
    f.save("x", 1);
    f.fork();
    let l = f.log();
    assert_eq!(l.conversation_cache("fork", "x").unwrap(), None);
    let c = l
        .save_conversation_cache(
            ConversationCacheInput {
                session: "fork".into(),
                ..input("x", 1)
            },
            &b"fork"[..],
        )
        .unwrap();
    assert_eq!(l.read_conversation_cache(&c, 0, 10).unwrap().bytes, b"fork");
}

#[test]
fn listing_pages_caches_in_derivation_name_order() {
    let f = Fixture::new();
    let z = f.save("z", 1);
    let aa = f.save("aa", 1);
    let l = f.log();
    let p = l.conversation_caches("chat", None, 1).unwrap();
    assert_eq!(p.caches, [aa]);
    assert_eq!(p.next.as_deref(), Some("aa"));
    for cursor in ["aa", "ab"] {
        let p = l.conversation_caches("chat", Some(cursor), 1).unwrap();
        assert_eq!(p.caches, [z.clone()]);
        assert_eq!(p.next, None);
    }
}

#[test]
fn a_cache_payload_spans_many_chunks_and_pages() {
    let f = Fixture::new();
    let l = f.log();
    let bytes: Vec<u8> = (0..(2 * MAX_PAYLOAD_PAGE_BYTES + 17))
        .map(|i| (i % 251) as u8)
        .collect();
    let c = l
        .save_conversation_cache(input("large", 1), bytes.as_slice())
        .unwrap();
    let mut all = Vec::new();
    let mut at = Some(0);
    while let Some(offset) = at {
        let p = l
            .read_conversation_cache(&c, offset, MAX_PAYLOAD_PAGE_BYTES - 3)
            .unwrap();
        all.extend(p.bytes);
        at = p.next_offset;
    }
    assert_eq!(all, bytes);
    for (at, n) in [(0, 0), (0, MAX_PAYLOAD_PAGE_BYTES + 1), (c.bytes + 1, 1)] {
        assert!(matches!(
            l.read_conversation_cache(&c, at, n),
            Err(ContextError::InvalidInput(_))
        ));
    }
}

#[test]
fn a_cache_behind_the_log_restores_the_rest_from_the_tail() {
    let f = Fixture::new();
    let l = f.log();
    for i in 2..=10 {
        event(&l, "chat", &i.to_string(), EventKind::Notice, vec![]);
    }
    event(&f.log(), "chat", "11", EventKind::Notice, vec![]);
    let c = f.save("x", 10);
    let tail = l.events("chat", c.position, 10).unwrap();
    assert_eq!(tail.entries.len(), 1);
    assert_eq!(tail.entries[0].event.as_ref().unwrap().sequence, 11);
    assert_eq!(tail.next_sequence, None);
}

#[test]
fn a_cache_and_the_log_tail_restore_the_conversation_after_reopen() {
    let f = Fixture::new();
    let l = f.log();
    let epoch = l.availability_epoch().unwrap();
    let a = l.read_payload(&f.first(), 0, 8).unwrap().bytes;
    let c = l
        .save_conversation_cache(
            ConversationCacheInput {
                expected_availability_epoch: epoch,
                ..input("x", 1)
            },
            a.as_slice(),
        )
        .unwrap();
    event(&l, "chat", "B", EventKind::Notice, vec![]);
    drop(f.store);
    let store = Store::open(f.dir.path().join("db")).unwrap();
    let l = store.session_log(access(Actor::User)).unwrap();
    let found = l.conversation_cache("chat", "x").unwrap().unwrap();
    assert_eq!(found, c);
    let mut restored = l.read_conversation_cache(&found, 0, 8).unwrap().bytes;
    for h in l.events("chat", found.position, 10).unwrap().entries {
        restored.extend(l.read_payload(&h.pin, 0, 8).unwrap().bytes);
    }
    assert_eq!(restored, b"AB");
}

#[test]
fn withdrawing_a_covered_event_makes_the_cache_stale() {
    let f = Fixture::new();
    f.save("x", 1);
    f.log().retract(OperationId::new(), f.first()).unwrap();
    assert!(
        f.log()
            .conversation_cache("chat", "x")
            .unwrap()
            .unwrap()
            .stale
    );
}

#[test]
fn withdrawing_a_shared_prefix_event_makes_fork_caches_stale() {
    let f = Fixture::new();
    f.fork();
    let l = f.log();
    l.save_conversation_cache(
        ConversationCacheInput {
            session: "fork".into(),
            ..input("x", 1)
        },
        &[][..],
    )
    .unwrap();
    l.purge(OperationId::new(), f.first()).unwrap();
    assert!(l.conversation_cache("fork", "x").unwrap().unwrap().stale);
}

#[test]
fn withdrawing_a_later_part_of_a_covered_message_makes_the_cache_stale() {
    for (derivation, later_part) in [
        (
            "fragment",
            EventKind::AssistantFragment {
                message_id: "m".into(),
                ordinal: 0,
            },
        ),
        (
            "tool",
            EventKind::ToolCall {
                call_id: "call".into(),
                tool_name: "lookup".into(),
                message_id: Some("m".into()),
            },
        ),
    ] {
        let f = Fixture::new();
        let l = f.log();
        for (id, k) in [
            ("turn", EventKind::TurnStarted),
            ("step", EventKind::StepStarted),
            (
                "start",
                EventKind::AssistantStarted {
                    message_id: "m".into(),
                },
            ),
        ] {
            event(&l, "chat", id, k, vec![]);
        }
        f.save(derivation, 4);
        let part = event(&l, "chat", "part", later_part, vec![]);
        let cache = || l.conversation_cache("chat", derivation).unwrap().unwrap();
        assert!(!cache().stale);
        l.retract(OperationId::new(), part).unwrap();
        assert!(cache().stale, "{derivation}");
    }
}

#[test]
fn later_withdrawals_and_other_changes_keep_the_cache_current() {
    let f = Fixture::new();
    let c = f.save("x", 1);
    let l = f.log();
    let b = event(&l, "chat", "B", EventKind::Notice, vec![]);
    l.purge(OperationId::new(), b).unwrap();
    l.update_session(
        OperationId::new(),
        l.session("chat").unwrap().pin,
        serde_json::json!({"title":"updated"}),
    )
    .unwrap();
    event(&l, "chat", "turn", EventKind::TurnStarted, vec![]);
    drop(f.store);
    let store = Store::open(f.dir.path().join("db")).unwrap();
    let l = store.session_log(access(Actor::User)).unwrap();
    assert_eq!(l.conversation_cache("chat", "x").unwrap(), Some(c));
}

#[test]
fn removing_a_cache_and_removing_nothing() {
    let f = Fixture::new();
    f.save("x", 1);
    let l = f.log();
    assert!(l.remove_conversation_cache("chat", "x").unwrap());
    assert!(!l.remove_conversation_cache("chat", "x").unwrap());
}

#[test]
fn saving_refuses_bad_input_and_writes_nothing() {
    let f = Fixture::new();
    let l = f.log();
    for i in [
        input("x", 0),
        input("x", 2),
        input("", 1),
        ConversationCacheInput {
            media_type: "\n".into(),
            ..input("x", 1)
        },
        ConversationCacheInput {
            expected_availability_epoch: 1,
            ..input("x", 1)
        },
    ] {
        assert!(matches!(
            l.save_conversation_cache(i, &[][..]),
            Err(ContextError::InvalidInput(_) | ContextError::PositionConflict { .. })
        ));
    }
    assert!(matches!(
        l.save_conversation_cache(
            ConversationCacheInput {
                session: "unknown".into(),
                ..input("x", 1)
            },
            &[][..]
        ),
        Err(ContextError::UnknownSession(_))
    ));
    assert!(l
        .conversation_caches("chat", None, 1)
        .unwrap()
        .caches
        .is_empty());
    let report = l.recover_payloads(32, 32).unwrap();
    assert_eq!((report.chunks_removed, report.uploads_removed), (0, 0));
}

#[test]
fn saving_refuses_a_derivation_that_raced_a_covered_withdrawal() {
    let f = Fixture::new();
    let l = f.log();
    let epoch = l.availability_epoch().unwrap();
    let a = f.first();
    l.read_payload(&a, 0, 1).unwrap();
    l.purge(OperationId::new(), a).unwrap();
    let b = event(&l, "chat", "B", EventKind::Notice, vec![]);
    l.read_payload(&b, 0, 1).unwrap();
    assert!(
        matches!(l.save_conversation_cache(ConversationCacheInput{expected_availability_epoch:epoch,..input("x",2)},&b"AB"[..]),Err(ContextError::AvailabilityConflict{expected,actual}) if expected==epoch && actual>epoch)
    );
}

#[test]
fn reading_reports_stale_and_replaced_caches() {
    let f = Fixture::new();
    let old = f.save("x", 1);
    let c = f.save("x", 1);
    let l = f.log();
    assert!(matches!(
        l.read_conversation_cache(&old, 0, 1),
        Err(ContextError::ConversationCacheReplaced)
    ));
    l.purge(OperationId::new(), f.first()).unwrap();
    assert!(matches!(
        l.read_conversation_cache(&c, 0, 1),
        Err(ContextError::ConversationCacheStale)
    ));
    l.remove_conversation_cache("chat", "x").unwrap();
    assert!(matches!(
        l.read_conversation_cache(&c, 0, 1),
        Err(ContextError::ConversationCacheReplaced)
    ));
}

#[test]
fn cache_names_and_pages_are_bounded() {
    let f = Fixture::new();
    let l = f.log();
    f.save(&"a".repeat(256), 1);
    for s in ["".into(), "\t".into(), "é".repeat(129)] {
        assert!(matches!(
            l.conversation_cache("chat", &s),
            Err(ContextError::InvalidInput(_))
        ));
        assert!(matches!(
            l.conversation_caches("chat", Some(&s), 1),
            Err(ContextError::InvalidInput(_))
        ));
    }
    for n in [0, 101] {
        assert!(matches!(
            l.conversation_caches("chat", None, n),
            Err(ContextError::InvalidInput(_))
        ));
    }
    assert_eq!(
        l.conversation_caches("chat", None, 100)
            .unwrap()
            .caches
            .len(),
        1
    );
}

#[test]
fn caches_survive_a_memory_reset() {
    let mut f = Fixture::new();
    let c = f.save("x", 1);
    let p = f
        .store
        .plan_memory_reset(UNIX_EPOCH, Duration::from_secs(60))
        .unwrap();
    f.store.reset_memory(&p, p.digest(), UNIX_EPOCH).unwrap();
    assert_eq!(
        f.log().conversation_cache("chat", "x").unwrap(),
        Some(c.clone())
    );
    assert_eq!(
        f.log().read_conversation_cache(&c, 0, 1).unwrap().bytes,
        b"x"
    );
}

#[test]
fn cache_contents_are_not_search_hits() {
    let f = Fixture::new();
    f.save("secret", 1);
    let result = f
        .log()
        .search(LogQuery {
            text: "secret".into(),
            session: Some("chat".into()),
            cursor: None,
            max_events: 100,
            max_bytes: 4096,
            limit: 10,
        })
        .unwrap();
    assert!(result.matches.is_empty());
}

#[test]
fn saving_or_removing_a_cache_keeps_the_session_change_number() {
    let f = Fixture::new();
    let l = f.log();
    let before = l.session("chat").unwrap().change;
    f.save("x", 1);
    assert_eq!(l.session("chat").unwrap().change, before);
    l.remove_conversation_cache("chat", "x").unwrap();
    assert_eq!(l.session("chat").unwrap().change, before);
}
