use crate::context::{self, ContextParts, ContextStore};
use crate::native_memory::{self, MemoryDatabase, MemoryError};
use crate::records::{self, RecordTable, Records};
use crate::session::{self, LogGate, SessionLog, SessionParts};
use crate::{
    ContextAccess, ContextError, ContextResult, EraId, StoreFormatDescriptor, StoreModule,
};
use mmdb_kernel::store_format::{StoreEraId, MAX_RESET_PLAN_TTL};
use mmdb_kernel::{Kernel, KernelError, ModuleId, ModuleSpec, PartitionSpec};
use std::fmt;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

pub const STORE_FORMAT_ID: &str = "mmdb-store-v3";
const SESSION: ModuleId = StoreModule::Session.id();
const MEMORY: ModuleId = StoreModule::Memory.id();

macro_rules! partitions {
    ($($group:path),+) => {{
        let mut names = [PartitionSpec::Inline(""); 0 $(+ $group.len())+];
        let mut remaining = names.as_mut_slice();
        $(
            let (group, rest) = remaining.split_at_mut($group.len());
            group.copy_from_slice($group);
            remaining = rest;
        )+
        assert!(remaining.is_empty());
        names
    }};
}

const MODULES: &[ModuleSpec] = &[
    ModuleSpec {
        id: SESSION,
        partitions: &partitions!(records::SESSION_PARTITIONS, session::SESSION_PARTITIONS),
    },
    ModuleSpec {
        id: MEMORY,
        partitions: &partitions!(
            records::MEMORY_PARTITIONS,
            context::MEMORY_PARTITIONS,
            native_memory::NATIVE_PARTITIONS
        ),
    },
];

/// A leased store containing the session log, domain context and native memory.
pub struct Store {
    kernel: Kernel,
    records: Records,
    context: ContextParts,
    session: SessionParts,
    memory: MemoryDatabase,
    #[cfg(test)]
    fail_reset_preparation: bool,
}

impl Store {
    /// Initialize a new store at an absent path.
    pub fn create(root: impl AsRef<Path>) -> StoreResult<Self> {
        let format = StoreFormatDescriptor::new(STORE_FORMAT_ID).map_err(KernelError::Format)?;
        Self::from_kernel(Kernel::create(root.as_ref(), &format, MODULES)?)
    }

    /// Open an existing directory with the exact store format.
    pub fn open(root: impl AsRef<Path>) -> StoreResult<Self> {
        let format = StoreFormatDescriptor::new(STORE_FORMAT_ID).map_err(KernelError::Format)?;
        let store = Self::from_kernel(Kernel::open(root.as_ref(), &format, MODULES)?)?;
        session::end_interrupted_turns(&store.records, &store.session)?;
        Ok(store)
    }

    fn from_kernel(kernel: Kernel) -> StoreResult<Self> {
        let session = kernel.module(SESSION);
        let memory = kernel.module(MEMORY);
        let context = ContextParts::open(&memory)?;
        let records = Records::open(
            kernel.clone(),
            &session,
            &memory,
            Box::new(LogGate::open(&session)?),
        )?;
        let session = SessionParts::open(&session)?;
        let memory = MemoryDatabase::on_era(kernel.clone(), &memory)?;
        Ok(Self {
            kernel,
            records,
            context,
            session,
            memory,
            #[cfg(test)]
            fail_reset_preparation: false,
        })
    }

    pub fn era(&self, module: StoreModule) -> EraId {
        self.kernel.era(module.id())
    }

    pub fn session_log(&self, access: ContextAccess) -> ContextResult<SessionLog<'_>> {
        access.validate()?;
        Ok(SessionLog {
            scoped: self.records.scoped(access),
            parts: &self.session,
        })
    }

    pub fn context(&self, access: ContextAccess) -> ContextResult<ContextStore<'_>> {
        access.validate()?;
        Ok(ContextStore {
            scoped: self.records.scoped(access),
            parts: &self.context,
        })
    }

    /// Plan an in-place reset of native memory and domain context. The plan is
    /// bound to this directory, its current eras and a millisecond time window.
    pub fn plan_memory_reset(
        &self,
        issued_at: SystemTime,
        ttl: Duration,
    ) -> StoreResult<MemoryResetPlan> {
        if ttl.is_zero() || ttl > MAX_RESET_PLAN_TTL {
            return Err(StoreError::ResetPlanTtl);
        }
        let issued_at_unix_ms = issued_at
            .duration_since(UNIX_EPOCH)
            .ok()
            .and_then(|elapsed| u64::try_from(elapsed.as_millis()).ok())
            .ok_or(StoreError::ResetPlanTtl)?;
        // A positive TTL has at least one tick in the plan's millisecond clock.
        let ttl_ms = ttl.as_nanos().div_ceil(1_000_000) as u64;
        let expires_at_unix_ms = issued_at_unix_ms
            .checked_add(ttl_ms)
            .ok_or(StoreError::ResetPlanTtl)?;
        let new_memory_era = EraId::new();
        Ok(MemoryResetPlan {
            canonical_root: self.kernel.canonical_root().to_owned(),
            store_era: self.kernel.store_era().clone(),
            memory_era: self.kernel.era(MEMORY),
            new_memory_era,
            issued_at_unix_ms,
            expires_at_unix_ms,
            digest: memory_reset_digest(
                &self.kernel,
                new_memory_era,
                issued_at_unix_ms,
                expires_at_unix_ms,
            ),
        })
    }

    /// Commit a confirmed reset. An unknown commit outcome requires reopening
    /// the store. Successful commits keep the session log and its receipts.
    pub fn reset_memory(
        &mut self,
        plan: &MemoryResetPlan,
        presented_digest: &str,
        now: SystemTime,
    ) -> StoreResult<MemoryResetReceipt> {
        let _guard = self.kernel.write_lock();
        plan.validate(&self.kernel, presented_digest, now)?;
        let prepared = self
            .kernel
            .prepare_module_reset(MEMORY, plan.new_memory_era)?;
        let table = RecordTable::open(&prepared)?;
        let context = ContextParts::open(&prepared)?;
        #[cfg(test)]
        if std::mem::take(&mut self.fail_reset_preparation) {
            return Err(KernelError::Storage("interrupted handle preparation".into()).into());
        }
        let memory = MemoryDatabase::on_era(self.kernel.clone(), &prepared)?;

        let mut batch = self.kernel.batch();
        self.records.count_memory_reset(&mut batch)?;
        let report = self.kernel.commit_module_reset(prepared, batch)?;
        self.records.replace_memory_table(table);
        self.context = context;
        self.memory = memory;
        Ok(MemoryResetReceipt {
            old_memory_era: report.old_era,
            new_memory_era: report.new_era,
            cleanup_pending: report.cleanup_pending,
        })
    }

    pub fn memory(&self) -> &MemoryDatabase {
        &self.memory
    }
}

#[derive(Debug)]
pub struct MemoryResetPlan {
    canonical_root: PathBuf,
    store_era: StoreEraId,
    memory_era: EraId,
    new_memory_era: EraId,
    issued_at_unix_ms: u64,
    expires_at_unix_ms: u64,
    digest: String,
}

impl MemoryResetPlan {
    pub fn canonical_root(&self) -> &Path {
        &self.canonical_root
    }
    pub fn memory_era(&self) -> EraId {
        self.memory_era
    }
    pub fn new_memory_era(&self) -> EraId {
        self.new_memory_era
    }
    pub fn issued_at_unix_ms(&self) -> u64 {
        self.issued_at_unix_ms
    }
    pub fn expires_at_unix_ms(&self) -> u64 {
        self.expires_at_unix_ms
    }
    pub fn digest(&self) -> &str {
        &self.digest
    }

    fn validate(
        &self,
        kernel: &Kernel,
        presented_digest: &str,
        now: SystemTime,
    ) -> StoreResult<()> {
        let elapsed = now
            .duration_since(UNIX_EPOCH)
            .map_err(|_| StoreError::ResetPlanNotYetValid)?;
        let now_ms =
            u64::try_from(elapsed.as_millis()).map_err(|_| StoreError::ResetPlanExpired)?;
        if now_ms < self.issued_at_unix_ms {
            return Err(StoreError::ResetPlanNotYetValid);
        }
        if now_ms >= self.expires_at_unix_ms {
            return Err(StoreError::ResetPlanExpired);
        }
        if kernel.canonical_root() != self.canonical_root {
            return Err(StoreError::ResetPlanForAnotherStore);
        }
        if kernel.store_era() != &self.store_era || kernel.era(MEMORY) != self.memory_era {
            return Err(StoreError::StaleResetPlan);
        }
        let expected = memory_reset_digest(
            kernel,
            self.new_memory_era,
            self.issued_at_unix_ms,
            self.expires_at_unix_ms,
        );
        if expected != self.digest || expected != presented_digest {
            return Err(StoreError::ResetDigestMismatch);
        }
        Ok(())
    }
}

fn memory_reset_digest(
    kernel: &Kernel,
    replacement: EraId,
    issued_ms: u64,
    expires_ms: u64,
) -> String {
    let mut digest = kernel.keyed_hasher("mmdb-memory-reset-v1");
    let path = kernel.canonical_root().as_os_str().as_encoded_bytes();
    digest.update(&(path.len() as u64).to_be_bytes());
    digest.update(path);
    digest.update(kernel.store_era().as_str().as_bytes());
    digest.update(&kernel.era(MEMORY).as_ulid().to_bytes());
    digest.update(&replacement.as_ulid().to_bytes());
    digest.update(&issued_ms.to_be_bytes());
    digest.update(&expires_ms.to_be_bytes());
    digest.finalize().to_hex().to_string()
}

#[derive(Debug)]
pub struct MemoryResetReceipt {
    pub old_memory_era: EraId,
    pub new_memory_era: EraId,
    pub cleanup_pending: bool,
}

#[derive(Debug)]
pub enum StoreError {
    Kernel(KernelError),
    Context(ContextError),
    Memory(MemoryError),
    ResetPlanTtl,
    ResetPlanNotYetValid,
    ResetPlanExpired,
    ResetPlanForAnotherStore,
    StaleResetPlan,
    ResetDigestMismatch,
}

pub type StoreResult<T> = Result<T, StoreError>;

impl fmt::Display for StoreError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Kernel(error) => error.fmt(f),
            Self::Context(error) => error.fmt(f),
            Self::Memory(error) => error.fmt(f),
            Self::ResetPlanTtl => f.write_str("invalid memory reset plan time window"),
            Self::ResetPlanNotYetValid => f.write_str("memory reset plan is not yet valid"),
            Self::ResetPlanExpired => f.write_str("memory reset plan has expired"),
            Self::ResetPlanForAnotherStore => {
                f.write_str("memory reset plan belongs to another directory")
            }
            Self::StaleResetPlan => f.write_str("memory reset plan belongs to an earlier era"),
            Self::ResetDigestMismatch => {
                f.write_str("memory reset confirmation digest does not match")
            }
        }
    }
}

impl std::error::Error for StoreError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Kernel(error) => Some(error),
            Self::Context(error) => Some(error),
            Self::Memory(error) => Some(error),
            _ => None,
        }
    }
}

impl From<KernelError> for StoreError {
    fn from(error: KernelError) -> Self {
        Self::Kernel(error)
    }
}

impl From<MemoryError> for StoreError {
    fn from(error: MemoryError) -> Self {
        Self::Memory(error)
    }
}

impl From<ContextError> for StoreError {
    fn from(error: ContextError) -> Self {
        Self::Context(error)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::context::TypeDefinition;
    use crate::session::EventInput;
    use crate::{
        Actor, EventKind, MessageStatus, OperationId, Scope, ToolOutcome, TurnEndReason, TurnError,
    };

    fn session_writer<'a>(store: &'a Store, owner: &str, session: &str) -> SessionLog<'a> {
        store
            .session_log(ContextAccess {
                session: Some(session.into()),
                run: Some(format!("run-{owner}-{session}")),
                ..ContextAccess::new(owner, Actor::System)
            })
            .unwrap()
    }

    fn event_input(session: &str, id: &str, kind: EventKind) -> EventInput {
        EventInput {
            session: session.into(),
            event_id: id.into(),
            kind,
            sources: vec![],
            scope: Scope::Personal,
            media_type: "text/plain".into(),
            occurred_at_ms: 1,
            expected_sequence: None,
        }
    }

    fn append_event(log: &SessionLog<'_>, session: &str, id: &str, kind: EventKind) {
        log.append(OperationId::new(), event_input(session, id, kind), &[][..])
            .unwrap();
    }

    fn start_step(store: &Store, owner: &str, session: &str) {
        let log = session_writer(store, owner, session);
        append_event(&log, session, "turn", EventKind::TurnStarted);
        append_event(&log, session, "step", EventKind::StepStarted);
    }

    fn assert_step_interrupted(store: &Store, owner: &str, session: &str) {
        let log = session_writer(store, owner, session);
        let entries = log.events(session, 2, 10).unwrap().entries;
        assert_eq!(log.position(session).unwrap(), 4);
        assert_eq!(entries.len(), 2);
        assert_eq!(
            entries[0].event.as_ref().unwrap().kind,
            EventKind::StepEnded {
                start_event_id: "step".into(),
            }
        );
        assert_eq!(
            entries[1].event.as_ref().unwrap().kind,
            EventKind::TurnEnded {
                reason: TurnEndReason::Interrupted,
                start_event_id: "turn".into(),
            }
        );
        for header in entries {
            assert_eq!(header.provenance.session.as_deref(), Some(session));
            assert_eq!(
                header.provenance.run,
                Some(format!("run-{owner}-{session}"))
            );
        }
    }

    #[test]
    fn opening_reads_only_sessions_with_open_turns() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("store");
        let store = Store::create(&root).unwrap();
        for owner in ["alice", "bob"] {
            start_step(&store, owner, "chat");
        }
        let idle = session_writer(&store, "alice", "idle");
        append_event(&idle, "idle", "notice", EventKind::Notice);
        let mut batch = store.kernel.batch();
        batch.insert(
            &store.session.heads,
            session::session_key("alice", "idle"),
            b"invalid",
        );
        batch.commit().unwrap();
        drop(store);

        let store = Store::open(&root).unwrap();
        for owner in ["alice", "bob"] {
            assert_step_interrupted(&store, owner, "chat");
        }
    }

    #[test]
    fn an_append_reads_only_the_owed_row_it_settles() {
        let temp = tempfile::tempdir().unwrap();
        let store = Store::create(temp.path().join("store")).unwrap();
        start_step(&store, "alice", "chat");
        let log = session_writer(&store, "alice", "chat");
        let append =
            |id: &str, kind| log.append(OperationId::new(), event_input("chat", id, kind), &[][..]);
        append(
            "message",
            EventKind::AssistantStarted {
                message_id: "m".into(),
            },
        )
        .unwrap();
        append(
            "terminal",
            EventKind::AssistantTerminal {
                message_id: "m".into(),
                status: MessageStatus::Completed,
            },
        )
        .unwrap();
        for id in ["A", "B"] {
            append(
                id,
                EventKind::ToolCall {
                    call_id: id.into(),
                    tool_name: "read".into(),
                    message_id: Some("m".into()),
                },
            )
            .unwrap();
        }
        let mut key = session::session_key("alice", "chat");
        key.extend_from_slice(&[1, b'B']);
        assert!(store.session.owed.get(&key).unwrap().is_some());
        let mut batch = store.kernel.batch();
        batch.insert(&store.session.owed, key, b"invalid");
        batch.commit().unwrap();

        append("notice", EventKind::Notice).unwrap();
        append(
            "result-A",
            EventKind::ToolResult {
                call_id: "A".into(),
                outcome: ToolOutcome::Succeeded,
            },
        )
        .unwrap();
        let before = log.position("chat").unwrap();
        assert!(matches!(
            append(
                "result-B",
                EventKind::ToolResult {
                    call_id: "B".into(),
                    outcome: ToolOutcome::Succeeded,
                }
            ),
            Err(ContextError::Corrupt(_))
        ));
        assert!(matches!(
            append(
                "step-end",
                EventKind::StepEnded {
                    start_event_id: "step".into()
                }
            ),
            Err(ContextError::Turn(TurnError::StepUnfinished))
        ));
        assert!(matches!(
            append(
                "cancel",
                EventKind::TurnEnded {
                    reason: TurnEndReason::Cancelled,
                    start_event_id: "turn".into(),
                }
            ),
            Err(ContextError::Corrupt(_))
        ));
        assert_eq!(log.position("chat").unwrap(), before);
    }

    #[test]
    fn turn_end_and_reopen_reject_inconsistent_owed_counts() {
        for (turn_owes, step_owes) in [(0, 0), (1, 0), (2, 1)] {
            let temp = tempfile::tempdir().unwrap();
            let root = temp.path().join("store");
            let store = Store::create(&root).unwrap();
            let log = unanswered_call_with_counts(&store, turn_owes, step_owes);
            let before = log.position("chat").unwrap();
            let end = event_input(
                "chat",
                "end",
                EventKind::TurnEnded {
                    reason: TurnEndReason::Completed,
                    start_event_id: "turn".into(),
                },
            );
            assert!(matches!(
                log.append(OperationId::new(), end, &b"end"[..]),
                Err(ContextError::Corrupt(_))
            ));
            assert_eq!(log.position("chat").unwrap(), before);
            let cleanup = log.recover_payloads(1, 1).unwrap();
            assert_eq!((cleanup.uploads_removed, cleanup.chunks_removed), (0, 0));
            drop(store);
            assert!(matches!(
                Store::open(&root),
                Err(StoreError::Context(ContextError::Corrupt(_)))
            ));
        }
    }

    fn unanswered_call_with_counts(
        store: &Store,
        turn_owes: u64,
        step_owes: u64,
    ) -> SessionLog<'_> {
        start_step(store, "alice", "chat");
        let log = session_writer(store, "alice", "chat");
        append_event(
            &log,
            "chat",
            "call",
            EventKind::ToolCall {
                call_id: "A".into(),
                tool_name: "read".into(),
                message_id: None,
            },
        );
        let key = session::session_key("alice", "chat");
        let row = store.session.open_turns.get(&key).unwrap().unwrap();
        let mut turn: serde_json::Value = serde_json::from_slice(&row).unwrap();
        assert_eq!(turn["owes"], 1);
        assert_eq!(turn["step"]["owes"], 1);
        turn["owes"] = turn_owes.into();
        turn["step"]["owes"] = step_owes.into();
        let mut batch = store.kernel.batch();
        batch.insert(
            &store.session.open_turns,
            key,
            serde_json::to_vec(&turn).unwrap(),
        );
        batch.commit().unwrap();
        log
    }

    #[test]
    fn settling_with_an_empty_step_count_stages_no_payload() {
        let temp = tempfile::tempdir().unwrap();
        let store = Store::create(temp.path().join("store")).unwrap();
        let log = unanswered_call_with_counts(&store, 1, 0);
        let before = log.position("chat").unwrap();
        let result = event_input(
            "chat",
            "result",
            EventKind::ToolResult {
                call_id: "A".into(),
                outcome: ToolOutcome::Succeeded,
            },
        );
        assert!(matches!(
            log.append(OperationId::new(), result, &b"result"[..]),
            Err(ContextError::Corrupt(_))
        ));
        assert_eq!(log.position("chat").unwrap(), before);
        let cleanup = log.recover_payloads(1, 1).unwrap();
        assert_eq!((cleanup.uploads_removed, cleanup.chunks_removed), (0, 0));
    }

    #[test]
    fn an_unknown_recovery_commit_closes_each_turn_once() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("store");
        let store = Store::create(&root).unwrap();
        for session in ["first", "second"] {
            start_step(&store, "owner", session);
        }
        store.kernel.lose_next_commit_ack(&store.session.open_turns);
        assert!(matches!(
            session::end_interrupted_turns(&store.records, &store.session),
            Err(ContextError::Storage(KernelError::CommitUnknown))
        ));
        for session in ["first", "second"] {
            assert_step_interrupted(&store, "owner", session);
        }
        assert!(store.session.open_turns.iter().next().is_none());
        drop(store);

        let store = Store::open(&root).unwrap();
        for session in ["first", "second"] {
            assert_step_interrupted(&store, "owner", session);
        }
    }

    #[test]
    fn changing_the_replacement_era_invalidates_a_confirmed_digest() {
        let temp = tempfile::tempdir().unwrap();
        let mut db = Store::create(temp.path().join("store")).unwrap();
        let mut plan = db
            .plan_memory_reset(UNIX_EPOCH, Duration::from_millis(1))
            .unwrap();
        plan.new_memory_era = EraId::new();
        assert!(matches!(
            db.reset_memory(&plan, plan.digest(), UNIX_EPOCH),
            Err(StoreError::ResetDigestMismatch)
        ));
        assert_eq!(db.era(StoreModule::Memory), plan.memory_era());
    }

    #[test]
    fn a_failed_memory_reset_leaves_the_store_usable() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("store");
        let mut db = Store::create(&root).unwrap();
        let access = ContextAccess::new("owner", Actor::User);
        let old = db
            .context(access.clone())
            .unwrap()
            .define_type(OperationId::new(), TypeDefinition::note(Scope::Personal))
            .unwrap();
        let plan = db
            .plan_memory_reset(UNIX_EPOCH, Duration::from_secs(60))
            .unwrap();
        db.fail_reset_preparation = true;
        assert!(matches!(
            db.reset_memory(&plan, plan.digest(), UNIX_EPOCH),
            Err(StoreError::Kernel(KernelError::Storage(_)))
        ));
        assert_eq!(db.era(StoreModule::Memory), plan.memory_era());
        assert_eq!(db.memory().era_id(), plan.memory_era());
        let ctx = db.context(access.clone()).unwrap();
        assert_eq!(ctx.availability_epoch().unwrap(), 0);
        assert!(ctx.read(&old.pin).is_ok());
        let mut definition = TypeDefinition::note(Scope::Personal);
        definition.name = "after-failure".into();
        let written = ctx.define_type(OperationId::new(), definition).unwrap();
        drop(db);
        let mut db = Store::open(&root).unwrap();
        assert!(db
            .context(access.clone())
            .unwrap()
            .read(&written.pin)
            .is_ok());
        assert_eq!(
            db.reset_memory(&plan, plan.digest(), UNIX_EPOCH)
                .unwrap()
                .new_memory_era,
            plan.new_memory_era()
        );
        assert_eq!(db.context(access).unwrap().availability_epoch().unwrap(), 1);
    }
}
