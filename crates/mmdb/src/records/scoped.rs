use super::codec::*;
use super::*;
use crate::{EraId, OperationId};
use mmdb_kernel::{
    Batch, EraPartitions, EraState, Kernel, KernelResult, Partition, PartitionSpec, WriteGuard,
};
use serde::Serialize;
use std::collections::{BTreeMap, BTreeSet};
use ulid::Ulid;

pub(crate) struct Availability {
    pub epoch: u64,
    pub redaction_epoch: u64,
}

pub(crate) trait Gate: Send + Sync {
    fn admits(
        &self,
        owner: &str,
        header: &RecordHeader,
        active: bool,
        now: &Availability,
    ) -> ContextResult<bool>;
}

const HEADERS: &str = "records_headers_v1";
const HEADS: &str = "records_heads_v1";
const PAYLOADS: &str = "records_payloads_v1";
const PENDING: &str = "records_pending_v1";
const LIFECYCLE: &str = "records_lifecycle_v1";
const OPERATIONS: &str = "records_operations_v1";
pub(crate) const MEMORY_PARTITIONS: &[PartitionSpec] = &[
    PartitionSpec::Inline(HEADERS),
    PartitionSpec::Inline(HEADS),
    PartitionSpec::LargeValues(PAYLOADS),
    PartitionSpec::Inline(PENDING),
];
pub(crate) const SESSION_PARTITIONS: &[PartitionSpec] = &[
    PartitionSpec::Inline(HEADERS),
    PartitionSpec::Inline(HEADS),
    PartitionSpec::LargeValues(PAYLOADS),
    PartitionSpec::Inline(PENDING),
    PartitionSpec::Inline(LIFECYCLE),
    PartitionSpec::Inline(OPERATIONS),
];

pub(crate) struct RecordTable {
    headers: Partition,
    heads: Partition,
    pub(super) payloads: Partition,
    pub(super) pending: Partition,
}

impl RecordTable {
    pub(crate) fn open(era: &EraPartitions) -> KernelResult<Self> {
        Ok(Self {
            headers: era.partition(HEADERS)?,
            heads: era.partition(HEADS)?,
            payloads: era.partition(PAYLOADS)?,
            pending: era.partition(PENDING)?,
        })
    }
}

pub(crate) struct Records {
    pub(super) kernel: Kernel,
    session: RecordTable,
    memory: RecordTable,
    pub(super) lifecycle: Partition,
    operations: Partition,
    gate: Box<dyn Gate>,
}

impl Records {
    pub(crate) fn open(
        kernel: Kernel,
        session: &EraPartitions,
        memory: &EraPartitions,
        gate: Box<dyn Gate>,
    ) -> KernelResult<Self> {
        Ok(Self {
            kernel,
            session: RecordTable::open(session)?,
            memory: RecordTable::open(memory)?,
            lifecycle: session.partition(LIFECYCLE)?,
            operations: session.partition(OPERATIONS)?,
            gate,
        })
    }
    pub(crate) fn replace_memory_table(&mut self, table: RecordTable) {
        self.memory = table;
    }

    pub(crate) fn scoped(&self, access: ContextAccess) -> Scoped<'_> {
        Scoped {
            records: self,
            access,
        }
    }
}

pub(crate) struct Scoped<'a> {
    pub(super) records: &'a Records,
    access: ContextAccess,
}

pub(crate) struct HeadPage {
    pub heads: Vec<RecordPin>,
    pub next: Option<ContextRef>,
}

impl Scoped<'_> {
    pub(crate) fn readable_header_locked(&self, pin: &RecordPin) -> ContextResult<RecordHeader> {
        let header = self.header_locked(pin)?;
        if !self.available_locked(pin, false)? {
            return Err(ContextError::Unavailable(pin.record.clone()));
        }
        Ok(header)
    }

    pub(crate) fn access(&self) -> &ContextAccess {
        &self.access
    }
    pub(crate) fn lock(&self) -> WriteGuard<'_> {
        self.records.kernel.write_lock()
    }
    pub(crate) fn batch(&self) -> Batch {
        self.records.kernel.batch()
    }
    pub(super) fn table(&self, module: StoreModule) -> &RecordTable {
        match module {
            StoreModule::Session => &self.records.session,
            StoreModule::Memory => &self.records.memory,
        }
    }

    pub(crate) fn era(&self, module: StoreModule) -> EraId {
        self.records.kernel.era(module.id())
    }

    pub(crate) fn scan_heads(
        &self,
        kind: RecordKind,
        after: Option<&ContextRef>,
        limit: usize,
    ) -> ContextResult<HeadPage> {
        use std::ops::Bound::{Excluded, Included, Unbounded};
        let mut prefix = owner_key(&self.access.owner);
        prefix.push(kind_byte(kind));
        let start = if let Some(after) = after {
            self.check_ref(after)?;
            if after.kind != kind {
                return Err(ContextError::InvalidInput(
                    "record cursor has the wrong family".into(),
                ));
            }
            Excluded(record_key(after))
        } else {
            Included(prefix.clone())
        };
        let mut heads = Vec::new();
        for entry in self
            .table(kind.module())
            .heads
            .range((start, Unbounded))
            .take(4096)
        {
            let (key, revision) = entry?;
            if !key.starts_with(&prefix) {
                break;
            }
            if heads.len() == limit.min(4096) {
                let next = heads.last().map(|pin: &RecordPin| pin.record.clone());
                return Ok(HeadPage { heads, next });
            }
            let id = key[prefix.len()..]
                .try_into()
                .map_err(|_| ContextError::Corrupt("invalid head key".into()))?;
            let revision = revision
                .as_ref()
                .try_into()
                .map_err(|_| ContextError::Corrupt("invalid head revision".into()))?;
            let record = ContextRef {
                era: self.era(kind.module()),
                owner: self.access.owner.clone(),
                kind,
                id: Ulid(u128::from_be_bytes(id)),
            };
            heads.push(RecordPin {
                record,
                revision: u64::from_be_bytes(revision),
            });
        }
        let next = if heads.len() == 4096 {
            heads.last().map(|pin| pin.record.clone())
        } else {
            None
        };
        Ok(HeadPage { heads, next })
    }
    #[cfg(test)]
    pub(crate) fn test_payloads(&self, kind: RecordKind) -> &Partition {
        &self.table(kind.module()).payloads
    }
    #[cfg(test)]
    pub(crate) fn lose_next_commit_ack(&self) {
        self.records
            .kernel
            .lose_next_commit_ack(&self.records.operations);
    }
}
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum SourceCheck {
    Current,
    Historical,
    Copy,
    // A new event must fit the graph budget for subsequent current checks.
    // Below a readable session snapshot, charge only the shape of its provenance.
    BudgetOnly,
}

pub(crate) type Memo = BTreeMap<(RecordPin, usize, SourceCheck), usize>;

#[derive(Default)]
struct SourceWalk {
    count: usize,
    path: BTreeSet<RecordPin>,
    scopes: BTreeSet<Scope>,
}

impl SourceWalk {
    fn for_new_record() -> Self {
        Self {
            count: 1,
            ..Self::default()
        }
    }

    fn other_scopes(mut self, own: &Scope) -> Vec<Scope> {
        self.scopes.remove(own);
        self.scopes.into_iter().collect()
    }
}

impl Scoped<'_> {
    pub(crate) fn check_scope(&self, scope: &Scope) -> ContextResult<()> {
        if !self.access.scopes.contains(scope) {
            return Err(ContextError::AccessDenied);
        }
        Ok(())
    }

    pub(crate) fn check_ref(&self, record: &ContextRef) -> ContextResult<()> {
        if record.owner != self.access.owner {
            return Err(ContextError::AccessDenied);
        }
        match self
            .records
            .kernel
            .era_state(record.kind.module().id(), record.era)
        {
            EraState::Current => Ok(()),
            EraState::Retired => Err(ContextError::Reset(record.clone())),
            EraState::Unknown => Err(ContextError::AccessDenied),
        }
    }

    pub(crate) fn fresh_pin(&self, kind: RecordKind) -> RecordPin {
        RecordPin {
            record: ContextRef {
                era: self.era(kind.module()),
                owner: self.access.owner.clone(),
                kind,
                id: Ulid::new(),
            },
            revision: 1,
        }
    }

    pub(crate) fn provenance(&self) -> Provenance {
        Provenance {
            actor: self.access.actor,
            agent: self.access.agent.clone(),
            session: self.access.session.clone(),
            run: self.access.run.clone(),
            parent_run: self.access.parent_run.clone(),
        }
    }

    pub(crate) fn head_locked(&self, record: &ContextRef) -> ContextResult<RecordHeader> {
        self.check_ref(record)?;
        let revision = self
            .table(record.kind.module())
            .heads
            .get(record_key(record))?
            .ok_or_else(|| ContextError::NotFound(record.clone()))?;
        let revision: [u8; 8] = revision
            .as_ref()
            .try_into()
            .map_err(|_| ContextError::Corrupt("invalid head revision".into()))?;
        self.header_locked(&RecordPin {
            record: record.clone(),
            revision: u64::from_be_bytes(revision),
        })
    }

    pub(crate) fn header_locked(&self, pin: &RecordPin) -> ContextResult<RecordHeader> {
        self.check_ref(&pin.record)?;
        if pin.revision == 0 {
            return Err(ContextError::InvalidInput(
                "revision must be positive".into(),
            ));
        }
        let bytes = self
            .table(pin.record.kind.module())
            .headers
            .get(revision_key(pin))?
            .ok_or_else(|| ContextError::NotFound(pin.record.clone()))?;
        let header: RecordHeader = decode(&bytes)?;
        if header.pin != *pin {
            return Err(ContextError::Corrupt(
                "record key and identity disagree".into(),
            ));
        }
        self.check_scope(&header.scope)?;
        Ok(header)
    }

    pub(crate) fn available_locked(&self, pin: &RecordPin, active: bool) -> ContextResult<bool> {
        self.available_memoized(pin, active, &mut BTreeMap::new())
    }

    pub(crate) fn available_memoized(
        &self,
        pin: &RecordPin,
        active: bool,
        memo: &mut Memo,
    ) -> ContextResult<bool> {
        let check = if active {
            SourceCheck::Current
        } else {
            SourceCheck::Historical
        };
        self.available_walk(pin, check, None, 0, &mut SourceWalk::default(), memo)
    }

    fn available_walk(
        &self,
        pin: &RecordPin,
        check: SourceCheck,
        excluded: Option<&ContextRef>,
        depth: usize,
        walk: &mut SourceWalk,
        memo: &mut Memo,
    ) -> ContextResult<bool> {
        let prior = walk.count;
        walk.count += 1;
        if depth > 32 || walk.count > 4096 {
            return Err(ContextError::BudgetExceeded);
        }
        if excluded == Some(&pin.record) || !walk.path.insert(pin.clone()) {
            return Ok(false);
        }
        // Charge the original cost when reusing a successful, acyclic walk.
        let key = (pin.clone(), depth, check);
        if let Some(cost) = memo.get(&key) {
            walk.count = prior + cost;
            walk.path.remove(pin);
            return if walk.count > 4096 {
                Err(ContextError::BudgetExceeded)
            } else {
                Ok(true)
            };
        }
        let header = match self.header_locked(pin) {
            Ok(header) => header,
            // Only a transitive source can terminate at a retired era. Direct
            // inputs are checked by the caller before their dependencies are walked.
            Err(ContextError::Reset(_)) if depth > 0 => {
                walk.path.remove(pin);
                return Ok(true);
            }
            Err(ContextError::NotFound(_) | ContextError::AccessDenied) => return Ok(false),
            Err(error) => return Err(error),
        };
        if check != SourceCheck::BudgetOnly {
            if !self.header_available(&header, check == SourceCheck::Current)? {
                return Ok(false);
            }
            walk.scopes.insert(header.scope.clone());
            walk.scopes.extend(header.source_scopes.iter().cloned());
        }
        let session_record = pin.record.kind.module() == StoreModule::Session;
        let source_check = match (session_record, check) {
            (true, SourceCheck::Historical) => None,
            (true, SourceCheck::Copy) => Some(SourceCheck::BudgetOnly),
            _ => Some(check),
        };
        if let Some(source_check) = source_check {
            for source in &header.sources {
                if !self.available_walk(source, source_check, excluded, depth + 1, walk, memo)? {
                    return Ok(false);
                }
            }
        }
        walk.path.remove(pin);
        if memo.len() < 4096 {
            memo.insert(key, walk.count - prior);
        }
        Ok(true)
    }

    fn header_available(&self, header: &RecordHeader, active: bool) -> ContextResult<bool> {
        let head = self.head_locked(&header.pin.record)?;
        if head.state == RecordState::Purged
            || header.state == RecordState::Purged
            || (active
                && (head.state != RecordState::Active
                    || head.pin != header.pin
                    || header.state != RecordState::Active))
            || !header
                .source_scopes
                .iter()
                .all(|scope| self.access.scopes.contains(scope))
        {
            return Ok(false);
        }
        if header.pin.record.kind.module() == StoreModule::Session {
            return self.records.gate.admits(
                &self.access.owner,
                header,
                active,
                &self.availability_locked()?,
            );
        }
        Ok(true)
    }
    pub(crate) fn validate_sources(
        &self,
        scope: &Scope,
        sources: &[RecordPin],
        information: InformationKind,
        revising: Option<&ContextRef>,
    ) -> ContextResult<Vec<Scope>> {
        self.check_scope(scope)?;
        if sources.is_empty() || sources.len() > MAX_CONTEXT_SOURCES {
            return Err(ContextError::InvalidInput(
                "context requires 1..=64 source references".into(),
            ));
        }
        if information == InformationKind::Instruction
            && !matches!(self.access.actor, Actor::User | Actor::Operator)
        {
            return Err(ContextError::AccessDenied);
        }
        let mut walk = SourceWalk::for_new_record();
        let mut memo = BTreeMap::new();
        for source in sources {
            let header = self.header_locked(source)?;
            if source.record.kind == RecordKind::Type {
                return Err(ContextError::InvalidInput(
                    "type definitions are not observation sources".into(),
                ));
            }
            if header.scope != *scope && header.scope != Scope::Personal {
                return Err(ContextError::AccessDenied);
            }
            if !self.available_walk(
                source,
                SourceCheck::Current,
                revising,
                1,
                &mut walk,
                &mut memo,
            )? {
                if revising.is_some() {
                    return Err(ContextError::InvalidInput("revision sources must be available and cannot depend on the revised record".into()));
                }
                return Err(ContextError::Unavailable(source.record.clone()));
            }
        }
        Ok(walk.other_scopes(scope))
    }

    /// Original snapshots may copy a historical version or a type descriptor.
    pub(crate) fn validate_copy_sources(
        &self,
        scope: &Scope,
        sources: &[RecordPin],
    ) -> ContextResult<Vec<Scope>> {
        self.check_scope(scope)?;
        if sources.len() > MAX_CONTEXT_SOURCES {
            return Err(ContextError::InvalidInput(
                "copy has too many source references".into(),
            ));
        }
        let mut walk = SourceWalk::for_new_record();
        let mut memo = BTreeMap::new();
        for source in sources {
            let header = self.header_locked(source)?;
            if header.scope != *scope && header.scope != Scope::Personal {
                return Err(ContextError::AccessDenied);
            }
            if !self.available_walk(source, SourceCheck::Copy, None, 1, &mut walk, &mut memo)? {
                return Err(ContextError::Unavailable(source.record.clone()));
            }
        }
        Ok(walk.other_scopes(scope))
    }

    pub(crate) fn digest<T: Serialize>(
        &self,
        domain: &'static str,
        input: &T,
    ) -> ContextResult<[u8; 32]> {
        let mut h = self.records.kernel.keyed_hasher("context-operation");
        h.update(domain.as_bytes());
        h.update(&encode(&(&self.access.owner, self.provenance(), input))?);
        Ok(*h.finalize().as_bytes())
    }

    pub(crate) fn has_operation(&self, operation: OperationId) -> ContextResult<bool> {
        Ok(self
            .records
            .operations
            .contains_key(operation_key(&self.access.owner, operation))?)
    }

    fn stored_operation(&self, operation: OperationId) -> ContextResult<Option<StoredOperation>> {
        self.records
            .operations
            .get(operation_key(&self.access.owner, operation))?
            .map(|bytes| decode(&bytes))
            .transpose()
    }

    pub(crate) fn operation_receipt(
        &self,
        operation: OperationId,
    ) -> ContextResult<Option<WriteReceipt>> {
        let _guard = self.lock();
        let stored = self.stored_operation(operation)?;
        if let Some(stored) = &stored {
            self.header_locked(&stored.receipt.pin)?;
        }
        Ok(stored.map(|stored| stored.receipt))
    }

    pub(crate) fn replay(
        &self,
        operation: OperationId,
        digest: [u8; 32],
    ) -> ContextResult<Option<WriteReceipt>> {
        let Some(stored) = self.stored_operation(operation)? else {
            return Ok(None);
        };
        if stored.digest != digest {
            return Err(ContextError::IdempotencyConflict);
        }
        self.header_locked(&stored.receipt.pin)?;
        Ok(Some(stored.receipt))
    }

    pub(crate) fn commit_operation(
        &self,
        mut batch: mmdb_kernel::Batch,
        operation: OperationId,
        digest: [u8; 32],
        receipt: WriteReceipt,
    ) -> ContextResult<WriteReceipt> {
        // An operation ID names one write across both modules; a receipt is never replaced.
        if self.has_operation(operation)? {
            return Err(ContextError::IdempotencyConflict);
        }
        batch.insert(
            &self.records.operations,
            operation_key(&self.access.owner, operation),
            encode(&StoredOperation {
                digest,
                receipt: receipt.clone(),
            })?,
        );
        batch
            .commit()
            .map_err(|_| ContextError::CommitUnknown(operation))?;
        Ok(receipt)
    }

    pub(crate) fn put_header(
        &self,
        batch: &mut mmdb_kernel::Batch,
        header: &RecordHeader,
    ) -> ContextResult<()> {
        let table = self.table(header.pin.record.kind.module());
        batch.insert(&table.headers, revision_key(&header.pin), encode(header)?);
        batch.insert(
            &table.heads,
            record_key(&header.pin.record),
            header.pin.revision.to_be_bytes(),
        );
        Ok(())
    }

    pub(crate) fn next_revision(
        &self,
        expected: &RecordPin,
    ) -> ContextResult<(RecordHeader, RecordPin)> {
        let head = self.head_locked(&expected.record)?;
        if head.pin.revision != expected.revision {
            return Err(ContextError::RevisionConflict {
                expected: expected.revision,
                actual: head.pin.revision,
            });
        }
        if head.state == RecordState::Purged {
            return Err(ContextError::Unavailable(expected.record.clone()));
        }
        let revision = expected
            .revision
            .checked_add(1)
            .ok_or_else(|| ContextError::Corrupt("revision overflow".into()))?;
        Ok((
            head,
            RecordPin {
                record: expected.record.clone(),
                revision,
            },
        ))
    }
}
