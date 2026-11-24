# 连续会话日志与原文搜索: implement spec

Feature: `docs/feature/session-log.md` @ `fecd633448b15ce2766b288e6c954372e65ad2bf`

This spec is open until the feature ships, and `/go-work` carries it out. Do not edit it while working it; only `to-implement-spec` changes it. Before work starts or resumes, check that the hash above still matches `git hash-object docs/feature/session-log.md`; if it does not, the feature changed, so stop and rerun `to-implement-spec`. Each step is one commit whose message ends with the trailer `Spec-Step: session-log#<n>`. The commit that lands the last step sets the feature doc's Status to `shipped` and moves this file to `docs/spec/archived/<YYYY-MM-DD>-session-log.md` with `git mv`, dated that day. Nothing reads an archived spec again.

新建 `mmdb-kernel` crate 作为存储内核：持有 fjall、租约、格式清单、按模块的 era 和写锁，并按「模块.era.名称」打开分区。`mmdb` 中新增 `records` 层，存放两个模块共用的记录头、载荷、回执、可用性 epoch 和跨模块的来源检查。会话日志与检查点从 `context` 搬进 `session` 模块，由 `SessionLog` 句柄提供。`Store` 是组装根：一个目录里放会话模块和记忆模块，记忆模块包括原生记忆和领域上下文。重置记忆时，记忆模块原地换 era，会话模块不动。

## Behavior map

| Feature doc line | Owner | Test |
| --- | --- | --- |
| Who: 可信 harness 以 `ContextAccess` 访问 | `records::ContextAccess::validate`，由 `Store::session_log` 和 `Store::context` 调用 | `session_log_and_context_reject_the_same_invalid_access`（新） |
| Who: access 绑定 session 时只能追加该会话 | `session::SessionLog::append` | `access_bound_to_a_session_appends_only_to_it`（新） |
| Who: 用户消息、列出会话、撤回或清除日志事件的 actor 限制 | `session::SessionLog::{append, sessions, retract, purge}` | `log_permissions_follow_the_actor`（新） |
| Who: 重置记忆由持有租约的进程凭摘要提交 | `Store::reset_memory` | `memory_reset_needs_the_exact_plan_digest`（新） |
| Entry: `Store::create/open`、`store.session_log(access)`，检查点在同一句柄上，方法名不带 history | `Store`、`session::SessionLog` | `independent_harnesses_share_types_history_and_relations_after_reopen`（改写） |
| Does: 两个模块同在一个存储，共用租约与批次提交 | `mmdb_kernel::Kernel` | `a_second_open_of_a_store_is_busy`（kernel）、`purged_sources_stay_unavailable_after_reopen_recovery_and_replay` |
| Does: 写入时分配单调序号，切换窗口仍追加到原会话 | `SessionLog::append` | `concurrent_appends_allocate_one_session_order` |
| Does 追加：事件 ID、预期位置、重试取回原序号、同身份不同内容冲突、完整写入后一起发布 | `SessionLog::append` | `append_retries_preserve_identity_and_order_even_after_later_events`、`committed_history_survives_a_lost_acknowledgement_without_duplication` |
| Does 消息：片段共用消息 ID，一个片段不可用则整条消息不可用；工具结果关联同会话唯一调用 | `session` 的消息状态与 `LogGate` | `a_withdrawn_fragment_hides_its_whole_message`（新）、`tool_results_require_their_call_and_follow_its_purge_state` |
| Does 来源：日志与对象互为来源，跨模块检查可用性，副本随清除失效 | `records::Scoped` 的来源检查 | `history_copies_old_object_and_type_pins_but_purge_blocks_object_copies`、`copied_history_tracks_source_availability_and_session_catalog_pages`、`tool_results_link_historical_call_sources_and_still_follow_purge` |
| Does 读取：按位置翻页、按 pin 读载荷、列出会话、工具调用配对 | `SessionLog::{events, read_payload, sessions, tool_pair}` | `complete_large_history_is_searchable_across_chunks_and_query_pages`、`copied_history_tracks_source_availability_and_session_catalog_pages` |
| Does 搜索：固定引用、字节位置、续查游标、跨块与跨页、大小写规则 | `SessionLog::search` | `complete_large_history_is_searchable_across_chunks_and_query_pages` |
| Does 重置记忆：计划、凭摘要提交、一次提交换 era、旧记录立刻不可见、清理旧分区、会话模块不受影响 | `Store::{plan_memory_reset, reset_memory}`、`Kernel::reset_module` | `memory_reset_keeps_the_session_log_and_hides_old_memory`（新） |
| Does 重置记忆之后：日志照常可读且照常通过当前检查；旧记忆来源显示已重置；直接引用旧记忆报已重置 | `records::Scoped::check_ref` 与来源检查 | `log_events_that_copied_reset_memory_stay_readable_and_current`（新） |
| Does 重置记忆之后：重置前的检查点载荷可读、不能用于当前恢复 | `session::LogGate` | `checkpoints_saved_before_a_memory_reset_cannot_resume`（新） |
| Does 整库重置：旧目录进隔离区，所有模块换新 era | `mmdb_kernel::store_format::commit_reset`、`Kernel::open` | `whole_store_reset_quarantines_the_directory_and_starts_new_eras`（改写自 `reset_creates_a_new_context_era_without_importing_previous_history`） |
| Fails: 已重置、已清除、另一 owner、另一存储（含整库重置之前）各自报错 | `records::Scoped::check_ref` | `reset_purged_and_foreign_refs_fail_differently`（新）、`format_and_store_identity_prevent_accidental_adoption`（改写） |
| Fails: 追加中断不推进位置，`recover_payloads` 有界清理 | `SessionLog::{append, recover_payloads}` | `incomplete_uploads_never_advance_history_and_can_be_cleaned`、`actual_process_interruption_leaves_no_partial_visible_history` |
| Fails: 重置要么整体生效要么没发生；没做完的清理下次打开时继续 | `Kernel::{reset_module, open}` | `module_reset_cleanup_resumes_after_interruption`（kernel，新） |
| Fails: 摘要不符或计划过期不做任何改变 | `Store::reset_memory` | `memory_reset_refuses_a_wrong_expired_or_stale_plan`（新） |
| Fails: 旧格式目录被拒绝，不迁移 | `Kernel::open`（`require_managed_store`） | `format_and_store_identity_prevent_accidental_adoption`（改写） |
| Limits: 32 KiB 分块、单次读 1 MiB、搜索 4,096 个事件或 16 MiB | `records::Scoped::put_payload`、`SessionLog::{read_payload, search}` | `complete_large_history_is_searchable_across_chunks_and_query_pages`、`log_reads_and_searches_refuse_budgets_over_their_limits`（新） |
| Limits: 读输入流期间持有存储写锁 | `SessionLog::append`（`Kernel::write_lock`） | 不测：是代价，不是拒绝 |
| Keeps: 日志、载荷、检查点保留到清除或整库重置；旧记忆分区删除，不留副本 | `Kernel::reset_module` | `memory_reset_keeps_the_session_log_and_hides_old_memory`、`module_reset_deletes_the_old_era_partitions`（kernel，新） |
| 目标 Part 1：会话模块不直接使用 fjall | 依赖方向 | `store_modules_do_not_name_fjall`（新，`tests/layering.rs`） |

Replaces: `reset_creates_a_new_context_era_without_importing_previous_history`（改写为整库重置测试）；`native_memory.rs` 中检查 `native_internal_metadata_v1` 与 `StoreEraMismatch` 的测试（元数据改由内核持有，改写为 kernel 测试 `store_metadata_is_created_once_and_checked_against_the_manifest`）。

## Modules

### `mmdb-kernel`（new crate）

`store_format` 原样搬入。内核只认识模块名、era 和分区名，不认识任何一类 agent 数据。

```rust
pub mod store_format; // 自 crates/mmdb/src/store_format.rs 搬入，接口不变

/// 存储模块的生命周期标识。
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct EraId(pub Ulid);
/// 调用方给出的操作标识，用于回执与重试。
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct OperationId(pub Ulid);
// 两者都有 new()、as_ulid()、Default、Display，与今天 native_memory 的 id_type! 相同。

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ModuleId(pub &'static str);

pub struct ModuleSpec {
    pub id: ModuleId,
    pub partitions: &'static [&'static str],
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EraState { Current, Retired, Unknown }

/// 可廉价克隆的存储句柄；最后一个克隆释放时释放租约。
#[derive(Clone)]
pub struct Kernel { /* Arc<Inner>: keyspace, lease, manifest, meta, eras, write_lock, digest_key */ }

impl Kernel {
    pub fn create(root: &Path, format: &StoreFormatDescriptor, modules: &'static [ModuleSpec]) -> KernelResult<Self>;
    pub fn open(root: &Path, format: &StoreFormatDescriptor, modules: &'static [ModuleSpec]) -> KernelResult<Self>;
    pub fn canonical_root(&self) -> &Path;
    pub fn store_era(&self) -> &StoreEraId;
    pub fn era(&self, module: ModuleId) -> EraId;
    pub fn era_state(&self, module: ModuleId, era: EraId) -> EraState;
    /// 当前 era 下名为 `name` 的分区；`name` 必须在该模块的 spec 里。
    pub fn partition(&self, module: ModuleId, name: &'static str) -> KernelResult<Partition>;
    /// SyncAll 持久批次，可以跨模块。
    pub fn batch(&self) -> Batch;
    /// 存储级写锁，所有模块共用。
    pub fn write_lock(&self) -> WriteGuard<'_>;
    pub fn digest_key(&self) -> &[u8; 32];
    /// 第 8 步加入：建好 `new_era` 的空分区，一次提交切换当前 era 并登记旧 era 为 Retired，
    /// 再删除旧 era 的分区。调用方必须已经丢弃该模块的全部 `Partition`。
    pub fn reset_module(&self, module: ModuleId, new_era: EraId) -> KernelResult<ModuleResetReport>;
}

pub struct ModuleResetReport { pub old_era: EraId, pub new_era: EraId, pub cleanup_pending: bool }

#[derive(Clone)]
pub struct Partition { /* fjall::PartitionHandle */ }
impl Partition {
    pub fn get(&self, key: impl AsRef<[u8]>) -> KernelResult<Option<Bytes>>;
    pub fn contains_key(&self, key: impl AsRef<[u8]>) -> KernelResult<bool>;
    pub fn insert(&self, key: impl AsRef<[u8]>, value: impl AsRef<[u8]>) -> KernelResult<()>;
    pub fn remove(&self, key: impl AsRef<[u8]>) -> KernelResult<()>;
    pub fn iter(&self) -> impl DoubleEndedIterator<Item = KernelResult<(Bytes, Bytes)>> + '_;
    pub fn range<K: AsRef<[u8]>, R: RangeBounds<K>>(&self, range: R)
        -> impl DoubleEndedIterator<Item = KernelResult<(Bytes, Bytes)>> + '_;
    pub fn prefix(&self, prefix: impl AsRef<[u8]>)
        -> impl DoubleEndedIterator<Item = KernelResult<(Bytes, Bytes)>> + '_;
}

pub struct Batch { /* fjall::Batch */ }
impl Batch {
    pub fn insert(&mut self, partition: &Partition, key: impl AsRef<[u8]>, value: impl AsRef<[u8]>);
    pub fn remove(&mut self, partition: &Partition, key: impl AsRef<[u8]>);
    pub fn commit(self) -> KernelResult<()>;
}

/// 存储引擎返回的值；按 `&[u8]` 使用。
#[derive(Clone)]
pub struct Bytes(/* fjall::Slice */);
impl std::ops::Deref for Bytes { type Target = [u8]; }
impl AsRef<[u8]> for Bytes {}

pub struct WriteGuard<'a>(/* parking_lot::MutexGuard<'a, ()> */);

#[derive(Debug)]
pub enum KernelError {
    Format(StoreFormatError),
    Storage(String),
    Corrupt(String),
    UnknownPartition { module: &'static str, name: &'static str },
}
pub type KernelResult<T> = Result<T, KernelError>;
```

- Invariants: 模块分区名为 `<module>.<era>.<name>`；内核自己的元数据在未限定名的 `kernel_v1` 分区。每个模块只有一个 Current era。Retired 只记录本存储内被 `reset_module` 换下的 era；另一存储的 era，以及整库重置之前的 era，都是 Unknown。`digest_key` 和 store era 在存储初始化时写一次，之后不变。
- Ordering: `create`/`open` 先取得租约（会先恢复中断的整库重置），再检查格式，最后打开 keyspace。`open` 删除所有不属于当前 era 的模块分区，然后才返回。`reset_module` 依次建新分区、提交切换、删旧分区。
- Errors: 格式不符返回 `Format(FormatMismatch)`，目录被占返回 `Format(StoreBusy)`；元数据缺失或与清单不一致返回 `Corrupt`；分区名不在 spec 里返回 `UnknownPartition`。`reset_module` 在提交前失败时什么也没变；提交后删分区失败只把 `cleanup_pending` 置为 true，下次 `open` 接着删。
- Configuration: 无。
- Performance: `open` 打开每个模块 spec 里的分区，并删掉残留分区，开销与分区数成正比。`reset_module` 建一组空分区、提交一个小批次、删除一组分区，开销与记录数无关。

Behind the seam: fjall 的 keyspace、分区句柄、批次持久化方式和名称规则；元数据编码；残留分区的识别。

Call site（`Store::open`）：

```rust
let kernel = Kernel::open(root, &StoreFormatDescriptor::new(STORE_FORMAT_ID)?, &MODULES)?;
let session = SessionParts::open(&kernel)?; // kernel.partition(SESSION, "session_order_v1")? …
let memory = MemoryDatabase::on_kernel(kernel.clone())?;
```

### `mmdb::records`（new，crate 内部，加上公开的值类型）

两个模块共用的记录层：记录头格式、修订、载荷分块、回执、按 owner 的可用性与脱敏 epoch，以及跨两张记录表的来源检查。会话模块的可用性规则通过 `Gate` 注入，所以记录层不依赖会话模块。

公开值类型（从 `context/types.rs` 和 `native_memory.rs` 搬来，在 `mmdb::` 根导出）：`Actor`、`Scope`、`TemporalFacts`、`RecordState`、`ContextAccess`、`Provenance`、`ContextRef`、`RecordPin`、`RecordKind`、`RecordHeader`、`InformationKind`、`Endpoints`、`PayloadDescriptor`、`PayloadPage`、`WriteReceipt`、`LogEvent`、`EventKind`、`MessageStatus`、`ToolOutcome`、`CheckpointMetadata`、`CheckpointScope`、`ContextError`、`ContextResult`，以及 `PAYLOAD_CHUNK_BYTES`、`MAX_PAYLOAD_PAGE_BYTES`、`MAX_RECORD_BYTES`、`MAX_CONTEXT_NAME_BYTES`、`MAX_CONTEXT_SOURCES`、`MAX_ACCESS_SCOPES`。`native_memory` 不再导出 `Actor`、`Scope`、`TemporalFacts`、`RecordState`、`EraId`、`OperationId`。

```rust
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StoreModule { Session, Memory }

impl RecordKind {
    /// Event、Checkpoint 属于会话模块，其余属于记忆模块。
    pub fn module(self) -> StoreModule;
}

impl ContextAccess {
    /// 今天 `MemoryDatabase::context` 里的校验：名称、子运行、1..=32 个 scope。
    pub(crate) fn validate(&self) -> ContextResult<()>;
}

pub enum ContextError {
    Storage(KernelError),          // 取代 Native(MemoryError) 和 From<fjall::Error>
    Io(std::io::Error),
    Serialization(serde_json::Error),
    InvalidInput(String),
    Corrupt(String),
    UnsupportedFormat,
    AccessDenied,
    NotFound(ContextRef),
    Unavailable(ContextRef),
    /// 第 8 步加入：引用属于本存储里一个已重置模块的旧 era。
    Reset(ContextRef),
    RevisionConflict { expected: u64, actual: u64 },
    AvailabilityConflict { expected: u64, actual: u64 },
    PositionConflict { expected: u64, actual: u64 },
    TypeConflict(String),
    IdempotencyConflict,
    CommitUnknown(OperationId),
    BudgetExceeded,
}

/// 一个 owner 在检查时刻的可用性状态，交给 `Gate` 使用。
pub(crate) struct Availability { pub epoch: u64, pub redaction_epoch: u64, pub memory_era: EraId }

/// 会话模块记录的额外可用性规则；`Records` 只在会话表的记录上调用它。
pub(crate) trait Gate: Send + Sync {
    fn admits(&self, owner: &str, header: &RecordHeader, active: bool, now: &Availability)
        -> ContextResult<bool>;
}

pub(crate) struct RecordTable { headers: Partition, heads: Partition, payloads: Partition, pending: Partition, operations: Partition }
impl RecordTable { pub(crate) fn open(kernel: &Kernel, module: ModuleId) -> KernelResult<Self>; }

pub(crate) struct Records {
    kernel: Kernel,
    session: RecordTable,
    memory: RecordTable,
    lifecycle: Partition, // 会话模块：按 owner 的可用性与脱敏 epoch
    gate: Box<dyn Gate>,
}
impl Records {
    pub(crate) fn new(kernel: Kernel, gate: Box<dyn Gate>) -> KernelResult<Self>;
    /// 第 8 步：记忆模块换 era 后换上新表。
    pub(crate) fn replace_memory_table(&mut self, table: RecordTable);
    pub(crate) fn scoped<'a>(&'a self, access: &'a ContextAccess) -> Scoped<'a>;
}

/// 一个 access 下的记录操作；`SessionLog` 和 `ContextStore` 各持有一个。
pub(crate) struct Scoped<'a> { records: &'a Records, access: &'a ContextAccess }
impl Scoped<'_> {
    pub(crate) fn lock(&self) -> WriteGuard<'_>;
    pub(crate) fn batch(&self) -> Batch;
    pub(crate) fn era(&self, module: StoreModule) -> EraId;
    pub(crate) fn check_scope(&self, scope: &Scope) -> ContextResult<()>;
    pub(crate) fn check_ref(&self, record: &ContextRef) -> ContextResult<()>;
    pub(crate) fn fresh_pin(&self, kind: RecordKind) -> RecordPin;
    pub(crate) fn provenance(&self) -> Provenance;
    pub(crate) fn head_locked(&self, record: &ContextRef) -> ContextResult<RecordHeader>;
    pub(crate) fn header_locked(&self, pin: &RecordPin) -> ContextResult<RecordHeader>;
    pub(crate) fn available_locked(&self, pin: &RecordPin, active: bool) -> ContextResult<bool>;
    pub(crate) fn available_memoized(&self, pin: &RecordPin, active: bool, memo: &mut Memo) -> ContextResult<bool>;
    pub(crate) fn validate_sources(&self, scope: &Scope, sources: &[RecordPin], information: InformationKind, revising: Option<&ContextRef>) -> ContextResult<()>;
    pub(crate) fn validate_copy_sources(&self, scope: &Scope, sources: &[RecordPin]) -> ContextResult<()>;
    pub(crate) fn digest<T: Serialize>(&self, domain: &str, input: &T) -> ContextResult<[u8; 32]>;
    pub(crate) fn content_digest(&self, bytes: &[u8]) -> [u8; 32];
    pub(crate) fn replay(&self, module: StoreModule, operation: OperationId, digest: [u8; 32]) -> ContextResult<Option<WriteReceipt>>;
    pub(crate) fn operation_receipt(&self, module: StoreModule, operation: OperationId) -> ContextResult<Option<WriteReceipt>>;
    pub(crate) fn commit_operation(&self, module: StoreModule, batch: Batch, operation: OperationId, digest: [u8; 32], receipt: WriteReceipt) -> ContextResult<WriteReceipt>;
    pub(crate) fn put_header(&self, batch: &mut Batch, header: &RecordHeader) -> ContextResult<()>;
    pub(crate) fn put_payload(&self, batch: &mut Batch, pin: &RecordPin, bytes: &[u8]);
    pub(crate) fn stage_upload(&self, pin: &RecordPin, operation: OperationId) -> ContextResult<()>;
    pub(crate) fn put_chunk(&self, pin: &RecordPin, chunk: u64, bytes: &[u8]) -> ContextResult<()>;
    pub(crate) fn payload_slice_locked(&self, header: &RecordHeader, offset: u64, limit: usize) -> ContextResult<PayloadPage>;
    pub(crate) fn small_payload_locked(&self, header: &RecordHeader) -> ContextResult<Vec<u8>>;
    pub(crate) fn next_revision(&self, expected: &RecordPin) -> ContextResult<(RecordHeader, RecordPin)>;
    /// 撤回或清除的共同部分：新修订头、epoch 推进、`redacts` 时同批推进脱敏 epoch、清除时登记待清理载荷。
    /// 调用方再往同一批次加入自己的索引变更。
    pub(crate) fn state_change(&self, batch: &mut Batch, operation: OperationId, expected: &RecordPin, state: RecordState, redacts: bool) -> ContextResult<(RecordHeader, RecordHeader)>;
    pub(crate) fn advance_epoch(&self, batch: &mut Batch) -> ContextResult<u64>;
    pub(crate) fn availability_epoch_locked(&self) -> ContextResult<u64>;
    pub(crate) fn redaction_epoch_locked(&self) -> ContextResult<u64>;
    pub(crate) fn recover_payloads(&self, module: StoreModule, max_chunks: usize, max_uploads: usize) -> ContextResult<RecoveryReport>;
}
```

- Invariants: 记录头和修订头存放在 `kind.module()` 对应的表里，键编码与今天相同（owner 段 + 种类字节 + id）。epoch 只增不减，存放在会话模块，重置记忆不影响它。`check_ref`：owner 不符返回 `AccessDenied`；era 为该模块的 Current 时通过；为 Retired 时返回 `Reset`；为 Unknown 时返回 `AccessDenied`。来源检查遇到 `Reset` 的来源时，把它当作检查的终点，在 active 与非 active 两种检查里都算可用；遇到 `NotFound` 或 `AccessDenied` 的来源时算不可用。只有会话表的记录才调用 `Gate`。
- Ordering: 每个公开操作先取 `lock()`，在同一个 `batch()` 里写入记录、索引、epoch 和回执，最后由 `commit_operation` 提交。
- Errors: 与今天的 `ContextStore` 相同，另外加上 `Reset`。
- Configuration: 无。
- Performance: 来源检查最大深度 32、最多访问 4,096 个记录（与今天相同）。按 pin 读取一次只查一张表。

Behind the seam: 记录头与载荷的编码、32 KiB 分块与校验、回执编码、epoch 和脱敏标记的规则（今天 `checkpoints.rs` 里的 `CheckpointVisibility`）、两张表的选择。

### `mmdb::session`（new，承接 `context/history.rs`、`context/checkpoints.rs` 与 `search_history`）

```rust
pub struct SessionLog<'a> { /* Scoped<'a>, &'a SessionParts */ }

impl SessionLog<'_> {
    pub fn append<R: Read>(&self, operation: OperationId, input: EventInput, payload: R) -> ContextResult<WriteReceipt>;
    pub fn position(&self, session: &str) -> ContextResult<u64>;
    pub fn events(&self, session: &str, after_sequence: u64, limit: usize) -> ContextResult<EventPage>;
    pub fn read_payload(&self, pin: &RecordPin, offset: u64, limit: usize) -> ContextResult<PayloadPage>;
    pub fn sessions(&self, limit: usize, after: Option<&str>) -> ContextResult<SessionPage>;
    pub fn tool_pair(&self, session: &str, call_id: &str) -> ContextResult<(Option<RecordHeader>, Option<RecordHeader>)>;
    pub fn message_state(&self, session: &str, message_id: &str) -> ContextResult<Option<MessageState>>;
    pub fn search(&self, query: LogQuery) -> ContextResult<LogSearch>;
    pub fn save_checkpoint(&self, operation: OperationId, input: CheckpointInput) -> ContextResult<WriteReceipt>;
    pub fn latest_checkpoint(&self, session: &str) -> ContextResult<Option<RecordHeader>>;
    pub fn latest_run_checkpoint(&self, session: &str, run: &str) -> ContextResult<Option<RecordHeader>>;
    pub fn read_checkpoint(&self, pin: &RecordPin) -> ContextResult<CheckpointRecord>;
    pub fn head(&self, record: &ContextRef) -> ContextResult<RecordHeader>;
    pub fn inspect_header(&self, pin: &RecordPin) -> ContextResult<RecordHeader>;
    pub fn operation_receipt(&self, operation: OperationId) -> ContextResult<Option<WriteReceipt>>;
    pub fn retract(&self, operation: OperationId, expected: RecordPin) -> ContextResult<WriteReceipt>;
    pub fn purge(&self, operation: OperationId, expected: RecordPin) -> ContextResult<WriteReceipt>;
    pub fn recover_payloads(&self, max_chunks: usize, max_uploads: usize) -> ContextResult<RecoveryReport>;
}

pub struct CheckpointRecord { pub header: RecordHeader, pub payload: serde_json::Value }
// 搬入并改名的值类型：EventInput、EventPage、SessionPage、MessageState、LogQuery、LogMatch、
// LogSearch、LogSearchCursor、RecoveryReport、CheckpointInput。

/// 会话模块的 `Gate`：消息不可用时，其事件都不可用；检查点在 active 检查中，
/// 要求 epoch 与当前 epoch 相同，且 `memory_era` 是当前记忆 era；
/// 非 active 检查要求 epoch 不早于脱敏 epoch。
pub(crate) struct LogGate { messages: Partition }

pub(crate) struct SessionParts { order: Partition, heads: Partition, events: Partition, calls: Partition, results: Partition, messages: Partition, checkpoints: Partition }
```

第 7 步的改名（第 6 步搬代码时沿用旧名）：

| 今天 | 之后 |
| --- | --- |
| `append_history` / `history_position` / `history` | `append` / `position` / `events` |
| `history_sessions` / `tool_history` / `search_history` | `sessions` / `tool_pair` / `search` |
| `HistoryInput` / `HistoryEntry` / `HistoryKind` / `HistoryPage` | `EventInput` / `LogEvent` / `EventKind` / `EventPage` |
| `HistoryQuery` / `HistoryMatch` / `HistorySearch` / `HistorySearchCursor` | `LogQuery` / `LogMatch` / `LogSearch` / `LogSearchCursor` |
| `HistorySessionPage` | `SessionPage` |
| `RecordKind::History`、`RecordHeader.history` | `RecordKind::Event`、`RecordHeader.event` |
| `WriteReceipt.history_sequence`、`CheckpointInput/CheckpointMetadata.history_sequence` | `sequence` |
| `ContextError::HistoryConflict`、内部的 `validate_history_sources` | `PositionConflict`、`validate_copy_sources` |

- Invariants: 句柄只接受会话模块的记录种类（Event、Checkpoint）。别的种类返回 `InvalidInput`。行为、上限和权限与今天的 `ContextStore` 对应方法相同。撤回或清除日志事件会同批推进脱敏 epoch，并让整条消息不可用。`CheckpointMetadata` 新增 `memory_era: EraId`，由 `save_checkpoint` 写入（第 8 步）。
- Ordering: `append` 先校验输入、来源和预期位置，再登记待上传，逐块提交载荷，最后在一个批次里发布事件头、序号、索引、消息状态和回执。整个过程持有存储写锁。
- Errors: 与今天相同。`read_payload` 和 `read_checkpoint` 遇到旧 era 的引用时返回 `Reset`。
- Performance: 与今天相同。

Behind the seam: 会话键编码、消息生命周期、工具调用配对、检查点的父版本比较与 epoch 比较、子串搜索的分块扫描。

Call site（harness 恢复一个会话）：

```rust
let log = store.session_log(ContextAccess::new("local-user", Actor::System))?;
let start = log.latest_checkpoint(&session)?;
let after = start.as_ref().and_then(|h| h.checkpoint.as_ref()).map_or(0, |c| c.sequence);
let page = log.events(&session, after, 100)?;
for header in &page.entries {
    let bytes = log.read_payload(&header.pin, 0, MAX_PAYLOAD_PAGE_BYTES)?;
    // harness 把事件放进窗口
}
log.append(OperationId::new(), EventInput { session: session.clone(), event_id, scope: Scope::Personal,
    kind: EventKind::UserMessage, media_type: "text/plain".into(), occurred_at_ms,
    expected_sequence: Some(log.position(&session)?), sources: Vec::new() }, text.as_bytes())?;
```

### `mmdb::context`（changed）

```rust
pub struct ContextStore<'a> { /* Scoped<'a>, &'a ContextParts */ }
// 删去：append_history、history_position、history、history_sessions、tool_history、message_state、
//       search_history、save_checkpoint、latest_checkpoint、latest_run_checkpoint、read_payload，
//       以及 RecordBody::History、RecordBody::Checkpoint。
// 保留：类型、对象、关系、动作、实体、抽取、recall、relations、query_current_valid、read、head、
//       inspect_header、operation_receipt、retract、purge、recover_payloads、
//       validate_current、validate_current_guard、availability_epoch。
```

- Invariants: `read`、`head`、`inspect_header`、`retract`、`purge` 只接受记忆模块的种类，别的种类返回 `InvalidInput`。`validate_current` 与 `validate_current_guard` 接受两个模块的 pin。`CurrentSourcesGuard` 持有 `WriteGuard`。修订、撤回、清除和动作结束时，通过 `Scoped::advance_epoch` 推进 epoch。
- Errors: 与今天相同，另外加上 `Reset`。

Call site（从日志事件派生对象）：

```rust
let context = store.context(ContextAccess::new("local-user", Actor::Assistant))?;
context.save_object(OperationId::new(), ObjectInput { type_pin, scope: Scope::Personal,
    properties, sources: vec![event.pin.clone()], information: InformationKind::Inference,
    temporal: TemporalFacts::observed_at(now) })?;
```

### `native_memory::MemoryDatabase`（changed）

```rust
impl MemoryDatabase {
    /// 打开记忆模块当前 era 的原生分区。
    pub(crate) fn on_kernel(kernel: Kernel) -> MemoryResult<Self>;
    /// 记忆模块的 era。
    pub fn era_id(&self) -> EraId;
    // 原生记忆的其余接口不变。
}
// 删去：create、open、create_context、open_context、context、
//       NativeMetadata、native_internal_metadata_v1、StoreEraMismatch。
```

- Invariants: 写锁、持久批次和摘要密钥都来自内核。审计事件、召回案例和清除令牌里的 era 是记忆模块的 era。

### `mmdb::store`（new，组装根）

```rust
pub const STORE_FORMAT_ID: &str = "mmdb-store-v1";

pub struct Store { /* kernel, records, session: SessionParts, context: ContextParts, memory: MemoryDatabase */ }

impl Store {
    pub fn create(root: impl AsRef<Path>) -> StoreResult<Self>;
    pub fn open(root: impl AsRef<Path>) -> StoreResult<Self>;
    pub fn era(&self, module: StoreModule) -> EraId;
    pub fn session_log(&self, access: ContextAccess) -> ContextResult<SessionLog<'_>>;
    pub fn context(&self, access: ContextAccess) -> ContextResult<ContextStore<'_>>;
    pub fn memory(&self) -> &MemoryDatabase;
    pub fn plan_memory_reset(&self, issued_at: SystemTime, ttl: Duration) -> StoreResult<MemoryResetPlan>;
    pub fn reset_memory(&mut self, plan: &MemoryResetPlan, presented_digest: &str, now: SystemTime)
        -> StoreResult<MemoryResetReceipt>;
}

pub struct MemoryResetPlan { /* canonical_root, store_era, memory_era, new_memory_era, issued_at_unix_ms, expires_at_unix_ms, digest */ }
impl MemoryResetPlan {
    pub fn canonical_root(&self) -> &Path;
    pub fn memory_era(&self) -> EraId;
    pub fn new_memory_era(&self) -> EraId;
    pub fn expires_at_unix_ms(&self) -> u64;
    pub fn digest(&self) -> &str;
}

pub struct MemoryResetReceipt { pub old_memory_era: EraId, pub new_memory_era: EraId, pub cleanup_pending: bool }

#[derive(Debug)]
pub enum StoreError {
    Kernel(KernelError),
    Memory(MemoryError),
    Context(ContextError),
    ResetPlanTtl,
    ResetPlanExpired,
    ResetDigestMismatch,
    StaleResetPlan,
}
pub type StoreResult<T> = Result<T, StoreError>;
```

- Invariants: 一个目录就是一个存储，里面有会话和记忆两个模块，一次由一个进程持有租约。`reset_memory` 需要 `&mut self`，所以调用时没有任何句柄还活着。
- Ordering（`reset_memory`）：校验摘要、过期时间，以及计划里的 store era 和记忆 era 仍是当前值；再丢弃记忆模块的句柄（原生分区、上下文分区、记忆记录表）；然后调用 `Kernel::reset_module(MEMORY, plan.new_memory_era)`；最后重新打开记忆模块，换进 `Records` 和 `memory`。
- Errors: TTL 不在 (0, 15 min] 内返回 `ResetPlanTtl`；过期返回 `ResetPlanExpired`；摘要不符返回 `ResetDigestMismatch`；计划之后记忆 era 已变返回 `StaleResetPlan`。这四种都不做任何改变。内核在提交前失败，同样什么都没变。
- Configuration: 无。计划摘要用 blake3 计算，域为 `mmdb-memory-reset-v1`，覆盖目录、store era、新旧记忆 era 和时间。

Call site（miumiu 的 `memory reset`）：

```rust
let mut store = Store::open(&paths.memory_store_dir)?;
let plan = store.plan_memory_reset(SystemTime::now(), Duration::from_secs(5 * 60))?;
show(plan.canonical_root(), plan.memory_era(), plan.new_memory_era(), plan.expires_at_unix_ms(), plan.digest());
let receipt = store.reset_memory(&plan, &typed_digest, SystemTime::now())?;
reconcile_runtime_memory_era(&runtime, &receipt.new_memory_era.to_string())?;
```

## Data

- Stored:
  - `kernel_v1` 分区的键 `store` 保存 `{ store_era, digest_key, eras: {session, memory}, retired: [(module, era)] }`（JSON），唯一写入者是 `Kernel`。目录里只有清单时，第一次打开写入它；之后只有 `reset_module` 会改它。
  - 会话模块的分区：`records_headers_v1`、`records_heads_v1`、`records_payloads_v1`、`records_pending_v1`、`records_operations_v1`、`records_lifecycle_v1`、`session_order_v1`、`session_heads_v1`、`session_events_v1`、`session_tool_calls_v1`、`session_tool_results_v1`、`session_messages_v1`、`session_checkpoints_v1`。写入者：`records` 和 `session`。
  - 记忆模块的分区：同名的五个 `records_*_v1`（不含 lifecycle）；`context_type_names_v1`、`context_action_names_v1`、`context_postings_v1`、`context_record_terms_v1`、`context_adjacency_v1`、`context_entity_identity_v1`、`context_extraction_receipts_v1`、`context_extraction_incomplete_v1`；以及除 `native_internal_metadata_v1` 以外的全部 `native_*_v1`。写入者：`records`、`context`、`native_memory`。
  - 记录头、载荷块、回执的编码与今天相同。第 7 步把 JSON 字段 `history` 改为 `event`，`history_sequence` 改为 `sequence`；第 8 步给 `CheckpointMetadata` 加上 `memory_era`。
- Existing records: reset（status `startup`）。`mmdb-context-v1` 和 `mmdb-native-memory-v1` 目录打开时报 `FormatMismatch`，操作者用 `store_format` 做整库重置。第 4 步的过渡格式 `mmdb-context-v2`、`mmdb-native-memory-v2` 只存在于这条分支，第 6 步起统一为 `mmdb-store-v1`；中间提交建出的目录都只是临时数据，最后一步不需要读取它们。

## Dependencies

| Dependency | Category | Production adapter | Test adapter |
| --- | --- | --- | --- |
| fjall | local-substitutable | `Kernel` 内的 fjall keyspace | 同一引擎，跑在 `tempfile::tempdir()` 里 |
| 文件系统（租约、清单、隔离目录） | local-substitutable | `std::fs` | `tempfile::tempdir()` |
| 时钟（计划的签发与过期） | in-process，由调用方传入 | 调用方传 `SystemTime::now()` | 测试传固定时间 |
| 会话模块的可用性规则 | in-process seam（`records::Gate`） | `session::LogGate` | `records` 单元测试里的 `FixedGate` |

## Steps

1. 把 `store_format` 搬进新 crate `mmdb-kernel`，`mmdb` 以 `pub use mmdb_kernel::store_format` 导出；`native_memory` 和 `state` 改用新路径；README 的 Architecture 与 Crate Map 加上 `mmdb-kernel`。只重构。After: none. Check: `cargo test --workspace --locked`
2. 在 `mmdb-kernel` 里加入内核存储：`Kernel::create/open`、写在 `kernel_v1` 里的模块 era 与摘要密钥、按「模块.era.名称」命名的分区、`Partition`/`Batch`/`Bytes`/`WriteGuard`、`EraId`/`OperationId`，以及 kernel 测试（`a_second_open_of_a_store_is_busy`、`store_metadata_is_created_once_and_checked_against_the_manifest`、`open_rejects_another_format`）。After: 1. Check: `cargo test -p mmdb-kernel --locked && cargo test --workspace --locked`
3. 把共用值类型移出原生记忆：`EraId`、`OperationId` 改用内核的；`Actor`、`Scope`、`TemporalFacts`、`RecordState` 搬进新的 `mmdb::records`，在 `mmdb::` 根导出；`native_memory` 和 `context` 改为引入它们。只重构。After: 2. Check: `cargo test --workspace --locked`
4. 让原生记忆和领域上下文经由内核打开：`MemoryDatabase` 的四个打开函数改走 `Kernel`，只有一个 memory 模块，格式改为 `-v2`；原生元数据分区交给内核；`native_memory`、`context`、`entity`、`extraction` 里的 fjall 类型全部换成内核类型。After: 3. Check: `cargo test --workspace --locked`
5. 从领域上下文抽出记录层：`records::{Records, RecordTable, Scoped, Gate, Availability}`，此时仍只有一张表；日志与检查点的可用性规则放到 `Gate` 后面，实现暂时留在 `context`。只重构。After: 4. Check: `cargo test --workspace --locked`
6. 加入带会话模块的 `Store`：两个内核模块；`Records` 有会话、记忆两张表，lifecycle 放在会话模块；`context/history.rs`、`checkpoints.rs` 与 `search_history` 搬进 `session/`，`LogGate` 和 `SessionLog`（先沿用旧方法名）也在这里；`Store::{create, open, era, session_log, context, memory}`，格式 `mmdb-store-v1`；删去 `MemoryDatabase` 的打开函数和 `context()`；`context` 从 `native_memory` 的子模块改为 crate 顶层模块；改写 `context_contract`、`context/tests.rs` 和三个 context example；加入行为表中 Who、消息和 Limits 行标了「新」的测试；加入 `tests/layering.rs`，断言 `src/session/`、`src/records/`、`src/context/`、`src/store.rs`、`src/native_memory.rs`、`src/entity.rs`、`src/extraction.rs` 中都不出现 `fjall`；更新 README 的 Harness Context 与 AGENTS.md 的 gotcha。After: 5. Check: `cargo test --workspace --locked && cargo test --locked -p mmdb --test context_contract independent_harnesses_share_types_history_and_relations_after_reopen -- --exact`
7. 按术语表给会话日志的类型、字段和方法改名（见上表），同步测试、example 和 README。只改名。After: 6. Check: `cargo test --workspace --locked`
8. 原地重置记忆：`Kernel::reset_module`、Retired era、打开时删除残留分区；`Store::{plan_memory_reset, reset_memory}`；`ContextError::Reset` 与来源检查的终点规则；检查点绑定 `memory_era`；加入行为表中与重置有关、标了「新」的测试，改写整库重置测试；更新 AGENTS.md 的重置 gotcha 和 README；把 session-log、context-ontology、context-checkpoints 三份 feature doc 设为 `shipped`，并把本 spec 移到 `docs/spec/archived/`。After: 7. Check: `cargo test --workspace --locked`

## Not this

- 不改 miumiu。它的入口、句柄、类型路径和记忆重置在 miumiu 仓库里改；本 spec 只规定它会用到的接口。
- 不统一原生记忆和记录层的操作回执，也不把回执放进内核；不收拢三套记忆模型。这些属于第 3 部分。
- 不动 `StateStore`、`Database::state()`、多模型 `Database`、`mmdb-storage`、`mmdb-vector`、`mmdb-graph`、`mmdb-blob`。它们仍直接使用 fjall，属于第 2、3 部分。
- 不加依赖白名单测试，也不移出 WebAssembly UDF。
- 不加会话头记录、fork、会话列表修订号、轮次事件或对话缓存。
- 读取输入流期间不释放写锁。
- 不提供单独重置会话模块的接口；整库重置仍是 `store_format` 的目录交换。
