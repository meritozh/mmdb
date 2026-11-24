# 连续会话日志与原文搜索: implement spec

Feature: `docs/feature/session-log.md` @ `521b65771d028b717502deda489fd61529542cf8`

This spec is open until the feature ships, and `/go-work` carries it out. Do not edit it while working it; only `to-implement-spec` changes it. Before work starts or resumes, check that the hash above still matches `git hash-object docs/feature/session-log.md`; if it does not, the feature changed, so stop and rerun `to-implement-spec`. Each step is one commit whose message ends with the trailer `Spec-Step: session-log#<n>`. The commit that lands the last step sets the feature doc's Status to `shipped` and moves this file to `docs/spec/archived/<YYYY-MM-DD>-session-log.md` with `git mv`, dated that day. Nothing reads an archived spec again.

新建 `mmdb-kernel` crate 作为存储内核：持有 fjall、租约、格式清单、按模块的 era 和写锁，并按「模块.era.名称」打开分区。`mmdb` 中新增 `records` 层，存放两个模块共用的记录头、载荷、回执、可用性 epoch 和来源检查。会话日志与检查点从 `context` 搬进 `session` 模块，由 `SessionLog` 句柄提供。日志事件写入后只读：历史读取只看事件自己、所在消息、关联的工具调用和追加时登记的 scope，不再沿来源检查。`Store` 是组装根：一个目录里放会话模块和记忆模块，记忆模块包括原生记忆和领域上下文。重置记忆时，先为新 era 建好记忆句柄，再用一次提交换 era，会话模块不动。

## Behavior map

| Feature doc line | Owner | Test |
| --- | --- | --- |
| Who: 可信 harness 以 `ContextAccess` 访问 | `records::ContextAccess::validate`，由 `Store::session_log` 和 `Store::context` 调用 | `session_log_and_context_reject_the_same_invalid_access`（新） |
| Who: access 绑定 session 时只能追加该会话 | `session::SessionLog::append` | `access_bound_to_a_session_appends_only_to_it`（新） |
| Who: 用户消息、列出会话、撤回或清除日志事件的 actor 限制 | `session::SessionLog::{append, sessions, retract, purge}` | `log_permissions_follow_the_actor`（新） |
| Who: 重置记忆由持有租约的进程凭摘要提交 | `Store::reset_memory` | `memory_reset_needs_the_exact_plan_digest`（新） |
| Entry: `Store::create/open`、`store.session_log(access)`，检查点在同一句柄上，方法名不带 history | `Store`、`session::SessionLog` | `independent_harnesses_share_types_history_and_relations_after_reopen`（改写） |
| Does: 两个模块同在一个存储，共用租约与批次提交 | `mmdb_kernel::Kernel` | `a_second_open_of_a_store_is_busy`（kernel）、`a_memory_purge_commits_its_head_epoch_and_receipt_together`（新） |
| Does: 写入时分配单调序号，切换窗口仍追加到原会话 | `SessionLog::append` | `concurrent_appends_allocate_one_session_order` |
| Does 追加：事件 ID、预期位置、重试取回原序号、同身份不同内容冲突、完整写入后一起发布 | `SessionLog::append`、`records::Scoped::replay` | `append_retries_preserve_identity_and_order_even_after_later_events`、`committed_history_survives_a_lost_acknowledgement_without_duplication`、`one_operation_id_names_one_write_across_the_log_and_context`（新）、`a_log_retry_after_a_memory_reset_returns_the_original_receipt`（新） |
| Does 消息：撤回或清除一个事件后整条消息不可读；工具结果关联同会话唯一调用，调用不可读时结果也不可读 | `session::LogGate` 与消息状态、工具调用索引 | `a_withdrawn_fragment_hides_its_whole_message`（新）、`tool_results_require_their_call_and_follow_its_purge_state` |
| Does 来源：追加时来源必须可读；日志事件写入后只读；读取需要来源的 scope；当前检查照常沿来源；清除日志事件后派生内容不可用 | `records::Scoped` 的来源检查与 `session::LogGate` | `log_copies_stay_readable_after_their_sources_change`（改写自 `history_copies_old_object_and_type_pins_but_purge_blocks_object_copies`）、`copied_history_tracks_source_availability_and_session_catalog_pages`（改写：来源变化只影响当前检查）、`a_log_copy_needs_the_scopes_of_its_sources`（新）、`purging_a_log_event_hides_what_was_derived_from_it`（新）、`tool_results_stay_readable_after_their_call_source_is_purged`（改写自 `tool_results_link_historical_call_sources_and_still_follow_purge`）、`live_revision_keeps_checkpoint_derived_originals_and_pairs_the_completed_tool`（改写：清除原始日志事件后，检查点和派生对象不可读，调用和结果仍可读、不再通过当前检查） |
| Does 读取：按位置翻页、按 pin 读载荷、列出会话、工具调用配对 | `SessionLog::{events, read_payload, sessions, tool_pair}` | `complete_large_history_is_searchable_across_chunks_and_query_pages`、`copied_history_tracks_source_availability_and_session_catalog_pages` |
| Does 搜索：固定引用、字节位置、续查游标、跨块与跨页、跨会话按键序、同会话按序号、大小写规则 | `SessionLog::search` | `complete_large_history_is_searchable_across_chunks_and_query_pages`、`search_walks_sessions_in_key_order_and_events_in_sequence`（新） |
| Does 重置记忆：计划、凭摘要提交、一次提交换 era、旧记录立刻不可见、清理旧分区、会话模块不受影响 | `Store::{plan_memory_reset, reset_memory}`、`Kernel::{prepare_module_reset, commit_module_reset}` | `memory_reset_keeps_the_session_log_and_hides_old_memory`（新） |
| Does 重置记忆之后：日志照常可读且通过当前检查；指向旧记忆的来源是终点；直接引用旧记忆报已重置 | `records::Scoped::check_ref` 与来源检查 | `log_events_that_copied_reset_memory_stay_readable_and_current`（新） |
| Does 重置记忆之后：重置前的检查点载荷可读、不能用于当前恢复 | `records` 的可用性 epoch（含记忆重置次数）与 `session::LogGate` | `checkpoints_saved_before_a_memory_reset_cannot_resume`（新）、`a_checkpoint_generated_before_a_memory_reset_is_refused`（新） |
| Does 整库重置：旧目录进隔离区，所有模块换新 era | `mmdb_kernel::store_format::commit_reset`、`Kernel::open` | `whole_store_reset_quarantines_the_directory_and_starts_new_eras`（改写自 `reset_creates_a_new_context_era_without_importing_previous_history`） |
| Fails: 已重置、已清除、另一 owner、另一存储（含整库重置之前）各自报错 | `records::Scoped::check_ref` | `reset_purged_and_foreign_refs_fail_differently`（新）、`format_and_store_identity_prevent_accidental_adoption`（改写） |
| Fails: 追加中断不推进位置，`recover_payloads` 有界清理 | `SessionLog::{append, recover_payloads}` | `incomplete_uploads_never_advance_history_and_can_be_cleaned`、`actual_process_interruption_leaves_no_partial_visible_history` |
| Fails: 重置要么整体生效要么没发生；没做完的清理下次打开时继续 | `Store::reset_memory`、`Kernel::{commit_module_reset, open}` | `a_failed_memory_reset_leaves_the_store_usable`（新）、`module_reset_cleanup_resumes_after_interruption`（kernel，新）、`an_unknown_reset_outcome_poisons_the_kernel`（kernel，新） |
| Fails: 摘要不符或计划过期不做任何改变 | `Store::reset_memory` | `memory_reset_refuses_a_plan_for_another_time_era_or_directory`（新） |
| Fails: 旧格式目录被拒绝，需要整库重置，不迁移 | `Kernel::open`、`store_format::ResetPlan` | `format_and_store_identity_prevent_accidental_adoption`（改写）、`reset_replaces_a_store_with_another_format`（kernel，新）、`a_legacy_directory_resets_into_an_empty_store`（新） |
| Limits: 32 KiB 分块、单次读 1 MiB、搜索 4,096 个事件或 16 MiB | `records::Scoped::stream_payload`、`SessionLog::{read_payload, search}` | `complete_large_history_is_searchable_across_chunks_and_query_pages`、`log_reads_and_searches_refuse_budgets_over_their_limits`（新） |
| Limits: 读输入流期间持有存储写锁 | `SessionLog::append`（`Kernel::write_lock`） | `a_streaming_append_blocks_memory_writes_until_it_ends`（新，crate 内部测试） |
| Keeps: 日志、载荷、检查点保留到清除或整库重置；旧记忆分区删除，不留副本 | `Kernel::commit_module_reset` | `memory_reset_keeps_the_session_log_and_hides_old_memory`、`module_reset_deletes_the_old_era_partitions`（kernel，新） |
| [context-checkpoints](../feature/context-checkpoints.md)：任何修订都推进可用性 epoch | `context` 的类型修订与抽取 | `revising_a_type_or_extracting_advances_the_availability_epoch`（新） |
| 目标 Part 1：会话模块与记录层不直接使用 fjall | 依赖方向 | `store_modules_do_not_name_fjall`（新，`tests/layering.rs`） |

Replaces:
- `reset_creates_a_new_context_era_without_importing_previous_history`：改写为整库重置测试。
- `history_copies_old_object_and_type_pins_but_purge_blocks_object_copies`、`tool_results_link_historical_call_sources_and_still_follow_purge`，以及 `live_revision_keeps_checkpoint_derived_originals_and_pairs_the_completed_tool` 末尾的清除断言：改写为只读规则的测试。
- `context/checkpoints.rs` 的 `missing_and_stale_visibility_markers_do_not_restore_old_checkpoint_payloads`：它测的是第 13 步删去的旧写入者兼容分支，随之删除。
- `native_memory.rs` 中检查 `native_internal_metadata_v1` 与 `StoreEraMismatch` 的测试：元数据改由内核持有，改写为 kernel 测试 `store_metadata_is_created_once_and_checked_against_the_manifest`。
- `native_memory.rs` 测试里对 `MEMORY_STORE_FORMAT_ID` 的断言：随格式 ID 改写。

## Modules

### `mmdb-kernel`（new crate）

`store_format` 搬入，`StoreFormatDescriptor::new_manifest` 改为 `pub`（`StateStore::open_path` 在另一个 crate 里调用它）。第 2 步让整库重置区分源格式和替换格式。内核只认识模块名、era 和分区名，不认识任何一类 agent 数据。

```rust
pub mod store_format;

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

/// 可廉价克隆的存储句柄。`EraPartitions`、`Partition`、`Batch` 都持有同一个内部 `Arc`，
/// 租约在最后一个句柄释放时才释放。
#[derive(Clone)]
pub struct Kernel { /* Arc<Inner>: keyspace, lease, manifest, eras: RwLock<..>, write_lock, digest_key, poisoned */ }

impl Kernel {
    pub fn create(root: &Path, format: &StoreFormatDescriptor, modules: &'static [ModuleSpec]) -> KernelResult<Self>;
    pub fn open(root: &Path, format: &StoreFormatDescriptor, modules: &'static [ModuleSpec]) -> KernelResult<Self>;
    pub fn canonical_root(&self) -> &Path;
    /// 清单里标识目录这一代的 ID，整库重置时更换；与模块 era 无关。
    pub fn store_era(&self) -> &StoreEraId;
    pub fn era(&self, module: ModuleId) -> EraId;
    pub fn era_state(&self, module: ModuleId, era: EraId) -> EraState;
    /// 模块当前 era 的分区。
    pub fn module(&self, module: ModuleId) -> EraPartitions;
    /// SyncAll 持久批次，可以跨模块。
    pub fn batch(&self) -> Batch;
    /// 存储级写锁，所有模块共用。
    pub fn write_lock(&self) -> WriteGuard<'_>;
    /// 以存储的摘要密钥建一个按 `domain` 区分的 keyed hasher；密钥本身不出内核。
    pub fn keyed_hasher(&self, domain: &'static str) -> blake3::Hasher;
    /// 第 12 步加入：为 `module` 建好 `new_era` 的空分区，当前 era 不变。
    pub fn prepare_module_reset(&self, module: ModuleId, new_era: EraId) -> KernelResult<EraPartitions>;
    /// 第 12 步加入：把「当前 era 换成 `prepared` 的 era、旧 era 登记为 Retired」写进 `batch` 并提交，
    /// 再删除旧 era 的分区。`batch` 里调用方的其他变更与切换同批生效。
    pub fn commit_module_reset(&self, prepared: EraPartitions, batch: Batch) -> KernelResult<ModuleResetReport>;
}

pub struct ModuleResetReport { pub old_era: EraId, pub new_era: EraId, pub cleanup_pending: bool }

/// 一个模块某个 era 的分区。
#[derive(Clone)]
pub struct EraPartitions { /* Arc<Inner>, module, era */ }
impl EraPartitions {
    pub fn era(&self) -> EraId;
    /// `name` 必须在该模块的 spec 里。
    pub fn partition(&self, name: &'static str) -> KernelResult<Partition>;
}

/// 只读的分区句柄；写入一律走 `Batch`。
#[derive(Clone)]
pub struct Partition { /* Arc<Inner>, module, era, fjall::PartitionHandle */ }
impl Partition {
    pub fn get(&self, key: impl AsRef<[u8]>) -> KernelResult<Option<Bytes>>;
    pub fn contains_key(&self, key: impl AsRef<[u8]>) -> KernelResult<bool>;
    pub fn iter(&self) -> impl DoubleEndedIterator<Item = KernelResult<(Bytes, Bytes)>> + '_;
    pub fn range<K: AsRef<[u8]>, R: RangeBounds<K>>(&self, range: R)
        -> impl DoubleEndedIterator<Item = KernelResult<(Bytes, Bytes)>> + '_;
    pub fn prefix(&self, prefix: impl AsRef<[u8]>)
        -> impl DoubleEndedIterator<Item = KernelResult<(Bytes, Bytes)>> + '_;
}

pub struct Batch { /* Arc<Inner>, fjall::Batch, 涉及的 (module, era) */ }
impl Batch {
    /// `partition` 必须来自同一个 `Kernel`，否则 panic（编程错误）。
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
    InvalidModuleSpec(String),
    UnknownPartition { module: &'static str, name: &'static str },
    /// `prepare_module_reset` 收到当前 era 或已登记的 Retired era。
    InvalidEra(EraId),
    /// 批次里有分区属于已不是当前 era 的模块 era；整批没有写入。
    StaleEra { module: &'static str, era: EraId },
    /// 提交结果不确定。
    CommitUnknown,
    /// 切换 era 的提交结果不确定之后，这个 `Kernel` 的每个操作都返回它。
    Poisoned,
}
pub type KernelResult<T> = Result<T, KernelError>;
```

- Invariants:
  - 模块分区名为 `<module>.<era>.<name>`，`<era>` 是 ULID 文本；内核元数据在未限定名的 `kernel_v1` 分区。
  - `create`/`open` 在碰磁盘之前校验 module spec：模块名和分区名只含 `[a-z0-9_]`，模块名不重复，同一模块的分区名不重复，限定后的名字不超过 255 字节。不满足时返回 `InvalidModuleSpec`。
  - 每个模块只有一个 Current era。Retired 只记录本存储内被换下的 era；另一存储的 era，以及整库重置之前的 era，都是 Unknown。摘要密钥和清单里的 store era 在存储初始化时写一次，之后不变。
  - 批次只接受同一内核的分区。`commit` 在内部的 era 读锁下检查批次涉及的每个模块 era 仍是当前 era，否则整批返回 `StaleEra`；`commit_module_reset` 持有写锁，所以提交与切换 era 互斥。批次在重置前开好、重置后才提交时，跨模块的整批都被拒绝，不会只写入一半。
  - `prepare_module_reset` 拒绝当前 era 和已登记的 Retired era。
- Ordering:
  - `create`/`open`：校验 spec，取得租约（先恢复中断的整库重置），检查格式，打开 keyspace，读出或首次写入元数据，再逐个识别分区。`kernel_v1` 和当前 era 的 spec 分区保留，缺的就创建；已知模块的非当前 era 分区删除；其他名字返回 `Corrupt`。
  - 重置一个模块：先 `prepare_module_reset` 建新分区，调用方再用返回的 `EraPartitions` 准备新句柄，最后 `commit_module_reset` 一次提交调用方的变更、新的当前 era 和 Retired 记录，然后删除旧 era 的分区。
- Errors:
  - 格式不符返回 `Format(FormatMismatch)`，目录被占返回 `Format(StoreBusy)`；元数据缺失或与清单不一致返回 `Corrupt`；分区名不在 spec 里返回 `UnknownPartition`。
  - 普通批次提交失败返回 `CommitUnknown`，之后引擎拒绝写入。
  - `commit_module_reset` 提交之前失败时，什么都没变，准备好的分区由下次 `open` 删除。提交本身返回错误时，结果为 `CommitUnknown`，内核随即中毒，之后只返回 `Poisoned`；重新打开时按持久化的元数据决定当前 era，删除另一组分区。提交成功、删除旧分区失败时，`cleanup_pending` 为 true，下次 `open` 接着删。
- Configuration: 无。
- Performance:
  - `open` 的开销与分区数成正比。
  - `prepare_module_reset` 只建一个模块的分区。`commit_module_reset` 只提交一个小批次，再删除一个模块的分区。两者的开销与该模块的分区数成正比，与记录数无关。
  - `delete_partition` 只做逻辑删除。磁盘文件在最后一个句柄释放时，或下次打开时，由 fjall 移除。`cleanup_pending` 只说明是否还有旧分区没做逻辑删除。

Behind the seam: fjall 的 keyspace、分区句柄、持久化方式和名称规则；元数据编码；残留分区的识别；era 读写锁与中毒标记。

Call site（`Store::open`）：

```rust
let kernel = Kernel::open(root, &StoreFormatDescriptor::new(STORE_FORMAT_ID)?, &MODULES)?;
let session = kernel.module(SESSION);
let memory = kernel.module(MEMORY);
let records = Records::open(kernel.clone(), &session, &memory, Box::new(LogGate::open(&session)?))?;
let native = MemoryDatabase::on_era(kernel.clone(), &memory)?;
```

### `mmdb::records`（new，crate 内部，加上公开的值类型）

这是两个模块共用的记录层，负责：
- 记录头格式和修订；
- 载荷分块；
- 操作回执；
- 按 owner 的可用性 epoch 与脱敏 epoch；
- 跨两张记录表的来源检查。

会话模块的可读规则通过 `Gate` 注入，所以记录层不依赖会话模块。

公开值类型从 `context/types.rs` 和 `native_memory.rs` 搬来，在 `mmdb::` 根导出：
- 记录与引用：`Actor`、`Scope`、`TemporalFacts`、`RecordState`、`ContextAccess`、`Provenance`、`ContextRef`、`RecordPin`、`RecordKind`、`RecordHeader`、`InformationKind`、`Endpoints`。
- 载荷与回执：`PayloadDescriptor`、`PayloadPage`、`WriteReceipt`。
- 会话日志：`LogEvent`、`EventKind`、`MessageStatus`、`ToolOutcome`、`CheckpointMetadata`、`CheckpointScope`。
- 错误：`ContextError`、`ContextResult`。
- 常量：`PAYLOAD_CHUNK_BYTES`、`MAX_PAYLOAD_PAGE_BYTES`、`MAX_RECORD_BYTES`、`MAX_CONTEXT_NAME_BYTES`、`MAX_CONTEXT_SOURCES`、`MAX_ACCESS_SCOPES`。`MAX_ACCESS_SCOPES` 由 `native_memory::MAX_RECALL_SCOPES` 改名而来，原生召回也用它。

`native_memory` 不再导出 `Actor`、`Scope`、`TemporalFacts`、`RecordState`、`EraId`、`OperationId`。

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

impl TemporalFacts {
    /// 原生记忆和领域上下文各自把它转成自己的 InvalidInput。
    pub(crate) fn validate(self) -> Result<(), InvalidTemporalFacts>;
}
pub(crate) struct InvalidTemporalFacts;

pub struct RecordHeader {
    // ……今天的字段……
    /// 会话模块的记录：写入时来源检查经过的各记录所在的 scope，不含自己的 scope。
    /// 读取这条记录时，这些 scope 都要对 access 可见。记忆模块的记录为空。
    pub source_scopes: Vec<Scope>,
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
    /// 第 13 步加入：引用属于本存储里一个已重置模块的旧 era。
    Reset(ContextRef),
    RevisionConflict { expected: u64, actual: u64 },
    AvailabilityConflict { expected: u64, actual: u64 },
    PositionConflict { expected: u64, actual: u64 },
    TypeConflict(String),
    IdempotencyConflict,
    CommitUnknown(OperationId),
    BudgetExceeded,
}

/// 一个 owner 在检查时刻的 epoch，交给 `Gate` 使用。
pub(crate) struct Availability { pub epoch: u64, pub redaction_epoch: u64 }

/// 会话模块记录的额外可读规则。`Records` 只对 `kind.module() == Session` 的记录调用它。
pub(crate) trait Gate: Send + Sync {
    fn admits(&self, owner: &str, header: &RecordHeader, active: bool, now: &Availability)
        -> ContextResult<bool>;
}

pub(crate) struct RecordTable { headers: Partition, heads: Partition, payloads: Partition, pending: Partition }
impl RecordTable { pub(crate) fn open(era: &EraPartitions) -> KernelResult<Self>; }

pub(crate) struct Records {
    kernel: Kernel,
    session: RecordTable,
    memory: RecordTable,
    lifecycle: Partition,  // 会话模块：按 owner 的计数、脱敏 epoch、记忆重置次数
    operations: Partition, // 会话模块：两个模块记录层操作的回执
    gate: Box<dyn Gate>,
}
impl Records {
    pub(crate) fn open(kernel: Kernel, session: &EraPartitions, memory: &EraPartitions, gate: Box<dyn Gate>)
        -> KernelResult<Self>;
    /// 第 13 步：换上新 era 的记忆记录表。只是赋值，不会失败。
    pub(crate) fn replace_memory_table(&mut self, table: RecordTable);
    /// 第 13 步：把记忆重置次数加一写进 `batch`；所有 owner 的可用性 epoch 因此一起前进。
    pub(crate) fn count_memory_reset(&self, batch: &mut Batch) -> ContextResult<()>;
    pub(crate) fn scoped(&self, access: ContextAccess) -> Scoped<'_>;
}

/// 一个 access 下的记录操作；`SessionLog` 和 `ContextStore` 各持有一个。access 已由 `Store` 校验。
pub(crate) struct Scoped<'a> { records: &'a Records, access: ContextAccess }
impl Scoped<'_> {
    pub(crate) fn access(&self) -> &ContextAccess;
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
    /// 普通写入的来源：1..=64 个当前可用的来源。返回检查经过的 scope（不含 `scope`），会话记录写进 `source_scopes`。
    pub(crate) fn validate_sources(&self, scope: &Scope, sources: &[RecordPin], information: InformationKind, revising: Option<&ContextRef>) -> ContextResult<Vec<Scope>>;
    /// 日志复制的来源：0..=64 个可读的来源，可以是旧修订或类型。返回值同上。
    pub(crate) fn validate_copy_sources(&self, scope: &Scope, sources: &[RecordPin]) -> ContextResult<Vec<Scope>>;
    /// 有界扫描一个记录族的 head：从 `after` 之后开始，最多返回 `limit` 个，最多看 4,096 个键。
    pub(crate) fn scan_heads(&self, kind: RecordKind, after: Option<&ContextRef>, limit: usize) -> ContextResult<HeadPage>;
    pub(crate) fn digest<T: Serialize>(&self, domain: &'static str, input: &T) -> ContextResult<[u8; 32]>;
    pub(crate) fn replay(&self, operation: OperationId, digest: [u8; 32]) -> ContextResult<Option<WriteReceipt>>;
    pub(crate) fn operation_receipt(&self, operation: OperationId) -> ContextResult<Option<WriteReceipt>>;
    pub(crate) fn commit_operation(&self, batch: Batch, operation: OperationId, digest: [u8; 32], receipt: WriteReceipt) -> ContextResult<WriteReceipt>;
    pub(crate) fn put_header(&self, batch: &mut Batch, header: &RecordHeader) -> ContextResult<()>;
    /// 不超过 128 KiB 的载荷随发布批次写入，返回它的描述。
    pub(crate) fn put_payload(&self, batch: &mut Batch, pin: &RecordPin, bytes: &[u8]) -> PayloadDescriptor;
    /// 读完 `payload`，计算长度和摘要。`upload` 为 Some 时先登记待上传，再按 32 KiB 分块逐块持久写入；
    /// 为 None 时只计算，供重试比较。
    pub(crate) fn stream_payload<R: Read>(&self, payload: R, upload: Option<(&RecordPin, OperationId)>) -> ContextResult<StreamedPayload>;
    pub(crate) fn payload_slice_locked(&self, header: &RecordHeader, offset: u64, limit: usize) -> ContextResult<PayloadPage>;
    pub(crate) fn small_payload_locked(&self, header: &RecordHeader) -> ContextResult<Vec<u8>>;
    pub(crate) fn next_revision(&self, expected: &RecordPin) -> ContextResult<(RecordHeader, RecordPin)>;
    /// 撤回或清除的共同部分：写新修订头，推进 epoch；`redacts` 为真时同批推进脱敏 epoch；清除时登记待清理的载荷。
    /// 调用方再往同一批次加入自己的索引变更。
    pub(crate) fn state_change(&self, batch: &mut Batch, operation: OperationId, expected: &RecordPin, state: RecordState, redacts: bool) -> ContextResult<(RecordHeader, RecordHeader)>;
    pub(crate) fn advance_epoch(&self, batch: &mut Batch) -> ContextResult<u64>;
    pub(crate) fn availability_locked(&self) -> ContextResult<Availability>;
    pub(crate) fn recover_payloads(&self, module: StoreModule, max_chunks: usize, max_uploads: usize) -> ContextResult<RecoveryReport>;
}

pub(crate) struct StreamedPayload { pub descriptor: PayloadDescriptor, upload: Option<RecordPin> }
impl StreamedPayload {
    /// 在发布批次里移除待上传标记；只计算时什么也不做。
    pub(crate) fn publish(&self, batch: &mut Batch);
}

pub(crate) struct HeadPage { pub heads: Vec<RecordPin>, pub next: Option<ContextRef> }
```

- Invariants:
  - 存放与回执：
    - 记录头和修订头存放在 `kind.module()` 对应的表里，键编码与今天相同（owner 段 + 种类字节 + id）。
    - 两个模块的操作回执在会话模块的同一张表里，键为 owner + 操作 ID，所以同一个 ID 只能命名一次写入，跨模块也一样。
    - 重置记忆后重试重置前的记忆写入时，回执里的 pin 属于 Retired era，返回 `Reset`。
  - epoch：
    - 可用性 epoch 等于该 owner 的计数加上记忆重置次数。两者都存放在会话模块，只增不减，脱敏 epoch 与它同一刻度。
    - 今天 `checkpoint_redaction_epoch_locked` 对旧写入者的兼容分支删去：新格式里没有旧写入者，脱敏 epoch 直接读取。
  - `check_ref`：owner 不符时返回 `AccessDenied`。era 为该模块的 Current 时通过，为 Retired 时返回 `Reset`，为 Unknown 时返回 `AccessDenied`。
  - 历史读取（非 active）：
    - 会话模块的记录不沿来源检查，只看自己的状态、`Gate`，以及 `source_scopes` 是否都可见。
    - 记忆模块的记录照常沿来源检查，走到会话记录时按会话规则判断。
  - 当前检查（active）：
    - 所有记录都沿来源检查，会话记录还要满足 `Gate` 和 `source_scopes`。
    - 来源的 era 为 Retired 时，它是检查的终点，算可用；来源返回 `NotFound` 或 `AccessDenied` 时算不可用。
    - 直接传入 Retired 的引用会返回 `Reset`，这包括读取、`validate_current` 的 pin 和新写入的来源。
  - `Gate` 按 `kind.module()` 选择要检查的记录，不看记录存放在哪张表，所以第 8 步只有一张表时也成立。
- Ordering:
  - 每个写操作先取 `lock()`，再查 `replay`，命中就直接返回原回执，不再做新写入的校验。
  - 没命中时，在同一个 `batch()` 里写入记录、索引、epoch 和回执，最后由 `commit_operation` 提交。
- Errors: 与今天的 `ContextStore` 相同，另外加上 `Reset`。提交失败返回 `CommitUnknown(operation)`。
- Configuration: 无。
- Performance:
  - 来源检查最大深度 32，最多访问 4,096 个记录，与今天相同。
  - 会话记录的历史读取不走来源，只做常数次查找。
  - 按 pin 读取一次只查一张表。

Behind the seam:
- 记录头与载荷的编码；
- 32 KiB 分块与校验；
- 回执编码；
- epoch、脱敏标记和记忆重置次数的规则（今天在 `checkpoints.rs` 的 `CheckpointVisibility` 里）；
- 两张表的选择。

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

/// 会话模块的 `Gate`：
/// - 消息成员：消息状态为不可用时不可读。撤回或清除任一成员时，同批置位。
/// - 工具结果：关联的调用不可读时，结果也不可读。调用被清除时，同批在调用索引上标记；
///   调用所在的消息不可用时，结果也跟着不可读。
/// - 检查点：active 检查要求 epoch 等于当前 epoch；非 active 检查要求 epoch 不早于脱敏 epoch。
pub(crate) struct LogGate { messages: Partition, calls: Partition }
impl LogGate { pub(crate) fn open(session: &EraPartitions) -> KernelResult<Self>; }

pub(crate) struct SessionParts { order: Partition, heads: Partition, events: Partition, calls: Partition, results: Partition, messages: Partition, checkpoints: Partition }
```

第 11 步的改名（第 10 步搬代码时沿用旧名）：

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

- Invariants:
  - 句柄只接受会话模块的记录种类（Event、Checkpoint）。种类检查先于其他检查：别的种类返回 `InvalidInput`，`read_checkpoint` 收到事件的 pin 也返回 `InvalidInput`。
  - 行为、上限和权限与今天的 `ContextStore` 对应方法相同，只有一处不同：日志事件和检查点的历史读取不再沿来源检查。
  - 写入事件和检查点时，把来源检查返回的 scope 写进 `source_scopes`。
  - 撤回或清除日志事件时，同批推进 epoch 和脱敏 epoch。如果事件是消息成员，整条消息变为不可用；如果是工具调用，调用索引也被标记。
  - `CheckpointInput` 的 `expected_availability_epoch` 在保存时与当前 epoch 比较。重置记忆让 epoch 前进，所以重置前生成、重置后才保存的检查点会被拒绝。
- Ordering（`append`）：
  1. 校验输入：名称、scope、access 绑定的 session、actor。
  2. 取存储写锁，查操作回执和事件索引。
  3. 两者都没有时，是新写入：
     1. 检查预期位置、调用关联、来源（`validate_copy_sources`，得到 `source_scopes`）和消息状态转换。
     2. 以 `stream_payload(payload, Some((pin, operation)))` 写入载荷。
     3. 在一个批次里发布事件头、序号、会话 head、事件索引、工具索引和消息状态，并以 `publish` 移除待上传标记，最后 `commit_operation`。
  4. 已有回执或事件时，是重试：
     1. 以 `stream_payload(payload, None)` 读完整个输入，再把摘要与原来的比较。
     2. 相同就返回原回执；如果是同一事件、新的操作 ID，补写这个操作的回执。不同就返回 `IdempotencyConflict`。
     3. 重试不检查预期位置和来源，所以即使后面追加过事件，或来源已经重置，也能取回原序号。
  5. 整个过程持有存储写锁。
- Errors: 与今天相同。`read_payload` 和 `read_checkpoint` 遇到旧 era 的引用时返回 `Reset`。
- Performance: 与今天相同，日志事件的历史读取更便宜（不走来源）。

Behind the seam:
- 会话键的编码；
- 消息的生命周期；
- 工具调用的配对与撤下标记；
- 检查点的父版本比较与 epoch 比较；
- 子串搜索的分块扫描。

Call site（harness 恢复一个会话）：

```rust
let access = ContextAccess::new("local-user", Actor::System);
let log = store.session_log(access.clone())?;
let context = store.context(access)?;
let mut after = 0;
if let Some(header) = log.latest_checkpoint(&session)? {
    let position = header.checkpoint.as_ref().map_or(0, |c| c.sequence);
    match context.validate_current(std::slice::from_ref(&header.pin), None) {
        Ok(()) => {
            let summary = log.read_checkpoint(&header.pin)?; // 放进窗口
            after = position;
        }
        Err(ContextError::Unavailable(_)) => {} // 读到 position 为止的日志，重新生成摘要
        Err(error) => return Err(error.into()),
    }
}
loop {
    let page = log.events(&session, after, 100)?;
    for header in &page.entries {
        let mut offset = 0;
        loop {
            let chunk = log.read_payload(&header.pin, offset, MAX_PAYLOAD_PAGE_BYTES)?;
            // harness 把 chunk.bytes 放进窗口
            match chunk.next_offset { Some(next) => offset = next, None => break }
        }
    }
    match page.next_sequence { Some(next) => after = next, None => break }
}
log.append(OperationId::new(), EventInput { session: session.clone(), event_id, scope: Scope::Personal,
    kind: EventKind::UserMessage, media_type: "text/plain".into(), occurred_at_ms,
    expected_sequence: Some(log.position(&session)?), sources: Vec::new() }, text.as_bytes())?;
```

### `mmdb::context`（changed）

```rust
pub struct ContextStore<'a> { /* Scoped<'a>, &'a ContextParts */ }
// 删去：append_history、history_position、history、history_sessions、tool_history、message_state、
//       search_history、save_checkpoint、latest_checkpoint、latest_run_checkpoint，
//       以及 RecordBody::History、RecordBody::Checkpoint。
// 保留：类型、对象、关系、动作、实体、抽取、recall、relations、query_current_valid、read、read_payload、
//       head、inspect_header、operation_receipt、retract、purge、recover_payloads、
//       validate_current、validate_current_guard、availability_epoch。
```

- Invariants:
  - 只接受记忆模块种类的方法有 `read`、`read_payload`、`head`、`inspect_header`、`retract`、`purge`。种类检查先于其他检查，别的种类返回 `InvalidInput`。
  - `validate_current` 与 `validate_current_guard` 接受两个模块的 pin。`CurrentSourcesGuard` 持有 `WriteGuard`。
  - 关系端点和召回种子可以是日志事件，召回只返回记忆模块的记录：日志事件只用来沿关系展开，不作为结果。
  - 以下操作通过 `Scoped::advance_epoch` 推进 epoch：对象和关系的修订，类型修订，包含修订的抽取批次，撤回、清除，以及动作结束。每个提交的批次推进一次；重试和失败的操作不推进。
  - 列出类型、发现动作和查询当前记录，都改用 `Scoped::scan_heads`。
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
    /// 打开记忆模块某个 era 的原生分区，只读打开，不写入。
    pub(crate) fn on_era(kernel: Kernel, era: &EraPartitions) -> MemoryResult<Self>;
    /// 记忆模块的 era。
    pub fn era_id(&self) -> EraId;
    // 原生记忆的其余接口不变。
}
// 删去：create、open、create_context、open_context、context、
//       NativeMetadata、native_internal_metadata_v1、StoreEraMismatch。
```

- Invariants:
  - 写锁、持久批次和摘要（`Kernel::keyed_hasher`）都来自内核。
  - 审计事件、召回案例和清除令牌里的 era 是记忆模块的 era。

### `mmdb::store`（new，组装根）

```rust
/// 第 13 步起；之前的步骤用开发格式 ID，见 Data。
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
    pub fn issued_at_unix_ms(&self) -> u64;
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
    ResetPlanNotYetValid,
    ResetPlanExpired,
    ResetPlanForAnotherStore,
    StaleResetPlan,
    ResetDigestMismatch,
}
pub type StoreResult<T> = Result<T, StoreError>;
```

- Invariants:
  - 一个目录就是一个存储，里面有会话和记忆两个模块，一次由一个进程持有租约。
  - `reset_memory` 需要 `&mut self`，所以调用时没有句柄还活着。
  - 重置失败时，存储要么原样可用，要么返回 `CommitUnknown` 后不可再用；不存在只换了一半句柄的状态。
- Ordering（`reset_memory`）：
  1. 取存储写锁。
  2. 依次校验计划，任一项失败都不做任何改变：
     1. `issued_at ≤ now < expires_at`，按毫秒比较，换算溢出时视为无效计划。
     2. 计划的目录等于本存储租约的规范路径。
     3. 计划里的 store era 和记忆 era 仍是当前值。
     4. 用本存储的目录和 era 重算摘要，结果与计划里的摘要、输入的摘要都相等。
  3. 调用 `prepare_module_reset(MEMORY, plan.new_memory_era)`，再用返回的 `EraPartitions` 打开新的记忆记录表、上下文分区和 `MemoryDatabase`。任何一步失败都丢弃它们，存储保持原样、照常可用。
  4. 开一个批次，写入 `records.count_memory_reset`，交给 `commit_module_reset`。
  5. 提交成功后，把准备好的新句柄换进 `Records`、`context` 和 `memory`。这一步只是赋值，不会失败。最后返回回执。
- Errors:
  - TTL 不在 (0, 15 min] 内：`plan_memory_reset` 返回 `ResetPlanTtl`。
  - 计划还没到签发时间：`ResetPlanNotYetValid`。
  - 计划已过期：`ResetPlanExpired`。
  - 计划属于另一个目录，包括复制出来的目录：`ResetPlanForAnotherStore`。
  - 计划之后 era 已变：`StaleResetPlan`。
  - 摘要不符：`ResetDigestMismatch`。
  - 上面这些都不做任何改变。准备新句柄时失败也一样。
  - 切换 era 的提交结果不确定时，返回 `Kernel(CommitUnknown)`。这个 `Store` 不能再用，调用方丢弃它后重新打开。
- Configuration: 无。计划摘要用 blake3 计算，域为 `mmdb-memory-reset-v1`，覆盖目录、store era、新旧记忆 era、签发时间和过期时间。

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
  - `kernel_v1` 分区：唯一写入者是 `Kernel`。
    - 键 `store` 存 `{ store_era, digest_key, eras: { session, memory } }`（JSON）。目录里只有清单时，第一次打开写入它；之后只有 `commit_module_reset` 改它。
    - 每个 Retired era 各存一个键 `retired.<module>.<era>`，值为空。
  - 会话模块的分区，写入者是 `records` 和 `session`：
    - `records_headers_v1`、`records_heads_v1`、`records_payloads_v1`、`records_pending_v1`、`records_operations_v1`、`records_lifecycle_v1`；
    - `session_order_v1`、`session_heads_v1`、`session_events_v1`、`session_tool_calls_v1`、`session_tool_results_v1`、`session_messages_v1`、`session_checkpoints_v1`。
  - 记忆模块的分区，写入者是 `records`、`context` 和 `native_memory`：
    - `records_headers_v1`、`records_heads_v1`、`records_payloads_v1`、`records_pending_v1`；
    - `context_type_names_v1`、`context_action_names_v1`、`context_postings_v1`、`context_record_terms_v1`、`context_adjacency_v1`、`context_entity_identity_v1`、`context_extraction_receipts_v1`、`context_extraction_incomplete_v1`；
    - 除 `native_internal_metadata_v1` 以外的全部 `native_*_v1`。
  - 记录头、载荷块和回执的编码与今天相同，只有这几处变化：
    - 第 4 步：会话记录的头加上 `source_scopes`；工具调用索引的值改为 `{ record, message_id, purged }`。
    - 第 11 步：JSON 字段 `history` 改为 `event`，`history_sequence` 改为 `sequence`。
    - 第 13 步：lifecycle 分区加上记忆重置次数。
- Existing records: reset（status `startup`）。
  - `mmdb-context-v1` 和 `mmdb-native-memory-v1` 目录打开时报 `FormatMismatch`。操作者用 `store_format` 做整库重置：源格式填旧 ID，替换格式填 `mmdb-store-v1`，得到一个能直接打开的空存储。
  - 改变磁盘布局的中间步骤，各自换一个开发格式 ID，所以较早步骤建出的目录会报 `FormatMismatch`，而不是在解码时出错：
    - 第 4 步：`mmdb-context-dev4`；
    - 第 7 步：`mmdb-context-dev7`、`mmdb-native-memory-dev7`；
    - 第 9 步：`mmdb-store-dev9`；
    - 第 11 步：`mmdb-store-dev11`。
  - 只有第 13 步使用 `mmdb-store-v1`。

## Dependencies

| Dependency | Category | Production adapter | Test adapter |
| --- | --- | --- | --- |
| fjall | local-substitutable | `Kernel` 内的 fjall keyspace | 同一引擎，跑在 `tempfile::tempdir()` 里 |
| 文件系统（租约、清单、隔离目录） | local-substitutable | `std::fs` | `tempfile::tempdir()` |
| 时钟（计划的签发与过期） | in-process，由调用方传入 | 调用方传 `SystemTime::now()` | 测试传固定时间 |
| 会话模块的可读规则 | in-process seam（`records::Gate`） | `session::LogGate` | `records` 单元测试里的 `FixedGate` |

## Steps

1. 把 `store_format` 搬进新 crate `mmdb-kernel`，只重构。
   - `mmdb` 以 `pub use mmdb_kernel::store_format` 导出；`StoreFormatDescriptor::new_manifest` 改为 `pub`；`native_memory` 和 `state` 改用新路径。
   - 先不带 `--locked` 构建一次，让 `Cargo.lock` 记下新 crate，随本步提交。
   - README 的 Architecture 与 Crate Map 加上 `mmdb-kernel`。
   - After: none. Check: `cargo test --workspace --locked`
2. 让整库重置区分源格式和替换格式。
   - `ResetPlan`、摘要、重置日志和恢复都同时带上两个格式 ID；两者相同时，行为与今天一样。
   - 本仓库里 `ResetPlan::build` 的调用方一起更新。
   - 在 `mmdb-kernel` 里加测试 `reset_replaces_a_store_with_another_format`，覆盖中断后接着替换的情况。
   - After: 1. Check: `cargo test --workspace --locked`
3. 修复可用性 epoch 漏推进：类型修订和包含修订的抽取批次推进 epoch，每个提交的批次推进一次，重试和失败不推进。
   - 加测试 `revising_a_type_or_extracting_advances_the_availability_epoch`。测试用一个未列为检查点输入的记录，免得逐项检查 pin 掩盖 epoch 没变。
   - After: 2. Check: `cargo test --workspace --locked`
4. 在今天的 `context` 里实现日志只读。
   - 日志事件和检查点的历史读取不再沿来源检查，只看：
     - 记录自己的状态；
     - 消息状态；
     - 工具调用索引上的撤下标记；
     - 检查点的 epoch；
     - `source_scopes`。
   - 来源校验返回经过的 scope，由 `append_history` 和 `save_checkpoint` 写进 `source_scopes`。
   - 工具调用索引的值改为 `{ record, message_id, purged }`，清除调用时同批标记。
   - 当前检查不变。
   - 上下文格式改为 `mmdb-context-dev4`。
   - 测试：
     - 改写 `history_copies_old_object_and_type_pins_but_purge_blocks_object_copies`，成为 `log_copies_stay_readable_after_their_sources_change`；
     - 改写 `copied_history_tracks_source_availability_and_session_catalog_pages`；
     - 改写 `tool_results_link_historical_call_sources_and_still_follow_purge`，成为 `tool_results_stay_readable_after_their_call_source_is_purged`：清除调用引用的对象后，调用和结果仍可读，当前检查失败；清除调用本身后，结果不可读；
     - 改写 `live_revision_keeps_checkpoint_derived_originals_and_pairs_the_completed_tool` 末尾的断言：清除原始日志事件后，检查点和派生对象不可读，调用和结果仍可读；
     - 现有测试里，凡是断言「日志事件或工具结果因为来源被清除、撤回或修订而读不到」的，都照同样的规则改写：事件仍可读，当前检查失败；
     - 加入 `a_log_copy_needs_the_scopes_of_its_sources`、`purging_a_log_event_hides_what_was_derived_from_it`、`a_withdrawn_fragment_hides_its_whole_message`。
   - After: 3. Check: `cargo test --workspace --locked`
5. 在 `mmdb-kernel` 里加入内核存储。
   - 接口：
     - `Kernel::{create, open, canonical_root, store_era, era, era_state, module, batch, write_lock, keyed_hasher}`；
     - 写在 `kernel_v1` 里的模块 era 与摘要密钥；
     - 按「模块.era.名称」命名的分区；
     - `EraPartitions`、`Partition`、`Batch`、`Bytes`、`WriteGuard`；
     - `EraId`、`OperationId`。
   - 行为：
     - 先校验 module spec；
     - 打开时识别分区，删除非当前 era 的分区，遇到未知名字返回 `Corrupt`；
     - 批次检查分区属于同一内核、era 仍是当前 era；
     - 普通提交失败返回 `CommitUnknown`。
   - kernel 测试：`a_second_open_of_a_store_is_busy`、`store_metadata_is_created_once_and_checked_against_the_manifest`、`open_rejects_another_format`、`module_specs_are_checked_before_opening`、`a_batch_refuses_a_partition_of_another_store`、`handles_keep_the_lease_until_the_last_one_drops`。
   - After: 2. Check: `cargo test -p mmdb-kernel --locked && cargo test --workspace --locked`
6. 把共用值类型移出原生记忆，只重构。
   - `EraId`、`OperationId` 改用内核的。
   - `Actor`、`Scope`、`TemporalFacts`、`RecordState` 搬进新的 `mmdb::records`，在 `mmdb::` 根导出。
   - `TemporalFacts::validate` 返回 `InvalidTemporalFacts`，两个调用方各自转换。
   - `MAX_RECALL_SCOPES` 改名为 `records::MAX_ACCESS_SCOPES`。
   - 本仓库里所有引入它们的地方一起更新，包括各 crate、测试和 example。
   - After: 4, 5. Check: `cargo test --workspace --locked`
7. 让原生记忆和领域上下文经由内核打开。
   - `MemoryDatabase` 的四个打开函数改走 `Kernel`，只有一个 `memory` 模块，内部经 `MemoryDatabase::on_era` 打开。
   - 原生元数据分区交给内核。原生记忆和上下文里直接使用摘要密钥的地方（`blake3::keyed_hash`、`Hasher::new_keyed`）一起改用 `Kernel::keyed_hasher`，每种用途取一个固定的 domain 名；摘要值因此改变，由本步的新格式 ID 隔开。
   - 格式改为 `mmdb-context-dev7` 和 `mmdb-native-memory-dev7`，同步原生测试里的格式断言。
   - `native_memory`、`context`、`entity`、`extraction` 里的 fjall 类型全部换成内核类型；`MemoryError` 与 `ContextError` 中包住 fjall 错误的变体改为包住 `KernelError`。crate 内部测试里直接写分区的地方（注入损坏、模拟旧写入者）改用 `Batch`。
   - After: 6. Check: `cargo test --workspace --locked`
8. 从领域上下文抽出记录层，只重构。
   - 新增 `records::{Records, RecordTable, Scoped, Gate, Availability, StreamedPayload, HeadPage}`，此时仍只有一张表。
   - 日志与检查点的可读规则放到 `Gate` 后面，按 `kind.module()` 选择要检查的记录；实现暂时留在 `context`。
   - 列表和当前查询改用 `scan_heads`，追加改用 `stream_payload` 与 `publish`。
   - `Scoped` 持有自己的 `ContextAccess`。
   - After: 7. Check: `cargo test --workspace --locked`
9. 加入带两个模块的 `Store`。
   - `Store::{create, open, era, context, memory}`，内核模块为 `session` 和 `memory`，格式 `mmdb-store-dev9`。
   - `Records` 有会话、记忆两张表，lifecycle 和操作回执都在会话模块。`ContextStore` 暂时保留日志方法，按记录种类写到对应的表。
   - `context` 从 `native_memory` 的子模块改为 crate 顶层模块。
   - 删去 `MemoryDatabase` 的打开函数和 `context()`。原生测试的 `create_test_database` 和重开测试改用 `Store`。
   - `context_contract`、`context/tests.rs` 和三个 context example 改用 `Store` 打开。
   - 测试：
     - 加入 `a_memory_purge_commits_its_head_epoch_and_receipt_together`、`a_legacy_directory_resets_into_an_empty_store`；
     - 改写 `format_and_store_identity_prevent_accidental_adoption`。
   - 加入 `tests/layering.rs`，断言 `src/records/`、`src/context/`、`src/store.rs`、`src/native_memory.rs`、`src/entity.rs`、`src/extraction.rs` 中都不出现 `fjall`。
   - 更新 README 的 Harness Context 和 AGENTS.md 里关于打开函数的 gotcha。
   - After: 8. Check: `cargo test --workspace --locked && cargo test --locked -p mmdb --test context_contract independent_harnesses_share_types_history_and_relations_after_reopen -- --exact`
10. 加入会话模块。
    - `context/history.rs`、`checkpoints.rs` 和 `search_history` 搬进 `session/`。`SessionLog`（先沿用旧方法名）和 `LogGate` 也放在这里，由 `Store::session_log` 提供。
    - `ContextStore` 删去日志方法：保留只用于记忆种类的 `read_payload`；召回只返回记忆记录；所有句柄先检查记录种类。
    - 日志相关测试改用 `store.session_log`：对日志事件的 `read` 改用 `SessionLog::read_payload`，对检查点的 `read` 改用 `read_checkpoint`；`validate_current` 仍在 `ContextStore` 上。
    - 新测试：`session_log_and_context_reject_the_same_invalid_access`、`access_bound_to_a_session_appends_only_to_it`、`log_permissions_follow_the_actor`、`log_reads_and_searches_refuse_budgets_over_their_limits`、`search_walks_sessions_in_key_order_and_events_in_sequence`、`a_streaming_append_blocks_memory_writes_until_it_ends`、`recall_reaches_through_log_events_but_returns_memory_records`、`one_operation_id_names_one_write_across_the_log_and_context`。
    - `tests/layering.rs` 加上 `src/session/`。
    - 磁盘布局不变。
    - After: 9. Check: `cargo test --workspace --locked && cargo test --locked -p mmdb --test context_contract independent_harnesses_share_types_history_and_relations_after_reopen -- --exact`
11. 按术语表给会话日志的类型、字段和方法改名（见上表），格式改为 `mmdb-store-dev11`，同步测试、example 和 README。只改名。After: 10. Check: `cargo test --workspace --locked`
12. 在内核里加入模块重置。
    - `Kernel::{prepare_module_reset, commit_module_reset}`、Retired 键和 `era_state` 的 Retired 状态；删除旧分区并报告 `cleanup_pending`；切换结果不确定时让内核中毒。
    - kernel 测试：`module_reset_deletes_the_old_era_partitions`、`module_reset_cleanup_resumes_after_interruption`、`module_reset_refuses_a_current_or_retired_era`、`a_batch_refuses_partitions_of_a_stale_era`、`an_unknown_reset_outcome_poisons_the_kernel`。
    - After: 5. Check: `cargo test -p mmdb-kernel --locked && cargo test --workspace --locked`
13. 原地重置记忆。
    - 代码：
      - `Store::{plan_memory_reset, reset_memory}`、`MemoryResetPlan`、`MemoryResetReceipt` 和 `StoreError` 的重置变体；
      - `ContextError::Reset` 和三分支的 `check_ref`；
      - 当前检查里 Retired 来源算终点；
      - 可用性 epoch 计入记忆重置次数，并删去脱敏 epoch 的兼容分支；
      - `Records::{replace_memory_table, count_memory_reset}`；
      - 格式改为 `mmdb-store-v1`。
    - 测试：
      - `memory_reset_needs_the_exact_plan_digest`；
      - `memory_reset_keeps_the_session_log_and_hides_old_memory`：先写入原生证据、本体对象和关系、动作、以记忆为来源的日志事件、会话和运行检查点，以及一个日志回执；重置并重开后，断言日志可读、能追加下一个序号、读取旧记忆返回 `Reset`、原生召回为空；
      - `log_events_that_copied_reset_memory_stay_readable_and_current`、`checkpoints_saved_before_a_memory_reset_cannot_resume`、`a_checkpoint_generated_before_a_memory_reset_is_refused`、`a_log_retry_after_a_memory_reset_returns_the_original_receipt`、`reset_purged_and_foreign_refs_fail_differently`、`memory_reset_refuses_a_plan_for_another_time_era_or_directory`、`a_failed_memory_reset_leaves_the_store_usable`；
      - 改写 `whole_store_reset_quarantines_the_directory_and_starts_new_eras`；
      - 删除 `missing_and_stale_visibility_markers_do_not_restore_old_checkpoint_payloads`。
    - 文档与收尾：
      - 更新 AGENTS.md 的重置 gotcha 和 README；
      - 把 session-log 和 context-checkpoints 两份 feature doc 中指向已搬走文件的链接改到 `session/`；
      - 把 session-log、context-ontology、context-checkpoints 三份 feature doc 设为 `shipped`；
      - 把本 spec 移到 `docs/spec/archived/`。
    - After: 11, 12. Check: `cargo test --workspace --locked`

## Not this

- 不改 miumiu。它的入口、句柄、类型路径和记忆重置在 miumiu 仓库里改；本 spec 只规定它会用到的接口。
- 不统一原生记忆和记录层的操作回执，也不把回执和键编码放进内核；不收拢三套记忆模型。这些属于第 3 部分。
- 不动 `StateStore`、`Database::state()`、多模型 `Database`、`mmdb-storage`、`mmdb-vector`、`mmdb-graph`、`mmdb-blob`。它们仍直接使用 fjall，属于第 2、3 部分。
- 不加依赖白名单测试，也不移出 WebAssembly UDF。
- 不加会话头记录、fork、会话列表修订号、轮次事件或对话缓存。
- 读取输入流期间不释放写锁。
- 不提供单独重置会话模块的接口；整库重置仍是 `store_format` 的目录交换。
- 不绕开 fjall 的批次提交路径。fjall 2.11 的批次会忽略 journal 写入本身的错误，但内核和今天所有写入一样，把提交成功当作已持久化。
