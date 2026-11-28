# 对话缓存: implement spec

Feature: `docs/feature/conversation-cache.md` @ `1eff1d2ab8ebde59000e36b2b98744c73cab4c6a`

This spec is open until the feature ships, and `/go-work` carries it out. Do not edit it while working it; only `to-implement-spec` changes it. Before work starts or resumes, check that the hash above still matches `git hash-object docs/feature/conversation-cache.md`; if it does not, the feature changed, so stop and rerun `to-implement-spec`. Each step is one commit whose message ends with the trailer `Spec-Step: conversation-cache#<n>`. The commit that lands the last step sets the feature doc's Status to `shipped` and moves this file to `docs/spec/archived/<YYYY-MM-DD>-conversation-cache.md` with `git mv`, dated that day. Nothing reads an archived spec again.

会话模块新增两份由日志写入路径维护的索引：每个会话各来源 scope 首次出现的位置，以及每次撤回或清除让哪个位置以前的内容不可读。新模块 `session::coverage` 独占这两份索引，回答「位置 1..N 需要哪些 scope」「epoch E 之后有没有撤回到达 1..N」。新模块 `session::caches` 把对话缓存存成不是记录的行和分块载荷，用 `coverage` 判定读取权限与过期，并由 `recover_payloads` 回收被替换、删除、中断和过期的载荷。分块载荷的格式从 `records` 的记录载荷里抽成一处，两边共用。

## Behavior map

| Feature doc line | Owner | Test |
| --- | --- | --- |
| Who: 保存和删除需要 System 或 Operator | `session::caches` | `only_system_or_operator_save_or_remove_caches` |
| Who: 保存需要能读到覆盖的每个事件 | `session::caches` 比对 `session::coverage::scopes_through` | `saving_needs_every_covered_source_scope` |
| Who: 查找、列出和读取需要能读到覆盖的每个事件 | `session::caches` 比对 `CacheRow.scopes` | `reading_a_cache_needs_every_covered_source_scope` |
| Who: access 绑定 session 时只能操作这个会话的缓存 | `session::caches` | `a_session_bound_access_reaches_only_its_session_caches` |
| Entry: 五个方法与 `availability_epoch` | `session::caches` | `a_cache_and_the_log_tail_restore_the_conversation_after_reopen` |
| Does: 每个派生名只保留最新一份，不论位置先后 | `session::caches` | `each_derivation_keeps_only_its_latest_cache` |
| Does: N 可以落在 fork 继承的前缀里；fork 不带缓存 | `session::caches` 经 `View::spans` | `a_cache_may_end_inside_a_forked_prefix_and_forks_start_without_caches` |
| Does: 查找与列出，每项字段与是否过期 | `session::caches` | `listing_pages_caches_in_derivation_name_order` |
| Does: 按版本和偏移翻页读取 | `session::caches` 经 `records::chunks` | `a_cache_payload_spans_many_chunks_and_pages` |
| Does: 读这份缓存，再读 N 之后的事件，就能还原对话 | `session::caches` | `a_cache_and_the_log_tail_restore_the_conversation_after_reopen` |
| Does: 读完缓存再从 N+1 起翻页读会话日志 | `session::caches` | `a_cache_behind_the_log_restores_the_rest_from_the_tail` |
| Does: 过期——撤回或清除覆盖的事件 | `session::history::withdraw_event` 记录，`session::coverage::withdrawn_since` 判定 | `withdrawing_a_covered_event_makes_the_cache_stale` |
| Does: 过期——N 之后的片段或工具调用使 N 以前开始的消息不可读 | 同上 | `withdrawing_a_later_part_of_a_covered_message_makes_the_cache_stale` |
| Does: 过期——来源会话里撤回 fork 共享的前缀 | 同上 | `withdrawing_a_shared_prefix_event_makes_fork_caches_stale` |
| Does: 仍然有效——N 之后的撤回、其他修订、重置记忆、重开收尾 | `session::coverage` | `later_withdrawals_and_other_changes_keep_the_cache_current` |
| Does: 删除；没有缓存时什么也不做 | `session::caches` | `removing_a_cache_and_removing_nothing` |
| Fails: 保存被拒绝时什么也不写 | `session::caches` | `saving_refuses_bad_input_and_writes_nothing` |
| Fails: 给定 epoch 之后 N 及以前有事件被撤回或清除，拒绝保存 | `session::caches` 经 `session::coverage::withdrawn_since` | `saving_refuses_a_derivation_that_raced_a_covered_withdrawal` |
| Fails: 不带操作 ID，重复保存只是再替换一次 | `session::caches` | `repeating_a_save_replaces_the_cache_again` |
| Fails: 保存中断，原缓存不变，`recover_payloads` 清理 | `session::caches` | `an_interrupted_save_keeps_the_previous_cache`（单元测试） |
| Fails: 读取已过期报已过期；被替换或删除报已被取代 | `session::caches` | `reading_reports_stale_and_replaced_caches` |
| Fails: 缺 scope 时查找和读取被拒绝访问 | `session::caches` | `reading_a_cache_needs_every_covered_source_scope` |
| Fails: 缺 scope 时列表不返回这份缓存 | `session::caches` | `listing_skips_unreadable_caches_without_naming_them` |
| Fails: 这个 feature 之前的存储打开时被拒绝 | `store`（`STORE_FORMAT_ID`） | `format_and_store_identity_prevent_accidental_adoption`（扩展） |
| Limits: 名称 1–256 字节；列表每页 1–100 份 | `session::caches` | `cache_names_and_pages_are_bounded` |
| Limits: 32 KiB 分块、单次读取最多 1 MiB | `records::chunks` | `a_cache_payload_spans_many_chunks_and_pages` |
| Limits: 不设大小和数量上限 | `session::caches` | `a_cache_payload_spans_many_chunks_and_pages` |
| Keeps: 重置记忆不影响 | 会话模块的分区 | `caches_survive_a_memory_reset` |
| Keeps: 过期缓存的磁盘空间由 `recover_payloads` 有界回收 | `session::caches` | `recovery_rescans_caches_withdrawn_during_a_sweep_after_reopen`（单元测试） |
| Keeps: 被替换或删除的缓存载荷同样由 `recover_payloads` 回收 | `session::caches` | `recovery_reclaims_replaced_removed_and_stale_payloads`（单元测试） |
| Keeps: 被替换或删除前，列表里仍显示为过期 | `session::caches` | `recovery_reclaims_replaced_removed_and_stale_payloads`（单元测试） |
| Keeps: 不是记录，不进搜索 | `session::caches`（不建记录头，不写 order） | `cache_contents_are_not_search_hits` |
| Keeps: 不推进会话变更号 | `session::caches`（不调 `Changes::put_head`） | `saving_or_removing_a_cache_keeps_the_session_change_number` |
| Not this: fork 时复制缓存 | `session::fork`（不碰缓存分区） | `a_cache_may_end_inside_a_forked_prefix_and_forks_start_without_caches` |

公开接口的测试放在新文件 `crates/mmdb/tests/conversation_cache.rs`；需要看分区内容或丢弃提交的测试是 `session/caches.rs` 和 `session/coverage.rs` 里的单元测试。

Replaces: none.

## Modules

### `records::chunks` (new, extracted from `records/payload.rs`)

```rust
/// Where one payload's chunks live: a large-value partition and a key prefix
/// that no other payload shares. Chunk keys are `prefix ‖ chunk_index_be`.
pub(crate) struct ChunkSpace<'a> {
    pub partition: &'a Partition,
    pub prefix: Vec<u8>,
}

/// Length and keyed digest of a complete payload.
pub(crate) struct ChunkedPayload {
    pub bytes: u64,
    pub digest: [u8; 32],
}

pub(crate) struct RemovedChunks {
    pub removed: usize,
    /// No chunk of the space remains.
    pub complete: bool,
}

impl Scoped<'_> {
    /// Streams `payload` in 32 KiB chunks. With `space`, each chunk is committed
    /// in its own batch; without it the payload is only measured and hashed.
    pub(crate) fn write_chunks<R: Read>(
        &self,
        space: Option<&ChunkSpace<'_>>,
        payload: R,
    ) -> ContextResult<ChunkedPayload>;

    /// Writes a small payload's chunks into `batch`.
    pub(crate) fn put_chunks(
        &self,
        batch: &mut Batch,
        space: &ChunkSpace<'_>,
        bytes: &[u8],
    ) -> ChunkedPayload;

    /// Bytes `offset..offset + limit` of a `total`-byte payload.
    pub(crate) fn read_chunks(
        &self,
        space: &ChunkSpace<'_>,
        total: u64,
        offset: u64,
        limit: usize,
    ) -> ContextResult<PayloadPage>;

    /// Removes up to `max` chunks of `space` in `batch`.
    pub(crate) fn remove_chunks(
        &self,
        batch: &mut Batch,
        space: &ChunkSpace<'_>,
        max: usize,
    ) -> ContextResult<RemovedChunks>;
}
```

- Invariants: 每个块存成 `digest ‖ bytes`，块摘要用 `context-content` 域；`write_chunks` 的整份摘要用 `context-history-payload` 域，`put_chunks` 的整份摘要用 `context-content` 域，与现在的记录载荷逐字节相同，不改格式。
- Ordering: `write_chunks` 之前由调用方登记清理项，之后由调用方在一个批次里发布指向这块空间的行并撤掉清理项；`write_chunks` 自己不登记也不发布。
- Errors: `read_chunks` 的 `limit` 不在 1..=1 MiB 或 `offset > total` 时 `InvalidInput`；缺块、块长度不对、摘要不符时 `Corrupt`；读入流出错时 `Io`；长度溢出时 `BudgetExceeded`。
- Performance: `write_chunks` 每 32 KiB 提交一次，内存只占一个块；`read_chunks` 只读覆盖所求范围的块。

Behind the seam: 块的编码、块大小、两种摘要域、块键的序号编码。`records/payload.rs` 的 `put_payload`、`stream_payload`、`payload_slice_locked` 和 `recover_payloads` 改为以 `ChunkSpace { partition: payloads, prefix: revision_key(pin) }`（清除时 `record_key`）调用这四个函数，自己只保留 `StagedUpload` 的登记、发布和核对。

Call site（`records/payload.rs`）:

```rust
let space = ChunkSpace { partition: &self.table(module).payloads, prefix: revision_key(pin) };
let page = self.read_chunks(&space, header.payload.bytes, offset, limit)?;
```

### `records::lifecycle` (changed)

```rust
impl Scoped<'_> {
    pub(crate) fn commit_state_change(
        &self,
        operation: OperationId,
        expected: RecordPin,
        state: RecordState,
        redacts: bool,
        /// Receives the previous header, the new header and the availability
        /// epoch this change advanced to.
        update_indexes: impl FnOnce(&mut Batch, &RecordHeader, &RecordHeader, u64) -> ContextResult<()>,
    ) -> ContextResult<WriteReceipt>;
}
```

- Invariants: 传给闭包的 epoch 就是同一批次写入的有效可用性 epoch（本 owner 的计数加记忆重置次数），提交后 `availability_epoch_locked` 返回它；每次状态变更恰好推进一次。
- `state_change` 改为返回 `(RecordHeader, RecordHeader, u64)`；`context::records` 的调用方忽略 epoch。

### `session::view::View` (changed)

```rust
/// The positions `after + 1..=through` one session holds in a view.
pub(super) struct Span<'a> {
    pub holder: &'a str,
    pub after: u64,
    pub through: u64,
}

impl View<'_> {
    /// Spans covering positions 1..=position, oldest first, clipped to `position`.
    pub fn spans(&self, position: u64) -> impl Iterator<Item = Span<'_>> + '_;
}
```

- Invariants: 各段首尾相接、不重叠，最后一段的 `through` 等于 `position`（`position` 为 0 时没有段）；`holder` 与 `View::holder` 对同一位置给出的会话一致。`entries_after` 改用 `spans` 计算各段的范围。

### `session::coverage` (new)

```rust
/// What positions 1..=N of a session log depend on: the scopes needed to read
/// them and the withdrawals that reached them. Owns the source-scope and
/// withdrawal indexes; no other module reads or writes their keys.
pub(super) struct Coverage<'a> {
    parts: &'a SessionParts,
    owner: &'a str,
}

impl<'a> Coverage<'a> {
    pub fn new(parts: &'a SessionParts, owner: &'a str) -> Self;

    /// Called for every event written to `session` at `position`, inside its batch.
    /// Several events of one batch may carry the same scope.
    pub fn record_scopes(
        &self,
        batch: &mut Batch,
        session: &str,
        position: u64,
        scopes: &[Scope],
    ) -> ContextResult<()>;

    /// Called once per retraction or purge of an event, inside its batch:
    /// the withdrawal stamped `epoch` made `holder`'s events from `position` on unreadable.
    /// It also becomes the owner's latest withdrawal.
    pub fn record_withdrawal(&self, batch: &mut Batch, holder: &str, epoch: u64, position: u64);

    /// Scopes, beyond the session's own, that reading positions 1..=position of `view` needs.
    pub fn scopes_through(&self, view: &View<'_>, position: u64) -> ContextResult<Vec<Scope>>;

    /// Whether a withdrawal stamped after `epoch` reached a position in 1..=position of `view`.
    pub fn withdrawn_since(&self, view: &View<'_>, position: u64, epoch: u64) -> ContextResult<bool>;

    /// The epoch stamped on the owner's latest withdrawal in any session; 0 when there is none.
    pub fn last_withdrawal(&self) -> ContextResult<u64>;
}
```

- Invariants:
  - 来源 scope 索引的每一行是（会话, scope, 位置），一个（会话, scope）最小的位置就是它第一次出现的位置。`record_scopes` 只看已提交的行：这个（会话, scope）已有行时不写，没有时为这个位置写一行。
  - 批次读不到自己未提交的写入，所以同一批次里几个事件带来同一个新 scope 时（例如轮次收尾或 fork 截断收尾补齐的几份工具结果），每个事件各写一行，最早的一行仍在。之后的批次能看到已提交的行，不再写。
  - 撤回索引对每个（holder, epoch）一行，另对每个 owner 一行记最近一次撤回的 epoch。一次状态变更的批次只撤回一个事件，所以一个批次不会两次写同一个（holder, epoch）。可用性 epoch 每次状态变更都推进，所以后写入的撤回 epoch 总是更大，`record_withdrawal` 直接覆盖 owner 行，不用先读。
  - `scopes_through` 不含会话自己的 scope，按 `Scope` 的顺序去重排列。被撤回事件带来的 scope 仍然计入，只会更严，不会放宽。
- Ordering: `record_scopes` 和 `record_withdrawal` 必须与写事件或改状态在同一批次提交。
- Errors: 存储错误照传；键或值解不开时 `Corrupt`。
- Performance:
  - `scopes_through` 对视图的每一段做一次前缀扫描。扫过的行数是这个会话出现过的来源 scope 数，加上首次带来某个 scope 的批次里同一 scope 多写的行，每个批次最多多出它写入的事件数。
  - `withdrawn_since` 对每一段从 `epoch + 1` 起做一次范围扫描，扫过的行数是这个会话在那之后的撤回次数。
  - 两者都与日志长度无关，段数不超过 64。`last_withdrawal` 读一行。

Behind the seam: scope 的键编码（`session_key ‖ segment(encode(scope)) ‖ position_be`），一个 scope 的行按位置排在一起，每段取第一行与该段的 `through` 比较；撤回键（`session_key(holder) ‖ epoch_be`）和 owner 行的键（`owner_key(owner)`，比任何撤回键都短，不落进按会话的前缀扫描）；沿 `View::spans` 逐段剪裁到 N；`epoch` 为 `u64::MAX` 时的边界。

写入方（`session::history`，changed）:

- `put_event` 写完事件头后调用 `record_scopes(batch, session, sequence, &header.source_scopes)`；追加、`put_turn_end` 补齐和 fork 截断收尾都经过它，所以每个事件都会登记。
- `withdraw_event(batch, event, epoch)` 多收一个 epoch，对每一种事件都调用一次 `record_withdrawal`：事件属于一条 assistant 消息（assistant 事件，或带 `message_id` 的工具调用）时，记在 `view.holder(message.started)` 与 `message.started`；否则记在 `event.session` 与 `event.sequence`。现有标记工具调用 purged 和消息 unavailable 的行为不变。`SessionLog::change_state` 把 `commit_state_change` 交给闭包的 epoch 传进来。

Call site（`session::caches` 判定过期）:

```rust
let view = self.view(session)?;
let coverage = Coverage::new(self.parts, owner);
let stale = row.reclaimed || coverage.withdrawn_since(&view, row.position, row.epoch)?;
```

### `session::caches` (new)

```rust
// Re-exported from `mmdb::session`.

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConversationCacheInput {
    pub session: String,
    /// Harness-defined derivation name; MMDB never interprets it.
    pub derivation: String,
    /// The last log position applied to the conversation; it holds every event in 1..=position.
    pub position: u64,
    pub media_type: String,
    /// `SessionLog::availability_epoch` read before the harness started reading
    /// the log, or the cache, it derived this conversation from.
    pub expected_availability_epoch: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConversationCache {
    pub session: String,
    pub derivation: String,
    /// New on every save. Pass the whole value to `read_conversation_cache`.
    pub version: Ulid,
    pub position: u64,
    pub media_type: String,
    pub bytes: u64,
    pub digest: [u8; 32],
    pub saved_at_ms: i64,
    pub provenance: Provenance,
    pub stale: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConversationCachePage {
    pub caches: Vec<ConversationCache>,
    /// The derivation of the last cache in `caches`; pass as `after` to continue.
    /// Set when the page is full and another cache the reader can read follows it.
    pub next: Option<String>,
}

impl SessionLog<'_> {
    /// The current availability epoch; read it before deriving a conversation.
    pub fn availability_epoch(&self) -> ContextResult<u64>;

    pub fn save_conversation_cache<R: Read>(
        &self,
        input: ConversationCacheInput,
        payload: R,
    ) -> ContextResult<ConversationCache>;

    pub fn conversation_cache(
        &self,
        session: &str,
        derivation: &str,
    ) -> ContextResult<Option<ConversationCache>>;

    /// The reader's readable caches of one session, in UTF-8 byte order of the
    /// derivation name, after `after`.
    pub fn conversation_caches(
        &self,
        session: &str,
        after: Option<&str>,
        limit: usize,
    ) -> ContextResult<ConversationCachePage>;

    pub fn read_conversation_cache(
        &self,
        cache: &ConversationCache,
        offset: u64,
        limit: usize,
    ) -> ContextResult<PayloadPage>;

    /// Returns whether a cache was removed.
    pub fn remove_conversation_cache(&self, session: &str, derivation: &str) -> ContextResult<bool>;
}

// records::error — two new variants.
pub enum ContextError {
    // …
    /// An event the cache covers was retracted or purged after its derivation started.
    ConversationCacheStale,
    /// The cache was replaced or removed since it was looked up.
    ConversationCacheReplaced,
}
```

- Invariants: 一个（owner, 会话, 派生名）最多一行；`version` 每次保存重新生成，块空间以它为前缀，所以被替换的旧载荷与新载荷不共享任何键。返回给调用方的值里没有 pin，缓存没有记录头，不写 order、events 或 changes 分区，所以不进搜索、不能作为来源、不推进会话变更号。
- Ordering:
  - 派生前读 `availability_epoch`，在读缓存或日志之前。恢复后继续在内存里追加对话的 harness，之后每次保存都用恢复前读到的那个 epoch；只有丢弃整份对话、重新读 epoch 之后再从缓存或日志重新派生，才换用新的 epoch。保存因 `AvailabilityConflict` 被拒绝时也这样重新派生。MMDB 分不出 harness 何时读到哪个 epoch，这条只能由 harness 遵守。
  - `position` 是已连续应用到对话的最后一个事件的位置，不是保存时 `position(session)` 的返回值：其他写者可能在派生和保存之间追加了事件。轮次中自己追加的事件也从日志尾部读回再应用，所以应用过的位置总是连续的。
  - 读一份缓存先用 `conversation_cache` 或 `conversation_caches` 取得 `ConversationCache`，再按 `next_offset` 翻页读，读完再从 `position + 1` 起用 `events` 读日志尾部。
- Errors:
  - `save_conversation_cache`：actor 不是 System 或 Operator、access 绑定了别的会话、读者不可访问会话 scope、保存者缺 `scopes_through` 里的某个 scope 时 `AccessDenied`；会话没有会话头时 `UnknownSession`；派生名或媒体类型不合法、`position` 为 0、`expected_availability_epoch` 大于当前值时 `InvalidInput`；`position` 超过会话当前位置时 `PositionConflict { expected: position, actual }`；`withdrawn_since(view, position, expected)` 为真时 `AvailabilityConflict { expected, actual }`。这些检查都在写任何东西之前。读入流出错时 `Io`，已写的块由清理项回收。最后的发布提交失败时报存储错误，harness 再保存一次。
  - `conversation_cache`：access 绑定了别的会话、读者不可访问会话 scope、或缺 `CacheRow.scopes` 里的某个 scope 时 `AccessDenied`；会话没有会话头时 `UnknownSession`；派生名不合法时 `InvalidInput`；没有这个派生名时 `Ok(None)`。任何 actor 都可以查找。
  - `conversation_caches`：会话绑定、会话 scope 和 `UnknownSession` 同 `conversation_cache`；`limit` 不在 1..=100 或 `after` 不是合法的派生名时 `InvalidInput`；读者缺 scope 的缓存跳过不报，`next` 也不会是它们的派生名。`after` 不必是现存的派生名，列表从它之后的第一个名称接着排。
  - `read_conversation_cache`：行不存在或 `version` 不符时 `ConversationCacheReplaced`；过期时 `ConversationCacheStale`；缺 scope 时 `AccessDenied`；范围不合法时 `InvalidInput`。
  - `remove_conversation_cache`：actor、会话绑定和会话 scope 的 `AccessDenied`；`UnknownSession`；没有这一行时 `Ok(false)`。
- Configuration: none.
- Performance: 保存在写锁下流式写入载荷，每 32 KiB 一次提交，加一次 `scopes_through` 和一次 `withdrawn_since`；查找和每次翻页读取各做一次 `withdrawn_since`。列表一页一直扫到凑满 `limit` 份可读缓存或到达末尾，不设扫描上限，最多扫过这个会话的全部缓存行：每份返回的缓存做一次 `withdrawn_since`，读者缺 scope 的行只解码并比较 `CacheRow.scopes` 就跳过。页满之后继续这样跳过读者缺 scope 的行，直到遇到下一份可读缓存或到达末尾，以决定 `next`，所以不会留下一个空的末页。

Behind the seam:

- 行与键：`caches` 分区键为 `session_key(owner, session) ‖ derivation 的 UTF-8 字节`，派生名不加长度前缀：它是键的最后一段，`session_key` 自带长度，所以前缀扫描只落在一个会话内，键序就是派生名的字节序。值为 `CacheRow { version, position, media_type, bytes, digest, saved_at_ms, provenance, epoch, scopes, reclaimed }`，其中 `epoch` 是保存时给出的 `expected_availability_epoch`，`scopes` 是 `scopes_through(view, position)`，`reclaimed` 在回收过期载荷时置位。块空间为 `cache_chunks` 分区的 `owner_key(owner) ‖ version`。`cache_pending` 分区键为 `owner_key(owner) ‖ version`，值为 `PendingCache { session, derivation }`。
- 保存顺序：锁住写锁；完成全部校验；单独提交一个 `cache_pending` 清理项；`write_chunks` 写入载荷；最后一个批次写新行，删掉新版本的清理项，原行存在且未回收时为原版本登记清理项。中途退出只留下清理项和孤立的块。
- 读取权限：会话 scope 和 `CacheRow.scopes` 都在 access 的 scope 里。
- 过期：`reclaimed`，或 `withdrawn_since(view, position, epoch)`。
- 删除：一个批次删行，未回收时为它的版本登记清理项。

Call site（harness 恢复后在轮次结束时保存）:

```rust
const DERIVATION: &str = "miumiu/anthropic-messages/v1";

/// Applies every event after `after`, advancing `after` past each one applied.
fn apply_tail(log: &SessionLog<'_>, conversation: &mut Conversation, after: &mut u64) -> ContextResult<()> {
    loop {
        let page = log.events("chat", *after, 100)?;
        for header in &page.entries {
            conversation.apply(log, header)?;
            *after = header.event.as_ref().map_or(*after, |event| event.sequence);
        }
        if page.next_sequence.is_none() {
            return Ok(());
        }
    }
}

let log = store.session_log(access)?;
let epoch = log.availability_epoch()?;
let (mut conversation, mut after) = match log.conversation_cache("chat", DERIVATION)? {
    Some(cache) if !cache.stale => {
        let mut bytes = Vec::with_capacity(cache.bytes as usize);
        let mut offset = Some(0);
        while let Some(at) = offset {
            let page = log.read_conversation_cache(&cache, at, 1 << 20)?;
            bytes.extend(page.bytes);
            offset = page.next_offset;
        }
        (Conversation::decode(&bytes)?, cache.position)
    }
    _ => (Conversation::default(), 0),
};
apply_tail(&log, &mut conversation, &mut after)?;
// … run the turn, appending events …
apply_tail(&log, &mut conversation, &mut after)?;
log.save_conversation_cache(
    ConversationCacheInput {
        session: "chat".into(),
        derivation: DERIVATION.into(),
        position: after,
        media_type: "application/json".into(),
        expected_availability_epoch: epoch,
    },
    conversation.encode()?.as_slice(),
)?;
```

### `session::SessionLog::recover_payloads` (changed)

```rust
impl SessionLog<'_> {
    /// Cleans abandoned uploads, purged payloads and the payloads of replaced,
    /// removed, interrupted or stale conversation caches; never live data.
    pub fn recover_payloads(&self, max_chunks: usize, max_uploads: usize) -> ContextResult<RecoveryReport>;
}
```

- Ordering:
  - 先做现有的记录清理。
  - 剩余预算再处理本 owner 的 `cache_pending`：每一项先核对它不是一行未回收缓存的当前版本（否则 `Corrupt`），再用 `remove_chunks` 删块，删完才删清理项并计一次 `uploads_removed`。
  - 预算还有余时扫描本 owner 的缓存行。`cache_sweep` 行记下本轮的游标，以及本轮开始时的 `Coverage::last_withdrawal`。没有游标、且 `last_withdrawal` 不大于记下的 epoch 时，跳过扫描；没有游标而 `last_withdrawal` 更大时，以它开始新一轮。从游标起每次最多检查 4,096 行，把过期且未回收的行置 `reclaimed` 并登记清理项，与推进游标在同一批次。扫到末尾时清掉游标、保留本轮的 epoch；`last_withdrawal` 已大于它时，下一次调用开始新一轮。
- Invariants:
  - 预算校验沿用 1..=4,096 块、1..=100 项。
  - 任何一步因预算停下、扫描没到末尾、本轮开始后又有撤回、或这次登记了新的清理项时，`more` 为真。
  - `more` 为假时，这次调用开始前提交的撤回、替换、删除和中断留下的缓存载荷都已回收，与清除的记录载荷相同。过期只来自撤回：一轮扫到末尾而其间没有新的撤回，就检查过本轮开始时已过期的每一行；本轮中保存的行在保存时未过期，本轮中被替换或删除的行由保存或删除自己登记清理项。
  - 一轮扫完、`last_withdrawal` 已更大时重开存储，下一次调用仍会开始新一轮，因为 epoch 一直留在 `cache_sweep` 行里，直到下一轮开始。
- Performance: 每次调用最多删 `max_chunks` 个块；核对清理项时每项读一行缓存，最多 `max_uploads` 项；扫描最多检查 4,096 个缓存行，上一轮扫完之后没有新的撤回时不扫描。

## Data

- Stored:
  - `session_source_scopes_v1`（Inline）：键 `session_key(owner, session) ‖ segment(encode(scope)) ‖ position_be`，值为空。一个（会话, scope）的行在首次出现的那个批次里写入，可能不止一行，键序第一行是首次出现的位置。唯一写入方 `Coverage::record_scopes`。
  - `session_withdrawals_v1`（Inline）：键 `session_key(owner, holder) ‖ epoch_be`，值是不可读的起始位置（u64 BE）；另有每个 owner 一行，键 `owner_key(owner)`，值是最近一次撤回的 epoch（u64 BE）。唯一写入方 `Coverage::record_withdrawal`。
  - `session_caches_v1`（Inline）：键 `session_key(owner, session) ‖ derivation 的 UTF-8 字节`，值 `CacheRow`，见上。唯一写入方 `session::caches`。
  - `session_cache_chunks_v1`（LargeValues）：键 `owner_key(owner) ‖ version ‖ chunk_be`，值 `digest ‖ bytes`。写入方 `session::caches` 经 `records::chunks`。
  - `session_cache_pending_v1`（Inline）：`PendingCache`，见上。唯一写入方 `session::caches`。
  - `session_cache_sweep_v1`（Inline）：键 `owner_key(owner)`，值 `CacheSweep { cursor: Option<Vec<u8>>, epoch: u64 }`。`cursor` 是本轮上次检查到的 `session_caches_v1` 键，没有进行中的一轮时为空；`epoch` 是最近一轮开始时的 `Coverage::last_withdrawal`，一轮扫完后仍保留，直到下一轮开始。没有这一行等同 `cursor` 为空、`epoch` 为 0。唯一写入方 `session::caches`。
- Existing records: reset（status `startup`）。旧存储没有来源 scope 和撤回索引，缓存的读取权限和过期都会算错，所以 `STORE_FORMAT_ID` 从 `mmdb-store-v5` 改为 `mmdb-store-v6`，旧目录打开时被拒绝，需要整库重置，不做迁移。

## Dependencies

| Dependency | Category | Production adapter | Test adapter |
| --- | --- | --- | --- |
| Fjall，经 `mmdb-kernel` | local-substitutable | 存储目录 | `tempfile` 临时目录里的真实存储；崩溃用 `drop_next_commit` 丢弃发布提交 |

没有新 seam。

## Steps

1. 把分块载荷的写、读、删抽成 `records::chunks`，记录载荷改为经它读写，磁盘格式不变。After: none. Check: `cargo test --workspace --locked`
2. `commit_state_change` 把这次状态变更推进到的可用性 epoch 交给索引闭包，`state_change` 一并返回它。After: none. Check: `cargo test --workspace --locked`
3. 新增 `session::coverage` 及来源 scope 与撤回两份索引：`put_event` 登记来源 scope，`withdraw_event` 按消息起点登记撤回并更新本 owner 最近一次撤回的 epoch（`last_withdrawal`），`View::spans` 给出视图各段；`STORE_FORMAT_ID` 改为 `mmdb-store-v6`，同步 `native_memory` 的格式断言、`format_and_store_identity_prevent_accidental_adoption` 的旧格式列表和 README 的格式说明。单元测试覆盖跨 fork 的 `scopes_through`、消息扩散、共享前缀、N 之后的撤回和 `last_withdrawal`。另有一个单元测试：在同一批次里直接调用 `record_scopes`，在位置 p 和 p+1 两次登记同一个新 scope，提交后 `scopes_through(view, p)` 含这个 scope。After: 2. Check: `cargo test --workspace --locked`
4. 新增 `session::caches`：`availability_epoch`，对话缓存的保存、查找、列出、读取和删除，`module_partitions!` 支持声明大值分区，`ContextError` 的两个新变体，`mmdb::session` 导出新类型；新建 `tests/conversation_cache.rs`，覆盖 Behavior map 中除回收以外的各行，以及 `an_interrupted_save_keeps_the_previous_cache`。其中：
   - `saving_refuses_a_derivation_that_raced_a_covered_withdrawal`：读 epoch E，读事件 A，清除 A，再读事件 B，用 E 保存，报 `AvailabilityConflict`。
   - `a_cache_behind_the_log_restores_the_rest_from_the_tail`：另一写者追加事件 11 之后，以位置 10 保存；恢复时从尾部恰好读到 11 一次。
   - `listing_pages_caches_in_derivation_name_order`：派生名 `aa` 与 `z`，每页一份，第一页是 `aa`。
   - `listing_skips_unreadable_caches_without_naming_them`：可读与不可读的缓存相间，每页凑满可读的，`next` 从不是不可读缓存的派生名；每页 1 份，`aa` 可读、之后只有不可读的 `z` 时，这一页是 `aa`，`next` 为空。

   After: 1, 3. Check: `cargo test --workspace --locked`
5. `recover_payloads` 回收被替换、删除、中断和过期的缓存载荷，扫描游标和本轮的撤回 epoch 存进 `session_cache_sweep_v1`；单元测试 `recovery_reclaims_replaced_removed_and_stale_payloads`（其中一项待清理的替换加 4,096 行待扫、只有最后一行过期：预算足够的一次调用清掉替换，并把最后一行置 `reclaimed`；被回收的过期缓存在替换或删除前仍列为过期）、`recovery_rescans_caches_withdrawn_during_a_sweep_after_reopen`（一轮越过缓存 A 之后清除 A 覆盖的事件，在扫到末尾后重开存储，再回收时从头扫描，A 的块删完才返回 `more = false`），以及清理项指向当前版本时报 `Corrupt`。After: 4. Check: `cargo test --workspace --locked`
6. `examples/context_harness.rs` 的第一个 harness 保存对话缓存、再追加事件，重开后的第二个 harness 用缓存加日志尾部还原对话；README 的 Harness Context 一节说明对话缓存；feature doc 的 Status 改为 `shipped`，本文件 `git mv` 到 `docs/spec/archived/<YYYY-MM-DD>-conversation-cache.md`。After: 5. Check: `cargo test --workspace --locked && cargo run --locked -p mmdb --example context_harness`

## Not this

- 不建 fork 的反向索引，也不在撤回或清除的批次里寻找受影响的缓存；过期在读取时判定，回收靠扫描。
- 读取路径不写任何东西，包括不替过期缓存置 `reclaimed`。
- 对话缓存不进 `records` 的记录头、操作回执或可用性检查；`records` 只为它提供分块。
- 不改变日志事件、检查点和记忆的可读性规则。
- 不为同一派生名保留旧版本，也不提供按版本恢复。
