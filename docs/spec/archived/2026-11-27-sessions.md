# 会话头、会话列表与 fork: implement spec

Feature: `docs/feature/sessions.md` @ `1961656feddaeeb82d20e88010280bf3e249cf99`

This spec is open until the feature ships, and `/go-work` carries it out. Do not edit it while working it; only `to-implement-spec` changes it. Before work starts or resumes, check that the hash above still matches `git hash-object docs/feature/sessions.md`; if it does not, the feature changed, so stop and rerun `to-implement-spec`. Each step is one commit whose message ends with the trailer `Spec-Step: sessions#<n>`. The commit that lands the last step sets the feature doc's Status to `shipped` and moves this file to `docs/spec/archived/<YYYY-MM-DD>-sessions.md` with `git mv`, dated that day. Nothing reads an archived spec again.

会话头是会话模块里的一条 `RecordKind::Session` 记录，`SessionHead` 行指向它并带上会话变更号，变更号索引给出从新到旧的列表。fork 出的会话不复制事件：一个新的 `View` 把会话日志看成跨祖先会话的一条位置序列，所有身份查找都经它按位置截止；截断的轮次由一个纯函数从轮次开始重放到 P，再走与取消相同的补齐写入 Forked。

## Behavior map

| Feature doc line | Owner | Test |
| --- | --- | --- |
| Who: 创建、fork、修改会话头和列出会话需要 System 或 Operator | `session::catalog`, `session::fork` | `sessions::only_system_or_operator_manage_sessions` |
| Who: access 绑定 session 时只能创建或 fork 出这个会话 | `session::catalog`, `session::fork` | `sessions::a_bound_access_creates_or_forks_only_its_session` |
| Who: 子会话需要父会话的 scope，fork 需要来源会话的 scope | `session::catalog`, `session::fork` | `sessions::parent_and_source_scopes_must_be_readable` |
| Entry: 会话必须先创建或 fork 出来，才能追加事件 | `session::history` (`append`) | `sessions::appending_to_a_session_without_a_header_is_refused` |
| Does: 会话头记下固定字段，类型和元数据由 harness 定义，MMDB 不解释 | `session::catalog` | `sessions::a_created_session_keeps_its_header_fields` |
| Does 创建: 日志从序号 1 开始；父会话同 owner 且可访问；子会话 scope 可不同；父 run 照原样记下 | `session::catalog` | `sessions::a_child_session_may_use_another_scope`, `sessions::a_created_session_keeps_its_header_fields` |
| Does 读取会话头: `session` 返回当前修订与维护的计数 | `session::catalog` | `sessions::session_state_reports_position_turns_open_run_and_change` |
| Does 列出: 按会话变更号从新到旧、游标翻页、按类型或父会话过滤、按读者 scope 过滤 | `session::catalog` | `sessions::sessions_list_newest_change_first_with_filters` |
| Does 列出: 对会话的任何写入都推进它的会话变更号 | `session::catalog` (`Changes::put_head`) | `sessions::every_write_advances_the_session_change_number` |
| Does 列出: 增量同步读到上次见过的变更号即停 | `session::catalog` | `sessions::incremental_sync_stops_at_the_last_seen_change` |
| Does 修改: 凭预期修订替换元数据，其他字段不能改 | `session::catalog` | `sessions::update_replaces_metadata_by_expected_revision` |
| Does fork: 1..P 读到来源原事件（pin 相同），新事件从 P+1 编号，轮次号和步骤号接着编 | `session::fork`, `session::view` | `sessions::a_fork_reads_the_source_prefix_then_its_own_events` |
| Does fork: 前缀中已有的事件 ID 在新会话里已被占用 | `session::view`, `session::history` | `sessions::prefix_event_identities_are_taken_in_the_fork` |
| Does fork: 身份查找只看 P 及以前，来源在 P 之后的写入不可见 | `session::view` | `sessions::a_fork_never_sees_source_writes_after_the_cut` |
| Does 截断的轮次: 同一批次补齐并以 Forked 结束；来源不受影响，变更号不推进 | `session::fork`, `session::turns` | `sessions::a_fork_closes_the_cut_turn_while_the_source_continues`, `turns::tests::replay_matches_live_turn_state` |
| Does: fork 出的会话也可以是子会话 | `session::fork` | `sessions::a_fork_can_be_a_child_session` |
| Does: 子会话和 fork 出的会话照常写入，重开时照常收尾 | `session::history` | `sessions::forks_and_children_append_and_close_on_reopen` |
| Fails: 违规调用被拒绝且不写入 | `session::catalog`, `session::fork` | `sessions::refused_creates_and_forks_write_nothing` |
| Fails: 同一操作同内容重试返回原回执；同名不同内容冲突 | `session::catalog`, `session::fork` | `sessions::create_and_fork_retries_return_the_original_receipt` |
| Fails: fork 整体提交，中途退出则没有发生 | `session::fork` | `session::fork::tests::a_lost_fork_commit_leaves_no_session` |
| Fails: 追加前缀中已有的事件 ID，除所指的会话外同内容取回原回执，否则冲突 | `session::history` | `sessions::prefix_event_identities_are_taken_in_the_fork` |
| Fails: `update_session` 预期修订不符时不改变 | `session::catalog` | `sessions::update_replaces_metadata_by_expected_revision` |
| Fails: 前缀中已撤回或清除的事件在 fork 里保持状态 | `session::view` | `sessions::withdrawn_prefix_events_keep_their_state_in_a_fork` |
| Fails: 之前格式的存储打开时被拒绝 | `store` (`STORE_FORMAT_ID`) | `context_contract::format_and_store_identity_prevent_accidental_adoption` |
| Limits: 元数据 ≤ 4 KiB；名称 1–256 字节 | `session::catalog` | `sessions::refused_creates_and_forks_write_nothing` |
| Limits: 每页 1–100 个会话，最多扫描 4,096 条索引，过滤时可空页带游标 | `session::catalog` | `sessions::filtered_pages_stop_after_4096_index_rows` |
| Limits: fork 链最多 64 个会话；落在继承前缀里不增加层数 | `session::view` (`fork_ancestors`) | `sessions::fork_chains_stop_at_64_sessions` |
| Limits: fork 开销取决于截断轮次在 P 之前的事件数 | `session::fork` | `session::fork::tests::forking_reads_only_the_cut_turn` |
| Keeps: 会话头保留到整库重置，重置记忆不影响 | `session::catalog` | `sessions::session_headers_survive_memory_reset_and_reopen` |
| Keeps: 只保留当前修订，旧修订的 pin 读作已被取代 | `session::catalog`, `records` (`header_locked`) | `sessions::older_header_pins_read_as_superseded` |
| Keeps: 会话变更号单调递增，不复用 | `session::catalog` | `sessions::every_write_advances_the_session_change_number` |
| Keeps: 清除前缀中的一条事件后，所有共享它的会话都读不到 | `session::view`, `LogGate` | `sessions::purging_a_prefix_event_hides_it_in_every_fork` |
| Not this: 删除会话或会话头 | `session` (`change_state`) | `sessions::session_headers_cannot_be_retracted_or_purged` |
| Not this: fork 时复制检查点 | `session::fork` | `sessions::a_fork_starts_without_checkpoints` |
| Not this: 会话头作为来源 | `records` (`validate_sources`, `validate_copy_sources`) | `sessions::session_headers_are_not_sources` |
| Not this: 修改 scope、owner、类型、父会话或 fork 来源 | interface: `update_session` takes only metadata | none |

Replaces: the catalog half of `context_contract::copied_history_tracks_source_availability_and_session_catalog_pages` (paging `sessions(2, after)` by name), which `sessions_list_newest_change_first_with_filters` covers; the test keeps its copied-source half.

## Modules

### `records` (changed)

```rust
pub enum RecordKind {
    Type, Object, Relation, Event, Checkpoint, Action, ActionExecution,
    /// A session header. Lives in the session module.
    Session,
}

/// The session section of a session header record.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionHeader {
    pub session: String,
    /// Harness-defined name; MMDB never interprets it.
    pub kind: String,
    pub parent: Option<SessionParent>,
    pub origin: Option<SessionOrigin>,
    pub created_at_ms: i64,
    /// Harness-defined JSON, at most 4 KiB encoded; MMDB never interprets it.
    pub metadata: serde_json::Value,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionParent { pub session: String, pub run: String }

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionOrigin { pub session: String, pub position: u64 }

pub struct RecordHeader {
    // ...existing fields...
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session: Option<SessionHeader>,
}

pub enum ContextError {
    // ...existing variants...
    /// The session has no header in this owner.
    UnknownSession(String),
    /// A fork would make the log span more than 64 sessions.
    ForkTooDeep,
}
```

- Invariants: `RecordKind::Session.module()` is `StoreModule::Session`. A header record has `session: Some(_)`, empty sources, and the session's scope as its record scope. Only its current revision row exists.
- Errors: `header_locked` for a pin whose revision row is gone while the head revision is higher returns `RevisionConflict { expected: pin.revision, actual: head }`. `validate_sources` refuses a `RecordKind::Session` pin with `InvalidInput`, beside its type-definition check. `validate_copy_sources` gets its own `RecordKind::Session` check and keeps accepting type definitions, which original snapshots copy. `session_headers_are_not_sources` drives both paths through public writers: an object, a relation and a checkpoint citing a header pin, and an append whose sources include one.

### `session::catalog` (new)

```rust
/// Fields a harness chooses when it creates or forks a session.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NewSession {
    pub session: String,
    pub kind: String,
    pub parent: Option<SessionParent>,
    pub metadata: serde_json::Value,
}

/// A session header with the counters MMDB maintains for it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionState {
    /// Current header revision; pass it to `update_session`.
    pub pin: RecordPin,
    pub scope: Scope,
    pub header: SessionHeader,
    pub position: u64,
    pub turns: u64,
    pub open_run: Option<String>,
    pub changed_at_ms: i64,
    pub change: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionQuery {
    pub kind: Option<String>,
    /// Only child sessions of this session.
    pub parent: Option<String>,
    /// Continue below this change number; `None` starts at the newest.
    pub before: Option<u64>,
    /// 1..=100
    pub limit: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionPage {
    pub sessions: Vec<SessionState>,
    /// Pass as `before`; `None` when no older session remains.
    pub next: Option<u64>,
}

impl SessionLog<'_> {
    pub fn create_session(&self, operation: OperationId, session: NewSession, scope: Scope)
        -> ContextResult<WriteReceipt>;
    pub fn update_session(&self, operation: OperationId, expected: RecordPin, metadata: serde_json::Value)
        -> ContextResult<WriteReceipt>;
    pub fn session(&self, session: &str) -> ContextResult<SessionState>;
    pub fn sessions(&self, query: SessionQuery) -> ContextResult<SessionPage>;
}
```

- Invariants: every write to `session_heads_v1` goes through `Changes::put_head`, which gives the session the owner's next change number, moves its `session_changes_v1` row and sets `changed_at_ms`. A change number is greater than every number the owner has used, so none is reused. `SessionHead.scope`, which equals the header record's scope, and `SessionHead.created` are written once, in the creating batch.
- Ordering: `create_session` and `fork_session` write the header record, `SessionHead` and change row in one batch before any event of the session exists. `append`, the turn closings at `Store::open`, and `retract`/`purge` of an event put the head of the event's own session in the batch that writes the event or state.
- Errors: `AccessDenied` when the actor is not System or Operator, a bound `access.session` differs from `NewSession.session`, or the reader lacks the session's, the parent's or the source's scope. `UnknownSession` for a missing parent, `session()` of a missing session, and `append` to a session without a header. `InvalidInput` for a name outside 1..=256 bytes or with control characters, metadata over 4 KiB encoded, a limit outside 1..=100, and `retract`/`purge` of a session header. `IdempotencyConflict` for an existing session whose creation content differs, or a reused operation. `RevisionConflict` for a stale `expected` in `update_session`.
- Configuration: none.
- Performance: `session` is a few point reads. `sessions` examines at most 4,096 change rows a page and reads the header and head of each match. Every write adds one reverse seek, one delete and one insert in `session_changes_v1`.

Behind the seam: the header record, `SessionHead` and the change index stay private. Creation identity is a digest of `(NewSession, scope, origin)` kept in `SessionHead.creation`. `SessionHead.created` is the session's position after its creating batch: 0 for a create, P plus the closing events for a fork. A create or fork with the same session name and the same digest returns the creation receipt `{ pin: revision 1, sequence: Some(created) }`, even under another operation and after later appends. `update_session` writes revision n+1, removes revision n's header row and returns `{ pin: n+1, sequence: position }`. `Changes` caches the next number per owner for one batch, so `Store::open` can close turns of many sessions in one commit. A change row holds `{ session, kind, parent session }` so filters do not read headers.

Call site:

```rust
let system = store.session_log(ContextAccess {
    run: Some("run-1".into()),
    ..ContextAccess::new("owner", Actor::System)
})?;
system.create_session(OperationId::new(), NewSession {
    session: "chat-7".into(),
    kind: "chat".into(),
    parent: None,
    metadata: json!({ "title": "Trip plan" }),
}, Scope::Personal)?;
system.append(OperationId::new(), EventInput { session: "chat-7".into(), ..input }, &b"hi"[..])?;

let mut before = None;
'sync: loop {
    let page = system.sessions(SessionQuery { kind: Some("chat".into()), parent: None, before, limit: 50 })?;
    for state in &page.sessions {
        if state.change <= last_seen { break 'sync; }
        render(state);
    }
    match page.next { Some(next) => before = Some(next), None => break }
}

let state = system.session("chat-7")?;
system.update_session(OperationId::new(), state.pin, json!({ "title": "Kyoto" }))?;
```

### `session::fork` (new)

```rust
impl SessionLog<'_> {
    /// Creates `session` whose log starts with the first `origin.position`
    /// events of `origin.session`, in the source's scope.
    pub fn fork_session(&self, operation: OperationId, session: NewSession, origin: SessionOrigin)
        -> ContextResult<WriteReceipt>;
}
```

- Invariants: the fork's scope is the source's scope. The source session gets no write and keeps its change number. After commit the fork has no open turn: a turn open at P is ended with Forked in the fork, after its owed items are closed as on cancel.
- Ordering: the header record, `SessionHead` (sequence P, the turn and step counters at P), the lineage row, the overlay message rows and the closing events commit in one batch with the operation receipt.
- Errors: as `create_session`, plus `UnknownSession` for a missing source, `InvalidInput` for position 0, `PositionConflict { expected: P, actual: source position }` for P past the source's position, and `ForkTooDeep` past 64 sessions.
- Performance: reads the order entry at P; when a turn is open at P, reads the order entries and headers from that turn's `TurnStarted` to P. Never reads the rest of the prefix.

Behind the seam: the turn open at P is found from the order entry at P (`open_turn`), the turn is rebuilt by `turns::replay` from its events, and `turns::close` gives the closing events. Messages that are unterminated at P get an overlay row in the fork (`next_fragment` as of P, terminal Interrupted, `started` unchanged); results for owed calls take the call as their source through `View::call`. The receipt is `{ pin: header revision 1, sequence: Some(created) }`, where `created` is the position after the closings.

Call site:

```rust
// Delegate a subagent that sees the parent's history.
let position = system.position("chat-7")?;
let receipt = system.fork_session(OperationId::new(), NewSession {
    session: format!("sub-{child_run}"),
    kind: "subagent".into(),
    parent: Some(SessionParent { session: "chat-7".into(), run: "run-1".into() }),
    metadata: json!({}),
}, SessionOrigin { session: "chat-7".into(), position })?;
// The child's own turn starts after receipt.sequence.
```

### `session::history` (changed)

`append`, `retract` and `purge` keep their signatures.

- Errors: `UnknownSession` when the session has no `SessionHead`. When `View::event` finds the event ID in an ancestor segment, the retry is compared as if addressed to `Found.session`. The event digest is computed over the input with `session` set to `Found.session`, and with the provenance session set to it as well when the access binds the fork. Every other field must match: kind, payload, sources, scope, `occurred_at_ms`, actor, agent and run. A match returns the stored receipt; anything else is `IdempotencyConflict`.

Behind the seam: only the content comparison is rebased. The operation receipt stays bound to the digest of the request as sent, naming the fork, so a reuse of that operation with other content still conflicts.

### `session::view` (new, crate-internal)

```rust
/// One session's log as a single sequence of positions across the sessions it was forked from.
pub(super) struct View<'a> { /* parts, owner, session, ancestors: Vec<Segment> */ }

#[derive(Serialize, Deserialize)]
pub(super) struct Segment { pub session: String, pub upto: u64 }

pub(super) struct Found<T> { pub session: String, pub row: T }

impl<'a> View<'a> {
    pub(super) fn load(parts: &'a SessionParts, owner: &str, session: &str) -> ContextResult<Self>;
    pub(super) fn entry(&self, position: u64) -> ContextResult<Option<OrderEntry>>;
    pub(super) fn entries_after(&self, after: u64)
        -> impl Iterator<Item = ContextResult<(u64, OrderEntry)>> + '_;
    pub(super) fn event(&self, event_id: &str) -> ContextResult<Option<Found<StoredEvent>>>;
    pub(super) fn call(&self, call_id: &str) -> ContextResult<Option<Found<ToolCallIndex>>>;
    pub(super) fn result(&self, call_id: &str) -> ContextResult<Option<Found<ResultIndex>>>;
    /// `unavailable` is read from the row where the message started.
    pub(super) fn message(&self, message_id: &str) -> ContextResult<Option<Found<MessageState>>>;
    /// The session whose own rows hold this position.
    pub(super) fn holder(&self, position: u64) -> &str;
    /// Ancestors for a fork of this log at `position`; ForkTooDeep past 64 sessions.
    pub(super) fn fork_ancestors(&self, position: u64) -> ContextResult<Vec<Segment>>;
}
```

- Invariants: positions `1..=ancestors[0].upto` are rows of `ancestors[0].session`, each later segment covers `(previous upto, upto]`, and the session's own rows cover the positions after the last `upto`. An identity lookup takes the session's own row first, then each ancestor from newest to oldest, accepting a row only when the position it records is at or before that ancestor's `upto`. A session without a lineage row is a single segment.
- Errors: `Corrupt` in two cases:
  - A lineage row is empty, holds more than 63 segments, has an `upto` of 0 or one that does not strictly increase, or names the session itself.
  - An order entry's record is not an `Event` of the view's owner.

  Each check reads only the row itself and its key. Swapping two well-formed order rows of one session is not detected, because a row holds neither its session nor its position.
- Performance: `load` is one point read. A lookup costs at most one point read per segment. `entry` is a binary search over segments plus one read.

Behind the seam: `fork_ancestors` flattens. When `position` falls in an inherited segment, the new list ends at that ancestor with `upto = position`, so depth does not grow; otherwise it appends `{ self.session, position }`. `history`, `checkpoints`, `search`, `fork` and `LogGate` never read `order`, `events`, `calls`, `results` or `messages` except through a `View`.

### `session::turns` (changed)

```rust
/// The session that holds a turn being started or rebuilt.
#[derive(Clone, Copy)]
pub(super) struct TurnHolder<'a> { pub owner: &'a str, pub session: &'a str }

/// The state change `admit` decided, applied in memory. Owed rows are the caller's.
pub(super) fn apply(state: TurnState, change: &Change, holder: TurnHolder<'_>, event_id: &str, position: u64)
    -> ContextResult<TurnState>;

pub(super) struct Replayed<'a> { pub position: u64, pub event_id: &'a str, pub kind: &'a EventKind, pub stamp: Option<TurnStamp>, pub run: Option<&'a str> }

pub(super) struct Replay {
    pub turn: OpenTurn,
    /// Identity order, terminals first, as `close` expects.
    pub owed: Vec<Owed>,
    /// Messages started in the turn and unterminated at the last event.
    pub open_messages: BTreeMap<String, MessageState>,
}

/// Rebuilds, as held by `holder`, the turn open after the last event from events that start at its TurnStarted.
pub(super) fn replay<'a>(holder: TurnHolder<'_>, events: impl IntoIterator<Item = Replayed<'a>>)
    -> ContextResult<Replay>;
```

- Invariants: for any admitted sequence of events, `replay` with the source as holder, of the turn's events up to position k, equals the `OpenTurn` and owed rows the live log held after position k. `OpenTurn` records `start: u64`, the position of its `TurnStarted`. A live append passes the appending session as holder; `fork_session` passes the fork, so the turn it closes, its closing events and their call lookups all belong to the fork, and the source's open-turn and owed rows are never written.
- Errors: `Corrupt` when the events do not start with `TurnStarted` or contradict admission.
- Performance: linear in the events given; no I/O.

Behind the seam: `put_turn_change` uses `turns::apply` instead of building turns inline, and `put_turn_end` takes the owed items as a parameter, so live closing, reopen closing and fork closing share one path.

## Data

- Stored:
  - Session header record: `RecordKind::Session` in the session module's record table, `RecordHeader { session: Some(SessionHeader), scope, sources: [], .. }`, metadata inline. Only the current revision row is kept. Writer: `session::catalog` and `session::fork`.
  - `session_heads_v1`: `SessionHead { header: ContextRef, scope, creation: [u8; 32], created: u64, sequence, numbers, change, changed_at_ms }`. Writer: `Changes::put_head`.
  - `session_changes_v1` (new): key `(owner, change as big-endian u64)`, value `{ session, kind, parent: Option<String> }`, one row per session. Writer: `Changes::put_head`.
  - `session_lineage_v1` (new): key `(owner, session)`, value `Vec<Segment>`, only for forks, written once. Writer: `session::fork`.
  - `session_order_v1`: value becomes `OrderEntry { record: ContextRef, numbers: Numbers, open_turn: Option<u64> }`, the counters and the open turn's start position after the event. Writer: `history::put_event`.
  - `session_tool_calls_v1`: `ToolCallIndex` gains `sequence`. `session_tool_results_v1`: value becomes `ResultIndex { record, sequence }`. `session_messages_v1`: `MessageState` gains `pub started: u64`; a fork holds overlay rows for messages it closed. Writer: `history` (and `fork` for overlays).
- Existing records: reset (status `startup`). A step that changes what a stored row holds, or must hold, sets a new `STORE_FORMAT_ID` and refuses the earlier ones, so `Store::open` turns away a store made at an earlier step and it needs a whole-store reset:
  - Step 1 sets `mmdb-store-v4`: rows carry positions. It refuses `mmdb-store-v3`.
  - Step 3 sets `mmdb-store-v5`: headers are required, and `SessionHead` gains change numbers and `created`. It refuses `mmdb-store-v4`.
  - Steps 4–7 add no shape that an earlier `mmdb-store-v5` store lacks. The kernel checks only module names on open, so the new `session_lineage_v1` partition needs no new ID, and a store without lineage rows holds no forks.

## Dependencies

| Dependency | Category | Production adapter | Test adapter |
| --- | --- | --- | --- |
| Fjall through `mmdb-kernel` | local-substitutable | `Store` on a directory | `Store::create` in a `tempfile` directory |
| Turn replay | in-process | `turns::replay` | same |

No new seam.

## Steps

1. Record each session log row's position: order rows carry `OrderEntry`, call, result and message rows carry their sequence, `OpenTurn` records its start position, and `View` (one segment) serves every identity read in `history`, `checkpoints`, `search` and `LogGate`. Store format becomes `mmdb-store-v4`: add `mmdb-store-v3` to the refused formats in `context_contract::format_and_store_identity_prevent_accidental_adoption`, and update the constant assertion in `native_memory::tests::fresh_store_gets_a_stable_era_and_foreign_roots_are_refused`. Apart from refusing `mmdb-store-v3` stores, no behavior changes. After: none. Check: `cargo test --workspace --locked`
2. Move turn bookkeeping into `turns::apply` and `turns::replay`, both taking a `TurnHolder`; `put_turn_end` takes the owed items. Add `replay_matches_live_turn_state`, which appends turns with steps, messages, direct calls and results, and compares `replay`, with the session as holder, at every position with the live state. After: 1. Check: `cargo test --workspace --locked`
3. Add session headers and require them: `RecordKind::Session`, `RecordHeader.session`, `create_session`, `session`, `SessionHead` with change numbers, `creation` and `created`, `session_changes_v1` and `Changes::put_head` on every head write, `UnknownSession` for appends without a header. Refuse `retract`/`purge` of headers, and headers as sources in both `validate_sources` and `validate_copy_sources`. Store format becomes `mmdb-store-v5`: add `mmdb-store-v4` to the refused formats and update the constant assertion again. Create sessions in every test, `context/tests.rs`, `src/tests` and example before they append. After: 2. Check: `cargo test --workspace --locked`
4. Replace the session catalog with `sessions(SessionQuery)`: newest change first, `before` cursor, kind and parent filters, reader scope, the 4,096-row scan bound. Move the catalog paging assertions out of `copied_history_tracks_source_availability_and_session_catalog_pages`. After: 3. Check: `cargo test --workspace --locked`
5. Add `update_session`: replace metadata by expected revision, keep only the current header revision, read older pins as `RevisionConflict`. After: 4. Check: `cargo test --workspace --locked`
6. Fork at positions outside a turn: `session_lineage_v1`, multi-segment `View` with `fork_ancestors` and the 64-session bound, `fork_session` validation, receipts and retries, prefix identities taken in the fork with retries compared as addressed to the session that holds the row, availability and withdrawal through the row where a message or call started, `events`, `search`, `tool_pair`, `message_state` and checkpoint anchors over the whole log. A fork at a position inside a turn returns `Turn(TurnOpen)` until step 7. After: 5. Check: `cargo test --workspace --locked`
7. Close the cut turn: a fork at a position inside a turn replays it with the fork as holder, writes overlay message rows and the closing events ending with Forked in its batch, and leaves the source untouched. `a_fork_closes_the_cut_turn_while_the_source_continues` checks that after the fork the source settles its owed items and ends the turn Completed. `create_and_fork_retries_return_the_original_receipt` retries a fork that closed a turn, under a fresh operation, after an append and a reopen. Add `a_fork_never_sees_source_writes_after_the_cut`, `forking_reads_only_the_cut_turn` and `a_lost_fork_commit_leaves_no_session`. Show create, list and fork in `examples/context_harness.rs`. Set `docs/feature/sessions.md` and `docs/feature/session-log.md` to `shipped`, and archive this spec. After: 6. Check: `cargo test --workspace --locked`

## Not this

- MiuMiu's switch from `NS_CHAT_SESSIONS` to MMDB session headers; that is MiuMiu's change.
- A migration from `mmdb-store-v3`, or from the intermediate `mmdb-store-v4`.
- Secondary indexes for kind or parent; filters scan the change index.
