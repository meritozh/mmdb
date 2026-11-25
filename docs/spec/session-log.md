# 连续会话日志与原文搜索：implement spec

Feature: `docs/feature/session-log.md` @ `51e03ccae11f5ad859e9d36b26e0d7bdcd833497`

This spec is open until the feature ships, and `/go-work` carries it out. Do not edit it while working it; only `to-implement-spec` changes it. Before work starts or resumes, check that the hash above still matches `git hash-object docs/feature/session-log.md`; if it does not, the feature changed, so stop and rerun `to-implement-spec`. Each step is one commit whose message ends with the trailer `Spec-Step: session-log#<n>`. The commit that lands the last step sets the feature doc's Status to `shipped` and moves this file to `docs/spec/archived/<YYYY-MM-DD>-session-log.md` with `git mv`, dated that day. Nothing reads an archived spec again.

会话日志已经能追加、翻页、搜索和配对工具结果，这次让它认得轮次和步骤。规则放在一个不做 I/O 的新模块 `session::turns` 里：它判断一条事件在当前轮次里能不能写、写下后欠下或还清了什么，以及没有正常完成的轮次要补哪些事件。`SessionLog::append` 读出会话的轮次状态，问 `turns`，再把事件、轮次号和步骤号、补齐的事件一起写进一个批次。`Store::open` 用同一套补齐，把上一个进程留下的轮次以 Interrupted 结束。

## Behavior map

| Feature doc line | Owner | Test |
| --- | --- | --- |
| Who：轮次和步骤的开始与结束只能由 System 或 Operator 写入 | `session::history`（输入校验，和用户消息的 actor 检查在一起） | `only_system_or_operator_write_turn_and_step_events` |
| Who：开启轮次的 access 必须带 run，轮次记下这个 run | `session::turns` | `a_turn_needs_a_run_and_a_session_holds_one_open_turn` |
| Who：步骤事件、assistant 事件、工具调用和结果只接受这个 run 写入 | `session::turns` | `only_the_opening_run_writes_inside_a_turn` |
| Who：这个 run 启动的工作以这个 run 的身份写入 | `session::turns`（只核对 run，不看 actor 或 handle） | `work_started_by_the_turn_run_writes_under_that_run` |
| Who：别的一方在这个轮次结束后开启自己的轮次 | `session::turns` | `a_foreign_caller_waits_for_the_open_turn` |
| Who：用户消息、列出会话、撤回或清除的权限；access 绑定 session；重置记忆 | `session::history`、`Store`（不变） | `log_permissions_follow_the_actor`、`access_bound_to_a_session_appends_only_to_it`、`memory_reset_needs_the_exact_plan_digest`（已有） |
| Entry：轮次和步骤的开始与结束和其他事件一样用追加写入 | `SessionLog::append` | `turn_and_step_numbers_increase_across_the_session` |
| Does：会话内递增的轮次号和步骤号 | `session::turns`（分配），`session::history`（写进事件头） | `turn_and_step_numbers_increase_across_the_session` |
| Does 追加：同一事件重试取回原序号；同一身份不同内容冲突 | `SessionLog::append`（重试判断先于轮次规则） | `retrying_a_turn_event_after_the_turn_moved_on_returns_its_receipt`、`append_retries_preserve_identity_and_order_even_after_later_events`（已有） |
| Does 轮次与步骤：一个会话最多一个未结束的轮次，一个轮次最多一个未结束的步骤 | `session::turns` | `a_turn_needs_a_run_and_a_session_holds_one_open_turn` |
| Does 轮次与步骤：assistant 事件和关联 assistant 消息的调用只能在步骤里 | `session::turns` | `assistant_events_and_linked_calls_need_an_open_step` |
| Does 轮次与步骤：不关联消息的调用只要求轮次；用户消息和通知在轮次内外都可以写 | `session::turns` | `direct_calls_inside_a_turn_need_no_step`、`turn_and_step_numbers_increase_across_the_session` |
| Does 轮次与步骤：结果与调用在同一步骤，调用不在步骤里时在同一轮次 | `session::turns`（步骤和轮次欠着结果就不能结束，所以结果只能落在调用所在的步骤或轮次） | `a_step_ends_only_when_it_owes_nothing`、`a_late_result_after_cancel_is_refused` |
| Does 消息：消息事件共用消息 ID；结果关联唯一的调用 | `session::history`（不变） | `a_withdrawn_fragment_hides_its_whole_message`、`tool_results_require_their_call_and_follow_its_purge_state`（已有） |
| Does 结束：步骤结束前消息已终止、调用都有结果 | `session::turns` | `a_step_ends_only_when_it_owes_nothing` |
| Does 结束：Completed 不能留下未结束的步骤或没有结果的调用 | `session::turns` | `completed_needs_no_open_step_and_no_unanswered_call` |
| Does 结束：Interrupted 只由重开存储写入，Forked 只由 fork 写入 | `session::turns` | `a_harness_cannot_write_interrupted_or_forked` |
| Does 补齐：Cancelled 或 Failed 时同一批次按顺序补齐，System 以轮次的 run 写入，ID 确定且以 `mmdb:` 开头 | `session::turns`（补哪些），`SessionLog::append`（写入） | `cancelling_a_turn_closes_what_it_owes_in_one_batch`、`failed_turns_close_like_cancelled_ones` |
| Does 补齐：harness 知道确切结果时先自己写 | `session::turns` | `results_written_before_the_end_are_kept` |
| Does 补齐：harness 不能用 `mmdb:` 前缀 | `session::history`（校验输入） | `event_ids_with_the_mmdb_prefix_are_reserved` |
| Does 读取：事件头带轮次号和步骤号 | `LogEvent::turn` | `turn_and_step_numbers_increase_across_the_session` |
| Does 来源、搜索、重置记忆、整库重置 | `session`、`records`、`Store`（不变） | 已有的 `context_contract` 测试 |
| Fails：违反轮次和步骤规则的追加被拒绝，什么也不写，错误指明违反的规则 | `session::turns`（`TurnError`），`SessionLog::append`（拒绝时不提交批次） | 每个轮次测试在拒绝后断言 `position` 不变 |
| Fails：进程在轮次中途退出，下次 `Store::open` 以 Interrupted 结束 | `session::history::end_interrupted_turns`，由 `Store::open` 调用 | `a_turn_left_open_by_an_exited_process_ends_interrupted_on_open` |
| Fails：同一进程放掉句柄再重开，自己的轮次也这样结束 | 同上 | `reopening_in_the_same_process_ends_its_own_open_turn` |
| Fails：旧格式的目录打开时被拒绝 | `Store::open`（`STORE_FORMAT_ID` 升到 `mmdb-store-v3`） | `format_and_store_identity_prevent_accidental_adoption`、`crates/mmdb/tests/fjall_format.rs`（已有） |
| Limits：打开存储时收尾不扫描全部会话 | `end_interrupted_turns`（只读未结束轮次的分区） | `opening_leaves_sessions_without_open_turns_untouched` |
| Keeps：撤回或清除轮次和步骤事件不改变已记下的号，不重新打开或关闭轮次 | `session::history`（撤回不碰轮次状态） | `withdrawn_turn_events_keep_numbers_and_turn_state` |

Replaces: none. Step 3 updates these existing tests so their assistant and tool events are written inside a turn: `a_withdrawn_fragment_hides_its_whole_message`, `live_revision_keeps_checkpoint_derived_originals_and_pairs_the_completed_tool`, `tool_results_require_their_call_and_follow_its_purge_state`, `tool_results_stay_readable_after_their_call_source_is_purged` (all in `crates/mmdb/tests/context_contract.rs`), and `committed_history_survives_a_lost_acknowledgement_without_duplication` (`crates/mmdb/src/context/tests.rs`).

## Modules

### `mmdb::EventKind`, `LogEvent` and the turn types (changed, `crates/mmdb/src/records/types.rs`, re-exported from `lib.rs`)

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TurnEndReason {
    Completed,
    Cancelled,
    Failed,
    /// Written only by `Store::open`, for a turn an exited process left open.
    Interrupted,
    /// Written only by fork.
    Forked,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum EventKind {
    UserMessage,
    AssistantMessage,
    AssistantStarted { message_id: String },
    AssistantFragment { message_id: String, ordinal: u64 },
    AssistantTerminal { message_id: String, status: MessageStatus },
    ToolCall {
        call_id: String,
        tool_name: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        message_id: Option<String>,
    },
    ToolResult { call_id: String, outcome: ToolOutcome },
    Notice,
    /// Opens a turn for the writer's run.
    TurnStarted,
    /// Ends the open turn. Cancelled and Failed first close what it still owes.
    TurnEnded { reason: TurnEndReason },
    /// Opens a step, one model request, in the open turn.
    StepStarted,
    /// Ends the open step.
    StepEnded,
}

/// Where MMDB placed an event. The harness never sets it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct TurnStamp {
    /// Session-wide, from 1, in the order turns started.
    pub number: u64,
    /// Session-wide, from 1, in the order steps started; None outside a step.
    pub step: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LogEvent {
    pub session: String,
    pub event_id: String,
    pub sequence: u64,
    pub kind: EventKind,
    pub media_type: String,
    /// The open turn and step when MMDB wrote the event; None outside a turn.
    pub turn: Option<TurnStamp>,
}

/// The turn or step rule that refused an append. Nothing was written.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TurnError {
    /// `TurnStarted` from an access without a run.
    RunRequired,
    /// The session already has an open turn. A caller from another run waits for it to end.
    TurnOpen,
    /// The turn already has an open step.
    StepOpen,
    /// A step event, a turn end, a tool call or a tool result with no open turn.
    OutsideTurn,
    /// An assistant event, a call linked to an assistant message, or `StepEnded` with no open step.
    OutsideStep,
    /// A step event, assistant event, tool call or result from a run other than the turn's.
    OtherRun,
    /// `StepEnded` while the step has an unterminated message or an unanswered call.
    StepUnfinished,
    /// `Completed` with an open step or an unanswered call.
    TurnUnfinished,
    /// The harness wrote `Interrupted` or `Forked`.
    ReservedReason,
}

pub enum ContextError {
    // existing variants unchanged
    Turn(TurnError),
}

pub const STORE_FORMAT_ID: &str = "mmdb-store-v3";
```

- Invariants: `LogEvent::turn` is `Some` exactly for events written while a turn was open, including `TurnStarted` (its new number, no step) and `TurnEnded` (no step: the step ended first). `StepStarted` and `StepEnded` carry their own step. Every turn that has ended owes nothing: every message in it has a terminal and every call has a result.
- Errors: `ContextError::Turn` for every rule above; `AccessDenied` when a turn or step event comes from an actor other than System or Operator.

### `SessionLog::append` (changed, `crates/mmdb/src/session/history.rs`)

The signature does not change. What each event needs and does:

| Event | Actor | Needs | Run | Then |
| --- | --- | --- | --- | --- |
| `TurnStarted` | System, Operator | no open turn, `access.run` set | — | opens turn `turns + 1` for `access.run` |
| `StepStarted` | System, Operator | open turn, no open step | the turn's | opens step `steps + 1` |
| `StepEnded` | System, Operator | open step that owes nothing | the turn's | ends the step |
| `TurnEnded { Completed }` | System, Operator | open turn, no open step, owes nothing | any | ends the turn |
| `TurnEnded { Cancelled \| Failed }` | System, Operator | open turn | any | writes the closing events, then ends the turn |
| `TurnEnded { Interrupted \| Forked }` | — | refused: `ReservedReason` | — | — |
| `AssistantStarted` | as today | open step | the turn's | owes a terminal |
| `AssistantFragment`, `AssistantMessage` | as today | open step | the turn's | — |
| `AssistantTerminal` | as today | open step | the turn's | settles the terminal |
| `ToolCall { message_id: Some }` | as today | open step | the turn's | owes a result, in the step |
| `ToolCall { message_id: None }` | as today | open turn | the turn's | owes a result, in the open step if there is one |
| `ToolResult` | as today | open turn | the turn's | settles the result |
| `UserMessage`, `Notice` | as today | — | any | stamped when a turn is open |

- Ordering: input validation runs first, as today, including the actor check for turn and step events. The turn rules run only for a new write: after the retry checks (same operation, or same event identity), so a retry returns its original receipt even after the turn has moved on, and after the expected position; before the existing call and message link checks. A late result for a call MMDB already answered is refused by the turn rules when no turn is open (`OutsideTurn`) or another run's turn is open (`OtherRun`), and by the link check when a turn of the same run is open (`IdempotencyConflict`).
- Closing order for Cancelled and Failed, all in the batch of the harness's end event and before it in sequence: an `AssistantTerminal { Interrupted }` for each unterminated message, then a `ToolResult { EffectUnknown }` with an empty payload for each unanswered call, each in identity order, then `StepEnded` if a step is open. The receipt is the harness's end event. Closing events have actor System, the turn's run and the session in their provenance, the session's scope, media type `application/octet-stream`, the end event's `occurred_at_ms`, and IDs `mmdb:turn-{n}:terminal-{h}`, `mmdb:turn-{n}:result-{h}` and `mmdb:turn-{n}:step-end`, where `{h}` is the hex BLAKE3 hash of the message or call ID. A closing result has the call as its one source and copies the call's `source_scopes`, without an availability walk, so a purged call still gets its result.
- Errors: an `event_id` that starts with `mmdb:` is `InvalidInput`. Everything else as above, or as today.
- Performance: one more point read per append (the open turn). Inside an open turn, one prefix scan over what the turn owes, which holds only its in-flight messages and calls. A cancel writes one event per owed item.

Behind the seam: numbering, the open turn, what it owes, the closing events and their identities. The harness never passes a turn or step number and never reads the owed list.

Call site, an agent turn with one model request and one tool call:

```rust
let run = |actor| ContextAccess {
    run: Some("run-1".into()),
    session: Some("chat-1".into()),
    ..ContextAccess::new("owner", actor)
};
let system = store.session_log(run(Actor::System))?;
let assistant = store.session_log(run(Actor::Assistant))?;

system.append(OperationId::new(), event("chat-1", "turn-7", EventKind::TurnStarted), &[][..])?;
system.append(OperationId::new(), event("chat-1", "step-7.1", EventKind::StepStarted), &[][..])?;
assistant.append(OperationId::new(), event("chat-1", "m1.start", EventKind::AssistantStarted { message_id: "m1".into() }), &[][..])?;
assistant.append(OperationId::new(), event("chat-1", "m1.0", EventKind::AssistantFragment { message_id: "m1".into(), ordinal: 0 }), text)?;
assistant.append(OperationId::new(), event("chat-1", "m1.end", EventKind::AssistantTerminal { message_id: "m1".into(), status: MessageStatus::Completed }), &[][..])?;
assistant.append(OperationId::new(), event("chat-1", "c1", EventKind::ToolCall { call_id: "c1".into(), tool_name: "search".into(), message_id: Some("m1".into()) }), arguments)?;
system.append(OperationId::new(), event("chat-1", "c1.result", EventKind::ToolResult { call_id: "c1".into(), outcome: ToolOutcome::Succeeded }), output)?;
system.append(OperationId::new(), event("chat-1", "step-7.1.end", EventKind::StepEnded), &[][..])?;
system.append(OperationId::new(), event("chat-1", "turn-7.end", EventKind::TurnEnded { reason: TurnEndReason::Completed }), &[][..])?;
```

Call site, the user stops the turn while the tool is still running:

```rust
let receipt = system.append(
    OperationId::new(),
    event("chat-1", "turn-7.end", EventKind::TurnEnded { reason: TurnEndReason::Cancelled }),
    serde_json::to_vec(&json!({ "source": "user" }))?.as_slice(),
)?;
// c1 now has an EffectUnknown result written by MMDB; receipt.sequence is the end event.
```

Call site, an API caller that hits someone else's turn:

```rust
match api.append(OperationId::new(), event("chat-1", "api-turn-3", EventKind::TurnStarted), &[][..]) {
    Ok(_) => run_direct_calls(&api)?,
    Err(ContextError::Turn(TurnError::TurnOpen)) => pending_inputs.push(request),
    Err(error) => return Err(error.into()),
}
```

### `session::turns` (new, internal, `crates/mmdb/src/session/turns.rs`)

```rust
//! The turn and step rules of one session. Pure: the session log loads the
//! state, asks what an event does, and writes the answer.

/// A session's open turn. Stored only while the turn is open.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct OpenTurn {
    pub owner: String,
    pub session: String,
    pub number: u64,
    pub run: String,
    pub step: Option<u64>,
}

/// Work the open turn still owes.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(super) enum Owed {
    Terminal { message_id: String, step: u64 },
    Result { call_id: String, step: Option<u64> },
}

/// The last turn and step numbers a session assigned.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct Numbers {
    pub turns: u64,
    pub steps: u64,
}

pub(super) enum Change {
    Nothing,
    Owe(Owed),
    Settle(Owed),
    StartTurn { number: u64, run: String },
    StartStep(u64),
    EndStep,
    /// Write `closing` in order, then the end event; empty for Completed.
    EndTurn { closing: Vec<Closing> },
}

pub(super) struct Admitted {
    pub stamp: Option<TurnStamp>,
    pub change: Change,
}

/// An event MMDB writes itself to close a turn.
pub(super) struct Closing {
    pub event_id: String,
    pub kind: EventKind,
    pub stamp: TurnStamp,
}

/// What appending `kind` from `run` does to the session's turn, or which rule
/// refuses it. `owed` is what `open` still owes, in identity order; empty when
/// no turn is open.
pub(super) fn admit(
    open: Option<&OpenTurn>,
    owed: &[Owed],
    numbers: Numbers,
    kind: &EventKind,
    run: Option<&str>,
) -> Result<Admitted, TurnError>;

/// The closing events of an unfinished turn. For Interrupted and Forked the
/// list ends with MMDB's own `TurnEnded`, ID `mmdb:turn-{n}:end`.
pub(super) fn close(turn: &OpenTurn, owed: &[Owed], reason: TurnEndReason) -> Vec<Closing>;
```

- Invariants: an owed item's step is the open step or `None`, because a step that owes something cannot end. Closing terminals and results carry the open step, if there is one, and are written before `StepEnded`. `admit` never returns `Change::EndTurn` for `Interrupted` or `Forked`. Only `close` builds those.
- Errors: `TurnError` only. The actor is not its concern: `history` checks it before asking.
- Performance: linear in `owed`.

Behind the seam: which event is allowed where, numbering, which run may write, what each event owes or settles, the closing order and identities. `history` does the reads and writes and knows none of these rules.

Call site, inside `append`'s writing branch:

```rust
let state = self.turn_state(&input.session)?; // open turn, owed, numbers
let admitted = turns::admit(
    state.open.as_ref(),
    &state.owed,
    state.numbers,
    &input.kind,
    access.run.as_deref(),
)
.map_err(ContextError::Turn)?;
```

### `session::history` (changed)

```rust
/// One event ready to write: identity, kind, payload, copy sources and stamp resolved.
struct NewEvent {
    session: String,
    event_id: String,
    kind: EventKind,
    media_type: String,
    scope: Scope,
    occurred_at_ms: i64,
    sources: Vec<RecordPin>,
    source_scopes: Vec<Scope>,
    pin: RecordPin,
    payload: PayloadDescriptor,
    turn: Option<TurnStamp>,
}

impl SessionLog<'_> {
    /// Writes the header, order, identity and call, result and message indexes of
    /// one event into `batch`, and advances `head`. Commits nothing.
    fn put_event(
        &self,
        batch: &mut mmdb_kernel::Batch,
        head: &mut SessionHead,
        event: NewEvent,
    ) -> ContextResult<WriteReceipt>;
}

/// Ends every turn an exited process left open as Interrupted, in one batch.
/// Reads only the open-turn partition. Returns how many turns it ended.
pub(crate) fn end_interrupted_turns(records: &Records, parts: &SessionParts) -> ContextResult<usize>;
```

- Ordering: `end_interrupted_turns` runs inside `Store::open`, after the handles are built and before the store is returned. It is not called by `Store::create`.
- Errors: a storage or decode error fails `Store::open` with `StoreError::Context`. Nothing is committed, and the next open tries again.
- Behind the seam: for each open turn, a `SessionLog` with access `{ owner, actor: System, session, run: the turn's run, scopes: [the session's scope] }` writes `turns::close(.., Interrupted)` through `put_event`, with `occurred_at_ms` from `now_ms()`.

Call site:

```rust
pub fn open(root: impl AsRef<Path>) -> StoreResult<Self> {
    let format = StoreFormatDescriptor::new(STORE_FORMAT_ID).map_err(KernelError::Format)?;
    let store = Self::from_kernel(Kernel::open(root.as_ref(), &format, MODULES)?)?;
    session::end_interrupted_turns(&store.records, &store.session)?;
    Ok(store)
}
```

## Data

- Stored: `session_heads_v1` gains the session's `Numbers` next to `sequence` and `scope`. Writer: `put_event`.
- Stored: `session_open_turns_v1`, key `(owner, session)`, value `OpenTurn`, present only while the turn is open. Writer: `append` and `end_interrupted_turns`.
- Stored: `session_owed_v1`, key `(owner, session, 0 = terminal | 1 = result, identity)`, value the owed item's step, present only while it is owed. Writer: `append` and `end_interrupted_turns`.
- Stored: `RecordHeader::event.turn` on every event header, written once with the event.
- Existing records: reset (status `startup`). `STORE_FORMAT_ID` becomes `mmdb-store-v3`, so `Store::open` refuses a `mmdb-store-v2` directory with the existing `FormatMismatch`, like any other format, and the operator runs the existing whole-store reset. No old event is renumbered or wrapped in a turn.

## Dependencies

| Dependency | Category | Production adapter | Test adapter |
| --- | --- | --- | --- |
| Storage engine, through `mmdb-kernel` | local-substitutable | the store directory | a store in a `tempfile` directory |
| Process exit, for the crash test | local-substitutable | the real process | the test binary re-run as a child that exits mid-turn, as `history_crash_child` does |
| Clock (`now_ms`) and BLAKE3 | in-process | — | — |

No new seam.

## Steps

1. Extract writing one event into a batch from `SessionLog::append` into `put_event`, so one batch can hold several events. No behavior change. After: none. Check: `cargo test --workspace --locked`
2. Record turns and steps. Add `TurnStarted`, `TurnEnded`, `StepStarted`, `StepEnded`, `TurnEndReason`, `TurnStamp`, `TurnError` and `ContextError::Turn`. Add `session::turns` with the start and end rules (`RunRequired`, `TurnOpen`, `StepOpen`, `OutsideTurn`, `OutsideStep` for `StepEnded`, `OtherRun` for step events, `TurnUnfinished` for an open step, `ReservedReason`), the System-or-Operator check for turn and step events in input validation, `Numbers` in the session head, `session_open_turns_v1`, and stamps on every event. Reserve the `mmdb:` event-ID prefix. Raise `STORE_FORMAT_ID` from `mmdb-store-v2` to `mmdb-store-v3`, and update the format line in `README.md` and the assertion in `native_memory.rs`. The Fjall 2 fixtures in `crates/mmdb/tests/fjall_format.rs` stay `mmdb-store-v1` and keep being refused. Until step 4, `Cancelled` and `Failed` with an open step are refused as `TurnUnfinished`. Tests in a new `crates/mmdb/tests/session_turns.rs`: `turn_and_step_numbers_increase_across_the_session`, `only_system_or_operator_write_turn_and_step_events`, `a_turn_needs_a_run_and_a_session_holds_one_open_turn`, `a_harness_cannot_write_interrupted_or_forked`, `event_ids_with_the_mmdb_prefix_are_reserved`, `retrying_a_turn_event_after_the_turn_moved_on_returns_its_receipt`, `withdrawn_turn_events_keep_numbers_and_turn_state`. After: 1. Check: `cargo test --workspace --locked`
3. Place assistant events and tool calls in turns. Add `session_owed_v1` and the rules for assistant events, calls and results (`OutsideStep`, `OutsideTurn`, `OtherRun`, owing and settling, `StepUnfinished`, `TurnUnfinished` for an owed item). Until step 4, `Cancelled` and `Failed` that still owe something are refused as `TurnUnfinished` too, so no ended turn owes anything. Update the existing tests listed under Replaces to open a turn, and a step where they write assistant events, with an access that has a run. Tests: `assistant_events_and_linked_calls_need_an_open_step`, `direct_calls_inside_a_turn_need_no_step`, `only_the_opening_run_writes_inside_a_turn`, `work_started_by_the_turn_run_writes_under_that_run`, `a_foreign_caller_waits_for_the_open_turn`, `a_step_ends_only_when_it_owes_nothing`, `completed_needs_no_open_step_and_no_unanswered_call`. After: 2. Check: `cargo test --workspace --locked`
4. Close Cancelled and Failed turns. Add `turns::close` and write its events through `put_event` in the end event's batch, with the IDs, provenance and order above. This replaces step 2's refusal of a cancel with an open step. Tests: `cancelling_a_turn_closes_what_it_owes_in_one_batch` (closing events in order before the end event, System actor and the turn's run, empty payloads, `mmdb:` IDs, no open turn afterwards), `failed_turns_close_like_cancelled_ones`, `results_written_before_the_end_are_kept`, `a_late_result_after_cancel_is_refused`, and a retry case in `retrying_a_turn_event_after_the_turn_moved_on_returns_its_receipt` (a retried cancel writes no second set of closing events). After: 3. Check: `cargo test --workspace --locked`
5. End turns left open at `Store::open` as Interrupted. Add `end_interrupted_turns` and call it from `Store::open`. Add a sentence on turns to `README.md`'s session log paragraph. Set the feature doc to `shipped` and archive this spec. Tests: `a_turn_left_open_by_an_exited_process_ends_interrupted_on_open` (a child run of the test binary exits mid-step, with an unterminated message and an unanswered call), `reopening_in_the_same_process_ends_its_own_open_turn`, `opening_leaves_sessions_without_open_turns_untouched` (two owners with open turns are both ended, and a session with no open turn keeps its position). After: 4. Check: `cargo test --workspace --locked`

## Not this

- Fork. `turns::close` accepts `Forked` so fork can reuse it, but nothing writes `Forked` until fork exists (feature 3 of `docs/polaris/storage-kernel.md`).
- A read interface by turn or step, or a query for the open turn. Readers group pages by `LogEvent::turn`. A harness learns about an open turn from `TurnError::TurnOpen`.
- Native memory checking commitment evidence against the turn end in the log. `EvidenceLifecycle::TerminalTurn` still takes the status the caller reports.
- Run ancestry. MMDB compares the writer's run with the turn's run only.
- Renaming `session/history.rs` or its internal digest domains.
- Changes to harnesses outside this repo, such as MiuMiu. They adapt when they take this version.
