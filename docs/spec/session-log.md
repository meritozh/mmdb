# 连续会话日志与原文搜索：implement spec

Feature: `docs/feature/session-log.md` @ `2ed9ea1b36fce3a9648deb0958f116d2fd45073d`

This spec is open until the feature ships, and `/go-work` carries it out. Do not edit it while working it; only `to-implement-spec` changes it. Before work starts or resumes, check that the hash above still matches `git hash-object docs/feature/session-log.md`; if it does not, the feature changed, so stop and rerun `to-implement-spec`. Each step is one commit whose message ends with the trailer `Spec-Step: session-log#<n>`. The commit that lands the last step sets the feature doc's Status to `shipped` and moves this file to `docs/spec/archived/<YYYY-MM-DD>-session-log.md` with `git mv`, dated that day. Nothing reads an archived spec again.

会话日志已经能追加、翻页、搜索和配对工具结果，这次让它认得轮次和步骤。规则放在一个不做 I/O 的新模块 `session::turns` 里：它判断一条事件在当前轮次里能不能写、写下后欠下或还清了什么，以及没有正常完成的轮次要补哪些事件。`SessionLog::append` 读出会话的轮次状态，问 `turns`，再把事件、轮次号和步骤号、补齐的事件一起写进一个批次。`Store::open` 用同一套补齐，把上一个进程留下的轮次以 Interrupted 结束。

## Behavior map

| Feature doc line | Owner | Test |
| --- | --- | --- |
| Who：轮次和步骤的开始与结束只能由 System 或 Operator 写入 | `session::history`（输入校验，和用户消息的 actor 检查在一起） | `only_system_or_operator_write_turn_and_step_events` |
| Who：开启轮次的 access 必须带 run，轮次记下这个 run | `session::turns` | `a_turn_needs_a_run_and_a_session_holds_one_open_turn` |
| Who：步骤事件、assistant 事件、工具调用、结果和轮次的结束只接受这个 run 写入 | `session::turns` | `only_the_opening_run_writes_inside_a_turn` |
| Who：结束事件以开启事件的 ID 指明它结束的轮次 | `session::turns` | `a_turn_end_names_the_turn_it_ends` |
| Who：这个 run 启动的工作以这个 run 的身份写入 | `session::turns`（只核对 run，不看 actor 或 handle） | `work_started_by_the_turn_run_writes_under_that_run` |
| Who：别的一方在这个轮次结束后开启自己的轮次 | `session::turns` | `a_foreign_caller_waits_for_the_open_turn` |
| Who：用户消息、列出会话、撤回或清除的权限；access 绑定 session；重置记忆 | `session::history`、`Store`（不变） | `log_permissions_follow_the_actor`、`access_bound_to_a_session_appends_only_to_it`、`memory_reset_needs_the_exact_plan_digest`（已有） |
| Entry：轮次和步骤的开始与结束和其他事件一样用追加写入 | `SessionLog::append` | `turn_and_step_numbers_increase_across_the_session` |
| Does：会话内递增的轮次号和步骤号 | `session::turns`（分配），`session::history`（写进事件头） | `turn_and_step_numbers_increase_across_the_session` |
| Does 追加：同一事件重试取回原序号；同一身份不同内容冲突 | `SessionLog::append`（重试判断先于轮次规则） | `retrying_a_turn_event_after_the_turn_moved_on_returns_its_receipt`、`append_retries_preserve_identity_and_order_even_after_later_events`（已有） |
| Does 轮次与步骤：一个会话最多一个未结束的轮次，一个轮次最多一个未结束的步骤 | `session::turns` | `a_turn_needs_a_run_and_a_session_holds_one_open_turn` |
| Does 轮次与步骤：assistant 事件只能在未结束的步骤里；assistant 消息发出的调用只能在这条消息所在、尚未结束的步骤里 | `session::turns`（消息状态记下消息开始时的步骤） | `assistant_events_and_linked_calls_need_an_open_step`、`a_linked_call_stays_in_its_message_step` |
| Does 轮次与步骤：不关联消息的调用只要求轮次；用户消息和通知在轮次内外都可以写 | `session::turns` | `direct_calls_inside_a_turn_need_no_step`、`turn_and_step_numbers_increase_across_the_session` |
| Does 轮次与步骤：结果与调用在同一步骤，调用不在步骤里时在同一轮次 | `session::turns`（步骤和轮次欠着结果就不能结束，所以结果只能落在调用所在的步骤或轮次） | `a_step_ends_only_when_it_owes_nothing`、`a_late_result_after_cancel_is_refused` |
| Does 消息：消息事件共用消息 ID；结果关联唯一的调用 | `session::history`（不变） | `a_withdrawn_fragment_hides_its_whole_message`、`tool_results_require_their_call_and_follow_its_purge_state`（已有） |
| Does 消息：欠着结果的调用即使已被清除或所在的消息已被撤回，结果照样写入，写入后随调用不可读 | `session::history`（关联检查不查欠着的调用是否可读） | `an_owed_call_gets_its_result_after_it_becomes_unreadable` |
| Does 来源：追加时来源必须可读，工具结果对它所还的调用除外 | `Scoped::validate_copy_sources`（`answers` 只计预算，带上调用记下的 scope） | `an_owed_call_gets_its_result_after_it_becomes_unreadable` |
| Does 结束：步骤结束前消息已终止、调用都有结果 | `session::turns` | `a_step_ends_only_when_it_owes_nothing` |
| Does 结束：Completed 不能留下未结束的步骤或没有结果的调用 | `session::turns` | `completed_needs_no_open_step_and_no_unanswered_call` |
| Does 结束：Interrupted 只由重开存储写入，Forked 只由 fork 写入 | `session::turns` | `a_harness_cannot_write_interrupted_or_forked` |
| Does 补齐：Cancelled 或 Failed 时同一批次按顺序补齐，System 以轮次的 run 写入，ID 确定且以 `mmdb:` 开头 | `session::turns`（补哪些），`SessionLog::append`（写入） | `cancelling_a_turn_closes_what_it_owes_in_one_batch`、`a_cancel_with_a_lost_acknowledgement_published_all_its_closing_events`、`failed_turns_close_like_cancelled_ones`、`a_closing_result_fits_the_reserved_budget` |
| Does 补齐：harness 知道确切结果时先自己写 | `session::turns` | `results_written_before_the_end_are_kept` |
| Does 补齐：harness 不能用 `mmdb:` 前缀 | `session::history`（校验输入） | `event_ids_with_the_mmdb_prefix_are_reserved` |
| Does 读取：事件头带轮次号和步骤号 | `LogEvent::turn` | `turn_and_step_numbers_increase_across_the_session` |
| Does 来源（其余）、搜索、重置记忆、整库重置 | `session`、`records`、`Store`（不变） | 已有的 `context_contract` 测试 |
| Fails：违反轮次和步骤规则的追加被拒绝，什么也不写，错误指明违反的规则 | `session::turns`（`TurnError` 和它的优先级），`SessionLog::append`（拒绝时不提交批次） | `turn_errors_follow_their_precedence`；每个轮次测试在拒绝后断言 `position` 不变 |
| Fails：工具结果与它的调用不在同一步骤或同一轮次 | `session::turns`（`Answered`） | `a_second_result_for_a_call_is_answered`、`a_late_result_after_cancel_is_refused` |
| Fails：进程在轮次中途退出，下次 `Store::open` 在同一批次里以 Interrupted 结束 | `session::history::end_interrupted_turns`，由 `Store::open` 调用 | `a_turn_left_open_by_an_exited_process_ends_interrupted_on_open`、`an_unknown_recovery_commit_closes_each_turn_once`、`recovery_reads_no_source_of_an_owed_call` |
| Fails：同一进程放掉句柄再重开，自己的轮次也这样结束 | 同上 | `reopening_in_the_same_process_ends_its_own_open_turn` |
| Fails：旧格式的目录打开时被拒绝 | `Store::open`（`STORE_FORMAT_ID` 升到 `mmdb-store-v3`） | `format_and_store_identity_prevent_accidental_adoption`（旧格式列表加入 `mmdb-store-v1` 和 `mmdb-store-v2`） |
| Limits：读取输入流期间持有存储的写锁 | `SessionLog::append`（`put_event` 在输入流读完之后写批次，期间不放锁） | `a_streaming_append_blocks_memory_writes_until_it_ends`（已有，`src/context/tests.rs`） |
| Limits：工具调用为它的结果预留一层深度和一次访问 | `Scoped::validate_copy_sources`（`reserve`） | `a_tool_call_reserves_room_for_its_result` |
| Limits：打开存储时收尾不扫描全部会话 | `end_interrupted_turns`（只枚举未结束轮次的分区） | `opening_reads_only_sessions_with_open_turns` |
| Keeps：撤回或清除轮次和步骤事件不改变已记下的号，不重新打开或关闭轮次 | `session::history`（撤回不碰轮次状态） | `withdrawn_turn_events_keep_numbers_and_turn_state` |

Replaces: none. Step 4 updates these existing tests so their assistant and tool events are written inside a turn: `a_withdrawn_fragment_hides_its_whole_message`, `live_revision_keeps_checkpoint_derived_originals_and_pairs_the_completed_tool`, `tool_results_require_their_call_and_follow_its_purge_state`, `tool_results_stay_readable_after_their_call_source_is_purged`, `a_tool_call_reserves_room_for_its_result` (all in `crates/mmdb/tests/context_contract.rs`), and `committed_history_survives_a_lost_acknowledgement_without_duplication` (`crates/mmdb/src/context/tests.rs`). It also adds `mmdb-store-v1` and `mmdb-store-v2` to the legacy-format list of `format_and_store_identity_prevent_accidental_adoption`.

## Modules

### `mmdb::EventKind`, `LogEvent`, `MessageState` and the turn types (changed, `crates/mmdb/src/records/types.rs` and `crates/mmdb/src/session/types.rs`, re-exported from `lib.rs`)

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
    /// Opens a turn for the writer's run. Its event ID names the turn.
    TurnStarted,
    /// Ends the open turn whose `TurnStarted` has the event ID `start_event_id`.
    /// Cancelled and Failed first close what it still owes.
    TurnEnded { reason: TurnEndReason, start_event_id: String },
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MessageState {
    pub next_fragment: u64,
    pub terminal: Option<MessageStatus>,
    #[serde(default)]
    pub unavailable: bool,
    /// The step the message's `AssistantStarted` was written in.
    pub step: u64,
}

/// The turn or step rule that refused an append. Nothing was written.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TurnError {
    /// The harness wrote `Interrupted` or `Forked`.
    ReservedReason,
    /// `TurnStarted` from an access without a run.
    RunRequired,
    /// `TurnStarted` while the session has an open turn. A caller from another run waits for it to end.
    TurnOpen,
    /// A tool result for a call that already has one, written by the harness or by MMDB's closing.
    Answered,
    /// No turn is open, for an event that needs one: a step event, an assistant event, a tool call or a turn end.
    OutsideTurn,
    /// A step event, assistant event, tool call, tool result or turn end from a run other than the turn's.
    OtherRun,
    /// A turn end whose `start_event_id` is not the open turn's.
    OtherTurn,
    /// `StepStarted` while the turn has an open step.
    StepOpen,
    /// A turn is open, but an assistant event, `StepEnded` or a linked call has no open step,
    /// or the linked call's message started in another step.
    OutsideStep,
    /// `StepEnded` while the step has an unterminated message or an unanswered call.
    StepUnfinished,
    /// `Completed` with an open step or an unanswered call.
    TurnUnfinished,
}

pub enum ContextError {
    // existing variants unchanged
    Turn(TurnError),
}

pub const STORE_FORMAT_ID: &str = "mmdb-store-v3";
```

- Invariants: `LogEvent::turn` is `Some` exactly for events written while a turn was open, including `TurnStarted` (its new number, no step) and `TurnEnded` (no step: the step ended first). `StepStarted` and `StepEnded` carry their own step. Every call without a result is owed by its session's open turn, so every turn that has ended owes nothing: every message in it has a terminal and every call has a result.
- Errors: `ContextError::Turn` for every rule above; `AccessDenied` when a turn or step event comes from an actor other than System or Operator. When an event breaks several rules, the first in the order of the variants above wins: a linked call or an `AssistantMessage` in an empty session is `OutsideTurn`, and another run's assistant event in a turn with no open step is `OtherRun`.

### `SessionLog::append` (changed, `crates/mmdb/src/session/history.rs`)

The signature does not change. What each event needs and does:

| Event | Actor | Needs | Run | Then |
| --- | --- | --- | --- | --- |
| `TurnStarted` | System, Operator | no open turn, `access.run` set | — | opens turn `turns + 1` for `access.run`, named by its event ID |
| `StepStarted` | System, Operator | open turn, no open step | the turn's | opens step `steps + 1` |
| `StepEnded` | System, Operator | open step that owes nothing | the turn's | ends the step |
| `TurnEnded { Completed, .. }` | System, Operator | open turn named by `start_event_id`, no open step, owes nothing | the turn's | ends the turn |
| `TurnEnded { Cancelled \| Failed, .. }` | System, Operator | open turn named by `start_event_id` | the turn's | writes the closing events, then ends the turn |
| `TurnEnded { Interrupted \| Forked, .. }` | — | refused: `ReservedReason` | — | — |
| `AssistantStarted` | as today | open step | the turn's | owes a terminal; the message records the step |
| `AssistantFragment`, `AssistantMessage` | as today | open step | the turn's | — |
| `AssistantTerminal` | as today | open step | the turn's | settles the terminal |
| `ToolCall { message_id: Some }` | as today | open step, which is the step the message started in | the turn's | owes a result, in the step |
| `ToolCall { message_id: None }` | as today | open turn | the turn's | owes a result, in the open step if there is one |
| `ToolResult` | as today | the open turn owes the call | the turn's | settles the result |
| `UserMessage`, `Notice` | as today | — | any | stamped when a turn is open |

- Ordering, for a new write:
  1. Input validation, as today, plus the actor check for turn and step events, the `mmdb:` prefix, and `start_event_id` as a name.
  2. The retry checks (same operation, or same event identity), so a retry returns its original receipt even after the turn has moved on.
  3. The expected position.
  4. The link lookups, as today except for results: a linked call's message must exist and be available, and its call ID must be new; a result's call must exist (`InvalidInput`). The result's old availability check and duplicate check (`IdempotencyConflict`) go away: `Answered` replaces the duplicate check, and a call the turn still owes needs no availability check.
  5. The turn rules (`turns::admit`), given the linked message's step.
  6. The copy sources (`validate_copy_sources`): a tool call reserves room for its result; a result passes its call as `answers`.

  A fresh result for a call that already has one is therefore `Answered`, whatever turn is open and whether or not the call is still readable.
- Closing order for Cancelled and Failed, all in the batch of the harness's end event and before it in sequence:
  1. An `AssistantTerminal { Interrupted }` for each unterminated message, in identity order.
  2. A `ToolResult { EffectUnknown }` with an empty payload for each unanswered call, in identity order.
  3. `StepEnded`, if a step is open.

  The receipt is the harness's end event. Closing events have actor System, the turn's run and the session in their provenance, the session's scope, media type `application/octet-stream`, the end event's `occurred_at_ms`, and IDs `mmdb:turn-{n}:terminal-{h}`, `mmdb:turn-{n}:result-{h}` and `mmdb:turn-{n}:step-end`, where `{h}` is the hex BLAKE3 hash of the message or call ID. A closing result has the call as its one source and copies the call header's `source_scopes`. It walks nothing: the call reserved its result's budget when it was written, and the call's header is read under the session's scope, which is the call's own.
- Errors: an `event_id` that starts with `mmdb:` is `InvalidInput`. Everything else as above, or as today.
- Performance: one more point read per append (the open turn). Inside an open turn, one prefix scan over what the turn owes, which holds only its in-flight messages and calls. A cancel writes one event per owed item and reads each owed call's header.

Behind the seam: numbering, the open turn, what it owes, the closing events and their identities. The harness never passes a turn or step number and never reads the owed list; it names a turn only by the event ID it gave its `TurnStarted`.

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
system.append(
    OperationId::new(),
    event("chat-1", "turn-7.end", EventKind::TurnEnded { reason: TurnEndReason::Completed, start_event_id: "turn-7".into() }),
    &[][..],
)?;
```

Call site, the user stops the turn while the tool is still running:

```rust
let receipt = system.append(
    OperationId::new(),
    event("chat-1", "turn-7.end", EventKind::TurnEnded { reason: TurnEndReason::Cancelled, start_event_id: "turn-7".into() }),
    serde_json::to_vec(&json!({ "source": "user" }))?.as_slice(),
)?;
// c1 now has an EffectUnknown result written by MMDB; receipt.sequence is the end event.
// The tool's own result, if it arrives later, is refused as TurnError::Answered.
```

Call site, an API caller that hits someone else's turn:

```rust
match api.append(OperationId::new(), event("chat-1", "api-turn-3", EventKind::TurnStarted), &[][..]) {
    Ok(_) => run_direct_calls(&api, "api-turn-3")?,
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
    /// The event ID of the turn's `TurnStarted`.
    pub start_event_id: String,
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

/// A session's turn state as stored.
pub(super) struct TurnState {
    pub open: Option<OpenTurn>,
    /// What `open` still owes, in identity order (terminals, then results); empty when no turn is open.
    pub owed: Vec<Owed>,
    pub numbers: Numbers,
}

/// What the turn rules need to know about one new event.
pub(super) struct Write<'a> {
    pub kind: &'a EventKind,
    pub run: Option<&'a str>,
    /// For a `ToolCall` linked to an assistant message: the step the message started in.
    pub message_step: Option<u64>,
}

pub(super) enum Change {
    Nothing,
    Owe(Owed),
    Settle(Owed),
    /// The caller stores an `OpenTurn` with this number, the run and the event's ID.
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

/// What appending one event does to the session's turn, or the first rule, in
/// `TurnError`'s order, that refuses it. A `ToolResult`'s call is known to exist.
pub(super) fn admit(state: &TurnState, write: Write<'_>) -> Result<Admitted, TurnError>;

/// The closing events of an unfinished turn. For Interrupted and Forked the
/// list ends with MMDB's own `TurnEnded`, ID `mmdb:turn-{n}:end`, naming the
/// turn's `start_event_id`.
pub(super) fn close(turn: &OpenTurn, owed: &[Owed], reason: TurnEndReason) -> Vec<Closing>;
```

- Invariants: an owed item's step is the open step or `None`, because a step that owes something cannot end. Closing terminals and results carry the open step, if there is one, and are written before `StepEnded`. A `ToolResult` whose call the open turn does not owe is `Answered`: since a step or turn cannot end while it owes a call, such a call already has a result. `admit` never returns `Change::EndTurn` for `Interrupted` or `Forked`. Only `close` builds those.
- Errors: `TurnError` only. The actor is not its concern: `history` checks it before asking.
- Performance: linear in `owed`.

Behind the seam: which event is allowed where, numbering, which run may write, which turn an end names, what each event owes or settles, the precedence of the rules, and the closing order and identities. `history` does the reads and writes and knows none of these rules.

Call site, inside `append`'s writing branch:

```rust
let state = self.turn_state(&input.session)?; // open turn, owed, numbers
let admitted = turns::admit(
    &state,
    Write { kind: &input.kind, run: access.run.as_deref(), message_step },
)
.map_err(ContextError::Turn)?;
```

### `Scoped::validate_copy_sources` (changed, internal, `crates/mmdb/src/records/scoped.rs`)

```rust
/// Checks a new session event's copy sources and returns the scopes it must carry.
/// `answers` is the header of the tool call a result answers: it is charged
/// against the source budget without an availability check, and its scope and
/// recorded source scopes are carried over. `reserve` keeps that many levels and
/// visits free above the new record, for a record that will cite it.
pub(crate) fn validate_copy_sources(
    &self,
    scope: &Scope,
    sources: &[RecordPin],
    answers: Option<&RecordHeader>,
    reserve: usize,
) -> ContextResult<Vec<Scope>>;
```

- Invariants: a tool call passes `reserve: 1`, so its walk starts its sources one level deeper and charges one more visit. Any result for it then fits the depth limit of 32 and the 4,096 visits: a result without other sources always does, and other sources share the remaining budget.
- Errors: as today. `BudgetExceeded` for a call whose sources leave no room for its result. A walk under `answers` still needs the writer's access to reach the call's sources, as today.
- Behind the seam: the walk, its budget and the scope collection. `history` is its only caller.

### `session::history` (changed)

```rust
/// One event ready to write: identity, kind, payload, provenance, copy sources and stamp resolved.
struct NewEvent {
    session: String,
    event_id: String,
    kind: EventKind,
    media_type: String,
    scope: Scope,
    occurred_at_ms: i64,
    provenance: Provenance,
    sources: Vec<RecordPin>,
    source_scopes: Vec<Scope>,
    pin: RecordPin,
    payload: PayloadDescriptor,
    /// The message lifecycle after this event, for assistant events.
    message: Option<MessageState>,
    /// Added with `TurnStamp`.
    turn: Option<TurnStamp>,
}

impl SessionLog<'_> {
    /// Writes the header, order, identity, call, result and message entries of one
    /// event into `batch`, and advances `head`. The caller writes `head` once, after
    /// the last event of the batch. Commits nothing.
    fn put_event(
        &self,
        batch: &mut mmdb_kernel::Batch,
        head: &mut SessionHead,
        event: NewEvent,
    ) -> ContextResult<WriteReceipt>;
}

/// Ends every turn an exited process left open as Interrupted, in one batch.
/// Returns how many turns it ended.
pub(crate) fn end_interrupted_turns(records: &Records, parts: &SessionParts) -> ContextResult<usize>;
```

- Ordering: `end_interrupted_turns` runs inside `Store::open`, after the handles are built and before the store is returned. It is not called by `Store::create`.
- Reads: it enumerates the open-turn partition only. For each open turn it reads that session's head (scope, sequence, numbers), prefix-scans the session's owed items, and reads each owed call's index and header for its pin and source scopes. It reads nothing of a session without an open turn, and it walks no sources.
- Errors: a storage or decode error before the commit writes nothing and fails `Store::open` with `StoreError::Context`; the next open tries again. A commit whose outcome is unknown fails `Store::open` with `StoreError::Context(ContextError::Storage(KernelError::CommitUnknown))`. The batch either landed whole, removing every open-turn row it closed, or not at all, so the next open closes each turn exactly once.
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

### `Kernel::lose_next_commit_ack` (moved, test-only, `crates/mmdb-kernel/src/kernel.rs`)

```rust
impl Kernel {
    /// The next `Batch::commit` whose batch writes `partition`, through any caller,
    /// lands and then reports `CommitUnknown`. Commits that do not write it are untouched.
    #[cfg(any(test, feature = "test-hooks"))]
    pub fn lose_next_commit_ack(&self, partition: &Partition);
}
```

- Invariants: one shot. The hook fires on the first matching commit, then disarms. A commit refused before the engine commit, for an unusable store or a stale era, neither lands nor disarms it. Arming again replaces the armed partition.
- Why a partition: one `append` with a payload commits several batches. It commits the pending-upload marker and each payload chunk before the batch that publishes the event and its receipt. A hook on the next commit of any kind would hit the marker. A test therefore names the partition that only its target commit writes.
- Configuration: `mmdb-kernel` gains the feature `test-hooks`, off by default. `crates/mmdb/Cargo.toml` enables it in `[dev-dependencies]` only. The kernel's own unit tests see the hook through `cfg(test)`.
- Behind the seam: the kernel holds the qualified name of the armed partition, `<module>.<era>.<name>`. Only with the hook compiled in, a `Batch` also records the qualified names of the partitions it writes. After the engine commit succeeds, `Batch::commit` checks whether the batch wrote the armed partition. If it did, it disarms the hook and returns `KernelError::CommitUnknown`. `Records::fail_commit_ack` and the check in `commit_operation` go away. `commit_operation` already maps any commit error to `ContextError::CommitUnknown(operation)`.
- `Scoped::lose_next_commit_ack()` (`cfg(test)`, no argument, `crates/mmdb/src/records/scoped.rs`) arms the kernel hook on the operation-receipt partition, `records_operations_v1`. It therefore hits the commit that publishes an operation's receipt, and never an earlier marker or chunk commit. Its two existing callers keep working unchanged.

Call sites:

```rust
// The commit that publishes an append's event and receipt, as today.
store.scoped.lose_next_commit_ack();

// In `store.rs`'s tests. Recovery writes no receipt; its one batch removes
// the open-turn rows it closes, so the test arms the open-turn partition of `SessionParts`.
store.kernel.lose_next_commit_ack(&store.session.open_turns);
```

## Data

- Stored: `session_heads_v1` gains the session's `Numbers` next to `sequence` and `scope`. Writer: `SessionLog::append` and `end_interrupted_turns`, once per batch.
- Stored: `session_open_turns_v1`, key `(owner, session)`, value `OpenTurn`, present only while the turn is open. Writer: `append` and `end_interrupted_turns`.
- Stored: `session_owed_v1`, key `(owner, session, 0 = terminal | 1 = result, identity)`, value the owed item's step, present only while it is owed. Writer: `append` and `end_interrupted_turns`.
- Stored: `session_messages_v1` values gain `MessageState::step`. Writer: `put_event`.
- Stored: `RecordHeader::event.turn` on every event header, written once with the event.
- Existing records: reset (status `startup`). `STORE_FORMAT_ID` becomes `mmdb-store-v3` in the same step that first writes turns, open turns and owed items, so no `mmdb-store-v3` directory holds a turn written without those rules. `Store::open` refuses a `mmdb-store-v2` directory with the existing `FormatMismatch`, like any other format, and the operator runs the existing whole-store reset. No old event is renumbered or wrapped in a turn.

## Dependencies

| Dependency | Category | Production adapter | Test adapter |
| --- | --- | --- | --- |
| Storage engine, through `mmdb-kernel` | local-substitutable | the store directory | a store in a `tempfile` directory; `Kernel::lose_next_commit_ack(partition)` for the unknown outcome of the commit that writes that partition |
| Process exit, for the crash test | local-substitutable | the real process | the test binary re-run as a child that exits mid-turn, as `history_crash_child` does |
| Clock (`now_ms`) and BLAKE3 | in-process | — | — |

No new seam. The commit hook moves down a layer so that recovery's commit, which writes no receipt, reaches it too. It picks its commit by a partition that commit writes, because one append commits a marker and chunks before its receipt.

## Steps

1. Extract writing one event into a batch from `SessionLog::append` into `put_event`, so one batch can hold several events. `NewEvent` has no `turn` field yet. No behavior change. After: none. Check: `cargo test --workspace --locked`
2. Move the lost-acknowledgement test hook from `commit_operation` into `Batch::commit` as `Kernel::lose_next_commit_ack(partition)`, behind the new `test-hooks` feature that `mmdb` enables for its tests. `Scoped::lose_next_commit_ack()` arms it on `records_operations_v1`. No behavior change. Two existing tests keep passing unchanged: `committed_history_survives_a_lost_acknowledgement_without_duplication`, whose append commits a pending-upload marker before its receipt, and the purge test in `crates/mmdb/src/context/tests.rs` that also arms the hook. New kernel unit test `a_lost_ack_hits_only_the_commit_that_writes_the_armed_partition`, in `crates/mmdb-kernel/src/kernel/tests.rs`. The hook is armed on partition A. A commit that writes only B returns `Ok`. The next commit writes A and B, returns `CommitUnknown`, and both writes are readable. The commit after that writes A and returns `Ok`. After: none. Check: `cargo test --workspace --locked`
3. Reserve room for a tool call's result. Add `reserve` to `validate_copy_sources`; a `ToolCall` passes 1. Test `a_tool_call_reserves_room_for_its_result` in `crates/mmdb/tests/context_contract.rs`, next to `history_copy_reserves_new_root_depth_before_committing_an_unreadable_record` and built on its chain: a call citing `chain[31]` is refused with `BudgetExceeded`; a call citing `chain[30]` is accepted, its result is accepted, and `validate_current` on the result succeeds. After: 1, 2. Check: `cargo test --workspace --locked`
4. Record turns and steps and place events in them. Add `TurnStarted`, `TurnEnded`, `StepStarted`, `StepEnded`, `TurnEndReason`, `TurnStamp`, `TurnError` and `ContextError::Turn`, `MessageState::step`, and `NewEvent::turn`. Add `session::turns` with every rule except closing. Add the System-or-Operator check, the `mmdb:` prefix and `start_event_id` validation to input validation. Add `Numbers` in the session head, `session_open_turns_v1`, `session_owed_v1`, stamps on every event, the new link lookups, and `answers` in `validate_copy_sources`. Until step 5, `Cancelled` and `Failed` with an open step or anything owed are refused as `TurnUnfinished`, so no ended turn owes anything. Raise `STORE_FORMAT_ID` from `mmdb-store-v2` to `mmdb-store-v3`, and update the format line in `README.md` and the assertion in `native_memory.rs`. The Fjall 2 fixtures in `crates/mmdb/tests/fjall_format.rs` stay `mmdb-store-v1` and keep being refused. Update the existing tests listed under Replaces: they open a turn with an access that has a run, and a step where they write assistant events. New tests in `crates/mmdb/tests/session_turns.rs`:
   - `turn_and_step_numbers_increase_across_the_session`
   - `only_system_or_operator_write_turn_and_step_events`
   - `a_turn_needs_a_run_and_a_session_holds_one_open_turn`
   - `a_harness_cannot_write_interrupted_or_forked`
   - `event_ids_with_the_mmdb_prefix_are_reserved`
   - `retrying_a_turn_event_after_the_turn_moved_on_returns_its_receipt`
   - `withdrawn_turn_events_keep_numbers_and_turn_state`
   - `assistant_events_and_linked_calls_need_an_open_step`
   - `a_linked_call_stays_in_its_message_step`: a call linked to a terminated message in the same step is accepted; the same link in a later step, and in a later turn, is `OutsideStep`.
   - `direct_calls_inside_a_turn_need_no_step`
   - `only_the_opening_run_writes_inside_a_turn`: covers steps, assistant events, calls, results and the turn end.
   - `a_turn_end_names_the_turn_it_ends`: an end naming another start event is `OtherTurn`. An end retried after its turn closed is refused, both when another run's turn is open (`OtherRun`) and when the same run's later turn is open (`OtherTurn`).
   - `work_started_by_the_turn_run_writes_under_that_run`
   - `a_foreign_caller_waits_for_the_open_turn`
   - `a_step_ends_only_when_it_owes_nothing`
   - `completed_needs_no_open_step_and_no_unanswered_call`
   - `an_owed_call_gets_its_result_after_it_becomes_unreadable`: after a purge of the call, a retraction of the call, and a withdrawn fragment of its message. Each result is accepted, is unreadable, carries the call's `source_scopes`, and lets the step and turn end Completed.
   - `a_second_result_for_a_call_is_answered`: a second result under a new event ID is `Answered`, in the same turn and in a later one.
   - `turn_errors_follow_their_precedence`: an `AssistantMessage`, an `AssistantStarted` and a linked call in an empty session are `OutsideTurn`. Another run's assistant event with no open step is `OtherRun`.

   After: 3. Check: `cargo test --workspace --locked`
5. Close Cancelled and Failed turns. Add `turns::close` and write its events through `put_event` in the end event's batch, with the IDs, provenance, sources and order above. This replaces step 4's refusal. Tests in `crates/mmdb/tests/session_turns.rs`:
   - `cancelling_a_turn_closes_what_it_owes_in_one_batch`: closing events in order before the end event, System actor and the turn's run, empty payloads, `mmdb:` IDs, no open turn afterwards.
   - `failed_turns_close_like_cancelled_ones`
   - `results_written_before_the_end_are_kept`
   - `a_late_result_after_cancel_is_refused`: `Answered` with no turn open, with another run's turn open, with a later turn of the same run open, and after the call was purged.
   - `a_closing_result_fits_the_reserved_budget`: a call citing `chain[30]` is cancelled, and `validate_current` on its closing result succeeds.
   - A retry case in `retrying_a_turn_event_after_the_turn_moved_on_returns_its_receipt`: a retried cancel writes no second set of closing events.

   In-crate, in `crates/mmdb/src/context/tests.rs`: `a_cancel_with_a_lost_acknowledgement_published_all_its_closing_events`. Arm `Scoped::lose_next_commit_ack()`, which hits the commit that publishes the cancel's receipt, then cancel a turn that owes one terminal and one call; the append returns `CommitUnknown`. The position advanced by exactly the closing events plus one, every closing event and the end event are present, and the same run can open a new turn. After: 4. Check: `cargo test --workspace --locked`
6. End turns left open at `Store::open` as Interrupted. Add `end_interrupted_turns` and call it from `Store::open`. Make `session::session_key` `pub(crate)` for the in-crate tests. Add a sentence on turns to `README.md`'s session log paragraph. Set the feature doc to `shipped` and archive this spec. Tests:
   - `a_turn_left_open_by_an_exited_process_ends_interrupted_on_open`, in `crates/mmdb/tests/session_turns.rs`: a child run of the test binary exits mid-step, with an unterminated message and an unanswered call.
   - `reopening_in_the_same_process_ends_its_own_open_turn`, in the same file.
   - `opening_reads_only_sessions_with_open_turns`, in-crate in `crates/mmdb/src/store.rs`'s tests. Set up two owners with open turns, plus a session with no open turn whose head value under `session_key` is overwritten with undecodable bytes. `Store::open` succeeds and ends both open turns.
   - `an_unknown_recovery_commit_closes_each_turn_once`, in the same in-crate tests. Two sessions have open turns. Arm the kernel hook on the open-turn partition of `SessionParts` and call `end_interrupted_turns` directly; it returns `CommitUnknown`. Before reopening, both sessions already show their closing events and end events, and no open-turn row is left. Reopening changes no position.
   - `recovery_reads_no_source_of_an_owed_call`, in `crates/mmdb/tests/session_turns.rs`: a workspace session's unanswered call cites a Personal event of another session with no open turn; the handle is dropped and the store reopened. The store opens, and the closing result carries the call's `source_scopes`.

   After: 5. Check: `cargo test --workspace --locked`

## Not this

- Fork. `turns::close` accepts `Forked` so fork can reuse it, but nothing writes `Forked` until fork exists (feature 3 of `docs/polaris/storage-kernel.md`).
- A read interface by turn or step, or a query for the open turn or its start event ID. Readers group pages by `LogEvent::turn`. A harness learns about an open turn from `TurnError::TurnOpen`, and keeps the start event ID it chose.
- A scan counter proving that recovery never touches the heads partition. `opening_reads_only_sessions_with_open_turns` catches decoding every head; a key-only scan is left to review.
- Native memory checking commitment evidence against the turn end in the log. `EvidenceLifecycle::TerminalTurn` still takes the status the caller reports.
- Run ancestry. MMDB compares the writer's run with the turn's run only.
- Renaming `session/history.rs` or its internal digest domains.
- Changes to harnesses outside this repo, such as MiuMiu. They adapt when they take this version.
