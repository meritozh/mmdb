# 连续会话日志与原文搜索：implement spec

Feature: `docs/feature/session-log.md` @ `ff92c11b29efc121fd18261cd78be41d8aeb2e46`

This spec is open until the feature ships, and `/go-work` carries it out. Do not edit it while working it; only `to-implement-spec` changes it. Before work starts or resumes, check that the hash above still matches `git hash-object docs/feature/session-log.md`; if it does not, the feature changed, so stop and rerun `to-implement-spec`. Each step is one commit whose message ends with the trailer `Spec-Step: session-log#<n>`. The commit that lands the last step sets the feature doc's Status to `shipped` and moves this file to `docs/spec/archived/<YYYY-MM-DD>-session-log.md` with `git mv`, dated that day. Nothing reads an archived spec again.

A step end names its `StepStarted` event ID, the way a turn end names its `TurnStarted`. The open turn records that ID. It also counts what the turn and its open step owe, so an append inside a turn reads only the owed row it settles. Only a turn end that closes owed work scans the owed rows.

Every feature line not listed below is already true in the code and keeps its tests.

## Behavior map

| Feature doc line | Owner | Test |
| --- | --- | --- |
| Who：轮次和步骤的结束事件都以开启事件的 ID 指明它结束的轮次或步骤 | `session::turns::admit` | `a_step_end_names_the_step_it_ends`; `a_turn_end_names_the_turn_it_ends` (existing) |
| Fails：步骤结束事件指明的不是当前未结束的步骤 | `session::turns` (`TurnError::OtherStep`) | `a_step_end_names_the_step_it_ends`, `turn_errors_follow_their_precedence` |
| Keeps：开启事件被撤回或清除后，结束事件照样以它的 ID 指明要结束的轮次或步骤 | `session::turns` compares the ID that `session::history` recorded when the step opened | `withdrawn_turn_events_keep_numbers_and_turn_state` (extended) |
| Does 补齐：未结束的步骤补一个结束事件 | `session::turns::close` | `cancelling_a_turn_closes_what_it_owes_in_one_batch`, `a_turn_left_open_by_an_exited_process_ends_interrupted_on_open` (both extended) |
| Does 结束：步骤结束前其中的消息都已终止、调用都有结果；以 Completed 结束时不能有未结束的步骤或没有结果的调用 | `session::turns::admit`, from the counts on `OpenTurn` | `a_step_ends_only_when_it_owes_nothing`, `completed_needs_no_open_step_and_no_unanswered_call` (existing); `a_direct_call_outside_a_step_holds_the_turn_but_not_a_later_step` |
| No feature line: an append inside an open turn reads only the owed row it settles | `session::history` | `an_append_reads_only_the_owed_row_it_settles` |

Replaces: no test. Step 1 updates every `EventKind::StepEnded` written in `crates/mmdb/tests/session_turns.rs`, `crates/mmdb/src/context/tests.rs` and `crates/mmdb/src/store.rs` so it names its `StepStarted` event ID.

## Modules

### `EventKind::StepEnded` and `TurnError::OtherStep` (changed, public, `crates/mmdb/src/records/types.rs`, `crates/mmdb/src/records/error.rs`)

```rust
pub enum EventKind {
    // ...
    /// Opens one model request in the current turn. Its event ID names the step.
    StepStarted,
    /// Ends the open step, named by the event ID of its `StepStarted`.
    StepEnded { start_event_id: String },
}

/// The first turn or step rule that refused an append.
pub enum TurnError {
    ReservedReason,
    RunRequired,
    TurnOpen,
    Answered,
    OutsideTurn,
    OtherRun,
    OtherTurn,
    StepOpen,
    OutsideStep,
    /// The step end names another step.
    OtherStep,
    StepUnfinished,
    TurnUnfinished,
}
```

- Invariants: a step end is accepted only when it names the open step's `StepStarted` event ID. MMDB compares the ID it recorded when the step opened. It never reads the `StepStarted` event, so retracting or purging that event does not stop the end. The same holds for a turn end and its `TurnStarted`.
- Ordering: the retry checks still run first. A step end that already landed returns its receipt, even after the next step opened. Among the turn rules, the variant order above is the precedence: `OutsideTurn`, `OtherRun`, `OutsideStep` when no step is open, then `OtherStep`, then `StepUnfinished`.
- Errors: `ContextError::Turn(TurnError::OtherStep)` when the ID names anything other than the open step, including a step that already ended while a later one is open. Nothing is written. A step end with no open step stays `OutsideStep`.
- Closing: the `mmdb:turn-{n}:step-end` event that MMDB writes for Cancelled, Failed and Interrupted names the open step's start event ID.

Call site:

```rust
log.append(OperationId::new(), event("chat-1", "turn-7.step-2", EventKind::StepStarted), empty())?;
// the model request: its assistant message, tool calls and results
log.append(
    OperationId::new(),
    event("chat-1", "turn-7.step-2.end", EventKind::StepEnded {
        start_event_id: "turn-7.step-2".into(),
    }),
    empty(),
)?;
// A refused or stale end retried after "turn-7.step-3" opened is TurnError::OtherStep.
```

### `session::turns` (changed, internal, `crates/mmdb/src/session/turns.rs`)

```rust
pub(super) struct OpenTurn {
    pub owner: String,
    pub session: String,
    pub number: u64,
    pub run: String,
    pub start_event_id: String,
    /// Owed items of this turn, inside a step or not.
    pub owes: u64,
    pub step: Option<OpenStep>,
}

pub(super) struct OpenStep {
    pub number: u64,
    pub start_event_id: String,
    /// Owed items of this step.
    pub owes: u64,
}

impl OpenTurn {
    /// Counts an owed item that the batch inserts.
    pub(super) fn owe(&mut self, item: &Owed);
    /// Uncounts an owed item that the batch removes.
    /// Returns false when a count would go below zero.
    pub(super) fn settle(&mut self, item: &Owed) -> bool;
}

pub(super) struct TurnState {
    pub open: Option<OpenTurn>,
    pub numbers: Numbers,
}

pub(super) struct Write<'a> {
    pub kind: &'a EventKind,
    pub run: Option<&'a str>,
    pub message_step: Option<u64>,
    /// The owed item this event would settle, read by its identity:
    /// a result's call, or a terminal's message. `None` when nothing is owed under it.
    pub owed: Option<Owed>,
}

pub(super) enum Change {
    Nothing,
    Owe(Owed),
    Settle(Owed),
    StartTurn { number: u64, run: String },
    StartStep(u64),
    EndStep,
    EndTurn(TurnEndReason),
}

pub(super) fn admit(state: &TurnState, write: Write<'_>) -> Result<Admitted, TurnError>;
/// Owed items arrive in identity order, terminals first and then results.
pub(super) fn close(turn: &OpenTurn, owed: &[Owed], reason: TurnEndReason) -> Vec<Closing>;
```

- Invariants:
  - `OpenTurn::owes` equals the number of the turn's owed rows. `OpenStep::owes` equals the number of owed rows whose step is the open step.
  - Both counts change only in the batch that inserts or removes those rows.
  - An owed item's step is the open step or `None`, so a step opens with `owes: 0`.
- Rules that read the counts:
  - `StepEnded` is `StepUnfinished` while `step.owes > 0`.
  - `TurnEnded { Completed }` is `TurnUnfinished` while a step is open or `owes > 0`.
  - A `ToolResult` whose `write.owed` is `None` is `Answered`, before the turn checks, as now.
  - An `AssistantTerminal` still needs the open step. It settles `write.owed` when that is `Some`, and otherwise changes nothing: the link lookups have already accepted the terminal, and removing an absent row changes nothing today.
  - `TurnEnded` returns `Change::EndTurn(reason)`. `admit` no longer builds the closing events, because it no longer holds the owed list.
- `close`: the closing `StepEnded` carries `step.start_event_id`. Its stamp's step is `step.number`.

Behind the seam: which event is allowed where, the precedence, which ID an end must name, how each owed item moves the counts, and the closing order and identities. `history` reads and writes rows and knows none of these rules.

### `session::history` (changed, internal, `crates/mmdb/src/session/history.rs`)

- Ordering, for a new write: input validation, retry checks, expected position and link lookups, all as now. Then:
  1. Read the open turn. This is the only turn state read for every append.
  2. For a `ToolResult` or an `AssistantTerminal`, point-read its owed row, `(session key, 1, call_id)` or `(session key, 0, message_id)`, into `Write::owed`.
  3. `turns::admit`, then the copy sources, as now.
- Applying a change:
  - `Owe` inserts the row, calls `OpenTurn::owe`, and writes the open-turn row.
  - `Settle` removes the row, calls `OpenTurn::settle`, and writes the open-turn row. When `settle` returns false, the append fails with `ContextError::Corrupt` and nothing is written.
  - `StartStep(n)` records `OpenStep { number: n, start_event_id: input.event_id, owes: 0 }`.
  - `EndTurn(reason)` prefix-scans the turn's owed rows when `owes > 0`, calls `turns::close`, and writes through `put_turn_end`. `end_interrupted_turns` takes this same path.
- Performance: every append makes one point read of the open turn. A result or a terminal adds one point read of its own owed row. An `Owe` or `Settle` adds one write of the open-turn row. Only a turn end that owes something prefix-scans the owed rows, which only Cancelled, Failed and Interrupted allow. No append reads an owed row other than the one it settles. Under the store-wide write lock, an append's cost no longer grows with how much the turn owes.

## Data

- Stored: the `session_open_turns_v1` value becomes `OpenTurn { owner, session, number, run, start_event_id, owes, step: Option<OpenStep { number, start_event_id, owes }> }`. Writers: `SessionLog::append`, which writes it, and `end_interrupted_turns`, which removes it.
- Stored: event headers carry `EventKind::StepEnded { start_event_id }`.
- Existing records: reset (status `startup`). `STORE_FORMAT_ID` stays `mmdb-store-v3`: that format exists only on this unpushed branch, so no store outside its tests holds it. A v3 store written by an earlier commit of this branch fails to decode its open-turn rows or step ends; reset it.

## Dependencies

| Dependency | Category | Production adapter | Test adapter |
| --- | --- | --- | --- |
| Storage engine, through `mmdb-kernel` | local-substitutable | the store directory | a store in a `tempfile` directory |
| Process exit, for the crash test | local-substitutable | the real process | the test binary re-run as a child, as the existing crash test does |

No new seam.

## Steps

1. Name the step that a step end closes.
   - Code: add `StepEnded { start_event_id }` and `TurnError::OtherStep`. Replace `OpenTurn::step: Option<u64>` with `Option<OpenStep { number, start_event_id }>`; step 2 adds `owes`. `history` records the step's start event ID when the step opens, `admit` compares it, and `close` writes it into the closing step end.
   - Update every existing `StepEnded` write in the tests named under Replaces.
   - New test in `crates/mmdb/tests/session_turns.rs`, `a_step_end_names_the_step_it_ends`:
     - An end naming another start event ID is `OtherStep`, and the position does not move.
     - The review's sequence: a step end is refused as `StepUnfinished`; the step is then closed by a new end under a new operation, and the same run opens the next step. Retrying the refused operation is `OtherStep`, the position does not move, and an `AssistantStarted` is then accepted in the open step.
     - Retrying a step end that landed, after the next step opened, returns its original receipt.
   - Extended tests:
     - `turn_errors_follow_their_precedence`: another run's end naming the wrong step is `OtherRun`. An end naming the wrong step while the step still owes a terminal is `OtherStep`. An end with no open step is `OutsideStep`.
     - `withdrawn_turn_events_keep_numbers_and_turn_state`: after the `StepStarted` is retracted, an end naming it is accepted. In a second session, the `StepStarted` is purged and its end is accepted the same way. The same two cases hold for a `TurnStarted` and its `Completed` end.
     - `cancelling_a_turn_closes_what_it_owes_in_one_batch` and `a_turn_left_open_by_an_exited_process_ends_interrupted_on_open`: the closing step end names the open step's start event ID.

   After: none. Check: `cargo test --workspace --locked`
2. Count what the open turn owes, and stop scanning owed rows on every append.
   - Code: add `owes` to `OpenTurn` and `OpenStep`, `OpenTurn::owe` and `OpenTurn::settle`, and `Write::owed` with its point read. Remove `TurnState::owed`, and change `Change::EndTurn` to carry the reason, with `history` scanning only at a turn end that owes something. No behavior change; every existing test keeps passing unchanged.
   - New test `a_direct_call_outside_a_step_holds_the_turn_but_not_a_later_step`, in `crates/mmdb/tests/session_turns.rs`: a direct call is written while no step is open, then a step opens and ends. A `Completed` end is then `TurnUnfinished` until the call's result is written, and is accepted after it.
   - New test `an_append_reads_only_the_owed_row_it_settles`, in-crate in `crates/mmdb/src/store.rs`'s tests:
     - Setup: a turn and a step, a terminated assistant message, and two calls linked to it, A and B. B's owed row value, under `session_key` plus `[1]` plus `"B"`, is overwritten with bytes that do not decode.
     - A `Notice` is accepted, and the result for A is accepted.
     - A step end naming the open step is `StepUnfinished`, because the count still holds B.
     - A `Cancelled` end fails with `ContextError::Corrupt`, because closing reads every owed row, and the position does not move.

   After: 1. Check: `cargo test --workspace --locked`

## Not this

- A new store format. See Data.
- A lookup of the open step or its start event ID. The harness keeps the ID it chose, as it does for a turn.
- A scan counter proving which owed rows an append reads. `an_append_reads_only_the_owed_row_it_settles` traps a read of another row.
- Changes to harnesses outside this repo, such as MiuMiu. Their step ends gain the start event ID when they take this version.
