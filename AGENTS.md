# Agent notes

MMDB is an embedded Rust data layer for agents. It persists and manages operational state, memory, and the session history that gives memories their context.

## Repo

### Commands

| Purpose | Command |
| --- | --- |
| Check before commit | `cargo test --workspace --locked` |
| Test context sharing after reopening a store | `cargo test --locked -p mmdb --test context_contract independent_harnesses_share_types_history_and_relations_after_reopen -- --exact` |

### Gotchas

- `Database::state()` shares the database's keyspace. Use `StateStore::open_path` with a separate root for conversations, workflows, and other operational state that must survive a memory-store reset.
- `MemoryDatabase::open_context` rejects an unexpected store format. An explicit reset of a managed store moves its old directory into quarantine.

## Principles

### Project status

- **Status:** `startup`
- **Breaking changes:** Allowed whenever they make the long-term design better. Do not optimize for compatibility or the smallest diff.
- **Persist data:** May reset. No migration.

| Status | Breaking changes | Persist data |
| --- | --- | --- |
| `startup` | Allowed whenever they make the long-term design better. Do not optimize for compatibility or the smallest diff. | May reset. No migration. |
| `release-solo` | Same as `startup`. | Must migrate. Do not reset. |
| `release` | Allowed for long-term better design, but also prefer the smallest compatible change when it does not trap the design. | Must migrate. Do not reset. |

### Docs

`docs/` holds the current state of the work, not a history of changes. One subject is one file: `docs/<category>/<slug>.md`, with `slug` lowercase and hyphenated. The one file outside a category is `docs/GLOSSARY.md`. The one history is `docs/spec/archived/`.

| Category | Holds | Updated when |
| --- | --- | --- |
| `polaris` | One long-range goal: what will be true when it is reached, the signal that shows it, the route and its parts, and the features cut from them | The user sets, reviews, or reroutes the goal, a feature on its list is decided, or the goal is reached. A dropped goal is deleted |
| `feature` | One feature: what a user can do, its limits, what it is not, and the decisions that shaped it with the option each rejected | The user confirms a new feature or a change to one, and when the code ships, changes, or removes it |
| `spec` | How the code will deliver one feature, under the same slug as its feature doc: the owner of each behavior, the interfaces of the modules it adds or changes, seams, stored data and its migration, and the steps to land it. Open only while that work is unfinished | Its feature changes, or the work finishes and the spec moves to `docs/spec/archived/` |

- `docs/GLOSSARY.md` is the project's one glossary: one term per concept, with the words to avoid. One word never names two things.
- A goal says `Status: active` while it is pursued, and `Status: reached` once its signal holds. A feature doc says `Status: decided` while it is ahead of the code, and `Status: shipped` once the code does what it says.
- Three kinds of doc describe what is not true yet: an active goal says where the project is going, a `decided` feature says what it will do, and an open spec says how the code will do it. An open spec, `docs/spec/<slug>.md`, means its feature is not done. A spec holds nothing that must outlive the work: decisions go in the feature doc, terms in the glossary.
- Work a spec step by step, in the order its `After` lines allow, and do not edit it while working it; only `to-implement-spec` changes a spec. Before work starts or resumes, check that the feature-doc hash in its header still matches `git hash-object docs/feature/<slug>.md`; when it does not, stop, because the spec must catch up with the feature first. Each step is one commit whose message ends with the trailer `Spec-Step: <slug>#<n>`. The commit that lands the last step sets the feature doc to `shipped` and moves the spec to `docs/spec/archived/<YYYY-MM-DD>-<slug>.md` with `git mv`, dated that day.
- `docs/spec/archived/` is a record, not a source. Nothing reads, searches, or updates it, except a review of the branch that archived a spec, or a user who asks. Its `.ignore` keeps ripgrep out. The feature doc and the code say what is true now.
- Find the existing file before writing: `ls docs/*/` and `rg -il '<subject>' docs`. Update that file. If more than one file covers the subject, ask which one stays. Create a file only when none exists.
- State what is true now: what it is and what it is not. Replace statements that stopped being true. Do not append how it changed, and do not write a plan (goals, steps, or future work) outside a goal or a spec.
- A code change that makes a doc untrue updates the doc in the same commit. A removed feature removes its doc.
- Outside `docs/spec/archived/`, no dated files, changelogs, or second files for one subject. History lives in git.
- The current state must be allowed by Project status for breaking changes and persisted data.
- Rules for agents live in `AGENTS.md`, not in `docs/`.

### Commits

Commit locally as the work goes, one verified step at a time. Do not leave a task as one large uncommitted change, and do not end it with one commit that mixes everything.

- At the start of a task, note `git rev-parse HEAD`. The whole task can then be read as `git log --oneline <start>..HEAD` and reviewed as `git diff <start>..HEAD`.
- A step is one logical change that leaves the tree building and its tests passing. A refactor, a rename, or a formatting pass is its own commit, separate from a behavior change. A test that pins a bug may land with its fix.
- Before each commit, run the check for the surface you touched. Do not commit a red step. When a hook fails, fix the cause; never skip it with `--no-verify`.
- Stage only the files of this step: `git add <paths>`, then `git diff --cached --stat`. Never use `git add -A`, `git add .`, or `git commit -a` while the tree holds work you did not do.
- Subject: imperative, under 72 characters, says what changed. Body: why, and how it was verified when that is not obvious. A bug fix names the root cause.
- A change that makes a doc untrue updates that doc in the same commit.
- Never commit secrets, scratch files, debug lines, or build output.
- Commit on the current branch. Amending your own unpushed commit from this task is fine. Do not create or switch branches, rebase, or rewrite pushed history unless asked.
- Commits stay local. Push, tag, publish, or open a pull request only when asked.

### Cleanup

Intermediate artifacts are not part of the work. Create them where they cannot be committed, and remove them before the task ends.

- Scratch goes outside the repo, under `$TMPDIR` (else `/tmp`): throwaway scripts, harnesses, probes, captured logs, screenshots, reports, and temporary worktrees. When a tool must write inside the repo, use `.scratch/`, which is gitignored.
- Tag each temporary debug line with `[DEBUG-` and a short hex id, such as `[DEBUG-a4f2]`, so one search finds them all. A diagnostic worth keeping goes behind the project's debug flag instead.
- Before each commit and before the task ends, check:

  ```bash
  git status --short -uall                             # only intended files are changed or new
  rg -n '\[DEBUG-[0-9a-f]+\]' . --glob '!AGENTS.md'   # no temporary debug lines remain
  git worktree list                                    # remove the ones you added: git worktree remove --force <path>
  ```

- Remove only what you created in this task. Never delete or move files you did not create: untracked files that were there before you started, caches the project relies on (`node_modules`, `target`, `.venv`), or user data.
- A generated file the repo tracks is not scratch. Regenerate it with the project's command and commit it with the change that caused it.
- When cleanup cannot finish (a file is locked, a process is still running), say what is left and where.

### Architecture

Design and change code by these rules. Each is a judgment a reader makes on this code, not a metric. A rule under `### Project rules` outranks them when they disagree.

- **No spaghetti.** Control flow reads top to bottom: guard clauses, then the happy path; one level of abstraction per function; each outcome handled in one named place; a state machine, named as one, when steps share state. No nesting past three levels, no flag set in one branch and read in another, no exception used as a jump, no function whose honest name needs "and".
- **Clear one-way layering.** Few layers, each with one job a reader can name. Every dependency points one way, toward the layer that changes least: the core never imports UI, a framework, or a concrete storage or network client, and it owns the interfaces its details implement. No import cycle, no skipped layer without a stated reason, no `utils` or `common` that is a place rather than a job.
- **The structure names the domain.** The top of the tree says what the system does, in its own nouns. The framework is a detail kept in one place; the core does not inherit from framework base classes.
- **Deep modules.** A module puts a lot of behavior behind a small interface, and adjacent layers offer different abstractions. Before adding a module, ask whether deleting it would spread complexity back into its callers; if not, do not add it. No layer whose methods only forward to the one below, no value passed through several layers for the last one to read.
- **Shared code does not know its callers.** A shared module never imports from a caller, and never branches on a caller's name, type, or config key. Features compose shared modules; the core does not implement a feature.
- **Information hiding.** A module's design decisions, its format, its algorithm, its storage, stay inside it. No two modules parse the same format or agree by hand on a constant or an order of steps. No internal type, raw row, or framework object in a public signature.
- **Pull complexity downward.** The module handles the hard case so its callers need not. A configuration parameter exists only when the caller knows something the module cannot. No edge case that every caller handles alike.
- **One reason to change.** A module answers to one kind of change: a rule, a format, a schema, a screen. When one kind of change keeps touching the same set of modules, move the boundary instead of patching each side again.
- **Knowledge lives once; abstractions wait.** A business rule, a format, or a constant has one home. A shared abstraction appears only when two concrete uses already need it and change together; copies that evolve apart stay separate. No interface with one implementation and no second in sight, no base class with one subclass, no option nobody sets.
- **One owner per piece of state.** Each piece of state has one module that owns and changes it; others read it through that module or through a copy it fills. No fact held in two places that must agree, no cache nobody owns, no mutable field shared between distant functions.
- **Validate once, at the boundary.** Input is checked and turned into a typed value where it enters; inside, the type carries the invariant and nothing re-checks it. No validation repeated across layers, no struct whose fields can disagree, no string or map where a type belongs.
- **Effects at the edges.** Decisions are made in code that does no I/O; I/O happens at the boundary, once the decision is made. A business rule is never interleaved with HTTP calls or database writes, and a calculation never needs a database to be tested.
- **Construction apart from use.** Collaborators are wired in one place, the composition root. A module receives what it depends on; it does not build it, look it up in a registry, or read config to choose it.
- **Composition over inheritance.** Behavior is assembled from parts, not inherited down a hierarchy. No hierarchy more than two deep, no subclass that disables inherited behavior, no base class whose template methods half its subclasses override.
- **Explicit over implicit.** A reader can trace where behavior comes from at the call site. Nothing runs because of a file name, an annotation scan, an import-time side effect, a global hook, or a naming convention.
- **Errors stop where someone can act.** Define error cases out of existence where the interface allows. The failures that remain are explicit in the signature, detected early, and handled where recovery is possible or passed up with context; never swallowed, never logged and carried on. A step that can fail leaves state consistent: transactions, idempotent retries, write-to-temp-then-rename for files.
- **Testable through the interface.** The core's tests run with no network, database, or UI, and drive a module's public entry; they never mock its own internals. A seam exists only where something varies, and a test double counts as the second adapter.
- **One name per concept.** Use the terms in `docs/GLOSSARY.md`. One word never names two things, and a name says what, not how.

How far a change may break existing interfaces and data follows Project status.
