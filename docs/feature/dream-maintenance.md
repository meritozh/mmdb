# Reversible dream maintenance

Status: shipped

`Database::maintain` selects unprocessed raw memories deterministically and
adds active derived memories as context. Turn-end maintenance requires 32 raw
nodes by default; context-compaction and manual maintenance run whenever raw
input is pending. Batches never exceed 128 nodes or 256 KiB.

Dream responses may create cited fact/entity nodes, add reserved relations, and
supersede memories previously created by a dream run. New nodes are staged as
`Pending` through projection work. After semantic fingerprint validation, a
durable `Completing` checkpoint precedes edge creation, input supersession, and
output activation; the run then captures each output revision before publishing
`Completed`. Raw episodes are never changed. Reopen rolls back only staging that
still matches the run's ownership and semantic fingerprints, preserving external
edits rather than silently erasing them. `revert_dream` first verifies
that every created node is still active, owned by the run, and at its captured
revision, and that every added edge still matches its captured revision; it
refuses to erase post-dream edits. A valid revert retracts outputs while
restoring superseded derived memories.
The external dreamer call stays outside the mutation lock; response validation,
application checkpoints, concurrent-run deduplication, repair, and revert run
inside it.

Use `dream_runs` and `dream_run` to inspect progress and provenance in `dream_runs_v1`. Concurrent equivalent runs deduplicate at the mutation boundary. Validation rejects duplicate edge overwrites and self-referential provenance. See [dream execution and reversal](../../crates/mmdb/src/dream.rs).
