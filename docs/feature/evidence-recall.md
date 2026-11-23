# Evidence recall and revision-checked proposals

Status: shipped

`Database::recall` returns ranked memories with lexical, vector, and graph evidence. The [harness context contract](context-ontology.md) exposes a separate recall operation for source-backed objects.

## Candidates and provenance

Recall runs BM25 and selected
embedding profiles, fuses branches with weighted reciprocal-rank fusion, then
expands valid weighted graph paths. Before optional lawyer adjudication it:

- checks validity at the requested time, excludes pending memories, and permits historical reads of superseded or retracted nodes inside their closed validity intervals;
- treats `derived_from` provenance as directional from derivative to source;
- follows causal relations only from source to effect;
- rejects causal paths whose node validity runs backward;
- returns contradictory memories together;
- marks facts without provenance as unverified.

Lifecycle and metadata filters run inside lexical and vector candidate selection,
before `candidate_limit`. `min_vector_similarity`, when set, removes weak vector
hits before evidence fusion while leaving lexical evidence independent. Candidate
node read failures are returned instead of being mistaken for filtered misses.
Embedding writes and queries reject non-finite or non-normalizable vectors.

The BM25 documents and postings live in `lexical_docs_v1` and `lexical_postings_v1`. Hosts can use `graph_slice` to inspect the nodes, timestamps, weights, evidence, and paths behind a result.

## Lawyer verdicts and application

Lawyer requests are capped at 50 candidates and 128 KiB of sanitized textual
evidence. A verdict may gate or reorder the returned candidates and create
`ChangeProposal` records, but it cannot mutate during recall. `apply_proposal`
rechecks every referenced revision, persists an `Applying` checkpoint, and
advances a durable per-change cursor. Each change is replay-safe; reopen resumes
an interrupted proposal and reconciles node indexes before advancing past a
node write that may have committed just before a crash. Proposal apply and
reject transitions share the node-mutation critical section, so validation,
node/edge changes, and terminal proposal status are serializable. Proposed
edges must be new rather than overwriting an existing graph fact.

Proposals live in `change_proposals_v1`. The caller can review the sanitized requests and outcomes in the [audit journal](memory-audit.md).

## Recording consumption

Hosts call `record_access` after evidence is actually consumed. The counters are
kept in `memory_access_v1`, so retrieval does not change semantic revisions or
force projection rebuilds. `merge_access_stats` atomically transfers source
counters into a canonical node and removes the source sidecars, so consolidation
retries do not double-count. `retract` is the audited, revision-checkable soft
forget path; it closes current validity while retaining historical evidence.

See [recall and proposal handling](../../crates/mmdb/src/recall.rs), [lexical indexing](../../crates/mmdb/src/lexical.rs), and [access accounting](../../crates/mmdb/src/access.rs).
