# mmdb Features

`mmdb` is the user-facing facade crate. It composes storage, vector, graph,
blob, catalog, and query crates into one `Database` handle.

## Responsibilities

- Open or create a database directory.
- Hide tenant handling from normal callers while preserving tenant-prefixed
  storage internally.
- Provide ergonomic node construction through `NodeBuilder`.
- Keep storage, vector indexes, graph indexes, blob refcounts, and catalog stats
  consistent at the facade boundary.
- Bind `mmdb-query` `LogicalPlan` leaves to the real persisted stores.
- Orchestrate runtime-only embedding and agent clients while persisting only
  public, versioned profiles.
- Fuse BM25, multiple vector branches, and weighted graph evidence.
- Validate and audit lawyer proposals and reversible dream maintenance.

## Opening

```rust
let db = mmdb::Database::open(path)?;

let db = mmdb::Database::open_with(
    path,
    mmdb::DatabaseConfig {
        tenant: 0,
        default_model: "bge-m3".into(),
    },
)?;
```

`open_with_embedder` attaches a text embedder. The embedder model name must match
`DatabaseConfig::default_model`, which prevents automatic text embeddings from
being written into a different model space than default recall uses.

`Database::builder` accepts a `MemoryProfile` and `ClientRegistry`. Profiles are
durable; clients are runtime-only. The host owns authentication, provider SDKs,
endpoints, timeouts, and retries.

The current HNSW backend uses cosine distance, so profile validation rejects
dot-product or Euclidean declarations rather than silently searching with the
wrong metric.

## Node API

Core node operations:

- `insert`
- `get`
- `scan_by_time`
- `delete`
- `insert_text`
- `insert_text_async`

`insert` force-stamps the configured tenant on every node. This keeps the public
API simple while retaining the on-disk tenant prefix for future isolation and
branching work.

When inserting over an existing id, the facade removes old vector entries,
releases replaced blob references, updates storage indexes, and adjusts catalog
stats for kind changes.

## Embedding Behavior

With an embedder configured, inserting non-empty text automatically appends an
embedding for the embedder model if the node does not already carry one.

Explicit embeddings win. A node that already has an embedding for the configured
model is not re-embedded.

`Database::ingest` is the multi-model path. It persists the raw memory before
calling any compatible `EmbeddingClient`, records each `ProjectionStatus`, and
keeps failed work retryable through `retry_projection`. Clients receive owned
text/JSON or a reopenable `BlobInput`.

`ingest_blob` applies the same raw-first workflow to bytes placed in the
content-addressed blob store.

The facade validates:

- embedding `dim` matches vector length;
- a node does not carry duplicate embeddings for the same model;
- the target vector index accepts the dimensions for that model.

## Vector And Hybrid Recall

Vector APIs:

- `vector_search`
- `vector_search_with_model`
- `vector_search_filtered`
- `search_text`
- `search_text_async`

`VectorFilter` supports kind, created-time bounds, and exact metadata equality.
Metadata equality first uses `mmdb-storage`'s `meta_index` to build a candidate
set, then combines it with the fast `nodes_meta` kind/time path.

`hybrid_search` starts with vector seeds, expands graph neighbors with BFS, and
blends vector score with graph-neighbor contribution using `HybridOpts`.

`recall` additionally runs the persisted BM25 inverted index, weighted RRF,
point-in-time lifecycle filtering, directional causal traversal, provenance,
and contradiction surfacing. It returns `RecallEvidence` rather than an opaque
score.

An optional lawyer profile receives at most 50 candidates and 128 KiB. Its
validated `LawyerVerdict` can gate/rerank recall and stage `ChangeProposal`
records. Only `apply_proposal` can mutate, after revision revalidation.

## Dream And Audit APIs

- `maintain`: deterministic bounded dream batches through `AgentClient`.
- `dream_runs` / `dream_run`: inspect provenance and status.
- `revert_dream`: retract created output and restore superseded derived memory.
- `audit_records` / `inspect_operation`: review append-only sanitized history.
- `graph_slice`: display valid nodes, timestamps, evidence, paths, and weights.

## Graph API

Graph facade methods:

- `add_edge`
- `remove_edge`
- `neighbours_out`
- `neighbours_in`
- `edge_labels`

These methods delegate to `mmdb-graph` while sharing the same fjall keyspace as
node storage.

## Blob API

Blob facade methods:

- `insert_blob`
- `get_blob_stream`
- `get_blob_stream_for`
- `blob_refcount`
- `gc_blobs`

Small blobs can be inlined into `Content::Blob` for direct node reads. The blob
store still tracks a refcount so small and large blob lifecycles share the same
accounting.

## Query API

Query methods:

- `register_query_udf`
- `execute_query`
- `execute_query_physical`
- `execute_query_async`
- `query_optimizer_stats`

`execute_query` runs `LogicalPlan` through the source-backed physical executor.
The facade implements `mmdb_query::QuerySource`, so `Scan`, `VectorSearch`, and
`GraphExpand` bind to real storage/vector/graph operations while higher
operators remain in `mmdb-query`.

Facade-local UDFs are Rust closures registered by name. The WASM runtime lives in
`mmdb-udf`; the query crate only depends on a lightweight closure contract.

## Source Files

- `crates/mmdb/src/db.rs`: facade handle and node/blob/graph/query APIs.
- `crates/mmdb/src/search.rs`: vector filters and hybrid scoring.
- `crates/mmdb/src/runtime.rs`: client traits, profiles, builder, projection work.
- `crates/mmdb/src/lexical.rs`: fjall-backed BM25 index.
- `crates/mmdb/src/recall.rs`: evidence fusion, temporal rules, lawyer proposals.
- `crates/mmdb/src/dream.rs`: validated staged maintenance and reversal.
- `crates/mmdb/src/audit.rs`: append-only sanitized operation journal.
- `crates/mmdb/src/embedder.rs`: embedder trait and database config.
- `crates/mmdb/src/query_impl.rs`: source-backed query bridge and async worker.
- `crates/mmdb/src/convert.rs`: node/query record conversion and predicate
  evaluation.
- `crates/mmdb/src/builder.rs`: `NodeBuilder`.
