# mmdb Architecture

mmdb is a Rust-native embedded multi-model database for AI agent memory. It
stores document-like nodes, vector embeddings, graph edges, and binary blobs
behind one facade while keeping the lower crates independently testable.

This document describes the durable architecture. Crate-specific behavior lives
in the feature docs under [`docs/crates`](crates/).

## Scope

mmdb is built for a single process opening a local data directory, similar in
deployment shape to SQLite or DuckDB. The primary workload is agent memory:
append-heavy writes, semantic recall, graph expansion, time filtering, metadata
filtering, and binary artifacts that belong to remembered events or facts.

The project intentionally does not try to be a distributed database, a complete
SQL engine, or a vector-only service. Vectors are one projection of a
node-centric memory model.

## Design Principles

1. One engine, many models.
2. Every user datum is a `MemoryNode`; vectors, edges, blobs, metadata, and
   query records are projections around that node.
3. Hybrid retrieval is the main API; pure vector search, graph traversal, and
   filtered scans are narrower cases.
4. Embedded-first APIs are the default; any future server mode should remain a
   thin wrapper.
5. Builder APIs, MMQL, and UDF scoring all route through `mmdb-query`
   `LogicalPlan`.
6. Lower crates keep narrow responsibilities so storage, vector, graph, blob,
   parser, and query behavior can be tested without the facade.

## System Shape

```text
+--------------------------------------------------------------+
| mmdb facade                                                  |
| Database, NodeBuilder, search, graph, blob, query bridge     |
+----------------------+----------------------+----------------+
| mmdb-mmql            | mmdb-query           | mmdb-udf       |
| parser + AST         | IR, optimizer, exec  | WASM registry  |
+----------+-----------+----------+-----------+----------------+
| mmdb-vector          | mmdb-graph           | mmdb-blob      |
| HNSW + persistence   | edges + BFS          | BLAKE3 blobs   |
+----------------------+----------+-----------+----------------+
| mmdb-catalog         | mmdb-storage                          |
| model/stats/snapshot | fjall partitions, node indexes        |
+----------------------+---------------------------------------+
| mmdb-core                                                    |
| shared types, errors, KV traits                              |
+--------------------------------------------------------------+
```

The facade crate, `mmdb`, owns user-facing composition. The model crates own
their storage and behavior but share the same `fjall::Keyspace` where that makes
atomic batching and open-time reconstruction practical.

## Data Model

The core unit is `MemoryNode`:

```rust
pub struct MemoryNode {
    pub id: Ulid,
    pub tenant: u32,
    pub kind: NodeKind,
    pub created_at_ms: i64,
    pub updated_at_ms: i64,
    pub content: Content,
    pub embeddings: SmallVec<[Embedding; 1]>,
    pub metadata: BTreeMap<String, serde_json::Value>,
}
```

`NodeKind` has four first-class memory categories:

- `Episode`
- `Fact`
- `Entity`
- `Artifact`

`Content` supports text, structured JSON, and blob references. Blob content may
also carry inline bytes for small payloads.

`Embedding` stores model name, dimension, and vector data. The facade defaults
to one configured embedding model, but the storage/vector layout remains keyed
by tenant and model so multiple embedding spaces can coexist later.

`Edge` links two node ids with a label, weight, timestamp, and metadata. Graph
storage is deliberately edge-oriented; graph facts still point back to nodes.

## Persistence Layout

`mmdb-storage` stores nodes in fjall partitions:

| Partition | Purpose |
| --- | --- |
| `nodes` | encoded `MemoryNode` by tenant and id |
| `nodes_by_time` | time-ordered scan index |
| `nodes_by_kind` | kind/time secondary index |
| `nodes_meta` | compact kind/created/updated row for fast filters |
| `meta_index` | metadata equality inverted index |

All numeric key components are big-endian so lexicographic order matches
numeric order.

`mmdb-vector` adds vector partitions:

| Partition | Purpose |
| --- | --- |
| `vector_meta` | source of truth for vectors and internal ids |
| `vector_rev` | internal id to `Ulid` reverse lookup |
| `vector_tomb` | persisted soft-delete markers |

Native HNSW snapshots are optional cold-start accelerators; `vector_meta`
remains the rebuild source of truth.

`mmdb-graph` adds:

| Partition | Purpose |
| --- | --- |
| `edges_out` | outgoing edge payloads |
| `edges_in` | reverse index markers |
| `edge_label_dict` | tenant-scoped observed labels |

`mmdb-blob` stores bytes outside the node LSM in a BLAKE3-addressed filesystem
layout with a small fjall-backed metadata store for size, chunking, and
refcount state.

## Write Path

`Database::insert` is the main write path:

1. Force the configured tenant onto the node.
2. Auto-embed text when an embedder is configured and the node lacks the
   default-model embedding.
3. Validate embedding dimensions and duplicate model names.
4. Update `mmdb-storage` node and secondary indexes in one fjall batch.
5. If replacing an existing id, tombstone old vector entries and adjust blob
   refcounts.
6. Insert all embeddings into `mmdb-vector`.
7. Update facade catalog stats.

Blob insertion starts in `mmdb-blob`, then creates a blob-backed node. If node
insertion fails, the acquired blob refcount is released.

Graph writes go through `GraphStore::add_edge` or `Database::add_edge`, writing
forward and reverse edge entries in one batch.

## Query Architecture

There are three query entry styles:

- Rust builder APIs in `mmdb-query`.
- MMQL text parsed by `mmdb-mmql`.
- UDF scoring hooks, either facade-local closures or `mmdb-udf` WASM runtime
  calls outside the query crate.

All planner-facing forms lower to `LogicalPlan`:

```rust
pub enum LogicalPlan {
    Scan { table, filter },
    VectorSearch { query, k, filter, model },
    GraphExpand { from, relation, depth },
    Filter { input, pred },
    Score { input, expr },
    TopK { input, k, by },
    Join { left, right, on },
    Aggregate { input, group_by, aggregate },
    Project { input, fields },
    Udf { input, name, args },
}
```

`mmdb-query` stays storage-independent. It exposes an in-memory executor for
semantic tests and a `QuerySource` trait for real scans/searches/graph expansion.
The facade implements `QuerySource` and binds the plan leaves to `Storage`,
`VectorStore`, and `GraphStore`.

`Database::query_optimizer_stats()` returns catalog-derived stats, including
node rows and a `NodeKind` histogram, for `Optimizer::with_stats` and
`Executor::explain`.

## Read And Recall Paths

Common read paths are:

- `get`: tenant-scoped node lookup.
- `scan_by_time`: time-range scan over `nodes_by_time`.
- `vector_search`: default-model HNSW search with node hydration.
- `vector_search_filtered`: HNSW search plus metadata/kind/time filtering.
- `hybrid_search`: vector seed recall, graph BFS expansion, score blending.
- `execute_query`: source-backed physical execution of `LogicalPlan`.
- `execute_query_async`: worker-thread offload for the synchronous source path.

## Crate Feature Docs

| Crate | Feature doc |
| --- | --- |
| `mmdb` | [`docs/crates/mmdb/FEATURES.md`](crates/mmdb/FEATURES.md) |
| `mmdb-core` | [`docs/crates/mmdb-core/FEATURES.md`](crates/mmdb-core/FEATURES.md) |
| `mmdb-storage` | [`docs/crates/mmdb-storage/FEATURES.md`](crates/mmdb-storage/FEATURES.md) |
| `mmdb-vector` | [`docs/crates/mmdb-vector/FEATURES.md`](crates/mmdb-vector/FEATURES.md) |
| `mmdb-graph` | [`docs/crates/mmdb-graph/FEATURES.md`](crates/mmdb-graph/FEATURES.md) |
| `mmdb-blob` | [`docs/crates/mmdb-blob/FEATURES.md`](crates/mmdb-blob/FEATURES.md) |
| `mmdb-catalog` | [`docs/crates/mmdb-catalog/FEATURES.md`](crates/mmdb-catalog/FEATURES.md) |
| `mmdb-query` | [`docs/crates/mmdb-query/FEATURES.md`](crates/mmdb-query/FEATURES.md) |
| `mmdb-mmql` | [`docs/crates/mmdb-mmql/FEATURES.md`](crates/mmdb-mmql/FEATURES.md) |
| `mmdb-udf` | [`docs/crates/mmdb-udf/FEATURES.md`](crates/mmdb-udf/FEATURES.md) |
