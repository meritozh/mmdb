# mmdb — Multi-Model Database for Agent Memory

> A Rust-native, embedded multi-model database purpose-built as the unified
> persistence layer for AI agent memory. Stores text, vectors, graphs, and
> blobs in a single engine with a hybrid retrieval query language.

**Status**: Design v1.0 · **Language**: Rust 2021 · **License**: TBD

---

## Table of Contents

1. [Vision & Scope](#1-vision--scope)
2. [Design Principles](#2-design-principles)
3. [High-Level Architecture](#3-high-level-architecture)
4. [Data Model](#4-data-model)
5. [Storage Layer (fjall)](#5-storage-layer-fjall)
6. [Vector Index](#6-vector-index)
7. [Graph Model](#7-graph-model)
8. [Blob Store](#8-blob-store)
9. [Query Language (MMQL + Builder API + WASM UDF)](#9-query-language)
10. [Query Execution Engine](#10-query-execution-engine)
11. [Transactions & Concurrency](#11-transactions--concurrency)
12. [Agent-Memory-Specific Features](#12-agent-memory-specific-features)
13. [Project Layout](#13-project-layout)
14. [Implementation Roadmap (P0–P4)](#14-implementation-roadmap)
15. [Testing Strategy](#15-testing-strategy)
16. [Operational Concerns](#16-operational-concerns)
17. [Open Questions](#17-open-questions)
18. [References](#18-references)

---

## 1. Vision & Scope

### 1.1 What mmdb is

A single embedded database (think SQLite/DuckDB, not Postgres) that serves
as the **unified persistent memory layer for AI agents**. One process, one
data directory, four data models (document, vector, graph, blob), one
query language designed for hybrid retrieval.

### 1.2 Why agent memory needs its own database

Agent memory workloads differ from traditional OLTP/OLAP along five axes:

| Axis | Traditional DB | Agent Memory |
|---|---|---|
| Write pattern | High-QPS small txns | Append-mostly, bursty, low QPS |
| Read pattern | Point lookup + range | Semantic recall + graph walk + time window |
| Consistency | Strong | Eventual + snapshot is enough |
| Data shape | Mostly structured | Heterogeneous (chunked text + vectors + edges + attachments) |
| Lifecycle | Permanent | TTL / decay / compression are first-class |

Bolting these onto Postgres + pgvector + a graph extension + S3 produces
operational pain, consistency gaps across systems, and poor hybrid query
optimization. mmdb solves all of this in one engine.

### 1.3 Non-goals

- **Not** a general-purpose OLTP database — no transactions across
  arbitrary user schemas, no SQL completeness
- **Not** distributed from day one — single-node embedded is the focus
- **Not** a vector-only database — vectors are *one* modality among four
- **Not** a server product first — embedded library is primary

---

## 2. Design Principles

1. **One engine, many models.** A single LSM keyspace hosts all modalities.
2. **Node-centric.** Every datum is a `MemoryNode`. Vectors, edges, and
   blobs are projections/attachments of nodes.
3. **Hybrid retrieval is the primary API.** Pure vector, graph, or filter
   are degenerate cases of one unified plan.
4. **Embedded-first.** Default usage is `cargo add mmdb` and open a local
   directory. Server mode is opt-in.
5. **Layered abstractions, single IR.** Builder API, DSL, and WASM UDFs
   all compile to the same LogicalPlan.
6. **MVCC snapshots.** Agents need to "try things" — snapshots enable
   cheap branching, retroactive reads, and time-travel debugging.
7. **Pluggable storage.** The `KvEngine` trait isolates fjall.
8. **No premature distribution.** Single-node first.

---

## 3. High-Level Architecture

```
+--------------------------------------------------------------+
|  Public API                                                  |
|  [Rust Builder API] [MMQL DSL] [WASM UDF] [gRPC/HTTP]        |
+--------------------------------------------------------------+
|  Logical Plan IR (shared)                                    |
|  Optimizer (rule-based + cost-based)                         |
|  Physical Plan (Volcano operators)                           |
+--------------------------------------------------------------+
|  Model Layer                                                 |
|  [Document JSONB] [Vector HNSW] [Graph adj+CSR] [Blob chunk] |
+--------------------------------------------------------------+
|  Catalog & Txn Manager (MVCC snapshots, schema, stats)       |
+--------------------------------------------------------------+
|  Storage Engine                                              |
|  fjall (LSM, partitioned) + mmap vector files                |
|  + content-addressed blob store (BLAKE3, chunked)            |
+--------------------------------------------------------------+
```

---

## 4. Data Model

### 4.1 Core types

```rust
pub struct MemoryNode {
    pub id: Ulid,
    pub kind: NodeKind,
    pub content: Content,
    pub embeddings: SmallVec<[Embedding; 2]>,
    pub metadata: serde_json::Value,
    pub created_at: u64,
    pub valid_from: Option<u64>,
    pub valid_to: Option<u64>,
    pub tenant: u32,
    pub version: u64,
}

pub enum NodeKind { Episode, Fact, Entity, Artifact }

pub enum Content {
    Text(String),
    Blob { hash: [u8; 32], mime: String, size: u64 },
    Structured(serde_json::Value),
    Composite(Vec<ContentRef>),
}

pub struct Embedding { pub model: String, pub dim: u16, pub vector: Vec<f32> }

pub struct Edge {
    pub src: Ulid, pub dst: Ulid,
    pub relation: String, pub weight: f32,
    pub props: serde_json::Value, pub created_at: u64,
}
```

### 4.2 Node lifecycle invariants

- `id` immutable; `version` monotonically increases on update
- Soft delete via `valid_to = now()`; physical delete only via compaction
- Embedding update creates new `version` but keeps `id` stable

### 4.3 Key encoding (LSM-friendly)

| Partition | Key | Value |
|---|---|---|
| `nodes` | `[tenant(4)][id(16)]` | rkyv(MemoryNode) |
| `nodes_by_time` | `[tenant(4)][created_at_be(8)][id(16)]` | empty |
| `nodes_by_kind` | `[tenant(4)][kind(1)][id(16)]` | empty |
| `edges_out` | `[tenant(4)][src(16)][rel_hash(4)][dst(16)]` | rkyv(Edge) |
| `edges_in` | `[tenant(4)][dst(16)][rel_hash(4)][src(16)]` | empty |
| `vector_meta` | `[tenant(4)][model_id(2)][node_id(16)]` | `[vec_offset(8)]` |
| `meta_index` | `[tenant(4)][field_hash(4)][value][node_id(16)]` | empty |
| `blobs` | `[hash(32)]` | rkyv({path, refcount, size}) |
| `udfs` | `[name]` | rkyv({wasm_bytes_hash, signature}) |

All numeric components are **big-endian** to make lexicographic order
match numeric order.

---

## 5. Storage Layer (fjall)

### 5.1 Why fjall

- Pure Rust LSM with WAL, MVCC snapshots, partitioned keyspaces
- Per-partition tuning (compaction, block size, bloom, KV-sep)
- KV separation keeps large MemoryNode payloads out of the index tree
- Cross-partition atomic batches

### 5.2 Partition configuration

| Partition | Compaction | Block | Bloom | KV-sep |
|---|---|---|---|---|
| `nodes` | Leveled | default | default | **on (>=1KB)** |
| `nodes_by_time` | Tiered | 4KB | default | off |
| `nodes_by_kind` | Tiered | 4KB | default | off |
| `edges_out` | Tiered | 4KB | 10 b/key | off |
| `edges_in` | Tiered | 4KB | 10 b/key | off |
| `vector_meta` | Leveled | default | default | off |
| `meta_index` | Leveled | 4KB | 10 b/key | off |
| `blobs` | Leveled | default | default | off |

### 5.3 KvEngine trait

```rust
pub trait KvEngine: Send + Sync + 'static {
    type Snapshot<'a>: Snapshot where Self: 'a;
    type Batch: WriteBatch;
    fn snapshot(&self) -> Result<Self::Snapshot<'_>>;
    fn batch(&self) -> Self::Batch;
    fn apply(&self, batch: Self::Batch) -> Result<SeqNo>;
}
```

### 5.4 Write path

1. `ks.batch()`
2. encode rkyv(node)
3. batch.put on nodes / nodes_by_time / nodes_by_kind
4. for each embedding: append to mmap vector file + put vector_meta + hnsw.insert
5. for each metadata field: put meta_index
6. if Blob content: blob_store.put_or_inc_ref
7. batch.commit() — atomic
8. keyspace.persist(SyncAll) — fsync
9. async hnsw.snapshot_if_needed

HNSW is fully reconstructible from vector_meta + vector file.

---

## 6. Vector Index

- **Library**: `hnsw_rs` (pure Rust)
- **Distance**: cosine default; dot/L2 configurable
- **SIMD**: `simsimd` (AVX2/NEON)
- **Storage**: append-only mmap vector files per (tenant, model)

### 6.3 Pre-filter pushdown

HNSW search accepts `filter: impl Fn(NodeId) -> bool` consulting metadata
indices during traversal. Post-filter only when selectivity > 50%.

### 6.4 Recall amplification

```
effective_k = k * max(1, 1 / estimated_selectivity)
effective_k = min(effective_k, k * 20)
```

### 6.5 Deletes

Tombstone bitset + background rebuild when tombstone ratio > 20%.

---

## 7. Graph Model

Edges stored twice (`edges_out` + `edges_in`) for O(degree) traversal both
directions.

```rust
pub trait GraphView {
    fn out_edges(&self, src: Ulid, relation: Option<&str>) -> EdgeIter;
    fn in_edges(&self, dst: Ulid, relation: Option<&str>) -> EdgeIter;
    fn bfs(&self, start: Ulid, max_depth: u8, filter: EdgeFilter) -> NodeIter;
}
```

Hot subgraph CSR cache invalidated by per-(tenant, relation) version counter.

Deliberately NOT in v1: Cypher patterns, PageRank/community detection, graph schema.

---

## 8. Blob Store

- Content-addressed by BLAKE3
- Small (<64KB) inline in `blobs` partition value
- Large split into 4MB chunks under `<data_dir>/blobs/<hash[0..2]>/<hash>`
- Refcounted; GC reclaims when refcount = 0

```rust
pub trait BlobStore {
    fn put_stream(&self, reader: impl Read) -> Result<BlobRef>;
    fn get_stream(&self, hash: &[u8; 32]) -> Result<impl Read>;
    fn inc_ref(&self, hash: &[u8; 32]) -> Result<()>;
    fn dec_ref(&self, hash: &[u8; 32]) -> Result<()>;
}
```

---

## 9. Query Language

Three layers sharing one LogicalPlan IR.

### 9.1 L1 — Rust Builder API

```rust
let plan = Query::recall()
    .tenant(1)
    .filter(Field::CreatedAt.after(now() - days(7)))
    .filter(Field::Kind.is_in([NodeKind::Episode, NodeKind::Fact]))
    .similar_to(embed("quant backtest"))
        .using_model("text-embedding-3-small").topk(200)
    .connected_from(Query::scan().filter(Field::Meta("name").eq("X")))
        .via("mentions").depth(1)
    .score_by(Score::similarity() * Score::decay(half_life = days(3)))
    .limit(10);
```

### 9.2 L2 — MMQL DSL

```mmql
recall n: Node
  where n.tenant = 1 and n.created_at > now() - 7d
    and n.kind in (Episode, Fact)
  similar to embed("quant backtest") using model "text-embedding-3-small" topk 200
  connected from (u: Node where u.name = "X") via mentions depth 1
  score by similarity * decay(n.created_at, half_life = 3d)
  limit 10
  return n.id, n.content, score
```

Why not SQL: hybrid retrieval requires nested CTEs that hide intent.
Why not Cypher: graph-first buries vector/filter modalities.

### 9.3 L3 — WASM UDFs

Compile custom scoring to WASM; sandboxed by wasmtime with memory/CPU caps.
Not the query entry point — optimizer cannot see inside.

### 9.4 Logical Plan IR

```rust
pub enum LogicalPlan {
    Scan        { table: TableId, filter: Option<Predicate> },
    VectorSearch{ query: VectorRef, k: usize, filter: Option<Predicate>, model: ModelId },
    GraphExpand { from: Box<LogicalPlan>, relation: Option<String>, depth: u8 },
    Filter      { input: Box<LogicalPlan>, pred: Predicate },
    Score       { input: Box<LogicalPlan>, expr: ScoreExpr },
    TopK        { input: Box<LogicalPlan>, k: usize, by: SortKey },
    Join        { left: Box<LogicalPlan>, right: Box<LogicalPlan>, on: JoinKey },
    Project     { input: Box<LogicalPlan>, fields: Vec<FieldRef> },
    Udf         { input: Box<LogicalPlan>, name: String, args: Vec<Expr> },
}
```

---

## 10. Query Execution Engine

### 10.1 Optimizer

1. Rule-based: pre-filter pushdown, k-inflation, filtered traversal,
   join reorder, subquery inlining.
2. Cost-based (when stats available): hash vs merge join, pre vs post
   filter HNSW.

### 10.2 Physical operators (Volcano)

`ScanOp`, `RangeScanOp`, `IndexLookupOp`, `HnswSearchOp`,
`GraphExpandOp`, `FilterOp`, `ProjectOp`, `HashJoinOp`, `MergeJoinOp`,
`TopKOp`, `ScoreOp`, `UdfOp`.

### 10.3 Vectorized batches

`RecordBatch` (~1024 rows, Arrow-inspired) for SIMD-friendly scoring.

### 10.4 Async execution

Top-level `execute` is `async fn` so runtime can interleave queries.

---

## 11. Transactions & Concurrency

- MVCC snapshots for reads
- Single-writer batches for writes (fjall serializes writers)
- Cross-partition atomicity

### 11.3 Branching

Named persistent snapshots enable:
- "before risky action" rollback
- parallel hypothesis exploration
- time-travel debug

---

## 12. Agent-Memory-Specific Features

1. **Time-decay scoring**: built-in `decay(field, half_life)`
2. **Write-time dedup**: cosine > 0.97 + same kind + overlapping source -> merge
3. **Forgetting policy**: `score = recency * importance * access_frequency`,
   physical delete during compaction
4. **Episode -> Fact distillation hook**: `summarize(query) -> Vec<NodeId>`
   callback; distilled facts maintain `derived_from` edges
5. **Multi-tenant isolation**: per-key tenant prefix, per-tenant quotas

---

## 13. Project Layout

```
mmdb/
|-- Cargo.toml                      # workspace
|-- README.md
|-- docs/
|   |-- IMPLEMENTATION.md
|-- crates/
|   |-- mmdb-core/                  # types + traits, no IO
|   |-- mmdb-storage/               # fjall wrapper
|   |-- mmdb-blob/                  # content-addressed blob store
|   |-- mmdb-vector/                # HNSW wrapper + mmap vector file
|   |-- mmdb-graph/                 # edges, traversal, CSR cache
|   |-- mmdb-catalog/               # schema, stats, txn manager
|   |-- mmdb-query/                 # IR + builder + optimizer + exec
|   |-- mmdb-mmql/                  # L2 DSL parser + resolver
|   |-- mmdb-udf/                   # L3 WASM UDF runtime (optional)
|   |-- mmdb/                       # facade (the crate users depend on)
|-- examples/
|-- benches/
|-- tests/
```

### Dependency graph

```
mmdb (facade)
 |-- mmdb-mmql
 |-- mmdb-udf
 |-- mmdb-query --+-- mmdb-vector
                  +-- mmdb-graph
                  +-- mmdb-blob  --+-- mmdb-storage -- mmdb-core
                  +-- mmdb-catalog
```

---

## 14. Implementation Roadmap

### P0 — Skeleton (2-3 weeks)

- Cargo workspace + all crate scaffolds
- mmdb-core: types, error, KvEngine trait
- mmdb-storage: fjall wrapper, all partitions configured
- mmdb-storage: put_node / get_node / scan_by_time / delete_node
- mmdb facade: Database, NodeBuilder
- examples/agent_memory.rs runs
- Unit tests for key encoding
- CI: cargo test + clippy + fmt

**Exit**: Insert 10K nodes, scan by time range, all correct, crash-recovery test passes.

### P1 — Vector + Blob (3-4 weeks)

- mmdb-vector: hnsw_rs, per-(tenant, model) indices
- mmap'd vector file with append + recover
- HNSW snapshot/restore
- mmdb-blob: BLAKE3, chunked, refcount, GC
- mmdb-query minimal: LogicalPlan + Builder API (scan/filter/vector/topk)
- examples/hybrid_recall.rs
- Bench: insert throughput, recall latency

**Exit**: 1M nodes, vector recall@10 p99 < 20ms on laptop SSD.

### P2 — Graph + Hybrid Query (4-6 weeks)

- mmdb-graph: edge writes, out/in iter, 1-2 hop BFS
- Hot subgraph CSR cache + invalidation
- mmdb-query: GraphExpand, Join, full hybrid optimizer
- Pre-filter pushdown into HNSW
- Recall amplification with cardinality stats
- mmdb-mmql: parser (chumsky/winnow), resolver, plan gen

**Exit**: hybrid query (vector top-200 + graph 1-hop + decay + top-10) p99 < 50ms at 1M / 5M edges.

### P3 — Productionization (4-6 weeks)

- Write-time dedup
- Forgetting policy + compaction-time GC
- Branching API (named snapshots, commit-to-branch)
- gRPC server (thin wrapper over facade)
- Python bindings (PyO3)
- Backup / restore
- Observability: tracing, metrics
- Full docs

**Exit**: 24h soak test passes; published as `mmdb` v0.1.

### P4 — Advanced (post v0.1)

- WASM UDF runtime (wasmtime Component Model)
- Cost-based optimizer with histograms
- Vectorized columnar execution end-to-end
- Replicated read replicas
- S3-tiered cold SSTs

---

## 15. Testing Strategy

- **Unit**: per-crate, fast (<100ms each), cover all codec + key encodings
- **Property** (proptest): key encoding round-trips, CRUD invariants,
  optimizer equivalence
- **Integration**: end-to-end scenarios per examples/
- **Crash**: random process kill during writes; verify no corruption
- **Soak**: 24h workload generator (write 10/s, recall 100/s, snapshot 1/min)
- **Benchmarks**: Criterion-based in benches/

---

## 16. Operational Concerns

### 16.1 On-disk layout

```
<data_dir>/
|-- fjall/                  # fjall keyspace (one dir per partition)
|-- vectors/tenant_<id>/model_<id>.vec
|-- hnsw/tenant_<id>/model_<id>.hnsw
|-- blobs/<hash[0..2]>/<hash>
|-- meta/version.toml
```

### 16.2 Crash recovery

- fjall WAL replays committed-but-unflushed batches
- HNSW reconstructed from vector_meta + vector file if snapshot stale
- Blob refcounts re-derived by scanning nodes if blobs partition corrupt

### 16.3 Backup

- Snapshot `<data_dir>` while named MVCC snapshot is held
- Incremental: ship new SSTs + WAL since last backup

### 16.4 Observability

- `tracing` spans on public APIs + operators
- Metrics: per-partition r/w counts, latency histograms, compaction stats,
  HNSW recall estimates, cache hit rates
- `EXPLAIN` returns physical plan with est + actual row counts

### 16.5 Configuration (mmdb.toml)

```toml
[storage]
data_dir = "./agent.mmdb"
persist_mode = "sync_all"   # sync_all | buffer
wal_size_mb = 64

[vector]
default_distance = "cosine"
hnsw_m = 16
hnsw_ef_construction = 200
hnsw_ef_search = 64

[memory]
max_cache_mb = 512
dedup_cosine_threshold = 0.97

[tenant.default]
forgetting_enabled = true
forgetting_half_life_days = 30
forgetting_score_threshold = 0.1
```

---

## 17. Open Questions

1. **Embedding model versioning** — proposal: each Embedding carries
   `model` + `model_version`; re-embedding is a background job
2. **Schema for metadata** — schemaless (current) vs optional schema for
   stronger index types? Inclined toward optional in P2
3. **MMQL aggregation** — group by / count / histogram? Defer to P3
4. **Multi-process access** — fjall is single-process; if needed, gRPC is
   the answer
5. **Vector quantization** — PQ/SQ at 100M+ scale; out of scope for v0.1
6. **Geographic types** — probably not needed for agent memory

---

## 18. References

- fjall: https://github.com/fjall-rs/fjall
- hnsw_rs: https://crates.io/crates/hnsw_rs
- LanceDB: https://lancedb.com/
- qdrant: https://qdrant.tech/
- KuzuDB: https://kuzudb.com/
- SurrealDB: https://surrealdb.com/
- rkyv: https://rkyv.org/
- BLAKE3: https://github.com/BLAKE3-team/BLAKE3
- wasmtime: https://wasmtime.dev/

---

*End of document. Last updated 2026-06-04.*
