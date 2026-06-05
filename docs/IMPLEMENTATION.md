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


---

## 19. Design Decisions Log

### 2026-06-04 — Single-tenant + single-model defaults

**Context.** The initial design exposed `tenant: u32` and per-(tenant, model)
HNSW indices as first-class concepts in the public API. For the actual
target deployment — a single user, single agent, single embedding model —
this added cognitive overhead without benefit.

**Decision.**

1. **Hide `tenant` from the user-facing API**, but keep it in the on-disk key
   encoding (`tenant_be(4)` prefix is unchanged). The facade pins
   `tenant = DEFAULT_TENANT (0)` and forcibly stamps it on every insert.
   Storage format remains forward-compatible with future MVCC branching and
   multi-agent isolation at zero migration cost.
2. **Single default embedding model**, configured at `Database::open_with`.
   The simple path is `db.vector_search(query, k)`. Power users that genuinely
   need multiple embedding spaces (CLIP + text, code-specific embeddings)
   can call `db.vector_search_with_model(model, query, k)`.
3. **`NodeBuilder::new(kind)`** — tenant parameter removed. `Embedding.model`
   field on `MemoryNode` is retained but typically only carries the default
   model name.

**Rationale for the multi-model architecture remaining in the storage layer.**

Even in single-model deployments, the per-(tenant, model) index keying is
preserved because:

- adding a second model later (e.g. CLIP for image artifacts) requires zero
  schema migration;
- different embedding models have incompatible dimensions and distance
  semantics, so they physically cannot share an HNSW graph;
- the cost in the single-model case is one extra `DashMap` entry — negligible.

**Non-goals confirmed for P1.**

- No multi-tenancy isolation primitives in the API (no per-tenant quotas,
  no per-tenant key derivation).
- No reranker pipeline (cross-encoder re-scoring is a P3 concern).
- No automatic embedding-model selection by node kind.

**API surface after this change.**

```rust
let db = Database::open(path)?;                       // defaults
let db = Database::open_with(path, DatabaseConfig {   // custom model
    tenant: 0,
    default_model: "bge-m3".into(),
})?;

let id = db.insert(NodeBuilder::new(NodeKind::Fact).text("...").build())?;
let n  = db.get(id)?;
let xs = db.scan_by_time(0, now_ms(), 50)?;
let hs = db.vector_search(&query, 10)?;
let hs = db.vector_search_with_model("clip-vit-b32", &query, 10)?;
```

---

## 20. P1 Vector Implementation Log (Jun 2026)

This section freezes the design decisions taken when implementing
`mmdb-vector` and integrating it into the `mmdb` facade. Anyone resuming
work after a context wipe can rebuild from this spec alone.

### 20.1 Crate boundary

```
crates/mmdb-vector/
├── src/
│   ├── hit.rs        # ScoredHit { node_id, score, distance }
│   ├── index.rs      # VectorIndex (HNSW wrapper + tombstones)
│   ├── store.rs      # VectorStore (multi-index facade + fjall persistence)
│   └── lib.rs
└── examples/
    └── recall_bench.rs   # 1k×384 random vectors, recall@10 + p50/p99 latency
```

### 20.2 HNSW configuration

| Param | Value | Rationale |
|---|---|---|
| `M` | 16 | hnsw_rs default — good recall/memory trade-off for ≤1M vectors |
| `max_elements` | 1_000_000 | per-(tenant, model) cap; revisit at P2 |
| `max_layer` | 16 | matches M; hnsw_rs convention |
| `ef_construction` | 200 | inserts are bulk anyway; favour graph quality |
| Distance | `DistCosine` | normalise inputs; map distance → similarity below |

`score = (1.0 - distance / 2.0).clamp(0.0, 1.0)` — cosine distance is in [0, 2],
similarity is [0, 1].

### 20.3 fjall persistence schema

Three partitions, all keyed by `[tenant_be(4) | model_hash_be(4) | …]`:

| Partition | Key suffix | Value | Purpose |
|---|---|---|---|
| `vector_meta` | `ulid_be(16)` | `[internal_id_be(8) | dim_be(4) | f32×dim (LE)]` | Source of truth; lets us rebuild HNSW from disk |
| `vector_rev`  | `internal_id_be(8)` | `ulid_bytes(16)` | Reverse lookup after HNSW returns internal_id |
| `vector_tomb` | `internal_id_be(8)` | `[]` (presence marker) | Persist soft-delete bitmap across restarts |

`model_hash` is **FNV-1a 32-bit** of the model name. The model name itself is
not stored on disk; on `open()` we rebuild placeholder indices keyed by
`__h::<mh>`, then promote them lazily the first time a caller uses the real
model name (see §20.4).

### 20.4 Open-time rebuild

`VectorStore::open` after partition init:

1. Scan `vector_meta`, group rows by `(tenant, model_hash)`, decode `(vec, internal_id, dim)`.
2. For each group: `VectorIndex::new(dim)` → `insert_batch(items)` (uses `hnsw_rs::parallel_insert`) → `set_next_id_at_least(max_id)`.
3. Scan `vector_tomb`, group by `(tenant, model_hash)`, load into each index via `load_tombstones`.
4. Store under placeholder key `IndexKey { tenant, model: "__h::<hex>" }`.
5. First subsequent `insert`/`search` for that tenant+model name calls
   `resolve_index` → finds placeholder by hash → re-keys to the real name +
   registers `(tenant, mh) -> name` in `model_names`.

This trades a tiny "first-call rename" cost for avoiding any need to store
model names on disk (which would have required a 4th partition).

### 20.5 Batch insert + id allocation

`VectorStore::insert_batch(tenant, model, items: &[(Ulid, Vec<f32>)])`:

1. Validate uniform `dim` across batch.
2. Allocate `len(items)` internal ids via `VectorIndex::next_internal_id_load_and_inc` (atomic `fetch_add`).
3. Call `VectorIndex::insert_batch` → wraps `hnsw_rs::parallel_insert(&[(&Vec<f32>, usize)])`.
4. Single fjall `batch` writes both `vector_meta` and `vector_rev` for every item, then `persist(SyncAll)`.

Single-item `insert` is now a 1-element batch.

### 20.6 Filtered search

`VectorStore::search_with_filter(..., filter: Option<&HitFilter>)` widens the
HNSW retrieval set when a filter is present:

```
over_fetch = if filter.is_some() { 4 } else { 1 };
widened    = max(k * over_fetch, k);
ef         = max(widened * 4, 32);
```

After HNSW returns `widened` candidates, we apply the predicate per-`Ulid`
and stop once `k` survivors collected. The 4× over-fetch is a heuristic;
revisit if recall regresses on highly-selective filters (P2 may need an
"adaptive widening" pass).

The facade exposes this as `Database::vector_search_filtered(query, k, VectorFilter)`,
where `VectorFilter` AND-combines `kind`, `after_ms`, `before_ms`. Each
candidate triggers one `Storage::get_node(tenant, id)` round-trip — acceptable
at small `k`, but P2 should add a "metadata-only" partition (`nodes_meta` with
just `kind` + `created_at_ms`) so filters can run without deserialising full
node payloads.

### 20.7 Facade integration

`Database` (in `crates/mmdb`) holds both `Storage` and `VectorStore`,
sharing one `Keyspace` (`storage.keyspace.clone()`):

```rust
pub struct Database {
    storage: Storage,
    vector_store: VectorStore,
    config: DatabaseConfig,
}
```

- `insert`: writes node to `Storage`, then for each `Embedding` in
  `node.embeddings` calls `vector_store.insert(tenant, &emb.model, id, &emb.vector)`.
- `delete`: reads node, calls `vector_store.delete(...)` for each embedding's
  model, then `Storage::delete_node`.
- `vector_search(q, k)` → `vector_search_with_model(config.default_model, …)`.
- `vector_search_filtered(q, k, filter)` → see §20.6.

### 20.8 Benchmark baseline

`cargo run --release -p mmdb-vector --example recall_bench` on the dev machine
(macOS, M-series, single workspace target dir):

| Metric | Value |
|---|---|
| Dataset | 1 000 vectors × 384 dim, random Gaussian, L2-normalised |
| Bulk insert (`insert_batch` + 1× fjall commit) | 86 ms (~11.6k vec/s) |
| Recall@10 (vs brute-force ground truth) | **0.902** |
| Query latency p50 / p99 | **220 µs / 374 µs** |

Numbers are reference-grade for the default `(M=16, ef_construction=200)` knobs.
Tuning `ef` upward at query time should push recall toward 0.95+ at the cost
of latency; left as a P2 tuning task.

### 20.9 Known limits and P2 backlog

| Limit | Plan |
|---|---|
| First-call model-name resolution requires that the caller eventually use the same model name as on insert | acceptable for now; alternative is a 4th `vector_models` partition |
| Full node fetch per filter candidate | add `nodes_meta` partition (kind + ts) in P2 |
| No `update_vector(node_id, new_vec)` API | implement as `delete + insert`; consider in-place when HNSW supports it |
| HNSW graph not snapshotted to disk; cold-start cost ~ O(N) inserts | acceptable up to ~100k; P3: `Hnsw::file_dump` snapshot + delta log |
| No per-(tenant, model) capacity guard | `INDEX_DEFAULT_MAX_ELEMENTS = 1_000_000`; bigger tenants need their own knob |
| Filter is a Rust closure, not pushable through HNSW | true post-filter; selective filters lose recall — see §20.6 over-fetch |


---

## §21 P2 设计纪要(2026-06-04)

本节记录 P2 阶段三件落地工作的设计权衡,与 §20 共同构成"向量检索 + 图遍历 + 自动嵌入"的最小可用形态。

### §21.1 `nodes_meta` 副表 — 过滤路径的快进通道

**动机**:`vector_search_filtered` 在 HNSW 召回后需要对每个候选 id 做 kind / 时间窗口的二次裁剪。最早的实现走 `Storage::get_node` → 解 JSON → 取字段,单次解码代价 O(node 大小),对长文本节点尤为浪费。

**Schema**(写在 `mmdb-storage` 的 `nodes_meta` partition):

```
key = [tenant_be(4) | ulid_be(16)]                 // 与 nodes 主表完全同 key
val = [kind_u8 | created_at_ms_be(8) | updated_at_ms_be(8)]  // 17 bytes 固定
```

**一致性**:`put_node` 和 `delete_node` 在同一个 fjall `Batch` 里同时更新 `nodes` 与 `nodes_meta`,保证两者要么都生效要么都回滚,无需读修复。

**facade 使用**:`vector_search_filtered` 把谓词从"取完整节点"换成 `Storage::get_node_meta`,只读 17 字节并直接套用 `VectorFilter::matches_meta(kind_u8, created_at_ms)`。冷数据场景下省去一次大值反序列化。

**未来扩展**:如果以后需要 `metadata` 字段上的过滤,会再加一张 `nodes_tags` 倒排表,而不是把 metadata 塞进 meta 副表 — 后者会把固定长度的优势抹掉。

### §21.2 `mmdb-graph` — 双向边 + 标签桶 + BFS

**Crate 边界**:`mmdb-graph` 只持有 `fjall::Keyspace` 句柄,自己再 `open_partition` 两张表;不依赖 `mmdb-storage` 的 `Storage`。这样 graph 既可独立测试,也可被 facade 嵌入而不会发生循环依赖。

**Schema**(两张 partition,边对称写入):

```
edges_out  key = [tenant(4) | src(16) | label_hash(4) | dst(16)]   val = payload(JSON)
edges_in   key = [tenant(4) | dst(16) | label_hash(4) | src(16)]   val = []    // 只做反向索引
```

* `label_hash` 用 FNV-1a 32-bit。哈希冲突在标签过滤时通过加载 payload 校验 `label` 字段做最终裁定(目前未实现,标签命名空间小时不会撞;若以后开放任意标签,会补一张 `label_dict` 字典表反查原值)。
* `Out` 拿到 payload、`In` 只是一个 marker;`neighbours_in` 需要 payload 时,会拼出对应的 out_key 再读一次。空间换 BFS 时间。

**Direction 枚举**:`Out` / `In` / `Both`。BFS 内部按 direction 分别 range-scan,visited 集合用 `HashSet<Ulid>` 去重,产出顺序按发现顺序(典型 BFS 层序)。

**写一致性**:`add_edge` / `remove_edge` 都通过一个 `Batch` 同时改两侧,确保不会出现"出边有、入边无"的悬挂态。

**测试矩阵**(6 个):add → list / 标签过滤 / 反向边 / remove 双删 / 两跳 BFS / 多租户隔离。

### §21.3 `Embedder` trait — 文本插入零样板

**契约**:

```rust
pub trait Embedder: Send + Sync {
    fn embed(&self, text: &str) -> Result<Vec<f32>>;
    fn model_name(&self) -> &str;
    fn dim(&self) -> u32;
    fn embed_batch(&self, texts: &[&str]) -> Result<Vec<Vec<f32>>> { /* loop default */ }
}
```

**facade 接线**:

* `Database::open_with_embedder(path, config, Box<dyn Embedder>)` 在打开数据库的同时挂载嵌入器。`debug_assert_eq!(embedder.model_name(), config.default_model)` 提醒模型名一致(release 不强制 — 留出多模型混用的口子)。
* `Database::insert` 内联自动嵌入:**只有**在嵌入器存在 ∧ 节点 content 是非空 `Text` ∧ 节点尚未含该 model 的 Embedding 时,才会调用 `embed` 并把结果 push 进 `node.embeddings`。
* `Database::insert_text(kind, text)` 是"只给文本"的快捷入口;未挂载嵌入器时返回 `InvalidArgument` 显式失败,而不是静默写入一个无向量的节点。
* `Database::search_text(query, k)` 用嵌入器把 query 编码后走 `vector_search_with_model(embedder.model_name(), …)`。

**显式优先原则**:用户通过 `NodeBuilder::embedding(model, v)` 主动给的向量永远不会被自动嵌入覆盖;同一节点想存多模型向量时,先 `embedding(...)` 再交给 `insert`,自动路径不会再叠一份。

**为什么不放在 trait object 的 `DatabaseConfig` 里**:`DatabaseConfig` 实现了 `Clone`,而 `Box<dyn Embedder>` 不天然 `Clone`。把嵌入器作为构造函数的单独参数,既避免给 trait 加 `Clone` 约束,也让"无嵌入器纯向量模式"和"挂嵌入器自动模式"在类型层面就是两条不同的入口。

**测试**(3 个):`auto_embeds_text_on_insert`(round-trip)、`explicit_embedding_overrides_auto`(显式优先)、`insert_text_without_embedder_errors`(失败语义)。

### §21.4 当前测试矩阵

| crate | tests | 说明 |
|---|---|---|
| mmdb | 9 | facade(增删查 / 过滤 / 自动嵌入 3 件) |
| mmdb-vector | 9 | HNSW + 持久化 + rebuild + 过滤回调 |
| mmdb-graph | 6 | 双向边 + BFS + 多租户隔离 |
| mmdb-storage | 3 | key 编解码 |

工作区 `cargo test --workspace` 全绿,无 ignored / 无 failed。

### §21.5 P3 候选(尚未启动)

1. **标签字典表 `label_dict`**:把 `label_hash → label_string` 物化,解决 FNV-1a 冲突 + 支持反向枚举。
2. **图 + 向量混合 ranker**:在 `vector_search` 之上叠加"邻居加权"或"K 跳图扩张",形成 GraphRAG 风格的召回。
3. **`metadata` 倒排表**:为 `VectorFilter` 新增任意 key/value 谓词的 sublinear 过滤通路。
4. **Embedder 异步路径**:目前 `embed` 是同步的;接入远程模型时需要一个 `async fn embed_async` 或在 facade 外做缓冲。
5. **`Hnsw::file_dump` 加速冷启动**:rebuild 在 ≥10⁵ 节点后会成为打开瓶颈,届时落地一份原生 dump,把 rebuild 降级成 fallback。

---

## §22 P3-1 图+向量混合 ranker(2026-06-04)

P3 第一刀:把 §20 的纯向量检索和 §21 的图存储拼起来,形成 mmdb 区别于"再造一个 Qdrant"的差异化能力 —— **召回阶段走向量,精排阶段叠图信号**。

### §22.1 算法

```
seeds = vector_search(query, max(seed_k, k))         // 召回阶段
for s in seeds:
    score[s] = alpha * cos_sim(s)                    // 向量贡献
    BFS expand_hops from s along (direction, label):
        score[n] += (1 - alpha) * s.score * decay^hop // 邻居贡献(可累加)
rank by score desc → top k → hydrate as MemoryNode
```

* `alpha` 是向量/图权重的拨杆;`1.0` 退化为纯向量,`0.0` 全靠邻居信号。
* `decay` 控制跳数衰减,典型取 `0.5` 让 2-hop 贡献仅为 1-hop 的一半。
* 同一个邻居被多个 seed 命中时分数 **累加**,反映"被多个高相关节点包围"的强信号。
* BFS 用 `local_visited` 仅在单个 seed 的扩散中去重,避免环;不同 seed 之间允许重复打分,这是有意为之。

### §22.2 facade API

```rust
pub struct HybridOpts {
    pub k: usize,             // 最终返回条数
    pub seed_k: usize,        // 召回阶段拿多少 seed (>= k)
    pub expand_hops: usize,   // BFS 深度,0 退化为纯向量
    pub direction: Direction, // Out / In / Both
    pub label: Option<String>,// 边标签过滤
    pub alpha: f32,           // 向量权重 [0,1]
    pub decay: f32,           // 每跳衰减系数
}
impl Default                 // k=10, seed_k=20, hops=1, Both, alpha=0.7, decay=0.5

impl Database {
    pub fn hybrid_search(&self, query: &[f32], opts: HybridOpts) -> Result<Vec<Hit>>;
}
```

### §22.3 facade 同时新增的图便捷接口

把 `mmdb-graph` 的 `GraphStore` 包进 `Database`,这样调用方不需要自己拿 `Keyspace` 再 `open`:

```rust
impl Database {
    pub fn add_edge(&self, edge: Edge) -> Result<()>;
    pub fn remove_edge(&self, src: Ulid, dst: Ulid, label: &str) -> Result<()>;
    pub fn neighbours_out(&self, node: Ulid, label: Option<&str>) -> Result<Vec<Edge>>;
    pub fn neighbours_in(&self, node: Ulid, label: Option<&str>) -> Result<Vec<Edge>>;
}
```

`GraphStore::open(storage.keyspace.clone())` 让边和节点共用同一个 fjall 实例,日后真要做跨 partition 事务时省心。

### §22.4 测试

* `hybrid_search_promotes_neighbour_via_graph`:三个 fact(A 与 query 同向、B 中等、C 正交)。先验证 **纯向量** 下 C 排在 B 之后;再用 `alpha=0.3, decay=1.0` 跑 hybrid,A→C 这条 `related` 边让 C 拿到 `0.7 * a.score` 的邻居信号,直接超过 B。
* `hybrid_search_alpha_one_equals_vector_only`:边界条件 `alpha=1.0, expand_hops=0` 必须等价于 `vector_search`。

### §22.5 工作区状态(2026-06-04 收尾)

| crate | tests | 备注 |
|---|---|---|
| mmdb | 11 | facade(+2 hybrid_search) |
| mmdb-vector | 9 | |
| mmdb-graph | 6 | |
| mmdb-storage | 3 | |
| **合计** | **29** | `cargo test --workspace` 全绿,`cargo doc --workspace` 0 warning |

`crates/mmdb/examples/` 现在有两份示范:`auto_embed_demo.rs`(自动嵌入端到端)和 `agent_memory.rs`(quickstart),都通过 `cargo run -p mmdb --example` 验证。

### §22.6 P3 余下事项

§21.5 的 5 件事除掉本节已完成的"图+向量混合 ranker",剩下 4 件:

1. `metadata` 倒排表(高优,agent 场景刚需)
2. Embedder 异步路径(接真实远程模型必备)
3. `label_dict` 字典表(标签命名空间变大时再做)
4. `Hnsw::file_dump` 冷启动加速(10⁵ 节点级别再优化)
