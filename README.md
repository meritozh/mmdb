# mmdb — Multi-Model Database for AI Agent Memory

A Rust-native, embedded multi-model database purpose-built as the unified persistence
layer for AI agent memory. Stores text, vectors, graphs, and blobs (images) in a
single engine.

## Features

- **Embedded** — zero-deployment, single-process, tenant-prefixed storage
- **Multi-model** — text/structured nodes, vector embeddings, edges (graph), blobs
- **fjall-based** — LSM storage with partitioned keyspaces, MVCC snapshots, KV separation
- **Time-ordered** — tenant-prefixed, big-endian ULID keys for efficient time-range scans
- **Node-centric data model** — `MemoryNode` with Episode / Fact / Entity / Artifact kinds
- **Builder API** — ergonomic `NodeBuilder` for fluent node construction
- **Client-aware projections** — caller-owned embedding and agent clients; credentials never persist
- **Evidence recall** — BM25 + multiple vector models + weighted graph paths with temporal rules
- **Lawyer workflow** — bounded adjudication with explicit, revision-checked change proposals
- **Reversible dreaming** — deterministic batches, cited derived memories, repair, and rollback
- **Auditable** — append-only records for queries, mutations, client calls, compaction, and repair
- **MMQL/IR/executor** — MMQL and builder plans lower into shared `LogicalPlan`

## Harness Context

The new `mmdb::context` contract provides versioned object and relation types,
source-backed records, ordered session history, and bounded lexical and graph
recall on the native memory transaction kernel. An assistant can save validated
context directly, with provenance and an explicit inference label.

Create a fresh root with `native_memory::MemoryDatabase::create_context(path)`,
reopen it with `open_context(path)`, and obtain a `ContextStore` using
`context(ContextAccess::new(owner, actor))`. The default scope shares context
across that owner's agents and sessions. The harness supplies trusted identity,
scope and tool permissions.

`TypeDefinition::note(scope)` is an optional starting schema registered through
the same catalog as custom types. Complete raw events stream into checksummed
chunks; history search uses bounded, resumable substring scans. Objects use
lexical postings and explicit relation paths. This contract currently requires
the fresh `mmdb-context-v1` format; ordinary opening never converts old data.
One process holds the store lease at a time.

```sh
cargo run -p mmdb --example context_harness
```

The example saves research objects and a relation, reopens the store as a trading
harness, and recalls an indirectly connected decision with its original source.
The same contract now includes streamed message lifecycles, immutable checkpoints,
current dependency validation, and business action definitions with harness-owned
execution records. MiuMiu uses it for continuous windows and dynamic workflows.
See the [implementation contract and limits](docs/CONTEXT-DESIGN.md).

## Quick Start

```rust
use mmdb::{Database, NodeBuilder};
use mmdb_core::NodeKind;
use tempfile::tempdir;

let dir = tempdir()?;
let db = Database::open(dir.path())?;

let node = NodeBuilder::new(NodeKind::Episode)
    .text("User asked about quarterly revenue.")
    .metadata("session", serde_json::json!("s-001"))
    .build();
let id = db.insert(node)?;

let recent = db.scan_by_time(0, mmdb::now_ms() + 1, 50)?;
db.delete(id)?;
```

For remote models, use `Database::builder(path)` with a persisted
`MemoryProfile` and a runtime-only `ClientRegistry`. `EmbeddingClient` and
`AgentClient` implementations own provider SDKs, endpoints, credentials,
timeouts, and retries. `Database::ingest`, `Database::recall`, and
`Database::maintain` persist and audit only typed public inputs and outputs.

## Architecture

```
┌──────────────────────────────────────────────────────────┐
│                     mmdb (facade)                        │
├──────────┬──────────┬──────────┬──────────┬──────────────┤
│  vector  │  graph   │   blob   │  query   │   mmql/udf   │
├──────────┴──────────┴──────────┴──────────┴──────────────┤
│                         catalog                          │
├──────────────────────────────────────────────────────────┤
│                    mmdb-storage (fjall)                  │
├──────────────────────────────────────────────────────────┤
│                         mmdb-core                        │
└──────────────────────────────────────────────────────────┘
```

## Crate Map

| Crate | Description | Feature doc |
|-------|-------------|-------------|
| `mmdb` | High-level facade, `NodeBuilder`, vector/graph/hybrid/blob/query APIs | [`FEATURES.md`](docs/crates/mmdb/FEATURES.md) |
| `mmdb-core` | Shared types, traits, and errors | [`FEATURES.md`](docs/crates/mmdb-core/FEATURES.md) |
| `mmdb-storage` | fjall node store, key encoding, time/kind/meta indexes | [`FEATURES.md`](docs/crates/mmdb-storage/FEATURES.md) |
| `mmdb-vector` | HNSW indexes, vector metadata, tombstones, snapshot reload | [`FEATURES.md`](docs/crates/mmdb-vector/FEATURES.md) |
| `mmdb-graph` | Bi-directional edges, BFS, label dictionary | [`FEATURES.md`](docs/crates/mmdb-graph/FEATURES.md) |
| `mmdb-blob` | BLAKE3 content-addressed blob store, chunks, refcounts, GC | [`FEATURES.md`](docs/crates/mmdb-blob/FEATURES.md) |
| `mmdb-catalog` | Embedding model registry, tenant stats, named snapshots | [`FEATURES.md`](docs/crates/mmdb-catalog/FEATURES.md) |
| `mmdb-query` | `LogicalPlan`, optimizer, batch/source executor, EXPLAIN | [`FEATURES.md`](docs/crates/mmdb-query/FEATURES.md) |
| `mmdb-mmql` | MMQL parser, AST, resolver, lowering to `LogicalPlan` | [`FEATURES.md`](docs/crates/mmdb-mmql/FEATURES.md) |
| `mmdb-udf` | WASM UDF registry, signatures, sandbox limits, runtime | [`FEATURES.md`](docs/crates/mmdb-udf/FEATURES.md) |

## Building

```bash
cargo check          # type-check all crates
cargo test           # run all tests
cargo run -p mmdb --example agent_memory  # run quickstart
```

## Documentation

- [`docs/FEATURES.md`](docs/FEATURES.md): agent context ontology product baseline (Chinese)
- [`docs/CONTEXT-DESIGN.md`](docs/CONTEXT-DESIGN.md): context implementation, public contract, limits and stage verification (Chinese)
- [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) — durable system architecture
- [`docs/crates/`](docs/crates/) — crate-level feature references

## License

Apache-2.0
