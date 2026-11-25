# mmdb — Multi-Model Database for AI Agent Memory

MMDB is an embedded Rust data layer for agents. It persists operational state,
memories, and session logs. A harness can keep its operational state in a separate
directory so workflows and queues survive a memory-store reset.

## Features

- **Embedded** — zero-deployment, single-process, tenant-prefixed storage
- **Multi-model** — text/structured nodes, vector embeddings, edges (graph), blobs
- **fjall-based** — Fjall 3 keyspaces, cross-keyspace MVCC snapshots and separated record payloads
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

The `mmdb::context` contract provides versioned object and relation types,
source-backed records, and bounded lexical and graph recall. The
`mmdb::session` module provides ordered session events and checkpoints. Both modules
share the store lease, write lock and durable batches. An assistant can save validated
context directly, with provenance and an explicit inference label.

Create a fresh root with `mmdb::Store::create(path)` and reopen it with
`mmdb::Store::open(path)`. Use `store.memory()` for native memory or
`store.context(access)` for a `ContextStore`. Use `store.session_log(access)`
for a `SessionLog`. Construct the access profile with
`ContextAccess::new(owner, actor)`. The default scope shares context across
that owner's agents and sessions. The harness supplies trusted identity,
scope and tool permissions.

`TypeDefinition::note(scope)` is an optional starting schema registered through
the same catalog as custom types. Complete raw events stream into checksummed
chunks. `SessionLog::search` uses bounded, resumable substring scans. Objects use
lexical postings and explicit relation paths. One process holds the store lease
at a time. The format is `mmdb-store-v2` on Fjall 3. Opening a Fjall 2 directory reports an
incompatible format. Choose a new directory, or use a confirmed whole-store
reset on a managed root to quarantine the original directory and start empty.
The original MMDB version can open that quarantine.

To reset native memory and domain context in place, call
`store.plan_memory_reset(issued_at, ttl)`, then pass the plan, its full
confirmation digest, and the current time to `store.reset_memory`. The commit
switches the memory era and advances every owner's availability epoch together.
Old memory references return `ContextError::Reset`. Session events, their
receipts, and checkpoint payloads remain available under their existing read
rules. Regenerate pre-reset checkpoints before resuming. When the receipt's
`cleanup_pending` is true, reopening finishes removing the old partitions.

```sh
cargo run -p mmdb --example context_harness
```

The example saves research objects and a relation, reopens the store as a trading
harness, and recalls an indirectly connected decision with its original source.
The same contract now includes streamed message lifecycles, immutable checkpoints,
current dependency validation, and business action definitions with harness-owned
execution records. MiuMiu uses it for continuous windows and dynamic workflows.
See [domain context](docs/feature/context-ontology.md), [session log](docs/feature/session-log.md), [checkpoints](docs/feature/context-checkpoints.md), and [business actions](docs/feature/business-actions.md).

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
│                        └── mmdb-kernel (managed stores)  │
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
| `mmdb` | High-level facade, `NodeBuilder`, vector/graph/hybrid/blob/query APIs | [Embedded multi-model memory](docs/feature/multi-model-memory.md) |
| `mmdb-core` | Shared types, traits, and errors | [Memory nodes](docs/feature/memory-nodes.md) |
| `mmdb-kernel` | Managed store manifests, leases, module eras, and reset | [Operational state](docs/feature/operational-state.md) |
| `mmdb-storage` | fjall node store, key encoding, time/kind/meta indexes | [Node persistence and indexes](docs/feature/memory-nodes.md) |
| `mmdb-vector` | HNSW indexes, vector metadata, tombstones, snapshot reload | [Vector and hybrid search](docs/feature/vector-search.md) |
| `mmdb-graph` | Bi-directional edges, BFS, label dictionary | [Graph traversal](docs/feature/graph-traversal.md) |
| `mmdb-blob` | BLAKE3 content-addressed blob store, chunks, refcounts, GC | [Blob storage](docs/feature/blob-storage.md) |
| `mmdb-catalog` | Embedding model registry, tenant stats, named snapshots | [Memory catalog](docs/feature/memory-catalog.md) |
| `mmdb-query` | `LogicalPlan`, optimizer, batch/source executor, EXPLAIN | [Query execution](docs/feature/query-execution.md) |
| `mmdb-mmql` | MMQL parser, AST, resolver, lowering to `LogicalPlan` | [MMQL](docs/feature/mmql.md) |
| `mmdb-udf` | WASM UDF registry, signatures, sandbox limits, runtime | [WebAssembly UDFs](docs/feature/wasm-udfs.md) |

## Building

```bash
cargo check                               # type-check all crates
cargo test --workspace --locked           # run the pre-commit check
cargo run -p mmdb --example agent_memory  # run quickstart
```

## Documentation

- [Cross-harness context goal](docs/polaris/agent-context.md) and [project glossary](docs/GLOSSARY.md).
- [Operational state](docs/feature/operational-state.md) covers namespaced writes, revision checks, and separate storage roots.
- [Embedding projections](docs/feature/embedding-projections.md) and [evidence recall](docs/feature/evidence-recall.md) describe ingestion, ranking, provenance, and revision-checked proposals.
- [Dream maintenance](docs/feature/dream-maintenance.md) and the [audit journal](docs/feature/memory-audit.md) cover reversible compaction and operation inspection.

## License

Apache-2.0
