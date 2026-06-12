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
- **Hybrid recall** — vector seeds can be reranked with graph-neighbour signal
- **MMQL/IR/executor** — MMQL and builder plans lower into shared `LogicalPlan`

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
cargo run --example agent_memory  # run quickstart
```

## Documentation

- [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) — durable system architecture
- [`docs/crates/`](docs/crates/) — crate-level feature references

## License

Apache-2.0
