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
- **MMQL/IR foundation** — minimal recall parser lowers into shared `LogicalPlan`

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
│                     mmdb  (facade)                        │
├──────────┬──────────┬──────────┬──────────┬──────────────┤
│  vector  │  graph   │   blob   │  query   │   mmql/udf   │
├──────────┴──────────┴──────────┴──────────┴──────────────┤
│                    mmdb-storage (fjall)                   │
├──────────────────────────────────────────────────────────┤
│                      mmdb-core                           │
└──────────────────────────────────────────────────────────┘
```

## Crate Map

| Crate | Status | Description |
|-------|--------|-------------|
| `mmdb-core` | Active | Types, traits, error |
| `mmdb-storage` | Active | fjall KV engine, key encoding, node/meta indexes |
| `mmdb` | Active | High-level facade, NodeBuilder, vector/graph/hybrid/query/stats/source/UDF/thread-backed async APIs |
| `mmdb-blob` | Active | BLAKE3 content-addressed chunked blob store |
| `mmdb-vector` | Active | HNSW index, persistence metadata, manifest-backed snapshot checkpoint/reload |
| `mmdb-graph` | Active | Bi-directional edges, BFS, label dictionary |
| `mmdb-catalog` | Active | Model registry, stats, named snapshots |
| `mmdb-query` | Active | LogicalPlan IR, recall builder, source-backed batch executor, UDF binding, aggregate, join costing/rewrite, instrumented EXPLAIN |
| `mmdb-mmql` | Active | MMQL recall parser with AST/resolver, diagnostics, embed text queries, relative time, boolean where predicates, graph/UDF, score expressions, count, ordered joins, connected subqueries, and return projections |
| `mmdb-udf` | Active | WASM UDF registry, signatures, sandbox limits |

## Building

```bash
cargo check          # type-check all crates
cargo test           # run all tests
cargo run --example agent_memory  # run quickstart
```

## Roadmap

See `docs/IMPLEMENTATION.md` for the full implementation plan (P0–P4).

## License

Apache-2.0
