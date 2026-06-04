# mmdb — Multi-Model Database for AI Agent Memory

A Rust-native, embedded multi-model database purpose-built as the unified persistence
layer for AI agent memory. Stores text, vectors, graphs, and blobs (images) in a
single engine.

## Features (P0 — current)

- **Embedded** — zero-deployment, single-process, multi-tenant
- **Multi-model** — text/structured nodes, vector embeddings, edges (graph), blobs
- **fjall-based** — LSM storage with partitioned keyspaces, MVCC snapshots, KV separation
- **Time-ordered** — tenant-prefixed, big-endian ULID keys for efficient time-range scans
- **Node-centric data model** — `MemoryNode` with Episode / Fact / Entity / Artifact kinds
- **Builder API** — ergonomic `NodeBuilder` for fluent node construction

## Quick Start

```rust
use mmdb::{Database, NodeBuilder};
use mmdb_core::NodeKind;
use tempfile::tempdir;

let dir = tempdir()?;
let db = Database::open(dir.path())?;

let node = NodeBuilder::new(1, NodeKind::Episode)
    .text("User asked about quarterly revenue.")
    .metadata("session", serde_json::json!("s-001"))
    .build();
let id = db.insert(node)?;

let recent = db.scan_by_time(1, 0, mmdb::now_ms() + 1, 50)?;
db.delete(1, id)?;
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
| `mmdb-core` | P0 | Types, traits, error |
| `mmdb-storage` | P0 | fjall KV engine, key encoding, codec |
| `mmdb` | P0 | High-level facade, NodeBuilder |
| `mmdb-blob` | P1 | BLAKE3 content-addressed chunked blob store |
| `mmdb-vector` | P1 | HNSW index (hnsw_rs + simsimd) |
| `mmdb-graph` | P1 | Bi-directional edges + CSR cache |
| `mmdb-catalog` | P2 | Schema / table catalog |
| `mmdb-query` | P2 | LogicalPlan IR + Volcano executor |
| `mmdb-mmql` | P2 | MMQL DSL parser |
| `mmdb-udf` | P3 | WASM UDF host (wasmtime) |

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
