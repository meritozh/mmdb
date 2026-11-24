# Embedded multi-model memory

Status: shipped

Open a local data directory with `mmdb::Database` to store text, JSON, vectors, edges, and binary artifacts in one process. The handle binds one tenant and one default embedding model. Calls that use a different model name select its own vector space. Agent execution records can use the separate [operational state store](operational-state.md). The [harness context API](context-ontology.md) has its own store format and record identities.

## Open a database

```rust
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let _db = mmdb::Database::open_with(
        directory.path(),
        mmdb::DatabaseConfig {
            tenant: 0,
            default_model: "bge-m3".into(),
        },
    )?;
    Ok(())
}
```

`Database::open` uses tenant 0 and model `default`. `NodeBuilder::new(kind)` creates a node without requiring the caller to repeat the tenant. The facade sets the tenant on every insert. [Node records and indexes](memory-nodes.md) describes the fields, lifecycle defaults and ordered keys.

## Node mutation and recovery

Core node operations:

- `insert`
- `get`
- `scan_by_time`
- `nodes_by_metadata`
- `retract`
- `delete`
- `insert_text`
- `insert_text_async`

`insert` force-stamps the configured tenant on every node. This keeps the public
API simple while retaining the on-disk tenant prefix for tenant-scoped reads.

`retract` is the idempotent soft-forget path: it moves a node to `Retracted` and
closes its current validity interval while preserving historical recall. Optional
revision checks reject stale callers. `delete` remains the lower-level hard node
deletion primitive.

When inserting over an existing id, the facade removes old vector entries,
releases replaced blob references, updates storage indexes, and adjusts catalog
stats for kind changes. The facade persists the affected model names before
changing a node and reconciles unfinished vector updates against the base node
on reopen. A crash can leave a conservative extra blob reference; the recovery
contract does not provide transaction isolation across concurrent vector reads.

`scan_by_time` and `nodes_by_metadata` select index entries and fetch nodes at
the same published storage version. This applies to these individual reads,
not to a sequence of separate API calls.

See the [recovery implementation](../../crates/mmdb/src/db/index_repair.rs) and [public mutation contract](../../crates/mmdb/tests/node_mutation_contract.rs).

## How the stores fit together

The facade composes the node store with [vector search](vector-search.md), [graph traversal](graph-traversal.md), and [blob storage](blob-storage.md). Node, vector and graph indexes use separate keyspaces in one Fjall database. The blob store keeps bytes in hash-addressed files with its own metadata. The [catalog](memory-catalog.md) supplies row estimates and kind counts to [query execution](query-execution.md).

At the facade boundary, [embedding projections](embedding-projections.md) run caller-provided clients. [Evidence recall](evidence-recall.md) combines lexical, vector and relation evidence. [Dream maintenance](dream-maintenance.md) stages derived memories, and the [audit journal](memory-audit.md) records operations.
