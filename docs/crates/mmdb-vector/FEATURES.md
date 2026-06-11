# mmdb-vector Features

`mmdb-vector` owns per-tenant, per-model vector indexing and vector persistence.
It wraps HNSW search while keeping a fjall-backed source of truth for rebuilds.

## Responsibilities

- Maintain one `VectorIndex` per `(tenant, model)`.
- Store vector rows, reverse lookup rows, and tombstones in fjall partitions.
- Rebuild HNSW indexes from persisted vector metadata.
- Load native HNSW snapshots when manifests prove they are current.
- Support filtered search by candidate `Ulid`.

## Persistence

| Partition | Key shape | Value |
| --- | --- | --- |
| `vector_meta` | `[tenant][model_hash][node_id]` | internal id, dim, vector |
| `vector_rev` | `[tenant][model_hash][internal_id]` | node id |
| `vector_tomb` | `[tenant][model_hash][internal_id]` | empty tombstone marker |

`vector_meta` is the durable source of truth. HNSW graph dumps are only a
cold-start optimization.

## Model Identity

The persisted key uses a 32-bit FNV-1a hash of the model name. On open, rebuilt
indexes may initially be keyed by a placeholder. The first caller using the real
model name promotes that placeholder to the real `(tenant, model)` key.

This avoids an extra model-name partition while keeping the common single-model
case small.

## Search

`VectorStore::search` returns scored node ids. Cosine distance is mapped to a
similarity score in `[0.0, 1.0]`.

`VectorStore::search_with_filter` over-fetches candidates when a filter closure
is supplied, then keeps candidates whose `Ulid` passes the closure. The facade
uses this to combine HNSW recall with metadata, kind, and time filters.

For small indexes, the store can use exact search instead of HNSW.

## Deletes And Rebuilds

Deletes are persisted as tombstones. Search skips tombstoned internal ids.

On open:

1. Group `vector_meta` rows by tenant/model hash.
2. Load tombstones from `vector_tomb`.
3. Try manifest-backed HNSW reload.
4. Fall back to rebuilding HNSW from vector rows.
5. Replay tombstones.

`flush_snapshots` writes native HNSW graph/data files plus a JSON manifest. The
manifest must cover live rows and tombstone high-water marks before it can be
trusted for reload.

## Public Surface

- `VectorStore::open`
- `VectorStore::open_with_snapshot_dir`
- `VectorStore::insert`
- `VectorStore::insert_batch`
- `VectorStore::delete`
- `VectorStore::search`
- `VectorStore::search_with_filter`
- `VectorStore::flush_snapshots`
- `VectorStore::validate_insert`

## Source Files

- `crates/mmdb-vector/src/store/mod.rs`: store facade and persistence layout.
- `crates/mmdb-vector/src/store/ops.rs`: insert/search/delete operations.
- `crates/mmdb-vector/src/store/snapshot.rs`: HNSW snapshot manifests.
- `crates/mmdb-vector/src/index.rs`: HNSW wrapper and tombstone-aware index.
- `crates/mmdb-vector/src/hit.rs`: scored hit type.

