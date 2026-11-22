# Memory nodes and indexed reads

Status: shipped

A caller can persist an episode, fact, entity or artifact and retrieve it by tenant and ID, creation time or metadata equality. `mmdb-core` defines the record. `mmdb-storage` writes the base record and its secondary keys together so a replacement removes stale index entries.

## Record format

`NodeKind` classifies memory:

- `Episode`
- `Fact`
- `Entity`
- `Artifact`

`Content` supports:

- `Text(String)`
- `Structured(serde_json::Value)`
- `Blob { hash, size, mime, inline }`

`Embedding` stores model name, dimension, and vector values.

`MemoryNode` is the central durable record:

- `id`
- `tenant`
- `kind`
- `created_at_ms`
- `updated_at_ms`
- `content`
- `embeddings`
- `metadata`
- `revision`
- `state` (`Pending`, `Active`, `Superseded`, or `Retracted`)
- `valid_from_ms`
- `valid_to_ms`

## Encoding and compatibility

- `NodeKind` has stable `u8` discriminants for key encoding.
- Blob content is represented by BLAKE3 hash and size; inline bytes are an
  optimization, not the identity.
- Embedding dimensions are recorded beside vector values so facade and vector
  storage can reject mismatches before persistence.
- Missing lifecycle fields in legacy JSON records default to active, with
  validity beginning at `created_at_ms`; opening a legacy database does not
  rewrite its raw nodes.

## Partitions

| Partition | Key shape | Value |
| --- | --- | --- |
| `nodes` | `[tenant][id]` | encoded `MemoryNode` |
| `nodes_by_time` | `[tenant][created_at_ms][id]` | empty |
| `nodes_by_kind` | `[tenant][kind][created_at_ms][id]` | empty |
| `nodes_meta` | `[tenant][id]` | kind, created, updated |
| `meta_index` | `[tenant][field_hash][value_hash][id]` | original field/value JSON |

Numeric key components are big-endian so range scans sort correctly.

## Node writes

`put_node`:

1. Reads the previous node for the same tenant/id.
2. Removes previous time, kind, and metadata secondary keys.
3. Inserts the encoded node.
4. Inserts current time and kind secondary keys.
5. Inserts compact `nodes_meta`.
6. Inserts metadata equality index entries.
7. Commits one fjall batch and persists with `SyncAll`.

`delete_node` removes the node and all secondary entries in one batch.

## Fast metadata paths

`get_node_meta` reads only kind and timestamps, avoiding full node
deserialization during vector post-filtering.

`node_ids_by_metadata` uses stable hashes for compact range lookup, then checks
the stored original field/value JSON to reject rare hash collisions.

Time and metadata lookups use a single published version for the index lookup and node hydration. Individual reads can therefore proceed during a write. A caller making two separate reads can see two different versions.

## Storage interfaces

`mmdb-core` exposes the following storage types and traits.

- `Snapshot`
- `WriteBatch`
- `KvEngine`
- `SeqNo`
- `TableHandle`

`mmdb-storage` provides the concrete node and index operations.

- `Storage::open`
- `Storage::put_node`
- `Storage::get_node`
- `Storage::scan_by_time`
- `Storage::delete_node`
- `Storage::get_node_meta`
- `Storage::node_ids_by_metadata`

## Source files

- [crates/mmdb-core/src/types.rs](../../crates/mmdb-core/src/types.rs): data model.
- [crates/mmdb-core/src/error.rs](../../crates/mmdb-core/src/error.rs): shared errors.
- [crates/mmdb-core/src/traits.rs](../../crates/mmdb-core/src/traits.rs): storage traits.
- [crates/mmdb-storage/src/engine.rs](../../crates/mmdb-storage/src/engine.rs): storage operations and secondary indexes.
- [crates/mmdb-storage/src/keys.rs](../../crates/mmdb-storage/src/keys.rs): ordered key encoding.
- [crates/mmdb-storage/src/codec.rs](../../crates/mmdb-storage/src/codec.rs): node serialization.
- [crates/mmdb-storage/src/partitions.rs](../../crates/mmdb-storage/src/partitions.rs): partition names.
