# mmdb-storage Features

`mmdb-storage` owns fjall-backed node persistence and node secondary indexes.
It is the source of truth for `MemoryNode` records.

## Responsibilities

- Open the fjall keyspace and node-related partitions.
- Encode and decode `MemoryNode` values.
- Maintain secondary indexes in the same fjall batch as node writes.
- Provide time scans and metadata equality lookup.

## Partitions

| Partition | Key shape | Value |
| --- | --- | --- |
| `nodes` | `[tenant][id]` | encoded `MemoryNode` |
| `nodes_by_time` | `[tenant][created_at_ms][id]` | empty |
| `nodes_by_kind` | `[tenant][kind][created_at_ms][id]` | empty |
| `nodes_meta` | `[tenant][id]` | kind, created, updated |
| `meta_index` | `[tenant][field_hash][value_hash][id]` | original field/value JSON |

Numeric key components are big-endian so range scans sort correctly.

## Node Writes

`put_node`:

1. Reads the previous node for the same tenant/id.
2. Removes previous time, kind, and metadata secondary keys.
3. Inserts the encoded node.
4. Inserts current time and kind secondary keys.
5. Inserts compact `nodes_meta`.
6. Inserts metadata equality index entries.
7. Commits one fjall batch and persists with `SyncAll`.

`delete_node` removes the node and all secondary entries in one batch.

## Fast Metadata Paths

`get_node_meta` reads only kind and timestamps, avoiding full node
deserialization during vector post-filtering.

`node_ids_by_metadata` uses stable hashes for compact range lookup, then checks
the stored original field/value JSON to reject rare hash collisions.

## Public Surface

- `Storage::open`
- `Storage::put_node`
- `Storage::get_node`
- `Storage::scan_by_time`
- `Storage::delete_node`
- `Storage::get_node_meta`
- `Storage::node_ids_by_metadata`

## Source Files

- `crates/mmdb-storage/src/engine.rs`: storage operations and secondary indexes.
- `crates/mmdb-storage/src/keys.rs`: ordered key encoding.
- `crates/mmdb-storage/src/codec.rs`: node serialization.
- `crates/mmdb-storage/src/partitions.rs`: partition names.

