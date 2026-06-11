# mmdb-graph Features

`mmdb-graph` stores labeled edges and provides neighbor scans plus BFS traversal.
It shares the facade's fjall keyspace but owns its own partitions.

## Responsibilities

- Persist directed edges with forward and reverse indexes.
- Support outgoing, incoming, and bidirectional traversal.
- Keep edge label enumeration available per tenant.
- Provide graph expansion used by hybrid search and query execution.

## Partitions

| Partition | Key shape | Value |
| --- | --- | --- |
| `edges_out` | `[tenant][src][label_hash][dst]` | encoded `Edge` |
| `edges_in` | `[tenant][dst][label_hash][src]` | empty |
| `edge_label_dict` | `[tenant][label_hash][label_utf8]` | empty |

Only `edges_out` stores the edge payload. Incoming scans materialize edges by
reading the corresponding outgoing key.

The label dictionary key includes both hash and original label, so hash
collisions remain enumerable.

## Traversal

`Direction` controls BFS expansion:

- `Out`
- `In`
- `Both`

`bfs` returns node ids in discovery order and excludes the seed. It deduplicates
visited nodes during traversal.

## Edge Lifecycle

`add_edge` writes forward edge, reverse marker, and label dictionary entries in
one fjall batch.

`remove_edge` deletes forward and reverse entries in one batch. Labels are
retained in the dictionary after edge deletion so the dictionary can act as a
small namespace of observed labels.

## Public Surface

- `GraphStore::open`
- `GraphStore::add_edge`
- `GraphStore::remove_edge`
- `GraphStore::neighbours_out`
- `GraphStore::neighbours_in`
- `GraphStore::bfs`
- `GraphStore::labels`

## Source Files

- `crates/mmdb-graph/src/lib.rs`: graph store, key encoding, traversal, and
  label dictionary.

