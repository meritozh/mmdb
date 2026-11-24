# Directed edges and graph traversal

Status: shipped

`mmdb-graph` stores labeled edges and provides neighbor scans plus BFS traversal.
It shares the facade’s Fjall database and owns its edge keyspaces.

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

## Edge records and facade calls

`Edge` stores graph relationships between nodes:

- `src`
- `dst`
- `label`
- `weight`
- `created_at_ms`
- `metadata`
- `revision`
- `valid_from_ms`
- `valid_to_ms`
- `evidence`

Graph facade methods:

- `add_edge`
- `remove_edge`
- `neighbours_out`
- `neighbours_in`
- `edge_labels`

These methods delegate to `mmdb-graph` while sharing the same Fjall database as
node storage.

The graph store accepts caller labels. Agent-produced relations in the evidence and dream APIs use the reserved labels `contains`, `derived_from`, `causes`, `enables`, `prevents`, `contradicts`, and `supersedes`.

## Source Files

- [crates/mmdb-graph/src/lib.rs](../../crates/mmdb-graph/src/lib.rs): graph store, key encoding, traversal, and
  label dictionary.
