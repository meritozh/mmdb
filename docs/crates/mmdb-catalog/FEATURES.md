# mmdb-catalog Features

`mmdb-catalog` owns lightweight registry and statistics state used by the
facade and query optimizer.

## Responsibilities

- Register embedding model definitions.
- Track per-tenant node statistics.
- Store named snapshots.
- Provide data that the facade can translate into query optimizer stats.

## Embedding Models

`EmbeddingModel` records:

- name
- dimension
- distance metric

`Catalog::register_model` is idempotent for identical definitions and rejects
same-name registrations with different dimensions or metrics.

## Tenant Stats

`TenantStats` tracks:

- `total_nodes`
- `nodes_by_kind`
- `edge_count`
- `blob_count`

The current facade updates node totals and per-kind counts on insert, delete,
and same-id replacement. `Database::query_optimizer_stats()` maps those stats to
`mmdb-query::Stats`, including a `FieldRef::Kind` histogram.

## Named Snapshots

`NamedSnapshot` maps a user-facing snapshot name to a sequence number and
creation timestamp.

The catalog enforces unique snapshot names.

## Public Surface

- `Catalog::register_model`
- `Catalog::model`
- `Catalog::record_node_insert`
- `Catalog::record_node_delete`
- `Catalog::tenant_stats`
- `Catalog::create_snapshot`
- `Catalog::snapshot`
- `Catalog::snapshot_names`

## Source Files

- `crates/mmdb-catalog/src/lib.rs`: model registry, stats, and snapshots.

