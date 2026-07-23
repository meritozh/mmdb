# mmdb-core Features

`mmdb-core` is the shared type and trait crate. It has no storage or filesystem
dependencies.

## Responsibilities

- Define the canonical memory data model.
- Provide project-wide `Error` and `Result` types.
- Define storage abstraction traits that keep upper layers independent of one
  concrete key-value engine.

## Data Types

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

## KV Traits

The crate exposes:

- `Snapshot`
- `WriteBatch`
- `KvEngine`
- `SeqNo`
- `TableHandle`

These traits model the storage surface needed by higher layers without forcing
every crate to depend directly on `fjall`.

## Invariants

- `NodeKind` has stable `u8` discriminants for key encoding.
- Blob content is represented by BLAKE3 hash and size; inline bytes are an
  optimization, not the identity.
- Embedding dimensions are recorded beside vector values so facade and vector
  storage can reject mismatches before persistence.
- Missing lifecycle fields in legacy JSON records default to active, with
  validity beginning at `created_at_ms`; opening a legacy database does not
  rewrite its raw nodes.

## Source Files

- `crates/mmdb-core/src/types.rs`: data model.
- `crates/mmdb-core/src/error.rs`: shared errors.
- `crates/mmdb-core/src/traits.rs`: storage traits.
