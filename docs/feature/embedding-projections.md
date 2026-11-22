# Embedding projections

Status: shipped

Store raw memory and project it into one or more embedding model spaces. A durable `MemoryProfile` names clients and public model configuration. The caller supplies the `ClientRegistry` at runtime and owns provider SDKs, authentication, endpoints, timeouts, and retry policy.

With an embedder configured, inserting non-empty text automatically appends an
embedding for the embedder model if the node does not already carry one.

Explicit embeddings win. A node that already has an embedding for the configured
model is not re-embedded.

`Database::ingest` is the multi-model path. It persists the raw memory before
calling any compatible `EmbeddingClient`, records each `ProjectionStatus`, and
keeps failed work retryable through `retry_projection`. Clients receive owned
text/JSON or a reopenable `BlobInput`.

Embedding vectors and vector queries must contain finite values with a finite,
non-zero L2 norm. Invalid vectors are rejected before either exact or HNSW search.
Invalid provider output is recorded as a retryable `ProjectionState::Failed`
without removing the raw node. Every projection status carries the deterministic
full-profile fingerprint; legacy statuses deserialize with an empty fingerprint
and therefore cannot be mistaken for current work after a same-ID profile change.
Vector search rejects result limits above 100,000 and uses saturating, capped
HNSW over-fetch arithmetic, so adversarial limits cannot overflow.

`ingest_blob` applies the same raw-first workflow to bytes placed in the
content-addressed blob store.

The facade validates:

- embedding `dim` matches vector length;
- a node does not carry duplicate embeddings for the same model;
- the target vector index accepts the dimensions for that model.

## Choosing the model

`Database::open_with_embedder` requires the embedder name to match the configured default model. This prevents a text write and its subsequent default search from silently selecting different spaces. The HNSW backend uses cosine distance and rejects profiles that request dot product or Euclidean distance.

Public profiles and per-node projection work live in `memory_profiles_v1` and `projection_status_v1`. The raw node and its lexical projection remain readable if a client fails. See the [runtime and profile implementation](../../crates/mmdb/src/runtime.rs).
