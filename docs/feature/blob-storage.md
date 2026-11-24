# Content-addressed blob storage

Status: shipped

`mmdb-blob` stores binary payloads by BLAKE3 content hash. It deliberately keeps
large bytes outside the node LSM while preserving refcounted lifecycle
semantics.

When blob files exist, opening the store requires a compatible metadata database. A
missing or incompatible database leaves the files intact. With valid metadata,
explicit repair can remove individual files that have no metadata row.

## Responsibilities

- Hash and persist binary payloads.
- Deduplicate identical payloads.
- Track refcounts and blob size/chunking metadata.
- Provide lazy garbage collection.
- Offer an inline-small-payload hint to the facade.

## Layout

```text
<root>/
  blobs/
    <xx>/<64hex>
  blob-meta/
    m/
```

The filesystem stores bytes. The metadata store tracks refcount, size, and
whether the bytes are chunked.

## Thresholds

- `INLINE_THRESHOLD`: payloads at or below 64 KiB can be embedded in the
  `Content::Blob` node variant by the caller.
- `CHUNK_SIZE`: payloads larger than 4 MiB are split into chunks on disk.

Even inline-eligible payloads are tracked in blob metadata, so refcounting stays
uniform.

## Lifecycle

`put_stream` reads all bytes, hashes them, writes new content if needed, and
increments refcount on dedup hits.

`dec_ref` only drops the refcount. `gc` physically removes blobs whose refcount
reached zero.

`open` performs reconciliation warnings for orphaned or dangling metadata.
`open_with_repair` can remove orphaned on-disk blobs.

## Public Surface

- `BlobStore::open`
- `BlobStore::open_with_repair`
- `BlobStore::put_stream`
- `BlobStore::get_stream`
- `BlobStore::inc_ref`
- `BlobStore::dec_ref`
- `BlobStore::gc`
- `BlobStore::refcount`
- `BlobStore::is_chunked`
- `BlobStore::total_tracked`

## Blob-backed nodes

Blob facade methods:

- `insert_blob`
- `get_blob_stream`
- `get_blob_stream_for`
- `blob_refcount`
- `gc_blobs`

Small blobs can be inlined into `Content::Blob` for direct node reads. The blob
store still tracks a refcount so small and large blob lifecycles share the same
accounting.

The stream-shaped API materializes the full payload in memory on both put and get. Disk chunking sets the file layout. It does not bound the working memory of one operation.

## Source Files

- [crates/mmdb-blob/src/lib.rs](../../crates/mmdb-blob/src/lib.rs): public API and lifecycle.
- [crates/mmdb-blob/src/fs.rs](../../crates/mmdb-blob/src/fs.rs): filesystem layout, hashing, chunk IO.
- [crates/mmdb-blob/src/metadata.rs](../../crates/mmdb-blob/src/metadata.rs): metadata/refcount store.
