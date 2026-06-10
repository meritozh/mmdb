//! Content-addressed blob store.
//!
//! This crate is the storage backend for large binary payloads (images,
//! documents, logs) in mmdb. It is deliberately layered *separately* from
//! the fjall-backed `mmdb-storage` LSM engine — see crate-level docs on
//! why: LSM trees with KV separation are the wrong primitive for MB-range,
//! write-once, content-hashed data.
//!
//! # Layout
//!
//! ```text
//! <root>/
//!   blobs/            # blob bytes on the filesystem
//!     <xx>/<64hex>    # one file per small blob, chunked directory per large
//!   blob-meta/        # fjall keyspace with a single partition `m`
//!     m/              # per-hash refcount + size + chunked flag
//! ```
//!
//! # Semantics
//!
//! - **Dedup**: inserting the same bytes twice returns the same `BlobRef`
//!   (same BLAKE3 hash) and bumps the refcount to 2.
//! - **Lazy release**: `dec_ref` only drops the refcount; actual byte
//!   deletion happens in `gc()`.
//! - **Small-inline hint**: `put_stream` now returns a `PutOutcome` which
//!   tells the caller whether the payload should be stored inline inside
//!   the node's Content (small values, ≤ `INLINE_THRESHOLD`) instead of
//!   relying on the blob fs for the byte storage. The metadata entry is
//!   *still created* so that reference-count accounting stays uniform
//!   across both paths; callers are free to skip the inline-hint branch
//!   and fall back to fs storage.

mod fs;
mod metadata;

/// Hex-encode a 32-byte BLAKE3 hash as a 64-character lowercase string.
pub fn hex_hash(hash: &[u8; 32]) -> String {
    fs::hex_hash(hash)
}

use std::io::{Cursor, Read};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use mmdb_core::{Error, Result};

use crate::metadata::{BlobMeta, MetaStore};

/// Blobs larger than this are split into 4 MiB chunks on disk.
pub const CHUNK_SIZE: usize = 4 * 1024 * 1024;

/// Blobs ≤ this threshold are small enough to be stored inline inside
/// the fjall `nodes` partition by the caller. They *can* also live on
/// the filesystem; this is purely a performance hint.
pub const INLINE_THRESHOLD: usize = 64 * 1024;

/// A content-addressed reference to stored bytes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlobRef {
    pub hash: [u8; 32],
    pub size: u64,
}

/// Outcome of [`BlobStore::put_stream`]. Callers decide how to persist
/// the reference. For `InlinedSmall` the payload bytes are returned so
/// the caller can embed them directly into a node's `Content::Blob`
/// variant; for `OnDisk` the bytes live on the fs and only the ref is
/// needed.
pub enum PutOutcome {
    /// Payload is small enough to inline. Bytes are returned.
    /// A metadata entry (refcount = 1) is still created so refcount
    /// accounting is uniform regardless of where bytes live.
    InlinedSmall {
        r#ref: BlobRef,
        bytes: Vec<u8>,
    },
    /// Payload was written to the filesystem (possibly chunked).
    OnDisk(BlobRef),
}

impl PutOutcome {
    pub fn into_ref(self) -> BlobRef {
        match self {
            PutOutcome::InlinedSmall { r#ref, .. } => r#ref,
            PutOutcome::OnDisk(r#ref) => r#ref,
        }
    }

    pub fn hash(&self) -> &[u8; 32] {
        match self {
            PutOutcome::InlinedSmall { r#ref, .. } => &r#ref.hash,
            PutOutcome::OnDisk(r#ref) => &r#ref.hash,
        }
    }

    pub fn size(&self) -> u64 {
        match self {
            PutOutcome::InlinedSmall { r#ref, .. } => r#ref.size,
            PutOutcome::OnDisk(r#ref) => r#ref.size,
        }
    }
}

/// Public API of the blob store.
pub struct BlobStore {
    root: PathBuf,
    meta: Arc<MetaStore>,
}

impl BlobStore {
    // ------------------------------------------------------------------
    // Constructors
    // ------------------------------------------------------------------

    /// Open (or create) a blob store rooted at `path`.
    ///
    /// On open, performs a lightweight consistency check: warns (via
    /// `tracing`) about on-disk blobs missing from metadata (orphans)
    /// and metadata entries missing on disk (dangling). No automatic
    /// repair is performed; callers that want aggressive clean-up can
    /// follow up with `repair_remove_orphans`.
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        Self::open_with(path, false)
    }

    /// Same as [`Self::open`] but also physically deletes any on-disk
    /// blob that has no matching metadata entry (orphans). This is the
    /// "repair" open — use it when you expect crash-recovery cleanup.
    pub fn open_with_repair(path: impl AsRef<Path>) -> Result<Self> {
        Self::open_with(path, true)
    }

    fn open_with(path: impl AsRef<Path>, repair: bool) -> Result<Self> {
        let root = path.as_ref().to_path_buf();
        fs::ensure_layout(&root)?;
        let meta = Arc::new(MetaStore::open(&root)?);
        let store = Self { root, meta };
        store.reconcile_on_open(repair)?;
        Ok(store)
    }

    // ------------------------------------------------------------------
    // Public API
    // ------------------------------------------------------------------

    /// Read bytes from `reader`, hash them, and persist them.
    ///
    /// Returns a [`PutOutcome`] so callers can decide whether to store
    /// bytes inline (≤ [`INLINE_THRESHOLD`]) or rely on the fs path.
    ///
    /// Deduplication: if the hash already exists the refcount is bumped
    /// and no bytes are rewritten. The returned `PutOutcome` matches the
    /// *current* payload size's hint even on dedup hits (i.e. a re-insert
    /// of a 10-byte blob always returns `InlinedSmall` regardless of
    /// whether the original caller inlined or stored on disk).
    pub fn put_stream(&self, mut reader: impl Read) -> Result<PutOutcome> {
        let (hash, bytes) = fs::hash_and_read(&mut reader)?;
        let size = bytes.len() as u64;

        // Fast path: already tracked?
        if let Some(existing) = self.meta.get(&hash)? {
            // Bump refcount; size on the record is authoritative.
            self.meta.inc_ref(&hash)?;
            let r#ref = BlobRef { hash, size: existing.size };
            return Ok(mk_outcome(r#ref, bytes));
        }

        // New blob: write bytes to fs (even if small — keeps refcount
        // semantics uniform, and bytes can still be inlined by the caller
        // from the returned PutOutcome).
        let chunked = fs::write_blob_bytes(&self.root, &hash, &bytes)?;
        self.meta.insert_new(&hash, size, chunked)?;
        let r#ref = BlobRef { hash, size };
        Ok(mk_outcome(r#ref, bytes))
    }

    /// Read bytes for a hash. Returns a boxed reader for parity with
    /// the previous API; the current implementation materialises into
    /// memory, but preserving the trait object keeps us forward-compatible
    /// with a future zero-copy mmap implementation.
    pub fn get_stream(&self, hash: &[u8; 32]) -> Result<Box<dyn Read + Send>> {
        let BlobMeta { chunked, size, .. } = self
            .meta
            .get(hash)?
            .ok_or(Error::NotFound)?;
        let data = fs::read_blob_bytes(&self.root, hash, chunked, size)?;
        Ok(Box::new(Cursor::new(data)))
    }

    pub fn inc_ref(&self, hash: &[u8; 32]) -> Result<()> {
        self.meta.inc_ref(hash)
    }

    pub fn dec_ref(&self, hash: &[u8; 32]) -> Result<()> {
        self.meta.dec_ref(hash)
    }

    /// Remove all on-disk blobs whose refcount is 0. Returns the count
    /// of removed blobs.
    pub fn gc(&self) -> Result<usize> {
        let garbage: Vec<[u8; 32]> = self
            .meta
            .iter_all()?
            .into_iter()
            .filter(|(_, m)| m.refcount == 0)
            .map(|(h, _)| h)
            .collect();
        for hash in &garbage {
            fs::remove_blob_bytes(&self.root, hash)?;
            self.meta.remove(hash)?;
        }
        Ok(garbage.len())
    }

    pub fn refcount(&self, hash: &[u8; 32]) -> Result<Option<u64>> {
        Ok(self.meta.get(hash)?.map(|m| m.refcount))
    }

    pub fn is_chunked(&self, hash: &[u8; 32]) -> Result<bool> {
        self.meta
            .get(hash)?
            .map(|m| m.chunked)
            .ok_or(Error::NotFound)
    }

    /// Number of metadata entries (useful for tests / telemetry).
    pub fn total_tracked(&self) -> Result<usize> {
        Ok(self.meta.iter_all()?.len())
    }

    // ------------------------------------------------------------------
    // Open-time reconciliation
    // ------------------------------------------------------------------

    fn reconcile_on_open(&self, repair: bool) -> Result<()> {
        let on_disk = fs::list_all_blobs_on_disk(&self.root)?;
        let all_meta = self.meta.iter_all()?;
        let mut in_meta = std::collections::HashSet::with_capacity(all_meta.len());
        // 1) warn for dangling refs (metadata present, no on-disk bytes)
        for (hash, m) in &all_meta {
            in_meta.insert(*hash);
            if m.size > 0 && !fs::blob_exists_on_disk(&self.root, hash) {
                tracing::warn!(
                    hash = %fs::hex_hash(hash),
                    refcount = m.refcount,
                    "blob metadata entry has no corresponding on-disk bytes (dangling ref)"
                );
            }
        }
        // 2) warn/repair for orphans (on disk but no metadata)
        let mut orphan_count = 0;
        for hash in on_disk {
            if in_meta.contains(&hash) {
                continue;
            }
            orphan_count += 1;
            tracing::warn!(
                hash = %fs::hex_hash(&hash),
                repair,
                "on-disk blob has no metadata entry (orphan)"
            );
            if repair {
                if let Err(e) = fs::remove_blob_bytes(&self.root, &hash) {
                    tracing::error!(
                        hash = %fs::hex_hash(&hash),
                        error = %e,
                        "failed to repair orphan blob"
                    );
                }
            }
        }
        if orphan_count > 0 {
            tracing::info!(orphan_count, repair, "open-time orphan reconciliation done");
        }
        Ok(())
    }
}

fn mk_outcome(r#ref: BlobRef, bytes: Vec<u8>) -> PutOutcome {
    if bytes.len() <= INLINE_THRESHOLD {
        PutOutcome::InlinedSmall { r#ref, bytes }
    } else {
        PutOutcome::OnDisk(r#ref)
    }
}

// Tests live in tests.rs
#[cfg(test)]
mod tests;
