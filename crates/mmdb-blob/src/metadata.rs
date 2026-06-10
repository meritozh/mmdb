//! fjall-backed blob metadata storage.
//!
//! Layout (single partition "m", key is the 64-char hex hash, value is
//! a fixed-size packed binary record):
//!
//! ```text
//! key   : 64-byte ASCII hex of the BLAKE3 hash
//! value : [refcount: u64 (8, BE)] [size: u64 (8, BE)] [chunked: u8 (1)]
//! ```
//!
//! Replaces the previous `Mutex<BTreeMap<String, BlobMeta>>` + JSON file
//! design, which had to re-serialize the entire table on every
//! `inc_ref`/`dec_ref` and had no crash-safety beyond atomic rename.
//!
//! A fjall KV partition gives us:
//! - O(log N) per-key reads and writes
//! - Batchable inc/dec across blob hashes (used by node-insert to atomically
//!   drop old-ref + gain new-ref with a single fsync)
//! - Linear scan for GC (iterate all, filter refcount == 0)
//! - Crash durability via fjall's WAL + MVCC

use fjall::{Config, Keyspace, PartitionCreateOptions, PartitionHandle, PersistMode};
use mmdb_core::{Error, Result};
use std::path::Path;

use crate::fs;

const PARTITION_META: &str = "m";

/// Fixed-size encoded value: 8 (refcount) + 8 (size) + 1 (chunked) = 17 bytes.
const VALUE_SIZE: usize = 8 + 8 + 1;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct BlobMeta {
    pub(crate) refcount: u64,
    pub(crate) size: u64,
    pub(crate) chunked: bool,
}

impl BlobMeta {
    fn encode(&self) -> [u8; VALUE_SIZE] {
        let mut v = [0u8; VALUE_SIZE];
        v[0..8].copy_from_slice(&self.refcount.to_be_bytes());
        v[8..16].copy_from_slice(&self.size.to_be_bytes());
        v[16] = if self.chunked { 1 } else { 0 };
        v
    }

    fn decode(b: &[u8]) -> Option<Self> {
        if b.len() != VALUE_SIZE {
            return None;
        }
        let mut rc = [0u8; 8];
        rc.copy_from_slice(&b[0..8]);
        let mut sz = [0u8; 8];
        sz.copy_from_slice(&b[8..16]);
        Some(Self {
            refcount: u64::from_be_bytes(rc),
            size: u64::from_be_bytes(sz),
            chunked: b[16] != 0,
        })
    }
}

/// Fjall-backed metadata store. Exposes a narrow KV interface keyed on the
/// raw 32-byte BLAKE3 hash. (We hex-encode internally so fjall range scans
/// produce human-readable debug output if needed.)
pub(crate) struct MetaStore {
    keyspace: Keyspace,
    meta: PartitionHandle,
}

impl MetaStore {
    pub(crate) fn open(root: impl AsRef<Path>) -> Result<Self> {
        let meta_dir = fs::metadata_dir(root.as_ref());
        std::fs::create_dir_all(&meta_dir)
            .map_err(|e| Error::Storage(format!("create blob-meta dir: {e}")))?;
        let ks = Config::new(&meta_dir)
            .open()
            .map_err(|e| Error::Storage(format!("open blob-meta keyspace: {e}")))?;
        let meta = ks
            .open_partition(PARTITION_META, PartitionCreateOptions::default())
            .map_err(|e| Error::Storage(format!("open meta partition: {e}")))?;
        Ok(Self { keyspace: ks, meta })
    }

    /// Insert a brand-new blob entry with refcount = 1.
    pub(crate) fn insert_new(&self, hash: &[u8; 32], size: u64, chunked: bool) -> Result<()> {
        let key = fs::hex_hash(hash);
        let val = BlobMeta { refcount: 1, size, chunked }.encode();
        self.meta
            .insert(key, val.as_slice())
            .map_err(|e| Error::Storage(format!("insert meta: {e}")))?;
        self.persist()
    }

    /// Look up a blob entry.
    pub(crate) fn get(&self, hash: &[u8; 32]) -> Result<Option<BlobMeta>> {
        let key = fs::hex_hash(hash);
        Ok(self
            .meta
            .get(&key)
            .map_err(|e| Error::Storage(format!("get meta: {e}")))?
            .and_then(|v| BlobMeta::decode(&v)))
    }

    /// Increment refcount by 1. Returns NotFound if the entry is missing.
    pub(crate) fn inc_ref(&self, hash: &[u8; 32]) -> Result<()> {
        let key = fs::hex_hash(hash);
        let existing = self
            .meta
            .get(&key)
            .map_err(|e| Error::Storage(format!("inc_ref get: {e}")))?
            .ok_or(Error::NotFound)?;
        let mut m = BlobMeta::decode(&existing).ok_or_else(|| {
            Error::Storage(format!("corrupt metadata for {key}"))
        })?;
        m.refcount = m.refcount.saturating_add(1);
        self.meta
            .insert(key, m.encode().as_slice())
            .map_err(|e| Error::Storage(format!("inc_ref insert: {e}")))?;
        self.persist()
    }

    /// Decrement refcount by 1 (saturating at 0). Returns NotFound if missing.
    pub(crate) fn dec_ref(&self, hash: &[u8; 32]) -> Result<()> {
        let key = fs::hex_hash(hash);
        let existing = self
            .meta
            .get(&key)
            .map_err(|e| Error::Storage(format!("dec_ref get: {e}")))?
            .ok_or(Error::NotFound)?;
        let mut m = BlobMeta::decode(&existing).ok_or_else(|| {
            Error::Storage(format!("corrupt metadata for {key}"))
        })?;
        m.refcount = m.refcount.saturating_sub(1);
        self.meta
            .insert(key, m.encode().as_slice())
            .map_err(|e| Error::Storage(format!("dec_ref insert: {e}")))?;
        self.persist()
    }

    /// Iterate all metadata entries (hash, BlobMeta). Used by GC and orphan
    /// reconciliation.
    pub(crate) fn iter_all(&self) -> Result<Vec<([u8; 32], BlobMeta)>> {
        let mut out = Vec::new();
        for kv in self.meta.iter() {
            let (k, v) = kv.map_err(|e| Error::Storage(format!("iter meta: {e}")))?;
            let key_bytes: &[u8] = k.as_ref();
            let Some(key_str) = std::str::from_utf8(key_bytes).ok() else {
                tracing::warn!(key = ?k, "non-utf8 metadata key, skipping");
                continue;
            };
            let Some(hash) = fs::parse_hex_hash(key_str) else {
                tracing::warn!(key = %key_str, "unparsable metadata key, skipping");
                continue;
            };
            let Some(m) = BlobMeta::decode(&v) else {
                tracing::warn!(key = %key_str, "corrupt metadata value, skipping");
                continue;
            };
            out.push((hash, m));
        }
        Ok(out)
    }

    /// Remove a metadata entry by hash. Used by GC after the bytes are
    /// unlinked from disk.
    pub(crate) fn remove(&self, hash: &[u8; 32]) -> Result<()> {
        let key = fs::hex_hash(hash);
        self.meta
            .remove(&key)
            .map_err(|e| Error::Storage(format!("remove meta: {e}")))?;
        self.persist()
    }

    fn persist(&self) -> Result<()> {
        self.keyspace
            .persist(PersistMode::SyncAll)
            .map_err(|e| Error::Storage(format!("persist meta: {e}")))?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn open(temp: &tempfile::TempDir) -> MetaStore {
        MetaStore::open(temp.path()).unwrap()
    }

    #[test]
    fn encode_decode_roundtrip() {
        let m = BlobMeta { refcount: 42, size: 123_456_789, chunked: true };
        let enc = m.encode();
        assert_eq!(BlobMeta::decode(&enc), Some(m));
        assert_eq!(BlobMeta::decode(&[0; 16]), None);
    }

    #[test]
    fn insert_get_inc_dec_remove() {
        let dir = tempdir().unwrap();
        let meta = open(&dir);
        let h = [0xab; 32];
        assert!(meta.get(&h).unwrap().is_none());

        meta.insert_new(&h, 4096, false).unwrap();
        let m = meta.get(&h).unwrap().unwrap();
        assert_eq!(m.refcount, 1);
        assert_eq!(m.size, 4096);
        assert!(!m.chunked);

        meta.inc_ref(&h).unwrap();
        assert_eq!(meta.get(&h).unwrap().unwrap().refcount, 2);
        meta.dec_ref(&h).unwrap();
        meta.dec_ref(&h).unwrap();
        assert_eq!(meta.get(&h).unwrap().unwrap().refcount, 0);
        // saturating sub: stays at 0
        meta.dec_ref(&h).unwrap();
        assert_eq!(meta.get(&h).unwrap().unwrap().refcount, 0);

        meta.remove(&h).unwrap();
        assert!(meta.get(&h).unwrap().is_none());

        // reopen and confirm absence
        drop(meta);
        let meta2 = open(&dir);
        assert!(meta2.get(&h).unwrap().is_none());
    }

    #[test]
    fn inc_ref_missing_is_notfound() {
        let dir = tempdir().unwrap();
        let meta = open(&dir);
        let h = [0x11; 32];
        assert!(matches!(meta.inc_ref(&h), Err(Error::NotFound)));
        assert!(matches!(meta.dec_ref(&h), Err(Error::NotFound)));
    }

    #[test]
    fn iter_all_survives_reopen() {
        let dir = tempdir().unwrap();
        let meta = open(&dir);
        for i in 0..3u8 {
            meta.insert_new(&[i; 32], i as u64, i == 1).unwrap();
        }
        drop(meta);

        let meta2 = open(&dir);
        let all = meta2.iter_all().unwrap();
        assert_eq!(all.len(), 3);
    }
}
