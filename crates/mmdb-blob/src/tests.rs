//! Unit tests for BlobStore public API.

use super::*;
use crate::fs;
use std::io::{Cursor, Read};
use tempfile::tempdir;

#[test]
fn put_stream_deduplicates_and_refcounts_until_gc() {
    let dir = tempdir().unwrap();
    let store = BlobStore::open(dir.path()).unwrap();
    let a = store
        .put_stream(Cursor::new(b"hello agent memory"))
        .unwrap();
    let b = store
        .put_stream(Cursor::new(b"hello agent memory"))
        .unwrap();

    assert_eq!(a.hash(), b.hash());
    assert_eq!(store.refcount(a.hash()).unwrap(), Some(2));

    store.dec_ref(a.hash()).unwrap();
    assert_eq!(store.refcount(a.hash()).unwrap(), Some(1));
    store.dec_ref(a.hash()).unwrap();
    assert_eq!(store.refcount(a.hash()).unwrap(), Some(0));
    store.gc().unwrap();
    assert_eq!(store.refcount(a.hash()).unwrap(), None);
    assert!(store.get_stream(a.hash()).is_err());
}

#[test]
fn large_blob_is_chunked_and_readable_after_reopen() {
    let dir = tempdir().unwrap();
    let data = vec![42_u8; CHUNK_SIZE + 17];
    let (hash, size) = {
        let store = BlobStore::open(dir.path()).unwrap();
        let out = store.put_stream(Cursor::new(data.clone())).unwrap();
        assert!(store.is_chunked(out.hash()).unwrap());
        match out {
            PutOutcome::OnDisk(r) => (r.hash, r.size),
            PutOutcome::InlinedSmall { .. } => panic!("expected OnDisk for >INLINE_THRESHOLD"),
        }
    };

    let store = BlobStore::open(dir.path()).unwrap();
    assert_eq!(store.refcount(&hash).unwrap(), Some(1));
    assert_eq!(size as usize, data.len());
    let mut buf = Vec::new();
    store
        .get_stream(&hash)
        .unwrap()
        .read_to_end(&mut buf)
        .unwrap();
    assert_eq!(buf, data);
}

#[test]
fn small_blob_put_stream_returns_inlined_small() {
    let dir = tempdir().unwrap();
    let store = BlobStore::open(dir.path()).unwrap();
    let data = b"tiny";
    let out = store.put_stream(Cursor::new(data.as_slice())).unwrap();
    let hash = *out.hash();
    match out {
        PutOutcome::InlinedSmall { r#ref, .. } => {
            assert_eq!(r#ref.size, 4);
        }
        PutOutcome::OnDisk(_) => panic!("expected InlinedSmall"),
    }
    // refcount still tracked uniformly
    assert_eq!(store.refcount(&hash).unwrap(), Some(1));
    // and bytes are also available via get_stream (fs fallback)
    let mut buf = Vec::new();
    store
        .get_stream(&hash)
        .unwrap()
        .read_to_end(&mut buf)
        .unwrap();
    assert_eq!(buf, data);
}

#[test]
fn gc_clears_only_refcount_zero() {
    let dir = tempdir().unwrap();
    let store = BlobStore::open(dir.path()).unwrap();
    let x = store
        .put_stream(Cursor::new(b"x"))
        .unwrap()
        .into_ref();
    let y = store
        .put_stream(Cursor::new(b"y"))
        .unwrap()
        .into_ref();
    store.dec_ref(&x.hash).unwrap();
    assert_eq!(store.total_tracked().unwrap(), 2);
    let removed = store.gc().unwrap();
    assert_eq!(removed, 1);
    assert!(store.refcount(&x.hash).unwrap().is_none());
    assert_eq!(store.refcount(&y.hash).unwrap(), Some(1));
}

#[test]
fn reopen_survives_refcounts() {
    let dir = tempdir().unwrap();
    let hash = {
        let s = BlobStore::open(dir.path()).unwrap();
        let r = s.put_stream(Cursor::new(b"persist me")).unwrap().into_ref();
        s.inc_ref(&r.hash).unwrap();
        assert_eq!(s.refcount(&r.hash).unwrap(), Some(2));
        r.hash
    };
    let s2 = BlobStore::open(dir.path()).unwrap();
    assert_eq!(s2.refcount(&hash).unwrap(), Some(2));
}

#[test]
fn open_with_repair_removes_orphan_on_disk_blob() {
    let dir = tempdir().unwrap();
    // Manually plant an orphan blob on disk (no metadata entry).
    fs::ensure_layout(dir.path()).unwrap();
    let orphan_hash = [0xc0; 32];
    fs::write_blob_bytes(dir.path(), &orphan_hash, b"orphan data").unwrap();
    assert!(fs::blob_exists_on_disk(dir.path(), &orphan_hash));

    // Plain open: warns but does NOT delete.
    {
        let s = BlobStore::open(dir.path()).unwrap();
        drop(s);
    }
    assert!(fs::blob_exists_on_disk(dir.path(), &orphan_hash));

    // Repair open: deletes it.
    {
        let s = BlobStore::open_with_repair(dir.path()).unwrap();
        drop(s);
    }
    assert!(!fs::blob_exists_on_disk(dir.path(), &orphan_hash));
}

#[test]
fn inc_ref_missing_is_notfound() {
    let dir = tempdir().unwrap();
    let s = BlobStore::open(dir.path()).unwrap();
    let h = [0x99; 32];
    assert!(matches!(s.inc_ref(&h), Err(Error::NotFound)));
    assert!(matches!(s.dec_ref(&h), Err(Error::NotFound)));
}

#[test]
fn empty_store_gc_is_noop() {
    let dir = tempdir().unwrap();
    let s = BlobStore::open(dir.path()).unwrap();
    assert_eq!(s.gc().unwrap(), 0);
    assert_eq!(s.total_tracked().unwrap(), 0);
}
