//! Filesystem operations for blob byte storage.
//!
//! Handles the raw byte-level on-disk layout:
//!
//! ```text
//! <root>/blobs/
//!   aa/
//!     <64-char hex hash>          — small blobs (<= INLINE_THRESHOLD bytes, one file
//!     <64-char hex hash>/        — large blobs (> INLINE_THRESHOLD bytes, chunked dir)
//!       00000000.chunk
//!       00000001.chunk
//!       ...
//! ```

use mmdb_core::Result;
use std::fs;
use std::io::Read;
use std::path::{Path, PathBuf};

use crate::{CHUNK_SIZE, INLINE_THRESHOLD};

/// Byte-level path for a hash. If chunked, this is a directory; else a file.
pub(crate) fn blob_path(root: &Path, hash: &[u8; 32]) -> PathBuf {
    let hex = hex_hash(hash);
    root.join("blobs").join(&hex[..2]).join(hex)
}

pub(crate) fn hex_hash(hash: &[u8; 32]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(64);
    for byte in hash {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0f) as usize] as char);
    }
    out
}

pub(crate) fn parse_hex_hash(s: &str) -> Option<[u8; 32]> {
    if s.len() != 64 {
        return None;
    }
    let mut out = [0u8; 32];
    for (idx, chunk) in s.as_bytes().chunks_exact(2).enumerate() {
        let hi = hex_nibble(chunk[0])?;
        let lo = hex_nibble(chunk[1])?;
        out[idx] = (hi << 4) | lo;
    }
    Some(out)
}

fn hex_nibble(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(b - b'a' + 10),
        b'A'..=b'F' => Some(b - b'A' + 10),
        _ => None,
    }
}

/// Returns true iff blob was stored chunked.
pub(crate) fn write_blob_bytes(root: &Path, hash: &[u8; 32], data: &[u8]) -> Result<bool> {
    let path = blob_path(root, hash);
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let chunked = data.len() > INLINE_THRESHOLD;
    if chunked {
        fs::create_dir_all(&path)?;
        for (idx, chunk) in data.chunks(CHUNK_SIZE).enumerate() {
            fs::write(path.join(format!("{idx:08}.chunk")), chunk)?;
        }
    } else {
        fs::write(&path, data)?;
    }
    Ok(chunked)
}

/// Read blob bytes back. Must know `chunked` and `size` to avoid a stat.
pub(crate) fn read_blob_bytes(
    root: &Path,
    hash: &[u8; 32],
    chunked: bool,
    size: u64,
) -> Result<Vec<u8>> {
    let path = blob_path(root, hash);
    if chunked {
        let chunk_count = (size as usize).div_ceil(CHUNK_SIZE);
        let mut data = Vec::with_capacity(size as usize);
        for idx in 0..chunk_count {
            data.extend_from_slice(&fs::read(path.join(format!("{idx:08}.chunk")))?);
        }
        Ok(data)
    } else {
        Ok(fs::read(path)?)
    }
}

/// Read blob bytes from a reader, compute hash, return (hash, bytes).
pub(crate) fn hash_and_read(mut reader: impl Read) -> Result<([u8; 32], Vec<u8>)> {
    let mut data = Vec::new();
    reader.read_to_end(&mut data)?;
    let hash = *blake3::hash(&data).as_bytes();
    Ok((hash, data))
}

/// Delete all on-disk bytes for a blob (file or chunked dir).
pub(crate) fn remove_blob_bytes(root: &Path, hash: &[u8; 32]) -> Result<()> {
    let path = blob_path(root, hash);
    if path.is_dir() {
        fs::remove_dir_all(path)?;
    } else if path.exists() {
        fs::remove_file(path)?;
    }
    Ok(())
}

/// True iff the on-disk representation exists (either as file or as chunked directory.
pub(crate) fn blob_exists_on_disk(root: &Path, hash: &[u8; 32]) -> bool {
    blob_path(root, hash).exists()
}

/// Return a list of all on-disk blob hashes (file or chunked) under `root/blobs/*/`.
///
/// Used for orphan reconciliation. Iterates the two-level split dir structure.
pub(crate) fn list_all_blobs_on_disk(root: &Path) -> Result<Vec<[u8; 32]>> {
    let blobs_root = root.join("blobs");
    let mut out = Vec::new();
    if !blobs_root.exists() {
        return Ok(out);
    }
    for prefix_entry in fs::read_dir(blobs_root)? {
        let prefix_entry = prefix_entry?;
        let prefix_path = prefix_entry.path();
        if !prefix_path.is_dir() {
            continue;
        }
        for entry in fs::read_dir(prefix_path)? {
            let entry = entry?;
            let name = entry.file_name();
            let Some(name) = name.to_str() else { continue };
            if let Some(hash) = parse_hex_hash(name) {
                out.push(hash);
            }
        }
    }
    Ok(out)
}

/// Ensure the blobs directory exists.
pub(crate) fn ensure_layout(root: &Path) -> Result<()> {
    fs::create_dir_all(root.join("blobs"))?;
    Ok(())
}

/// Atomic write: ensures directory for fjall metadata lives in <root>/ so it doesn't
/// get mixed up with blob fs data by external callers.
pub(crate) fn metadata_dir(root: &Path) -> PathBuf {
    root.join("blob-meta")
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn hex_roundtrips() {
        let hash = [0x11u8; 32];
        let h = hex_hash(&hash);
        assert_eq!(h.len(), 64);
        assert_eq!(parse_hex_hash(&h), Some(hash));
        assert!(parse_hex_hash("zz").is_none());
    }

    #[test]
    fn small_blob_file_vs_chunked() {
        let dir = tempdir().unwrap();
        ensure_layout(dir.path()).unwrap();
        let small = vec![7u8; 1000];
        let hash = *blake3::hash(&small).as_bytes();
        assert!(!write_blob_bytes(dir.path(), &hash, &small).unwrap());
        assert!(blob_path(dir.path(), &hash).is_file());
        let big = vec![9u8; CHUNK_SIZE + 17];
        let big_hash = *blake3::hash(&big).as_bytes();
        assert!(write_blob_bytes(dir.path(), &big_hash, &big).unwrap());
        assert!(blob_path(dir.path(), &big_hash).is_dir());

        assert_eq!(
            read_blob_bytes(dir.path(), &hash, false, small.len() as u64).unwrap(),
            small
        );
        assert_eq!(
            read_blob_bytes(dir.path(), &big_hash, true, big.len() as u64).unwrap(),
            big
        );

        remove_blob_bytes(dir.path(), &hash).unwrap();
        remove_blob_bytes(dir.path(), &big_hash).unwrap();
        assert!(!blob_exists_on_disk(dir.path(), &hash));
        assert!(!blob_exists_on_disk(dir.path(), &big_hash));
    }

    #[test]
    fn list_all_blobs_enumerates_split_dirs() {
        let dir = tempdir().unwrap();
        ensure_layout(dir.path()).unwrap();
        let a = [0xaa; 32];
        let b = [0xbb; 32];
        write_blob_bytes(dir.path(), &a, &[1; 10]).unwrap();
        write_blob_bytes(dir.path(), &b, &[2; 10]).unwrap();
        let mut list = list_all_blobs_on_disk(dir.path()).unwrap();
        list.sort();
        assert_eq!(list, vec![a, b]);
    }
}
