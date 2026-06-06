//! Content-addressed blob store.

use mmdb_core::{Error, Result};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fs;
use std::io::{Cursor, Read};
use std::path::{Path, PathBuf};
use std::sync::{Mutex, MutexGuard};

pub const CHUNK_SIZE: usize = 4 * 1024 * 1024;
pub const INLINE_THRESHOLD: usize = 64 * 1024;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlobRef {
    pub hash: [u8; 32],
    pub size: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct BlobMeta {
    refcount: u64,
    size: u64,
    chunked: bool,
}

pub struct BlobStore {
    root: PathBuf,
    meta: Mutex<BTreeMap<String, BlobMeta>>,
}

impl BlobStore {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let root = path.as_ref().to_path_buf();
        fs::create_dir_all(root.join("blobs"))?;
        let meta = if metadata_path(&root).exists() {
            let bytes = fs::read(metadata_path(&root))?;
            serde_json::from_slice(&bytes)?
        } else {
            BTreeMap::new()
        };
        Ok(Self {
            root,
            meta: Mutex::new(meta),
        })
    }

    pub fn put_stream(&self, mut reader: impl Read) -> Result<BlobRef> {
        let mut data = Vec::new();
        reader.read_to_end(&mut data)?;
        let hash = *blake3::hash(&data).as_bytes();
        let key = hex_hash(&hash);

        let mut meta = self.lock_meta()?;
        if let Some(existing) = meta.get_mut(&key) {
            existing.refcount += 1;
            let size = existing.size;
            self.save_metadata(&meta)?;
            return Ok(BlobRef { hash, size });
        }

        let chunked = data.len() > INLINE_THRESHOLD;
        self.write_blob_bytes(&hash, &data, chunked)?;
        meta.insert(
            key,
            BlobMeta {
                refcount: 1,
                size: data.len() as u64,
                chunked,
            },
        );
        self.save_metadata(&meta)?;
        Ok(BlobRef {
            hash,
            size: data.len() as u64,
        })
    }

    pub fn get_stream(&self, hash: &[u8; 32]) -> Result<Box<dyn Read + Send>> {
        let key = hex_hash(hash);
        let meta = self
            .lock_meta()?
            .get(&key)
            .cloned()
            .ok_or(Error::NotFound)?;
        let data = if meta.chunked {
            self.read_chunked(hash, meta.size)?
        } else {
            fs::read(blob_path(&self.root, hash))?
        };
        Ok(Box::new(Cursor::new(data)))
    }

    pub fn inc_ref(&self, hash: &[u8; 32]) -> Result<()> {
        let key = hex_hash(hash);
        let mut meta = self.lock_meta()?;
        let Some(existing) = meta.get_mut(&key) else {
            return Err(Error::NotFound);
        };
        existing.refcount += 1;
        self.save_metadata(&meta)
    }

    pub fn dec_ref(&self, hash: &[u8; 32]) -> Result<()> {
        let key = hex_hash(hash);
        let mut meta = self.lock_meta()?;
        let Some(existing) = meta.get_mut(&key) else {
            return Err(Error::NotFound);
        };
        if existing.refcount > 0 {
            existing.refcount -= 1;
        }
        self.save_metadata(&meta)
    }

    pub fn gc(&self) -> Result<usize> {
        let mut meta = self.lock_meta()?;
        let garbage: Vec<[u8; 32]> = meta
            .iter()
            .filter(|(_, m)| m.refcount == 0)
            .filter_map(|(hash, _)| parse_hex_hash(hash))
            .collect();
        for hash in &garbage {
            remove_blob_bytes(&self.root, hash)?;
            meta.remove(&hex_hash(hash));
        }
        self.save_metadata(&meta)?;
        Ok(garbage.len())
    }

    pub fn refcount(&self, hash: &[u8; 32]) -> Result<Option<u64>> {
        Ok(self.lock_meta()?.get(&hex_hash(hash)).map(|m| m.refcount))
    }

    pub fn is_chunked(&self, hash: &[u8; 32]) -> Result<bool> {
        self.lock_meta()?
            .get(&hex_hash(hash))
            .map(|m| m.chunked)
            .ok_or(Error::NotFound)
    }

    fn lock_meta(&self) -> Result<MutexGuard<'_, BTreeMap<String, BlobMeta>>> {
        self.meta
            .lock()
            .map_err(|_| Error::Storage("blob metadata lock poisoned".into()))
    }

    fn save_metadata(&self, meta: &BTreeMap<String, BlobMeta>) -> Result<()> {
        let path = metadata_path(&self.root);
        let tmp = path.with_extension("json.tmp");
        fs::write(&tmp, serde_json::to_vec_pretty(meta)?)?;
        fs::rename(tmp, path)?;
        Ok(())
    }

    fn write_blob_bytes(&self, hash: &[u8; 32], data: &[u8], chunked: bool) -> Result<()> {
        let path = blob_path(&self.root, hash);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        if chunked {
            fs::create_dir_all(&path)?;
            for (idx, chunk) in data.chunks(CHUNK_SIZE).enumerate() {
                fs::write(path.join(format!("{idx:08}.chunk")), chunk)?;
            }
        } else {
            fs::write(path, data)?;
        }
        Ok(())
    }

    fn read_chunked(&self, hash: &[u8; 32], size: u64) -> Result<Vec<u8>> {
        let dir = blob_path(&self.root, hash);
        let chunk_count = (size as usize).div_ceil(CHUNK_SIZE);
        let mut data = Vec::with_capacity(size as usize);
        for idx in 0..chunk_count {
            data.extend_from_slice(&fs::read(dir.join(format!("{idx:08}.chunk")))?);
        }
        Ok(data)
    }
}

fn metadata_path(root: &Path) -> PathBuf {
    root.join("blobs").join("metadata.json")
}

fn blob_path(root: &Path, hash: &[u8; 32]) -> PathBuf {
    let hex = hex_hash(hash);
    root.join("blobs").join(&hex[..2]).join(hex)
}

fn remove_blob_bytes(root: &Path, hash: &[u8; 32]) -> Result<()> {
    let path = blob_path(root, hash);
    if path.is_dir() {
        fs::remove_dir_all(path)?;
    } else if path.exists() {
        fs::remove_file(path)?;
    }
    Ok(())
}

fn hex_hash(hash: &[u8; 32]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(64);
    for byte in hash {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0f) as usize] as char);
    }
    out
}

fn parse_hex_hash(s: &str) -> Option<[u8; 32]> {
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

#[cfg(test)]
mod tests {
    use super::*;
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

        assert_eq!(a.hash, b.hash);
        assert_eq!(store.refcount(&a.hash).unwrap(), Some(2));

        store.dec_ref(&a.hash).unwrap();
        assert_eq!(store.refcount(&a.hash).unwrap(), Some(1));
        store.dec_ref(&a.hash).unwrap();
        assert_eq!(store.refcount(&a.hash).unwrap(), Some(0));
        store.gc().unwrap();
        assert_eq!(store.refcount(&a.hash).unwrap(), None);
        assert!(store.get_stream(&a.hash).is_err());
    }

    #[test]
    fn large_blob_is_chunked_and_readable_after_reopen() {
        let dir = tempdir().unwrap();
        let data = vec![42_u8; CHUNK_SIZE + 17];
        let hash = {
            let store = BlobStore::open(dir.path()).unwrap();
            let r = store.put_stream(Cursor::new(data.clone())).unwrap();
            assert!(store.is_chunked(&r.hash).unwrap());
            r.hash
        };

        let store = BlobStore::open(dir.path()).unwrap();
        assert_eq!(store.refcount(&hash).unwrap(), Some(1));
        let mut buf = Vec::new();
        store
            .get_stream(&hash)
            .unwrap()
            .read_to_end(&mut buf)
            .unwrap();
        assert_eq!(buf, data);
    }
}
