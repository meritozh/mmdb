use crate::error::Result;

pub trait Snapshot {
    fn get(&self, partition: &str, key: &[u8]) -> Result<Option<Vec<u8>>>;
}

pub trait WriteBatch {
    fn put(&mut self, partition: &str, key: &[u8], value: &[u8]) -> Result<()>;
    fn delete(&mut self, partition: &str, key: &[u8]) -> Result<()>;
}

pub trait KvEngine: Send + Sync {
    type Snap<'a>: Snapshot
    where
        Self: 'a;
    type Batch: WriteBatch;

    fn snapshot(&self) -> Result<Self::Snap<'_>>;
    fn batch(&self) -> Result<Self::Batch>;
    fn commit(&self, batch: Self::Batch) -> Result<()>;
}

pub type SeqNo = u64;

#[derive(Debug, Clone, Copy)]
pub struct TableHandle(pub &'static str);
