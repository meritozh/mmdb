use super::turns::Numbers;
use super::*;
use crate::records::codec::decode;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
pub(super) struct OrderEntry {
    pub record: ContextRef,
    pub numbers: Numbers,
    pub open_turn: Option<u64>,
}

#[derive(Serialize, Deserialize)]
pub(super) struct ResultIndex {
    pub record: ContextRef,
    pub sequence: u64,
}

#[derive(Serialize, Deserialize)]
pub(super) struct ToolCallIndex {
    pub record: ContextRef,
    pub message_id: Option<String>,
    pub purged: bool,
    pub sequence: u64,
}

pub(super) fn identity_key(owner: &str, session: &str, identity: &str) -> Vec<u8> {
    let mut key = session_key(owner, session);
    segment(&mut key, identity.as_bytes());
    key
}

pub(super) struct View<'a> {
    parts: &'a SessionParts,
    owner: &'a str,
    session: &'a str,
}

impl<'a> View<'a> {
    pub fn load(parts: &'a SessionParts, owner: &'a str, session: &'a str) -> ContextResult<Self> {
        Ok(Self {
            parts,
            owner,
            session,
        })
    }

    fn lookup<T: DeserializeOwned>(
        &self,
        partition: &mmdb_kernel::Partition,
        id: &str,
    ) -> ContextResult<Option<T>> {
        partition
            .get(identity_key(self.owner, self.session, id))?
            .map(|bytes| decode(&bytes))
            .transpose()
    }

    pub fn message(&self, id: &str) -> ContextResult<Option<MessageState>> {
        self.lookup(&self.parts.messages, id)
    }
    pub fn event(&self, id: &str) -> ContextResult<Option<StoredEvent>> {
        self.lookup(&self.parts.events, id)
    }
    pub fn call(&self, id: &str) -> ContextResult<Option<ToolCallIndex>> {
        self.lookup(&self.parts.calls, id)
    }
    pub fn result(&self, id: &str) -> ContextResult<Option<ResultIndex>> {
        self.lookup(&self.parts.results, id)
    }

    pub fn entry(&self, position: u64) -> ContextResult<Option<OrderEntry>> {
        let mut key = session_key(self.owner, self.session);
        key.extend_from_slice(&position.to_be_bytes());
        self.parts
            .order
            .get(key)?
            .map(|bytes| decode(&bytes))
            .transpose()
    }

    pub fn entries_after(
        &self,
        after: u64,
    ) -> impl Iterator<Item = ContextResult<(u64, OrderEntry)>> + '_ {
        let prefix = session_key(self.owner, self.session);
        let mut start = prefix.clone();
        start.extend_from_slice(&after.to_be_bytes());
        let prefix_len = prefix.len();
        scan(self.parts, prefix, start).filter_map(move |entry| {
            let row = entry.and_then(|(key, value)| {
                let suffix = key[prefix_len..]
                    .try_into()
                    .map_err(|_| ContextError::Corrupt("invalid order key".into()))?;
                Ok((u64::from_be_bytes(suffix), value))
            });
            match row {
                Ok((position, _)) if position == after => None,
                other => Some(other),
            }
        })
    }
}

pub(super) fn scan(
    parts: &SessionParts,
    prefix: Vec<u8>,
    start: Vec<u8>,
) -> impl Iterator<Item = ContextResult<(Vec<u8>, OrderEntry)>> + '_ {
    parts
        .order
        .range(start..)
        .take_while(move |row| {
            row.as_ref()
                .map_or(true, |(key, _)| key.starts_with(&prefix))
        })
        .map(|row| {
            let (key, bytes) = row?;
            Ok((key.to_vec(), decode(&bytes)?))
        })
}
