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

impl OrderEntry {
    fn decode(owner: &str, bytes: &[u8]) -> ContextResult<Self> {
        let entry: Self = decode(bytes)?;
        if entry.record.owner != owner || entry.record.kind != RecordKind::Event {
            return Err(ContextError::Corrupt("invalid session order record".into()));
        }
        Ok(entry)
    }
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

#[derive(Clone, Serialize, Deserialize)]
pub(super) struct Segment {
    pub session: String,
    pub upto: u64,
}

pub(super) struct Found<T> {
    pub session: String,
    pub row: T,
}

pub(super) struct Span<'a> {
    pub holder: &'a str,
    pub after: u64,
    pub through: u64,
}

pub(super) struct View<'a> {
    parts: &'a SessionParts,
    owner: &'a str,
    session: &'a str,
    ancestors: Vec<Segment>,
}

impl<'a> View<'a> {
    pub fn load(parts: &'a SessionParts, owner: &'a str, session: &'a str) -> ContextResult<Self> {
        let ancestors = match parts.lineage.get(session_key(owner, session))? {
            None => Vec::new(),
            Some(bytes) => {
                let segments: Vec<Segment> = decode(&bytes)?;
                if segments.is_empty()
                    || segments.len() > 63
                    || segments.iter().any(|s| s.session == session || s.upto == 0)
                    || segments.windows(2).any(|w| w[0].upto >= w[1].upto)
                {
                    return Err(ContextError::Corrupt("invalid session lineage".into()));
                }
                segments
            }
        };
        Ok(Self {
            parts,
            owner,
            session,
            ancestors,
        })
    }

    pub fn spans(&self, position: u64) -> impl Iterator<Item = Span<'_>> + '_ {
        self.ancestors
            .iter()
            .map(|s| (s.session.as_str(), s.upto))
            .chain(std::iter::once((self.session, u64::MAX)))
            .scan(0, move |after, (holder, end)| {
                if *after == position {
                    return None;
                }
                let through = end.min(position);
                let span = Span {
                    holder,
                    after: *after,
                    through,
                };
                *after = through;
                Some(span)
            })
    }

    pub fn holder(&self, position: u64) -> &str {
        self.ancestors
            .get(self.ancestors.partition_point(|s| s.upto < position))
            .map_or(self.session, |s| &s.session)
    }

    pub fn fork_ancestors(&self, position: u64) -> ContextResult<Vec<Segment>> {
        let cut = self.ancestors.partition_point(|s| s.upto < position);
        if cut >= 63 {
            return Err(ContextError::ForkTooDeep);
        }
        let mut segments = self.ancestors[..cut].to_vec();
        segments.push(Segment {
            session: self.holder(position).into(),
            upto: position,
        });
        Ok(segments)
    }

    pub fn fork(&self, session: &'a str, position: u64) -> ContextResult<Self> {
        Ok(Self {
            parts: self.parts,
            owner: self.owner,
            session,
            ancestors: self.fork_ancestors(position)?,
        })
    }

    pub fn ancestors(&self) -> &[Segment] {
        &self.ancestors
    }

    fn lookup<T: DeserializeOwned>(
        &self,
        partition: &mmdb_kernel::Partition,
        id: &str,
        position: impl Fn(&T) -> u64,
    ) -> ContextResult<Option<Found<T>>> {
        let segments = std::iter::once((self.session, u64::MAX)).chain(
            self.ancestors
                .iter()
                .rev()
                .map(|s| (s.session.as_str(), s.upto)),
        );
        for (session, upto) in segments {
            if let Some(bytes) = partition.get(identity_key(self.owner, session, id))? {
                let row = decode(&bytes)?;
                if position(&row) <= upto {
                    return Ok(Some(Found {
                        session: session.into(),
                        row,
                    }));
                }
            }
        }
        Ok(None)
    }

    pub fn message(&self, id: &str) -> ContextResult<Option<Found<MessageState>>> {
        let Some(mut found) =
            self.lookup(&self.parts.messages, id, |m: &MessageState| m.started)?
        else {
            return Ok(None);
        };
        let holder = self.holder(found.row.started);
        if holder != found.session {
            let bytes = self
                .parts
                .messages
                .get(identity_key(self.owner, holder, id))?
                .ok_or_else(|| ContextError::Corrupt("missing original message".into()))?;
            found.row.unavailable = decode::<MessageState>(&bytes)?.unavailable;
        }
        Ok(Some(found))
    }
    pub fn event(&self, id: &str) -> ContextResult<Option<Found<StoredEvent>>> {
        self.lookup(&self.parts.events, id, |e: &StoredEvent| {
            e.receipt.sequence.unwrap_or(0)
        })
    }
    pub fn call(&self, id: &str) -> ContextResult<Option<Found<ToolCallIndex>>> {
        self.lookup(&self.parts.calls, id, |c: &ToolCallIndex| c.sequence)
    }
    pub fn result(&self, id: &str) -> ContextResult<Option<Found<ResultIndex>>> {
        self.lookup(&self.parts.results, id, |r: &ResultIndex| r.sequence)
    }
    pub fn entry(&self, position: u64) -> ContextResult<Option<OrderEntry>> {
        let mut key = session_key(self.owner, self.holder(position));
        key.extend_from_slice(&position.to_be_bytes());
        self.parts
            .order
            .get(key)?
            .map(|bytes| OrderEntry::decode(self.owner, &bytes))
            .transpose()
    }

    pub fn entries_after(
        &self,
        after: u64,
    ) -> impl Iterator<Item = ContextResult<(u64, OrderEntry)>> + '_ {
        self.spans(u64::MAX).flat_map(move |span| {
            let from = after.max(span.after);
            let upto = span.through;
            let prefix = session_key(self.owner, span.holder);
            let len = prefix.len();
            let mut start = prefix.clone();
            start.extend(from.saturating_add(1).to_be_bytes());
            let mut end = prefix;
            end.extend(upto.to_be_bytes());
            (from < upto)
                .then(|| self.parts.order.range(start..=end))
                .into_iter()
                .flatten()
                .map(move |row| {
                    let (key, bytes) = row?;
                    let suffix = key[len..]
                        .try_into()
                        .map_err(|_| ContextError::Corrupt("invalid order key".into()))?;
                    Ok((
                        u64::from_be_bytes(suffix),
                        OrderEntry::decode(self.owner, &bytes)?,
                    ))
                })
        })
    }
}

pub(super) fn scan<'a>(
    parts: &'a SessionParts,
    owner: &'a str,
    prefix: Vec<u8>,
    start: Vec<u8>,
) -> impl Iterator<Item = ContextResult<(Vec<u8>, OrderEntry)>> + 'a {
    parts
        .order
        .range(start..)
        .take_while(move |row| {
            row.as_ref()
                .map_or(true, |(key, _)| key.starts_with(&prefix))
        })
        .map(move |row| {
            let (key, bytes) = row?;
            Ok((key.to_vec(), OrderEntry::decode(owner, &bytes)?))
        })
}
