use super::{catalog::SessionHead, coverage::Coverage, view::View, *};
use crate::records::{
    chunks::ChunkSpace,
    codec::{decode, encode, validate_name},
};
use serde::{Deserialize, Serialize};
use std::{io::Read, ops::Bound};
use ulid::Ulid;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConversationCacheInput {
    pub session: String,
    pub derivation: String,
    /// Last consecutive event applied to the conversation.
    pub position: u64,
    pub media_type: String,
    /// Epoch read before deriving this conversation from the cache or log.
    pub expected_availability_epoch: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConversationCache {
    pub session: String,
    pub derivation: String,
    pub version: Ulid,
    /// Last consecutive event applied to the conversation.
    pub position: u64,
    pub media_type: String,
    pub bytes: u64,
    pub digest: [u8; 32],
    pub saved_at_ms: i64,
    pub provenance: Provenance,
    pub stale: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConversationCachePage {
    pub caches: Vec<ConversationCache>,
    /// Pass this derivation as the exclusive cursor for the next readable page.
    pub next: Option<String>,
}

#[derive(Serialize, Deserialize)]
struct CacheRow {
    version: Ulid,
    position: u64,
    media_type: String,
    bytes: u64,
    digest: [u8; 32],
    saved_at_ms: i64,
    provenance: Provenance,
    epoch: u64,
    scopes: Vec<Scope>,
    reclaimed: bool,
}

#[derive(Serialize, Deserialize)]
struct PendingCache {
    session: String,
    derivation: String,
}

impl CacheRow {
    fn describe(self, session: &str, derivation: &str, stale: bool) -> ConversationCache {
        ConversationCache {
            session: session.into(),
            derivation: derivation.into(),
            version: self.version,
            position: self.position,
            media_type: self.media_type,
            bytes: self.bytes,
            digest: self.digest,
            saved_at_ms: self.saved_at_ms,
            provenance: self.provenance,
            stale,
        }
    }

    fn stale(&self, coverage: &Coverage<'_>, view: &View<'_>) -> ContextResult<bool> {
        Ok(self.reclaimed || coverage.withdrawn_since(view, self.position, self.epoch)?)
    }
}

impl SessionLog<'_> {
    fn cache_head(&self, session: &str) -> ContextResult<SessionHead> {
        let access = self.scoped.access();
        if access
            .session
            .as_deref()
            .is_some_and(|bound| bound != session)
        {
            return Err(ContextError::AccessDenied);
        }
        validate_name(session, "session")?;
        let head = self.session_head(session)?;
        self.scoped.check_scope(&head.scope)?;
        Ok(head)
    }

    fn cache_writer(&self) -> ContextResult<()> {
        if matches!(self.scoped.access().actor, Actor::Operator | Actor::System) {
            Ok(())
        } else {
            Err(ContextError::AccessDenied)
        }
    }

    fn cache_key(&self, session: &str, name: &str) -> ContextResult<Vec<u8>> {
        cache_label(name)?;
        let mut key = session_key(&self.scoped.access().owner, session);
        key.extend(name.as_bytes());
        Ok(key)
    }

    fn cache_row(&self, key: &[u8]) -> ContextResult<Option<CacheRow>> {
        self.parts.caches.get(key)?.map(|v| decode(&v)).transpose()
    }

    fn cache_space(&self, version: Ulid) -> ChunkSpace<'_> {
        let mut prefix = owner_key(&self.scoped.access().owner);
        prefix.extend(version.to_bytes());
        ChunkSpace {
            partition: &self.parts.cache_chunks,
            prefix,
        }
    }

    fn coverage(&self) -> Coverage<'_> {
        Coverage::new(self.parts, &self.scoped.access().owner)
    }

    fn readable_cache_scopes(&self, scopes: &[Scope]) -> bool {
        scopes
            .iter()
            .all(|s| self.scoped.access().scopes.contains(s))
    }

    /// Read before loading the cache or events used to derive a conversation.
    pub fn availability_epoch(&self) -> ContextResult<u64> {
        let _guard = self.scoped.lock();
        self.scoped.availability_epoch_locked()
    }

    pub fn save_conversation_cache<R: Read>(
        &self,
        input: ConversationCacheInput,
        payload: R,
    ) -> ContextResult<ConversationCache> {
        let _guard = self.scoped.lock();
        self.cache_writer()?;
        let head = self.cache_head(&input.session)?;
        let key = self.cache_key(&input.session, &input.derivation)?;
        cache_label(&input.media_type)?;
        let expected = input.expected_availability_epoch;
        let actual = self.scoped.availability_epoch_locked()?;
        if input.position == 0 || expected > actual {
            return Err(ContextError::InvalidInput(
                "invalid cache position or epoch".into(),
            ));
        }
        if input.position > head.sequence {
            return Err(ContextError::PositionConflict {
                expected: input.position,
                actual: head.sequence,
            });
        }
        let view = self.view(&input.session)?;
        let coverage = self.coverage();
        let scopes = coverage.scopes_through(&view, input.position)?;
        if !self.readable_cache_scopes(&scopes) {
            return Err(ContextError::AccessDenied);
        }
        if coverage.withdrawn_since(&view, input.position, expected)? {
            return Err(ContextError::AvailabilityConflict { expected, actual });
        }
        let old = self.cache_row(&key)?;
        let version = Ulid::new();
        let space = self.cache_space(version);
        let pending = encode(&PendingCache {
            session: input.session.clone(),
            derivation: input.derivation.clone(),
        })?;
        let mut stage = self.scoped.batch();
        stage.insert(&self.parts.cache_pending, &space.prefix, &pending);
        stage.commit()?;
        let payload = self.scoped.write_chunks(Some(&space), payload)?;
        let row = CacheRow {
            version,
            position: input.position,
            media_type: input.media_type,
            bytes: payload.bytes,
            digest: payload.digest,
            saved_at_ms: crate::now_ms(),
            provenance: self.scoped.provenance(),
            epoch: expected,
            scopes,
            reclaimed: false,
        };
        let mut publish = self.scoped.batch();
        publish.insert(&self.parts.caches, key, encode(&row)?);
        publish.remove(&self.parts.cache_pending, space.prefix);
        if let Some(old) = old.filter(|row| !row.reclaimed) {
            publish.insert(
                &self.parts.cache_pending,
                self.cache_space(old.version).prefix,
                pending,
            );
        }
        publish.commit()?;
        Ok(row.describe(&input.session, &input.derivation, false))
    }

    pub fn conversation_cache(
        &self,
        session: &str,
        derivation: &str,
    ) -> ContextResult<Option<ConversationCache>> {
        let _guard = self.scoped.lock();
        self.cache_head(session)?;
        let Some(row) = self.cache_row(&self.cache_key(session, derivation)?)? else {
            return Ok(None);
        };
        if !self.readable_cache_scopes(&row.scopes) {
            return Err(ContextError::AccessDenied);
        }
        let stale = row.stale(&self.coverage(), &self.view(session)?)?;
        Ok(Some(row.describe(session, derivation, stale)))
    }

    pub fn conversation_caches(
        &self,
        session: &str,
        after: Option<&str>,
        limit: usize,
    ) -> ContextResult<ConversationCachePage> {
        let _guard = self.scoped.lock();
        self.cache_head(session)?;
        if !(1..=100).contains(&limit) {
            return Err(ContextError::InvalidInput("invalid cache page size".into()));
        }
        let prefix = session_key(&self.scoped.access().owner, session);
        let start = match after {
            Some(name) => Bound::Excluded(self.cache_key(session, name)?),
            None => Bound::Included(prefix.clone()),
        };
        let mut caches: Vec<ConversationCache> = Vec::new();
        let mut next = None;
        let view = self.view(session)?;
        for item in self.parts.caches.range((start, Bound::Unbounded)) {
            let (key, value) = item?;
            if !key.starts_with(&prefix) {
                break;
            }
            let row: CacheRow = decode(&value)?;
            if !self.readable_cache_scopes(&row.scopes) {
                continue;
            }
            if caches.len() == limit {
                next = caches.last().map(|c| c.derivation.clone());
                break;
            }
            let name = std::str::from_utf8(&key[prefix.len()..])
                .map_err(|_| ContextError::Corrupt("invalid cache name".into()))?;
            let stale = row.stale(&self.coverage(), &view)?;
            caches.push(row.describe(session, name, stale));
        }
        Ok(ConversationCachePage { caches, next })
    }

    pub fn read_conversation_cache(
        &self,
        cache: &ConversationCache,
        offset: u64,
        limit: usize,
    ) -> ContextResult<PayloadPage> {
        let _guard = self.scoped.lock();
        self.cache_head(&cache.session)?;
        let row = self
            .cache_row(&self.cache_key(&cache.session, &cache.derivation)?)?
            .ok_or(ContextError::ConversationCacheReplaced)?;
        if !self.readable_cache_scopes(&row.scopes) {
            return Err(ContextError::AccessDenied);
        }
        if cache.version != row.version {
            return Err(ContextError::ConversationCacheReplaced);
        }
        if row.stale(&self.coverage(), &self.view(&cache.session)?)? {
            return Err(ContextError::ConversationCacheStale);
        }
        self.scoped
            .read_chunks(&self.cache_space(row.version), row.bytes, offset, limit)
    }

    pub fn remove_conversation_cache(
        &self,
        session: &str,
        derivation: &str,
    ) -> ContextResult<bool> {
        let _guard = self.scoped.lock();
        self.cache_writer()?;
        self.cache_head(session)?;
        let key = self.cache_key(session, derivation)?;
        let Some(row) = self.cache_row(&key)? else {
            return Ok(false);
        };
        let mut batch = self.scoped.batch();
        batch.remove(&self.parts.caches, key);
        if !row.reclaimed {
            batch.insert(
                &self.parts.cache_pending,
                self.cache_space(row.version).prefix,
                encode(&PendingCache {
                    session: session.into(),
                    derivation: derivation.into(),
                })?,
            );
        }
        batch.commit()?;
        Ok(true)
    }
}
#[derive(Default, Serialize, Deserialize)]
struct CacheSweep {
    cursor: Option<Vec<u8>>,
    epoch: u64,
}

impl SessionLog<'_> {
    pub(super) fn recover_caches(
        &self,
        mut report: RecoveryReport,
        max_chunks: usize,
        max_uploads: usize,
    ) -> ContextResult<RecoveryReport> {
        let _guard = self.scoped.lock();
        let owner = owner_key(&self.scoped.access().owner);
        for entry in self.parts.cache_pending.prefix(&owner) {
            if report.chunks_removed == max_chunks || report.uploads_removed == max_uploads {
                report.more = true;
                return Ok(report);
            }
            let (key, value) = entry?;
            let pending: PendingCache = decode(&value)?;
            let row = self.cache_row(&self.cache_key(&pending.session, &pending.derivation)?)?;
            if key.len() != owner.len() + 16
                || row.is_some_and(|r| !r.reclaimed && key[owner.len()..] == r.version.to_bytes())
            {
                return Err(ContextError::Corrupt("invalid cache cleanup entry".into()));
            }
            let mut batch = self.scoped.batch();
            let chunks = self.scoped.remove_chunks(
                &mut batch,
                &ChunkSpace {
                    partition: &self.parts.cache_chunks,
                    prefix: key.to_vec(),
                },
                max_chunks - report.chunks_removed,
            )?;
            report.chunks_removed += chunks.removed;
            if chunks.complete {
                batch.remove(&self.parts.cache_pending, key);
                report.uploads_removed += 1;
            }
            batch.commit()?;
            if !chunks.complete {
                report.more = true;
                return Ok(report);
            }
        }
        let latest = self.coverage().last_withdrawal()?;
        let mut sweep: CacheSweep = self
            .parts
            .cache_sweep
            .get(&owner)?
            .map(|v| decode(&v))
            .transpose()?
            .unwrap_or_default();
        if sweep.cursor.is_none() {
            if latest <= sweep.epoch {
                return Ok(report);
            }
            sweep.epoch = latest;
        }
        if report.chunks_removed == max_chunks || report.uploads_removed == max_uploads {
            report.more |= self
                .parts
                .caches
                .prefix_keys(&owner)
                .next()
                .transpose()?
                .is_some();
            return Ok(report);
        }
        let start = sweep
            .cursor
            .take()
            .map(Bound::Excluded)
            .unwrap_or_else(|| Bound::Included(owner.clone()));
        let mut batch = self.scoped.batch();
        let mut entries = self.parts.caches.range((start, Bound::Unbounded));
        for i in 0..=4096 {
            let Some(entry) = entries.next() else {
                sweep.cursor = None;
                break;
            };
            let (key, value) = entry?;
            if !key.starts_with(&owner) {
                sweep.cursor = None;
                break;
            }
            if i == 4096 {
                report.more = true;
                break;
            }
            sweep.cursor = Some(key.to_vec());
            let mut row: CacheRow = decode(&value)?;
            if row.reclaimed {
                continue;
            }
            let invalid = || ContextError::Corrupt("invalid cache key".into());
            let (size, suffix) = key[owner.len()..]
                .split_first_chunk::<4>()
                .ok_or_else(invalid)?;
            let (session, derivation) = suffix
                .split_at_checked(u32::from_be_bytes(*size) as usize)
                .ok_or_else(invalid)?;
            let session = std::str::from_utf8(session).map_err(|_| invalid())?;
            if row.stale(&self.coverage(), &self.view(session)?)? {
                let derivation = std::str::from_utf8(derivation).map_err(|_| invalid())?;
                row.reclaimed = true;
                batch.insert(&self.parts.caches, &key, encode(&row)?);
                batch.insert(
                    &self.parts.cache_pending,
                    self.cache_space(row.version).prefix,
                    encode(&PendingCache {
                        session: session.into(),
                        derivation: derivation.into(),
                    })?,
                );
                report.more = true;
            }
        }
        batch.insert(&self.parts.cache_sweep, owner, encode(&sweep)?);
        batch.commit()?;
        report.more |= latest > sweep.epoch;
        Ok(report)
    }
}
fn cache_label(value: &str) -> ContextResult<()> {
    if value.is_empty()
        || value.len() > MAX_CONTEXT_NAME_BYTES
        || value.chars().any(char::is_control)
    {
        return Err(ContextError::InvalidInput(
            "invalid cache name or media type".into(),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Store;
    #[test]
    fn an_interrupted_save_keeps_the_previous_cache() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::create(dir.path().join("db")).unwrap();
        let log = store
            .session_log(ContextAccess::new("owner", Actor::System))
            .unwrap();
        let input = initialize(&log);
        let old = log
            .save_conversation_cache(input.clone(), &b"old"[..])
            .unwrap();
        log.scoped.drop_next_commit(&log.parts.caches);
        assert!(matches!(
            log.save_conversation_cache(input, &b"new"[..]),
            Err(ContextError::Storage(_))
        ));
        assert_eq!(
            log.conversation_cache("chat", "test").unwrap(),
            Some(old.clone())
        );
        assert_eq!(
            log.read_conversation_cache(&old, 0, 3).unwrap().bytes,
            b"old"
        );
        let pending = log
            .parts
            .cache_pending
            .prefix_keys(owner_key("owner"))
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(pending.len(), 1);
        assert_ne!(pending[0].as_ref(), log.cache_space(old.version).prefix);
        assert_eq!(log.parts.cache_chunks.prefix_keys(&pending[0]).count(), 1);
    }

    fn initialize(log: &SessionLog<'_>) -> ConversationCacheInput {
        log.create_session(
            OperationId::new(),
            NewSession {
                session: "chat".into(),
                kind: "chat".into(),
                metadata: serde_json::Value::Null,
                parent: None,
            },
            Scope::Personal,
        )
        .unwrap();
        log.append(
            OperationId::new(),
            EventInput {
                session: "chat".into(),
                event_id: "A".into(),
                kind: EventKind::Notice,
                scope: Scope::Personal,
                sources: vec![],
                expected_sequence: None,
                media_type: "text/plain".into(),
                occurred_at_ms: 1,
            },
            &[][..],
        )
        .unwrap();
        ConversationCacheInput {
            session: "chat".into(),
            derivation: "test".into(),
            position: 1,
            media_type: "text/plain".into(),
            expected_availability_epoch: 0,
        }
    }

    fn filler_rows(log: &SessionLog<'_>, session: &str, count: usize) {
        let mut row = log
            .cache_row(&log.cache_key("chat", "test").unwrap())
            .unwrap()
            .unwrap();
        let mut batch = log.scoped.batch();
        for n in 0..count {
            row.version = Ulid::new();
            batch.insert(
                &log.parts.caches,
                log.cache_key(session, &format!("{n:04}")).unwrap(),
                encode(&row).unwrap(),
            );
        }
        batch.commit().unwrap();
    }

    fn withdraw(log: &SessionLog<'_>, session: &str, epoch: u64) {
        let mut batch = log.scoped.batch();
        log.coverage()
            .record_withdrawal(&mut batch, session, epoch, 1);
        batch.commit().unwrap();
    }

    #[test]
    fn recovery_reclaims_replaced_removed_and_stale_payloads() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::create(dir.path().join("db")).unwrap();
        let log = store
            .session_log(ContextAccess::new("owner", Actor::System))
            .unwrap();
        let input = initialize(&log);
        let save = || {
            log.save_conversation_cache(input.clone(), &b"payload"[..])
                .unwrap()
        };
        let removed = save();
        log.remove_conversation_cache("chat", "test").unwrap();
        assert_eq!(log.recover_payloads(32, 32).unwrap().chunks_removed, 1);
        assert_eq!(
            log.parts
                .cache_chunks
                .prefix_keys(log.cache_space(removed.version).prefix)
                .count(),
            0
        );
        let old = save();
        let stale = save();
        filler_rows(&log, "aaaa", 4095);
        withdraw(&log, "chat", 1);
        let report = log.recover_payloads(32, 32).unwrap();
        assert_eq!(
            (report.chunks_removed, report.uploads_removed, report.more),
            (1, 1, true)
        );
        assert_eq!(
            log.parts
                .cache_chunks
                .prefix_keys(log.cache_space(old.version).prefix)
                .count(),
            0
        );
        assert!(
            log.cache_row(&log.cache_key("chat", "test").unwrap())
                .unwrap()
                .unwrap()
                .reclaimed
        );
        let report = log.recover_payloads(32, 32).unwrap();
        assert_eq!((report.chunks_removed, report.more), (1, false));
        assert_eq!(
            log.parts
                .cache_chunks
                .prefix_keys(log.cache_space(stale.version).prefix)
                .count(),
            0
        );
        assert!(log.conversation_caches("chat", None, 1).unwrap().caches[0].stale);
        log.remove_conversation_cache("chat", "test").unwrap();
    }

    #[test]
    fn recovery_rescans_caches_withdrawn_during_a_sweep_after_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let version = {
            let store = Store::create(&path).unwrap();
            let log = store
                .session_log(ContextAccess::new("owner", Actor::System))
                .unwrap();
            let input = initialize(&log);
            let cache = log.save_conversation_cache(input, &b"A"[..]).unwrap();
            filler_rows(&log, "fill", 4096);
            withdraw(&log, "other", 1);
            assert!(log.recover_payloads(32, 32).unwrap().more);
            withdraw(&log, "chat", 2);
            assert!(log.recover_payloads(32, 32).unwrap().more);
            cache.version
        };
        let store = Store::open(&path).unwrap();
        let log = store
            .session_log(ContextAccess::new("owner", Actor::System))
            .unwrap();
        assert!(log.recover_payloads(32, 32).unwrap().more);
        let report = log.recover_payloads(32, 32).unwrap();
        assert_eq!((report.more, report.chunks_removed), (false, 1));
        assert_eq!(
            log.parts
                .cache_chunks
                .prefix_keys(log.cache_space(version).prefix)
                .count(),
            0
        );
    }

    #[test]
    fn recovery_rejects_a_pending_current_version() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::create(dir.path().join("db")).unwrap();
        let log = store
            .session_log(ContextAccess::new("owner", Actor::System))
            .unwrap();
        let input = initialize(&log);
        let cache = log.save_conversation_cache(input, &b"A"[..]).unwrap();
        let mut batch = log.scoped.batch();
        batch.insert(
            &log.parts.cache_pending,
            log.cache_space(cache.version).prefix,
            encode(&PendingCache {
                session: "chat".into(),
                derivation: "test".into(),
            })
            .unwrap(),
        );
        batch.commit().unwrap();
        assert!(matches!(
            log.recover_payloads(32, 32),
            Err(ContextError::Corrupt(_))
        ));
        assert_eq!(
            log.read_conversation_cache(&cache, 0, 1).unwrap().bytes,
            b"A"
        );
    }
}
