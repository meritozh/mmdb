use super::{
    owner_key, segment, session_key, view::View, ContextError, ContextResult, Scope, SessionParts,
};
use crate::records::codec::{decode, encode, split_segment};
use mmdb_kernel::Batch;
use std::collections::BTreeSet;

pub(super) struct Coverage<'a> {
    parts: &'a SessionParts,
    owner: &'a str,
}

fn number(bytes: &[u8]) -> ContextResult<u64> {
    Ok(u64::from_be_bytes(bytes.try_into().map_err(|_| {
        ContextError::Corrupt("invalid coverage number".into())
    })?))
}

impl<'a> Coverage<'a> {
    pub fn new(parts: &'a SessionParts, owner: &'a str) -> Self {
        Self { parts, owner }
    }

    pub fn record_scopes(
        &self,
        batch: &mut Batch,
        session: &str,
        position: u64,
        scopes: &[Scope],
    ) -> ContextResult<()> {
        for scope in scopes {
            let mut key = session_key(self.owner, session);
            segment(&mut key, &encode(scope)?);
            if self
                .parts
                .source_scopes
                .prefix_keys(&key)
                .next()
                .transpose()?
                .is_none()
            {
                key.extend(position.to_be_bytes());
                batch.insert(&self.parts.source_scopes, key, []);
            }
        }
        Ok(())
    }

    pub fn record_withdrawal(&self, batch: &mut Batch, holder: &str, epoch: u64, position: u64) {
        let mut key = session_key(self.owner, holder);
        key.extend(epoch.to_be_bytes());
        batch.insert(&self.parts.withdrawals, key, position.to_be_bytes());
        batch.insert(
            &self.parts.withdrawals,
            owner_key(self.owner),
            epoch.to_be_bytes(),
        );
    }

    pub fn scopes_through(&self, view: &View<'_>, position: u64) -> ContextResult<Vec<Scope>> {
        let mut scopes = BTreeSet::new();
        for span in view.spans(position) {
            let prefix = session_key(self.owner, span.holder);
            let mut previous = None;
            for row in self.parts.source_scopes.prefix(&prefix) {
                let (key, value) = row?;
                let suffix = &key[prefix.len()..];
                if suffix.len() < 12 || !value.is_empty() {
                    return Err(ContextError::Corrupt("invalid source scope row".into()));
                }
                let (encoded, position_bytes) = split_segment(suffix)
                    .filter(|(_, rest)| rest.len() == 8)
                    .ok_or_else(|| ContextError::Corrupt("invalid source scope length".into()))?;
                let scope: Scope = decode(encoded)?;
                let first = number(position_bytes)?;
                if previous.as_ref() != Some(&scope) && first <= span.through {
                    scopes.insert(scope.clone());
                }
                previous = Some(scope);
            }
        }
        Ok(scopes.into_iter().collect())
    }

    pub fn withdrawn_since(
        &self,
        view: &View<'_>,
        position: u64,
        epoch: u64,
    ) -> ContextResult<bool> {
        let Some(next) = epoch.checked_add(1) else {
            return Ok(false);
        };
        for span in view.spans(position) {
            let prefix = session_key(self.owner, span.holder);
            let mut start = prefix.clone();
            start.extend(next.to_be_bytes());
            for row in self.parts.withdrawals.range(start..) {
                let (key, value) = row?;
                if !key.starts_with(&prefix) {
                    break;
                }
                number(&key[prefix.len()..])?;
                if number(&value)? <= span.through {
                    return Ok(true);
                }
            }
        }
        Ok(false)
    }

    pub fn last_withdrawal(&self) -> ContextResult<u64> {
        self.parts
            .withdrawals
            .get(owner_key(self.owner))?
            .map_or(Ok(0), |bytes| number(&bytes))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::session::*;
    use crate::{OperationId, Store};

    #[test]
    fn same_batch_preserves_first_scope_position_across_a_fork() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::create(dir.path().join("db")).unwrap();
        let log = store
            .session_log(ContextAccess::new("owner", Actor::System))
            .unwrap();
        let coverage = Coverage::new(log.parts, "owner");
        let mut batch = log.scoped.batch();
        for position in [2, 3] {
            coverage
                .record_scopes(&mut batch, "parent", position, &[Scope::Personal])
                .unwrap();
        }
        coverage
            .record_scopes(&mut batch, "parent", 4, &[Scope::System])
            .unwrap();
        batch.commit().unwrap();
        let parent = log.view("parent").unwrap();
        let fork = parent.fork("fork", 2).unwrap();
        assert!(coverage.scopes_through(&parent, 1).unwrap().is_empty());
        assert_eq!(
            coverage.scopes_through(&parent, 2).unwrap(),
            [Scope::Personal]
        );
        assert_eq!(
            coverage.scopes_through(&fork, 5).unwrap(),
            [Scope::Personal]
        );
    }

    #[test]
    fn message_withdrawal_reaches_its_shared_start() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::create(dir.path().join("db")).unwrap();
        let log = store
            .session_log(ContextAccess {
                run: Some("run".into()),
                ..ContextAccess::new("owner", Actor::System)
            })
            .unwrap();
        log.create_session(
            OperationId::new(),
            NewSession {
                session: "parent".into(),
                kind: "chat".into(),
                parent: None,
                metadata: serde_json::Value::Null,
            },
            Scope::Personal,
        )
        .unwrap();
        let mut receipts = Vec::new();
        for (i, kind) in [
            EventKind::TurnStarted,
            EventKind::StepStarted,
            EventKind::AssistantStarted {
                message_id: "msg".into(),
            },
            EventKind::AssistantFragment {
                message_id: "msg".into(),
                ordinal: 0,
            },
        ]
        .into_iter()
        .enumerate()
        {
            receipts.push(
                log.append(
                    OperationId::new(),
                    EventInput {
                        session: "parent".into(),
                        event_id: i.to_string(),
                        kind,
                        scope: Scope::Personal,
                        sources: vec![],
                        expected_sequence: None,
                        media_type: "text/plain".into(),
                        occurred_at_ms: 1,
                    },
                    &[][..],
                )
                .unwrap(),
            );
        }
        let coverage = Coverage::new(log.parts, "owner");
        assert_eq!(coverage.last_withdrawal().unwrap(), 0);
        let operator = store
            .session_log(ContextAccess::new("owner", Actor::Operator))
            .unwrap();
        operator
            .purge(OperationId::new(), receipts[3].pin.clone())
            .unwrap();
        let epoch = operator.scoped.availability_epoch_locked().unwrap();
        assert_eq!(coverage.last_withdrawal().unwrap(), epoch);
        let fork = log.view("parent").unwrap().fork("fork", 3).unwrap();
        assert!(coverage.withdrawn_since(&fork, 3, 0).unwrap());
        assert!(!coverage.withdrawn_since(&fork, 2, 0).unwrap());
        assert!(!coverage.withdrawn_since(&fork, 3, epoch).unwrap());
    }
}
