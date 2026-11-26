use super::turns::Numbers;
use super::*;
use crate::records::codec::{encode, validate_name};

impl SessionLog<'_> {
    /// Create a session sharing the source log through the given position.
    pub fn fork_session(
        &self,
        operation: OperationId,
        session: NewSession,
        origin: SessionOrigin,
    ) -> ContextResult<WriteReceipt> {
        if !matches!(self.scoped.access().actor, Actor::System | Actor::Operator) {
            return Err(ContextError::AccessDenied);
        }
        validate_name(&origin.session, "source session")?;
        let _guard = self.scoped.lock();
        let source = self.session_head(&origin.session)?;
        self.scoped.check_scope(&source.scope)?;
        if origin.position == 0 {
            return Err(ContextError::InvalidInput(
                "fork position must be positive".into(),
            ));
        }
        if origin.position > source.sequence {
            return Err(ContextError::PositionConflict {
                expected: origin.position,
                actual: source.sequence,
            });
        }
        self.create_locked(operation, session, source.scope, Some(origin))
    }

    pub(super) fn put_lineage(
        &self,
        batch: &mut mmdb_kernel::Batch,
        session: &str,
        origin: &SessionOrigin,
    ) -> ContextResult<(u64, Numbers)> {
        let view = self.view(&origin.session)?;
        let entry = view
            .entry(origin.position)?
            .ok_or_else(|| ContextError::Corrupt("missing fork position".into()))?;
        if entry.open_turn.is_some() {
            return Err(ContextError::Turn(TurnError::TurnOpen));
        }
        let ancestors = view.fork_ancestors(origin.position)?;
        batch.insert(
            &self.parts.lineage,
            session_key(&self.scoped.access().owner, session),
            encode(&ancestors)?,
        );
        Ok((origin.position, entry.numbers))
    }
}
