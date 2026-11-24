use super::storage::*;
use super::*;
use crate::{Actor, OperationId, RecordState, Scope};

impl ContextStore<'_> {
    /// Validate a bounded planning/dispatch snapshot against current heads,
    /// lifecycle state, and transitive sources. Historical `read` stays exact.
    pub fn validate_current(
        &self,
        pins: &[RecordPin],
        valid_at_ms: Option<i64>,
    ) -> ContextResult<()> {
        let _guard = self.validate_current_guard(pins, valid_at_ms, None)?;
        Ok(())
    }

    /// Validate sources and retain their validity through a short synchronous
    /// commit in another store. See [`CurrentSourcesGuard`] for lock ordering.
    /// `expected_epoch` additionally protects observations made before this
    /// call, including a set of dependencies discovered by a prior query.
    pub fn validate_current_guard(
        &self,
        pins: &[RecordPin],
        valid_at_ms: Option<i64>,
        expected_epoch: Option<u64>,
    ) -> ContextResult<CurrentSourcesGuard<'_>> {
        if pins.len() > MAX_CONTEXT_SOURCES {
            return Err(ContextError::BudgetExceeded);
        }
        let guard = self.scoped.lock();
        let availability_epoch = self.scoped.availability_epoch_locked()?;
        if let Some(expected) = expected_epoch {
            if expected != availability_epoch {
                return Err(ContextError::AvailabilityConflict {
                    expected,
                    actual: availability_epoch,
                });
            }
        }
        let mut memo = std::collections::BTreeMap::new();
        for pin in pins {
            let head = self.scoped.head_locked(&pin.record)?;
            if head.pin != *pin
                || !self.scoped.available_memoized(pin, true, &mut memo)?
                || valid_at_ms.is_some_and(|at| !head.temporal.is_valid_at(at))
            {
                return Err(ContextError::Unavailable(pin.record.clone()));
            }
        }
        Ok(CurrentSourcesGuard {
            _lock: guard,
            availability_epoch,
        })
    }

    pub fn save_object(
        &self,
        operation: OperationId,
        input: ObjectInput,
    ) -> ContextResult<WriteReceipt> {
        self.write_data(operation, None, input, None)
    }

    pub fn revise_object(
        &self,
        operation: OperationId,
        expected: RecordPin,
        input: ObjectInput,
    ) -> ContextResult<WriteReceipt> {
        self.write_data(operation, Some(expected), input, None)
    }

    pub fn save_relation(
        &self,
        operation: OperationId,
        input: RelationInput,
    ) -> ContextResult<WriteReceipt> {
        self.relation_data(operation, None, input)
    }

    pub fn revise_relation(
        &self,
        operation: OperationId,
        expected: RecordPin,
        input: RelationInput,
    ) -> ContextResult<WriteReceipt> {
        self.relation_data(operation, Some(expected), input)
    }

    fn relation_data(
        &self,
        operation: OperationId,
        expected: Option<RecordPin>,
        input: RelationInput,
    ) -> ContextResult<WriteReceipt> {
        let endpoints = Endpoints {
            from: input.from,
            to: input.to,
        };
        let object = ObjectInput {
            type_pin: input.type_pin,
            scope: input.scope,
            properties: input.properties,
            sources: input.sources,
            information: input.information,
            temporal: input.temporal,
        };
        self.write_data(operation, expected, object, Some(endpoints))
    }

    fn write_data(
        &self,
        operation: OperationId,
        expected: Option<RecordPin>,
        input: ObjectInput,
        endpoints: Option<Endpoints>,
    ) -> ContextResult<WriteReceipt> {
        let _guard = self.scoped.lock();
        let kind = if endpoints.is_some() {
            RecordKind::Relation
        } else {
            RecordKind::Object
        };
        let digest = self
            .scoped
            .digest("save-context-record", &(&expected, &input, &endpoints))?;
        if let Some(receipt) = self.scoped.replay(operation, digest)? {
            return Ok(receipt);
        }
        input.temporal.validate()?;
        self.scoped.validate_sources(
            &input.scope,
            &input.sources,
            input.information,
            expected.as_ref().map(|pin| &pin.record),
        )?;
        let definition = self.type_definition_locked(&input.type_pin)?;
        if definition.scope != input.scope && definition.scope != Scope::Personal {
            return Err(ContextError::AccessDenied);
        }
        match (&definition.kind, &endpoints) {
            (TypeKind::Object, None) => {}
            (
                TypeKind::Relation {
                    from_types,
                    to_types,
                },
                Some(endpoints),
            ) => {
                self.validate_endpoint(&endpoints.from, from_types, &input.scope)?;
                self.validate_endpoint(&endpoints.to, to_types, &input.scope)?;
            }
            _ => {
                return Err(ContextError::InvalidInput(
                    "record and type categories disagree".into(),
                ))
            }
        }
        self.validate_properties(&definition, &input.properties, &input.scope)?;
        let (previous, pin) = if let Some(expected) = expected {
            if expected.record.kind != kind {
                return Err(ContextError::InvalidInput(
                    "record family cannot change".into(),
                ));
            }
            let (head, pin) = self.scoped.next_revision(&expected)?;
            if head.scope != input.scope
                || head.type_pin.as_ref().map(|pin| &pin.record) != Some(&input.type_pin.record)
            {
                return Err(ContextError::InvalidInput(
                    "record scope and type identity cannot change".into(),
                ));
            }
            (Some(head), pin)
        } else {
            (None, self.scoped.fresh_pin(kind))
        };
        let bytes = encode(&input.properties)?;
        let mut batch = self.scoped.batch();
        let header = RecordHeader {
            pin: pin.clone(),
            scope: input.scope,
            state: RecordState::Active,
            type_pin: Some(input.type_pin),
            sources: input.sources,
            source_scopes: Vec::new(),
            provenance: self.scoped.provenance(),
            information: input.information,
            temporal: input.temporal,
            recorded_at_ms: super::super::now_ms(),
            payload: self.scoped.put_payload(&mut batch, &pin, &bytes),
            endpoints,
            history: None,
            checkpoint: None,
        };
        if previous.is_some() {
            self.scoped.advance_epoch(&mut batch)?;
        }
        self.scoped.put_header(&mut batch, &header)?;
        self.replace_postings(&mut batch, &header, Some(&input.properties))?;
        self.replace_adjacency(&mut batch, previous.as_ref(), &header)?;
        self.scoped.commit_operation(
            batch,
            operation,
            digest,
            WriteReceipt {
                pin,
                history_sequence: None,
            },
        )
    }

    fn validate_endpoint(
        &self,
        reference: &ContextRef,
        types: &[ContextRef],
        scope: &Scope,
    ) -> ContextResult<()> {
        if reference.kind == RecordKind::Type {
            return Err(ContextError::InvalidInput(
                "a type definition cannot be a relation endpoint".into(),
            ));
        }
        let header = self.scoped.head_locked(reference)?;
        if header.scope != *scope && header.scope != Scope::Personal {
            return Err(ContextError::AccessDenied);
        }
        if !self.scoped.available_locked(&header.pin, true)? {
            return Err(ContextError::Unavailable(reference.clone()));
        }
        if !types.is_empty()
            && !header
                .type_pin
                .as_ref()
                .is_some_and(|pin| types.contains(&pin.record))
        {
            return Err(ContextError::InvalidInput(
                "relation endpoint has an incompatible type".into(),
            ));
        }
        Ok(())
    }

    /// Return the records that are currently "live" at wall-clock time:
    /// `state == Active` **and** their valid-time window covers `now`.
    ///
    /// This is the working-context equivalent of recalling only facts that both
    /// survived retraction and have not expired. Records filtered by `type_filter`
    /// (exact type pin) and/or `scope`. At most `limit` records are returned.
    pub fn query_current_valid(
        &self,
        type_filter: Option<&RecordPin>,
        scope: Option<&Scope>,
        limit: usize,
    ) -> ContextResult<Vec<ContextRecord>> {
        if limit == 0 || limit > 1000 {
            return Err(ContextError::InvalidInput(
                "current-valid query limit must be 1..=1000".into(),
            ));
        }
        let _guard = self.scoped.lock();
        let now = crate::now_ms();
        let mut out = Vec::new();
        for kind in [RecordKind::Object, RecordKind::Relation] {
            let mut cursor = None;
            loop {
                let page = self.scoped.scan_heads(kind, cursor.as_ref(), 256)?;
                for pin in page.heads {
                    let header = match self.scoped.header_locked(&pin) {
                        Ok(header) => header,
                        Err(ContextError::AccessDenied) => continue,
                        Err(error) => return Err(error),
                    };
                    if !header.is_valid_at(now) {
                        continue;
                    }
                    if scope.is_some_and(|expected| header.scope != *expected) {
                        continue;
                    }
                    if type_filter
                        .is_some_and(|expected| header.type_pin.as_ref() != Some(expected))
                    {
                        continue;
                    }
                    out.push(self.read_locked(&header.pin)?);
                    if out.len() == limit {
                        return Ok(out);
                    }
                }
                if page.next.is_none() {
                    break;
                }
                cursor = page.next;
            }
        }
        Ok(out)
    }

    pub fn availability_epoch(&self) -> ContextResult<u64> {
        let _guard = self.scoped.lock();
        self.scoped.availability_epoch_locked()
    }

    /// Cleans abandoned uploads and purged payloads, never live committed data.
    /// The chunk and upload counts bound work even for very large originals.
    pub fn recover_payloads(
        &self,
        max_chunks: usize,
        max_uploads: usize,
    ) -> ContextResult<RecoveryReport> {
        self.scoped
            .recover_payloads(StoreModule::Memory, max_chunks, max_uploads)
    }

    pub fn retract(
        &self,
        operation: OperationId,
        expected: RecordPin,
    ) -> ContextResult<WriteReceipt> {
        self.change_state(operation, expected, RecordState::Retracted)
    }

    /// Hides this record and dependent memory records immediately.
    /// Physical chunk cleanup is bounded and resumable with `recover_payloads`.
    pub fn purge(
        &self,
        operation: OperationId,
        expected: RecordPin,
    ) -> ContextResult<WriteReceipt> {
        self.change_state(operation, expected, RecordState::Purged)
    }

    fn change_state(
        &self,
        operation: OperationId,
        expected: RecordPin,
        state: RecordState,
    ) -> ContextResult<WriteReceipt> {
        expected.record.kind.require_module(StoreModule::Memory)?;
        if expected.record.kind == RecordKind::Type {
            return Err(ContextError::InvalidInput(
                "type versions cannot be retracted or purged".into(),
            ));
        }
        if (state == RecordState::Purged || expected.record.kind == RecordKind::ActionExecution)
            && !matches!(self.scoped.access().actor, Actor::User | Actor::Operator)
        {
            return Err(ContextError::AccessDenied);
        }
        self.scoped.commit_state_change(
            operation,
            expected,
            state,
            state == RecordState::Purged,
            |batch, previous, header| {
                self.replace_postings(batch, header, None)?;
                self.replace_adjacency(batch, Some(previous), header)
            },
        )
    }
}
