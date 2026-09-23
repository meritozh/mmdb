use super::super::{Actor, OperationId, RecordState, Scope};
use super::storage::*;
use super::*;
use ulid::Ulid;

impl ContextStore<'_> {
    /// Validate a bounded planning/dispatch snapshot against current heads,
    /// lifecycle state, and transitive sources. Historical `read` stays exact.
    pub fn validate_current(
        &self,
        pins: &[RecordPin],
        valid_at_ms: Option<i64>,
    ) -> ContextResult<()> {
        if pins.len() > MAX_CONTEXT_SOURCES {
            return Err(ContextError::BudgetExceeded);
        }
        let _guard = self.db.write_lock.lock();
        for pin in pins {
            let head = self.head_locked(&pin.record)?;
            if head.pin != *pin
                || !self.available_locked(pin, true)?
                || valid_at_ms.is_some_and(|at| !head.temporal.contains_valid_time(at))
            {
                return Err(ContextError::Unavailable(pin.record.clone()));
            }
        }
        Ok(())
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
        let _guard = self.db.write_lock.lock();
        let kind = if endpoints.is_some() {
            RecordKind::Relation
        } else {
            RecordKind::Object
        };
        let digest = self.digest("save-context-record", &(&expected, &input, &endpoints))?;
        if let Some(receipt) = self.replay(operation, digest)? {
            return Ok(receipt);
        }
        input.temporal.validate()?;
        self.validate_sources(
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
            let (head, pin) = self.next_revision(&expected)?;
            if head.scope != input.scope
                || head.type_pin.as_ref().map(|pin| &pin.record) != Some(&input.type_pin.record)
            {
                return Err(ContextError::InvalidInput(
                    "record scope and type identity cannot change".into(),
                ));
            }
            (Some(head), pin)
        } else {
            (None, self.fresh_pin(kind))
        };
        let bytes = encode(&input.properties)?;
        let header = RecordHeader {
            pin: pin.clone(),
            scope: input.scope,
            state: RecordState::Active,
            type_pin: Some(input.type_pin),
            sources: input.sources,
            provenance: self.provenance(),
            information: input.information,
            temporal: input.temporal,
            recorded_at_ms: super::super::now_ms(),
            payload: PayloadDescriptor {
                revision: pin.revision,
                bytes: bytes.len() as u64,
                digest: self.content_digest(&bytes),
            },
            endpoints,
            history: None,
            checkpoint: None,
        };
        let mut batch = self.db.durable_batch();
        if previous.is_some() {
            self.advance_availability_epoch(&mut batch)?;
        }
        self.put_payload(&mut batch, &pin, &bytes);
        self.put_header(&mut batch, &header)?;
        self.replace_postings(&mut batch, &header, Some(&input.properties))?;
        self.replace_adjacency(&mut batch, previous.as_ref(), &header)?;
        self.commit_operation(
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
        let header = self.head_locked(reference)?;
        if header.scope != *scope && header.scope != Scope::Personal {
            return Err(ContextError::AccessDenied);
        }
        if !self.available_locked(&header.pin, true)? {
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
        let _guard = self.db.write_lock.lock();
        let now = crate::now_ms();
        let mut out = Vec::new();
        for kind in [RecordKind::Object, RecordKind::Relation] {
            let mut start = owner_key(&self.access.owner);
            start.push(kind_byte(kind));
            for entry in self.parts.heads.range(start.clone()..) {
                let (key, revision) = entry?;
                if !key.starts_with(&start) {
                    break;
                }
                if out.len() >= limit || key.len() != start.len() + 16 || revision.len() != 8 {
                    break;
                }
                let id = u128::from_be_bytes(
                    key[start.len()..]
                        .try_into()
                        .map_err(|_| ContextError::Corrupt("invalid record cursor".into()))?,
                );
                let reference = ContextRef {
                    era: self.db.era_id,
                    owner: self.access.owner.clone(),
                    kind,
                    id: Ulid(id),
                };
                let header = match self.head_locked(&reference) {
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
                if type_filter.is_some_and(|expected| header.type_pin.as_ref() != Some(expected)) {
                    continue;
                }
                out.push(self.read_locked(&header.pin)?);
            }
            if out.len() >= limit {
                break;
            }
        }
        Ok(out)
    }

    pub fn retract(
        &self,
        operation: OperationId,
        expected: RecordPin,
    ) -> ContextResult<WriteReceipt> {
        self.change_state(operation, expected, RecordState::Retracted)
    }

    /// Makes this record and payloads derived from it unavailable immediately.
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
        let _guard = self.db.write_lock.lock();
        if expected.record.kind == RecordKind::Type {
            return Err(ContextError::InvalidInput(
                "type versions cannot be retracted or purged".into(),
            ));
        }
        if (state == RecordState::Purged
            || matches!(
                expected.record.kind,
                RecordKind::History | RecordKind::ActionExecution
            ))
            && !matches!(self.access.actor, Actor::User | Actor::Operator)
        {
            return Err(ContextError::AccessDenied);
        }
        let digest = self.digest("set-context-state", &(&expected, state))?;
        if let Some(receipt) = self.replay(operation, digest)? {
            return Ok(receipt);
        }
        let (previous, pin) = self.next_revision(&expected)?;
        // `validate_sources` already forbids non-User/Operator actors from
        // *creating or revising* `Instruction` records. Retraction must be
        // symmetric: an Assistant must not be able to retract an Instruction
        // that it could not have authored. (Purple is already guarded above;
        // ordinary retractions of non-Instruction records stay allowed.)
        if state == RecordState::Retracted
            && previous.information == InformationKind::Instruction
            && !matches!(self.access.actor, Actor::User | Actor::Operator)
        {
            return Err(ContextError::AccessDenied);
        }
        let mut header = previous.clone();
        header.pin = pin.clone();
        header.state = state;
        header.provenance = self.provenance();
        header.recorded_at_ms = super::super::now_ms();
        let mut batch = self.db.durable_batch();
        let epoch = self.advance_availability_epoch(&mut batch)?;
        if state == RecordState::Purged || header.history.is_some() {
            self.invalidate_checkpoint_payloads(&mut batch, epoch)?;
        }
        if let Some(history) = &header.history {
            self.invalidate_message(&mut batch, history)?;
        }
        self.put_header(&mut batch, &header)?;
        self.replace_postings(&mut batch, &header, None)?;
        self.replace_adjacency(&mut batch, Some(&previous), &header)?;
        if state == RecordState::Purged {
            batch.insert(
                &self.parts.pending,
                record_key(&pin.record),
                encode(&StagedUpload {
                    pin: pin.clone(),
                    operation,
                    purge: true,
                })?,
            );
        }
        self.commit_operation(
            batch,
            operation,
            digest,
            WriteReceipt {
                pin,
                history_sequence: header.history.as_ref().map(|history| history.sequence),
            },
        )
    }
}
