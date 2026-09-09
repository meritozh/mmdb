use super::super::{Actor, OperationId, RecordState, Scope, TemporalFacts};
use super::storage::*;
use super::*;
use serde_json::Value;

fn action_name_key(owner: &str, scope: &Scope, name: &str) -> ContextResult<Vec<u8>> {
    let mut key = owner_key(owner);
    segment(&mut key, &encode(scope)?);
    segment(&mut key, name.as_bytes());
    Ok(key)
}

impl ContextStore<'_> {
    pub fn define_action(
        &self,
        operation: OperationId,
        definition: ActionDefinition,
    ) -> ContextResult<WriteReceipt> {
        self.write_action(operation, None, definition)
    }

    pub fn revise_action(
        &self,
        operation: OperationId,
        expected: RecordPin,
        definition: ActionDefinition,
    ) -> ContextResult<WriteReceipt> {
        self.write_action(operation, Some(expected), definition)
    }

    fn write_action(
        &self,
        operation: OperationId,
        expected: Option<RecordPin>,
        definition: ActionDefinition,
    ) -> ContextResult<WriteReceipt> {
        let _guard = self.db.write_lock.lock();
        let digest = self.digest("define-action", &(&expected, &definition))?;
        if let Some(receipt) = self.replay(operation, digest)? {
            return Ok(receipt);
        }
        validate_name(&definition.name, "action name")?;
        if definition.description.trim().is_empty()
            || definition.description.len() > 4096
            || definition.preconditions.len() > 32
        {
            return Err(ContextError::InvalidInput(
                "action description or condition bounds exceeded".into(),
            ));
        }
        let input_type = TypeDefinition {
            name: definition.name.clone(),
            scope: definition.scope.clone(),
            kind: TypeKind::Object,
            properties: definition.inputs.clone(),
        };
        self.validate_definition(&input_type)?;
        let target_type = self.type_definition_locked(&definition.target_type)?;
        if target_type.kind != TypeKind::Object
            || (target_type.scope != Scope::Personal && target_type.scope != definition.scope)
        {
            return Err(ContextError::InvalidInput(
                "action requires an accessible object type".into(),
            ));
        }
        for condition in &definition.preconditions {
            let property = match condition {
                ActionCondition::Equals { property, .. } | ActionCondition::Exists { property } => {
                    property
                }
            };
            let property_type = target_type.properties.get(property).ok_or_else(|| {
                ContextError::InvalidInput(
                    "action condition references an unknown target property".into(),
                )
            })?;
            if let ActionCondition::Equals { value, .. } = condition {
                self.validate_value(&property_type.value_type, value, &definition.scope)?;
            }
        }
        let information = match self.access.actor {
            Actor::Assistant => InformationKind::Inference,
            Actor::User | Actor::Operator => InformationKind::Instruction,
            _ => InformationKind::Observation,
        };
        self.validate_sources(
            &definition.scope,
            &definition.sources,
            information,
            expected.as_ref().map(|pin| &pin.record),
        )?;
        let key = action_name_key(&self.access.owner, &definition.scope, &definition.name)?;
        let pin = if let Some(expected) = expected {
            let previous = self.action_locked(&expected)?;
            if previous.name != definition.name || previous.scope != definition.scope {
                return Err(ContextError::InvalidInput(
                    "action identity and scope cannot change".into(),
                ));
            }
            self.next_revision(&expected)?.1
        } else if let Some(bytes) = self.parts.action_names.get(&key)? {
            let reference = decode(&bytes)?;
            let previous = self.head_locked(&reference)?;
            if self.action_locked(&previous.pin)? != definition {
                return Err(ContextError::TypeConflict(definition.name));
            }
            return self.commit_operation(
                self.db.durable_batch(),
                operation,
                digest,
                WriteReceipt {
                    pin: previous.pin,
                    history_sequence: None,
                },
            );
        } else {
            self.fresh_pin(RecordKind::Action)
        };
        let bytes = encode(&definition)?;
        if bytes.len() > MAX_RECORD_BYTES {
            return Err(ContextError::BudgetExceeded);
        }
        let header = self.action_header(
            pin.clone(),
            definition.scope,
            definition.sources,
            information,
            &bytes,
        );
        let mut batch = self.db.durable_batch();
        if pin.revision > 1 {
            self.advance_availability_epoch(&mut batch)?;
        }
        self.put_payload(&mut batch, &pin, &bytes);
        self.put_header(&mut batch, &header)?;
        batch.insert(&self.parts.action_names, key, encode(&pin.record)?);
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

    pub fn find_action(
        &self,
        scope: &Scope,
        name: &str,
    ) -> ContextResult<Option<(RecordPin, ActionDefinition)>> {
        let _guard = self.db.write_lock.lock();
        self.check_scope(scope)?;
        validate_name(name, "action name")?;
        let Some(bytes) =
            self.parts
                .action_names
                .get(action_name_key(&self.access.owner, scope, name)?)?
        else {
            return Ok(None);
        };
        let reference = decode(&bytes)?;
        let header = self.head_locked(&reference)?;
        Ok(Some((header.pin.clone(), self.action_locked(&header.pin)?)))
    }

    /// Bounded discovery by object. Applicability does not grant execution rights.
    pub fn actions_for(
        &self,
        target: &RecordPin,
        limit: usize,
        after: Option<&ContextRef>,
    ) -> ContextResult<ActionPage> {
        if limit == 0 || limit > 100 {
            return Err(ContextError::InvalidInput(
                "action page limit must be 1..=100".into(),
            ));
        }
        let _guard = self.db.write_lock.lock();
        let target_record = self.current_action_target(target)?;
        let RecordBody::Object(properties) = target_record.body else {
            return Err(ContextError::InvalidInput(
                "action target must be an object".into(),
            ));
        };
        let mut prefix = owner_key(&self.access.owner);
        prefix.push(kind_byte(RecordKind::Action));
        let start = if let Some(after) = after {
            self.check_ref(after)?;
            if after.kind != RecordKind::Action {
                return Err(ContextError::InvalidInput(
                    "action cursor has the wrong family".into(),
                ));
            }
            record_key(after)
        } else {
            prefix.clone()
        };
        let mut actions = Vec::new();
        let mut next = None;
        for (scanned, entry) in self.parts.heads.range(start.clone()..).enumerate() {
            let (key, revision) = entry?;
            if !key.starts_with(&prefix) {
                break;
            }
            if after.is_some() && key.as_ref() == start {
                continue;
            }
            if actions.len() == limit || scanned == 4096 {
                return Ok(ActionPage { actions, next });
            }
            if key.len() != prefix.len() + 16 || revision.len() != 8 {
                return Err(ContextError::Corrupt("invalid action index".into()));
            }
            let id = u128::from_be_bytes(
                key[prefix.len()..]
                    .try_into()
                    .map_err(|_| ContextError::Corrupt("invalid action identity".into()))?,
            );
            let reference = ContextRef {
                era: self.db.era_id,
                owner: self.access.owner.clone(),
                kind: RecordKind::Action,
                id: ulid::Ulid(id),
            };
            next = Some(reference.clone());
            let header = match self.head_locked(&reference) {
                Ok(header) => header,
                Err(ContextError::AccessDenied) => continue,
                Err(error) => return Err(error),
            };
            if !self.available_locked(&header.pin, true)? {
                continue;
            }
            let definition = self.action_locked(&header.pin)?;
            if target_record.header.type_pin.as_ref() != Some(&definition.target_type) {
                continue;
            }
            let preconditions_met = conditions_match(&definition.preconditions, &properties);
            actions.push(ActionOffer {
                pin: header.pin,
                definition,
                preconditions_met,
            });
        }
        Ok(ActionPage {
            actions,
            next: None,
        })
    }

    /// A durable execution intent. Only a trusted harness may create it.
    /// Replaying the operation returns the original receipt; inspect its HEAD
    /// before deciding whether a previous invocation already settled.
    pub fn prepare_action(
        &self,
        operation: OperationId,
        invocation: ActionInvocation,
    ) -> ContextResult<WriteReceipt> {
        Ok(self.prepare_action_once(operation, invocation)?.receipt)
    }

    pub fn prepare_action_once(
        &self,
        operation: OperationId,
        invocation: ActionInvocation,
    ) -> ContextResult<ActionPreparation> {
        let _guard = self.db.write_lock.lock();
        self.require_action_harness()?;
        let digest = self.digest("prepare-action", &invocation)?;
        if let Some(receipt) = self.replay(operation, digest)? {
            return Ok(ActionPreparation {
                receipt,
                already_present: true,
            });
        }
        let definition = self.action_locked(&invocation.action)?;
        if !self.available_locked(&invocation.action, true)? {
            return Err(ContextError::Unavailable(invocation.action.record.clone()));
        }
        if self.head_locked(&invocation.action.record)?.pin != invocation.action {
            return Err(ContextError::InvalidInput("action version changed".into()));
        }
        let target = self.current_action_target(&invocation.target)?;
        if target.header.type_pin.as_ref() != Some(&definition.target_type) {
            return Err(ContextError::InvalidInput(
                "action target type version does not match".into(),
            ));
        }
        if definition.scope != Scope::Personal && definition.scope != target.header.scope {
            return Err(ContextError::AccessDenied);
        }
        let RecordBody::Object(properties) = target.body else {
            return Err(ContextError::InvalidInput(
                "action target is not an object".into(),
            ));
        };
        if !conditions_match(&definition.preconditions, &properties) {
            return Err(ContextError::InvalidInput(
                "action preconditions are not satisfied".into(),
            ));
        }
        let input_type = TypeDefinition {
            name: definition.name,
            scope: definition.scope,
            kind: TypeKind::Object,
            properties: definition.inputs,
        };
        self.validate_properties(&input_type, &invocation.inputs, &target.header.scope)?;
        let mut sources = invocation.sources.clone();
        sources.push(invocation.action.clone());
        sources.push(invocation.target.clone());
        self.validate_sources(
            &target.header.scope,
            &sources,
            InformationKind::Observation,
            None,
        )?;
        let pin = self.fresh_pin(RecordKind::ActionExecution);
        let execution = ActionExecution {
            invocation,
            status: ActionStatus::Prepared,
            result: Value::Null,
            result_sources: Vec::new(),
        };
        let bytes = encode(&execution)?;
        if bytes.len() > MAX_RECORD_BYTES {
            return Err(ContextError::BudgetExceeded);
        }
        let header = self.action_header(
            pin.clone(),
            target.header.scope,
            sources,
            InformationKind::Observation,
            &bytes,
        );
        let mut batch = self.db.durable_batch();
        self.put_payload(&mut batch, &pin, &bytes);
        self.put_header(&mut batch, &header)?;
        Ok(ActionPreparation {
            receipt: self.commit_operation(
                batch,
                operation,
                digest,
                WriteReceipt {
                    pin,
                    history_sequence: None,
                },
            )?,
            already_present: false,
        })
    }

    /// Stores the actual harness outcome. A prepared or unknown write must
    /// never be automatically re-executed merely because its result is absent.
    pub fn finish_action(
        &self,
        operation: OperationId,
        expected: RecordPin,
        status: ActionStatus,
        result: Value,
        result_sources: Vec<RecordPin>,
    ) -> ContextResult<WriteReceipt> {
        let _guard = self.db.write_lock.lock();
        self.require_action_harness()?;
        let digest = self.digest(
            "finish-action",
            &(&expected, status, &result, &result_sources),
        )?;
        if let Some(receipt) = self.replay(operation, digest)? {
            return Ok(receipt);
        }
        if expected.record.kind != RecordKind::ActionExecution || status == ActionStatus::Prepared {
            return Err(ContextError::InvalidInput(
                "invalid action outcome transition".into(),
            ));
        }
        let (previous, pin) = self.next_revision(&expected)?;
        // A target may have changed as a consequence of this action. Its exact
        // previous revision remains provenance, without asserting it is current.
        let mut execution: ActionExecution = decode(&self.small_payload_locked(&previous)?)?;
        if execution.status != ActionStatus::Prepared {
            return Err(ContextError::InvalidInput(
                "action already has a terminal outcome".into(),
            ));
        }
        self.validate_sources(
            &previous.scope,
            &result_sources,
            InformationKind::Observation,
            Some(&expected.record),
        )?;
        execution.status = status;
        execution.result = result;
        execution.result_sources = result_sources.clone();
        let bytes = encode(&execution)?;
        if bytes.len() > MAX_RECORD_BYTES {
            return Err(ContextError::BudgetExceeded);
        }
        let mut sources = previous.sources;
        sources.extend(result_sources);
        if sources.len() > MAX_CONTEXT_SOURCES {
            return Err(ContextError::BudgetExceeded);
        }
        let header = self.action_header(
            pin.clone(),
            previous.scope,
            sources,
            InformationKind::Observation,
            &bytes,
        );
        let mut batch = self.db.durable_batch();
        self.advance_availability_epoch(&mut batch)?;
        self.put_payload(&mut batch, &pin, &bytes);
        self.put_header(&mut batch, &header)?;
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

    fn require_action_harness(&self) -> ContextResult<()> {
        if matches!(self.access.actor, Actor::System | Actor::Operator) {
            Ok(())
        } else {
            Err(ContextError::AccessDenied)
        }
    }

    fn action_locked(&self, pin: &RecordPin) -> ContextResult<ActionDefinition> {
        if pin.record.kind != RecordKind::Action {
            return Err(ContextError::InvalidInput(
                "expected an action definition".into(),
            ));
        }
        if !self.available_locked(pin, false)? {
            return Err(ContextError::Unavailable(pin.record.clone()));
        }
        decode(&self.small_payload_locked(&self.header_locked(pin)?)?)
    }

    fn current_action_target(&self, pin: &RecordPin) -> ContextResult<ContextRecord> {
        if pin.record.kind != RecordKind::Object || self.head_locked(&pin.record)?.pin != *pin {
            return Err(ContextError::InvalidInput(
                "action target must be the current object version".into(),
            ));
        }
        if !self.available_locked(pin, true)?
            || !self
                .header_locked(pin)?
                .temporal
                .contains_valid_time(super::super::now_ms())
        {
            return Err(ContextError::Unavailable(pin.record.clone()));
        }
        self.read_locked(pin)
    }

    fn action_header(
        &self,
        pin: RecordPin,
        scope: Scope,
        sources: Vec<RecordPin>,
        information: InformationKind,
        bytes: &[u8],
    ) -> RecordHeader {
        let now = super::super::now_ms();
        RecordHeader {
            pin: pin.clone(),
            scope,
            state: RecordState::Active,
            type_pin: None,
            sources,
            provenance: self.provenance(),
            information,
            temporal: TemporalFacts::observed_at(now),
            recorded_at_ms: now,
            payload: PayloadDescriptor {
                revision: pin.revision,
                bytes: bytes.len() as u64,
                digest: self.content_digest(bytes),
            },
            endpoints: None,
            history: None,
            checkpoint: None,
        }
    }
}

fn conditions_match(conditions: &[ActionCondition], properties: &Properties) -> bool {
    conditions.iter().all(|condition| match condition {
        ActionCondition::Equals { property, value } => properties.get(property) == Some(value),
        ActionCondition::Exists { property } => properties.contains_key(property),
    })
}
