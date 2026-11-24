use super::storage::*;
use super::*;
use crate::{OperationId, RecordState, Scope, TemporalFacts};
use serde_json::Value;
use std::collections::BTreeMap;

fn name_key(owner: &str, scope: &Scope, name: &str) -> ContextResult<Vec<u8>> {
    let mut key = owner_key(owner);
    segment(&mut key, &encode(scope)?);
    segment(&mut key, name.as_bytes());
    Ok(key)
}

impl ContextStore<'_> {
    pub fn define_type(
        &self,
        operation: OperationId,
        definition: TypeDefinition,
    ) -> ContextResult<WriteReceipt> {
        let _guard = self.scoped.lock();
        self.validate_definition(&definition)?;
        let digest = self.scoped.digest("define-type", &definition)?;
        if let Some(receipt) = self.scoped.replay(operation, digest)? {
            return Ok(receipt);
        }
        let key = name_key(
            &self.scoped.access().owner,
            &definition.scope,
            &definition.name,
        )?;
        if let Some(reference) = self.parts.names.get(&key)? {
            let record: ContextRef = decode(&reference)?;
            let header = self.scoped.head_locked(&record)?;
            let current = self.type_definition_locked(&header.pin)?;
            if current != definition {
                return Err(ContextError::TypeConflict(definition.name));
            }
            return self.scoped.commit_operation(
                self.scoped.batch(),
                operation,
                digest,
                WriteReceipt {
                    pin: header.pin,
                    history_sequence: None,
                },
            );
        }
        self.write_type_locked(
            operation,
            digest,
            self.scoped.fresh_pin(RecordKind::Type),
            definition,
        )
    }

    /// Look up a type by name under the already-held write lock, creating a
    /// minimal one when absent. Callers (`crate::entity`, `crate::extraction`)
    /// already hold `write_lock`, so this must not re-acquire it.
    pub(crate) fn ensure_type_locked(
        &self,
        name: &str,
        kind: TypeKind,
    ) -> ContextResult<RecordPin> {
        let key = name_key(&self.scoped.access().owner, &Scope::Personal, name)?;
        if let Some(reference) = self.parts.names.get(&key)? {
            let record: ContextRef = decode(&reference)?;
            return self.scoped.head_locked(&record).map(|header| header.pin);
        }
        let definition = TypeDefinition {
            name: name.to_string(),
            scope: Scope::Personal,
            kind,
            properties: BTreeMap::new(),
        };
        let receipt = self.write_type_locked(
            OperationId::new(),
            self.scoped.digest("define-type", &definition)?,
            self.scoped.fresh_pin(RecordKind::Type),
            definition,
        )?;
        Ok(receipt.pin)
    }

    pub fn revise_type(
        &self,
        operation: OperationId,
        expected: RecordPin,
        definition: TypeDefinition,
    ) -> ContextResult<WriteReceipt> {
        let _guard = self.scoped.lock();
        self.validate_definition(&definition)?;
        let digest = self
            .scoped
            .digest("revise-type", &(&expected, &definition))?;
        if let Some(receipt) = self.scoped.replay(operation, digest)? {
            return Ok(receipt);
        }
        let (_, pin) = self.scoped.next_revision(&expected)?;
        let previous = self.type_definition_locked(&expected)?;
        if previous.name != definition.name
            || previous.scope != definition.scope
            || std::mem::discriminant(&previous.kind) != std::mem::discriminant(&definition.kind)
        {
            return Err(ContextError::InvalidInput(
                "type name, scope and category are immutable".into(),
            ));
        }
        self.write_type_locked(operation, digest, pin, definition)
    }

    fn write_type_locked(
        &self,
        operation: OperationId,
        digest: [u8; 32],
        pin: RecordPin,
        definition: TypeDefinition,
    ) -> ContextResult<WriteReceipt> {
        let bytes = encode(&definition)?;
        let now = super::super::now_ms();
        let mut batch = self.scoped.batch();
        let header = RecordHeader {
            pin: pin.clone(),
            scope: definition.scope.clone(),
            state: RecordState::Active,
            type_pin: None,
            sources: Vec::new(),
            source_scopes: Vec::new(),
            provenance: self.scoped.provenance(),
            information: InformationKind::Observation,
            temporal: TemporalFacts::observed_at(now),
            recorded_at_ms: now,
            payload: self.scoped.put_payload(&mut batch, &pin, &bytes),
            endpoints: None,
            history: None,
            checkpoint: None,
        };
        if pin.revision > 1 {
            self.scoped.advance_epoch(&mut batch)?;
        }
        self.scoped.put_header(&mut batch, &header)?;
        batch.insert(
            &self.parts.names,
            name_key(
                &self.scoped.access().owner,
                &definition.scope,
                &definition.name,
            )?,
            encode(&pin.record)?,
        );
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

    pub fn find_type(
        &self,
        scope: &Scope,
        name: &str,
    ) -> ContextResult<Option<(RecordPin, TypeDefinition)>> {
        let _guard = self.scoped.lock();
        self.scoped.check_scope(scope)?;
        validate_name(name, "type name")?;
        let Some(bytes) =
            self.parts
                .names
                .get(name_key(&self.scoped.access().owner, scope, name)?)?
        else {
            return Ok(None);
        };
        let reference = decode(&bytes)?;
        let header = self.scoped.head_locked(&reference)?;
        Ok(Some((
            header.pin.clone(),
            self.type_definition_locked(&header.pin)?,
        )))
    }

    pub fn list_types(&self, limit: usize, after: Option<&ContextRef>) -> ContextResult<TypePage> {
        if limit == 0 || limit > 100 {
            return Err(ContextError::InvalidInput(
                "type page limit must be 1..=100".into(),
            ));
        }
        let _guard = self.scoped.lock();
        let mut definitions = Vec::new();
        let mut after = after.cloned();
        let mut remaining = 4096;
        loop {
            let page = self.scoped.scan_heads(
                RecordKind::Type,
                after.as_ref(),
                (limit - definitions.len()).min(remaining),
            )?;
            remaining -= page.heads.len();
            for pin in page.heads {
                let header = match self.scoped.header_locked(&pin) {
                    Ok(header) => header,
                    Err(ContextError::AccessDenied) => continue,
                    Err(e) => return Err(e),
                };
                definitions.push((
                    header.pin.clone(),
                    self.type_definition_locked(&header.pin)?,
                ));
            }
            if page.next.is_none() || remaining == 0 || definitions.len() == limit {
                return Ok(TypePage {
                    definitions,
                    next: page.next,
                });
            }
            after = page.next;
        }
    }

    pub(crate) fn type_definition_locked(&self, pin: &RecordPin) -> ContextResult<TypeDefinition> {
        if pin.record.kind != RecordKind::Type {
            return Err(ContextError::InvalidInput(
                "expected a type reference".into(),
            ));
        }
        let header = self.scoped.header_locked(pin)?;
        if header.state != RecordState::Active {
            return Err(ContextError::Unavailable(pin.record.clone()));
        }
        decode(&self.scoped.small_payload_locked(&header)?)
    }

    pub(crate) fn validate_definition(&self, definition: &TypeDefinition) -> ContextResult<()> {
        self.scoped.check_scope(&definition.scope)?;
        validate_name(&definition.name, "type name")?;
        if definition.properties.len() > 128 || encode(definition)?.len() > MAX_RECORD_BYTES {
            return Err(ContextError::InvalidInput(
                "type definition exceeds its size bound".into(),
            ));
        }
        for (name, property) in &definition.properties {
            validate_name(name, "property name")?;
            self.validate_property_type(&property.value_type, 0)?;
        }
        if let TypeKind::Relation {
            from_types,
            to_types,
        } = &definition.kind
        {
            if from_types.len() > 64 || to_types.len() > 64 {
                return Err(ContextError::InvalidInput(
                    "too many relation endpoint types".into(),
                ));
            }
            for reference in from_types.iter().chain(to_types) {
                self.validate_object_type_ref(reference)?;
            }
        }
        Ok(())
    }

    fn validate_property_type(&self, kind: &PropertyType, depth: usize) -> ContextResult<()> {
        if depth > 1 {
            return Err(ContextError::InvalidInput(
                "nested property lists are not supported".into(),
            ));
        }
        match kind {
            PropertyType::Reference {
                target_type: Some(reference),
            } => self.validate_object_type_ref(reference),
            PropertyType::List { element } => self.validate_property_type(element, depth + 1),
            _ => Ok(()),
        }
    }

    fn validate_object_type_ref(&self, reference: &ContextRef) -> ContextResult<()> {
        let header = self.scoped.head_locked(reference)?;
        if !matches!(
            self.type_definition_locked(&header.pin)?.kind,
            TypeKind::Object
        ) {
            return Err(ContextError::InvalidInput(
                "relation and reference targets require object types".into(),
            ));
        }
        Ok(())
    }

    pub(crate) fn validate_properties(
        &self,
        definition: &TypeDefinition,
        properties: &Properties,
        scope: &Scope,
    ) -> ContextResult<()> {
        if encode(properties)?.len() > MAX_RECORD_BYTES {
            return Err(ContextError::InvalidInput(
                "structured context exceeds 128 KiB; retain large originals in history".into(),
            ));
        }
        for (name, property) in &definition.properties {
            match properties.get(name) {
                Some(value) => self.validate_value(&property.value_type, value, scope)?,
                None if property.required => {
                    return Err(ContextError::InvalidInput(format!(
                        "required property missing: {name}"
                    )))
                }
                None => {}
            }
        }
        if properties
            .keys()
            .any(|key| !definition.properties.contains_key(key))
        {
            return Err(ContextError::InvalidInput("unknown property".into()));
        }
        Ok(())
    }

    pub(crate) fn validate_value(
        &self,
        kind: &PropertyType,
        value: &Value,
        scope: &Scope,
    ) -> ContextResult<()> {
        let valid = match kind {
            PropertyType::Text => value.is_string(),
            PropertyType::Boolean => value.is_boolean(),
            PropertyType::Integer | PropertyType::Timestamp => value.as_i64().is_some(),
            PropertyType::Number => value.as_f64().is_some_and(f64::is_finite),
            PropertyType::Reference { target_type } => {
                let pin: RecordPin = serde_json::from_value(value.clone()).map_err(|_| {
                    ContextError::InvalidInput(
                        "reference property needs a versioned record reference".into(),
                    )
                })?;
                let header = self.scoped.header_locked(&pin)?;
                if header.scope != *scope && header.scope != Scope::Personal {
                    return Err(ContextError::AccessDenied);
                }
                if !self.scoped.available_locked(&pin, true)? {
                    return Err(ContextError::Unavailable(pin.record));
                }
                target_type.as_ref().is_none_or(|expected| {
                    header
                        .type_pin
                        .as_ref()
                        .is_some_and(|actual| actual.record == *expected)
                })
            }
            PropertyType::List { element } => {
                let values = value.as_array().ok_or_else(|| {
                    ContextError::InvalidInput("list property needs an array".into())
                })?;
                if values.len() > 1024 {
                    return Err(ContextError::InvalidInput(
                        "property list exceeds 1024 items".into(),
                    ));
                }
                for value in values {
                    self.validate_value(element, value, scope)?;
                }
                true
            }
        };
        if !valid {
            return Err(ContextError::InvalidInput(
                "property does not match its declared type".into(),
            ));
        }
        Ok(())
    }
}
