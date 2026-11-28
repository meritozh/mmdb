use super::*;
use crate::OperationId;
use serde::{de::DeserializeOwned, Serialize};

pub(crate) fn validate_name(value: &str, label: &str) -> ContextResult<()> {
    if value.trim().is_empty()
        || value.len() > MAX_CONTEXT_NAME_BYTES
        || value.chars().any(char::is_control)
    {
        return Err(ContextError::InvalidInput(format!(
            "{label} must have 1..={MAX_CONTEXT_NAME_BYTES} bytes without control characters"
        )));
    }
    Ok(())
}

pub(crate) fn segment(key: &mut Vec<u8>, value: &[u8]) {
    key.extend_from_slice(&(value.len() as u32).to_be_bytes());
    key.extend_from_slice(value);
}

pub(crate) fn owner_key(owner: &str) -> Vec<u8> {
    let mut key = Vec::new();
    segment(&mut key, owner.as_bytes());
    key
}

pub(crate) fn kind_byte(kind: RecordKind) -> u8 {
    match kind {
        RecordKind::Type => 0,
        RecordKind::Object => 1,
        RecordKind::Relation => 2,
        RecordKind::Event => 3,
        RecordKind::Checkpoint => 4,
        RecordKind::Action => 5,
        RecordKind::ActionExecution => 6,
        RecordKind::Session => 7,
    }
}

pub(crate) fn record_key(record: &ContextRef) -> Vec<u8> {
    let mut key = owner_key(&record.owner);
    key.push(kind_byte(record.kind));
    key.extend_from_slice(&record.id.0.to_be_bytes());
    key
}

pub(crate) fn revision_key(pin: &RecordPin) -> Vec<u8> {
    let mut key = record_key(&pin.record);
    key.extend_from_slice(&pin.revision.to_be_bytes());
    key
}

#[cfg(test)]
pub(crate) fn payload_key(pin: &RecordPin, chunk: u64) -> Vec<u8> {
    let mut key = revision_key(pin);
    key.extend_from_slice(&chunk.to_be_bytes());
    key
}

pub(crate) fn operation_key(owner: &str, operation: OperationId) -> Vec<u8> {
    let mut key = owner_key(owner);
    key.extend_from_slice(&operation.0 .0.to_be_bytes());
    key
}

pub(crate) fn decode<T: DeserializeOwned>(bytes: &[u8]) -> ContextResult<T> {
    serde_json::from_slice(bytes)
        .map_err(|_| ContextError::Corrupt("invalid persisted record".into()))
}

pub(crate) fn encode<T: Serialize>(value: &T) -> ContextResult<Vec<u8>> {
    Ok(serde_json::to_vec(value)?)
}
