use super::*;
pub(crate) use crate::records::codec::*;
pub(crate) fn session_key(owner: &str, session: &str) -> Vec<u8> {
    let mut key = owner_key(owner);
    segment(&mut key, session.as_bytes());
    key
}

impl ContextStore<'_> {
    pub fn head(&self, record: &ContextRef) -> ContextResult<RecordHeader> {
        let _guard = self.scoped.lock();
        self.scoped.head_locked(record)
    }

    pub fn inspect_header(&self, pin: &RecordPin) -> ContextResult<RecordHeader> {
        let _guard = self.scoped.lock();
        self.scoped.header_locked(pin)
    }

    pub fn read_payload(
        &self,
        pin: &RecordPin,
        offset: u64,
        limit: usize,
    ) -> ContextResult<PayloadPage> {
        let _guard = self.scoped.lock();
        let header = self.scoped.header_locked(pin)?;
        if !self.scoped.available_locked(pin, false)? {
            return Err(ContextError::Unavailable(pin.record.clone()));
        }
        self.scoped.payload_slice_locked(&header, offset, limit)
    }

    pub(crate) fn read_locked(&self, pin: &RecordPin) -> ContextResult<ContextRecord> {
        let header = self.scoped.header_locked(pin)?;
        if !self.scoped.available_locked(pin, false)? {
            return Err(ContextError::Unavailable(pin.record.clone()));
        }
        let body = match pin.record.kind {
            RecordKind::Type => {
                RecordBody::Type(decode(&self.scoped.small_payload_locked(&header)?)?)
            }
            RecordKind::Object => {
                RecordBody::Object(decode(&self.scoped.small_payload_locked(&header)?)?)
            }
            RecordKind::Relation => {
                RecordBody::Relation(decode(&self.scoped.small_payload_locked(&header)?)?)
            }
            RecordKind::History => RecordBody::History(
                header
                    .history
                    .clone()
                    .ok_or_else(|| ContextError::Corrupt("history metadata missing".into()))?,
            ),
            RecordKind::Action => {
                RecordBody::Action(decode(&self.scoped.small_payload_locked(&header)?)?)
            }
            RecordKind::ActionExecution => {
                RecordBody::ActionExecution(decode(&self.scoped.small_payload_locked(&header)?)?)
            }
            RecordKind::Checkpoint => {
                RecordBody::Checkpoint(decode(&self.scoped.small_payload_locked(&header)?)?)
            }
        };
        let current_state = self.scoped.head_locked(&pin.record)?.state;
        Ok(ContextRecord {
            header,
            current_state,
            body,
        })
    }

    pub fn read(&self, pin: &RecordPin) -> ContextResult<ContextRecord> {
        let _guard = self.scoped.lock();
        self.read_locked(pin)
    }

    pub fn operation_receipt(
        &self,
        operation: crate::OperationId,
    ) -> ContextResult<Option<WriteReceipt>> {
        self.scoped.operation_receipt(operation)
    }
}
