use super::*;
pub(crate) use crate::records::codec::*;

impl ContextStore<'_> {
    pub fn head(&self, record: &ContextRef) -> ContextResult<RecordHeader> {
        record.kind.require_module(StoreModule::Memory)?;
        let _guard = self.scoped.lock();
        self.scoped.head_locked(record)
    }

    pub fn inspect_header(&self, pin: &RecordPin) -> ContextResult<RecordHeader> {
        pin.record.kind.require_module(StoreModule::Memory)?;
        let _guard = self.scoped.lock();
        self.scoped.header_locked(pin)
    }

    pub fn read_payload(
        &self,
        pin: &RecordPin,
        offset: u64,
        limit: usize,
    ) -> ContextResult<PayloadPage> {
        pin.record.kind.require_module(StoreModule::Memory)?;
        let _guard = self.scoped.lock();
        let header = self.scoped.readable_header_locked(pin)?;
        self.scoped.payload_slice_locked(&header, offset, limit)
    }

    pub(crate) fn read_locked(&self, pin: &RecordPin) -> ContextResult<ContextRecord> {
        let header = self.scoped.readable_header_locked(pin)?;
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
            RecordKind::Action => {
                RecordBody::Action(decode(&self.scoped.small_payload_locked(&header)?)?)
            }
            RecordKind::ActionExecution => {
                RecordBody::ActionExecution(decode(&self.scoped.small_payload_locked(&header)?)?)
            }
            RecordKind::History | RecordKind::Checkpoint => {
                return Err(ContextError::InvalidInput(
                    "expected a memory record".into(),
                ));
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
        pin.record.kind.require_module(StoreModule::Memory)?;
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
