pub(crate) mod codec;
mod error;
mod lifecycle;
mod payload;
mod scoped;
mod types;

pub use error::*;
pub(crate) use scoped::*;
pub use types::*;

use serde::{Deserialize, Serialize};
use std::fmt;
use ulid::Ulid;

pub const MAX_ACCESS_SCOPES: usize = 32;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Actor {
    User,
    Assistant,
    System,
    Operator,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum Scope {
    Personal,
    Workspace(Ulid),
    Session(Ulid),
    Artifact(Ulid),
    System,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct TemporalFacts {
    pub observed_at_ms: i64,
    pub source_at_ms: Option<i64>,
    pub valid_from_ms: Option<i64>,
    pub valid_to_ms: Option<i64>,
}

impl TemporalFacts {
    pub fn observed_at(observed_at_ms: i64) -> Self {
        Self {
            observed_at_ms,
            source_at_ms: None,
            valid_from_ms: None,
            valid_to_ms: None,
        }
    }

    pub(crate) fn validate(self) -> Result<(), InvalidTemporalFacts> {
        if matches!((self.valid_from_ms, self.valid_to_ms), (Some(from), Some(to)) if from >= to) {
            return Err(InvalidTemporalFacts);
        }
        Ok(())
    }

    /// Whether `at_ms` falls inside the record's explicit valid-time window.
    ///
    /// This deliberately only reasons about *valid time* (when the fact holds in
    /// the world). It is distinct from `observed_at_ms`, which records when the
    /// observation was captured, and from `recorded_at_ms` on the header, which
    /// is when the row was written to the store. The three concepts are kept on
    /// separate fields so they can never be conflated.
    pub fn is_valid_at(&self, at_ms: i64) -> bool {
        self.valid_from_ms.is_none_or(|from| at_ms >= from)
            && self.valid_to_ms.is_none_or(|to| at_ms < to)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RecordState {
    Active,
    Retracted,
    Superseded,
    Unsupported,
    Purged,
}

#[derive(Debug)]
pub(crate) struct InvalidTemporalFacts;

impl fmt::Display for InvalidTemporalFacts {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("valid time must use a non-empty [from, to) interval")
    }
}

pub const PAYLOAD_CHUNK_BYTES: usize = 32 * 1024;
pub const MAX_PAYLOAD_PAGE_BYTES: usize = 1024 * 1024;
pub const MAX_RECORD_BYTES: usize = 128 * 1024;
pub const MAX_CONTEXT_NAME_BYTES: usize = 256;
pub const MAX_CONTEXT_SOURCES: usize = 64;

impl ContextAccess {
    pub(crate) fn validate(&self) -> ContextResult<()> {
        codec::validate_name(&self.owner, "owner")?;
        for value in [&self.agent, &self.session, &self.run, &self.parent_run]
            .into_iter()
            .flatten()
        {
            codec::validate_name(value, "provenance identity")?;
        }
        if self.parent_run.is_some() && (self.run.is_none() || self.parent_run == self.run) {
            return Err(ContextError::InvalidInput(
                "a child run requires a distinct parent identity".into(),
            ));
        }
        if self.scopes.is_empty() || self.scopes.len() > MAX_ACCESS_SCOPES {
            return Err(ContextError::InvalidInput(
                "provide 1..=32 allowed scopes".into(),
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StoreModule {
    Session,
    Memory,
}

impl StoreModule {
    pub(crate) const fn id(self) -> mmdb_kernel::ModuleId {
        match self {
            Self::Session => mmdb_kernel::ModuleId("session"),
            Self::Memory => mmdb_kernel::ModuleId("memory"),
        }
    }
}

impl RecordKind {
    pub(crate) fn require_module(self, module: StoreModule) -> ContextResult<()> {
        if self.module() != module {
            return Err(ContextError::InvalidInput(format!(
                "expected a {module:?} record"
            )));
        }
        Ok(())
    }

    pub fn module(self) -> StoreModule {
        match self {
            Self::Event | Self::Checkpoint => StoreModule::Session,
            _ => StoreModule::Memory,
        }
    }
}

pub(crate) struct StreamedPayload {
    pub descriptor: PayloadDescriptor,
    upload: Option<(RecordPin, mmdb_kernel::Partition)>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::StoreFormatDescriptor;
    use mmdb_kernel::{Kernel, ModuleId, ModuleSpec};

    struct FixedGate(bool);

    impl Gate for FixedGate {
        fn admits(
            &self,
            _: &str,
            _: &RecordHeader,
            _: bool,
            _: &Availability,
        ) -> ContextResult<bool> {
            Ok(self.0)
        }
    }

    #[test]
    fn only_session_kinds_require_the_gate() {
        const MEMORY: ModuleId = ModuleId("memory");
        const SESSION: ModuleId = ModuleId("session");
        const MODULES: &[ModuleSpec] = &[
            ModuleSpec {
                id: MEMORY,
                partitions: MEMORY_PARTITIONS,
            },
            ModuleSpec {
                id: SESSION,
                partitions: SESSION_PARTITIONS,
            },
        ];
        let tmp = tempfile::tempdir().unwrap();
        let format = StoreFormatDescriptor::new("records-test").unwrap();
        let kernel = Kernel::create(&tmp.path().join("store"), &format, MODULES).unwrap();
        let memory = kernel.module(MEMORY);
        let session = kernel.module(SESSION);
        let records = Records::open(kernel, &session, &memory, Box::new(FixedGate(false))).unwrap();
        let scoped = records.scoped(ContextAccess::new("owner", Actor::System));
        let mut batch = scoped.batch();
        let mut pins = Vec::new();
        for kind in [
            RecordKind::Object,
            RecordKind::Event,
            RecordKind::Checkpoint,
        ] {
            let pin = scoped.fresh_pin(kind);
            let payload = scoped.put_payload(&mut batch, &pin, b"{}");
            scoped
                .put_header(
                    &mut batch,
                    &RecordHeader {
                        pin: pin.clone(),
                        scope: Scope::Personal,
                        state: RecordState::Active,
                        type_pin: None,
                        sources: vec![],
                        source_scopes: vec![],
                        provenance: scoped.provenance(),
                        information: InformationKind::Observation,
                        temporal: TemporalFacts::observed_at(1),
                        recorded_at_ms: 1,
                        payload,
                        endpoints: None,
                        event: None,
                        checkpoint: None,
                    },
                )
                .unwrap();
            pins.push(pin);
        }
        batch.commit().unwrap();
        for pin in pins {
            for active in [false, true] {
                assert_eq!(
                    scoped.available_locked(&pin, active).unwrap(),
                    pin.record.kind == RecordKind::Object
                );
            }
        }
    }
}
