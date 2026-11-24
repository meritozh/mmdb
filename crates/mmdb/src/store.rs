use crate::context::{self, ContextParts, ContextStore};
use crate::native_memory::{self, MemoryDatabase, MemoryError};
use crate::records::{self, Records};
use crate::session::{self, LogGate, SessionLog, SessionParts};
use crate::{
    ContextAccess, ContextError, ContextResult, EraId, StoreFormatDescriptor, StoreModule,
};
use mmdb_kernel::{Kernel, KernelError, ModuleId, ModuleSpec};
use std::fmt;
use std::path::Path;

pub const STORE_FORMAT_ID: &str = "mmdb-store-dev9";
const SESSION: ModuleId = ModuleId("session");
const MEMORY: ModuleId = ModuleId("memory");

macro_rules! partitions {
    ($($group:path),+) => {{
        let mut names = [""; 0 $(+ $group.len())+];
        let mut remaining = names.as_mut_slice();
        $(
            let (group, rest) = remaining.split_at_mut($group.len());
            group.copy_from_slice($group);
            remaining = rest;
        )+
        assert!(remaining.is_empty());
        names
    }};
}

const MODULES: &[ModuleSpec] = &[
    ModuleSpec {
        id: SESSION,
        partitions: &partitions!(records::SESSION_PARTITIONS, session::SESSION_PARTITIONS),
    },
    ModuleSpec {
        id: MEMORY,
        partitions: &partitions!(
            records::MEMORY_PARTITIONS,
            context::MEMORY_PARTITIONS,
            native_memory::NATIVE_PARTITIONS
        ),
    },
];

/// A leased store containing the session log, domain context and native memory.
pub struct Store {
    kernel: Kernel,
    records: Records,
    context: ContextParts,
    session: SessionParts,
    memory: MemoryDatabase,
}

impl Store {
    /// Initialize a new store at an absent path.
    pub fn create(root: impl AsRef<Path>) -> StoreResult<Self> {
        let format = StoreFormatDescriptor::new(STORE_FORMAT_ID).map_err(KernelError::Format)?;
        Self::from_kernel(Kernel::create(root.as_ref(), &format, MODULES)?)
    }

    /// Open an existing directory with the exact store format.
    pub fn open(root: impl AsRef<Path>) -> StoreResult<Self> {
        let format = StoreFormatDescriptor::new(STORE_FORMAT_ID).map_err(KernelError::Format)?;
        Self::from_kernel(Kernel::open(root.as_ref(), &format, MODULES)?)
    }

    fn from_kernel(kernel: Kernel) -> StoreResult<Self> {
        let session = kernel.module(SESSION);
        let memory = kernel.module(MEMORY);
        let context = ContextParts::open(&memory)?;
        let records = Records::open(
            kernel.clone(),
            &session,
            &memory,
            Box::new(LogGate::open(&session)?),
        )?;
        let session = SessionParts::open(&session)?;
        let memory = MemoryDatabase::on_era(kernel.clone(), &memory)?;
        Ok(Self {
            kernel,
            records,
            context,
            session,
            memory,
        })
    }

    pub fn era(&self, module: StoreModule) -> EraId {
        self.kernel.era(match module {
            StoreModule::Session => SESSION,
            StoreModule::Memory => MEMORY,
        })
    }

    pub fn session_log(&self, access: ContextAccess) -> ContextResult<SessionLog<'_>> {
        access.validate()?;
        Ok(SessionLog {
            scoped: self.records.scoped(access),
            parts: &self.session,
        })
    }

    pub fn context(&self, access: ContextAccess) -> ContextResult<ContextStore<'_>> {
        access.validate()?;
        Ok(ContextStore {
            scoped: self.records.scoped(access),
            parts: &self.context,
        })
    }

    pub fn memory(&self) -> &MemoryDatabase {
        &self.memory
    }
}

#[derive(Debug)]
pub enum StoreError {
    Kernel(KernelError),
    Context(ContextError),
    Memory(MemoryError),
}

pub type StoreResult<T> = Result<T, StoreError>;

impl fmt::Display for StoreError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Kernel(error) => error.fmt(f),
            Self::Context(error) => error.fmt(f),
            Self::Memory(error) => error.fmt(f),
        }
    }
}

impl std::error::Error for StoreError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Kernel(error) => Some(error),
            Self::Context(error) => Some(error),
            Self::Memory(error) => Some(error),
        }
    }
}

impl From<KernelError> for StoreError {
    fn from(error: KernelError) -> Self {
        Self::Kernel(error)
    }
}

impl From<MemoryError> for StoreError {
    fn from(error: MemoryError) -> Self {
        Self::Memory(error)
    }
}

impl From<ContextError> for StoreError {
    fn from(error: ContextError) -> Self {
        Self::Context(error)
    }
}
