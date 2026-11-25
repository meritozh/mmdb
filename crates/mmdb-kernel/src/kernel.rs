use crate::store_format::{
    require_managed_store, StoreEraId, StoreFormatDescriptor, StoreFormatError, StoreLease,
};
use fjall::{Database as FjallDatabase, Keyspace, PersistMode};
use parking_lot::{Mutex, MutexGuard, RwLock, RwLockReadGuard};
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet, HashSet},
    fmt, fs,
    ops::{Deref, RangeBounds},
    path::Path,
    sync::Arc,
};
use ulid::Ulid;

const KERNEL_PARTITION: &str = "kernel_v1";
const STORE_KEY: &[u8] = b"store";
const RETIRED_PREFIX: &str = "retired.";

macro_rules! id_type {
    ($name:ident) => {
        #[derive(
            Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
        )]
        pub struct $name(pub Ulid);

        impl $name {
            pub fn new() -> Self {
                Self(Ulid::new())
            }

            pub fn as_ulid(self) -> Ulid {
                self.0
            }
        }

        impl Default for $name {
            fn default() -> Self {
                Self::new()
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                self.0.fmt(f)
            }
        }
    };
}

id_type!(EraId);
id_type!(OperationId);

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ModuleId(pub &'static str);

pub struct ModuleSpec {
    pub id: ModuleId,
    pub partitions: &'static [PartitionSpec],
}

/// Physical layout for one named partition, applied when an era is created.
#[derive(Clone, Copy)]
pub enum PartitionSpec {
    Inline(&'static str),
    LargeValues(&'static str),
}

impl PartitionSpec {
    fn name(self) -> &'static str {
        match self {
            Self::Inline(name) | Self::LargeValues(name) => name,
        }
    }

    fn options(self) -> fjall::KeyspaceCreateOptions {
        let options = fjall::KeyspaceCreateOptions::default();
        match self {
            Self::Inline(_) => options,
            Self::LargeValues(_) => options.with_kv_separation(Some(
                fjall::KvSeparationOptions::default().separation_threshold(8 * 1024),
            )),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EraState {
    Current,
    Retired,
    Unknown,
}

#[derive(Debug, thiserror::Error)]
pub enum KernelError {
    #[error(transparent)]
    Format(#[from] StoreFormatError),
    #[error("storage error: {0}")]
    Storage(String),
    #[error("corrupt store: {0}")]
    Corrupt(String),
    #[error("invalid module specification: {0}")]
    InvalidModuleSpec(String),
    #[error("unknown module {0}")]
    UnknownModule(&'static str),
    #[error("unknown partition {module}.{name}")]
    UnknownPartition {
        module: &'static str,
        name: &'static str,
    },
    #[error("invalid new module era {0}")]
    InvalidEra(EraId),
    #[error("module {module} no longer has era {era}")]
    StaleEra { module: &'static str, era: EraId },
    #[error("batch commit outcome is unknown")]
    CommitUnknown,
    #[error("kernel must be reopened after an uncertain reset")]
    Poisoned,
}

pub type KernelResult<T> = Result<T, KernelError>;

impl From<fjall::Error> for KernelError {
    fn from(error: fjall::Error) -> Self {
        Self::Storage(error.to_string())
    }
}

struct Schema(BTreeMap<&'static str, BTreeMap<&'static str, PartitionSpec>>);

impl Schema {
    fn new(specs: &'static [ModuleSpec]) -> KernelResult<Self> {
        let mut modules = BTreeMap::new();
        for spec in specs {
            let module = spec.id.0;
            if !valid_name(module) || modules.contains_key(module) {
                return Err(KernelError::InvalidModuleSpec(format!(
                    "invalid or duplicate module {module:?}"
                )));
            }
            let mut partitions = BTreeMap::new();
            for &partition in spec.partitions {
                let name = partition.name();
                // Two separators and one 26-character ULID are part of the on-disk name.
                if !valid_name(name)
                    || module.len() + 28 + name.len() > 255
                    || partitions.insert(name, partition).is_some()
                {
                    return Err(KernelError::InvalidModuleSpec(format!(
                        "invalid or duplicate partition {module}.{name}"
                    )));
                }
            }
            modules.insert(module, partitions);
        }
        Ok(Self(modules))
    }

    fn parse_partition<'a>(&self, qualified: &'a str) -> KernelResult<(&'a str, EraId)> {
        let corrupt = || KernelError::Corrupt(format!("unrecognized partition {qualified:?}"));
        let (module, rest) = qualified.split_once('.').ok_or_else(corrupt)?;
        let (era, name) = rest.split_once('.').ok_or_else(corrupt)?;
        if !self
            .0
            .get(module)
            .is_some_and(|names| names.contains_key(name))
        {
            return Err(corrupt());
        }
        let id = era.parse::<Ulid>().map_err(|_| corrupt())?;
        if id.to_string() != era {
            return Err(corrupt());
        }
        Ok((module, EraId(id)))
    }

    fn open_partitions(
        &self,
        database: &FjallDatabase,
        eras: &BTreeMap<String, EraId>,
    ) -> KernelResult<()> {
        let mut obsolete = Vec::new();
        for qualified in database.list_keyspace_names() {
            if qualified.as_ref() == KERNEL_PARTITION {
                continue;
            }
            let (module, era) = self.parse_partition(&qualified)?;
            if era != eras[module] {
                obsolete.push(qualified);
            }
        }
        for name in obsolete {
            database.delete_keyspace(database.keyspace(&name, Default::default)?)?;
        }
        for &module in self.0.keys() {
            self.open_module(database, ModuleId(module), eras[module])?;
        }
        Ok(())
    }

    fn open_module(
        &self,
        database: &FjallDatabase,
        module: ModuleId,
        era: EraId,
    ) -> KernelResult<()> {
        for (&name, spec) in &self.0[module.0] {
            database.keyspace(&partition_name(module.0, era, name), || spec.options())?;
        }
        Ok(())
    }
}

fn valid_name(name: &str) -> bool {
    !name.is_empty()
        && name
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_')
}

fn partition_name(module: &str, era: EraId, name: &str) -> String {
    format!("{module}.{era}.{name}")
}

#[derive(Serialize, Deserialize)]
struct StoreMetadata {
    store_era: String,
    digest_key: [u8; 32],
    eras: BTreeMap<String, EraId>,
}

impl StoreMetadata {
    fn load(
        database: &FjallDatabase,
        store_era: &StoreEraId,
        schema: &Schema,
    ) -> KernelResult<(Self, Keyspace)> {
        let has_modules = database
            .list_keyspace_names()
            .iter()
            .any(|name| name.as_ref() != KERNEL_PARTITION);
        if has_modules && !database.keyspace_exists(KERNEL_PARTITION) {
            return Err(KernelError::Corrupt(
                "kernel metadata partition is missing".into(),
            ));
        }
        let partition = database.keyspace(KERNEL_PARTITION, Default::default)?;
        if let Some(bytes) = partition.get(STORE_KEY)? {
            let metadata: Self =
                serde_json::from_slice(&bytes).map_err(|e| KernelError::Corrupt(e.to_string()))?;
            if metadata.store_era != store_era.as_str() {
                return Err(KernelError::Corrupt(
                    "kernel store era does not match the manifest".into(),
                ));
            }
            if metadata
                .eras
                .keys()
                .map(String::as_str)
                .ne(schema.0.keys().copied())
            {
                return Err(KernelError::Corrupt(
                    "kernel modules do not match the format".into(),
                ));
            }
            return Ok((metadata, partition));
        }
        // Module partitions appear only after the first metadata commit, so a store
        // without them is new or its first open stopped before that commit.
        if has_modules || !partition.is_empty()? {
            return Err(KernelError::Corrupt(
                "kernel store metadata is missing".into(),
            ));
        }
        let mut digest_key = [0; 32];
        getrandom::fill(&mut digest_key).map_err(|e| KernelError::Storage(e.to_string()))?;
        let metadata = Self {
            store_era: store_era.to_string(),
            digest_key,
            eras: schema
                .0
                .keys()
                .map(|&module| (module.to_owned(), EraId::new()))
                .collect(),
        };
        let mut batch = durable_batch(database);
        metadata.stage(&mut batch, &partition)?;
        batch.commit().map_err(|_| KernelError::CommitUnknown)?;
        Ok((metadata, partition))
    }

    fn stage(&self, batch: &mut fjall::OwnedWriteBatch, partition: &Keyspace) -> KernelResult<()> {
        let encoded = serde_json::to_vec(self).map_err(|e| KernelError::Storage(e.to_string()))?;
        batch.insert(partition, STORE_KEY, encoded);
        Ok(())
    }
}

struct ModuleEras {
    current: BTreeMap<String, EraId>,
    retired: HashSet<(ModuleId, EraId)>,
    poisoned: bool,
}

impl ModuleEras {
    fn load(
        current: BTreeMap<String, EraId>,
        partition: &Keyspace,
        schema: &Schema,
    ) -> KernelResult<Self> {
        let mut retired = HashSet::new();
        for pair in partition.prefix(RETIRED_PREFIX) {
            let (key, value) = pair.into_inner()?;
            let corrupt = || KernelError::Corrupt(format!("invalid retired era key {key:?}"));
            let key = std::str::from_utf8(&key).map_err(|_| corrupt())?;
            let (module, era) = key
                .strip_prefix(RETIRED_PREFIX)
                .and_then(|suffix| suffix.split_once('.'))
                .ok_or_else(corrupt)?;
            let (&module, _) = schema.0.get_key_value(module).ok_or_else(corrupt)?;
            let id = EraId(era.parse().map_err(|_| corrupt())?);
            if !value.is_empty() || id.to_string() != era || current[module] == id {
                return Err(corrupt());
            }
            retired.insert((ModuleId(module), id));
        }
        Ok(Self {
            current,
            retired,
            poisoned: false,
        })
    }

    fn state(&self, module: ModuleId, era: EraId) -> EraState {
        if self.current.get(module.0) == Some(&era) {
            EraState::Current
        } else if self.retired.contains(&(module, era)) {
            EraState::Retired
        } else {
            EraState::Unknown
        }
    }

    fn ensure_usable(&self) -> KernelResult<()> {
        if self.poisoned {
            return Err(KernelError::Poisoned);
        }
        Ok(())
    }

    fn ensure_new(&self, module: ModuleId, era: EraId) -> KernelResult<()> {
        if self.state(module, era) != EraState::Unknown {
            return Err(KernelError::InvalidEra(era));
        }
        Ok(())
    }
}

fn durable_batch(database: &FjallDatabase) -> fjall::OwnedWriteBatch {
    database.batch().durability(Some(PersistMode::SyncAll))
}

#[derive(Clone)]
pub struct Kernel(Arc<Inner>);

struct Inner {
    metadata: Keyspace,
    database: FjallDatabase,
    schema: Schema,
    eras: RwLock<ModuleEras>,
    store_era: StoreEraId,
    digest_key: [u8; 32],
    write_lock: Mutex<()>,
    #[cfg(any(test, feature = "test-hooks"))]
    lose_ack_partition: Mutex<Option<String>>,
    #[cfg(test)]
    reset_fault: Mutex<Option<tests::ResetFault>>,
    // Keep the path leased until all engine handles have been dropped.
    lease: StoreLease,
}

impl Kernel {
    pub fn create(
        root: &Path,
        format: &StoreFormatDescriptor,
        modules: &'static [ModuleSpec],
    ) -> KernelResult<Self> {
        let schema = Schema::new(modules)?;
        let lease = StoreLease::acquire(root)?;
        fs::create_dir(root).map_err(|source| StoreFormatError::Io {
            operation: "create store directory",
            path: root.to_owned(),
            source,
        })?;
        format.new_manifest()?.write_new(root)?;
        Self::open_with_lease(root, format, schema, lease)
    }

    pub fn open(
        root: &Path,
        format: &StoreFormatDescriptor,
        modules: &'static [ModuleSpec],
    ) -> KernelResult<Self> {
        let schema = Schema::new(modules)?;
        let lease = StoreLease::acquire(root)?;
        Self::open_with_lease(root, format, schema, lease)
    }

    fn open_with_lease(
        root: &Path,
        format: &StoreFormatDescriptor,
        schema: Schema,
        lease: StoreLease,
    ) -> KernelResult<Self> {
        let managed = require_managed_store(root, format.format_id())?;
        crate::engine_format::inspect(
            managed.canonical_root(),
            &[crate::store_format::STORE_MANIFEST_FILE],
        )
        .map_err(|e| KernelError::Storage(e.to_string()))?;
        let database = FjallDatabase::builder(managed.canonical_root()).open()?;
        let store_era = managed.manifest().store_era_id().clone();
        let (metadata, partition) = StoreMetadata::load(&database, &store_era, &schema)?;
        let eras = ModuleEras::load(metadata.eras, &partition, &schema)?;
        schema.open_partitions(&database, &eras.current)?;
        Ok(Self(Arc::new(Inner {
            metadata: partition,
            database,
            schema,
            store_era,
            lease,
            eras: RwLock::new(eras),
            digest_key: metadata.digest_key,
            write_lock: Mutex::new(()),
            #[cfg(any(test, feature = "test-hooks"))]
            lose_ack_partition: Mutex::new(None),
            #[cfg(test)]
            reset_fault: Mutex::new(None),
        })))
    }

    pub fn canonical_root(&self) -> &Path {
        self.0.lease.canonical_root()
    }

    pub fn store_era(&self) -> &StoreEraId {
        &self.0.store_era
    }

    /// # Panics
    ///
    /// When `module` is not one of the kernel's module specifications.
    pub fn era(&self, module: ModuleId) -> EraId {
        let eras = self.0.eras.read();
        let Some(&era) = eras.current.get(module.0) else {
            panic!("module {} is not registered with this kernel", module.0);
        };
        era
    }

    pub fn era_state(&self, module: ModuleId, era: EraId) -> EraState {
        self.0.eras.read().state(module, era)
    }

    /// # Panics
    ///
    /// When `module` is not one of the kernel's module specifications.
    pub fn module(&self, module: ModuleId) -> EraPartitions {
        EraPartitions {
            kernel: self.clone(),
            module,
            era: self.era(module),
        }
    }

    pub fn batch(&self) -> Batch {
        Batch {
            raw: durable_batch(&self.0.database),
            touched: BTreeSet::new(),
            #[cfg(any(test, feature = "test-hooks"))]
            written_partitions: BTreeSet::new(),
            kernel: self.clone(),
        }
    }

    /// The next successful batch commit that writes this partition reports
    /// `CommitUnknown` after its writes land. Arming again replaces the target.
    #[cfg(any(test, feature = "test-hooks"))]
    pub fn lose_next_commit_ack(&self, partition: &Partition) {
        assert!(
            Arc::ptr_eq(&self.0, &partition.owner.kernel.0),
            "partition belongs to another kernel"
        );
        *self.0.lose_ack_partition.lock() = Some(partition.raw.name().to_string());
    }

    pub fn write_lock(&self) -> WriteGuard<'_> {
        WriteGuard {
            _guard: self.0.write_lock.lock(),
        }
    }

    pub fn keyed_hasher(&self, domain: &'static str) -> blake3::Hasher {
        let mut hasher = blake3::Hasher::new_keyed(&self.0.digest_key);
        hasher.update(&(domain.len() as u64).to_be_bytes());
        hasher.update(domain.as_bytes());
        hasher
    }

    /// Create empty partitions for the replacement era while the old era stays current.
    pub fn prepare_module_reset(
        &self,
        module: ModuleId,
        new_era: EraId,
    ) -> KernelResult<EraPartitions> {
        let eras = self.0.eras.read();
        eras.ensure_usable()?;
        if !self.0.schema.0.contains_key(module.0) {
            return Err(KernelError::UnknownModule(module.0));
        }
        eras.ensure_new(module, new_era)?;
        self.0
            .schema
            .open_module(&self.0.database, module, new_era)?;
        Ok(EraPartitions {
            kernel: self.clone(),
            module,
            era: new_era,
        })
    }

    /// Durably switch eras and apply the caller's writes as one batch. Any remaining
    /// old partitions are logically deleted on the next open.
    pub fn commit_module_reset(
        &self,
        prepared: EraPartitions,
        mut batch: Batch,
    ) -> KernelResult<ModuleResetReport> {
        let mut eras = self.0.eras.write();
        eras.ensure_usable()?;
        assert!(
            Arc::ptr_eq(&self.0, &prepared.kernel.0),
            "prepared era belongs to another kernel"
        );
        assert!(
            Arc::ptr_eq(&self.0, &batch.kernel.0),
            "batch belongs to another kernel"
        );
        let module = prepared.module;
        let new_era = prepared.era;
        eras.ensure_new(module, new_era)?;
        batch.check_eras(&eras)?;
        let mut current = eras.current.clone();
        let old_era = std::mem::replace(
            current.get_mut(module.0).expect("registered module"),
            new_era,
        );
        let metadata = StoreMetadata {
            store_era: self.0.store_era.to_string(),
            digest_key: self.0.digest_key,
            eras: current,
        };
        metadata.stage(&mut batch.raw, &self.0.metadata)?;
        batch.raw.insert(
            &self.0.metadata,
            format!("{RETIRED_PREFIX}{}.{old_era}", module.0),
            [],
        );
        if let Err(error) = self.commit_reset_batch(batch.raw) {
            eras.poisoned = true;
            return Err(error);
        }
        eras.current = metadata.eras;
        eras.retired.insert((module, old_era));
        drop(eras);
        let cleanup_pending = self.delete_old_partitions(module, old_era).is_err();
        Ok(ModuleResetReport {
            old_era,
            new_era,
            cleanup_pending,
        })
    }

    fn commit_reset_batch(&self, batch: fjall::OwnedWriteBatch) -> KernelResult<()> {
        #[cfg(test)]
        if self.0.reset_fault.lock().as_ref() == Some(&tests::ResetFault::UnappliedCommit) {
            return Err(KernelError::CommitUnknown);
        }
        batch.commit().map_err(|_| KernelError::CommitUnknown)?;
        #[cfg(test)]
        if self.0.reset_fault.lock().as_ref() == Some(&tests::ResetFault::LostCommitAck) {
            return Err(KernelError::CommitUnknown);
        }
        Ok(())
    }

    fn delete_old_partitions(&self, module: ModuleId, era: EraId) -> KernelResult<()> {
        for &name in self.0.schema.0[module.0].keys() {
            #[cfg(test)]
            if self.0.reset_fault.lock().as_ref() == Some(&tests::ResetFault::DeletePartition(name))
            {
                return Err(KernelError::Storage("interrupted partition cleanup".into()));
            }
            let qualified = partition_name(module.0, era, name);
            let handle = self.0.database.keyspace(&qualified, Default::default)?;
            self.0.database.delete_keyspace(handle)?;
        }
        Ok(())
    }
}

#[derive(Debug)]
pub struct ModuleResetReport {
    pub old_era: EraId,
    pub new_era: EraId,
    pub cleanup_pending: bool,
}

pub struct WriteGuard<'a> {
    _guard: MutexGuard<'a, ()>,
}

#[derive(Clone)]
pub struct EraPartitions {
    kernel: Kernel,
    module: ModuleId,
    era: EraId,
}

impl EraPartitions {
    pub fn era(&self) -> EraId {
        self.era
    }

    pub fn partition(&self, name: &'static str) -> KernelResult<Partition> {
        let _eras = self.readable()?;
        let spec = self.kernel.0.schema.0[self.module.0].get(name).ok_or(
            KernelError::UnknownPartition {
                module: self.module.0,
                name,
            },
        )?;
        let qualified = partition_name(self.module.0, self.era, name);
        let raw = self
            .kernel
            .0
            .database
            .keyspace(&qualified, || spec.options())?;
        Ok(Partition {
            raw,
            owner: self.clone(),
        })
    }

    fn readable(&self) -> KernelResult<RwLockReadGuard<'_, ModuleEras>> {
        let eras = self.kernel.0.eras.read();
        eras.ensure_usable()?;
        if eras.retired.contains(&(self.module, self.era)) {
            return Err(KernelError::StaleEra {
                module: self.module.0,
                era: self.era,
            });
        }
        Ok(eras)
    }
}

#[derive(Clone)]
pub struct Partition {
    raw: Keyspace,
    owner: EraPartitions,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Bytes(fjall::Slice);

impl Deref for Bytes {
    type Target = [u8];
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl AsRef<[u8]> for Bytes {
    fn as_ref(&self) -> &[u8] {
        self
    }
}

fn read_pair(pair: fjall::Guard) -> KernelResult<(Bytes, Bytes)> {
    let (key, value) = pair.into_inner()?;
    Ok((Bytes(key), Bytes(value)))
}

fn read_key(row: fjall::Guard) -> KernelResult<Bytes> {
    Ok(Bytes(row.key()?))
}

impl Partition {
    pub fn get(&self, key: impl AsRef<[u8]>) -> KernelResult<Option<Bytes>> {
        let _eras = self.owner.readable()?;
        Ok(self.raw.get(key)?.map(Bytes))
    }

    pub fn contains_key(&self, key: impl AsRef<[u8]>) -> KernelResult<bool> {
        let _eras = self.owner.readable()?;
        Ok(self.raw.contains_key(key)?)
    }

    pub fn iter(&self) -> impl DoubleEndedIterator<Item = KernelResult<(Bytes, Bytes)>> + '_ {
        self.scan(|| self.raw.iter(), read_pair)
    }

    pub fn range<K: AsRef<[u8]>, R: RangeBounds<K>>(
        &self,
        range: R,
    ) -> impl DoubleEndedIterator<Item = KernelResult<(Bytes, Bytes)>> + '_ {
        self.scan(|| self.raw.range(range), read_pair)
    }

    pub fn prefix(
        &self,
        prefix: impl AsRef<[u8]>,
    ) -> impl DoubleEndedIterator<Item = KernelResult<(Bytes, Bytes)>> + '_ {
        self.scan(|| self.raw.prefix(prefix), read_pair)
    }

    /// Scan matching keys without fetching separated values. As with row scans,
    /// the first era or storage error terminates iteration in both directions.
    pub fn prefix_keys(
        &self,
        prefix: impl AsRef<[u8]>,
    ) -> impl DoubleEndedIterator<Item = KernelResult<Bytes>> + '_ {
        self.scan(|| self.raw.prefix(prefix), read_key)
    }

    pub fn range_keys<K: AsRef<[u8]>, R: RangeBounds<K>>(
        &self,
        range: R,
    ) -> impl DoubleEndedIterator<Item = KernelResult<Bytes>> + '_ {
        self.scan(|| self.raw.range(range), read_key)
    }

    pub fn prefix_values(
        &self,
        prefix: impl AsRef<[u8]>,
    ) -> impl DoubleEndedIterator<Item = KernelResult<Bytes>> + '_ {
        self.scan(|| self.raw.prefix(prefix), |row| Ok(Bytes(row.value()?)))
    }

    fn scan<T>(
        &self,
        open: impl FnOnce() -> fjall::Iter,
        project: fn(fjall::Guard) -> KernelResult<T>,
    ) -> PartitionScan<'_, T> {
        PartitionScan {
            owner: &self.owner,
            rows: Some(self.owner.readable().map(|_eras| open())),
            project,
        }
    }
}

// A caller may retain a scan across a reset. Check each read without holding
// the era lock while the caller processes a row.
struct PartitionScan<'a, T> {
    owner: &'a EraPartitions,
    rows: Option<KernelResult<fjall::Iter>>,
    project: fn(fjall::Guard) -> KernelResult<T>,
}

impl<T> PartitionScan<'_, T> {
    fn advance(
        &mut self,
        next: impl FnOnce(&mut fjall::Iter) -> Option<fjall::Guard>,
    ) -> Option<KernelResult<T>> {
        let row = match self.rows.as_mut()? {
            Err(_) => return self.rows.take()?.err().map(Err),
            Ok(rows) => self
                .owner
                .readable()
                .and_then(|_eras| next(rows).map(self.project).transpose()),
        };
        if !matches!(row, Ok(Some(_))) {
            self.rows = None;
        }
        row.transpose()
    }
}

impl<T> Iterator for PartitionScan<'_, T> {
    type Item = KernelResult<T>;

    fn next(&mut self) -> Option<Self::Item> {
        self.advance(Iterator::next)
    }
}

impl<T> DoubleEndedIterator for PartitionScan<'_, T> {
    fn next_back(&mut self) -> Option<Self::Item> {
        self.advance(DoubleEndedIterator::next_back)
    }
}

pub struct Batch {
    raw: fjall::OwnedWriteBatch,
    touched: BTreeSet<(ModuleId, EraId)>,
    #[cfg(any(test, feature = "test-hooks"))]
    written_partitions: BTreeSet<String>,
    kernel: Kernel,
}

impl Batch {
    fn touch(&mut self, partition: &Partition) {
        let owner = &partition.owner;
        assert!(
            Arc::ptr_eq(&self.kernel.0, &owner.kernel.0),
            "partition belongs to another kernel"
        );
        self.touched.insert((owner.module, owner.era));
        #[cfg(any(test, feature = "test-hooks"))]
        self.written_partitions
            .insert(partition.raw.name().to_string());
    }

    pub fn insert(
        &mut self,
        partition: &Partition,
        key: impl AsRef<[u8]>,
        value: impl AsRef<[u8]>,
    ) {
        self.touch(partition);
        self.raw
            .insert(&partition.raw, key.as_ref(), value.as_ref());
    }

    pub fn remove(&mut self, partition: &Partition, key: impl AsRef<[u8]>) {
        self.touch(partition);
        self.raw.remove(&partition.raw, key.as_ref());
    }

    fn check_eras(&self, eras: &ModuleEras) -> KernelResult<()> {
        for &(module, era) in &self.touched {
            if eras.current[module.0] != era {
                return Err(KernelError::StaleEra {
                    module: module.0,
                    era,
                });
            }
        }
        Ok(())
    }

    pub fn commit(self) -> KernelResult<()> {
        let eras = self.kernel.0.eras.read();
        eras.ensure_usable()?;
        self.check_eras(&eras)?;
        #[cfg(any(test, feature = "test-hooks"))]
        let mut armed = self.kernel.0.lose_ack_partition.lock();
        self.raw.commit().map_err(|_| KernelError::CommitUnknown)?;
        #[cfg(any(test, feature = "test-hooks"))]
        if armed
            .as_ref()
            .is_some_and(|name| self.written_partitions.contains(name))
        {
            *armed = None;
            return Err(KernelError::CommitUnknown);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests;
