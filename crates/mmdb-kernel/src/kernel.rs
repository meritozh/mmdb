use crate::store_format::{
    directory_contains_only_manifest, require_managed_store, StoreEraId, StoreFormatDescriptor,
    StoreFormatError, StoreLease,
};
use fjall::{Config, Keyspace, PartitionHandle, PersistMode};
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
    pub partitions: &'static [&'static str],
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

struct Schema(BTreeMap<&'static str, BTreeSet<&'static str>>);

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
            let mut partitions = BTreeSet::new();
            for &name in spec.partitions {
                // Two separators and one 26-character ULID are part of the on-disk name.
                if !valid_name(name)
                    || module.len() + 28 + name.len() > 255
                    || !partitions.insert(name)
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
        if !self.0.get(module).is_some_and(|names| names.contains(name)) {
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
        keyspace: &Keyspace,
        eras: &BTreeMap<String, EraId>,
    ) -> KernelResult<()> {
        let mut obsolete = Vec::new();
        for qualified in keyspace.list_partitions() {
            if qualified.as_ref() == KERNEL_PARTITION {
                continue;
            }
            let (module, era) = self.parse_partition(&qualified)?;
            if era != eras[module] {
                obsolete.push(qualified);
            }
        }
        for name in obsolete {
            keyspace.delete_partition(keyspace.open_partition(&name, Default::default())?)?;
        }
        for &module in self.0.keys() {
            self.open_module(keyspace, ModuleId(module), eras[module])?;
        }
        Ok(())
    }

    fn open_module(&self, keyspace: &Keyspace, module: ModuleId, era: EraId) -> KernelResult<()> {
        for name in &self.0[module.0] {
            keyspace.open_partition(&partition_name(module.0, era, name), Default::default())?;
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
        keyspace: &Keyspace,
        store_era: &StoreEraId,
        schema: &Schema,
        pristine: bool,
    ) -> KernelResult<(Self, PartitionHandle)> {
        if !pristine && !keyspace.partition_exists(KERNEL_PARTITION) {
            return Err(KernelError::Corrupt(
                "kernel metadata partition is missing".into(),
            ));
        }
        let partition = keyspace.open_partition(KERNEL_PARTITION, Default::default())?;
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
        if !pristine {
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
        let mut batch = durable_batch(keyspace);
        metadata.stage(&mut batch, &partition)?;
        batch.commit().map_err(|_| KernelError::CommitUnknown)?;
        Ok((metadata, partition))
    }

    fn stage(&self, batch: &mut fjall::Batch, partition: &PartitionHandle) -> KernelResult<()> {
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
        partition: &PartitionHandle,
        schema: &Schema,
    ) -> KernelResult<Self> {
        let mut retired = HashSet::new();
        for pair in partition.prefix(RETIRED_PREFIX) {
            let (key, value) = pair?;
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
        if self.current[module.0] == era {
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

fn durable_batch(keyspace: &Keyspace) -> fjall::Batch {
    keyspace.batch().durability(Some(PersistMode::SyncAll))
}

#[derive(Clone)]
pub struct Kernel(Arc<Inner>);

struct Inner {
    metadata: PartitionHandle,
    keyspace: Keyspace,
    schema: Schema,
    eras: RwLock<ModuleEras>,
    store_era: StoreEraId,
    digest_key: [u8; 32],
    write_lock: Mutex<()>,
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
        let pristine = directory_contains_only_manifest(managed.canonical_root())?;
        let keyspace = Config::new(managed.canonical_root()).open()?;
        let store_era = managed.manifest().store_era_id().clone();
        let (metadata, partition) = StoreMetadata::load(&keyspace, &store_era, &schema, pristine)?;
        let eras = ModuleEras::load(metadata.eras, &partition, &schema)?;
        schema.open_partitions(&keyspace, &eras.current)?;
        Ok(Self(Arc::new(Inner {
            metadata: partition,
            keyspace,
            schema,
            store_era,
            lease,
            eras: RwLock::new(eras),
            digest_key: metadata.digest_key,
            write_lock: Mutex::new(()),
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

    pub fn era(&self, module: ModuleId) -> EraId {
        self.0.eras.read().current[module.0]
    }

    pub fn era_state(&self, module: ModuleId, era: EraId) -> EraState {
        self.0.eras.read().state(module, era)
    }

    pub fn module(&self, module: ModuleId) -> EraPartitions {
        EraPartitions {
            kernel: self.clone(),
            module,
            era: self.era(module),
        }
    }

    pub fn batch(&self) -> Batch {
        Batch {
            raw: durable_batch(&self.0.keyspace),
            touched: BTreeSet::new(),
            kernel: self.clone(),
        }
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
        eras.ensure_new(module, new_era)?;
        self.0
            .schema
            .open_module(&self.0.keyspace, module, new_era)?;
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

    fn commit_reset_batch(&self, batch: fjall::Batch) -> KernelResult<()> {
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
        for name in &self.0.schema.0[module.0] {
            #[cfg(test)]
            if self.0.reset_fault.lock().as_ref() == Some(&tests::ResetFault::DeletePartition(name))
            {
                return Err(KernelError::Storage("interrupted partition cleanup".into()));
            }
            let qualified = partition_name(module.0, era, name);
            let handle = self
                .0
                .keyspace
                .open_partition(&qualified, Default::default())?;
            self.0.keyspace.delete_partition(handle)?;
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
        if !self.kernel.0.schema.0[self.module.0].contains(name) {
            return Err(KernelError::UnknownPartition {
                module: self.module.0,
                name,
            });
        }
        let qualified = partition_name(self.module.0, self.era, name);
        let raw = self
            .kernel
            .0
            .keyspace
            .open_partition(&qualified, Default::default())?;
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
    raw: PartitionHandle,
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

fn read_pair(pair: fjall::Result<fjall::KvPair>) -> KernelResult<(Bytes, Bytes)> {
    let (key, value) = pair?;
    Ok((Bytes(key), Bytes(value)))
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
        self.scan(|| self.raw.iter())
    }

    pub fn range<K: AsRef<[u8]>, R: RangeBounds<K>>(
        &self,
        range: R,
    ) -> impl DoubleEndedIterator<Item = KernelResult<(Bytes, Bytes)>> + '_ {
        self.scan(|| self.raw.range(range))
    }

    pub fn prefix(
        &self,
        prefix: impl AsRef<[u8]>,
    ) -> impl DoubleEndedIterator<Item = KernelResult<(Bytes, Bytes)>> + '_ {
        self.scan(|| self.raw.prefix(prefix))
    }

    fn scan<I>(&self, open: impl FnOnce() -> I) -> PartitionScan<'_, I> {
        PartitionScan {
            owner: &self.owner,
            rows: Some(self.owner.readable().map(|_eras| open())),
        }
    }
}

// A caller may retain a scan across a reset. Check each read without holding
// the era lock while the caller processes a row.
struct PartitionScan<'a, I> {
    owner: &'a EraPartitions,
    rows: Option<KernelResult<I>>,
}

impl<I: DoubleEndedIterator<Item = fjall::Result<fjall::KvPair>>> PartitionScan<'_, I> {
    fn advance(
        &mut self,
        next: impl FnOnce(&mut I) -> Option<I::Item>,
    ) -> Option<KernelResult<(Bytes, Bytes)>> {
        let row = match self.rows.as_mut()? {
            Err(_) => return self.rows.take()?.err().map(Err),
            Ok(rows) => self
                .owner
                .readable()
                .and_then(|_eras| next(rows).map(read_pair).transpose()),
        };
        if !matches!(row, Ok(Some(_))) {
            self.rows = None;
        }
        row.transpose()
    }
}

impl<I: DoubleEndedIterator<Item = fjall::Result<fjall::KvPair>>> Iterator
    for PartitionScan<'_, I>
{
    type Item = KernelResult<(Bytes, Bytes)>;

    fn next(&mut self) -> Option<Self::Item> {
        self.advance(Iterator::next)
    }
}

impl<I: DoubleEndedIterator<Item = fjall::Result<fjall::KvPair>>> DoubleEndedIterator
    for PartitionScan<'_, I>
{
    fn next_back(&mut self) -> Option<Self::Item> {
        self.advance(DoubleEndedIterator::next_back)
    }
}

pub struct Batch {
    raw: fjall::Batch,
    touched: BTreeSet<(ModuleId, EraId)>,
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
        self.raw.commit().map_err(|_| KernelError::CommitUnknown)
    }
}

#[cfg(test)]
mod tests;
