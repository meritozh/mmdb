//! Clean-store format recognition, bounded reset planning, and recoverable
//! reset transactions.
//!
//! Reset never deletes the old store. It moves that era to a deterministic
//! quarantine under an exclusive lease and keeps a durable receipt so an
//! interrupted caller or later store open can finish the same transaction.

use std::fmt;
use std::fs::{self, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use ulid::Ulid;

#[cfg(unix)]
use std::os::unix::ffi::OsStrExt;
#[cfg(unix)]
use std::os::unix::fs::MetadataExt;

pub const STORE_MANIFEST_FILE: &str = ".mmdb-clean-store";
const STORE_MANIFEST_MAGIC: &str = "MMDB-CLEAN-STORE";
const STORE_MANIFEST_VERSION: u32 = 1;
const MAX_STORE_MANIFEST_BYTES: u64 = 4 * 1024;
const RESET_PLAN_VERSION: u32 = 1;
const RESET_JOURNAL_MAGIC: &str = "MMDB-RESET-JOURNAL";
const RESET_JOURNAL_VERSION: u32 = 1;
const MAX_RESET_JOURNAL_BYTES: u64 = 16 * 1024;
pub const MAX_RESET_PLAN_TTL: Duration = Duration::from_secs(15 * 60);
const MAX_RESET_SNAPSHOT_ENTRIES: u64 = 1_000_000;
const MAX_RESET_SNAPSHOT_FILE_BYTES: u64 = 1024 * 1024 * 1024 * 1024;
const MAX_RESET_SNAPSHOT_PATH_BYTES: u64 = 256 * 1024 * 1024;

const WORKSPACE_MARKERS: &[&str] = &[
    ".git",
    ".hg",
    ".jj",
    ".svn",
    "Cargo.toml",
    "go.work",
    "package.json",
    "pnpm-workspace.yaml",
    "pyproject.toml",
];

/// Opaque identity for one clean-store era.
///
/// A new era is required whenever the store is explicitly reset. The textual
/// representation is a canonical, uppercase ULID. mmdb owns generation so a
/// caller-supplied format never controls or accidentally reuses store identity.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct StoreEraId(String);

impl StoreEraId {
    pub fn new() -> Self {
        Self(Ulid::new().to_string())
    }

    pub fn parse(value: impl Into<String>) -> StoreFormatResult<Self> {
        let value = value.into();
        let valid = value.len() == 26
            && value.as_bytes().first().is_some_and(|byte| *byte <= b'7')
            && value.bytes().all(|byte| {
                byte.is_ascii_digit()
                    || matches!(
                        byte,
                        b'A'..=b'H' | b'J' | b'K' | b'M' | b'N' | b'P'..=b'T' | b'V'..=b'Z'
                    )
            });
        if !valid {
            return Err(StoreFormatError::InvalidStoreEraId(value));
        }
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl Default for StoreEraId {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for StoreEraId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl FromStr for StoreEraId {
    type Err = StoreFormatError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::parse(value)
    }
}

/// Caller-owned identity for one exact on-disk store contract.
///
/// A descriptor names the application format only. It is deliberately not a
/// store instance identifier: every newly initialized root receives a fresh
/// mmdb-owned [`StoreEraId`]. Reopening with a different descriptor is always
/// rejected before the storage engine sees the path.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct StoreFormatDescriptor {
    format_id: String,
}

impl StoreFormatDescriptor {
    pub fn new(format_id: impl Into<String>) -> StoreFormatResult<Self> {
        let format_id = format_id.into();
        validate_format_id(&format_id)?;
        Ok(Self { format_id })
    }

    pub fn format_id(&self) -> &str {
        &self.format_id
    }

    pub(crate) fn new_manifest(&self) -> StoreFormatResult<StoreManifest> {
        StoreManifest::new(self.format_id.clone(), StoreEraId::new())
    }
}

/// The only marker that makes a non-empty root recognizable as managed.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StoreManifest {
    format_id: String,
    store_era_id: StoreEraId,
}

impl StoreManifest {
    pub fn new(format_id: impl Into<String>, store_era_id: StoreEraId) -> StoreFormatResult<Self> {
        let format_id = format_id.into();
        validate_format_id(&format_id)?;
        Ok(Self {
            format_id,
            store_era_id,
        })
    }

    pub fn format_id(&self) -> &str {
        &self.format_id
    }

    pub fn store_era_id(&self) -> &StoreEraId {
        &self.store_era_id
    }

    /// Write the marker once, and only into an existing empty directory.
    pub fn write_new(&self, root: impl AsRef<Path>) -> StoreFormatResult<()> {
        let inspected = inspect_store_root(root.as_ref())?;
        if !matches!(inspected.state(), StoreRootState::Empty) {
            return Err(StoreFormatError::StoreRootIsNotEmpty(
                inspected.canonical_root,
            ));
        }

        let marker = inspected.canonical_root.join(STORE_MANIFEST_FILE);
        let mut file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&marker)
            .map_err(|source| StoreFormatError::Io {
                operation: "create store manifest",
                path: marker.clone(),
                source,
            })?;
        file.write_all(self.encode().as_bytes())
            .map_err(|source| StoreFormatError::Io {
                operation: "write store manifest",
                path: marker.clone(),
                source,
            })?;
        file.sync_all().map_err(|source| StoreFormatError::Io {
            operation: "sync store manifest",
            path: marker,
            source,
        })?;
        sync_directory(&inspected.canonical_root)?;
        Ok(())
    }

    fn encode(&self) -> String {
        format!(
            "{STORE_MANIFEST_MAGIC}\nmanifest-version:{STORE_MANIFEST_VERSION}\nformat-id:{}\nstore-era-id:{}\n",
            self.format_id, self.store_era_id
        )
    }

    fn decode(path: &Path, encoded: &str) -> StoreFormatResult<Self> {
        let mut lines = encoded.lines();
        let magic = lines.next();
        let version = lines
            .next()
            .and_then(|line| line.strip_prefix("manifest-version:"))
            .and_then(|value| value.parse::<u32>().ok());
        let format_id = lines.next();
        let era_id = lines.next();
        if magic != Some(STORE_MANIFEST_MAGIC)
            || version != Some(STORE_MANIFEST_VERSION)
            || lines.next().is_some()
        {
            return Err(StoreFormatError::MalformedManifest(path.to_path_buf()));
        }
        let format_id = format_id
            .and_then(|line| line.strip_prefix("format-id:"))
            .ok_or_else(|| StoreFormatError::MalformedManifest(path.to_path_buf()))?;
        let era_id = era_id
            .and_then(|line| line.strip_prefix("store-era-id:"))
            .ok_or_else(|| StoreFormatError::MalformedManifest(path.to_path_buf()))?;
        let era_id = StoreEraId::parse(era_id)
            .map_err(|_| StoreFormatError::MalformedManifest(path.to_path_buf()))?;
        Self::new(format_id, era_id)
            .map_err(|_| StoreFormatError::MalformedManifest(path.to_path_buf()))
    }
}

fn validate_format_id(format_id: &str) -> StoreFormatResult<()> {
    let valid = !format_id.is_empty()
        && format_id.len() <= 128
        && format_id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'));
    if !valid {
        return Err(StoreFormatError::InvalidFormatId(format_id.to_owned()));
    }
    Ok(())
}

/// The recognized state of an existing store root.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum StoreRootState {
    /// The directory has no entries and can be initialized explicitly.
    Empty,
    /// The exact native marker was present and valid.
    Managed(StoreManifest),
    /// Entries exist, but the native marker does not. Contents are never read.
    UnrecognizedNonEmpty,
}

/// A classified store root and its resolved canonical location.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InspectedStoreRoot {
    canonical_root: PathBuf,
    state: StoreRootState,
}

/// An exact-format managed root that is safe to pass to native opening code.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ManagedStoreRoot {
    canonical_root: PathBuf,
    manifest: StoreManifest,
}

/// An exclusive operating-system-backed lease for one canonical store path.
///
/// The lock file is a stable sibling of the store root, so the lease remains
/// effective while a reset renames the root itself. The lease is released when
/// this value is dropped.
#[derive(Debug)]
pub struct StoreLease {
    canonical_root: PathBuf,
    file: fs::File,
}

impl StoreLease {
    /// Acquire exclusive ownership and finish any already-authorized reset
    /// journal before returning the handle to normal store-opening code.
    pub fn acquire(root: impl AsRef<Path>) -> StoreFormatResult<Self> {
        let lease = Self::acquire_without_recovery(root.as_ref())?;
        recover_active_reset(lease.canonical_root())?;
        Ok(lease)
    }

    fn acquire_without_recovery(root: &Path) -> StoreFormatResult<Self> {
        let canonical_root = canonicalize_lease_target(root)?;
        let parent = canonical_root
            .parent()
            .ok_or_else(|| StoreFormatError::BroadResetTarget(canonical_root.clone()))?;
        let mut hasher = blake3::Hasher::new();
        hasher.update(b"mmdb-store-lease-v1");
        update_path(&mut hasher, &canonical_root);
        let lock_path = parent.join(format!(
            ".mmdb-store-lease-{}.lock",
            hasher.finalize().to_hex()
        ));

        if let Ok(metadata) = fs::symlink_metadata(&lock_path) {
            if !metadata.is_file() || metadata.file_type().is_symlink() {
                return Err(StoreFormatError::InvalidStoreLeaseFile(lock_path));
            }
        }
        let file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(&lock_path)
            .map_err(|source| StoreFormatError::Io {
                operation: "open store lease",
                path: lock_path.clone(),
                source,
            })?;
        let metadata = fs::symlink_metadata(&lock_path).map_err(|source| StoreFormatError::Io {
            operation: "inspect store lease",
            path: lock_path.clone(),
            source,
        })?;
        if !metadata.is_file() || metadata.file_type().is_symlink() {
            return Err(StoreFormatError::InvalidStoreLeaseFile(lock_path));
        }
        match file.try_lock() {
            Ok(()) => Ok(Self {
                canonical_root,
                file,
            }),
            Err(fs::TryLockError::WouldBlock) => Err(StoreFormatError::StoreBusy(canonical_root)),
            Err(fs::TryLockError::Error(source)) => Err(StoreFormatError::Io {
                operation: "acquire store lease",
                path: lock_path,
                source,
            }),
        }
    }

    pub fn canonical_root(&self) -> &Path {
        &self.canonical_root
    }
}

impl Drop for StoreLease {
    fn drop(&mut self) {
        let _ = self.file.unlock();
    }
}

/// Explicit broad paths a reset target must not equal or contain.
///
/// Callers should pass the current home and every workspace root they know.
/// A target inside one of these roots remains valid; a target that is the root
/// itself, or an ancestor containing it, is rejected.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ResetSafety {
    forbidden_broad_roots: Vec<PathBuf>,
}

impl ResetSafety {
    pub fn new(
        home_dir: Option<PathBuf>,
        workspace_roots: Vec<PathBuf>,
    ) -> StoreFormatResult<Self> {
        let mut forbidden_broad_roots = Vec::new();
        if let Some(home_dir) = home_dir {
            forbidden_broad_roots.push(canonicalize_policy_root(&home_dir)?);
        }
        for workspace_root in workspace_roots {
            forbidden_broad_roots.push(canonicalize_policy_root(&workspace_root)?);
        }
        forbidden_broad_roots.sort();
        forbidden_broad_roots.dedup();
        Ok(Self {
            forbidden_broad_roots,
        })
    }

    pub fn for_current_process(workspace_roots: Vec<PathBuf>) -> StoreFormatResult<Self> {
        let home_dir = std::env::var_os("HOME")
            .map(PathBuf::from)
            .ok_or(StoreFormatError::HomeDirectoryUnavailable)?;
        Self::new(Some(home_dir), workspace_roots)
    }

    fn validate_target(&self, target: &Path) -> StoreFormatResult<()> {
        if target.parent().is_none() {
            return Err(StoreFormatError::BroadResetTarget(target.to_path_buf()));
        }
        for forbidden in &self.forbidden_broad_roots {
            if target == forbidden || forbidden.starts_with(target) {
                return Err(StoreFormatError::BroadResetTarget(target.to_path_buf()));
            }
        }
        for marker in WORKSPACE_MARKERS {
            let marker_path = target.join(marker);
            match fs::symlink_metadata(&marker_path) {
                Ok(_) => {
                    return Err(StoreFormatError::WorkspaceLikeResetTarget(
                        target.to_path_buf(),
                    ));
                }
                Err(source) if source.kind() == io::ErrorKind::NotFound => {}
                Err(source) => {
                    return Err(StoreFormatError::Io {
                        operation: "inspect workspace marker",
                        path: marker_path,
                        source,
                    });
                }
            }
        }
        Ok(())
    }
}

/// Digest shown to a user and required by a later reset commit operation.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct ResetPlanDigest(String);

impl ResetPlanDigest {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for ResetPlanDigest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct MetadataSnapshot {
    digest: String,
    entry_count: u64,
    total_file_bytes: u64,
}

/// One exact managed root captured by a reset plan.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ResetTarget {
    canonical_path: PathBuf,
    store_era_id: StoreEraId,
    metadata: MetadataSnapshot,
}

impl ResetTarget {
    pub fn canonical_path(&self) -> &Path {
        &self.canonical_path
    }

    pub fn store_era_id(&self) -> &StoreEraId {
        &self.store_era_id
    }

    pub fn entry_count(&self) -> u64 {
        self.metadata.entry_count
    }

    pub fn total_file_bytes(&self) -> u64 {
        self.metadata.total_file_bytes
    }
}

/// Read-only evidence for a future destructive reset commit.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ResetPlan {
    expected_format_id: String,
    issued_at_unix_ms: u64,
    expires_at_unix_ms: u64,
    targets: Vec<ResetTarget>,
    digest: ResetPlanDigest,
}

impl ResetPlan {
    pub fn build(
        targets: &[PathBuf],
        expected_format_id: &str,
        issued_at: SystemTime,
        ttl: Duration,
        safety: &ResetSafety,
    ) -> StoreFormatResult<Self> {
        Self::build_with_snapshot_limits(
            targets,
            expected_format_id,
            issued_at,
            ttl,
            safety,
            RESET_SNAPSHOT_LIMITS,
        )
    }

    fn build_with_snapshot_limits(
        targets: &[PathBuf],
        expected_format_id: &str,
        issued_at: SystemTime,
        ttl: Duration,
        safety: &ResetSafety,
        snapshot_limits: SnapshotLimits,
    ) -> StoreFormatResult<Self> {
        validate_format_id(expected_format_id)?;
        if targets.is_empty() {
            return Err(StoreFormatError::EmptyResetTargetSet);
        }
        let ttl_ms = u64::try_from(ttl.as_millis())
            .map_err(|_| StoreFormatError::InvalidResetPlanTtl(ttl))?;
        if ttl_ms == 0 || ttl > MAX_RESET_PLAN_TTL {
            return Err(StoreFormatError::InvalidResetPlanTtl(ttl));
        }
        let issued_at_unix_ms = unix_time_ms(issued_at)?;
        let expires_at_unix_ms = issued_at_unix_ms
            .checked_add(ttl_ms)
            .ok_or(StoreFormatError::InvalidResetPlanTtl(ttl))?;

        let mut resolved = Vec::with_capacity(targets.len());
        for target in targets {
            ensure_explicit_non_symlink_path(target)?;
            let canonical_target =
                fs::canonicalize(target).map_err(|source| StoreFormatError::Io {
                    operation: "canonicalize reset target",
                    path: target.clone(),
                    source,
                })?;
            safety.validate_target(&canonical_target)?;
            let managed = require_managed_store(target, expected_format_id)?;
            let metadata = snapshot_tree_with_limits(managed.canonical_root(), snapshot_limits)?;
            resolved.push(ResetTarget {
                canonical_path: managed.canonical_root,
                store_era_id: managed.manifest.store_era_id,
                metadata,
            });
        }
        resolved.sort_by(|left, right| left.canonical_path.cmp(&right.canonical_path));
        if resolved
            .windows(2)
            .any(|pair| pair[0].canonical_path == pair[1].canonical_path)
        {
            return Err(StoreFormatError::DuplicateResetTarget);
        }
        if let Some(pair) = resolved
            .windows(2)
            .find(|pair| pair[1].canonical_path.starts_with(&pair[0].canonical_path))
        {
            return Err(StoreFormatError::OverlappingResetTargets {
                ancestor: pair[0].canonical_path.clone(),
                descendant: pair[1].canonical_path.clone(),
            });
        }

        let digest = digest_reset_plan(
            expected_format_id,
            issued_at_unix_ms,
            expires_at_unix_ms,
            &resolved,
        );
        Ok(Self {
            expected_format_id: expected_format_id.to_owned(),
            issued_at_unix_ms,
            expires_at_unix_ms,
            targets: resolved,
            digest,
        })
    }

    pub fn expected_format_id(&self) -> &str {
        &self.expected_format_id
    }

    pub fn issued_at_unix_ms(&self) -> u64 {
        self.issued_at_unix_ms
    }

    pub fn expires_at_unix_ms(&self) -> u64 {
        self.expires_at_unix_ms
    }

    pub fn targets(&self) -> &[ResetTarget] {
        &self.targets
    }

    pub fn digest(&self) -> &ResetPlanDigest {
        &self.digest
    }

    /// Revalidate every safety fact. This returns authorization evidence only;
    /// it performs no rename, deletion, or store initialization.
    pub fn validate(
        &self,
        presented_digest: &str,
        now: SystemTime,
        safety: &ResetSafety,
    ) -> StoreFormatResult<ValidatedResetPlan> {
        let now_ms = unix_time_ms(now)?;
        if now_ms < self.issued_at_unix_ms {
            return Err(StoreFormatError::ResetPlanNotYetValid {
                issued_at_unix_ms: self.issued_at_unix_ms,
                now_unix_ms: now_ms,
            });
        }
        if now_ms >= self.expires_at_unix_ms {
            return Err(StoreFormatError::ResetPlanExpired {
                expires_at_unix_ms: self.expires_at_unix_ms,
                now_unix_ms: now_ms,
            });
        }
        let expected_digest = digest_reset_plan(
            &self.expected_format_id,
            self.issued_at_unix_ms,
            self.expires_at_unix_ms,
            &self.targets,
        );
        if expected_digest != self.digest || presented_digest != self.digest.as_str() {
            return Err(StoreFormatError::ResetPlanDigestMismatch);
        }

        let mut canonical_targets = Vec::with_capacity(self.targets.len());
        for target in &self.targets {
            ensure_explicit_non_symlink_path(&target.canonical_path)?;
            safety.validate_target(&target.canonical_path)?;
            let managed = require_managed_store(&target.canonical_path, &self.expected_format_id)?;
            if managed.manifest.store_era_id != target.store_era_id {
                return Err(StoreFormatError::ResetTargetChanged(
                    target.canonical_path.clone(),
                ));
            }
            let current_metadata = snapshot_tree(&target.canonical_path)?;
            if current_metadata != target.metadata {
                return Err(StoreFormatError::ResetTargetChanged(
                    target.canonical_path.clone(),
                ));
            }
            canonical_targets.push(target.canonical_path.clone());
        }
        Ok(ValidatedResetPlan {
            digest: self.digest.clone(),
            canonical_targets,
        })
    }
}

/// Successfully revalidated reset evidence. It intentionally has no commit API.
#[derive(Clone, Debug, Eq, PartialEq)]
#[must_use = "validation evidence must be handed directly to a future reset commit"]
pub struct ValidatedResetPlan {
    digest: ResetPlanDigest,
    canonical_targets: Vec<PathBuf>,
}

impl ValidatedResetPlan {
    pub fn digest(&self) -> &ResetPlanDigest {
        &self.digest
    }

    pub fn canonical_targets(&self) -> &[PathBuf] {
        &self.canonical_targets
    }
}

/// Result of replacing one managed store with a fresh era.
///
/// The previous store is renamed beside the replacement instead of being
/// deleted, so an operator can recover it until they deliberately remove the
/// quarantine after verification.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ResetCommitReceipt {
    canonical_root: PathBuf,
    quarantine_path: PathBuf,
    old_store_era_id: StoreEraId,
    new_store_era_id: StoreEraId,
}

impl ResetCommitReceipt {
    pub fn canonical_root(&self) -> &Path {
        &self.canonical_root
    }

    pub fn quarantine_path(&self) -> &Path {
        &self.quarantine_path
    }

    pub fn old_store_era_id(&self) -> &StoreEraId {
        &self.old_store_era_id
    }

    pub fn new_store_era_id(&self) -> &StoreEraId {
        &self.new_store_era_id
    }
}

/// Replace one closed, unchanged managed store with a fresh empty era.
///
/// The destructive sink owns all volatile safety inputs: it acquires the
/// store lease before validation, reads the current system clock, and builds
/// the broad-path policy for the current process. An immutable, fsynced intent
/// journal makes every subsequent rename recoverable by either a retry or the
/// next ordinary [`StoreLease::acquire`]. The old quarantine is never deleted.
pub fn commit_reset(
    plan: &ResetPlan,
    presented_digest: &str,
    new_store_era_id: StoreEraId,
    workspace_roots: &[PathBuf],
) -> StoreFormatResult<ResetCommitReceipt> {
    let workspace_roots = workspace_roots.to_vec();
    commit_reset_with_inputs(
        plan,
        presented_digest,
        new_store_era_id,
        move || {
            let now = SystemTime::now();
            let safety = ResetSafety::for_current_process(workspace_roots)?;
            Ok((now, safety))
        },
        None,
    )
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ResetTransition {
    JournalPublished,
    StagingDirectoryCreated,
    StagingInitialized,
    OldStoreRenamed,
    ReplacementInstalled,
    ReceiptPublished,
    FinalParentSynced,
}

impl ResetTransition {
    #[cfg(test)]
    const ALL: &'static [Self] = &[
        Self::JournalPublished,
        Self::StagingDirectoryCreated,
        Self::StagingInitialized,
        Self::OldStoreRenamed,
        Self::ReplacementInstalled,
        Self::ReceiptPublished,
        Self::FinalParentSynced,
    ];

    fn name(self) -> &'static str {
        match self {
            Self::JournalPublished => "journal-published",
            Self::StagingDirectoryCreated => "staging-directory-created",
            Self::StagingInitialized => "staging-initialized",
            Self::OldStoreRenamed => "old-store-renamed",
            Self::ReplacementInstalled => "replacement-installed",
            Self::ReceiptPublished => "receipt-published",
            Self::FinalParentSynced => "final-parent-synced",
        }
    }

    fn outcome_is_unknown(self) -> bool {
        matches!(
            self,
            Self::OldStoreRenamed
                | Self::ReplacementInstalled
                | Self::ReceiptPublished
                | Self::FinalParentSynced
        )
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ResetJournal {
    root_digest: String,
    plan_digest: ResetPlanDigest,
    issued_at_unix_ms: u64,
    expires_at_unix_ms: u64,
    expected_format_id: String,
    old_store_era_id: StoreEraId,
    new_store_era_id: StoreEraId,
    old_metadata: MetadataSnapshot,
}

impl ResetJournal {
    fn from_plan(plan: &ResetPlan, new_store_era_id: StoreEraId) -> StoreFormatResult<Self> {
        if plan.targets.len() != 1 {
            return Err(StoreFormatError::ResetCommitRequiresOneTarget(
                plan.targets.len(),
            ));
        }
        let target = &plan.targets[0];
        if target.store_era_id == new_store_era_id {
            return Err(StoreFormatError::ResetMustAdvanceStoreEra(new_store_era_id));
        }
        Ok(Self {
            root_digest: reset_root_digest(&target.canonical_path),
            plan_digest: plan.digest.clone(),
            issued_at_unix_ms: plan.issued_at_unix_ms,
            expires_at_unix_ms: plan.expires_at_unix_ms,
            expected_format_id: plan.expected_format_id.clone(),
            old_store_era_id: target.store_era_id.clone(),
            new_store_era_id,
            old_metadata: target.metadata.clone(),
        })
    }

    fn encode(&self) -> String {
        let body = format!(
            "{RESET_JOURNAL_MAGIC}\njournal-version:{RESET_JOURNAL_VERSION}\nroot-digest:{}\nplan-digest:{}\nissued-at-unix-ms:{}\nexpires-at-unix-ms:{}\nformat-id:{}\nold-store-era-id:{}\nnew-store-era-id:{}\nmetadata-digest:{}\nentry-count:{}\ntotal-file-bytes:{}\n",
            self.root_digest,
            self.plan_digest,
            self.issued_at_unix_ms,
            self.expires_at_unix_ms,
            self.expected_format_id,
            self.old_store_era_id,
            self.new_store_era_id,
            self.old_metadata.digest,
            self.old_metadata.entry_count,
            self.old_metadata.total_file_bytes,
        );
        let checksum = blake3::hash(body.as_bytes()).to_hex();
        format!("{body}checksum:{checksum}\n")
    }

    fn decode(path: &Path, encoded: &str) -> StoreFormatResult<Self> {
        let mut lines = encoded.lines();
        let magic = lines.next();
        let version = parse_prefixed_u32(lines.next(), "journal-version:");
        let root_digest = parse_prefixed(lines.next(), "root-digest:");
        let plan_digest = parse_prefixed(lines.next(), "plan-digest:");
        let issued_at_unix_ms = parse_prefixed_u64(lines.next(), "issued-at-unix-ms:");
        let expires_at_unix_ms = parse_prefixed_u64(lines.next(), "expires-at-unix-ms:");
        let expected_format_id = parse_prefixed(lines.next(), "format-id:");
        let old_store_era_id = parse_prefixed(lines.next(), "old-store-era-id:");
        let new_store_era_id = parse_prefixed(lines.next(), "new-store-era-id:");
        let metadata_digest = parse_prefixed(lines.next(), "metadata-digest:");
        let entry_count = parse_prefixed_u64(lines.next(), "entry-count:");
        let total_file_bytes = parse_prefixed_u64(lines.next(), "total-file-bytes:");
        let checksum = parse_prefixed(lines.next(), "checksum:");
        if magic != Some(RESET_JOURNAL_MAGIC)
            || version != Some(RESET_JOURNAL_VERSION)
            || lines.next().is_some()
        {
            return Err(StoreFormatError::InvalidResetJournal(path.to_path_buf()));
        }
        let (
            Some(root_digest),
            Some(plan_digest),
            Some(issued_at_unix_ms),
            Some(expires_at_unix_ms),
            Some(expected_format_id),
            Some(old_store_era_id),
            Some(new_store_era_id),
            Some(metadata_digest),
            Some(entry_count),
            Some(total_file_bytes),
            Some(checksum),
        ) = (
            root_digest,
            plan_digest,
            issued_at_unix_ms,
            expires_at_unix_ms,
            expected_format_id,
            old_store_era_id,
            new_store_era_id,
            metadata_digest,
            entry_count,
            total_file_bytes,
            checksum,
        )
        else {
            return Err(StoreFormatError::InvalidResetJournal(path.to_path_buf()));
        };
        if !is_lower_hex(root_digest, 64)
            || !is_lower_hex(plan_digest, 64)
            || !is_lower_hex(metadata_digest, 64)
            || !is_lower_hex(checksum, 64)
        {
            return Err(StoreFormatError::InvalidResetJournal(path.to_path_buf()));
        }
        if expires_at_unix_ms <= issued_at_unix_ms
            || expires_at_unix_ms - issued_at_unix_ms
                > u64::try_from(MAX_RESET_PLAN_TTL.as_millis()).unwrap_or(u64::MAX)
        {
            return Err(StoreFormatError::InvalidResetJournal(path.to_path_buf()));
        }
        validate_format_id(expected_format_id)
            .map_err(|_| StoreFormatError::InvalidResetJournal(path.to_path_buf()))?;
        let old_store_era_id = StoreEraId::parse(old_store_era_id)
            .map_err(|_| StoreFormatError::InvalidResetJournal(path.to_path_buf()))?;
        let new_store_era_id = StoreEraId::parse(new_store_era_id)
            .map_err(|_| StoreFormatError::InvalidResetJournal(path.to_path_buf()))?;
        if old_store_era_id == new_store_era_id {
            return Err(StoreFormatError::InvalidResetJournal(path.to_path_buf()));
        }
        let journal = Self {
            root_digest: root_digest.to_owned(),
            plan_digest: ResetPlanDigest(plan_digest.to_owned()),
            issued_at_unix_ms,
            expires_at_unix_ms,
            expected_format_id: expected_format_id.to_owned(),
            old_store_era_id,
            new_store_era_id,
            old_metadata: MetadataSnapshot {
                digest: metadata_digest.to_owned(),
                entry_count,
                total_file_bytes,
            },
        };
        let expected = journal.encode();
        if expected != encoded {
            return Err(StoreFormatError::InvalidResetJournal(path.to_path_buf()));
        }
        Ok(journal)
    }

    fn validate_for_root(
        &self,
        canonical_root: &Path,
        path: &Path,
        receipt: bool,
    ) -> StoreFormatResult<()> {
        if self.root_digest != reset_root_digest(canonical_root) {
            return Err(StoreFormatError::InvalidResetJournal(path.to_path_buf()));
        }
        let target = ResetTarget {
            canonical_path: canonical_root.to_path_buf(),
            store_era_id: self.old_store_era_id.clone(),
            metadata: self.old_metadata.clone(),
        };
        if digest_reset_plan(
            &self.expected_format_id,
            self.issued_at_unix_ms,
            self.expires_at_unix_ms,
            &[target],
        ) != self.plan_digest
        {
            return Err(StoreFormatError::InvalidResetJournal(path.to_path_buf()));
        }
        let expected_path = if receipt {
            reset_receipt_path(canonical_root, &self.plan_digest)
        } else {
            active_journal_path(canonical_root, &self.plan_digest)
        };
        if path != expected_path {
            return Err(StoreFormatError::InvalidResetJournal(path.to_path_buf()));
        }
        validate_reset_sibling(canonical_root, path)?;
        validate_reset_sibling(canonical_root, &self.quarantine_path(canonical_root))?;
        validate_reset_sibling(canonical_root, &self.staging_path(canonical_root))?;
        Ok(())
    }

    fn quarantine_path(&self, canonical_root: &Path) -> PathBuf {
        reset_sibling(
            canonical_root,
            &format!(
                ".mmdb-reset-{}-old-{}",
                self.root_digest, self.old_store_era_id
            ),
        )
    }

    fn staging_path(&self, canonical_root: &Path) -> PathBuf {
        reset_sibling(
            canonical_root,
            &format!(
                ".mmdb-reset-{}-new-{}",
                self.root_digest, self.new_store_era_id
            ),
        )
    }

    fn receipt(&self, canonical_root: &Path) -> ResetCommitReceipt {
        ResetCommitReceipt {
            canonical_root: canonical_root.to_path_buf(),
            quarantine_path: self.quarantine_path(canonical_root),
            old_store_era_id: self.old_store_era_id.clone(),
            new_store_era_id: self.new_store_era_id.clone(),
        }
    }
}

fn commit_reset_with_inputs<F>(
    plan: &ResetPlan,
    presented_digest: &str,
    new_store_era_id: StoreEraId,
    environment: F,
    fail_after: Option<ResetTransition>,
) -> StoreFormatResult<ResetCommitReceipt>
where
    F: FnOnce() -> StoreFormatResult<(SystemTime, ResetSafety)>,
{
    if plan.targets.len() != 1 {
        return Err(StoreFormatError::ResetCommitRequiresOneTarget(
            plan.targets.len(),
        ));
    }
    let canonical_root = plan.targets[0].canonical_path.clone();
    let _lease = StoreLease::acquire_without_recovery(&canonical_root)?;

    validate_plan_digest_envelope(plan, presented_digest)?;
    let expected_journal = ResetJournal::from_plan(plan, new_store_era_id)?;
    let receipt_path = reset_receipt_path(&canonical_root, plan.digest());
    if let Some(receipt_journal) = read_reset_record(&canonical_root, &receipt_path, true)? {
        if receipt_journal != expected_journal {
            return Err(StoreFormatError::ResetTransactionConflict(canonical_root));
        }
        validate_committed_receipt(&canonical_root, &receipt_journal)?;
        sync_final_parent(&canonical_root)?;
        return Ok(receipt_journal.receipt(&canonical_root));
    }

    if let Some((journal_path, active_journal)) = find_active_reset(&canonical_root)? {
        if active_journal != expected_journal {
            return Err(StoreFormatError::ResetTransactionConflict(canonical_root));
        }
        return drive_reset_transaction(
            &canonical_root,
            &journal_path,
            &active_journal,
            fail_after,
        );
    }

    let (now, safety) = environment()?;
    let validated = plan.validate(presented_digest, now, &safety)?;
    if validated.canonical_targets != [canonical_root.clone()] {
        return Err(StoreFormatError::ResetTargetChanged(canonical_root));
    }

    let journal_path = active_journal_path(&canonical_root, plan.digest());
    let quarantine_path = expected_journal.quarantine_path(&canonical_root);
    let staging_path = expected_journal.staging_path(&canonical_root);
    require_absent_reset_destination(&journal_path)?;
    require_absent_reset_destination(&receipt_path)?;
    require_absent_reset_destination(&quarantine_path)?;
    require_absent_reset_destination(&staging_path)?;
    write_reset_journal(&canonical_root, &journal_path, &expected_journal)?;
    inject_reset_fault(
        fail_after,
        ResetTransition::JournalPublished,
        &canonical_root,
    )?;
    drive_reset_transaction(
        &canonical_root,
        &journal_path,
        &expected_journal,
        fail_after,
    )
}

#[cfg(test)]
fn commit_reset_with_environment(
    plan: &ResetPlan,
    presented_digest: &str,
    new_store_era_id: StoreEraId,
    now: SystemTime,
    safety: &ResetSafety,
    fail_after: Option<ResetTransition>,
) -> StoreFormatResult<ResetCommitReceipt> {
    let safety = safety.clone();
    commit_reset_with_inputs(
        plan,
        presented_digest,
        new_store_era_id,
        move || Ok((now, safety)),
        fail_after,
    )
}

fn validate_plan_digest_envelope(
    plan: &ResetPlan,
    presented_digest: &str,
) -> StoreFormatResult<()> {
    let expected_digest = digest_reset_plan(
        &plan.expected_format_id,
        plan.issued_at_unix_ms,
        plan.expires_at_unix_ms,
        &plan.targets,
    );
    if expected_digest != plan.digest || presented_digest != plan.digest.as_str() {
        return Err(StoreFormatError::ResetPlanDigestMismatch);
    }
    Ok(())
}

fn drive_reset_transaction(
    canonical_root: &Path,
    journal_path: &Path,
    journal: &ResetJournal,
    fail_after: Option<ResetTransition>,
) -> StoreFormatResult<ResetCommitReceipt> {
    journal.validate_for_root(canonical_root, journal_path, false)?;
    let parent = reset_parent(canonical_root)?;
    let quarantine_path = journal.quarantine_path(canonical_root);
    let staging_path = journal.staging_path(canonical_root);

    let root_era = managed_era_if_present(canonical_root, &journal.expected_format_id)?;
    let quarantine_era = managed_era_if_present(&quarantine_path, &journal.expected_format_id)?;
    match (root_era.as_ref(), quarantine_era.as_ref()) {
        (Some(root), None) if root == &journal.old_store_era_id => {
            let current_metadata = snapshot_tree(canonical_root)?;
            if current_metadata != journal.old_metadata {
                return Err(StoreFormatError::ResetTargetChanged(
                    canonical_root.to_path_buf(),
                ));
            }
            ensure_fresh_staging(canonical_root, journal, fail_after)?;
            fs::rename(canonical_root, &quarantine_path).map_err(|source| {
                StoreFormatError::Io {
                    operation: "move old store to reset quarantine",
                    path: canonical_root.to_path_buf(),
                    source,
                }
            })?;
            inject_reset_fault(fail_after, ResetTransition::OldStoreRenamed, canonical_root)?;
            sync_directory(parent).map_err(|error| {
                reset_outcome_unknown(ResetTransition::OldStoreRenamed, canonical_root, error)
            })?;
        }
        (None, Some(quarantine)) if quarantine == &journal.old_store_era_id => {}
        (Some(root), Some(quarantine))
            if root == &journal.new_store_era_id && quarantine == &journal.old_store_era_id =>
        {
            return publish_reset_receipt(canonical_root, journal_path, journal, fail_after);
        }
        _ => {
            return Err(StoreFormatError::InvalidResetLayout(
                canonical_root.to_path_buf(),
            ));
        }
    }

    let root_era = managed_era_if_present(canonical_root, &journal.expected_format_id)?;
    let quarantine_era = managed_era_if_present(&quarantine_path, &journal.expected_format_id)?;
    if root_era.is_none() && quarantine_era.as_ref() == Some(&journal.old_store_era_id) {
        ensure_fresh_staging(canonical_root, journal, fail_after)?;
        fs::rename(&staging_path, canonical_root).map_err(|source| {
            StoreFormatError::ResetCommitOutcomeUnknown {
                canonical_root: canonical_root.to_path_buf(),
                transition: ResetTransition::ReplacementInstalled.name(),
                detail: source.to_string(),
            }
        })?;
        inject_reset_fault(
            fail_after,
            ResetTransition::ReplacementInstalled,
            canonical_root,
        )?;
        sync_directory(parent).map_err(|error| {
            reset_outcome_unknown(ResetTransition::ReplacementInstalled, canonical_root, error)
        })?;
    }

    validate_committed_layout(canonical_root, journal)?;
    publish_reset_receipt(canonical_root, journal_path, journal, fail_after)
}

fn ensure_fresh_staging(
    canonical_root: &Path,
    journal: &ResetJournal,
    fail_after: Option<ResetTransition>,
) -> StoreFormatResult<()> {
    let parent = reset_parent(canonical_root)?;
    let staging_path = journal.staging_path(canonical_root);
    match fs::symlink_metadata(&staging_path) {
        Ok(metadata) => {
            if !metadata.is_dir() || metadata.file_type().is_symlink() {
                return Err(StoreFormatError::InvalidResetLayout(staging_path));
            }
        }
        Err(source) if source.kind() == io::ErrorKind::NotFound => {
            fs::create_dir(&staging_path).map_err(|source| StoreFormatError::Io {
                operation: "create reset replacement staging directory",
                path: staging_path.clone(),
                source,
            })?;
            inject_reset_fault(
                fail_after,
                ResetTransition::StagingDirectoryCreated,
                canonical_root,
            )?;
            sync_directory(parent)?;
        }
        Err(source) => {
            return Err(StoreFormatError::Io {
                operation: "inspect reset replacement staging directory",
                path: staging_path,
                source,
            });
        }
    }

    let inspected = inspect_store_root(&staging_path)?;
    match inspected.state() {
        StoreRootState::Empty => {
            StoreManifest::new(
                journal.expected_format_id.clone(),
                journal.new_store_era_id.clone(),
            )?
            .write_new(&staging_path)?;
            inject_reset_fault(
                fail_after,
                ResetTransition::StagingInitialized,
                canonical_root,
            )?;
        }
        StoreRootState::Managed(manifest)
            if manifest.format_id() == journal.expected_format_id
                && manifest.store_era_id() == &journal.new_store_era_id
                && directory_contains_only_manifest(&staging_path)? => {}
        StoreRootState::Managed(_) | StoreRootState::UnrecognizedNonEmpty => {
            return Err(StoreFormatError::InvalidResetLayout(staging_path));
        }
    }
    Ok(())
}

fn publish_reset_receipt(
    canonical_root: &Path,
    journal_path: &Path,
    journal: &ResetJournal,
    fail_after: Option<ResetTransition>,
) -> StoreFormatResult<ResetCommitReceipt> {
    validate_committed_layout(canonical_root, journal)?;
    let receipt_path = reset_receipt_path(canonical_root, &journal.plan_digest);
    require_absent_reset_destination(&receipt_path)?;
    fs::rename(journal_path, &receipt_path).map_err(|source| {
        StoreFormatError::ResetCommitOutcomeUnknown {
            canonical_root: canonical_root.to_path_buf(),
            transition: ResetTransition::ReceiptPublished.name(),
            detail: source.to_string(),
        }
    })?;
    inject_reset_fault(
        fail_after,
        ResetTransition::ReceiptPublished,
        canonical_root,
    )?;
    sync_final_parent(canonical_root)?;
    inject_reset_fault(
        fail_after,
        ResetTransition::FinalParentSynced,
        canonical_root,
    )?;
    Ok(journal.receipt(canonical_root))
}

fn validate_committed_layout(
    canonical_root: &Path,
    journal: &ResetJournal,
) -> StoreFormatResult<()> {
    let root_era = managed_era_if_present(canonical_root, &journal.expected_format_id)?;
    let quarantine_era = managed_era_if_present(
        &journal.quarantine_path(canonical_root),
        &journal.expected_format_id,
    )?;
    if root_era.as_ref() != Some(&journal.new_store_era_id)
        || quarantine_era.as_ref() != Some(&journal.old_store_era_id)
    {
        return Err(StoreFormatError::InvalidResetLayout(
            canonical_root.to_path_buf(),
        ));
    }
    validate_staging_residue(canonical_root, journal)?;
    Ok(())
}

fn validate_staging_residue(
    canonical_root: &Path,
    journal: &ResetJournal,
) -> StoreFormatResult<()> {
    let staging_path = journal.staging_path(canonical_root);
    let metadata = match fs::symlink_metadata(&staging_path) {
        Ok(metadata) => metadata,
        Err(source) if source.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(source) => {
            return Err(StoreFormatError::Io {
                operation: "inspect reset staging residue",
                path: staging_path,
                source,
            });
        }
    };
    if !metadata.is_dir() || metadata.file_type().is_symlink() {
        return Err(StoreFormatError::InvalidResetLayout(staging_path));
    }
    let inspected = inspect_store_root(&staging_path)?;
    match inspected.state() {
        StoreRootState::Empty => Ok(()),
        StoreRootState::Managed(manifest)
            if manifest.format_id() == journal.expected_format_id
                && manifest.store_era_id() == &journal.new_store_era_id
                && directory_contains_only_manifest(&staging_path)? =>
        {
            Ok(())
        }
        StoreRootState::Managed(_) | StoreRootState::UnrecognizedNonEmpty => {
            Err(StoreFormatError::InvalidResetLayout(staging_path))
        }
    }
}

fn validate_committed_receipt(
    canonical_root: &Path,
    journal: &ResetJournal,
) -> StoreFormatResult<()> {
    let quarantine_path = journal.quarantine_path(canonical_root);
    let quarantine_era = managed_era_if_present(&quarantine_path, &journal.expected_format_id)?;
    if quarantine_era.as_ref() != Some(&journal.old_store_era_id) {
        return Err(StoreFormatError::InvalidResetLayout(quarantine_path));
    }
    Ok(())
}

fn sync_final_parent(canonical_root: &Path) -> StoreFormatResult<()> {
    sync_directory(reset_parent(canonical_root)?).map_err(|error| {
        reset_outcome_unknown(ResetTransition::ReceiptPublished, canonical_root, error)
    })
}

fn reset_outcome_unknown(
    transition: ResetTransition,
    canonical_root: &Path,
    error: StoreFormatError,
) -> StoreFormatError {
    StoreFormatError::ResetCommitOutcomeUnknown {
        canonical_root: canonical_root.to_path_buf(),
        transition: transition.name(),
        detail: error.to_string(),
    }
}

fn inject_reset_fault(
    fail_after: Option<ResetTransition>,
    transition: ResetTransition,
    canonical_root: &Path,
) -> StoreFormatResult<()> {
    if fail_after != Some(transition) {
        return Ok(());
    }
    if transition.outcome_is_unknown() {
        Err(StoreFormatError::ResetCommitOutcomeUnknown {
            canonical_root: canonical_root.to_path_buf(),
            transition: transition.name(),
            detail: "deterministic fault injection".to_owned(),
        })
    } else {
        Err(StoreFormatError::ResetInterrupted {
            canonical_root: canonical_root.to_path_buf(),
            transition: transition.name(),
        })
    }
}

fn write_reset_journal(
    canonical_root: &Path,
    journal_path: &Path,
    journal: &ResetJournal,
) -> StoreFormatResult<()> {
    journal.validate_for_root(canonical_root, journal_path, false)?;
    let mut file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(journal_path)
        .map_err(|source| StoreFormatError::Io {
            operation: "create reset journal",
            path: journal_path.to_path_buf(),
            source,
        })?;
    file.write_all(journal.encode().as_bytes())
        .map_err(|source| StoreFormatError::Io {
            operation: "write reset journal",
            path: journal_path.to_path_buf(),
            source,
        })?;
    file.sync_all().map_err(|source| StoreFormatError::Io {
        operation: "sync reset journal",
        path: journal_path.to_path_buf(),
        source,
    })?;
    sync_directory(reset_parent(canonical_root)?)?;
    Ok(())
}

fn recover_active_reset(canonical_root: &Path) -> StoreFormatResult<()> {
    if let Some((journal_path, journal)) = find_active_reset(canonical_root)? {
        drive_reset_transaction(canonical_root, &journal_path, &journal, None)?;
    }
    Ok(())
}

fn find_active_reset(canonical_root: &Path) -> StoreFormatResult<Option<(PathBuf, ResetJournal)>> {
    let parent = reset_parent(canonical_root)?;
    let prefix = format!(".mmdb-reset-journal-{}-", reset_root_digest(canonical_root));
    let mut found = None;
    for entry in fs::read_dir(parent).map_err(|source| StoreFormatError::Io {
        operation: "scan reset journals",
        path: parent.to_path_buf(),
        source,
    })? {
        let entry = entry.map_err(|source| StoreFormatError::Io {
            operation: "read reset journal entry",
            path: parent.to_path_buf(),
            source,
        })?;
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            continue;
        };
        if !name.starts_with(&prefix) || !name.ends_with(".journal") {
            continue;
        }
        let path = entry.path();
        let journal = read_reset_record(canonical_root, &path, false)?
            .ok_or_else(|| StoreFormatError::InvalidResetJournal(path.clone()))?;
        if found.is_some() {
            return Err(StoreFormatError::MultipleActiveResetJournals(
                canonical_root.to_path_buf(),
            ));
        }
        found = Some((path, journal));
    }
    Ok(found)
}

fn read_reset_record(
    canonical_root: &Path,
    path: &Path,
    receipt: bool,
) -> StoreFormatResult<Option<ResetJournal>> {
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(source) if source.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(source) => {
            return Err(StoreFormatError::Io {
                operation: "inspect reset journal",
                path: path.to_path_buf(),
                source,
            });
        }
    };
    if !metadata.is_file()
        || metadata.file_type().is_symlink()
        || metadata.len() > MAX_RESET_JOURNAL_BYTES
    {
        return Err(StoreFormatError::InvalidResetJournal(path.to_path_buf()));
    }
    let encoded = fs::read_to_string(path)
        .map_err(|_| StoreFormatError::InvalidResetJournal(path.to_path_buf()))?;
    let journal = ResetJournal::decode(path, &encoded)?;
    journal.validate_for_root(canonical_root, path, receipt)?;
    Ok(Some(journal))
}

fn managed_era_if_present(
    path: &Path,
    expected_format_id: &str,
) -> StoreFormatResult<Option<StoreEraId>> {
    match fs::symlink_metadata(path) {
        Ok(metadata) => {
            if !metadata.is_dir() || metadata.file_type().is_symlink() {
                return Err(StoreFormatError::InvalidResetLayout(path.to_path_buf()));
            }
        }
        Err(source) if source.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(source) => {
            return Err(StoreFormatError::Io {
                operation: "inspect reset transaction location",
                path: path.to_path_buf(),
                source,
            });
        }
    }
    let managed = require_managed_store(path, expected_format_id)
        .map_err(|_| StoreFormatError::InvalidResetLayout(path.to_path_buf()))?;
    Ok(Some(managed.manifest.store_era_id))
}

fn directory_contains_only_manifest(path: &Path) -> StoreFormatResult<bool> {
    let mut entries = fs::read_dir(path).map_err(|source| StoreFormatError::Io {
        operation: "read reset staging directory",
        path: path.to_path_buf(),
        source,
    })?;
    let Some(entry) = entries.next() else {
        return Ok(false);
    };
    let entry = entry.map_err(|source| StoreFormatError::Io {
        operation: "read reset staging entry",
        path: path.to_path_buf(),
        source,
    })?;
    Ok(entry.file_name() == STORE_MANIFEST_FILE && entries.next().is_none())
}

fn active_journal_path(canonical_root: &Path, plan_digest: &ResetPlanDigest) -> PathBuf {
    reset_sibling(
        canonical_root,
        &format!(
            ".mmdb-reset-journal-{}-{}.journal",
            reset_root_digest(canonical_root),
            plan_digest
        ),
    )
}

fn reset_receipt_path(canonical_root: &Path, plan_digest: &ResetPlanDigest) -> PathBuf {
    reset_sibling(
        canonical_root,
        &format!(
            ".mmdb-reset-journal-{}-{}.receipt",
            reset_root_digest(canonical_root),
            plan_digest
        ),
    )
}

fn reset_root_digest(canonical_root: &Path) -> String {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"mmdb-reset-root-v1");
    update_path(&mut hasher, canonical_root);
    hasher.finalize().to_hex().to_string()
}

fn reset_sibling(canonical_root: &Path, name: &str) -> PathBuf {
    canonical_root
        .parent()
        .expect("validated reset roots always have a parent")
        .join(name)
}

fn reset_parent(canonical_root: &Path) -> StoreFormatResult<&Path> {
    let parent = canonical_root
        .parent()
        .ok_or_else(|| StoreFormatError::BroadResetTarget(canonical_root.to_path_buf()))?;
    let canonical_parent = fs::canonicalize(parent).map_err(|source| StoreFormatError::Io {
        operation: "canonicalize reset transaction parent",
        path: parent.to_path_buf(),
        source,
    })?;
    if canonical_parent != parent {
        return Err(StoreFormatError::InvalidResetSibling(parent.to_path_buf()));
    }
    Ok(parent)
}

fn validate_reset_sibling(canonical_root: &Path, sibling: &Path) -> StoreFormatResult<()> {
    let parent = reset_parent(canonical_root)?;
    if sibling.parent() != Some(parent) || sibling.file_name().is_none() {
        return Err(StoreFormatError::InvalidResetSibling(sibling.to_path_buf()));
    }
    Ok(())
}

fn parse_prefixed<'a>(line: Option<&'a str>, prefix: &str) -> Option<&'a str> {
    line.and_then(|line| line.strip_prefix(prefix))
}

fn parse_prefixed_u32(line: Option<&str>, prefix: &str) -> Option<u32> {
    parse_prefixed(line, prefix)?.parse().ok()
}

fn parse_prefixed_u64(line: Option<&str>, prefix: &str) -> Option<u64> {
    parse_prefixed(line, prefix)?.parse().ok()
}

fn is_lower_hex(value: &str, length: usize) -> bool {
    value.len() == length
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
}

fn require_absent_reset_destination(path: &Path) -> StoreFormatResult<()> {
    match fs::symlink_metadata(path) {
        Ok(_) => Err(StoreFormatError::ResetDestinationExists(path.to_path_buf())),
        Err(source) if source.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(source) => Err(StoreFormatError::Io {
            operation: "inspect reset destination",
            path: path.to_path_buf(),
            source,
        }),
    }
}

impl ManagedStoreRoot {
    pub fn canonical_root(&self) -> &Path {
        &self.canonical_root
    }

    pub fn manifest(&self) -> &StoreManifest {
        &self.manifest
    }
}

impl InspectedStoreRoot {
    pub fn canonical_root(&self) -> &Path {
        &self.canonical_root
    }

    pub fn state(&self) -> &StoreRootState {
        &self.state
    }
}

/// Errors returned before a store can be opened or reset.
#[derive(Debug)]
pub enum StoreFormatError {
    Io {
        operation: &'static str,
        path: PathBuf,
        source: io::Error,
    },
    StoreRootIsNotDirectory(PathBuf),
    StoreRootIsNotEmpty(PathBuf),
    InvalidFormatId(String),
    InvalidStoreEraId(String),
    ManifestIsNotAFile(PathBuf),
    InvalidStoreLeaseFile(PathBuf),
    StoreBusy(PathBuf),
    MalformedManifest(PathBuf),
    MissingManagedMarker(PathBuf),
    FormatMismatch {
        path: PathBuf,
        expected: String,
        found: String,
    },
    EmptyResetTargetSet,
    InvalidResetPlanTtl(Duration),
    ResetTargetMustBeAbsolute(PathBuf),
    ResetTargetHasAmbiguousComponent(PathBuf),
    SymlinkInResetTarget(PathBuf),
    BroadResetTarget(PathBuf),
    WorkspaceLikeResetTarget(PathBuf),
    DuplicateResetTarget,
    OverlappingResetTargets {
        ancestor: PathBuf,
        descendant: PathBuf,
    },
    UnsupportedResetTargetEntry(PathBuf),
    CrossDeviceResetTargetEntry(PathBuf),
    ResetSnapshotEntryLimitExceeded {
        path: PathBuf,
        limit: u64,
    },
    ResetSnapshotByteLimitExceeded {
        path: PathBuf,
        limit: u64,
    },
    ResetSnapshotPathLimitExceeded {
        path: PathBuf,
        limit: u64,
    },
    InvalidSystemTime,
    HomeDirectoryUnavailable,
    ResetPlanNotYetValid {
        issued_at_unix_ms: u64,
        now_unix_ms: u64,
    },
    ResetPlanExpired {
        expires_at_unix_ms: u64,
        now_unix_ms: u64,
    },
    ResetPlanDigestMismatch,
    ResetTargetChanged(PathBuf),
    ResetCommitRequiresOneTarget(usize),
    ResetMustAdvanceStoreEra(StoreEraId),
    ResetDestinationExists(PathBuf),
    InvalidResetJournal(PathBuf),
    InvalidResetSibling(PathBuf),
    InvalidResetLayout(PathBuf),
    MultipleActiveResetJournals(PathBuf),
    ResetTransactionConflict(PathBuf),
    ResetInterrupted {
        canonical_root: PathBuf,
        transition: &'static str,
    },
    ResetCommitOutcomeUnknown {
        canonical_root: PathBuf,
        transition: &'static str,
        detail: String,
    },
}

impl fmt::Display for StoreFormatError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io {
                operation,
                path,
                source,
            } => write!(
                formatter,
                "failed to {operation} `{}`: {source}",
                path.display()
            ),
            Self::StoreRootIsNotDirectory(path) => {
                write!(
                    formatter,
                    "store root `{}` is not a directory",
                    path.display()
                )
            }
            Self::StoreRootIsNotEmpty(path) => {
                write!(formatter, "store root `{}` is not empty", path.display())
            }
            Self::InvalidFormatId(value) => write!(formatter, "invalid store format id `{value}`"),
            Self::InvalidStoreEraId(value) => write!(formatter, "invalid store era id `{value}`"),
            Self::ManifestIsNotAFile(path) => {
                write!(
                    formatter,
                    "store manifest `{}` is not a regular file",
                    path.display()
                )
            }
            Self::InvalidStoreLeaseFile(path) => write!(
                formatter,
                "store lease `{}` is not a regular file",
                path.display()
            ),
            Self::StoreBusy(path) => {
                write!(formatter, "store root `{}` is already open", path.display())
            }
            Self::MalformedManifest(path) => {
                write!(
                    formatter,
                    "store manifest `{}` is malformed",
                    path.display()
                )
            }
            Self::MissingManagedMarker(path) => {
                write!(
                    formatter,
                    "store root `{}` has no managed marker",
                    path.display()
                )
            }
            Self::FormatMismatch {
                path,
                expected,
                found,
            } => write!(
                formatter,
                "store root `{}` has format `{found}`, expected exact format `{expected}`",
                path.display()
            ),
            Self::EmptyResetTargetSet => formatter.write_str("reset plan has no explicit targets"),
            Self::InvalidResetPlanTtl(ttl) => {
                write!(
                    formatter,
                    "reset plan ttl {ttl:?} must be between 1ms and {MAX_RESET_PLAN_TTL:?}"
                )
            }
            Self::ResetTargetMustBeAbsolute(path) => {
                write!(
                    formatter,
                    "reset target `{}` must be absolute",
                    path.display()
                )
            }
            Self::ResetTargetHasAmbiguousComponent(path) => write!(
                formatter,
                "reset target `{}` contains `.` or `..`",
                path.display()
            ),
            Self::SymlinkInResetTarget(path) => {
                write!(
                    formatter,
                    "reset target contains symlink `{}`",
                    path.display()
                )
            }
            Self::BroadResetTarget(path) => {
                write!(
                    formatter,
                    "reset target `{}` is dangerously broad",
                    path.display()
                )
            }
            Self::WorkspaceLikeResetTarget(path) => write!(
                formatter,
                "reset target `{}` looks like a workspace root",
                path.display()
            ),
            Self::DuplicateResetTarget => {
                formatter.write_str("reset plan contains a duplicate target")
            }
            Self::OverlappingResetTargets {
                ancestor,
                descendant,
            } => write!(
                formatter,
                "reset targets overlap: `{}` contains `{}`",
                ancestor.display(),
                descendant.display()
            ),
            Self::UnsupportedResetTargetEntry(path) => write!(
                formatter,
                "reset target contains unsupported entry `{}`",
                path.display()
            ),
            Self::CrossDeviceResetTargetEntry(path) => write!(
                formatter,
                "reset target crosses a filesystem boundary at `{}`",
                path.display()
            ),
            Self::ResetSnapshotEntryLimitExceeded { path, limit } => write!(
                formatter,
                "reset snapshot exceeded its {limit}-entry limit at `{}`",
                path.display()
            ),
            Self::ResetSnapshotByteLimitExceeded { path, limit } => write!(
                formatter,
                "reset snapshot exceeded its {limit}-byte aggregate file limit at `{}`",
                path.display()
            ),
            Self::ResetSnapshotPathLimitExceeded { path, limit } => write!(
                formatter,
                "reset snapshot exceeded its {limit}-byte path-work limit at `{}`",
                path.display()
            ),
            Self::InvalidSystemTime => {
                formatter.write_str("system time is before the Unix epoch or too large")
            }
            Self::HomeDirectoryUnavailable => formatter.write_str(
                "home directory is unavailable; reset safety cannot infer broad targets",
            ),
            Self::ResetPlanNotYetValid {
                issued_at_unix_ms,
                now_unix_ms,
            } => write!(
                formatter,
                "reset plan was issued at {issued_at_unix_ms}ms (now {now_unix_ms}ms)"
            ),
            Self::ResetPlanExpired {
                expires_at_unix_ms,
                now_unix_ms,
            } => write!(
                formatter,
                "reset plan expired at {expires_at_unix_ms}ms (now {now_unix_ms}ms)"
            ),
            Self::ResetPlanDigestMismatch => {
                formatter.write_str("reset plan digest does not match its contents")
            }
            Self::ResetTargetChanged(path) => write!(
                formatter,
                "reset target `{}` changed after planning",
                path.display()
            ),
            Self::ResetCommitRequiresOneTarget(count) => write!(
                formatter,
                "reset commit requires exactly one target, found {count}"
            ),
            Self::ResetMustAdvanceStoreEra(era) => {
                write!(formatter, "reset must advance beyond store era `{era}`")
            }
            Self::ResetDestinationExists(path) => write!(
                formatter,
                "reset destination `{}` already exists",
                path.display()
            ),
            Self::InvalidResetJournal(path) => write!(
                formatter,
                "reset journal `{}` is malformed, tampered, or not a regular file",
                path.display()
            ),
            Self::InvalidResetSibling(path) => write!(
                formatter,
                "reset transaction path `{}` is not an exact sibling of its canonical target",
                path.display()
            ),
            Self::InvalidResetLayout(path) => write!(
                formatter,
                "reset transaction locations around `{}` do not match a recoverable state",
                path.display()
            ),
            Self::MultipleActiveResetJournals(path) => write!(
                formatter,
                "multiple active reset journals exist beside `{}`",
                path.display()
            ),
            Self::ResetTransactionConflict(path) => write!(
                formatter,
                "a different reset transaction is already recorded for `{}`",
                path.display()
            ),
            Self::ResetInterrupted {
                canonical_root,
                transition,
            } => write!(
                formatter,
                "reset of `{}` was interrupted after {transition}; retry is safe",
                canonical_root.display()
            ),
            Self::ResetCommitOutcomeUnknown {
                canonical_root,
                transition,
                detail,
            } => write!(
                formatter,
                "reset of `{}` reached {transition}, but its durable outcome is unknown ({detail}); retry is required",
                canonical_root.display()
            ),
        }
    }
}

impl std::error::Error for StoreFormatError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io { source, .. } => Some(source),
            _ => None,
        }
    }
}

pub type StoreFormatResult<T> = Result<T, StoreFormatError>;

/// Inspect an existing directory without interpreting any legacy contents.
pub fn inspect_store_root(root: impl AsRef<Path>) -> StoreFormatResult<InspectedStoreRoot> {
    let root = root.as_ref();
    let metadata = fs::symlink_metadata(root).map_err(|source| StoreFormatError::Io {
        operation: "inspect store root",
        path: root.to_path_buf(),
        source,
    })?;
    if !metadata.is_dir() {
        return Err(StoreFormatError::StoreRootIsNotDirectory(
            root.to_path_buf(),
        ));
    }

    let canonical_root = fs::canonicalize(root).map_err(|source| StoreFormatError::Io {
        operation: "canonicalize store root",
        path: root.to_path_buf(),
        source,
    })?;
    let entries = fs::read_dir(&canonical_root).map_err(|source| StoreFormatError::Io {
        operation: "read store root",
        path: canonical_root.clone(),
        source,
    })?;
    let mut has_entries = false;
    let mut has_marker = false;
    for entry in entries {
        let entry = entry.map_err(|source| StoreFormatError::Io {
            operation: "read store root entry",
            path: canonical_root.clone(),
            source,
        })?;
        has_entries = true;
        if entry.file_name() == STORE_MANIFEST_FILE {
            has_marker = true;
        }
    }

    let state = if !has_entries {
        StoreRootState::Empty
    } else if !has_marker {
        StoreRootState::UnrecognizedNonEmpty
    } else {
        let marker = canonical_root.join(STORE_MANIFEST_FILE);
        let metadata = fs::symlink_metadata(&marker).map_err(|source| StoreFormatError::Io {
            operation: "inspect store manifest",
            path: marker.clone(),
            source,
        })?;
        if !metadata.is_file() || metadata.file_type().is_symlink() {
            return Err(StoreFormatError::ManifestIsNotAFile(marker));
        }
        if metadata.len() > MAX_STORE_MANIFEST_BYTES {
            return Err(StoreFormatError::MalformedManifest(marker));
        }
        let encoded = fs::read_to_string(&marker).map_err(|source| StoreFormatError::Io {
            operation: "read store manifest",
            path: marker.clone(),
            source,
        })?;
        StoreRootState::Managed(StoreManifest::decode(&marker, &encoded)?)
    };

    Ok(InspectedStoreRoot {
        canonical_root,
        state,
    })
}

/// Require a native marker whose format id exactly matches the caller.
pub fn require_managed_store(
    root: impl AsRef<Path>,
    expected_format_id: &str,
) -> StoreFormatResult<ManagedStoreRoot> {
    validate_format_id(expected_format_id)?;
    let inspected = inspect_store_root(root)?;
    let manifest = match inspected.state {
        StoreRootState::Managed(manifest) => manifest,
        StoreRootState::Empty | StoreRootState::UnrecognizedNonEmpty => {
            return Err(StoreFormatError::MissingManagedMarker(
                inspected.canonical_root,
            ));
        }
    };
    if manifest.format_id != expected_format_id {
        return Err(StoreFormatError::FormatMismatch {
            path: inspected.canonical_root,
            expected: expected_format_id.to_owned(),
            found: manifest.format_id,
        });
    }
    Ok(ManagedStoreRoot {
        canonical_root: inspected.canonical_root,
        manifest,
    })
}

fn canonicalize_policy_root(path: &Path) -> StoreFormatResult<PathBuf> {
    fs::canonicalize(path).map_err(|source| StoreFormatError::Io {
        operation: "canonicalize reset safety root",
        path: path.to_path_buf(),
        source,
    })
}

fn canonicalize_lease_target(path: &Path) -> StoreFormatResult<PathBuf> {
    let absolute = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()
            .map_err(|source| StoreFormatError::Io {
                operation: "resolve current directory for store lease",
                path: path.to_path_buf(),
                source,
            })?
            .join(path)
    };
    match fs::canonicalize(&absolute) {
        Ok(canonical) => Ok(canonical),
        Err(source) if source.kind() == io::ErrorKind::NotFound => {
            let parent = absolute
                .parent()
                .ok_or_else(|| StoreFormatError::BroadResetTarget(absolute.clone()))?;
            let file_name = absolute
                .file_name()
                .ok_or_else(|| StoreFormatError::BroadResetTarget(absolute.clone()))?;
            let canonical_parent =
                fs::canonicalize(parent).map_err(|source| StoreFormatError::Io {
                    operation: "canonicalize store lease parent",
                    path: parent.to_path_buf(),
                    source,
                })?;
            Ok(canonical_parent.join(file_name))
        }
        Err(source) => Err(StoreFormatError::Io {
            operation: "canonicalize store lease target",
            path: absolute,
            source,
        }),
    }
}

fn ensure_explicit_non_symlink_path(path: &Path) -> StoreFormatResult<()> {
    use std::path::Component;

    if !path.is_absolute() {
        return Err(StoreFormatError::ResetTargetMustBeAbsolute(
            path.to_path_buf(),
        ));
    }

    let mut prefix = PathBuf::new();
    for component in path.components() {
        match component {
            Component::CurDir | Component::ParentDir => {
                return Err(StoreFormatError::ResetTargetHasAmbiguousComponent(
                    path.to_path_buf(),
                ));
            }
            _ => prefix.push(component.as_os_str()),
        }
        let metadata = fs::symlink_metadata(&prefix).map_err(|source| StoreFormatError::Io {
            operation: "inspect reset target path component",
            path: prefix.clone(),
            source,
        })?;
        if metadata.file_type().is_symlink() {
            return Err(StoreFormatError::SymlinkInResetTarget(prefix));
        }
    }
    Ok(())
}

fn unix_time_ms(time: SystemTime) -> StoreFormatResult<u64> {
    let elapsed = time
        .duration_since(UNIX_EPOCH)
        .map_err(|_| StoreFormatError::InvalidSystemTime)?;
    u64::try_from(elapsed.as_millis()).map_err(|_| StoreFormatError::InvalidSystemTime)
}

#[derive(Clone, Copy, Debug)]
struct SnapshotLimits {
    max_entries: u64,
    max_file_bytes: u64,
    max_path_bytes: u64,
}

const RESET_SNAPSHOT_LIMITS: SnapshotLimits = SnapshotLimits {
    max_entries: MAX_RESET_SNAPSHOT_ENTRIES,
    max_file_bytes: MAX_RESET_SNAPSHOT_FILE_BYTES,
    max_path_bytes: MAX_RESET_SNAPSHOT_PATH_BYTES,
};

fn snapshot_tree(root: &Path) -> StoreFormatResult<MetadataSnapshot> {
    snapshot_tree_with_limits(root, RESET_SNAPSHOT_LIMITS)
}

fn snapshot_tree_with_limits(
    root: &Path,
    limits: SnapshotLimits,
) -> StoreFormatResult<MetadataSnapshot> {
    let root_metadata = fs::symlink_metadata(root).map_err(|source| StoreFormatError::Io {
        operation: "inspect reset target",
        path: root.to_path_buf(),
        source,
    })?;
    if root_metadata.file_type().is_symlink() {
        return Err(StoreFormatError::SymlinkInResetTarget(root.to_path_buf()));
    }

    #[cfg(unix)]
    let root_device = root_metadata.dev();

    let mut hasher = blake3::Hasher::new();
    let mut entry_count = 0_u64;
    let mut total_file_bytes = 0_u64;
    let mut total_path_bytes = 0_u64;
    let mut pending_path_bytes = 0_u64;
    let mut pending = vec![(root.to_path_buf(), 0_u64)];
    while let Some((path, relative_path_bytes)) = pending.pop() {
        pending_path_bytes = pending_path_bytes
            .checked_sub(relative_path_bytes)
            .ok_or_else(|| StoreFormatError::ResetTargetChanged(root.to_path_buf()))?;
        let metadata = fs::symlink_metadata(&path).map_err(|source| StoreFormatError::Io {
            operation: "inspect reset target entry",
            path: path.clone(),
            source,
        })?;
        if metadata.file_type().is_symlink() {
            return Err(StoreFormatError::SymlinkInResetTarget(path));
        }
        #[cfg(unix)]
        if metadata.dev() != root_device {
            return Err(StoreFormatError::CrossDeviceResetTargetEntry(path));
        }

        let relative = path
            .strip_prefix(root)
            .map_err(|_| StoreFormatError::ResetTargetChanged(root.to_path_buf()))?;
        total_path_bytes = total_path_bytes
            .checked_add(relative_path_bytes)
            .ok_or_else(|| StoreFormatError::ResetSnapshotPathLimitExceeded {
                path: path.clone(),
                limit: limits.max_path_bytes,
            })?;
        if total_path_bytes > limits.max_path_bytes {
            return Err(StoreFormatError::ResetSnapshotPathLimitExceeded {
                path,
                limit: limits.max_path_bytes,
            });
        }
        update_path(&mut hasher, relative);
        let entry_type = if metadata.is_dir() {
            b'd'
        } else if metadata.is_file() {
            total_file_bytes = total_file_bytes
                .checked_add(metadata.len())
                .ok_or_else(|| StoreFormatError::ResetSnapshotByteLimitExceeded {
                    path: path.clone(),
                    limit: limits.max_file_bytes,
                })?;
            if total_file_bytes > limits.max_file_bytes {
                return Err(StoreFormatError::ResetSnapshotByteLimitExceeded {
                    path,
                    limit: limits.max_file_bytes,
                });
            }
            b'f'
        } else {
            return Err(StoreFormatError::UnsupportedResetTargetEntry(path));
        };
        hasher.update(&[entry_type]);
        update_metadata(&mut hasher, &metadata);
        entry_count = entry_count.checked_add(1).ok_or_else(|| {
            StoreFormatError::ResetSnapshotEntryLimitExceeded {
                path: path.clone(),
                limit: limits.max_entries,
            }
        })?;
        if entry_count > limits.max_entries {
            return Err(StoreFormatError::ResetSnapshotEntryLimitExceeded {
                path,
                limit: limits.max_entries,
            });
        }

        if metadata.is_dir() {
            let entries = fs::read_dir(&path).map_err(|source| StoreFormatError::Io {
                operation: "read reset target directory",
                path: path.clone(),
                source,
            })?;
            let mut children = Vec::new();
            let mut children_path_bytes = 0_u64;
            for entry in entries {
                let entry = entry.map_err(|source| StoreFormatError::Io {
                    operation: "read reset target entry",
                    path: path.clone(),
                    source,
                })?;
                let child = entry.path();
                let queued_entries = entry_count
                    .checked_add(u64::try_from(pending.len()).unwrap_or(u64::MAX))
                    .and_then(|count| {
                        count.checked_add(u64::try_from(children.len()).unwrap_or(u64::MAX))
                    })
                    .and_then(|count| count.checked_add(1))
                    .unwrap_or(u64::MAX);
                if queued_entries > limits.max_entries {
                    return Err(StoreFormatError::ResetSnapshotEntryLimitExceeded {
                        path: child,
                        limit: limits.max_entries,
                    });
                }
                let relative = child
                    .strip_prefix(root)
                    .map_err(|_| StoreFormatError::ResetTargetChanged(root.to_path_buf()))?;
                let child_path_bytes = u64::try_from(path_byte_len(relative)).map_err(|_| {
                    StoreFormatError::ResetSnapshotPathLimitExceeded {
                        path: child.clone(),
                        limit: limits.max_path_bytes,
                    }
                })?;
                let queued_path_bytes = total_path_bytes
                    .checked_add(pending_path_bytes)
                    .and_then(|bytes| bytes.checked_add(children_path_bytes))
                    .and_then(|bytes| bytes.checked_add(child_path_bytes))
                    .unwrap_or(u64::MAX);
                if queued_path_bytes > limits.max_path_bytes {
                    return Err(StoreFormatError::ResetSnapshotPathLimitExceeded {
                        path: child,
                        limit: limits.max_path_bytes,
                    });
                }
                children_path_bytes = children_path_bytes
                    .checked_add(child_path_bytes)
                    .ok_or_else(|| StoreFormatError::ResetSnapshotPathLimitExceeded {
                        path: child.clone(),
                        limit: limits.max_path_bytes,
                    })?;
                children.push((child, child_path_bytes));
            }
            pending_path_bytes = pending_path_bytes
                .checked_add(children_path_bytes)
                .ok_or_else(|| StoreFormatError::ResetSnapshotPathLimitExceeded {
                    path: path.clone(),
                    limit: limits.max_path_bytes,
                })?;
            children.sort_by(|left, right| left.0.cmp(&right.0));
            pending.extend(children.into_iter().rev());
        }
    }

    Ok(MetadataSnapshot {
        digest: hasher.finalize().to_hex().to_string(),
        entry_count,
        total_file_bytes,
    })
}

fn path_byte_len(path: &Path) -> usize {
    #[cfg(unix)]
    {
        path.as_os_str().as_bytes().len()
    }
    #[cfg(not(unix))]
    {
        path.to_string_lossy().len()
    }
}

fn update_path(hasher: &mut blake3::Hasher, path: &Path) {
    #[cfg(unix)]
    let bytes = path.as_os_str().as_bytes();
    #[cfg(not(unix))]
    let bytes = path.to_string_lossy().as_bytes();
    hasher.update(&(bytes.len() as u64).to_le_bytes());
    hasher.update(bytes);
}

fn update_metadata(hasher: &mut blake3::Hasher, metadata: &fs::Metadata) {
    hasher.update(&metadata.len().to_le_bytes());
    #[cfg(unix)]
    {
        for value in [
            metadata.dev(),
            metadata.ino(),
            u64::from(metadata.mode()),
            metadata.mtime() as u64,
            metadata.mtime_nsec() as u64,
            metadata.ctime() as u64,
            metadata.ctime_nsec() as u64,
        ] {
            hasher.update(&value.to_le_bytes());
        }
    }
    #[cfg(not(unix))]
    {
        let modified_ms = metadata
            .modified()
            .ok()
            .and_then(|time| time.duration_since(UNIX_EPOCH).ok())
            .and_then(|duration| u64::try_from(duration.as_millis()).ok())
            .unwrap_or_default();
        hasher.update(&modified_ms.to_le_bytes());
        hasher.update(&[u8::from(metadata.permissions().readonly())]);
    }
}

fn digest_reset_plan(
    expected_format_id: &str,
    issued_at_unix_ms: u64,
    expires_at_unix_ms: u64,
    targets: &[ResetTarget],
) -> ResetPlanDigest {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"mmdb-reset-plan");
    hasher.update(&RESET_PLAN_VERSION.to_le_bytes());
    update_bytes(&mut hasher, expected_format_id.as_bytes());
    hasher.update(&issued_at_unix_ms.to_le_bytes());
    hasher.update(&expires_at_unix_ms.to_le_bytes());
    hasher.update(&(targets.len() as u64).to_le_bytes());
    for target in targets {
        update_path(&mut hasher, &target.canonical_path);
        update_bytes(&mut hasher, target.store_era_id.as_str().as_bytes());
        update_bytes(&mut hasher, target.metadata.digest.as_bytes());
        hasher.update(&target.metadata.entry_count.to_le_bytes());
        hasher.update(&target.metadata.total_file_bytes.to_le_bytes());
    }
    ResetPlanDigest(hasher.finalize().to_hex().to_string())
}

fn update_bytes(hasher: &mut blake3::Hasher, bytes: &[u8]) {
    hasher.update(&(bytes.len() as u64).to_le_bytes());
    hasher.update(bytes);
}

fn sync_directory(path: &Path) -> StoreFormatResult<()> {
    let directory = fs::File::open(path).map_err(|source| StoreFormatError::Io {
        operation: "open directory for sync",
        path: path.to_path_buf(),
        source,
    })?;
    directory.sync_all().map_err(|source| StoreFormatError::Io {
        operation: "sync directory",
        path: path.to_path_buf(),
        source,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::{Path, PathBuf};
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{Duration, UNIX_EPOCH};

    static NEXT_TEST_DIR: AtomicU64 = AtomicU64::new(0);

    struct TestDir(PathBuf);

    impl TestDir {
        fn new(label: &str) -> Self {
            let nonce = NEXT_TEST_DIR.fetch_add(1, Ordering::Relaxed);
            let path = std::env::temp_dir().join(format!(
                "mmdb-store-format-{label}-{}-{nonce}",
                std::process::id()
            ));
            std::fs::create_dir(&path).expect("create isolated test directory");
            Self(path)
        }

        fn path(&self) -> &Path {
            &self.0
        }
    }

    impl Drop for TestDir {
        fn drop(&mut self) {
            std::fs::remove_dir_all(&self.0).expect("remove isolated test directory");
        }
    }

    #[test]
    fn inspection_recognizes_an_empty_root() {
        let root = TestDir::new("empty");

        let inspected = inspect_store_root(root.path()).expect("inspect empty root");

        assert_eq!(
            inspected.canonical_root(),
            std::fs::canonicalize(root.path())
                .expect("canonicalize independently for the expected path")
        );
        assert!(matches!(inspected.state(), StoreRootState::Empty));
    }

    #[test]
    fn manifest_turns_an_empty_root_into_an_exact_managed_store() {
        let root = TestDir::new("managed");
        let era = StoreEraId::parse("01ARZ3NDEKTSV4RRFFQ69G5FAV").expect("valid era id");
        let manifest = StoreManifest::new("memory-v1", era).expect("valid manifest");

        manifest
            .write_new(root.path())
            .expect("write manifest once");
        let inspected = inspect_store_root(root.path()).expect("inspect managed root");

        assert_eq!(
            inspected.state(),
            &StoreRootState::Managed(manifest.clone())
        );
        assert_eq!(
            require_managed_store(root.path(), "memory-v1")
                .expect("accept exact format")
                .manifest(),
            &manifest
        );
    }

    #[test]
    fn format_descriptor_is_exact_bounded_and_never_supplies_the_store_epoch() {
        assert!(matches!(
            StoreFormatDescriptor::new(""),
            Err(StoreFormatError::InvalidFormatId(_))
        ));
        assert!(matches!(
            StoreFormatDescriptor::new("x".repeat(129)),
            Err(StoreFormatError::InvalidFormatId(_))
        ));
        assert!(matches!(
            StoreFormatDescriptor::new("runtime state"),
            Err(StoreFormatError::InvalidFormatId(_))
        ));

        let descriptor = StoreFormatDescriptor::new("example.runtime-state-v1").unwrap();
        let first = descriptor.new_manifest().unwrap();
        let second = descriptor.new_manifest().unwrap();
        assert_eq!(first.format_id(), descriptor.format_id());
        assert_eq!(second.format_id(), descriptor.format_id());
        assert_ne!(first.store_era_id(), second.store_era_id());
    }

    #[test]
    fn store_lease_excludes_a_second_same_root_handle_until_drop() {
        let container = TestDir::new("lease-contention");
        let root = container.path().join("memory");
        std::fs::create_dir(&root).expect("create lease target");
        let canonical_root = std::fs::canonicalize(&root).expect("canonical lease target");

        let first = StoreLease::acquire(&root).expect("acquire first lifetime lease");
        let error = StoreLease::acquire(&canonical_root)
            .expect_err("a second handle must not acquire the same root");
        assert!(matches!(
            error,
            StoreFormatError::StoreBusy(path) if path == canonical_root
        ));

        drop(first);
        StoreLease::acquire(&root).expect("lease is released with the owning handle");
    }

    #[test]
    fn store_lease_excludes_a_second_process() {
        let container = TestDir::new("lease-process-contention");
        let root = container.path().join("memory");
        std::fs::create_dir(&root).expect("create lease target");
        let _lease = StoreLease::acquire(&root).expect("acquire parent-process lease");

        let status =
            std::process::Command::new(std::env::current_exe().expect("current test binary"))
                .arg("--exact")
                .arg("store_format::tests::store_lease_child_probe")
                .arg("--nocapture")
                .env("MMDB_STORE_LEASE_CHILD_PROBE", &root)
                .status()
                .expect("run child-process lease probe");
        assert!(status.success(), "child process did not observe StoreBusy");
    }

    #[test]
    fn store_lease_child_probe() {
        let Some(root) = std::env::var_os("MMDB_STORE_LEASE_CHILD_PROBE") else {
            return;
        };
        let canonical_root = std::fs::canonicalize(&root).expect("canonical lease probe root");
        let error = StoreLease::acquire(&root)
            .expect_err("the parent process must retain the exclusive lease");
        assert!(matches!(
            error,
            StoreFormatError::StoreBusy(path) if path == canonical_root
        ));
    }

    #[test]
    fn non_empty_root_without_the_marker_is_unrecognized() {
        let root = TestDir::new("unrecognized");
        std::fs::write(root.path().join("legacy.data"), b"legacy")
            .expect("write unrecognized content");

        let inspected = inspect_store_root(root.path()).expect("classify without interpreting");

        assert!(matches!(
            inspected.state(),
            StoreRootState::UnrecognizedNonEmpty
        ));
    }

    #[test]
    fn reset_plan_resolves_and_revalidates_an_exact_managed_target() {
        let root = TestDir::new("reset-plan");
        let era = StoreEraId::parse("01ARZ3NDEKTSV4RRFFQ69G5FAV").expect("valid era id");
        StoreManifest::new("memory-v1", era.clone())
            .expect("valid manifest")
            .write_new(root.path())
            .expect("mark root as managed");
        let survivor = root.path().join("survives-planning");
        std::fs::write(&survivor, b"not deleted by planning").expect("write managed data");
        let canonical_root = std::fs::canonicalize(root.path()).expect("canonical test root");
        let safety = ResetSafety::new(None, Vec::new()).expect("empty explicit safety policy");
        let issued_at = UNIX_EPOCH + Duration::from_secs(1_000);

        let plan = ResetPlan::build(
            std::slice::from_ref(&canonical_root),
            "memory-v1",
            issued_at,
            Duration::from_secs(300),
            &safety,
        )
        .expect("plan exact reset");

        assert_eq!(plan.targets().len(), 1);
        assert_eq!(plan.targets()[0].canonical_path(), canonical_root);
        assert_eq!(plan.targets()[0].store_era_id(), &era);
        assert_eq!(plan.expires_at_unix_ms(), 1_300_000);
        assert_eq!(plan.digest().as_str().len(), 64);

        let validated = plan
            .validate(
                plan.digest().as_str(),
                issued_at + Duration::from_secs(1),
                &safety,
            )
            .expect("unchanged plan remains valid");
        assert_eq!(validated.digest(), plan.digest());
        assert_eq!(validated.canonical_targets(), &[canonical_root]);
        assert_eq!(
            std::fs::read(survivor).expect("planning must leave data untouched"),
            b"not deleted by planning"
        );
    }

    #[test]
    fn reset_snapshot_limits_accept_exact_boundaries_and_reject_one_over() {
        let entries = TestDir::new("snapshot-entry-limit");
        std::fs::write(entries.path().join("a"), b"").expect("write first entry");
        snapshot_tree_with_limits(
            entries.path(),
            SnapshotLimits {
                max_entries: 2,
                max_file_bytes: 10,
                max_path_bytes: 10,
            },
        )
        .expect("root plus one file is the exact entry boundary");
        std::fs::write(entries.path().join("b"), b"").expect("write one extra entry");
        let entry_error = snapshot_tree_with_limits(
            entries.path(),
            SnapshotLimits {
                max_entries: 2,
                max_file_bytes: 10,
                max_path_bytes: 10,
            },
        )
        .expect_err("one entry over must stop traversal");
        assert!(matches!(
            entry_error,
            StoreFormatError::ResetSnapshotEntryLimitExceeded { limit: 2, .. }
        ));

        let bytes = TestDir::new("snapshot-byte-limit");
        std::fs::write(bytes.path().join("a"), b"1234").expect("write exact byte boundary");
        snapshot_tree_with_limits(
            bytes.path(),
            SnapshotLimits {
                max_entries: 10,
                max_file_bytes: 4,
                max_path_bytes: 10,
            },
        )
        .expect("four bytes is the exact aggregate boundary");
        std::fs::write(bytes.path().join("b"), b"5").expect("write one extra byte");
        let byte_error = snapshot_tree_with_limits(
            bytes.path(),
            SnapshotLimits {
                max_entries: 10,
                max_file_bytes: 4,
                max_path_bytes: 10,
            },
        )
        .expect_err("one aggregate byte over must stop traversal");
        assert!(matches!(
            byte_error,
            StoreFormatError::ResetSnapshotByteLimitExceeded { limit: 4, .. }
        ));

        let paths = TestDir::new("snapshot-path-limit");
        std::fs::write(paths.path().join("abcd"), b"").expect("write exact path boundary");
        snapshot_tree_with_limits(
            paths.path(),
            SnapshotLimits {
                max_entries: 10,
                max_file_bytes: 10,
                max_path_bytes: 4,
            },
        )
        .expect("four relative-path bytes is the exact work boundary");
        std::fs::write(paths.path().join("e"), b"").expect("write one extra path byte");
        let path_error = snapshot_tree_with_limits(
            paths.path(),
            SnapshotLimits {
                max_entries: 10,
                max_file_bytes: 10,
                max_path_bytes: 4,
            },
        )
        .expect_err("one path-work byte over must stop traversal");
        assert!(matches!(
            path_error,
            StoreFormatError::ResetSnapshotPathLimitExceeded { limit: 4, .. }
        ));
    }

    #[test]
    fn over_limit_reset_plan_fails_before_any_journal_or_rename() {
        let container = TestDir::new("reset-plan-work-bound");
        let root = container.path().join("memory");
        std::fs::create_dir(&root).expect("create managed root");
        let old_era = StoreEraId::parse("01ARZ3NDEKTSV4RRFFQ69G5FAV").expect("valid old era id");
        StoreManifest::new("memory-v1", old_era.clone())
            .expect("valid manifest")
            .write_new(&root)
            .expect("mark managed root");
        std::fs::write(root.join("evidence.data"), b"too much scoped work")
            .expect("write managed evidence");
        let root = std::fs::canonicalize(root).expect("canonical root");
        let safety = ResetSafety::new(None, Vec::new()).expect("test safety");

        let error = ResetPlan::build_with_snapshot_limits(
            std::slice::from_ref(&root),
            "memory-v1",
            UNIX_EPOCH + Duration::from_secs(1_000),
            Duration::from_secs(300),
            &safety,
            SnapshotLimits {
                max_entries: 1,
                max_file_bytes: u64::MAX,
                max_path_bytes: u64::MAX,
            },
        )
        .expect_err("bounded planning must fail before reset authorization exists");

        assert!(matches!(
            error,
            StoreFormatError::ResetSnapshotEntryLimitExceeded { limit: 1, .. }
        ));
        assert_eq!(
            require_managed_store(&root, "memory-v1")
                .expect("the original root was never renamed")
                .manifest()
                .store_era_id(),
            &old_era
        );
        assert_eq!(reset_transaction_files(&root).len(), 0);
    }

    #[test]
    fn reset_commit_replaces_one_closed_store_and_quarantines_the_old_era() {
        let container = TestDir::new("reset-commit");
        let root = container.path().join("memory");
        std::fs::create_dir(&root).expect("create managed memory root");
        let old_era = StoreEraId::parse("01ARZ3NDEKTSV4RRFFQ69G5FAV").expect("valid old era id");
        StoreManifest::new("memory-v1", old_era.clone())
            .expect("valid old manifest")
            .write_new(&root)
            .expect("mark old store");
        std::fs::write(root.join("evidence.data"), b"recoverable old evidence")
            .expect("write old data");
        let canonical_root = std::fs::canonicalize(&root).expect("canonical memory root");
        let safety = ResetSafety::new(None, Vec::new()).expect("test safety");
        let issued_at = UNIX_EPOCH + Duration::from_secs(1_000);
        let plan = ResetPlan::build(
            std::slice::from_ref(&canonical_root),
            "memory-v1",
            issued_at,
            Duration::from_secs(300),
            &safety,
        )
        .expect("build reset plan");
        let new_era = StoreEraId::parse("01BX5ZZKBKACTAV9WEVGEMMVRZ").expect("valid new era id");

        let receipt = commit_reset_with_environment(
            &plan,
            plan.digest().as_str(),
            new_era.clone(),
            issued_at + Duration::from_secs(1),
            &safety,
            None,
        )
        .expect("commit recoverable reset");

        let replacement = require_managed_store(&canonical_root, "memory-v1")
            .expect("replacement is exact-format managed");
        assert_eq!(replacement.manifest().store_era_id(), &new_era);
        assert!(!canonical_root.join("evidence.data").exists());
        assert_eq!(receipt.old_store_era_id(), &old_era);
        assert_eq!(receipt.new_store_era_id(), &new_era);
        assert_eq!(receipt.canonical_root(), canonical_root);
        assert_eq!(
            std::fs::read(receipt.quarantine_path().join("evidence.data"))
                .expect("old evidence remains recoverable"),
            b"recoverable old evidence"
        );
        assert_eq!(
            require_managed_store(receipt.quarantine_path(), "memory-v1")
                .expect("quarantine preserves old marker")
                .manifest()
                .store_era_id(),
            &old_era
        );
    }

    #[test]
    fn reset_commit_holds_the_store_lease_before_validation() {
        let fixture = ResetFixture::new("reset-live-handle");
        let _live_handle = StoreLease::acquire(&fixture.root).expect("open live store handle");

        let error = commit_reset_with_environment(
            &fixture.plan,
            fixture.plan.digest().as_str(),
            fixture.new_era.clone(),
            fixture.issued_at - Duration::from_secs(1),
            &fixture.safety,
            None,
        )
        .expect_err("a live store wins before even a stale-plan validation");

        assert!(matches!(error, StoreFormatError::StoreBusy(path) if path == fixture.root));
    }

    #[test]
    fn reset_commit_rechecks_current_time_and_policy_before_publishing_intent() {
        let stale = ResetFixture::new("reset-stale-time");
        let expired = commit_reset_with_environment(
            &stale.plan,
            stale.plan.digest().as_str(),
            stale.new_era.clone(),
            stale.issued_at + Duration::from_secs(301),
            &stale.safety,
            None,
        )
        .expect_err("expired authorization must not start a transaction");
        assert!(matches!(expired, StoreFormatError::ResetPlanExpired { .. }));
        assert_eq!(reset_transaction_files(&stale.root).len(), 0);

        let forbidden = ResetFixture::new("reset-current-policy");
        let current_policy = ResetSafety::new(Some(forbidden.root.clone()), Vec::new())
            .expect("canonical current policy");
        let policy_error = commit_reset_with_environment(
            &forbidden.plan,
            forbidden.plan.digest().as_str(),
            forbidden.new_era,
            forbidden.issued_at + Duration::from_secs(1),
            &current_policy,
            None,
        )
        .expect_err("commit must apply the current process policy");
        assert!(
            matches!(policy_error, StoreFormatError::BroadResetTarget(path) if path == forbidden.root)
        );
        assert_eq!(reset_transaction_files(&forbidden.root).len(), 0);
    }

    #[test]
    fn reset_retry_recovers_after_every_filesystem_transition() {
        for transition in ResetTransition::ALL {
            let fixture = ResetFixture::new(transition.name());
            let first = commit_reset_with_environment(
                &fixture.plan,
                fixture.plan.digest().as_str(),
                fixture.new_era.clone(),
                fixture.issued_at + Duration::from_secs(1),
                &fixture.safety,
                Some(*transition),
            )
            .expect_err("fault injection interrupts the first attempt");
            if transition.outcome_is_unknown() {
                assert!(matches!(
                    first,
                    StoreFormatError::ResetCommitOutcomeUnknown { .. }
                ));
            } else {
                assert!(matches!(first, StoreFormatError::ResetInterrupted { .. }));
            }

            let recovered = commit_reset_with_environment(
                &fixture.plan,
                fixture.plan.digest().as_str(),
                fixture.new_era.clone(),
                fixture.issued_at + Duration::from_secs(1),
                &fixture.safety,
                None,
            )
            .expect("retry completes or observes the durable reset");
            let repeated = commit_reset_with_environment(
                &fixture.plan,
                fixture.plan.digest().as_str(),
                fixture.new_era.clone(),
                fixture.issued_at + Duration::from_secs(1),
                &fixture.safety,
                None,
            )
            .expect("committed retry is idempotent");

            assert_eq!(repeated, recovered);
            assert_eq!(recovered.old_store_era_id(), &fixture.old_era);
            assert_eq!(recovered.new_store_era_id(), &fixture.new_era);
            assert_eq!(
                std::fs::read(recovered.quarantine_path().join("evidence.data"))
                    .expect("old evidence remains in quarantine"),
                b"recoverable old evidence"
            );
        }
    }

    #[test]
    fn ordinary_store_open_recovers_an_authorized_in_flight_reset() {
        let fixture = ResetFixture::new("reset-startup-recovery");
        let error = commit_reset_with_environment(
            &fixture.plan,
            fixture.plan.digest().as_str(),
            fixture.new_era.clone(),
            fixture.issued_at + Duration::from_secs(1),
            &fixture.safety,
            Some(ResetTransition::OldStoreRenamed),
        )
        .expect_err("simulate a crash with the canonical root absent");
        assert!(matches!(
            error,
            StoreFormatError::ResetCommitOutcomeUnknown { .. }
        ));

        let lease = StoreLease::acquire(&fixture.root)
            .expect("normal startup repairs the authorized transaction under its lease");
        let managed =
            require_managed_store(&fixture.root, "memory-v1").expect("startup sees the fresh root");
        assert_eq!(managed.manifest().store_era_id(), &fixture.new_era);
        drop(lease);

        let receipt = commit_reset_with_environment(
            &fixture.plan,
            fixture.plan.digest().as_str(),
            fixture.new_era,
            fixture.issued_at + Duration::from_secs(1),
            &fixture.safety,
            None,
        )
        .expect("the original caller observes the startup-completed receipt");
        assert_eq!(receipt.old_store_era_id(), &fixture.old_era);
    }

    #[test]
    fn reset_recovery_recreates_only_the_fresh_staging_side_after_quarantine() {
        let fixture = ResetFixture::new("reset-recreate-staging");
        commit_reset_with_environment(
            &fixture.plan,
            fixture.plan.digest().as_str(),
            fixture.new_era.clone(),
            fixture.issued_at + Duration::from_secs(1),
            &fixture.safety,
            Some(ResetTransition::OldStoreRenamed),
        )
        .expect_err("leave old data quarantined and replacement staged");
        let journal_path = active_journal_path(&fixture.root, fixture.plan.digest());
        let journal = read_reset_record(&fixture.root, &journal_path, false)
            .expect("read journal")
            .expect("active journal exists");
        let staging_path = journal.staging_path(&fixture.root);
        std::fs::remove_dir_all(&staging_path).expect("simulate loss of fresh empty staging");

        let receipt = commit_reset_with_environment(
            &fixture.plan,
            fixture.plan.digest().as_str(),
            fixture.new_era.clone(),
            fixture.issued_at + Duration::from_secs(1),
            &fixture.safety,
            None,
        )
        .expect("recovery may recreate fresh staging, never the old quarantine");

        assert_eq!(receipt.new_store_era_id(), &fixture.new_era);
        assert_eq!(
            std::fs::read(receipt.quarantine_path().join("evidence.data"))
                .expect("the original quarantine is untouched"),
            b"recoverable old evidence"
        );
    }

    #[test]
    fn committed_retry_returns_the_original_receipt_without_reauthorizing() {
        let fixture = ResetFixture::new("reset-original-receipt");
        let receipt = commit_reset_with_environment(
            &fixture.plan,
            fixture.plan.digest().as_str(),
            fixture.new_era.clone(),
            fixture.issued_at + Duration::from_secs(1),
            &fixture.safety,
            None,
        )
        .expect("commit initial reset");
        let now_forbidden = ResetSafety::new(Some(fixture.root.clone()), Vec::new())
            .expect("canonical stricter policy");

        let replay = commit_reset_with_environment(
            &fixture.plan,
            fixture.plan.digest().as_str(),
            fixture.new_era,
            fixture.issued_at + Duration::from_secs(301),
            &now_forbidden,
            None,
        )
        .expect("a durable receipt is an observation, not a new destructive authorization");

        assert_eq!(replay, receipt);
    }

    #[test]
    #[cfg(unix)]
    fn reset_recovery_rejects_unsafe_staging_residue_after_install() {
        let fixture = ResetFixture::new("reset-staging-residue");
        commit_reset_with_environment(
            &fixture.plan,
            fixture.plan.digest().as_str(),
            fixture.new_era.clone(),
            fixture.issued_at + Duration::from_secs(1),
            &fixture.safety,
            Some(ResetTransition::ReplacementInstalled),
        )
        .expect_err("leave the replacement installed before receipt publication");
        let journal_path = active_journal_path(&fixture.root, fixture.plan.digest());
        let journal = read_reset_record(&fixture.root, &journal_path, false)
            .expect("read journal")
            .expect("active journal exists");
        let staging_path = journal.staging_path(&fixture.root);
        std::os::unix::fs::symlink(journal.quarantine_path(&fixture.root), &staging_path)
            .expect("create hostile staging residue");

        let error = StoreLease::acquire(&fixture.root)
            .expect_err("startup refuses to traverse staging residue");
        assert!(
            matches!(error, StoreFormatError::InvalidResetLayout(path) if path == staging_path)
        );
    }

    #[test]
    fn reset_rejects_a_tampered_or_symlinked_journal() {
        let fixture = ResetFixture::new("reset-tampered-journal");
        commit_reset_with_environment(
            &fixture.plan,
            fixture.plan.digest().as_str(),
            fixture.new_era.clone(),
            fixture.issued_at + Duration::from_secs(1),
            &fixture.safety,
            Some(ResetTransition::JournalPublished),
        )
        .expect_err("leave an authorized journal behind");
        let journal = active_journal_path(&fixture.root, fixture.plan.digest());
        std::fs::write(&journal, b"tampered").expect("tamper journal bytes");
        let error =
            StoreLease::acquire(&fixture.root).expect_err("startup refuses a tampered journal");
        assert!(matches!(error, StoreFormatError::InvalidResetJournal(path) if path == journal));

        let symlink_fixture = ResetFixture::new("reset-symlink-journal");
        let journal = active_journal_path(&symlink_fixture.root, symlink_fixture.plan.digest());
        #[cfg(unix)]
        {
            std::os::unix::fs::symlink(&symlink_fixture.root, &journal)
                .expect("create hostile journal symlink");
            let error = commit_reset_with_environment(
                &symlink_fixture.plan,
                symlink_fixture.plan.digest().as_str(),
                symlink_fixture.new_era,
                symlink_fixture.issued_at + Duration::from_secs(1),
                &symlink_fixture.safety,
                None,
            )
            .expect_err("commit refuses a journal symlink");
            assert!(
                matches!(error, StoreFormatError::InvalidResetJournal(path) if path == journal)
            );
        }
    }

    struct ResetFixture {
        _container: TestDir,
        root: PathBuf,
        plan: ResetPlan,
        safety: ResetSafety,
        issued_at: SystemTime,
        old_era: StoreEraId,
        new_era: StoreEraId,
    }

    impl ResetFixture {
        fn new(label: &str) -> Self {
            let container = TestDir::new(label);
            let root = container.path().join("memory");
            std::fs::create_dir(&root).expect("create managed memory root");
            let old_era =
                StoreEraId::parse("01ARZ3NDEKTSV4RRFFQ69G5FAV").expect("valid old era id");
            StoreManifest::new("memory-v1", old_era.clone())
                .expect("valid old manifest")
                .write_new(&root)
                .expect("mark old store");
            std::fs::write(root.join("evidence.data"), b"recoverable old evidence")
                .expect("write old data");
            let root = std::fs::canonicalize(root).expect("canonical memory root");
            let safety = ResetSafety::new(None, Vec::new()).expect("test safety");
            let issued_at = UNIX_EPOCH + Duration::from_secs(1_000);
            let plan = ResetPlan::build(
                std::slice::from_ref(&root),
                "memory-v1",
                issued_at,
                Duration::from_secs(300),
                &safety,
            )
            .expect("build reset plan");
            Self {
                _container: container,
                root,
                plan,
                safety,
                issued_at,
                old_era,
                new_era: StoreEraId::parse("01BX5ZZKBKACTAV9WEVGEMMVRZ").expect("valid new era id"),
            }
        }
    }

    fn reset_transaction_files(root: &Path) -> Vec<PathBuf> {
        let parent = root.parent().expect("test root has parent");
        std::fs::read_dir(parent)
            .expect("read test parent")
            .map(|entry| entry.expect("read entry").path())
            .filter(|path| {
                path.file_name()
                    .and_then(|name| name.to_str())
                    .is_some_and(|name| name.starts_with(".mmdb-reset-"))
            })
            .collect()
    }

    #[test]
    fn reset_plan_rejects_the_filesystem_root_before_marker_probe() {
        let safety = ResetSafety::new(None, Vec::new()).expect("empty explicit safety policy");

        let error = ResetPlan::build(
            &[PathBuf::from("/")],
            "memory-v1",
            UNIX_EPOCH + Duration::from_secs(1_000),
            Duration::from_secs(300),
            &safety,
        )
        .expect_err("filesystem root must never become a reset target");

        assert!(
            matches!(error, StoreFormatError::BroadResetTarget(path) if path == Path::new("/"))
        );
    }

    #[test]
    fn exact_format_mismatch_is_refused_without_opening_store_contents() {
        let root = TestDir::new("format-mismatch");
        StoreManifest::new(
            "memory-v1",
            StoreEraId::parse("01ARZ3NDEKTSV4RRFFQ69G5FAV").expect("valid era id"),
        )
        .expect("valid manifest")
        .write_new(root.path())
        .expect("mark root as managed");
        std::fs::write(root.path().join("opaque.data"), b"must not be interpreted")
            .expect("write opaque store content");

        let error = require_managed_store(root.path(), "memory-v2")
            .expect_err("different exact format must be refused");

        assert!(matches!(
            error,
            StoreFormatError::FormatMismatch {
                expected,
                found,
                ..
            } if expected == "memory-v2" && found == "memory-v1"
        ));
    }

    #[cfg(unix)]
    #[test]
    fn reset_plan_rejects_a_symlink_target_without_following_it() {
        use std::os::unix::fs::symlink;

        let container = TestDir::new("symlink");
        let canonical_container =
            std::fs::canonicalize(container.path()).expect("canonical container");
        let managed_root = canonical_container.join("managed");
        std::fs::create_dir(&managed_root).expect("create managed root");
        StoreManifest::new(
            "memory-v1",
            StoreEraId::parse("01ARZ3NDEKTSV4RRFFQ69G5FAV").expect("valid era id"),
        )
        .expect("valid manifest")
        .write_new(&managed_root)
        .expect("mark root as managed");
        let linked_root = canonical_container.join("linked");
        symlink(&managed_root, &linked_root).expect("create target symlink");
        let safety = ResetSafety::new(None, Vec::new()).expect("empty explicit safety policy");

        let error = ResetPlan::build(
            std::slice::from_ref(&linked_root),
            "memory-v1",
            UNIX_EPOCH + Duration::from_secs(1_000),
            Duration::from_secs(300),
            &safety,
        )
        .expect_err("symlink target must be refused");

        assert!(matches!(
            error,
            StoreFormatError::SymlinkInResetTarget(path) if path == linked_root
        ));
    }

    #[test]
    fn store_era_id_rejects_values_outside_the_ulid_bit_range() {
        let error = StoreEraId::parse("81ARZ3NDEKTSV4RRFFQ69G5FAV")
            .expect_err("first ULID character may not exceed seven");

        assert!(matches!(error, StoreFormatError::InvalidStoreEraId(_)));
    }

    #[test]
    fn reset_plan_requires_a_valid_managed_marker() {
        let root = TestDir::new("missing-marker");
        std::fs::write(root.path().join("legacy.data"), b"legacy")
            .expect("write unrecognized data");
        let canonical_root = std::fs::canonicalize(root.path()).expect("canonical root");
        let safety = ResetSafety::new(None, Vec::new()).expect("empty explicit safety policy");

        let error = ResetPlan::build(
            std::slice::from_ref(&canonical_root),
            "memory-v1",
            UNIX_EPOCH + Duration::from_secs(1_000),
            Duration::from_secs(300),
            &safety,
        )
        .expect_err("unrecognized data must not become reset-authorized");

        assert!(matches!(
            error,
            StoreFormatError::MissingManagedMarker(path) if path == canonical_root
        ));
    }

    #[test]
    fn reset_plan_rejects_home_and_workspace_like_roots() {
        let home = TestDir::new("home-root");
        StoreManifest::new(
            "memory-v1",
            StoreEraId::parse("01ARZ3NDEKTSV4RRFFQ69G5FAV").expect("valid era id"),
        )
        .expect("valid manifest")
        .write_new(home.path())
        .expect("mark home-like root");
        let canonical_home = std::fs::canonicalize(home.path()).expect("canonical home");
        let home_safety =
            ResetSafety::new(Some(canonical_home.clone()), Vec::new()).expect("home policy");

        let home_error = ResetPlan::build(
            std::slice::from_ref(&canonical_home),
            "memory-v1",
            UNIX_EPOCH + Duration::from_secs(1_000),
            Duration::from_secs(300),
            &home_safety,
        )
        .expect_err("home root must be refused");
        assert!(matches!(
            home_error,
            StoreFormatError::BroadResetTarget(path) if path == canonical_home
        ));

        let workspace = TestDir::new("workspace-root");
        StoreManifest::new(
            "memory-v1",
            StoreEraId::parse("01BX5ZZKBKACTAV9WEVGEMMVRZ").expect("valid era id"),
        )
        .expect("valid manifest")
        .write_new(workspace.path())
        .expect("mark workspace-like root");
        std::fs::create_dir(workspace.path().join(".git")).expect("add workspace marker");
        let canonical_workspace =
            std::fs::canonicalize(workspace.path()).expect("canonical workspace");
        let workspace_safety =
            ResetSafety::new(None, Vec::new()).expect("empty explicit safety policy");

        let workspace_error = ResetPlan::build(
            std::slice::from_ref(&canonical_workspace),
            "memory-v1",
            UNIX_EPOCH + Duration::from_secs(1_000),
            Duration::from_secs(300),
            &workspace_safety,
        )
        .expect_err("workspace-like root must be refused");
        assert!(matches!(
            workspace_error,
            StoreFormatError::WorkspaceLikeResetTarget(path) if path == canonical_workspace
        ));
    }

    #[test]
    fn reset_plan_expires_at_its_exact_deadline() {
        let root = TestDir::new("expiry");
        StoreManifest::new(
            "memory-v1",
            StoreEraId::parse("01ARZ3NDEKTSV4RRFFQ69G5FAV").expect("valid era id"),
        )
        .expect("valid manifest")
        .write_new(root.path())
        .expect("mark root as managed");
        let canonical_root = std::fs::canonicalize(root.path()).expect("canonical root");
        let safety = ResetSafety::new(None, Vec::new()).expect("empty explicit safety policy");
        let issued_at = UNIX_EPOCH + Duration::from_secs(1_000);
        let plan = ResetPlan::build(
            &[canonical_root],
            "memory-v1",
            issued_at,
            Duration::from_secs(300),
            &safety,
        )
        .expect("build plan");

        let error = plan
            .validate(
                plan.digest().as_str(),
                issued_at + Duration::from_secs(300),
                &safety,
            )
            .expect_err("deadline is no longer valid");

        assert!(matches!(error, StoreFormatError::ResetPlanExpired { .. }));
    }

    #[test]
    fn reset_plan_detects_metadata_changed_after_planning() {
        let root = TestDir::new("changed");
        StoreManifest::new(
            "memory-v1",
            StoreEraId::parse("01ARZ3NDEKTSV4RRFFQ69G5FAV").expect("valid era id"),
        )
        .expect("valid manifest")
        .write_new(root.path())
        .expect("mark root as managed");
        let canonical_root = std::fs::canonicalize(root.path()).expect("canonical root");
        let safety = ResetSafety::new(None, Vec::new()).expect("empty explicit safety policy");
        let issued_at = UNIX_EPOCH + Duration::from_secs(1_000);
        let plan = ResetPlan::build(
            std::slice::from_ref(&canonical_root),
            "memory-v1",
            issued_at,
            Duration::from_secs(300),
            &safety,
        )
        .expect("build plan");
        std::fs::write(canonical_root.join("arrived-after-plan"), b"changed")
            .expect("change target metadata");

        let error = plan
            .validate(
                plan.digest().as_str(),
                issued_at + Duration::from_secs(1),
                &safety,
            )
            .expect_err("changed target must invalidate plan");

        assert!(matches!(
            error,
            StoreFormatError::ResetTargetChanged(path) if path == canonical_root
        ));
    }

    #[test]
    fn reset_plan_rejects_overlapping_managed_targets() {
        let outer = TestDir::new("overlap");
        StoreManifest::new(
            "memory-v1",
            StoreEraId::parse("01ARZ3NDEKTSV4RRFFQ69G5FAV").expect("valid outer era id"),
        )
        .expect("valid outer manifest")
        .write_new(outer.path())
        .expect("mark outer root");
        let inner = outer.path().join("inner");
        std::fs::create_dir(&inner).expect("create inner root");
        StoreManifest::new(
            "memory-v1",
            StoreEraId::parse("01BX5ZZKBKACTAV9WEVGEMMVRZ").expect("valid inner era id"),
        )
        .expect("valid inner manifest")
        .write_new(&inner)
        .expect("mark inner root");
        let canonical_outer = std::fs::canonicalize(outer.path()).expect("canonical outer");
        let canonical_inner = std::fs::canonicalize(&inner).expect("canonical inner");
        let safety = ResetSafety::new(None, Vec::new()).expect("empty explicit safety policy");

        let error = ResetPlan::build(
            &[canonical_inner.clone(), canonical_outer.clone()],
            "memory-v1",
            UNIX_EPOCH + Duration::from_secs(1_000),
            Duration::from_secs(300),
            &safety,
        )
        .expect_err("nested destructive targets must be refused");

        assert!(matches!(
            error,
            StoreFormatError::OverlappingResetTargets { ancestor, descendant }
                if ancestor == canonical_outer && descendant == canonical_inner
        ));
    }

    #[test]
    fn reset_plan_is_not_valid_before_its_issue_time() {
        let root = TestDir::new("not-yet-valid");
        StoreManifest::new(
            "memory-v1",
            StoreEraId::parse("01ARZ3NDEKTSV4RRFFQ69G5FAV").expect("valid era id"),
        )
        .expect("valid manifest")
        .write_new(root.path())
        .expect("mark root as managed");
        let canonical_root = std::fs::canonicalize(root.path()).expect("canonical root");
        let safety = ResetSafety::new(None, Vec::new()).expect("empty explicit safety policy");
        let issued_at = UNIX_EPOCH + Duration::from_secs(1_000);
        let plan = ResetPlan::build(
            &[canonical_root],
            "memory-v1",
            issued_at,
            Duration::from_secs(300),
            &safety,
        )
        .expect("build plan");

        let error = plan
            .validate(
                plan.digest().as_str(),
                issued_at - Duration::from_millis(1),
                &safety,
            )
            .expect_err("plan must not survive a backwards clock jump");

        assert!(matches!(
            error,
            StoreFormatError::ResetPlanNotYetValid { .. }
        ));
    }

    #[test]
    fn reset_plan_requires_the_presented_confirmation_digest() {
        let root = TestDir::new("wrong-digest");
        StoreManifest::new(
            "memory-v1",
            StoreEraId::parse("01ARZ3NDEKTSV4RRFFQ69G5FAV").expect("valid era id"),
        )
        .expect("valid manifest")
        .write_new(root.path())
        .expect("mark root as managed");
        let canonical_root = std::fs::canonicalize(root.path()).expect("canonical root");
        let safety = ResetSafety::new(None, Vec::new()).expect("empty explicit safety policy");
        let issued_at = UNIX_EPOCH + Duration::from_secs(1_000);
        let plan = ResetPlan::build(
            &[canonical_root],
            "memory-v1",
            issued_at,
            Duration::from_secs(300),
            &safety,
        )
        .expect("build plan");

        let error = plan
            .validate(
                "0000000000000000000000000000000000000000000000000000000000000000",
                issued_at + Duration::from_secs(1),
                &safety,
            )
            .expect_err("confirmation must present the planned digest");

        assert!(matches!(error, StoreFormatError::ResetPlanDigestMismatch));
    }

    #[test]
    fn reset_plan_rejects_a_ttl_that_rounds_to_zero_milliseconds() {
        let root = TestDir::new("sub-millisecond-ttl");
        StoreManifest::new(
            "memory-v1",
            StoreEraId::parse("01ARZ3NDEKTSV4RRFFQ69G5FAV").expect("valid era id"),
        )
        .expect("valid manifest")
        .write_new(root.path())
        .expect("mark root as managed");
        let canonical_root = std::fs::canonicalize(root.path()).expect("canonical root");
        let safety = ResetSafety::new(None, Vec::new()).expect("empty explicit safety policy");

        let error = ResetPlan::build(
            &[canonical_root],
            "memory-v1",
            UNIX_EPOCH + Duration::from_secs(1_000),
            Duration::from_nanos(1),
            &safety,
        )
        .expect_err("serialized expiry must be later than issue time");

        assert!(matches!(error, StoreFormatError::InvalidResetPlanTtl(_)));
    }
}
