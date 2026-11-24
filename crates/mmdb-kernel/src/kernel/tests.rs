use super::*;

const SESSION: ModuleId = ModuleId("session");
const MEMORY: ModuleId = ModuleId("memory");
const MODULES: &[ModuleSpec] = &[
    ModuleSpec {
        id: SESSION,
        partitions: &["entries"],
    },
    ModuleSpec {
        id: MEMORY,
        partitions: &["entries", "index"],
    },
];

#[derive(PartialEq, Eq)]
pub(super) enum ResetFault {
    UnappliedCommit,
    LostCommitAck,
    DeletePartition(&'static str),
}

fn format() -> StoreFormatDescriptor {
    StoreFormatDescriptor::new("test-v1").unwrap()
}

fn create(root: &Path) -> Kernel {
    Kernel::create(root, &format(), MODULES).unwrap()
}

fn open(root: &Path) -> KernelResult<Kernel> {
    Kernel::open(root, &format(), MODULES)
}

fn assert_busy(root: &Path) {
    assert!(matches!(
        open(root),
        Err(KernelError::Format(StoreFormatError::StoreBusy(_)))
    ));
}

#[test]
fn a_second_open_of_a_store_is_busy() {
    let parent = tempfile::tempdir().unwrap();
    let root = parent.path().join("store");
    let first = create(&root);
    assert_busy(&root);
    drop(first);
    open(&root).unwrap();
}

#[test]
fn handles_keep_the_lease_until_the_last_one_drops() {
    let parent = tempfile::tempdir().unwrap();
    let root = parent.path().join("store");
    let kernel = create(&root);
    let module = kernel.module(SESSION);
    let partition = module.partition("entries").unwrap();
    let mut batch = kernel.batch();
    batch.insert(&partition, "a", "saved");
    drop(kernel);
    assert_busy(&root);
    drop(module);
    assert_busy(&root);
    drop(partition);
    assert_busy(&root);
    batch.commit().unwrap();
    let partition = open(&root)
        .unwrap()
        .module(SESSION)
        .partition("entries")
        .unwrap();
    assert_eq!(partition.get("a").unwrap().as_deref(), Some(&b"saved"[..]));
    assert_busy(&root);
    drop(partition);
    open(&root).unwrap();
}

#[test]
fn store_metadata_is_created_once_and_checked_against_the_manifest() {
    let parent = tempfile::tempdir().unwrap();
    let root = parent.path().join("store");
    // Whole-store reset leaves a manifest-only directory for the first open.
    fs::create_dir(&root).unwrap();
    let manifest = format().new_manifest().unwrap();
    manifest.write_new(&root).unwrap();
    let kernel = open(&root).unwrap();
    let session = kernel.era(SESSION);
    let memory = kernel.era(MEMORY);
    let digest = kernel.keyed_hasher("operation").finalize();
    assert_ne!(digest, kernel.keyed_hasher("plan").finalize());
    assert_eq!(kernel.canonical_root(), fs::canonicalize(&root).unwrap());
    assert_eq!(kernel.store_era(), manifest.store_era_id());
    assert_ne!(session, memory);
    drop(kernel);

    let reopened = open(&root).unwrap();
    assert_eq!(reopened.era_state(SESSION, session), EraState::Current);
    assert_eq!(reopened.era_state(SESSION, memory), EraState::Unknown);
    assert_eq!(reopened.module(MEMORY).era(), memory);
    assert_eq!(reopened.keyed_hasher("operation").finalize(), digest);
    drop(reopened);

    let keyspace = Config::new(&root).open().unwrap();
    let partition = keyspace
        .open_partition(KERNEL_PARTITION, Default::default())
        .unwrap();
    let mut metadata: StoreMetadata =
        serde_json::from_slice(&partition.get(STORE_KEY).unwrap().unwrap()).unwrap();
    metadata.store_era = StoreEraId::new().to_string();
    let mut batch = durable_batch(&keyspace);
    batch.insert(
        &partition,
        STORE_KEY,
        serde_json::to_vec(&metadata).unwrap(),
    );
    batch.commit().unwrap();
    drop(partition);
    drop(keyspace);
    assert!(matches!(open(&root), Err(KernelError::Corrupt(_))));

    let keyspace = Config::new(&root).open().unwrap();
    let partition = keyspace
        .open_partition(KERNEL_PARTITION, Default::default())
        .unwrap();
    let mut batch = durable_batch(&keyspace);
    batch.remove(&partition, STORE_KEY);
    batch.commit().unwrap();
    drop(partition);
    drop(keyspace);
    assert!(matches!(open(&root), Err(KernelError::Corrupt(_))));
}

#[test]
fn open_rejects_another_format() {
    let parent = tempfile::tempdir().unwrap();
    let root = parent.path().join("store");
    drop(create(&root));
    let foreign = StoreFormatDescriptor::new("other").unwrap();
    assert!(matches!(
        Kernel::open(&root, &foreign, MODULES),
        Err(KernelError::Format(StoreFormatError::FormatMismatch { .. }))
    ));
    open(&root).unwrap();
}

#[test]
fn module_specs_are_checked_before_opening() {
    const BAD_MODULE: &[ModuleSpec] = &[ModuleSpec {
        id: ModuleId("bad.name"),
        partitions: &[],
    }];
    const EMPTY_MODULE: &[ModuleSpec] = &[ModuleSpec {
        id: ModuleId(""),
        partitions: &[],
    }];
    const BAD_NAME: &[ModuleSpec] = &[ModuleSpec {
        id: MEMORY,
        partitions: &["Not_lower"],
    }];
    const EMPTY_NAME: &[ModuleSpec] = &[ModuleSpec {
        id: MEMORY,
        partitions: &[""],
    }];
    const DUPLICATE_NAME: &[ModuleSpec] = &[ModuleSpec {
        id: MEMORY,
        partitions: &["x", "x"],
    }];
    const DUPLICATE_MODULE: &[ModuleSpec] = &[
        ModuleSpec {
            id: MEMORY,
            partitions: &["x"],
        },
        ModuleSpec {
            id: MEMORY,
            partitions: &["y"],
        },
    ];
    const TOO_LONG: &[ModuleSpec] = &[ModuleSpec {
        id: MEMORY,
        partitions: &[concat!(
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        )],
    }];
    let parent = tempfile::tempdir().unwrap();
    let root = parent.path().join("store");
    for specs in [
        BAD_MODULE,
        EMPTY_MODULE,
        BAD_NAME,
        EMPTY_NAME,
        DUPLICATE_NAME,
        DUPLICATE_MODULE,
        TOO_LONG,
    ] {
        assert!(matches!(
            Kernel::create(&root, &format(), specs),
            Err(KernelError::InvalidModuleSpec(_))
        ));
        assert!(matches!(
            Kernel::open(&root, &format(), specs),
            Err(KernelError::InvalidModuleSpec(_))
        ));
    }
    assert_eq!(fs::read_dir(parent.path()).unwrap().count(), 0);
}

#[test]
#[should_panic(expected = "partition belongs to another kernel")]
fn a_batch_refuses_a_partition_of_another_store() {
    let parent = tempfile::tempdir().unwrap();
    let first = create(&parent.path().join("first"));
    let second = create(&parent.path().join("second"));
    first
        .batch()
        .remove(&second.module(SESSION).partition("entries").unwrap(), "a");
}

#[test]
fn a_batch_commits_across_modules_and_partitions_read_in_key_order() {
    let parent = tempfile::tempdir().unwrap();
    let root = parent.path().join("store");
    let kernel = create(&root);
    let session = kernel.module(SESSION).partition("entries").unwrap();
    let memory = kernel.module(MEMORY).partition("entries").unwrap();
    assert!(matches!(
        kernel.module(MEMORY).partition("unknown"),
        Err(KernelError::UnknownPartition { .. })
    ));
    let mut batch = kernel.batch();
    batch.insert(&session, "aa", "one");
    batch.insert(&session, "ab", "two");
    batch.insert(&session, "b", "three");
    batch.insert(&memory, "aa", "other");
    batch.remove(&session, "b");
    assert!(!session.contains_key("aa").unwrap());
    batch.commit().unwrap();
    assert_eq!(session.iter().count(), 2);
    assert_eq!(&*session.prefix("a").next_back().unwrap().unwrap().0, b"ab");
    assert_eq!(
        &*session.range("aa"..="aa").next().unwrap().unwrap().1,
        b"one"
    );
    assert_eq!(memory.get("aa").unwrap().as_deref(), Some(&b"other"[..]));
}

#[test]
fn open_cleans_noncurrent_partitions_and_rejects_unrecognized_names() {
    let parent = tempfile::tempdir().unwrap();
    let root = parent.path().join("store");
    let kernel = create(&root);
    let current = partition_name(SESSION.0, kernel.era(SESSION), "entries");
    drop(kernel);
    let keyspace = Config::new(&root).open().unwrap();
    let leftover = partition_name(SESSION.0, EraId::new(), "entries");
    keyspace
        .open_partition(&leftover, Default::default())
        .unwrap();
    keyspace
        .delete_partition(
            keyspace
                .open_partition(&current, Default::default())
                .unwrap(),
        )
        .unwrap();
    drop(keyspace);
    drop(open(&root).unwrap());
    let keyspace = Config::new(&root).open().unwrap();
    assert!(keyspace.partition_exists(&current));
    assert!(!keyspace.partition_exists(&leftover));
    keyspace
        .open_partition("stray", Default::default())
        .unwrap();
    drop(keyspace);
    assert!(matches!(open(&root), Err(KernelError::Corrupt(_))));
}

#[test]
fn module_reset_deletes_the_old_era_partitions() {
    let parent = tempfile::tempdir().unwrap();
    let root = parent.path().join("store");
    let kernel = create(&root);
    let session = kernel.module(SESSION);
    let memory = kernel.module(MEMORY);
    let entries = memory.partition("entries").unwrap();
    let receipts = session.partition("entries").unwrap();
    let mut seed = kernel.batch();
    seed.insert(&entries, "a", "old");
    seed.commit().unwrap();

    let replacement = EraId::new();
    let prepared = kernel.prepare_module_reset(MEMORY, replacement).unwrap();
    assert_eq!(memory.era(), kernel.era(MEMORY));
    assert_eq!(kernel.era_state(MEMORY, replacement), EraState::Unknown);
    assert!(entries.contains_key("a").unwrap());
    assert!(!prepared
        .partition("entries")
        .unwrap()
        .contains_key("a")
        .unwrap());
    let mut batch = kernel.batch();
    batch.insert(&receipts, "reset", "done");
    let report = kernel.commit_module_reset(prepared, batch).unwrap();
    assert_eq!(report.old_era, memory.era());
    assert_eq!(report.new_era, replacement);
    assert!(!report.cleanup_pending);
    assert_eq!(kernel.era(SESSION), session.era());
    assert_eq!(kernel.era(MEMORY), replacement);
    assert!(receipts.contains_key("reset").unwrap());
    for name in &kernel.0.schema.0[MEMORY.0] {
        assert!(!kernel
            .0
            .keyspace
            .partition_exists(&partition_name(MEMORY.0, memory.era(), name)));
        assert!(kernel
            .0
            .keyspace
            .partition_exists(&partition_name(MEMORY.0, replacement, name)));
    }
    assert!(matches!(
        memory.partition("entries"),
        Err(KernelError::StaleEra { .. })
    ));
    drop((kernel, entries, memory, receipts, session));
    let kernel = open(&root).unwrap();
    assert!(kernel
        .module(SESSION)
        .partition("entries")
        .unwrap()
        .contains_key("reset")
        .unwrap());
    assert!(!kernel
        .module(MEMORY)
        .partition("entries")
        .unwrap()
        .contains_key("a")
        .unwrap());
    assert_eq!(kernel.era_state(MEMORY, report.old_era), EraState::Retired);
}

#[test]
fn module_reset_cleanup_resumes_after_interruption() {
    let parent = tempfile::tempdir().unwrap();
    let root = parent.path().join("store");
    let kernel = create(&root);
    let old = kernel.era(MEMORY);
    let replacement = EraId::new();
    let abandoned = EraId::new();
    kernel.prepare_module_reset(MEMORY, abandoned).unwrap();
    let prepared = kernel.prepare_module_reset(MEMORY, replacement).unwrap();
    *kernel.0.reset_fault.lock() = Some(ResetFault::DeletePartition("index"));
    let report = kernel
        .commit_module_reset(prepared, kernel.batch())
        .unwrap();
    assert!(report.cleanup_pending);
    assert!(!kernel
        .0
        .keyspace
        .partition_exists(&partition_name(MEMORY.0, old, "entries")));
    assert!(kernel
        .0
        .keyspace
        .partition_exists(&partition_name(MEMORY.0, old, "index")));
    assert_eq!(kernel.era_state(MEMORY, old), EraState::Retired);
    drop(kernel);

    let kernel = open(&root).unwrap();
    for name in &kernel.0.schema.0[MEMORY.0] {
        assert!(!kernel
            .0
            .keyspace
            .partition_exists(&partition_name(MEMORY.0, old, name)));
        assert!(!kernel
            .0
            .keyspace
            .partition_exists(&partition_name(MEMORY.0, abandoned, name)));
        assert!(kernel
            .0
            .keyspace
            .partition_exists(&partition_name(MEMORY.0, replacement, name)));
    }
    assert_eq!(kernel.era_state(MEMORY, old), EraState::Retired);
    assert_eq!(kernel.era_state(MEMORY, abandoned), EraState::Unknown);
    assert_eq!(kernel.era_state(MEMORY, replacement), EraState::Current);
}

#[test]
fn module_reset_refuses_a_current_or_retired_era() {
    let parent = tempfile::tempdir().unwrap();
    let root = parent.path().join("store");
    let kernel = create(&root);
    let original = kernel.era(MEMORY);
    let middle = EraId::new();
    let last = EraId::new();
    for era in [middle, last] {
        let prepared = kernel.prepare_module_reset(MEMORY, era).unwrap();
        kernel
            .commit_module_reset(prepared.clone(), kernel.batch())
            .unwrap();
        assert!(
            matches!(kernel.commit_module_reset(prepared, kernel.batch()),
            Err(KernelError::InvalidEra(id)) if id == era)
        );
    }
    drop(kernel);
    let kernel = open(&root).unwrap();
    for retired in [original, middle] {
        assert_eq!(kernel.era_state(MEMORY, retired), EraState::Retired);
        assert_eq!(kernel.era_state(SESSION, retired), EraState::Unknown);
    }
    for era in [original, middle, last] {
        assert!(matches!(kernel.prepare_module_reset(MEMORY, era),
            Err(KernelError::InvalidEra(id)) if id == era));
    }
    assert_eq!(kernel.era_state(MEMORY, EraId::new()), EraState::Unknown);
}

#[test]
fn a_batch_refuses_partitions_of_a_stale_era() {
    let parent = tempfile::tempdir().unwrap();
    let root = parent.path().join("store");
    let kernel = create(&root);
    let memory = kernel.module(MEMORY);
    let entries = memory.partition("entries").unwrap();
    let receipts = kernel.module(SESSION).partition("entries").unwrap();
    let cross_module_write = || {
        let mut batch = kernel.batch();
        batch.insert(&entries, "a", "payload");
        batch.insert(&receipts, "a", "receipt");
        batch
    };
    let late = cross_module_write();
    let replacement = EraId::new();
    let prepared = kernel.prepare_module_reset(MEMORY, replacement).unwrap();
    kernel
        .commit_module_reset(prepared, kernel.batch())
        .unwrap();
    assert!(
        matches!(late.commit(), Err(KernelError::StaleEra { module: "memory", era }) if era == memory.era())
    );
    let another = kernel.prepare_module_reset(MEMORY, EraId::new()).unwrap();
    assert!(
        matches!(kernel.commit_module_reset(another, cross_module_write()),
        Err(KernelError::StaleEra { module: "memory", era }) if era == memory.era())
    );
    assert!(!receipts.contains_key("a").unwrap());
    assert_eq!(kernel.era(MEMORY), replacement);
    let mut valid = kernel.batch();
    valid.insert(&receipts, "b", "ok");
    valid.commit().unwrap();
}

#[test]
fn an_unknown_reset_outcome_poisons_the_kernel() {
    for applied in [false, true] {
        let parent = tempfile::tempdir().unwrap();
        let root = parent.path().join("store");
        let kernel = create(&root);
        let memory = kernel.module(MEMORY);
        let receipts = kernel.module(SESSION).partition("entries").unwrap();
        let replacement = EraId::new();
        let prepared = kernel.prepare_module_reset(MEMORY, replacement).unwrap();
        let mut batch = kernel.batch();
        batch.insert(&receipts, "reset", "done");
        let mut pending_read = receipts.iter();
        let digest = kernel.keyed_hasher("test").finalize();
        *kernel.0.reset_fault.lock() = Some(if applied {
            ResetFault::LostCommitAck
        } else {
            ResetFault::UnappliedCommit
        });
        assert!(matches!(
            kernel.commit_module_reset(prepared.clone(), batch),
            Err(KernelError::CommitUnknown)
        ));
        assert_eq!(kernel.era(MEMORY), memory.era());
        assert_eq!(kernel.module(MEMORY).era(), memory.era());
        assert_eq!(kernel.era_state(MEMORY, replacement), EraState::Unknown);
        assert_eq!(kernel.keyed_hasher("test").finalize(), digest);
        drop(kernel.write_lock());
        assert!(matches!(
            memory.partition("entries"),
            Err(KernelError::Poisoned)
        ));
        assert!(matches!(receipts.get("reset"), Err(KernelError::Poisoned)));
        assert!(matches!(
            receipts.contains_key("reset"),
            Err(KernelError::Poisoned)
        ));
        assert!(matches!(
            pending_read.next(),
            Some(Err(KernelError::Poisoned))
        ));
        assert!(pending_read.next_back().is_none());
        assert!(matches!(
            receipts.iter().next(),
            Some(Err(KernelError::Poisoned))
        ));
        assert!(matches!(
            receipts.prefix("z").next(),
            Some(Err(KernelError::Poisoned))
        ));
        assert!(matches!(
            receipts.range("a"..="z").next_back(),
            Some(Err(KernelError::Poisoned))
        ));
        assert!(matches!(
            kernel.batch().commit(),
            Err(KernelError::Poisoned)
        ));
        assert!(matches!(
            kernel.prepare_module_reset(MEMORY, EraId::new()),
            Err(KernelError::Poisoned)
        ));
        assert!(matches!(
            kernel.commit_module_reset(prepared, kernel.batch()),
            Err(KernelError::Poisoned)
        ));
        let old = memory.era();
        drop(pending_read);
        drop((kernel, receipts, memory));

        let kernel = open(&root).unwrap();
        let (current, leftover, leftover_state) = if applied {
            (replacement, old, EraState::Retired)
        } else {
            (old, replacement, EraState::Unknown)
        };
        assert_eq!(kernel.era(MEMORY), current);
        assert_eq!(kernel.era_state(MEMORY, leftover), leftover_state);
        assert!(!kernel
            .0
            .keyspace
            .partition_exists(&partition_name(MEMORY.0, leftover, "entries")));
        assert_eq!(
            kernel
                .module(SESSION)
                .partition("entries")
                .unwrap()
                .contains_key("reset")
                .unwrap(),
            applied
        );
    }
}
