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
        partitions: &["entries"],
    },
];

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
