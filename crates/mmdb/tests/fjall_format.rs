use mmdb::store_format::{commit_reset, ResetPlan, ResetSafety, StoreEraId};
use mmdb::{
    Actor, ContextAccess, Database, StateStore, Store, StoreFormatDescriptor, STORE_FORMAT_ID,
};
use mmdb_blob::BlobStore;
use std::{
    collections::BTreeMap,
    fs,
    path::{Path, PathBuf},
    process::Command,
    time::{Duration, SystemTime},
};

fn inventory(root: &Path) -> BTreeMap<PathBuf, Option<blake3::Hash>> {
    fn visit(root: &Path, path: &Path, entries: &mut BTreeMap<PathBuf, Option<blake3::Hash>>) {
        for child in fs::read_dir(path).unwrap() {
            let path = child.unwrap().path();
            let name = path.strip_prefix(root).unwrap().to_owned();
            if path.is_dir() {
                entries.insert(name, None);
                visit(root, &path, entries);
            } else {
                entries.insert(name, Some(blake3::hash(&fs::read(path).unwrap())));
            }
        }
    }
    let mut entries = BTreeMap::new();
    visit(root, root, &mut entries);
    entries
}

#[test]
fn compound_and_standalone_opens_refuse_incompatible_roots_before_mutation() {
    for (main_version, meta_version) in [(2, 3), (3, 2), (2, 0), (0, 2)] {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        let metadata = root.join("blob-meta");
        fs::create_dir(&metadata).unwrap();
        for (path, version) in [(root, main_version), (metadata.as_path(), meta_version)] {
            match version {
                2 => fs::write(path.join("version"), b"FJL\x02").unwrap(),
                3 => drop(fjall::Database::builder(path).open().unwrap()),
                _ => {}
            }
        }
        let before = inventory(root);
        let old = if main_version == 2 {
            root
        } else {
            metadata.as_path()
        };
        for err in [
            Database::open(root).err().unwrap(),
            BlobStore::open(root).err().unwrap(),
            BlobStore::open_with_repair(root).err().unwrap(),
        ] {
            assert!(
                err.to_string().contains("Fjall 2")
                    && err.to_string().contains(old.to_str().unwrap()),
                "{err}"
            );
        }
        assert_eq!(before, inventory(root));
    }
}

#[test]
fn damaged_markers_and_orphan_bytes_survive_open_and_repair() {
    for (dir_name, file_name, bytes) in [
        ("blob-meta", "version", &b"FJ"[..]),
        ("blob-meta", "version", b"FJL\x04"),
        ("blob-meta", "journal", b"residue"),
        ("blobs", "orphan", b"keep me"),
    ] {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        fs::create_dir(root.join(dir_name)).unwrap();
        fs::write(root.join(dir_name).join(file_name), bytes).unwrap();
        let before = inventory(root);
        assert!(Database::open(root).is_err());
        assert!(BlobStore::open_with_repair(root).is_err());
        assert_eq!(before, inventory(root));
    }
}

#[test]
fn missing_blob_metadata_keyspace_preserves_payload_and_both_databases() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    drop(fjall::Database::builder(root).open().unwrap());
    let metadata = root.join("blob-meta");
    let db = fjall::Database::builder(&metadata).open().unwrap();
    db.keyspace("unrelated", Default::default).unwrap();
    drop(db);
    let bytes = b"retained payload";
    let hash = blake3::hash(bytes).to_hex();
    let shard = root.join("blobs").join(&hash[..2]);
    fs::create_dir_all(&shard).unwrap();
    let payload = shard.join(hash.as_str());
    fs::write(&payload, bytes).unwrap();

    for error in [
        BlobStore::open_with_repair(root).err(),
        BlobStore::open(root).err(),
        Database::open(root).err(),
    ] {
        let message = error
            .expect("the missing metadata keyspace must reject open")
            .to_string();
        assert!(
            message.contains("metadata") && message.contains(metadata.to_str().unwrap()),
            "{message}"
        );
        assert_eq!(fs::read(&payload).unwrap(), bytes);
    }
    let db = fjall::Database::builder(&metadata).open().unwrap();
    assert!(db.keyspace_exists("unrelated"));
    assert!(!db.keyspace_exists("m"));
    assert_eq!(
        fjall::Database::builder(root)
            .open()
            .unwrap()
            .keyspace_count(),
        0
    );
}

// Prepare MMDB_V2_FIXTURE using tools/fjall-v2/fixture.py. All destructive
// operations below target independent copies inside the test's temporary root.
#[test]
#[ignore = "requires the original reader and two real Fjall 2 fixture directories"]
fn old_roots_are_refused_then_quarantined_for_the_original_reader() {
    let fixture = PathBuf::from(std::env::var_os("MMDB_V2_FIXTURE").expect("set MMDB_V2_FIXTURE"));
    let temp = tempfile::tempdir().unwrap();
    let parent = temp.path().canonicalize().unwrap();
    let managed = parent.join("managed");
    let state = parent.join("state");
    for root in [&managed, &state] {
        let old = fixture.join(root.file_name().unwrap());
        fs::create_dir(root).unwrap();
        for (relative, hash) in inventory(&old) {
            if hash.is_some() {
                fs::copy(old.join(&relative), root.join(relative)).unwrap();
            } else {
                fs::create_dir(root.join(relative)).unwrap();
            }
        }
        assert_eq!(inventory(&old), inventory(root));
    }
    let state_id = "fixture.runtime-v1";
    let descriptor = StoreFormatDescriptor::new(state_id).unwrap();
    for _ in 0..2 {
        let error = Store::open(&managed).err().unwrap().to_string();
        assert!(
            error.contains(STORE_FORMAT_ID) && error.contains("mmdb-store-v1"),
            "{error}"
        );
        let error = StateStore::open_path(&state, 7, &descriptor)
            .err()
            .unwrap()
            .to_string();
        assert!(
            error.contains("Fjall 2") && error.contains(state.to_str().unwrap()),
            "{error}"
        );
    }
    let safety = ResetSafety::for_current_process(vec![]).unwrap();
    let quarantine = |root: &PathBuf, old_id: &str, new_id: &str| {
        let old = fixture.join(root.file_name().unwrap());
        assert_eq!(inventory(&old), inventory(root));
        let plan = ResetPlan::build(
            std::slice::from_ref(root),
            old_id,
            new_id,
            SystemTime::now(),
            Duration::from_secs(60),
            &safety,
        )
        .unwrap();
        let receipt = commit_reset(&plan, plan.digest().as_str(), StoreEraId::new(), &[]).unwrap();
        let previous = receipt.quarantine_path().to_owned();
        assert_eq!(inventory(&old), inventory(&previous));
        previous
    };
    let previous_managed = quarantine(&managed, "mmdb-store-v1", STORE_FORMAT_ID);
    let previous_state = quarantine(&state, state_id, state_id);
    let store = Store::open(&managed).unwrap();
    let log = store
        .session_log(ContextAccess::new("fixture-owner", Actor::System))
        .unwrap();
    assert_eq!(log.position("session-1").unwrap(), 0);
    let fresh = StateStore::open_path(state, 7, &descriptor).unwrap();
    let identities = fixture.join("identities.json");
    let rows: serde_json::Value = serde_json::from_slice(&fs::read(&identities).unwrap()).unwrap();
    for row in rows["state"].as_array().unwrap() {
        assert!(fresh
            .get(
                row["namespace"].as_str().unwrap(),
                row["key"].as_str().unwrap()
            )
            .unwrap()
            .is_none());
    }
    let status = Command::new(fixture.join("reader"))
        .arg("read")
        .arg(identities)
        .arg(previous_managed)
        .arg(previous_state)
        .status()
        .unwrap();
    assert!(status.success(), "original reader exited with {status}");
}
