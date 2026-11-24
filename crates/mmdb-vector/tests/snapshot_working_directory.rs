//! A separate test binary isolates the process-wide working directory change.
use fjall::Database;
use mmdb_vector::VectorStore;
use std::{env, fs};
use ulid::Ulid;

#[test]
fn relative_snapshot_paths_stay_at_their_open_location() -> Result<(), Box<dyn std::error::Error>> {
    let original = env::current_dir()?;
    let dir = tempfile::tempdir()?;
    let other = dir.path().join("other");
    fs::create_dir(&other)?;
    env::set_current_dir(dir.path())?;
    let default = VectorStore::open(Database::builder("default").open()?, "default")?;
    let custom =
        VectorStore::open_with_snapshot_dir(Database::builder("custom").open()?, "checkpoints")?;
    for store in [&default, &custom] {
        store.insert(0, "m", Ulid::new(), &[1.0, 0.0])?;
    }

    env::set_current_dir(&other)?;
    let written = (default.flush_snapshots()?, custom.flush_snapshots()?);
    env::set_current_dir(original)?;
    drop((default, custom));
    assert_eq!(written, (1, 1));
    assert_eq!(fs::read_dir(&other)?.count(), 0);

    let root = dir.path().join("default");
    let reopened = VectorStore::open(Database::builder(&root).open()?, &root)?;
    assert_eq!(reopened.snapshot_reload_count(), 1);
    let reopened = VectorStore::open_with_snapshot_dir(
        Database::builder(dir.path().join("custom")).open()?,
        dir.path().join("checkpoints"),
    )?;
    assert_eq!(reopened.snapshot_reload_count(), 1);
    Ok(())
}
