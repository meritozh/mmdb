//! Quickstart: insert an Episode + a Fact, scan recent memories, then delete.
use mmdb::{Database, NodeBuilder};
use mmdb_core::NodeKind;
use tempfile::tempdir;

fn main() -> anyhow::Result<()> {
    let dir = tempdir()?;
    let db = Database::open(dir.path())?;

    let ep = NodeBuilder::new(1, NodeKind::Episode)
        .text("User asked about quarterly revenue.")
        .metadata("session", serde_json::json!("s-001"))
        .build();
    let ep_id = db.insert(ep)?;

    let fact = NodeBuilder::new(1, NodeKind::Fact)
        .text("Q1 2026 revenue = 42.0M USD")
        .embedding("text-embedding-3-small", vec![0.1; 8])
        .build();
    let _ = db.insert(fact)?;

    let recent = db.scan_by_time(1, 0, mmdb::now_ms() + 1, 50)?;
    println!("recent count = {}", recent.len());
    for n in &recent {
        println!("  {:?} {:?}", n.kind, n.content);
    }

    db.delete(1, ep_id)?;
    println!("deleted episode {ep_id}");
    Ok(())
}
