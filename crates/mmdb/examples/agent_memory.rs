//! Quickstart: insert an Episode + a Fact, scan recent memories, then delete.
//! Single-tenant API — no tenant id passed.
use mmdb::{Database, NodeBuilder, DEFAULT_MODEL};
use mmdb_core::NodeKind;
use tempfile::tempdir;

fn main() -> anyhow::Result<()> {
    let dir = tempdir()?;
    let db = Database::open(dir.path())?;

    let ep = NodeBuilder::new(NodeKind::Episode)
        .text("User asked about quarterly revenue.")
        .metadata("session", serde_json::json!("s-001"))
        .build();
    let ep_id = db.insert(ep)?;

    let fact = NodeBuilder::new(NodeKind::Fact)
        .text("Q1 2026 revenue = 42.0M USD")
        .embedding(DEFAULT_MODEL, vec![0.1; 8])
        .build();
    let _ = db.insert(fact)?;

    let recent = db.scan_by_time(0, mmdb::now_ms() + 1, 50)?;
    println!("recent count = {}", recent.len());
    for n in &recent {
        println!("  {:?} {:?}", n.kind, n.content);
    }

    // Vector search stub (returns empty until P1 mmdb-vector lands).
    let hits = db.vector_search(&[0.1; 8], 5)?;
    println!("vector hits (stub) = {}", hits.len());

    db.delete(ep_id)?;
    println!("deleted episode {ep_id}");
    Ok(())
}
