//! Quickstart: insert an Episode + a Fact, scan recent memories, run vector
//! search, then delete. Single-tenant API — no tenant id passed.
use mmdb::{Database, NodeBuilder, DEFAULT_MODEL};
use mmdb_core::NodeKind;
use tempfile::tempdir;

fn norm(v: Vec<f32>) -> Vec<f32> {
    let n: f32 = v.iter().map(|x| x * x).sum::<f32>().sqrt();
    if n == 0.0 { v } else { v.into_iter().map(|x| x / n).collect() }
}

fn main() -> anyhow::Result<()> {
    let dir = tempdir()?;
    let db = Database::open(dir.path())?;

    // 1) Insert an Episode without embedding — purely time-indexed.
    let ep_id = db.insert(
        NodeBuilder::new(NodeKind::Episode)
            .text("User asked about quarterly revenue.")
            .metadata("session", serde_json::json!("s-001"))
            .build(),
    )?;

    // 2) Insert three Facts with embeddings in a tiny 4-D space.
    let revenue_id = db.insert(
        NodeBuilder::new(NodeKind::Fact)
            .text("Q1 2026 revenue = 42.0M USD")
            .embedding(DEFAULT_MODEL, norm(vec![1.0, 0.0, 0.0, 0.0]))
            .build(),
    )?;
    let _ = db.insert(
        NodeBuilder::new(NodeKind::Fact)
            .text("CEO hometown is Seattle")
            .embedding(DEFAULT_MODEL, norm(vec![0.0, 1.0, 0.0, 0.0]))
            .build(),
    )?;
    let _ = db.insert(
        NodeBuilder::new(NodeKind::Fact)
            .text("Q2 2026 revenue projection = 45.0M USD")
            .embedding(DEFAULT_MODEL, norm(vec![0.95, 0.05, 0.0, 0.0]))
            .build(),
    )?;

    // 3) Time-range scan.
    let recent = db.scan_by_time(0, mmdb::now_ms() + 1, 50)?;
    println!("recent count = {}", recent.len());

    // 4) Vector search — query close to the "revenue" axis.
    let query = norm(vec![1.0, 0.0, 0.0, 0.0]);
    let hits = db.vector_search(&query, 3)?;
    println!("vector hits = {}", hits.len());
    for h in &hits {
        println!("  score={:.4}  {:?}", h.score, h.node.content);
    }
    assert_eq!(hits.first().map(|h| h.node.id), Some(revenue_id));

    db.delete(ep_id)?;
    println!("deleted episode {ep_id}");
    Ok(())
}
