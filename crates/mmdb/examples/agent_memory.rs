//! Quickstart: a 5-line agent memory loop.
//!
//! Open a database with a text embedder → drop in strings → query in natural
//! language. No tenant id, no manual vector math.
//!
//! Run with: `cargo run -p mmdb --example agent_memory`
use mmdb::{Database, DatabaseConfig, Embedder, VectorFilter};
use mmdb_core::{NodeKind, Result};
use tempfile::tempdir;

struct HashEmbedder {
    dim: u32,
}
impl HashEmbedder {
    fn new(dim: u32) -> Self {
        Self { dim }
    }
}
impl Embedder for HashEmbedder {
    fn embed(&self, text: &str) -> Result<Vec<f32>> {
        let mut v = vec![0.0f32; self.dim as usize];
        for tok in text.split_whitespace() {
            let mut h: u32 = 0x811c9dc5;
            for b in tok.to_ascii_lowercase().as_bytes() {
                h ^= *b as u32;
                h = h.wrapping_mul(0x01000193);
            }
            v[(h as usize) % self.dim as usize] += 1.0;
        }
        let n: f32 = v.iter().map(|x| x * x).sum::<f32>().sqrt();
        if n > 0.0 {
            for x in v.iter_mut() {
                *x /= n;
            }
        }
        Ok(v)
    }
    fn model_name(&self) -> &str {
        "demo-hash-64"
    }
    fn dim(&self) -> u32 {
        self.dim
    }
}

fn main() -> anyhow::Result<()> {
    let dir = tempdir()?;
    let cfg = DatabaseConfig {
        tenant: 0,
        default_model: "demo-hash-64".into(),
    };
    let db = Database::open_with_embedder(dir.path(), cfg, Box::new(HashEmbedder::new(64)))?;

    // 1) Sprinkle in a few memories — Facts get vectors auto-attached.
    let _ep = db.insert_text(NodeKind::Episode, "User asked about quarterly revenue.")?;
    let revenue_q1 = db.insert_text(NodeKind::Fact, "Q1 2026 revenue 42.0M USD")?;
    let _ = db.insert_text(NodeKind::Fact, "CEO hometown Seattle")?;
    let _ = db.insert_text(NodeKind::Fact, "Q2 2026 revenue projection 45.0M USD")?;

    // 2) Time-range scan still works (no embedding required).
    let recent = db.scan_by_time(0, mmdb::now_ms() + 1, 50)?;
    println!("recent count = {}", recent.len());

    // 3) Natural-language vector search.
    println!("\ntop hits for 'quarterly revenue':");
    for h in db.search_text("quarterly revenue", 3)? {
        println!("  score={:.4}  {:?}", h.score, h.node.content);
    }

    // 4) Filtered search — only Fact-kind nodes.
    let q = db.search_text("quarterly revenue", 5)?;
    assert!(q.iter().any(|h| h.node.id == revenue_q1));
    let only_facts = {
        let embedder_dim = 64;
        let mut v = vec![0.0f32; embedder_dim];
        v[0] = 1.0;
        // use search_text path so we re-encode; then re-filter via facade api.
        db.vector_search_filtered(&v, 5, VectorFilter::new().kind(NodeKind::Fact))?
    };
    println!("\nfact-only filtered hits = {}", only_facts.len());

    Ok(())
}
