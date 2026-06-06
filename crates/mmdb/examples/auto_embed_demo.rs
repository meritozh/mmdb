//! End-to-end demo of the auto-embedding path.
//!
//! `Database::open_with_embedder` wires a text encoder so callers can stay at
//! the "give me a string back what's relevant" level — no manual vector math.
//!
//! Run with: `cargo run -p mmdb --example auto_embed_demo`
use mmdb::{Database, DatabaseConfig, Embedder};
use mmdb_core::{NodeKind, Result};
use tempfile::tempdir;

/// Tiny deterministic embedder for demos and tests.
/// Tokenizes on whitespace and FNV-1a-hashes each token into a fixed bucket.
struct HashEmbedder {
    dim: u32,
}
impl HashEmbedder {
    fn new(dim: u32) -> Self {
        Self { dim }
    }
    fn fnv1a(s: &str) -> u32 {
        let mut h: u32 = 0x811c9dc5;
        for b in s.as_bytes() {
            h ^= *b as u32;
            h = h.wrapping_mul(0x01000193);
        }
        h
    }
}
impl Embedder for HashEmbedder {
    fn embed(&self, text: &str) -> Result<Vec<f32>> {
        let mut v = vec![0.0f32; self.dim as usize];
        for tok in text.split_whitespace() {
            let h = Self::fnv1a(&tok.to_ascii_lowercase()) as usize;
            v[h % self.dim as usize] += 1.0;
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

    // Insert raw text — embedding is generated automatically.
    let texts = [
        "Q1 2026 revenue 42.0M USD",
        "CEO hometown Seattle",
        "Q2 2026 revenue projection 45.0M USD",
        "User asked about quarterly revenue figures",
        "Office snack budget approved",
    ];
    for t in &texts {
        db.insert_text(NodeKind::Fact, *t)?;
    }

    // Query by string too.
    let hits = db.search_text("quarterly revenue", 3)?;
    println!("top-{} for 'quarterly revenue':", hits.len());
    for h in &hits {
        println!("  score={:.4}  {:?}", h.score, h.node.content);
    }
    Ok(())
}
