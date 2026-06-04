//! Quick recall + latency bench for the HNSW VectorStore.
//!
//! 1k vectors of dim=384, gaussian-random + L2-normalised. For each of 50
//! random query vectors, compute ground-truth top-10 by brute force, then
//! query HNSW and measure recall@10 + p50/p99 query latency.
//!
//! Run with:
//!   cargo run --release -p mmdb-vector --example recall_bench

use fjall::Config;
use mmdb_vector::VectorStore;
use std::time::Instant;
use tempfile::tempdir;
use ulid::Ulid;

const N: usize = 1_000;
const DIM: usize = 384;
const QUERIES: usize = 50;
const K: usize = 10;

fn rand_vec(seed: &mut u64) -> Vec<f32> {
    // Tiny xorshift to avoid pulling in rand crate.
    let mut out = Vec::with_capacity(DIM);
    for _ in 0..DIM {
        *seed ^= *seed << 13;
        *seed ^= *seed >> 7;
        *seed ^= *seed << 17;
        // Map to roughly N(0,1) via low-quality Box-Muller-ish trick.
        let u = ((*seed >> 32) as u32) as f32 / u32::MAX as f32;
        out.push(u - 0.5);
    }
    // L2 normalise
    let n: f32 = out.iter().map(|x| x * x).sum::<f32>().sqrt();
    out.into_iter().map(|x| x / n).collect()
}

fn cosine_sim(a: &[f32], b: &[f32]) -> f32 {
    a.iter().zip(b.iter()).map(|(x, y)| x * y).sum()
}

fn main() {
    let dir = tempdir().unwrap();
    let ks = Config::new(dir.path()).open().unwrap();
    let store = VectorStore::open(ks).unwrap();

    let mut seed: u64 = 0xdead_beef_cafe_babe;
    // Build dataset
    let mut items: Vec<(Ulid, Vec<f32>)> = Vec::with_capacity(N);
    for _ in 0..N {
        items.push((Ulid::new(), rand_vec(&mut seed)));
    }

    // Bulk insert
    let t = Instant::now();
    store.insert_batch(0, "default", &items).unwrap();
    let insert_ms = t.elapsed().as_secs_f64() * 1e3;
    println!(
        "indexed {} vectors of dim {} in {:.1} ms  ({:.1} k vec/s)",
        N, DIM, insert_ms,
        (N as f64) / insert_ms
    );

    // Queries
    let mut queries: Vec<Vec<f32>> = Vec::with_capacity(QUERIES);
    for _ in 0..QUERIES {
        queries.push(rand_vec(&mut seed));
    }

    // Ground truth via brute force
    let mut gts: Vec<Vec<Ulid> > = Vec::with_capacity(QUERIES);
    for q in &queries {
        let mut sims: Vec<(Ulid, f32)> = items
            .iter()
            .map(|(id, v)| (*id, cosine_sim(q, v)))
            .collect();
        sims.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap());
        gts.push(sims.into_iter().take(K).map(|(id, _)| id).collect());
    }

    // HNSW queries
    let mut lats_us: Vec<f64> = Vec::with_capacity(QUERIES);
    let mut total_recall = 0.0_f64;
    for (q, gt) in queries.iter().zip(gts.iter()) {
        let t = Instant::now();
        let hits = store.search(0, "default", q, K).unwrap();
        lats_us.push(t.elapsed().as_secs_f64() * 1e6);
        let got: std::collections::HashSet<Ulid> =
            hits.iter().map(|h| h.node_id).collect();
        let intersect = gt.iter().filter(|id| got.contains(id)).count();
        total_recall += intersect as f64 / K as f64;
    }
    lats_us.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let p50 = lats_us[lats_us.len() / 2];
    let p99 = lats_us[(lats_us.len() as f64 * 0.99) as usize];
    let recall = total_recall / QUERIES as f64;

    println!("recall@{K} over {QUERIES} queries = {:.3}", recall);
    println!("query latency p50 = {:.1} us, p99 = {:.1} us", p50, p99);
}
