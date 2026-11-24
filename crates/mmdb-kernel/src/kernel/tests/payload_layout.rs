use super::*;
use std::time::{Duration, Instant};

const CHUNKS: usize = 512;
const BATCH: usize = 16;
const GENERATIONS: usize = 3;
const INLINE: &[ModuleSpec] = &[ModuleSpec {
    id: MEMORY,
    partitions: &[PartitionSpec::Inline("entries")],
}];
const SEPARATED: &[ModuleSpec] = &[ModuleSpec {
    id: MEMORY,
    partitions: &[PartitionSpec::LargeValues("entries")],
}];

fn chunk_key(id: usize) -> [u8; 4] {
    (id as u32).to_be_bytes()
}

fn chunk(generation: usize, id: usize) -> Vec<u8> {
    // Match a stored payload chunk: its digest followed by the body.
    let length = match id % 8 {
        0 => 4096,
        1 => 8192 - 32,
        _ => 32768,
    };
    let mut encoded = vec![0; 32 + length];
    let mut seed = blake3::Hasher::new();
    seed.update(b"mmdb-fjall-layout-v1");
    seed.update(&chunk_key(generation));
    seed.update(&chunk_key(id));
    seed.finalize_xof().fill(&mut encoded[32..]);
    let digest = blake3::hash(&encoded[32..]);
    encoded[..32].copy_from_slice(digest.as_bytes());
    encoded
}

fn directory_bytes(path: &Path) -> u64 {
    fs::read_dir(path)
        .unwrap()
        .map(|entry| {
            let entry = entry.unwrap();
            let meta = entry.metadata().unwrap();
            if meta.is_dir() {
                directory_bytes(&entry.path())
            } else {
                meta.len()
            }
        })
        .sum()
}

fn p95(mut samples: Vec<Duration>) -> u128 {
    samples.sort_unstable();
    samples[(samples.len() * 95).div_ceil(100) - 1].as_micros()
}

fn page_p95(entries: &Partition, live: &[usize], expected: &[Vec<u8>]) -> u128 {
    // Step 17 permutes the 48 pages, covering every live chunk once per pass.
    let pages = live.chunks_exact(8);
    let count = pages.len();
    let mut samples = Vec::with_capacity(count);
    for index in 0..count {
        let offset = ((index * 17) % count) * 8;
        let start = Instant::now();
        for &id in &live[offset..offset + 8] {
            let value = entries.get(chunk_key(id)).unwrap().unwrap();
            assert_eq!(value.as_ref(), expected[id]);
        }
        samples.push(start.elapsed());
    }
    p95(samples)
}

fn measure(name: &str, schema: &'static [ModuleSpec], run: usize, generations: &[Vec<Vec<u8>>]) {
    let parent = tempfile::tempdir().unwrap();
    let root = parent.path().join("store");
    let kernel = Kernel::create(&root, &format(), schema).unwrap();
    let entries = kernel.module(MEMORY).partition("entries").unwrap();
    let mut writes = Vec::new();
    for (generation, values) in generations.iter().enumerate() {
        for first in (0..CHUNKS).step_by(BATCH) {
            let start = Instant::now();
            let mut batch = kernel.batch();
            for (offset, value) in values[first..first + BATCH].iter().enumerate() {
                batch.insert(&entries, chunk_key(first + offset), value);
            }
            batch.commit().unwrap();
            writes.push(start.elapsed());
        }
        let mut deletions = kernel.batch();
        for id in (generation..CHUNKS).step_by(4) {
            deletions.remove(&entries, chunk_key(id));
        }
        deletions.commit().unwrap();
        entries.raw.rotate_memtable_and_wait().unwrap();
        entries.raw.major_compact().unwrap();
    }
    assert_eq!(entries.raw.blob_file_count() > 0, name == "separated");
    drop((entries, kernel));
    let bytes = directory_bytes(&root);
    let kernel = Kernel::open(&root, &format(), schema).unwrap();
    let entries = kernel.module(MEMORY).partition("entries").unwrap();
    let live: Vec<_> = (0..CHUNKS).filter(|id| id % 4 != GENERATIONS - 1).collect();
    let expected = generations.last().unwrap();
    let cold = page_p95(&entries, &live, expected);
    let warm = page_p95(&entries, &live, expected);

    let start = Instant::now();
    for _ in 0..256 / BATCH {
        let mut batch = kernel.batch();
        let mut count = 0;
        for key in entries.prefix_keys([]).take(BATCH) {
            batch.remove(&entries, key.unwrap());
            count += 1;
        }
        assert_eq!(count, BATCH);
        batch.commit().unwrap();
    }
    let recovery = start.elapsed().as_micros();
    let remaining: Vec<_> = entries
        .prefix_keys([])
        .map(|key| key.unwrap().to_vec())
        .collect();
    assert_eq!(
        remaining,
        live[256..]
            .iter()
            .map(|id| chunk_key(*id))
            .collect::<Vec<_>>()
    );
    println!("run={run} layout={name} bytes={bytes} write_p95_us={} cold_page_p95_us={cold} warm_page_p95_us={warm} reclaim_us={recovery}", p95(writes));
}

#[test]
#[ignore = "opt-in release-mode storage layout measurement"]
fn compare_payload_layouts() {
    let values: Vec<Vec<_>> = (0..GENERATIONS)
        .map(|generation| (0..CHUNKS).map(|id| chunk(generation, id)).collect())
        .collect();
    for run in 1..=4 {
        let mut layouts = [("inline", INLINE), ("separated", SEPARATED)];
        if run % 2 == 0 {
            layouts.reverse();
        }
        for (name, schema) in layouts {
            measure(name, schema, run, &values);
        }
    }
}
