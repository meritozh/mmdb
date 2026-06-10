use super::keys::*;
use super::snapshot::manifest_path;
use super::{HitFilter, SnapshotManifest, VectorStore};
use fjall::Config;
use tempfile::tempdir;
use ulid::Ulid;

fn make_store_at(path: &std::path::Path) -> VectorStore {
    let ks = Config::new(path).open().unwrap();
    VectorStore::open(ks).unwrap()
}

fn norm(v: Vec<f32>) -> Vec<f32> {
    let n: f32 = v.iter().map(|x| x * x).sum::<f32>().sqrt();
    v.into_iter().map(|x| x / n).collect()
}

#[test]
fn insert_then_search_returns_inserted_id() {
    let dir = tempdir().unwrap();
    let s = make_store_at(dir.path());
    let id1 = Ulid::new();
    let id2 = Ulid::new();
    s.insert(0, "m", id1, &norm(vec![1.0, 0.0, 0.0, 0.0]))
        .unwrap();
    s.insert(0, "m", id2, &norm(vec![0.0, 1.0, 0.0, 0.0]))
        .unwrap();
    let hits = s
        .search(0, "m", &norm(vec![1.0, 0.0, 0.0, 0.0]), 1)
        .unwrap();
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].node_id, id1);
    assert!(hits[0].score > 0.99);
}

#[test]
fn delete_excludes_from_search() {
    let dir = tempdir().unwrap();
    let s = make_store_at(dir.path());
    let id = Ulid::new();
    s.insert(0, "m", id, &norm(vec![1.0, 0.0, 0.0])).unwrap();
    s.delete(0, "m", id).unwrap();
    let hits = s.search(0, "m", &norm(vec![1.0, 0.0, 0.0]), 5).unwrap();
    assert!(hits.iter().all(|h| h.node_id != id));
}

#[test]
fn dim_mismatch_is_rejected() {
    let dir = tempdir().unwrap();
    let s = make_store_at(dir.path());
    s.insert(0, "m", Ulid::new(), &[1.0, 0.0, 0.0]).unwrap();
    let err = s.insert(0, "m", Ulid::new(), &[1.0, 0.0]).unwrap_err();
    assert!(format!("{err}").contains("dim mismatch"));
}

#[test]
fn empty_index_search_returns_empty() {
    let dir = tempdir().unwrap();
    let s = make_store_at(dir.path());
    let hits = s.search(0, "absent", &[1.0, 0.0, 0.0], 5).unwrap();
    assert!(hits.is_empty());
}

#[test]
fn top_k_orders_by_similarity() {
    let dir = tempdir().unwrap();
    let s = make_store_at(dir.path());
    let near = Ulid::new();
    let far = Ulid::new();
    s.insert(0, "m", near, &norm(vec![1.0, 0.01, 0.0, 0.0]))
        .unwrap();
    s.insert(0, "m", far, &norm(vec![0.0, 0.0, 1.0, 0.0]))
        .unwrap();
    let hits = s
        .search(0, "m", &norm(vec![1.0, 0.0, 0.0, 0.0]), 2)
        .unwrap();
    assert_eq!(hits.len(), 2);
    assert_eq!(hits[0].node_id, near);
    assert!(hits[0].score > hits[1].score);
}

#[test]
fn rebuild_after_reopen() {
    let dir = tempdir().unwrap();
    let id1 = Ulid::new();
    let id2 = Ulid::new();
    {
        let s = make_store_at(dir.path());
        s.insert(0, "m", id1, &norm(vec![1.0, 0.0, 0.0, 0.0]))
            .unwrap();
        s.insert(0, "m", id2, &norm(vec![0.0, 1.0, 0.0, 0.0]))
            .unwrap();
    }
    // Reopen: HNSW graph must be rebuilt from meta.
    let s = make_store_at(dir.path());
    let hits = s
        .search(0, "m", &norm(vec![1.0, 0.0, 0.0, 0.0]), 1)
        .unwrap();
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].node_id, id1);
}

#[test]
fn open_reloads_checkpointed_hnsw_snapshot() {
    let dir = tempdir().unwrap();
    let id1 = Ulid::new();
    let id2 = Ulid::new();
    {
        let s = make_store_at(dir.path());
        s.insert(0, "m", id1, &norm(vec![1.0, 0.0, 0.0, 0.0]))
            .unwrap();
        s.insert(0, "m", id2, &norm(vec![0.0, 1.0, 0.0, 0.0]))
            .unwrap();
        s.flush_snapshots().unwrap();
    }

    let s = make_store_at(dir.path());

    assert_eq!(s.snapshot_reload_count(), 1);
    let hits = s
        .search(0, "m", &norm(vec![1.0, 0.0, 0.0, 0.0]), 1)
        .unwrap();
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].node_id, id1);
}

#[test]
fn open_falls_back_to_rebuild_when_hnsw_snapshot_is_corrupt() {
    let dir = tempdir().unwrap();
    let id = Ulid::new();
    let snapshot_dir = {
        let s = make_store_at(dir.path());
        s.insert(0, "m", id, &norm(vec![1.0, 0.0, 0.0, 0.0]))
            .unwrap();
        assert_eq!(s.flush_snapshots().unwrap(), 1);
        s.snapshot_dir.clone()
    };

    let mh = model_hash("m");
    let manifest: SnapshotManifest =
        serde_json::from_slice(&std::fs::read(manifest_path(&snapshot_dir, 0, mh)).unwrap())
            .unwrap();
    std::fs::write(
        snapshot_dir.join(format!("{}.hnsw.graph", manifest.basename)),
        b"not a valid graph",
    )
    .unwrap();

    let s = make_store_at(dir.path());

    assert_eq!(s.snapshot_reload_count(), 0);
    let hits = s
        .search(0, "m", &norm(vec![1.0, 0.0, 0.0, 0.0]), 1)
        .unwrap();
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].node_id, id);
}

#[test]
fn tombstones_survive_reopen() {
    let dir = tempdir().unwrap();
    let id = Ulid::new();
    {
        let s = make_store_at(dir.path());
        s.insert(0, "m", id, &norm(vec![1.0, 0.0, 0.0])).unwrap();
        s.delete(0, "m", id).unwrap();
    }
    let s = make_store_at(dir.path());
    let hits = s.search(0, "m", &norm(vec![1.0, 0.0, 0.0]), 5).unwrap();
    assert!(hits.iter().all(|h| h.node_id != id));
}

#[test]
fn insert_after_all_vectors_deleted_and_reopened_is_searchable() {
    let dir = tempdir().unwrap();
    let deleted = Ulid::new();
    {
        let s = make_store_at(dir.path());
        s.insert(0, "m", deleted, &norm(vec![1.0, 0.0, 0.0]))
            .unwrap();
        s.delete(0, "m", deleted).unwrap();
    }

    let s = make_store_at(dir.path());
    let fresh = Ulid::new();
    s.insert(0, "m", fresh, &norm(vec![1.0, 0.0, 0.0])).unwrap();

    let hits = s.search(0, "m", &norm(vec![1.0, 0.0, 0.0]), 1).unwrap();
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].node_id, fresh);
}

#[test]
fn insert_batch_works() {
    let dir = tempdir().unwrap();
    let s = make_store_at(dir.path());
    let items: Vec<_> = (0..50)
        .map(|i| {
            let mut v = vec![0.0_f32; 8];
            v[i % 8] = 1.0;
            (Ulid::new(), norm(v))
        })
        .collect();
    s.insert_batch(0, "m", &items).unwrap();
    let hits = s
        .search(
            0,
            "m",
            &norm(vec![1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0]),
            3,
        )
        .unwrap();
    assert!(!hits.is_empty());
}

#[test]
fn search_with_filter_keeps_only_matching() {
    let dir = tempdir().unwrap();
    let s = make_store_at(dir.path());
    let keep = Ulid::new();
    let drop = Ulid::new();
    s.insert(0, "m", keep, &norm(vec![1.0, 0.0, 0.0, 0.0]))
        .unwrap();
    s.insert(0, "m", drop, &norm(vec![0.99, 0.01, 0.0, 0.0]))
        .unwrap();
    let f: Box<HitFilter> = Box::new(move |u| u == keep);
    let hits = s
        .search_with_filter(0, "m", &norm(vec![1.0, 0.0, 0.0, 0.0]), 5, Some(&*f))
        .unwrap();
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].node_id, keep);
}
