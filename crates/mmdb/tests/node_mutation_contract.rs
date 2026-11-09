use mmdb::{
    core::{Content, NodeKind},
    Database, NodeBuilder,
};
use std::{
    io::Read,
    sync::{Arc, Barrier},
};

#[test]
fn concurrent_blob_refs_survive_gc_and_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let db = Arc::new(Database::open(dir.path()).unwrap());
    let gate = Arc::new(Barrier::new(12));
    let bytes = vec![0x57; 70_000];
    let ids = std::thread::scope(|s| {
        let threads: Vec<_> = (0..12)
            .map(|_| {
                let (db, gate, bytes) = (db.clone(), gate.clone(), &bytes);
                s.spawn(move || {
                    gate.wait();
                    db.insert_blob(
                        NodeKind::Artifact,
                        bytes.as_slice(),
                        "application/octet-stream",
                    )
                    .unwrap()
                })
            })
            .collect();
        threads
            .into_iter()
            .map(|t| t.join().unwrap())
            .collect::<Vec<_>>()
    });
    let Content::Blob { hash, .. } = db.get(ids[0]).unwrap().unwrap().content else {
        panic!()
    };
    assert_eq!(db.blob_refcount(&hash).unwrap(), Some(12));
    for id in &ids[1..] {
        db.delete(*id).unwrap();
        assert_eq!(db.gc_blobs().unwrap(), 0);
    }
    drop(db);
    let db = Database::open(dir.path()).unwrap();
    assert_eq!(db.blob_refcount(&hash).unwrap(), Some(1));
    assert_eq!(db.gc_blobs().unwrap(), 0);
    let mut found = Vec::new();
    db.get_blob_stream_for(&hash, ids[0])
        .unwrap()
        .read_to_end(&mut found)
        .unwrap();
    assert_eq!(found, bytes);
}

#[test]
fn interrupted_multi_index_write_recovers_all_models() {
    const KEY: &str = "MMDB_TEST_INTERRUPTED_INDEX_ROOT";
    if let Some(path) = std::env::var_os(KEY) {
        let db = Arc::new(Database::open(path).unwrap());
        let mut b = NodeBuilder::new(NodeKind::Fact).text("interrupted");
        for i in 0..128 {
            b = b.embedding(format!("model-{i}"), vec![1., 2., 3.]);
        }
        let node = b.build();
        let writer = db.clone();
        std::thread::spawn(move || writer.insert(node).unwrap());
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
        while std::time::Instant::now() < deadline {
            if !db
                .vector_search_with_model("model-0", &[1., 2., 3.], 1)
                .unwrap()
                .is_empty()
                && db
                    .vector_search_with_model("model-127", &[1., 2., 3.], 1)
                    .unwrap()
                    .is_empty()
            {
                std::process::exit(77);
            }
            std::thread::yield_now();
        }
        panic!("did not observe interrupted mutation window");
    }
    let dir = tempfile::tempdir().unwrap();
    let status = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "interrupted_multi_index_write_recovers_all_models",
        ])
        .env(KEY, dir.path())
        .status()
        .unwrap();
    assert_eq!(status.code(), Some(77));
    let db = Database::open(dir.path()).unwrap();
    let nodes = db.scan_by_time(0, i64::MAX, 2).unwrap();
    assert_eq!(nodes.len(), 1);
    assert_eq!(nodes[0].embeddings.len(), 128);
    for i in 0..128 {
        assert_eq!(
            db.vector_search_with_model(&format!("model-{i}"), &[1., 2., 3.], 1)
                .unwrap()
                .len(),
            1
        );
    }
}

#[test]
fn metadata_query_matches_one_prefix_of_ordered_commits() {
    let dir = tempfile::tempdir().unwrap();
    let db = Arc::new(Database::open(dir.path()).unwrap());
    let ids = (0..48)
        .map(|i| {
            db.insert(
                NodeBuilder::new(NodeKind::Fact)
                    .created_at(i)
                    .metadata("state", serde_json::json!("A"))
                    .build(),
            )
            .unwrap()
        })
        .collect::<Vec<_>>();
    let ready = Barrier::new(3);
    std::thread::scope(|s| {
        for _ in 0..2 {
            s.spawn(|| {
                ready.wait();
                for _ in 0..100 {
                    let rows = db
                        .nodes_by_metadata("state", &serde_json::json!("A"))
                        .unwrap();
                    assert!(rows.iter().all(|n| n.metadata["state"] == "A"));
                    assert!(ids.ends_with(&rows.into_iter().map(|n| n.id).collect::<Vec<_>>()));
                }
            });
        }
        ready.wait();
        for id in &ids {
            let mut n = db.get(*id).unwrap().unwrap();
            n.metadata.insert("state".into(), serde_json::json!("B"));
            db.insert(n).unwrap();
        }
    });
}
