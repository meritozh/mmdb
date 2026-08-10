use crate::builder::{now_ms, NodeBuilder};
use crate::db::Database;
use crate::embedder::{DatabaseConfig, EmbedFuture, Embedder, DEFAULT_MODEL, DEFAULT_TENANT};
use crate::search::{Hit, HybridOpts, VectorFilter};
use mmdb_core::{Content, Edge, NodeKind, Result};
use mmdb_query::{
    AggregateExpr, FieldRef, Literal, LogicalPlan, ModelId, Predicate, SortKey, SourceExecutor,
    TableId, VectorRef,
};
use std::collections::BTreeMap;
use tempfile::tempdir;
use ulid::Ulid;

#[test]
fn insert_get_scan_delete_roundtrip() {
    let dir = tempdir().unwrap();
    let db = Database::open(dir.path()).unwrap();
    let node = NodeBuilder::new(NodeKind::Episode)
        .text("hello world")
        .metadata("source", serde_json::json!("test"))
        .created_at(1000)
        .build();
    let id = db.insert(node).unwrap();

    let got = db.get(id).unwrap().unwrap();
    assert!(matches!(got.content, Content::Text(ref s) if s == "hello world"));
    assert_eq!(got.tenant, DEFAULT_TENANT);

    let scanned = db.scan_by_time(0, 2000, 10).unwrap();
    assert_eq!(scanned.len(), 1);

    db.delete(id).unwrap();
    assert!(db.get(id).unwrap().is_none());
}

#[test]
fn metadata_lookup_is_exact_and_deterministically_ordered() {
    let dir = tempdir().unwrap();
    let db = Database::open(dir.path()).unwrap();
    let later = db
        .insert(
            NodeBuilder::new(NodeKind::Fact)
                .text("later")
                .metadata("memory_key", serde_json::json!("preference:tea"))
                .created_at(20)
                .build(),
        )
        .unwrap();
    let earlier = db
        .insert(
            NodeBuilder::new(NodeKind::Fact)
                .text("earlier")
                .metadata("memory_key", serde_json::json!("preference:tea"))
                .created_at(10)
                .build(),
        )
        .unwrap();
    db.insert(
        NodeBuilder::new(NodeKind::Fact)
            .text("other")
            .metadata("memory_key", serde_json::json!("preference:coffee"))
            .created_at(5)
            .build(),
    )
    .unwrap();

    let found = db
        .nodes_by_metadata("memory_key", &serde_json::json!("preference:tea"))
        .unwrap();
    assert_eq!(
        found.iter().map(|node| node.id).collect::<Vec<_>>(),
        vec![earlier, later]
    );
}

#[test]
fn access_stats_serialize_concurrent_increments_and_survive_reopen() {
    use std::sync::Arc;

    let dir = tempdir().unwrap();
    let id;
    {
        let db = Arc::new(Database::open(dir.path()).unwrap());
        id = db
            .insert(NodeBuilder::new(NodeKind::Fact).text("remember me").build())
            .unwrap();
        let threads = (0..8)
            .map(|_| {
                let db = Arc::clone(&db);
                std::thread::spawn(move || {
                    db.record_access([id, id], Default::default()).unwrap();
                })
            })
            .collect::<Vec<_>>();
        for thread in threads {
            thread.join().unwrap();
        }

        let stats = db.access_stats(id).unwrap().unwrap();
        assert_eq!(stats.node_id, id);
        assert_eq!(stats.access_count, 8);
        assert_eq!(db.get(id).unwrap().unwrap().revision, 1);
        assert_eq!(
            db.audit_records(Default::default())
                .unwrap()
                .iter()
                .filter(|record| record.name == "record_access")
                .count(),
            8
        );
    }

    let db = Database::open(dir.path()).unwrap();
    assert_eq!(db.access_stats(id).unwrap().unwrap().access_count, 8);
}

#[test]
fn access_stats_merge_is_additive_idempotent_and_survives_reopen() {
    let dir = tempdir().unwrap();
    let target;
    let source;
    {
        let db = Database::open(dir.path()).unwrap();
        target = db
            .insert(NodeBuilder::new(NodeKind::Fact).text("canonical").build())
            .unwrap();
        source = db
            .insert(NodeBuilder::new(NodeKind::Fact).text("duplicate").build())
            .unwrap();
        for _ in 0..3 {
            db.record_access([target], Default::default()).unwrap();
        }
        for _ in 0..2 {
            db.record_access([source], Default::default()).unwrap();
        }

        let merged = db
            .merge_access_stats(target, [source], Default::default())
            .unwrap()
            .unwrap();
        assert_eq!(merged.access_count, 5);
        assert!(db.access_stats(source).unwrap().is_none());
        assert_eq!(
            db.merge_access_stats(target, [source], Default::default())
                .unwrap()
                .unwrap()
                .access_count,
            5
        );
        db.record_access([target], Default::default()).unwrap();
        assert_eq!(db.access_stats(target).unwrap().unwrap().access_count, 6);
    }

    let db = Database::open(dir.path()).unwrap();
    assert_eq!(db.access_stats(target).unwrap().unwrap().access_count, 6);
    assert!(db.access_stats(source).unwrap().is_none());
}

#[test]
fn access_timestamp_is_monotonic_and_hard_delete_clears_stats() {
    let dir = tempdir().unwrap();
    let db = Database::open(dir.path()).unwrap();
    let id = db
        .insert(
            NodeBuilder::new(NodeKind::Fact)
                .text("accessed fact")
                .build(),
        )
        .unwrap();
    let future = now_ms() + 60_000;

    db.access_store
        .record(DEFAULT_TENANT, &[id], future)
        .unwrap();
    db.record_access([id], Default::default()).unwrap();
    let stats = db.access_stats(id).unwrap().unwrap();
    assert_eq!(stats.access_count, 2);
    assert_eq!(stats.last_accessed_at_ms, future);

    db.delete(id).unwrap();
    assert!(db.access_stats(id).unwrap().is_none());
}

#[test]
fn record_access_and_delete_do_not_leave_orphan_stats() {
    use std::sync::{Arc, Barrier};

    let dir = tempdir().unwrap();
    let db = Arc::new(Database::open(dir.path()).unwrap());
    for index in 0..8 {
        let id = db
            .insert(
                NodeBuilder::new(NodeKind::Fact)
                    .text(format!("race {index}"))
                    .build(),
            )
            .unwrap();
        let barrier = Arc::new(Barrier::new(3));
        let access = {
            let db = Arc::clone(&db);
            let barrier = Arc::clone(&barrier);
            std::thread::spawn(move || {
                barrier.wait();
                db.record_access([id], Default::default())
            })
        };
        let deletion = {
            let db = Arc::clone(&db);
            let barrier = Arc::clone(&barrier);
            std::thread::spawn(move || {
                barrier.wait();
                db.delete(id)
            })
        };
        barrier.wait();
        let _ = access.join().unwrap();
        deletion.join().unwrap().unwrap();

        assert!(db.get(id).unwrap().is_none());
        assert!(db.access_stats(id).unwrap().is_none());
    }
}

#[test]
fn concurrent_retract_and_insert_are_serializable() {
    use std::sync::{Arc, Barrier};

    let dir = tempdir().unwrap();
    let db = Arc::new(Database::open(dir.path()).unwrap());
    for index in 0..8 {
        let id = db
            .insert(
                NodeBuilder::new(NodeKind::Fact)
                    .text(format!("before {index}"))
                    .build(),
            )
            .unwrap();
        let original = db.get(id).unwrap().unwrap();
        let mut replacement = original.clone();
        replacement.content = Content::Text(format!("after {index}"));
        let barrier = Arc::new(Barrier::new(3));
        let retraction = {
            let db = Arc::clone(&db);
            let barrier = Arc::clone(&barrier);
            std::thread::spawn(move || {
                barrier.wait();
                db.retract(
                    id,
                    Some(original.revision),
                    "concurrent update",
                    Default::default(),
                )
            })
        };
        let insertion = {
            let db = Arc::clone(&db);
            let barrier = Arc::clone(&barrier);
            std::thread::spawn(move || {
                barrier.wait();
                db.insert(replacement)
            })
        };
        barrier.wait();
        let retract_result = retraction.join().unwrap();
        insertion.join().unwrap().unwrap();

        let final_node = db.get(id).unwrap().unwrap();
        assert!(matches!(
            &final_node.content,
            Content::Text(text) if text == &format!("after {index}")
        ));
        assert_eq!(final_node.state, mmdb_core::MemoryState::Active);
        if let Err(error) = retract_result {
            assert!(error.to_string().contains("stale node revision"));
        }
    }
}

#[test]
fn concurrent_retract_and_delete_never_resurrect_a_node() {
    use std::sync::{Arc, Barrier};

    let dir = tempdir().unwrap();
    let db = Arc::new(Database::open(dir.path()).unwrap());
    for index in 0..8 {
        let id = db
            .insert(
                NodeBuilder::new(NodeKind::Fact)
                    .text(format!("delete race {index}"))
                    .build(),
            )
            .unwrap();
        let revision = db.get(id).unwrap().unwrap().revision;
        let barrier = Arc::new(Barrier::new(3));
        let retraction = {
            let db = Arc::clone(&db);
            let barrier = Arc::clone(&barrier);
            std::thread::spawn(move || {
                barrier.wait();
                db.retract(id, Some(revision), "concurrent delete", Default::default())
            })
        };
        let deletion = {
            let db = Arc::clone(&db);
            let barrier = Arc::clone(&barrier);
            std::thread::spawn(move || {
                barrier.wait();
                db.delete(id)
            })
        };
        barrier.wait();
        let _ = retraction.join().unwrap();
        deletion.join().unwrap().unwrap();

        assert!(db.get(id).unwrap().is_none());
    }
}

#[test]
fn open_with_custom_model_persists_config() {
    let dir = tempdir().unwrap();
    let cfg = DatabaseConfig {
        tenant: DEFAULT_TENANT,
        default_model: "bge-m3".to_string(),
    };
    let db = Database::open_with(dir.path(), cfg).unwrap();
    assert_eq!(db.config().default_model, "bge-m3");

    // No nodes inserted -> empty result
    let hits = db.vector_search(&[0.1, 0.2, 0.3], 5).unwrap();
    assert!(hits.is_empty());
}

fn norm(v: Vec<f32>) -> Vec<f32> {
    let n: f32 = v.iter().map(|x| x * x).sum::<f32>().sqrt();
    v.into_iter().map(|x| x / n).collect()
}

#[test]
fn vector_search_returns_inserted_nodes_ranked() {
    let dir = tempdir().unwrap();
    let db = Database::open(dir.path()).unwrap();
    let mk = |v: Vec<f32>, label: &str| {
        NodeBuilder::new(NodeKind::Fact)
            .text(label)
            .embedding(DEFAULT_MODEL, norm(v))
            .build()
    };
    let n1 = mk(vec![1.0, 0.0, 0.0, 0.0], "axis-x");
    let n2 = mk(vec![0.0, 1.0, 0.0, 0.0], "axis-y");
    let n3 = mk(vec![0.95, 0.05, 0.0, 0.0], "near-x");
    let id1 = db.insert(n1).unwrap();
    let _id2 = db.insert(n2).unwrap();
    let id3 = db.insert(n3).unwrap();

    let q = norm(vec![1.0, 0.0, 0.0, 0.0]);
    let hits = db.vector_search(&q, 2).unwrap();
    assert_eq!(
        hits.len(),
        2,
        "got {:?}",
        hits.iter().map(|h| &h.node.id).collect::<Vec<_>>()
    );
    assert_eq!(hits[0].node.id, id1);
    assert_eq!(hits[1].node.id, id3);
    assert!(hits[0].score >= hits[1].score);
}

#[test]
fn vector_search_filtered_by_kind_and_time() {
    let dir = tempdir().unwrap();
    let db = Database::open(dir.path()).unwrap();
    let v = norm(vec![1.0, 0.0, 0.0, 0.0]);
    let fact_id = db
        .insert(
            NodeBuilder::new(NodeKind::Fact)
                .text("fact")
                .created_at(1_000)
                .embedding(DEFAULT_MODEL, v.clone())
                .build(),
        )
        .unwrap();
    let ep_id = db
        .insert(
            NodeBuilder::new(NodeKind::Episode)
                .text("episode")
                .created_at(2_000)
                .embedding(DEFAULT_MODEL, v.clone())
                .build(),
        )
        .unwrap();
    // kind filter — only Fact survives
    let hits = db
        .vector_search_filtered(&v, 5, VectorFilter::new().kind(NodeKind::Fact))
        .unwrap();
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].node.id, fact_id);
    // time-window — only Episode survives
    let hits = db
        .vector_search_filtered(&v, 5, VectorFilter::new().after_ms(1_500))
        .unwrap();
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].node.id, ep_id);
    // both — empty
    let hits = db
        .vector_search_filtered(
            &v,
            5,
            VectorFilter::new().kind(NodeKind::Fact).after_ms(1_500),
        )
        .unwrap();
    assert!(hits.is_empty());
}

#[test]
fn vector_search_filtered_by_metadata_value() {
    let dir = tempdir().unwrap();
    let db = Database::open(dir.path()).unwrap();
    let v = norm(vec![1.0, 0.0, 0.0, 0.0]);

    let keep = db
        .insert(
            NodeBuilder::new(NodeKind::Fact)
                .text("keep")
                .metadata("project", serde_json::json!("alpha"))
                .embedding(DEFAULT_MODEL, v.clone())
                .build(),
        )
        .unwrap();
    db.insert(
        NodeBuilder::new(NodeKind::Fact)
            .text("drop")
            .metadata("project", serde_json::json!("beta"))
            .embedding(DEFAULT_MODEL, v.clone())
            .build(),
    )
    .unwrap();

    let hits = db
        .vector_search_filtered(
            &v,
            5,
            VectorFilter::new().metadata_eq("project", serde_json::json!("alpha")),
        )
        .unwrap();
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].node.id, keep);

    let mut updated = db.get(keep).unwrap().unwrap();
    updated
        .metadata
        .insert("project".into(), serde_json::json!("gamma"));
    db.insert(updated).unwrap();
    let hits = db
        .vector_search_filtered(
            &v,
            5,
            VectorFilter::new().metadata_eq("project", serde_json::json!("alpha")),
        )
        .unwrap();
    assert!(hits.is_empty());
    let hits = db
        .vector_search_filtered(
            &v,
            5,
            VectorFilter::new().metadata_eq("project", serde_json::json!("gamma")),
        )
        .unwrap();
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].node.id, keep);

    db.delete(keep).unwrap();
    let hits = db
        .vector_search_filtered(
            &v,
            5,
            VectorFilter::new().metadata_eq("project", serde_json::json!("gamma")),
        )
        .unwrap();
    assert!(hits.is_empty());
}

#[test]
fn execute_query_scans_persisted_nodes_with_filter() {
    let dir = tempdir().unwrap();
    let db = Database::open(dir.path()).unwrap();
    let keep = db
        .insert(
            NodeBuilder::new(NodeKind::Fact)
                .text("alpha fact")
                .created_at(1_000)
                .metadata("project", serde_json::json!("alpha"))
                .build(),
        )
        .unwrap();
    db.insert(
        NodeBuilder::new(NodeKind::Episode)
            .text("alpha episode")
            .created_at(1_100)
            .metadata("project", serde_json::json!("alpha"))
            .build(),
    )
    .unwrap();
    db.insert(
        NodeBuilder::new(NodeKind::Fact)
            .text("beta fact")
            .created_at(1_200)
            .metadata("project", serde_json::json!("beta"))
            .build(),
    )
    .unwrap();

    let rows = db
        .execute_query(&LogicalPlan::Scan {
            table: TableId::Nodes,
            filter: Some(Predicate::and([
                Predicate::kind_eq(NodeKind::Fact),
                Predicate::eq(
                    FieldRef::Metadata("project".to_string()),
                    Literal::String("alpha".to_string()),
                ),
            ])),
        })
        .unwrap();

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].node_id, keep.to_string());
    assert_eq!(
        rows[0].fields.get("project"),
        Some(&Literal::String("alpha".to_string()))
    );
}

#[test]
fn execute_query_projects_content_and_metadata_fields() {
    let dir = tempdir().unwrap();
    let db = Database::open(dir.path()).unwrap();
    let id = db
        .insert(
            NodeBuilder::new(NodeKind::Fact)
                .text("project me")
                .metadata("project", serde_json::json!("alpha"))
                .build(),
        )
        .unwrap();

    let rows = db
        .execute_query(&LogicalPlan::Project {
            input: Box::new(LogicalPlan::Scan {
                table: TableId::Nodes,
                filter: Some(Predicate::kind_eq(NodeKind::Fact)),
            }),
            fields: vec![
                FieldRef::NodeId,
                FieldRef::Content,
                FieldRef::Metadata("project".to_string()),
                FieldRef::Score,
            ],
        })
        .unwrap();

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].node_id, id.to_string());
    assert_eq!(
        rows[0].fields,
        BTreeMap::from([
            ("node_id".to_string(), Literal::String(id.to_string())),
            (
                "content".to_string(),
                Literal::String("project me".to_string())
            ),
            ("project".to_string(), Literal::String("alpha".to_string())),
            (
                "score".to_string(),
                Literal::F32(mmdb_query::OrderedF32(0.0))
            ),
        ])
    );
}

#[test]
fn execute_query_projects_vector_score_field() {
    let dir = tempdir().unwrap();
    let db = Database::open(dir.path()).unwrap();
    let q = norm(vec![1.0, 0.0, 0.0]);
    let id = db
        .insert(
            NodeBuilder::new(NodeKind::Fact)
                .text("scored")
                .embedding(DEFAULT_MODEL, q.clone())
                .build(),
        )
        .unwrap();

    let rows = db
        .execute_query(&LogicalPlan::Project {
            input: Box::new(LogicalPlan::VectorSearch {
                query: VectorRef::Vector(q),
                k: 1,
                filter: None,
                model: ModelId::from(DEFAULT_MODEL),
            }),
            fields: vec![FieldRef::NodeId, FieldRef::Score],
        })
        .unwrap();

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].node_id, id.to_string());
    let Some(Literal::F32(score)) = rows[0].fields.get("score") else {
        panic!("expected projected score");
    };
    assert!(score.0 > 0.99);
}

#[test]
fn execute_query_filters_updated_at_field() {
    let dir = tempdir().unwrap();
    let db = Database::open(dir.path()).unwrap();
    let mut old = NodeBuilder::new(NodeKind::Fact)
        .text("old")
        .created_at(100)
        .build();
    old.updated_at_ms = 200;
    db.insert(old).unwrap();
    let mut fresh = NodeBuilder::new(NodeKind::Fact)
        .text("fresh")
        .created_at(100)
        .build();
    fresh.updated_at_ms = 900;
    let fresh_id = fresh.id;
    db.insert(fresh).unwrap();

    let rows = db
        .execute_query(&LogicalPlan::Scan {
            table: TableId::Nodes,
            filter: Some(Predicate::Gte(FieldRef::UpdatedAtMs, Literal::I64(800))),
        })
        .unwrap();

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].node_id, fresh_id.to_string());
}

#[test]
fn execute_query_uses_vector_and_graph_stores() {
    let dir = tempdir().unwrap();
    let db = Database::open(dir.path()).unwrap();
    let q = norm(vec![1.0, 0.0, 0.0, 0.0]);
    let seed = db
        .insert(
            NodeBuilder::new(NodeKind::Fact)
                .text("seed")
                .created_at(1_000)
                .embedding(DEFAULT_MODEL, q.clone())
                .build(),
        )
        .unwrap();
    let related = db
        .insert(
            NodeBuilder::new(NodeKind::Fact)
                .text("related")
                .created_at(1_100)
                .build(),
        )
        .unwrap();
    db.insert(
        NodeBuilder::new(NodeKind::Fact)
            .text("far")
            .created_at(1_200)
            .embedding(DEFAULT_MODEL, norm(vec![0.0, 1.0, 0.0, 0.0]))
            .build(),
    )
    .unwrap();
    db.add_edge(Edge {
        src: seed,
        dst: related,
        label: "related".to_string(),
        weight: 1.0,
        created_at_ms: 1_300,
        metadata: BTreeMap::new(),
        revision: 1,
        valid_from_ms: None,
        valid_to_ms: None,
        evidence: Vec::new(),
    })
    .unwrap();

    let rows = db
        .execute_query(&LogicalPlan::TopK {
            input: Box::new(LogicalPlan::GraphExpand {
                from: Box::new(LogicalPlan::VectorSearch {
                    query: VectorRef::Vector(q),
                    k: 1,
                    filter: None,
                    model: ModelId::from(DEFAULT_MODEL),
                }),
                relation: Some("related".to_string()),
                depth: 1,
            }),
            k: 2,
            by: SortKey::ScoreDesc,
        })
        .unwrap();

    let ids = rows
        .iter()
        .map(|row| row.node_id.as_str())
        .collect::<Vec<_>>();
    assert!(ids.contains(&seed.to_string().as_str()));
    assert!(ids.contains(&related.to_string().as_str()));
}

#[test]
fn execute_query_embeds_text_vector_ref_with_configured_embedder() {
    let dir = tempdir().unwrap();
    let cfg = DatabaseConfig {
        tenant: DEFAULT_TENANT,
        default_model: "hash-32".into(),
    };
    let db =
        Database::open_with_embedder(dir.path(), cfg, Box::new(HashEmbedder::new("hash-32", 32)))
            .unwrap();
    let keep = db
        .insert_text(NodeKind::Fact, "quarterly revenue memo")
        .unwrap();
    db.insert_text(NodeKind::Fact, "garden planning note")
        .unwrap();

    let rows = db
        .execute_query(&LogicalPlan::VectorSearch {
            query: VectorRef::Text("quarterly revenue".to_string()),
            k: 1,
            filter: Some(Predicate::kind_eq(NodeKind::Fact)),
            model: ModelId::from("hash-32"),
        })
        .unwrap();

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].node_id, keep.to_string());
}

#[test]
fn source_executor_runs_against_database_stores() {
    let dir = tempdir().unwrap();
    let db = Database::open(dir.path()).unwrap();
    let q = norm(vec![1.0, 0.0, 0.0, 0.0]);
    let seed = db
        .insert(
            NodeBuilder::new(NodeKind::Fact)
                .text("seed")
                .created_at(1_000)
                .embedding(DEFAULT_MODEL, q.clone())
                .build(),
        )
        .unwrap();
    let related = db
        .insert(
            NodeBuilder::new(NodeKind::Episode)
                .text("related")
                .created_at(1_100)
                .build(),
        )
        .unwrap();
    db.add_edge(Edge {
        src: seed,
        dst: related,
        label: "related".to_string(),
        weight: 1.0,
        created_at_ms: 1_200,
        metadata: BTreeMap::new(),
        revision: 1,
        valid_from_ms: None,
        valid_to_ms: None,
        evidence: Vec::new(),
    })
    .unwrap();

    let plan = LogicalPlan::TopK {
        input: Box::new(LogicalPlan::GraphExpand {
            from: Box::new(LogicalPlan::VectorSearch {
                query: VectorRef::Vector(q),
                k: 1,
                filter: None,
                model: ModelId::from(DEFAULT_MODEL),
            }),
            relation: Some("related".to_string()),
            depth: 1,
        }),
        k: 2,
        by: SortKey::ScoreDesc,
    };

    let mut op = SourceExecutor::new(&db).compile(&plan, 1).unwrap();
    let mut rows = Vec::new();
    while let Some(batch) = op.next_batch().unwrap() {
        rows.extend(batch.rows);
    }

    let ids = rows
        .iter()
        .map(|row| row.node_id.as_str())
        .collect::<Vec<_>>();
    assert!(ids.contains(&seed.to_string().as_str()));
    assert!(ids.contains(&related.to_string().as_str()));

    let explain = SourceExecutor::new(&db)
        .explain(&plan, &db.query_optimizer_stats(), 2)
        .unwrap();
    assert_eq!(explain.operator, "TopKOp");
    assert_eq!(explain.actual_rows, Some(2));
    assert_eq!(explain.children[0].operator, "GraphExpandOp");
    assert_eq!(explain.children[0].actual_rows, Some(2));
    assert_eq!(explain.children[0].children[0].operator, "HnswSearchOp");
    assert_eq!(explain.children[0].children[0].actual_rows, Some(1));
}

#[test]
fn execute_query_physical_matches_facade_for_udf_free_plan() {
    let dir = tempdir().unwrap();
    let db = Database::open(dir.path()).unwrap();
    let q = norm(vec![1.0, 0.0, 0.0, 0.0]);
    let seed = db
        .insert(
            NodeBuilder::new(NodeKind::Fact)
                .text("seed")
                .created_at(1_000)
                .embedding(DEFAULT_MODEL, q.clone())
                .build(),
        )
        .unwrap();
    let related = db
        .insert(
            NodeBuilder::new(NodeKind::Fact)
                .text("related")
                .created_at(1_100)
                .build(),
        )
        .unwrap();
    db.add_edge(Edge {
        src: seed,
        dst: related,
        label: "related".to_string(),
        weight: 1.0,
        created_at_ms: 1_200,
        metadata: BTreeMap::new(),
        revision: 1,
        valid_from_ms: None,
        valid_to_ms: None,
        evidence: Vec::new(),
    })
    .unwrap();
    let plan = LogicalPlan::TopK {
        input: Box::new(LogicalPlan::GraphExpand {
            from: Box::new(LogicalPlan::VectorSearch {
                query: VectorRef::Vector(q),
                k: 1,
                filter: None,
                model: ModelId::from(DEFAULT_MODEL),
            }),
            relation: Some("related".to_string()),
            depth: 1,
        }),
        k: 2,
        by: SortKey::ScoreDesc,
    };

    let recursive_rows = db.execute_query(&plan).unwrap();
    let physical_rows = db.execute_query_physical(&plan).unwrap();

    assert_eq!(physical_rows, recursive_rows);
}

#[test]
fn execute_query_counts_rows_grouped_by_kind() {
    let dir = tempdir().unwrap();
    let db = Database::open(dir.path()).unwrap();
    db.insert(NodeBuilder::new(NodeKind::Fact).text("fact one").build())
        .unwrap();
    db.insert(NodeBuilder::new(NodeKind::Fact).text("fact two").build())
        .unwrap();
    db.insert(NodeBuilder::new(NodeKind::Episode).text("episode").build())
        .unwrap();

    let rows = db
        .execute_query(&LogicalPlan::Aggregate {
            input: Box::new(LogicalPlan::Scan {
                table: TableId::Nodes,
                filter: None,
            }),
            group_by: vec![FieldRef::Kind],
            aggregate: AggregateExpr::Count,
        })
        .unwrap();

    assert_eq!(
        rows.iter()
            .find(|row| row.fields.get("kind") == Some(&Literal::NodeKind(NodeKind::Fact)))
            .and_then(|row| row.fields.get("count")),
        Some(&Literal::I64(2))
    );
    assert_eq!(
        rows.iter()
            .find(|row| row.fields.get("kind") == Some(&Literal::NodeKind(NodeKind::Episode)))
            .and_then(|row| row.fields.get("count")),
        Some(&Literal::I64(1))
    );
}

#[test]
fn execute_query_applies_registered_udf_score() {
    let dir = tempdir().unwrap();
    let db = Database::open(dir.path()).unwrap();
    db.insert(NodeBuilder::new(NodeKind::Fact).text("low").build())
        .unwrap();
    let boosted = db
        .insert(NodeBuilder::new(NodeKind::Episode).text("boosted").build())
        .unwrap();
    db.register_query_udf("boost_episode", |record, _args| {
        if record.kind == NodeKind::Episode {
            10.0
        } else {
            1.0
        }
    });

    let rows = db
        .execute_query(&LogicalPlan::TopK {
            input: Box::new(LogicalPlan::Udf {
                input: Box::new(LogicalPlan::Scan {
                    table: TableId::Nodes,
                    filter: None,
                }),
                name: "boost_episode".to_string(),
                args: vec![],
            }),
            k: 1,
            by: SortKey::ScoreDesc,
        })
        .unwrap();

    assert_eq!(rows[0].node_id, boosted.to_string());
    assert_eq!(rows[0].score, 10.0);
}

#[test]
fn execute_query_physical_applies_registered_udf_score() {
    let dir = tempdir().unwrap();
    let db = Database::open(dir.path()).unwrap();
    db.insert(NodeBuilder::new(NodeKind::Fact).text("low").build())
        .unwrap();
    let boosted = db
        .insert(NodeBuilder::new(NodeKind::Episode).text("boosted").build())
        .unwrap();
    db.register_query_udf("boost_episode", |record, _args| {
        if record.kind == NodeKind::Episode {
            10.0
        } else {
            1.0
        }
    });

    let rows = db
        .execute_query_physical(&LogicalPlan::TopK {
            input: Box::new(LogicalPlan::Udf {
                input: Box::new(LogicalPlan::Scan {
                    table: TableId::Nodes,
                    filter: None,
                }),
                name: "boost_episode".to_string(),
                args: vec![],
            }),
            k: 1,
            by: SortKey::ScoreDesc,
        })
        .unwrap();

    assert_eq!(rows[0].node_id, boosted.to_string());
    assert_eq!(rows[0].score, 10.0);
}

#[test]
fn execute_query_async_matches_sync_facade() {
    let dir = tempdir().unwrap();
    let db = Database::open(dir.path()).unwrap();
    db.insert(
        NodeBuilder::new(NodeKind::Fact)
            .text("async query")
            .created_at(1_000)
            .build(),
    )
    .unwrap();
    let plan = LogicalPlan::Scan {
        table: TableId::Nodes,
        filter: Some(Predicate::kind_eq(NodeKind::Fact)),
    };

    let sync_rows = db.execute_query(&plan).unwrap();
    let async_rows = block_on(db.execute_query_async(&plan)).unwrap();

    assert_eq!(async_rows, sync_rows);
}

#[test]
fn execute_query_async_returns_pending_before_worker_finishes() {
    let dir = tempdir().unwrap();
    let db = Database::open(dir.path()).unwrap();
    db.insert(NodeBuilder::new(NodeKind::Fact).text("async yield").build())
        .unwrap();
    let plan = LogicalPlan::Scan {
        table: TableId::Nodes,
        filter: Some(Predicate::kind_eq(NodeKind::Fact)),
    };

    let waker = noop_waker();
    let mut cx = std::task::Context::from_waker(&waker);
    let mut future = Box::pin(db.execute_query_async(&plan));

    assert!(matches!(
        std::future::Future::poll(future.as_mut(), &mut cx),
        std::task::Poll::Pending
    ));
    let started = std::time::Instant::now();
    loop {
        match std::future::Future::poll(future.as_mut(), &mut cx) {
            std::task::Poll::Ready(Ok(rows)) => {
                assert_eq!(rows.len(), 1);
                break;
            }
            std::task::Poll::Ready(Err(err)) => panic!("async query failed: {err}"),
            std::task::Poll::Pending => {
                assert!(
                    started.elapsed() < std::time::Duration::from_secs(2),
                    "async query worker did not finish"
                );
                std::thread::yield_now();
            }
        }
    }
}

#[test]
fn execute_query_async_does_not_block_polling_thread_on_sync_work() {
    let dir = tempdir().unwrap();
    let db = Database::open(dir.path()).unwrap();
    db.insert(
        NodeBuilder::new(NodeKind::Fact)
            .text("async offload")
            .build(),
    )
    .unwrap();
    db.register_query_udf("slow_boost", |record, _args| {
        std::thread::sleep(std::time::Duration::from_millis(200));
        record.score + 1.0
    });
    let plan = LogicalPlan::Udf {
        input: Box::new(LogicalPlan::Scan {
            table: TableId::Nodes,
            filter: Some(Predicate::kind_eq(NodeKind::Fact)),
        }),
        name: "slow_boost".to_string(),
        args: Vec::new(),
    };

    let waker = noop_waker();
    let mut cx = std::task::Context::from_waker(&waker);
    let mut future = Box::pin(db.execute_query_async(&plan));

    assert!(matches!(
        std::future::Future::poll(future.as_mut(), &mut cx),
        std::task::Poll::Pending
    ));
    let started = std::time::Instant::now();
    let second_poll = std::future::Future::poll(future.as_mut(), &mut cx);

    assert!(
        started.elapsed() < std::time::Duration::from_millis(50),
        "polling thread was blocked by synchronous query work"
    );
    assert!(matches!(second_poll, std::task::Poll::Pending));

    std::thread::sleep(std::time::Duration::from_millis(250));
    let ready = std::future::Future::poll(future.as_mut(), &mut cx);
    assert!(matches!(ready, std::task::Poll::Ready(Ok(_))));
}

#[test]
fn query_optimizer_stats_are_rebuilt_from_persisted_nodes() {
    let dir = tempdir().unwrap();
    {
        let db = Database::open(dir.path()).unwrap();
        db.insert(NodeBuilder::new(NodeKind::Fact).text("fact one").build())
            .unwrap();
        db.insert(NodeBuilder::new(NodeKind::Fact).text("fact two").build())
            .unwrap();
        db.insert(
            NodeBuilder::new(NodeKind::Episode)
                .text("episode one")
                .build(),
        )
        .unwrap();
    }

    let db = Database::open(dir.path()).unwrap();
    let stats = db.query_optimizer_stats();
    let kind_histogram = stats.histograms.get(&FieldRef::Kind).unwrap();

    assert_eq!(stats.node_rows, 3);
    assert_eq!(kind_histogram.total_count(), 3);
    assert_eq!(kind_histogram.count(&Literal::NodeKind(NodeKind::Fact)), 2);
    assert_eq!(
        kind_histogram.count(&Literal::NodeKind(NodeKind::Episode)),
        1
    );
    assert_eq!(
        stats.estimate_selectivity(&Predicate::kind_eq(NodeKind::Fact)),
        2.0 / 3.0
    );
}

#[test]
fn delete_removes_from_vector_search() {
    let dir = tempdir().unwrap();
    let db = Database::open(dir.path()).unwrap();
    let node = NodeBuilder::new(NodeKind::Fact)
        .text("x")
        .embedding(DEFAULT_MODEL, norm(vec![1.0, 0.0, 0.0]))
        .build();
    let id = db.insert(node).unwrap();
    let q = norm(vec![1.0, 0.0, 0.0]);
    assert_eq!(db.vector_search(&q, 5).unwrap().len(), 1);
    db.delete(id).unwrap();
    assert_eq!(db.vector_search(&q, 5).unwrap().len(), 0);
}

#[test]
fn insert_forces_tenant_from_config() {
    let dir = tempdir().unwrap();
    let db = Database::open(dir.path()).unwrap();
    let mut node = NodeBuilder::new(NodeKind::Fact).text("x").build();
    // Even if a caller tampers with tenant pre-insert, Database normalizes it.
    node.tenant = 999;
    let id = db.insert(node).unwrap();
    let got = db.get(id).unwrap().unwrap();
    assert_eq!(got.tenant, DEFAULT_TENANT);
}

#[test]
fn insert_rejects_vector_dim_mismatch_without_persisting_node() {
    let dir = tempdir().unwrap();
    let db = Database::open(dir.path()).unwrap();
    let seed = db
        .insert(
            NodeBuilder::new(NodeKind::Fact)
                .text("seed")
                .embedding(DEFAULT_MODEL, norm(vec![1.0, 0.0, 0.0]))
                .build(),
        )
        .unwrap();
    let bad = NodeBuilder::new(NodeKind::Fact)
        .text("bad")
        .embedding(DEFAULT_MODEL, vec![1.0, 0.0])
        .build();
    let bad_id = bad.id;

    let err = db.insert(bad).unwrap_err();

    assert!(matches!(err, mmdb_core::Error::InvalidArgument(_)));
    assert!(db.get(bad_id).unwrap().is_none());
    let hits = db.vector_search(&norm(vec![1.0, 0.0, 0.0]), 5).unwrap();
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].node.id, seed);
}

/// Toy embedder: tokenize on whitespace + FNV1a hash into a fixed-dim bucket.
/// Deterministic & content-discriminating enough for unit tests.
struct HashEmbedder {
    dim: u32,
    name: String,
}
impl HashEmbedder {
    fn new(name: &str, dim: u32) -> Self {
        Self {
            dim,
            name: name.to_string(),
        }
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
            let h = Self::fnv1a(tok) as usize;
            v[h % self.dim as usize] += 1.0;
        }
        // L2 normalize so cosine works.
        let n: f32 = v.iter().map(|x| x * x).sum::<f32>().sqrt();
        if n > 0.0 {
            for x in v.iter_mut() {
                *x /= n;
            }
        }
        Ok(v)
    }
    fn model_name(&self) -> &str {
        &self.name
    }
    fn dim(&self) -> u32 {
        self.dim
    }
}

#[test]
fn auto_embeds_text_on_insert() {
    let dir = tempdir().unwrap();
    let cfg = DatabaseConfig {
        tenant: DEFAULT_TENANT,
        default_model: "hash-32".into(),
    };
    let db =
        Database::open_with_embedder(dir.path(), cfg, Box::new(HashEmbedder::new("hash-32", 32)))
            .unwrap();
    assert!(db.has_embedder());

    let id = db
        .insert_text(NodeKind::Fact, "the quick brown fox")
        .unwrap();
    let got = db.get(id).unwrap().unwrap();
    assert_eq!(got.embeddings.len(), 1);
    assert_eq!(got.embeddings[0].model, "hash-32");
    assert_eq!(got.embeddings[0].dim, 32);

    // search_text should round-trip the same string back as the top hit.
    let hits = db.search_text("the quick brown fox", 3).unwrap();
    assert!(!hits.is_empty());
    assert_eq!(hits[0].node.id, id);
}

#[test]
fn explicit_embedding_overrides_auto() {
    let dir = tempdir().unwrap();
    let cfg = DatabaseConfig {
        tenant: DEFAULT_TENANT,
        default_model: "hash-32".into(),
    };
    let db =
        Database::open_with_embedder(dir.path(), cfg, Box::new(HashEmbedder::new("hash-32", 32)))
            .unwrap();
    // Pre-attach an embedding under the embedder's model -> auto-embed skipped.
    let mut v = vec![0.0f32; 32];
    v[0] = 1.0;
    let node = NodeBuilder::new(NodeKind::Fact)
        .text("ignored for embedding purposes")
        .embedding("hash-32", v.clone())
        .build();
    let id = db.insert(node).unwrap();
    let got = db.get(id).unwrap().unwrap();
    assert_eq!(got.embeddings.len(), 1);
    assert_eq!(got.embeddings[0].vector.as_slice(), v.as_slice());
}

#[test]
fn insert_text_without_embedder_errors() {
    let dir = tempdir().unwrap();
    let db = Database::open(dir.path()).unwrap();
    let err = db.insert_text(NodeKind::Fact, "x").unwrap_err();
    assert!(matches!(err, mmdb_core::Error::InvalidArgument(_)));
}

#[test]
fn open_with_embedder_rejects_model_mismatch() {
    let dir = tempdir().unwrap();
    let cfg = DatabaseConfig {
        tenant: DEFAULT_TENANT,
        default_model: "configured".into(),
    };

    let result =
        Database::open_with_embedder(dir.path(), cfg, Box::new(HashEmbedder::new("actual", 32)));
    let err = match result {
        Ok(_) => panic!("expected model mismatch to be rejected"),
        Err(err) => err,
    };

    assert!(format!("{err}").contains("does not match"));
}

struct AsyncOnlyEmbedder;
impl Embedder for AsyncOnlyEmbedder {
    fn embed(&self, _text: &str) -> Result<Vec<f32>> {
        Err(mmdb_core::Error::InvalidArgument(
            "sync embed should not run".into(),
        ))
    }
    fn model_name(&self) -> &str {
        "async-4"
    }
    fn dim(&self) -> u32 {
        4
    }
    fn embed_async<'a>(&'a self, _text: &'a str) -> EmbedFuture<'a> {
        Box::pin(async move { Ok(vec![1.0, 0.0, 0.0, 0.0]) })
    }
}

fn block_on<F: std::future::Future>(future: F) -> F::Output {
    let waker = noop_waker();
    let mut cx = std::task::Context::from_waker(&waker);
    let mut future = Box::pin(future);
    loop {
        match std::future::Future::poll(future.as_mut(), &mut cx) {
            std::task::Poll::Ready(value) => return value,
            std::task::Poll::Pending => std::thread::yield_now(),
        }
    }
}

fn noop_waker() -> std::task::Waker {
    fn raw_waker() -> std::task::RawWaker {
        fn clone(_: *const ()) -> std::task::RawWaker {
            raw_waker()
        }
        fn wake(_: *const ()) {}
        fn wake_by_ref(_: *const ()) {}
        fn drop(_: *const ()) {}
        std::task::RawWaker::new(
            std::ptr::null(),
            &std::task::RawWakerVTable::new(clone, wake, wake_by_ref, drop),
        )
    }

    unsafe { std::task::Waker::from_raw(raw_waker()) }
}

#[test]
fn async_text_paths_use_async_embedder() {
    let dir = tempdir().unwrap();
    let cfg = DatabaseConfig {
        tenant: DEFAULT_TENANT,
        default_model: "async-4".into(),
    };
    let db = Database::open_with_embedder(dir.path(), cfg, Box::new(AsyncOnlyEmbedder)).unwrap();

    let id = block_on(db.insert_text_async(NodeKind::Fact, "async memory")).unwrap();
    let got = db.get(id).unwrap().unwrap();
    assert_eq!(got.embeddings[0].model, "async-4");

    let hits = block_on(db.search_text_async("async memory", 1)).unwrap();
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].node.id, id);
}

#[test]
fn hybrid_search_promotes_neighbour_via_graph() {
    let dir = tempdir().unwrap();
    let db = Database::open(dir.path()).unwrap();

    // Three facts: query is closest to A; B is mid; C is far.
    let a = db
        .insert(
            NodeBuilder::new(NodeKind::Fact)
                .text("A")
                .embedding(DEFAULT_MODEL, norm(vec![1.0, 0.0, 0.0, 0.0]))
                .build(),
        )
        .unwrap();
    let b = db
        .insert(
            NodeBuilder::new(NodeKind::Fact)
                .text("B")
                .embedding(DEFAULT_MODEL, norm(vec![0.6, 0.8, 0.0, 0.0]))
                .build(),
        )
        .unwrap();
    let c = db
        .insert(
            NodeBuilder::new(NodeKind::Fact)
                .text("C")
                .embedding(DEFAULT_MODEL, norm(vec![0.0, 0.0, 1.0, 0.0]))
                .build(),
        )
        .unwrap();

    // Wire C as a related neighbour of A.
    db.add_edge(Edge {
        src: a,
        dst: c,
        label: "related".into(),
        weight: 1.0,
        created_at_ms: 0,
        metadata: BTreeMap::new(),
        revision: 1,
        valid_from_ms: None,
        valid_to_ms: None,
        evidence: Vec::new(),
    })
    .unwrap();

    let q = norm(vec![1.0, 0.0, 0.0, 0.0]);

    // Pure vector: C is ranked below B because it's orthogonal to the query.
    let pure = db.vector_search(&q, 3).unwrap();
    let pure_order: Vec<_> = pure.iter().map(|h| h.node.id).collect();
    assert_eq!(pure_order[0], a);
    // B should beat C in pure vector ranking.
    assert!(pure_order.iter().position(|x| *x == b) < pure_order.iter().position(|x| *x == c));

    // Hybrid: C gets a neighbour bump from A and may rank above B.
    let opts = HybridOpts {
        k: 3,
        seed_k: 5,
        expand_hops: 1,
        direction: crate::graph::Direction::Out,
        label: Some("related".into()),
        alpha: 0.3,
        decay: 1.0,
    };
    let hyb = db.hybrid_search(&q, opts).unwrap();
    let pos_b = hyb.iter().position(|h| h.node.id == b);
    let pos_c = hyb.iter().position(|h| h.node.id == c);
    assert!(pos_c.is_some(), "C must appear in hybrid result");
    // With alpha=0.3 and decay=1.0, C inherits 0.7 * a.score which dominates B.
    assert!(
        pos_c < pos_b || pos_b.is_none(),
        "C should be promoted above B; got order {:?}",
        hyb.iter().map(|h| (h.node.id, h.score)).collect::<Vec<_>>()
    );
}

#[test]
fn hybrid_search_alpha_one_equals_vector_only() {
    let dir = tempdir().unwrap();
    let db = Database::open(dir.path()).unwrap();
    let v = norm(vec![1.0, 0.0, 0.0, 0.0]);
    let id = db
        .insert(
            NodeBuilder::new(NodeKind::Fact)
                .text("x")
                .embedding(DEFAULT_MODEL, v.clone())
                .build(),
        )
        .unwrap();
    let opts = HybridOpts {
        alpha: 1.0,
        expand_hops: 0,
        ..Default::default()
    };
    let hits = db.hybrid_search(&v, opts).unwrap();
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].node.id, id);
}

#[test]
fn edge_labels_are_available_from_facade() {
    let dir = tempdir().unwrap();
    let db = Database::open(dir.path()).unwrap();
    let a = Ulid::new();
    let b = Ulid::new();
    db.add_edge(Edge {
        src: a,
        dst: b,
        label: "mentions".into(),
        weight: 1.0,
        created_at_ms: 0,
        metadata: BTreeMap::new(),
        revision: 1,
        valid_from_ms: None,
        valid_to_ms: None,
        evidence: Vec::new(),
    })
    .unwrap();

    assert_eq!(db.edge_labels().unwrap(), vec!["mentions".to_string()]);
}

#[test]
fn insert_blob_stores_artifact_and_reads_stream() {
    use std::io::{Cursor, Read};

    let dir = tempdir().unwrap();
    let db = Database::open(dir.path()).unwrap();
    let id = db
        .insert_blob(
            NodeKind::Artifact,
            Cursor::new(b"blob payload".to_vec()),
            "text/plain",
        )
        .unwrap();

    let node = db.get(id).unwrap().unwrap();
    let Content::Blob {
        hash,
        size,
        mime,
        inline,
    } = node.content
    else {
        panic!("expected blob content");
    };
    assert_eq!(size, 12);
    assert_eq!(mime, "text/plain");
    // Small blob ≤64KB: bytes must be inlined in the node record.
    assert_eq!(inline.as_deref(), Some(b"blob payload".as_slice()));
    assert_eq!(db.blob_refcount(&hash).unwrap(), Some(1));

    let mut bytes = Vec::new();
    db.get_blob_stream(&hash)
        .unwrap()
        .read_to_end(&mut bytes)
        .unwrap();
    assert_eq!(bytes, b"blob payload");

    // Short-circuiting get_blob_stream_for returns inlined bytes directly.
    let mut bytes = Vec::new();
    db.get_blob_stream_for(&hash, id)
        .unwrap()
        .read_to_end(&mut bytes)
        .unwrap();
    assert_eq!(bytes, b"blob payload");
}

mod agent_memory {
    use super::{block_on, Database, NodeBuilder};
    use crate::{
        AgentClient, AgentRequest, AgentResponse, AuditAction, AuditFilter, ChangeProposal,
        ChangeProposalStatus, ClientFuture, ClientRegistry, DreamProfile, DreamRun, DreamRunStatus,
        EmbeddingClient, EmbeddingDistance, EmbeddingInput, EmbeddingOutput, EmbeddingProfile,
        LawyerFailureMode, LawyerProfile, MaintenanceTrigger, MemoryProfile, ProjectionState,
        ProjectionStatus, ProposedChange, RecallRequest, RecallStatus, SupportedContent,
    };
    use fjall::PersistMode;
    use mmdb_core::{Edge, MemoryState, NodeKind};
    use std::collections::BTreeMap;
    use std::io::Cursor;
    use std::sync::{Arc, Barrier, Mutex};
    use tempfile::tempdir;
    use ulid::Ulid;

    struct FixedEmbedding {
        vector: Vec<f32>,
        projection: Option<String>,
        calls: Arc<Mutex<Vec<String>>>,
    }

    impl EmbeddingClient for FixedEmbedding {
        fn embed<'a>(
            &'a self,
            input: EmbeddingInput,
            profile: &'a EmbeddingProfile,
        ) -> ClientFuture<'a, EmbeddingOutput> {
            let input_type = match input {
                EmbeddingInput::Text(_) => "text",
                EmbeddingInput::Json(_) => "json",
                EmbeddingInput::Blob(blob) => {
                    blob.open().unwrap();
                    "blob"
                }
            };
            self.calls
                .lock()
                .unwrap()
                .push(format!("{}:{input_type}", profile.id));
            let output = EmbeddingOutput {
                vector: self.vector.clone(),
                searchable_text: self.projection.clone(),
            };
            Box::pin(async move { Ok(output) })
        }
    }

    struct BlockingEmbedding {
        vector: Vec<f32>,
        projection: Option<String>,
        entered: Arc<Barrier>,
        release: Arc<Barrier>,
    }

    impl EmbeddingClient for BlockingEmbedding {
        fn embed<'a>(
            &'a self,
            _input: EmbeddingInput,
            _profile: &'a EmbeddingProfile,
        ) -> ClientFuture<'a, EmbeddingOutput> {
            let vector = self.vector.clone();
            let searchable_text = self.projection.clone();
            let entered = Arc::clone(&self.entered);
            let release = Arc::clone(&self.release);
            Box::pin(async move {
                entered.wait();
                release.wait();
                Ok(EmbeddingOutput {
                    vector,
                    searchable_text,
                })
            })
        }
    }

    struct DynamicAgent {
        handler: Arc<dyn Fn(&AgentRequest) -> serde_json::Value + Send + Sync>,
    }

    impl AgentClient for DynamicAgent {
        fn call<'a>(&'a self, request: AgentRequest) -> ClientFuture<'a, AgentResponse> {
            let payload = (self.handler)(&request);
            Box::pin(async move { Ok(AgentResponse { payload }) })
        }
    }

    fn embedding_profile(
        id: &str,
        client_id: &str,
        model: &str,
        dimension: u32,
    ) -> EmbeddingProfile {
        EmbeddingProfile {
            id: id.into(),
            client_id: client_id.into(),
            model: model.into(),
            model_revision: "r1".into(),
            dimension,
            distance: EmbeddingDistance::Cosine,
            supported_content: vec![
                SupportedContent::Text,
                SupportedContent::Json,
                SupportedContent::Blob,
            ],
            supported_mime_types: Vec::new(),
            weight: 1.0,
        }
    }

    fn edge(src: Ulid, dst: Ulid, label: &str, at: i64) -> Edge {
        Edge {
            src,
            dst,
            label: label.into(),
            weight: 1.0,
            created_at_ms: at,
            metadata: BTreeMap::new(),
            revision: 1,
            valid_from_ms: Some(at),
            valid_to_ms: None,
            evidence: Vec::new(),
        }
    }

    fn pending_state_proposal(
        node_id: Ulid,
        expected_revision: u64,
        state: MemoryState,
    ) -> ChangeProposal {
        ChangeProposal {
            id: Ulid::new(),
            reason: "concurrency regression".into(),
            changes: vec![ProposedChange::SetState {
                node_id,
                expected_revision,
                state,
            }],
            status: ChangeProposalStatus::Pending,
            source_operation: None,
            created_at_ms: crate::now_ms(),
            applied_at_ms: None,
            next_change: 0,
        }
    }

    #[test]
    fn embedding_profile_fingerprint_is_full_deterministic_and_legacy_statuses_default_empty() {
        let base = embedding_profile("profile", "client", "model", 2);
        let fingerprint = base.fingerprint();
        assert_eq!(fingerprint, base.clone().fingerprint());
        assert_eq!(fingerprint.len(), 64);

        let mut variants = Vec::new();
        let mut variant = base.clone();
        variant.id = "other-profile".into();
        variants.push(variant);
        let mut variant = base.clone();
        variant.client_id = "other-client".into();
        variants.push(variant);
        let mut variant = base.clone();
        variant.model = "other-model".into();
        variants.push(variant);
        let mut variant = base.clone();
        variant.model_revision = "r2".into();
        variants.push(variant);
        let mut variant = base.clone();
        variant.dimension = 3;
        variants.push(variant);
        let mut variant = base.clone();
        variant.distance = EmbeddingDistance::Dot;
        variants.push(variant);
        let mut variant = base.clone();
        variant.supported_content = vec![SupportedContent::Text];
        variants.push(variant);
        let mut variant = base.clone();
        variant.supported_mime_types = vec!["text/plain".into()];
        variants.push(variant);
        let mut variant = base;
        variant.weight = 0.5;
        variants.push(variant);
        assert!(variants
            .iter()
            .all(|variant| variant.fingerprint() != fingerprint));

        let legacy: ProjectionStatus = serde_json::from_value(serde_json::json!({
            "node_id": Ulid::new(),
            "node_revision": 1,
            "profile_id": "profile",
            "state": "ready",
            "attempts": 1,
            "updated_at_ms": 10,
            "last_error": null,
            "searchable_text": null
        }))
        .unwrap();
        assert!(legacy.profile_fingerprint.is_empty());
    }

    #[test]
    fn multimodel_ingest_is_raw_first_retryable_and_lexically_searchable() {
        let dir = tempdir().unwrap();
        let calls = Arc::new(Mutex::new(Vec::new()));
        let clients = ClientRegistry::new();
        clients
            .register_embedding(
                "good-client",
                Arc::new(FixedEmbedding {
                    vector: vec![1.0, 0.0],
                    projection: Some("model generated projection".into()),
                    calls: calls.clone(),
                }),
            )
            .unwrap();
        clients
            .register_embedding(
                "bad-client",
                Arc::new(FixedEmbedding {
                    vector: vec![1.0],
                    projection: None,
                    calls: calls.clone(),
                }),
            )
            .unwrap();
        let profile = MemoryProfile {
            version: 1,
            revision: 1,
            embedding_profiles: vec![
                embedding_profile("good", "good-client", "good-model", 2),
                embedding_profile("bad", "bad-client", "bad-model", 2),
            ],
            dreamer: None,
            lawyer: None,
        };
        let db = Database::builder(dir.path())
            .clients(clients)
            .profile(profile)
            .build()
            .unwrap();

        let report = block_on(
            db.ingest(
                NodeBuilder::new(NodeKind::Fact)
                    .text("source survives provider failure")
                    .build(),
            ),
        )
        .unwrap();
        assert!(db.get(report.node_id).unwrap().is_some());
        assert_eq!(report.projections.len(), 2);
        assert_eq!(report.projections[0].state, ProjectionState::Ready);
        assert_eq!(report.projections[1].state, ProjectionState::Failed);
        let current_profile = db.memory_profile().unwrap();
        for status in &report.projections {
            let profile = current_profile
                .embedding_profiles
                .iter()
                .find(|profile| profile.id == status.profile_id)
                .unwrap();
            assert_eq!(status.profile_fingerprint, profile.fingerprint());
        }
        block_on(
            db.ingest(
                NodeBuilder::new(NodeKind::Fact)
                    .structured(serde_json::json!({
                        "topic": "structured",
                        "api_key": "must-not-enter-audit"
                    }))
                    .build(),
            ),
        )
        .unwrap();
        block_on(db.ingest_blob(
            NodeKind::Artifact,
            Cursor::new(b"blob payload"),
            "application/octet-stream",
        ))
        .unwrap();
        assert_eq!(
            calls.lock().unwrap().as_slice(),
            [
                "good:text",
                "bad:text",
                "good:json",
                "bad:json",
                "good:blob",
                "bad:blob"
            ]
        );

        let mut request = RecallRequest::new("model generated projection");
        request.vector_profiles.clear();
        request.limit = 5;
        let recalled = block_on(db.recall(request)).unwrap();
        assert_eq!(recalled.evidence[0].node.id, report.node_id);
        assert!(matches!(recalled.status, RecallStatus::Degraded { .. }));

        let audit = db.audit_records(AuditFilter::default()).unwrap();
        let encoded = serde_json::to_string(&audit).unwrap();
        assert!(!encoded.contains("[1.0,0.0]"));
        assert!(!encoded.contains("must-not-enter-audit"));
        assert!(audit
            .iter()
            .any(|record| record.action == AuditAction::ClientCall));
    }

    #[test]
    fn zero_and_overflow_norm_projection_outputs_fail_without_aborting_raw_ingest() {
        let dir = tempdir().unwrap();
        let calls = Arc::new(Mutex::new(Vec::new()));
        let clients = ClientRegistry::new();
        clients
            .register_embedding(
                "zero-client",
                Arc::new(FixedEmbedding {
                    vector: vec![0.0, 0.0],
                    projection: None,
                    calls: Arc::clone(&calls),
                }),
            )
            .unwrap();
        clients
            .register_embedding(
                "overflow-client",
                Arc::new(FixedEmbedding {
                    vector: vec![f32::MAX, f32::MAX],
                    projection: None,
                    calls,
                }),
            )
            .unwrap();
        let profile = MemoryProfile {
            version: 1,
            revision: 1,
            embedding_profiles: vec![
                embedding_profile("zero", "zero-client", "zero-model", 2),
                embedding_profile("overflow", "overflow-client", "overflow-model", 2),
            ],
            dreamer: None,
            lawyer: None,
        };
        let db = Database::builder(dir.path())
            .clients(clients)
            .profile(profile)
            .build()
            .unwrap();
        let raw = NodeBuilder::new(NodeKind::Episode)
            .text("raw episode survives invalid projections")
            .build();
        let raw_id = raw.id;

        let report = block_on(db.ingest(raw)).unwrap();
        assert_eq!(report.node_id, raw_id);
        assert_eq!(report.projections.len(), 2);
        let current_profile = db.memory_profile().unwrap();
        for status in &report.projections {
            assert_eq!(status.state, ProjectionState::Failed);
            let profile = current_profile
                .embedding_profiles
                .iter()
                .find(|profile| profile.id == status.profile_id)
                .unwrap();
            assert_eq!(status.profile_fingerprint, profile.fingerprint());
            assert!(status
                .last_error
                .as_deref()
                .is_some_and(|error| error.contains("finite, non-zero L2 norm")));
        }
        let stored = db.get(raw_id).unwrap().unwrap();
        assert_eq!(stored.revision, 1);
        assert!(stored.embeddings.is_empty());
        assert!(matches!(
            &stored.content,
            mmdb_core::Content::Text(text)
                if text == "raw episode survives invalid projections"
        ));
        assert_eq!(
            db.projection_statuses(raw_id)
                .unwrap()
                .iter()
                .filter(|status| status.state == ProjectionState::Failed)
                .count(),
            2
        );

        let retried = block_on(db.retry_projection(raw_id, "zero")).unwrap();
        assert_eq!(retried.state, ProjectionState::Failed);
        assert_eq!(retried.attempts, 2);
    }

    #[test]
    fn projection_currentness_tracks_content_and_profile_not_incidental_revision() {
        let dir = tempdir().unwrap();
        let clients = ClientRegistry::new();
        clients
            .register_embedding(
                "projection-client",
                Arc::new(FixedEmbedding {
                    vector: vec![1.0, 0.0],
                    projection: Some("projected-only-token".into()),
                    calls: Arc::new(Mutex::new(Vec::new())),
                }),
            )
            .unwrap();
        let profile = MemoryProfile {
            version: 1,
            revision: 1,
            embedding_profiles: vec![embedding_profile(
                "projection",
                "projection-client",
                "projection-model",
                2,
            )],
            dreamer: None,
            lawyer: None,
        };
        let db = Database::builder(dir.path())
            .clients(clients.clone())
            .profile(profile)
            .build()
            .unwrap();
        let report = block_on(
            db.ingest(
                NodeBuilder::new(NodeKind::Fact)
                    .text("raw source text")
                    .build(),
            ),
        )
        .unwrap();
        let mut node = db.get(report.node_id).unwrap().unwrap();
        let status = report.projections[0].clone();
        let active_profile = db.memory_profile().unwrap().embedding_profiles.remove(0);
        assert!(status.is_current_for(&active_profile, &node));

        node.metadata
            .insert("touch".into(), serde_json::json!(true));
        db.insert(node).unwrap();
        let node = db.get(report.node_id).unwrap().unwrap();
        assert_ne!(node.revision, status.node_revision);
        assert!(status.is_current_for(&active_profile, &node));

        let mut changed = db.memory_profile().unwrap();
        changed.embedding_profiles[0].model_revision = "r2".into();
        db.set_memory_profile(changed, Default::default()).unwrap();
        clients
            .register_embedding(
                "projection-client",
                Arc::new(FixedEmbedding {
                    vector: vec![0.0, 1.0],
                    projection: None,
                    calls: Arc::new(Mutex::new(Vec::new())),
                }),
            )
            .unwrap();

        let mut stale = RecallRequest::new("projected-only-token");
        stale.graph_depth = 0;
        stale.min_vector_similarity = Some(0.99);
        assert!(block_on(db.recall(stale.clone()))
            .unwrap()
            .evidence
            .is_empty());
        drop(db);

        let db = Database::builder(dir.path())
            .clients(clients)
            .build()
            .unwrap();
        assert!(block_on(db.recall(stale)).unwrap().evidence.is_empty());

        let direct = db
            .insert(
                NodeBuilder::new(NodeKind::Fact)
                    .text("direct explicit vector")
                    .embedding("projection-model", vec![0.0, 1.0])
                    .build(),
            )
            .unwrap();
        let mut direct_request = RecallRequest::new("vector-only-query");
        direct_request.lexical = false;
        direct_request.graph_depth = 0;
        direct_request.min_vector_similarity = Some(0.99);
        let recalled = block_on(db.recall(direct_request)).unwrap();
        assert_eq!(recalled.evidence.len(), 1);
        assert_eq!(recalled.evidence[0].node.id, direct);
    }

    #[test]
    fn failed_reprojection_hides_prior_vector_and_searchable_text() {
        let dir = tempdir().unwrap();
        let clients = ClientRegistry::new();
        clients
            .register_embedding(
                "projection-client",
                Arc::new(FixedEmbedding {
                    vector: vec![1.0, 0.0],
                    projection: Some("failed-projection-token".into()),
                    calls: Arc::new(Mutex::new(Vec::new())),
                }),
            )
            .unwrap();
        let profile = MemoryProfile {
            version: 1,
            revision: 1,
            embedding_profiles: vec![embedding_profile(
                "projection",
                "projection-client",
                "projection-model",
                2,
            )],
            dreamer: None,
            lawyer: None,
        };
        let db = Database::builder(dir.path())
            .clients(clients.clone())
            .profile(profile)
            .build()
            .unwrap();
        let report = block_on(
            db.ingest(
                NodeBuilder::new(NodeKind::Fact)
                    .text("raw content only")
                    .build(),
            ),
        )
        .unwrap();
        clients
            .register_embedding(
                "projection-client",
                Arc::new(FixedEmbedding {
                    vector: vec![0.0, 0.0],
                    projection: None,
                    calls: Arc::new(Mutex::new(Vec::new())),
                }),
            )
            .unwrap();
        let failed = block_on(db.retry_projection(report.node_id, "projection")).unwrap();
        assert_eq!(failed.state, ProjectionState::Failed);
        clients
            .register_embedding(
                "projection-client",
                Arc::new(FixedEmbedding {
                    vector: vec![1.0, 0.0],
                    projection: None,
                    calls: Arc::new(Mutex::new(Vec::new())),
                }),
            )
            .unwrap();

        let mut request = RecallRequest::new("failed-projection-token");
        request.graph_depth = 0;
        request.min_vector_similarity = Some(0.99);
        assert!(block_on(db.recall(request)).unwrap().evidence.is_empty());
    }

    #[test]
    fn projection_result_does_not_overwrite_content_changed_during_client_wait() {
        let dir = tempdir().unwrap();
        let entered = Arc::new(Barrier::new(2));
        let release = Arc::new(Barrier::new(2));
        let clients = ClientRegistry::new();
        clients
            .register_embedding(
                "blocking-client",
                Arc::new(BlockingEmbedding {
                    vector: vec![1.0, 0.0],
                    projection: Some("stale generated text".into()),
                    entered: Arc::clone(&entered),
                    release: Arc::clone(&release),
                }),
            )
            .unwrap();
        let profile = MemoryProfile {
            version: 1,
            revision: 1,
            embedding_profiles: vec![embedding_profile(
                "projection",
                "blocking-client",
                "projection-model",
                2,
            )],
            dreamer: None,
            lawyer: None,
        };
        let db = Arc::new(
            Database::builder(dir.path())
                .clients(clients)
                .profile(profile)
                .build()
                .unwrap(),
        );
        let id = db
            .insert(NodeBuilder::new(NodeKind::Fact).text("old content").build())
            .unwrap();
        let projection = {
            let db = Arc::clone(&db);
            std::thread::spawn(move || block_on(db.retry_projection(id, "projection")))
        };
        entered.wait();
        db.insert(
            NodeBuilder::new(NodeKind::Fact)
                .id(id)
                .text("new content")
                .embedding("projection-model", vec![0.0, 1.0])
                .build(),
        )
        .unwrap();
        release.wait();

        let status = projection.join().unwrap().unwrap();
        assert_eq!(status.state, ProjectionState::Failed);
        assert!(status
            .last_error
            .as_deref()
            .is_some_and(|error| error.contains("content changed")));
        let node = db.get(id).unwrap().unwrap();
        assert!(matches!(&node.content, mmdb_core::Content::Text(text) if text == "new content"));
        assert_eq!(node.embeddings[0].vector.as_slice(), &[0.0, 1.0]);
    }

    #[test]
    fn recall_applies_temporal_causal_and_conflict_rules_before_models() {
        let dir = tempdir().unwrap();
        let db = Database::open(dir.path()).unwrap();
        let active = db
            .insert(
                NodeBuilder::new(NodeKind::Fact)
                    .text("weather is sunny")
                    .created_at(50)
                    .build(),
            )
            .unwrap();
        let future = db
            .insert(
                NodeBuilder::new(NodeKind::Fact)
                    .text("weather future")
                    .created_at(150)
                    .build(),
            )
            .unwrap();
        let mut expired = NodeBuilder::new(NodeKind::Fact)
            .text("weather expired")
            .created_at(20)
            .build();
        expired.valid_to_ms = Some(90);
        let expired = db.insert(expired).unwrap();
        let mut superseded = NodeBuilder::new(NodeKind::Fact)
            .text("weather superseded")
            .created_at(10)
            .build();
        superseded.state = MemoryState::Superseded;
        let superseded = db.insert(superseded).unwrap();
        let mut historical = NodeBuilder::new(NodeKind::Fact)
            .text("historical marker")
            .created_at(10)
            .build();
        historical.state = MemoryState::Superseded;
        historical.valid_to_ms = Some(80);
        let historical = db.insert(historical).unwrap();
        let contradiction = db
            .insert(
                NodeBuilder::new(NodeKind::Fact)
                    .text("sky is green")
                    .created_at(40)
                    .build(),
            )
            .unwrap();
        let causal_past = db
            .insert(
                NodeBuilder::new(NodeKind::Fact)
                    .text("past effect")
                    .created_at(10)
                    .build(),
            )
            .unwrap();
        db.add_edge(edge(active, contradiction, "contradicts", 60))
            .unwrap();
        db.add_edge(edge(active, causal_past, "causes", 60))
            .unwrap();

        let mut request = RecallRequest::new("weather");
        request.as_of_ms = 100;
        request.graph_depth = 2;
        let recalled = block_on(db.recall(request)).unwrap();
        let ids: Vec<_> = recalled.evidence.iter().map(|item| item.node.id).collect();
        assert!(ids.contains(&active));
        assert!(ids.contains(&contradiction));
        assert!(!ids.contains(&future));
        assert!(!ids.contains(&expired));
        assert!(!ids.contains(&superseded));
        assert!(!ids.contains(&causal_past));
        let active_evidence = recalled
            .evidence
            .iter()
            .find(|item| item.node.id == active)
            .unwrap();
        assert_eq!(active_evidence.conflicts, vec![contradiction]);
        assert!(!active_evidence.verified);

        let mut historical_request = RecallRequest::new("historical marker");
        historical_request.as_of_ms = 70;
        let historical_recall = block_on(db.recall(historical_request)).unwrap();
        assert_eq!(historical_recall.evidence[0].node.id, historical);
    }

    #[test]
    fn derived_from_provenance_only_verifies_the_outgoing_derivative() {
        let dir = tempdir().unwrap();
        let db = Database::open(dir.path()).unwrap();
        let source = db
            .insert(
                NodeBuilder::new(NodeKind::Fact)
                    .text("rootfacttoken")
                    .created_at(10)
                    .build(),
            )
            .unwrap();
        let derived = db
            .insert(
                NodeBuilder::new(NodeKind::Fact)
                    .text("childfacttoken")
                    .created_at(20)
                    .build(),
            )
            .unwrap();
        let mut derivation = edge(derived, source, "derived_from", 20);
        derivation.evidence = vec![source];
        db.add_edge(derivation).unwrap();

        let mut source_request = RecallRequest::new("rootfacttoken");
        source_request.graph_depth = 0;
        let source_result = block_on(db.recall(source_request)).unwrap();
        let source_evidence = source_result
            .evidence
            .iter()
            .find(|evidence| evidence.node.id == source)
            .unwrap();
        assert!(source_evidence.provenance.is_empty());
        assert!(!source_evidence.verified);

        let mut derived_request = RecallRequest::new("childfacttoken");
        derived_request.graph_depth = 0;
        let derived_result = block_on(db.recall(derived_request)).unwrap();
        let derived_evidence = derived_result
            .evidence
            .iter()
            .find(|evidence| evidence.node.id == derived)
            .unwrap();
        assert_eq!(derived_evidence.provenance, vec![source]);
        assert!(derived_evidence.verified);
    }

    #[test]
    fn verified_filter_runs_before_candidate_budget_and_allows_owned_records() {
        let dir = tempdir().unwrap();
        let db = Database::open(dir.path()).unwrap();
        let source = db
            .insert(
                NodeBuilder::new(NodeKind::Episode)
                    .text("source episode")
                    .build(),
            )
            .unwrap();
        let verified = db
            .insert(
                NodeBuilder::new(NodeKind::Fact)
                    .text(format!("trustneedle {}", "padding ".repeat(200)))
                    .build(),
            )
            .unwrap();
        db.add_edge(edge(verified, source, "derived_from", crate::now_ms()))
            .unwrap();
        for index in 0..30 {
            db.insert(
                NodeBuilder::new(NodeKind::Fact)
                    .text(format!("trustneedle decoy {index}"))
                    .build(),
            )
            .unwrap();
        }

        let mut request = RecallRequest::new("trustneedle");
        request.limit = 1;
        request.candidate_limit = 1;
        request.graph_depth = 0;
        request.filter.require_verified = true;
        let recalled = block_on(db.recall(request)).unwrap();
        assert_eq!(recalled.evidence.len(), 1);
        assert_eq!(recalled.evidence[0].node.id, verified);
        assert!(recalled.evidence[0].verified);

        let owned = db
            .insert(
                NodeBuilder::new(NodeKind::Fact)
                    .text("owned-record-token")
                    .metadata("miumiu_record", serde_json::json!(false))
                    .build(),
            )
            .unwrap();
        let mut request = RecallRequest::new("owned-record-token");
        request.limit = 1;
        request.candidate_limit = 1;
        request.graph_depth = 0;
        request.filter.require_verified = true;
        request.filter.allow_unverified_metadata_keys = vec!["miumiu_record".into()];
        let recalled = block_on(db.recall(request)).unwrap();
        assert_eq!(recalled.evidence[0].node.id, owned);
        assert!(!recalled.evidence[0].verified);
    }

    #[test]
    fn derived_from_self_loop_is_rejected_and_cannot_self_verify() {
        let dir = tempdir().unwrap();
        let db = Database::open(dir.path()).unwrap();
        let id = db
            .insert(
                NodeBuilder::new(NodeKind::Fact)
                    .text("self-loop-token")
                    .build(),
            )
            .unwrap();
        let self_loop = edge(id, id, "derived_from", crate::now_ms());
        assert!(db.add_edge(self_loop.clone()).is_err());

        // Defensive read behavior also neutralizes legacy or corrupt raw edges.
        db.graph_store.add_edge(0, self_loop).unwrap();
        let mut request = RecallRequest::new("self-loop-token");
        request.graph_depth = 0;
        let recalled = block_on(db.recall(request)).unwrap();
        assert_eq!(recalled.evidence[0].node.id, id);
        assert!(!recalled.evidence[0].verified);
        assert!(recalled.evidence[0].provenance.is_empty());
    }

    #[test]
    fn recall_builds_snapshot_after_remote_query_embedding_without_blocking_writes() {
        let dir = tempdir().unwrap();
        let entered = Arc::new(Barrier::new(2));
        let release = Arc::new(Barrier::new(2));
        let clients = ClientRegistry::new();
        clients
            .register_embedding(
                "blocking-client",
                Arc::new(BlockingEmbedding {
                    vector: vec![1.0, 0.0],
                    projection: None,
                    entered: Arc::clone(&entered),
                    release: Arc::clone(&release),
                }),
            )
            .unwrap();
        let profile = MemoryProfile {
            version: 1,
            revision: 1,
            embedding_profiles: vec![embedding_profile(
                "recall",
                "blocking-client",
                "recall-model",
                2,
            )],
            dreamer: None,
            lawyer: None,
        };
        let db = Arc::new(
            Database::builder(dir.path())
                .clients(clients)
                .profile(profile)
                .build()
                .unwrap(),
        );
        let id = db
            .insert(
                NodeBuilder::new(NodeKind::Fact)
                    .text("oldsnapshottoken")
                    .embedding("recall-model", vec![1.0, 0.0])
                    .build(),
            )
            .unwrap();
        let recall = {
            let db = Arc::clone(&db);
            std::thread::spawn(move || {
                let mut request = RecallRequest::new("oldsnapshottoken");
                request.graph_depth = 0;
                request.min_vector_similarity = Some(0.99);
                block_on(db.recall(request))
            })
        };
        entered.wait();

        let (updated, received) = std::sync::mpsc::channel();
        let update = {
            let db = Arc::clone(&db);
            std::thread::spawn(move || {
                let result = db.insert(
                    NodeBuilder::new(NodeKind::Fact)
                        .id(id)
                        .text("newsnapshottoken")
                        .embedding("recall-model", vec![0.0, 1.0])
                        .build(),
                );
                let _ = updated.send(result);
            })
        };
        let update_before_release = received.recv_timeout(std::time::Duration::from_secs(2));
        release.wait();
        update.join().unwrap();
        update_before_release
            .expect("remote query embedding must not hold the node mutation lock")
            .unwrap();

        let recalled = recall.join().unwrap().unwrap();
        assert!(recalled.evidence.is_empty());
    }

    #[test]
    fn cjk_lexical_recall_works_without_embeddings() {
        let dir = tempdir().unwrap();
        let db = Database::open(dir.path()).unwrap();
        let id = db
            .insert(
                NodeBuilder::new(NodeKind::Fact)
                    .text("用户喜欢乌龙茶")
                    .build(),
            )
            .unwrap();

        let recalled = block_on(db.recall(RecallRequest::new("乌龙茶"))).unwrap();
        assert_eq!(recalled.evidence[0].node.id, id);
        assert!(!recalled.evidence[0].lexical_terms.is_empty());
    }

    #[test]
    fn lifecycle_filter_prevents_lexical_candidate_starvation_and_preserves_history() {
        let dir = tempdir().unwrap();
        let db = Database::open(dir.path()).unwrap();
        let active = db
            .insert(
                NodeBuilder::new(NodeKind::Fact)
                    .text("orchid active memory")
                    .created_at(10)
                    .build(),
            )
            .unwrap();
        let mut retired = NodeBuilder::new(NodeKind::Fact)
            .text("orchid ".repeat(24))
            .created_at(20)
            .build();
        retired.state = MemoryState::Retracted;
        retired.valid_to_ms = Some(100);
        let retired = db.insert(retired).unwrap();

        let mut current = RecallRequest::new("orchid");
        current.as_of_ms = 200;
        current.limit = 1;
        current.candidate_limit = 1;
        current.graph_depth = 0;
        let current = block_on(db.recall(current)).unwrap();
        assert_eq!(current.evidence[0].node.id, active);

        let mut historical = RecallRequest::new("orchid");
        historical.as_of_ms = 50;
        historical.limit = 1;
        historical.candidate_limit = 1;
        historical.graph_depth = 0;
        let historical = block_on(db.recall(historical)).unwrap();
        assert_eq!(historical.evidence[0].node.id, retired);
    }

    #[test]
    fn lexical_lifecycle_filter_propagates_node_storage_errors() {
        let dir = tempdir().unwrap();
        let db = Database::open(dir.path()).unwrap();
        let id = db
            .insert(
                NodeBuilder::new(NodeKind::Fact)
                    .text("corrupt lexical candidate")
                    .build(),
            )
            .unwrap();
        db.storage
            .nodes
            .insert(mmdb_storage::keys::node_key(0, id), b"{".as_slice())
            .unwrap();

        let error =
            block_on(db.recall(RecallRequest::new("corrupt lexical candidate"))).unwrap_err();
        assert!(error.to_string().contains("json error"));
    }

    #[test]
    fn vector_lifecycle_filter_propagates_node_storage_errors() {
        let dir = tempdir().unwrap();
        let clients = ClientRegistry::new();
        clients
            .register_embedding(
                "recall-client",
                Arc::new(FixedEmbedding {
                    vector: vec![1.0, 0.0],
                    projection: None,
                    calls: Arc::new(Mutex::new(Vec::new())),
                }),
            )
            .unwrap();
        let profile = MemoryProfile {
            version: 1,
            revision: 1,
            embedding_profiles: vec![embedding_profile(
                "recall",
                "recall-client",
                "recall-model",
                2,
            )],
            dreamer: None,
            lawyer: None,
        };
        let db = Database::builder(dir.path())
            .clients(clients)
            .profile(profile)
            .build()
            .unwrap();
        let id = db
            .insert(
                NodeBuilder::new(NodeKind::Fact)
                    .text("corrupt vector candidate")
                    .embedding("recall-model", vec![1.0, 0.0])
                    .build(),
            )
            .unwrap();
        db.storage
            .nodes
            .insert(mmdb_storage::keys::node_key(0, id), b"{".as_slice())
            .unwrap();

        let mut request = RecallRequest::new("vector query");
        request.lexical = false;
        request.graph_depth = 0;
        let error = block_on(db.recall(request)).unwrap_err();
        assert!(error.to_string().contains("json error"));
    }

    #[test]
    fn vector_lifecycle_filter_and_similarity_threshold_apply_before_candidate_budget() {
        let dir = tempdir().unwrap();
        let calls = Arc::new(Mutex::new(Vec::new()));
        let clients = ClientRegistry::new();
        clients
            .register_embedding(
                "recall-client",
                Arc::new(FixedEmbedding {
                    vector: vec![1.0, 0.0],
                    projection: None,
                    calls,
                }),
            )
            .unwrap();
        let profile = MemoryProfile {
            version: 1,
            revision: 1,
            embedding_profiles: vec![embedding_profile(
                "recall",
                "recall-client",
                "recall-model",
                2,
            )],
            dreamer: None,
            lawyer: None,
        };
        let db = Database::builder(dir.path())
            .clients(clients)
            .profile(profile)
            .build()
            .unwrap();
        let active = db
            .insert(
                NodeBuilder::new(NodeKind::Fact)
                    .text("active vector memory")
                    .embedding("recall-model", vec![0.8, 0.6])
                    .created_at(10)
                    .build(),
            )
            .unwrap();
        let mut retired = NodeBuilder::new(NodeKind::Fact)
            .text("retired vector memory")
            .embedding("recall-model", vec![1.0, 0.0])
            .created_at(20)
            .build();
        retired.state = MemoryState::Retracted;
        retired.valid_to_ms = Some(100);
        let retired = db.insert(retired).unwrap();

        let mut current = RecallRequest::new("unmatched vector query");
        current.as_of_ms = 200;
        current.limit = 1;
        current.candidate_limit = 1;
        current.lexical = false;
        current.graph_depth = 0;
        let current_result = block_on(db.recall(current.clone())).unwrap();
        assert_eq!(current_result.evidence[0].node.id, active);

        let mut historical = current.clone();
        historical.as_of_ms = 50;
        let historical_result = block_on(db.recall(historical.clone())).unwrap();
        assert_eq!(historical_result.evidence[0].node.id, retired);

        current.min_vector_similarity = Some(0.95);
        assert!(block_on(db.recall(current)).unwrap().evidence.is_empty());
        let mut lexical = RecallRequest::new("active vector memory");
        lexical.as_of_ms = 200;
        lexical.min_vector_similarity = Some(0.95);
        lexical.graph_depth = 0;
        let lexical = block_on(db.recall(lexical)).unwrap();
        assert_eq!(lexical.evidence[0].node.id, active);
        assert!(lexical.evidence[0].vectors.is_empty());
        historical.min_vector_similarity = Some(0.95);
        assert_eq!(
            block_on(db.recall(historical)).unwrap().evidence[0].node.id,
            retired
        );

        let mut invalid = RecallRequest::new("invalid threshold");
        invalid.min_vector_similarity = Some(f32::NAN);
        let error = block_on(db.recall(invalid)).unwrap_err();
        assert!(error.to_string().contains("min_vector_similarity"));
    }

    #[test]
    fn zero_norm_embeddings_are_rejected_for_nodes_and_recall_queries() {
        let dir = tempdir().unwrap();
        let clients = ClientRegistry::new();
        clients
            .register_embedding(
                "zero-client",
                Arc::new(FixedEmbedding {
                    vector: vec![0.0, 0.0],
                    projection: None,
                    calls: Arc::new(Mutex::new(Vec::new())),
                }),
            )
            .unwrap();
        let profile = MemoryProfile {
            version: 1,
            revision: 1,
            embedding_profiles: vec![embedding_profile(
                "zero-query",
                "zero-client",
                "recall-model",
                2,
            )],
            dreamer: None,
            lawyer: None,
        };
        let db = Database::builder(dir.path())
            .clients(clients)
            .profile(profile)
            .build()
            .unwrap();

        let invalid = NodeBuilder::new(NodeKind::Fact)
            .text("invalid zero vector")
            .embedding("recall-model", vec![0.0, 0.0])
            .build();
        let invalid_id = invalid.id;
        let error = db.insert(invalid).unwrap_err();
        assert!(error.to_string().contains("non-zero L2 norm"));
        assert!(db.get(invalid_id).unwrap().is_none());

        db.insert(
            NodeBuilder::new(NodeKind::Fact)
                .text("valid vector")
                .embedding("recall-model", vec![1.0, 0.0])
                .build(),
        )
        .unwrap();
        let mut request = RecallRequest::new("zero query");
        request.lexical = false;
        request.graph_depth = 0;
        request.min_vector_similarity = Some(0.9);
        let error = block_on(db.recall(request)).unwrap_err();
        assert!(error.to_string().contains("non-zero L2 norm"));
    }

    #[test]
    fn retract_is_revision_checked_idempotent_and_persistent() {
        let dir = tempdir().unwrap();
        let id;
        let retracted_at;
        let retracted_revision;
        {
            let db = Database::open(dir.path()).unwrap();
            id = db
                .insert(
                    NodeBuilder::new(NodeKind::Fact)
                        .text("temporary preference")
                        .build(),
                )
                .unwrap();
            let revision = db.get(id).unwrap().unwrap().revision;

            let stale = db
                .retract(id, Some(revision + 1), "stale request", Default::default())
                .unwrap_err();
            assert!(stale.to_string().contains("stale node revision"));
            assert_eq!(db.get(id).unwrap().unwrap().state, MemoryState::Active);

            let retracted = db
                .retract(id, Some(revision), "user requested", Default::default())
                .unwrap();
            assert_eq!(retracted.state, MemoryState::Retracted);
            retracted_at = retracted.valid_to_ms.unwrap();
            retracted_revision = retracted.revision;
            assert!(retracted_revision > revision);

            let recalled = block_on(db.recall(RecallRequest::new("temporary preference"))).unwrap();
            assert!(recalled.evidence.is_empty());

            let repeated = db.retract(id, None, "retry", Default::default()).unwrap();
            assert_eq!(repeated.revision, retracted_revision);
            assert_eq!(repeated.valid_to_ms, Some(retracted_at));
        }

        let db = Database::open(dir.path()).unwrap();
        let retracted = db.get(id).unwrap().unwrap();
        assert_eq!(retracted.state, MemoryState::Retracted);
        assert_eq!(retracted.valid_to_ms, Some(retracted_at));
        assert_eq!(retracted.revision, retracted_revision);
    }

    #[test]
    fn retracting_pending_memory_keeps_its_historical_interval_empty() {
        let dir = tempdir().unwrap();
        let db = Database::open(dir.path()).unwrap();
        let mut pending = NodeBuilder::new(NodeKind::Fact)
            .text("never published")
            .created_at(10)
            .build();
        pending.state = MemoryState::Pending;
        pending.valid_from_ms = Some(10);
        let id = db.insert(pending).unwrap();

        let retracted = db
            .retract(id, None, "discard staging", Default::default())
            .unwrap();
        assert_eq!(retracted.state, MemoryState::Retracted);
        assert_eq!(retracted.valid_from_ms, retracted.valid_to_ms);
        assert!(!retracted.is_valid_at(10));
        assert!(!retracted.is_valid_at(retracted.valid_to_ms.unwrap()));
    }

    #[test]
    fn lawyer_can_gate_and_propose_but_stale_proposals_cannot_mutate() {
        let dir = tempdir().unwrap();
        let clients = ClientRegistry::new();
        clients
            .register_agent(
                "law-client",
                Arc::new(DynamicAgent {
                    handler: Arc::new(|request| {
                        let node = &request.payload["evidence"][0]["node"];
                        let id = node["id"].clone();
                        let revision = node["revision"].clone();
                        serde_json::json!({
                            "accepted_candidate_ids": [id.clone()],
                            "rejected_candidate_ids": [],
                            "final_order": [id.clone()],
                            "annotations": [],
                            "cited_evidence_ids": [id.clone()],
                            "unresolved_conflicts": [],
                            "proposals": [{
                                "reason": "review retirement",
                                "changes": [{
                                    "type": "set_state",
                                    "node_id": id,
                                    "expected_revision": revision,
                                    "state": "Retracted"
                                }]
                            }]
                        })
                    }),
                }),
            )
            .unwrap();
        let profile = MemoryProfile {
            version: 1,
            revision: 1,
            embedding_profiles: Vec::new(),
            dreamer: None,
            lawyer: Some(LawyerProfile {
                id: "causal-lawyer".into(),
                client_id: "law-client".into(),
                agent_id: "law-agent".into(),
                model_id: "law-model".into(),
                prompt_version: "v1".into(),
                rule_set: "temporal-causal-v1".into(),
                evidence_limit: 50,
                failure_mode: LawyerFailureMode::ReturnDeterministic,
            }),
        };
        let db = Database::builder(dir.path())
            .clients(clients)
            .profile(profile)
            .build()
            .unwrap();
        let id = db
            .insert(
                NodeBuilder::new(NodeKind::Fact)
                    .text("owner is Alice")
                    .build(),
            )
            .unwrap();
        let mut request = RecallRequest::new("owner Alice");
        request.lawyer_profile = Some("causal-lawyer".into());
        let recalled = block_on(db.recall(request.clone())).unwrap();
        assert_eq!(recalled.status, RecallStatus::Adjudicated);
        assert_eq!(recalled.evidence[0].node.id, id);
        let proposal_id = recalled.verdict.unwrap().proposals[0].id;
        assert_eq!(db.get(id).unwrap().unwrap().state, MemoryState::Active);

        let node = db.get(id).unwrap().unwrap();
        db.insert(node).unwrap();
        assert!(db.apply_proposal(proposal_id, Default::default()).is_err());
        assert_eq!(
            db.proposal(proposal_id).unwrap().unwrap().status,
            ChangeProposalStatus::Stale
        );
        assert_eq!(db.get(id).unwrap().unwrap().state, MemoryState::Active);

        let operation_records = db.inspect_operation(recalled.operation_id).unwrap();
        assert_eq!(
            operation_records
                .iter()
                .filter(|record| record.action == AuditAction::Query)
                .count(),
            1
        );

        let fresh = block_on(db.recall(request)).unwrap();
        let fresh_proposal = fresh.verdict.unwrap().proposals[0].id;
        db.apply_proposal(fresh_proposal, Default::default())
            .unwrap();
        assert_eq!(db.get(id).unwrap().unwrap().state, MemoryState::Retracted);
    }

    #[test]
    fn proposal_apply_and_reject_are_serializable() {
        let dir = tempdir().unwrap();
        let db = Arc::new(Database::open(dir.path()).unwrap());
        for index in 0..16 {
            let node_id = db
                .insert(
                    NodeBuilder::new(NodeKind::Fact)
                        .text(format!("proposal race {index}"))
                        .build(),
                )
                .unwrap();
            let revision = db.get(node_id).unwrap().unwrap().revision;
            let proposal = pending_state_proposal(node_id, revision, MemoryState::Retracted);
            let proposal_id = proposal.id;
            db.runtime_store
                .put_proposal(0, proposal_id, &proposal)
                .unwrap();

            let barrier = Arc::new(Barrier::new(3));
            let apply = {
                let db = Arc::clone(&db);
                let barrier = Arc::clone(&barrier);
                std::thread::spawn(move || {
                    barrier.wait();
                    db.apply_proposal(proposal_id, Default::default())
                })
            };
            let reject = {
                let db = Arc::clone(&db);
                let barrier = Arc::clone(&barrier);
                std::thread::spawn(move || {
                    barrier.wait();
                    db.reject_proposal(proposal_id, Default::default())
                })
            };
            barrier.wait();
            let apply = apply.join().unwrap();
            let reject = reject.join().unwrap();
            assert_ne!(apply.is_ok(), reject.is_ok());

            let status = db.proposal(proposal_id).unwrap().unwrap().status;
            let state = db.get(node_id).unwrap().unwrap().state;
            match status {
                ChangeProposalStatus::Applied => assert_eq!(state, MemoryState::Retracted),
                ChangeProposalStatus::Rejected => assert_eq!(state, MemoryState::Active),
                other => panic!("unexpected terminal proposal status: {other:?}"),
            }
        }
    }

    #[test]
    fn proposal_revision_validation_and_node_update_are_serializable() {
        let dir = tempdir().unwrap();
        let db = Arc::new(Database::open(dir.path()).unwrap());
        for index in 0..16 {
            let node_id = db
                .insert(
                    NodeBuilder::new(NodeKind::Fact)
                        .text(format!("before proposal race {index}"))
                        .build(),
                )
                .unwrap();
            let original = db.get(node_id).unwrap().unwrap();
            let proposal =
                pending_state_proposal(node_id, original.revision, MemoryState::Retracted);
            let proposal_id = proposal.id;
            db.runtime_store
                .put_proposal(0, proposal_id, &proposal)
                .unwrap();
            let mut replacement = original;
            replacement.content = mmdb_core::Content::Text(format!("after proposal race {index}"));

            let barrier = Arc::new(Barrier::new(3));
            let apply = {
                let db = Arc::clone(&db);
                let barrier = Arc::clone(&barrier);
                std::thread::spawn(move || {
                    barrier.wait();
                    db.apply_proposal(proposal_id, Default::default())
                })
            };
            let insert = {
                let db = Arc::clone(&db);
                let barrier = Arc::clone(&barrier);
                std::thread::spawn(move || {
                    barrier.wait();
                    db.insert(replacement)
                })
            };
            barrier.wait();
            let _ = apply.join().unwrap();
            insert.join().unwrap().unwrap();

            let final_node = db.get(node_id).unwrap().unwrap();
            assert!(matches!(
                &final_node.content,
                mmdb_core::Content::Text(text) if text == &format!("after proposal race {index}")
            ));
            assert_eq!(final_node.state, MemoryState::Active);
            assert!(matches!(
                db.proposal(proposal_id).unwrap().unwrap().status,
                ChangeProposalStatus::Applied | ChangeProposalStatus::Stale
            ));
        }
    }

    #[test]
    fn reopen_resumes_interrupted_multi_change_proposal_and_repairs_indexes() {
        let dir = tempdir().unwrap();
        let proposal_id = Ulid::new();
        let first_id;
        let second_id;
        {
            let db = Database::open(dir.path()).unwrap();
            first_id = db
                .insert(
                    NodeBuilder::new(NodeKind::Fact)
                        .text("first interrupted change")
                        .embedding("proposal-model", vec![1.0, 0.0])
                        .build(),
                )
                .unwrap();
            second_id = db
                .insert(
                    NodeBuilder::new(NodeKind::Fact)
                        .text("second interrupted change")
                        .build(),
                )
                .unwrap();
            let first_revision = db.get(first_id).unwrap().unwrap().revision;
            let second_revision = db.get(second_id).unwrap().unwrap().revision;
            let proposal = ChangeProposal {
                id: proposal_id,
                reason: "recover partial multi-change apply".into(),
                changes: vec![
                    ProposedChange::SetState {
                        node_id: first_id,
                        expected_revision: first_revision,
                        state: MemoryState::Retracted,
                    },
                    ProposedChange::SetValidity {
                        node_id: second_id,
                        expected_revision: second_revision,
                        valid_from_ms: Some(10),
                        valid_to_ms: Some(20),
                    },
                ],
                status: ChangeProposalStatus::Applying,
                source_operation: None,
                created_at_ms: crate::now_ms(),
                applied_at_ms: None,
                next_change: 0,
            };
            db.runtime_store
                .put_proposal(0, proposal_id, &proposal)
                .unwrap();

            // Simulate a crash after the source node commit but before its
            // vector reindex and before the proposal cursor checkpoint.
            let mut first = db.get(first_id).unwrap().unwrap();
            first.state = MemoryState::Retracted;
            first.valid_to_ms = Some(crate::now_ms());
            first.revision = first_revision + 1;
            db.storage.put_node(&first).unwrap();
            db.vector_store
                .delete(0, "proposal-model", first_id)
                .unwrap();
            assert!(db
                .vector_store
                .search(0, "proposal-model", &[1.0, 0.0], 4)
                .unwrap()
                .is_empty());
        }

        let db = Database::open(dir.path()).unwrap();
        let proposal = db.proposal(proposal_id).unwrap().unwrap();
        assert_eq!(proposal.status, ChangeProposalStatus::Applied);
        assert_eq!(proposal.next_change, 2);
        assert!(proposal.applied_at_ms.is_some());
        assert_eq!(
            db.get(first_id).unwrap().unwrap().state,
            MemoryState::Retracted
        );
        let second = db.get(second_id).unwrap().unwrap();
        assert_eq!(second.valid_from_ms, Some(10));
        assert_eq!(second.valid_to_ms, Some(20));
        assert!(db
            .vector_store
            .search(0, "proposal-model", &[1.0, 0.0], 4)
            .unwrap()
            .iter()
            .any(|hit| hit.node_id == first_id));
        assert!(db
            .audit_records(AuditFilter {
                action: Some(AuditAction::Repair),
                ..AuditFilter::default()
            })
            .unwrap()
            .iter()
            .any(|record| record.name == "repair_proposal"));
    }

    #[test]
    fn applying_proposal_revalidates_edge_endpoints_before_unapplied_edge() {
        let dir = tempdir().unwrap();
        let db = Database::open(dir.path()).unwrap();
        let source = db
            .insert(NodeBuilder::new(NodeKind::Fact).text("edge source").build())
            .unwrap();
        let destination = db
            .insert(
                NodeBuilder::new(NodeKind::Fact)
                    .text("edge destination")
                    .build(),
            )
            .unwrap();
        let source_revision = db.get(source).unwrap().unwrap().revision;
        let destination_revision = db.get(destination).unwrap().unwrap().revision;
        let proposal_id = Ulid::new();
        let proposal = ChangeProposal {
            id: proposal_id,
            reason: "resume an interrupted edge change".into(),
            changes: vec![ProposedChange::AddEdge {
                edge: edge(source, destination, "causes", crate::now_ms()),
                expected_src_revision: source_revision,
                expected_dst_revision: destination_revision,
            }],
            status: ChangeProposalStatus::Applying,
            source_operation: None,
            created_at_ms: crate::now_ms(),
            applied_at_ms: None,
            next_change: 0,
        };
        db.runtime_store
            .put_proposal(0, proposal_id, &proposal)
            .unwrap();

        // Simulate an endpoint edit after a transient failure but before the
        // edge was inserted and the Applying proposal was retried.
        let mut changed_source = db.get(source).unwrap().unwrap();
        changed_source.content = mmdb_core::Content::Text("changed edge source".into());
        db.insert(changed_source).unwrap();

        let error = db
            .apply_proposal(proposal_id, Default::default())
            .unwrap_err();
        assert!(error.to_string().contains("edge source"));
        assert!(db
            .neighbours_out(source, Some("causes"))
            .unwrap()
            .is_empty());
        let proposal = db.proposal(proposal_id).unwrap().unwrap();
        assert_eq!(proposal.status, ChangeProposalStatus::Applying);
        assert_eq!(proposal.next_change, 0);
    }

    #[test]
    fn lawyer_rejects_out_of_scope_verdicts_with_configured_failure_mode() {
        let dir = tempdir().unwrap();
        let clients = ClientRegistry::new();
        clients
            .register_agent(
                "bad-lawyer",
                Arc::new(DynamicAgent {
                    handler: Arc::new(|_| {
                        let unknown = Ulid::new();
                        serde_json::json!({
                            "accepted_candidate_ids": [unknown],
                            "rejected_candidate_ids": [],
                            "final_order": [unknown],
                            "annotations": [],
                            "cited_evidence_ids": [],
                            "unresolved_conflicts": [],
                            "proposals": []
                        })
                    }),
                }),
            )
            .unwrap();
        let lawyer = LawyerProfile {
            id: "strict".into(),
            client_id: "bad-lawyer".into(),
            agent_id: "law-agent".into(),
            model_id: "law-model".into(),
            prompt_version: "v1".into(),
            rule_set: "temporal-causal-v1".into(),
            evidence_limit: 50,
            failure_mode: LawyerFailureMode::ReturnDeterministic,
        };
        let profile = MemoryProfile {
            version: 1,
            revision: 1,
            embedding_profiles: Vec::new(),
            dreamer: None,
            lawyer: Some(lawyer),
        };
        let db = Database::builder(dir.path())
            .clients(clients)
            .profile(profile)
            .build()
            .unwrap();
        let id = db
            .insert(NodeBuilder::new(NodeKind::Fact).text("stable fact").build())
            .unwrap();
        let mut request = RecallRequest::new("stable fact");
        request.lawyer_profile = Some("strict".into());
        let recalled = block_on(db.recall(request.clone())).unwrap();
        assert_eq!(recalled.evidence[0].node.id, id);
        assert!(matches!(recalled.status, RecallStatus::Degraded { .. }));

        let mut profile = db.memory_profile().unwrap();
        profile.lawyer.as_mut().unwrap().failure_mode = LawyerFailureMode::FailClosed;
        db.set_memory_profile(profile, Default::default()).unwrap();
        assert!(block_on(db.recall(request)).is_err());
    }

    #[test]
    fn dream_runs_are_provenanced_idempotent_and_reversible() {
        let dir = tempdir().unwrap();
        let clients = ClientRegistry::new();
        clients
            .register_agent(
                "dream-client",
                Arc::new(DynamicAgent {
                    handler: Arc::new(|request| {
                        let source = request.payload["sources"][0]["id"].clone();
                        serde_json::json!({
                            "nodes": [{
                                "temporary_id": "summary",
                                "kind": "Fact",
                                "content": {"Text": "durable summary"},
                                "source_citations": [source],
                                "metadata": {"confidence": 0.9}
                            }],
                            "edges": [],
                            "supersede": [],
                            "explanation": "compact one episode"
                        })
                    }),
                }),
            )
            .unwrap();
        let profile = MemoryProfile {
            version: 1,
            revision: 1,
            embedding_profiles: Vec::new(),
            dreamer: Some(DreamProfile {
                id: "nightly".into(),
                revision: "r1".into(),
                client_id: "dream-client".into(),
                agent_id: "dream-agent".into(),
                model_id: "dream-model".into(),
                prompt_version: "v1".into(),
                response_schema: serde_json::json!({"type": "object"}),
                turn_end_threshold: 32,
                max_nodes: 128,
                max_input_bytes: 256 * 1024,
            }),
            lawyer: None,
        };
        let db = Database::builder(dir.path())
            .clients(clients)
            .profile(profile)
            .build()
            .unwrap();
        let source = db
            .insert(
                NodeBuilder::new(NodeKind::Episode)
                    .text("user prefers tea")
                    .build(),
            )
            .unwrap();
        assert!(
            block_on(db.maintain(MaintenanceTrigger::TurnEnd, Default::default()))
                .unwrap()
                .is_none()
        );
        let run = block_on(db.maintain(MaintenanceTrigger::Manual, Default::default()))
            .unwrap()
            .unwrap();
        let created = run.created_ids[0];
        assert_eq!(db.get(source).unwrap().unwrap().state, MemoryState::Active);
        assert_eq!(db.get(created).unwrap().unwrap().state, MemoryState::Active);
        assert_eq!(
            db.neighbours_out(created, Some("derived_from")).unwrap()[0].dst,
            source
        );
        assert!(
            block_on(db.maintain(MaintenanceTrigger::Manual, Default::default()))
                .unwrap()
                .is_none()
        );

        db.revert_dream(run.id, Default::default()).unwrap();
        let retracted = db.get(created).unwrap().unwrap();
        assert_eq!(retracted.state, MemoryState::Retracted);
        let retracted_at = retracted.valid_to_ms.unwrap();
        if retracted.valid_from_ms.unwrap() < retracted_at {
            assert!(retracted.is_valid_at(retracted_at - 1));
        }
        assert!(!retracted.is_valid_at(retracted_at));
        assert!(db
            .neighbours_out(created, Some("derived_from"))
            .unwrap()
            .is_empty());
    }

    #[test]
    fn dream_revert_refuses_to_erase_an_edited_output_node() {
        let dir = tempdir().unwrap();
        let db = Database::open(dir.path()).unwrap();
        let run_id = Ulid::new();
        let created_id = db
            .insert(
                NodeBuilder::new(NodeKind::Fact)
                    .text("dreamed fact")
                    .metadata("dream_run_id", serde_json::json!(run_id))
                    .build(),
            )
            .unwrap();
        let created_revision = db.get(created_id).unwrap().unwrap().revision;
        let run = DreamRun {
            id: run_id,
            profile_id: "test".into(),
            profile_revision: "r1".into(),
            source_hash: "edited-output".into(),
            source_ids: Vec::new(),
            created_ids: vec![created_id],
            created_revisions: BTreeMap::from([(created_id, created_revision)]),
            created_fingerprints: BTreeMap::new(),
            added_edges: Vec::new(),
            superseded: Vec::new(),
            explanation: "edited output guard".into(),
            status: DreamRunStatus::Completed,
            created_at_ms: crate::now_ms(),
            completed_at_ms: Some(crate::now_ms()),
            error: None,
        };
        db.runtime_store.put_dream_run(0, run_id, &run).unwrap();

        let mut edited = db.get(created_id).unwrap().unwrap();
        edited.content = mmdb_core::Content::Text("human-edited fact".into());
        db.insert(edited).unwrap();

        let error = db.revert_dream(run_id, Default::default()).unwrap_err();
        assert!(error.to_string().contains("changed after compaction"));
        let preserved = db.get(created_id).unwrap().unwrap();
        assert_eq!(preserved.state, MemoryState::Active);
        assert!(matches!(
            preserved.content,
            mmdb_core::Content::Text(ref text) if text == "human-edited fact"
        ));
        assert_eq!(
            db.dream_run(run_id).unwrap().unwrap().status,
            DreamRunStatus::Completed
        );
    }

    #[test]
    fn dream_revert_refuses_to_remove_an_edited_edge() {
        let dir = tempdir().unwrap();
        let db = Database::open(dir.path()).unwrap();
        let run_id = Ulid::new();
        let source_id = db
            .insert(NodeBuilder::new(NodeKind::Episode).text("source").build())
            .unwrap();
        let created_id = db
            .insert(
                NodeBuilder::new(NodeKind::Fact)
                    .text("dreamed fact")
                    .metadata("dream_run_id", serde_json::json!(run_id))
                    .build(),
            )
            .unwrap();
        db.add_edge(edge(created_id, source_id, "derived_from", crate::now_ms()))
            .unwrap();
        let dreamed_edge = db
            .neighbours_out(created_id, Some("derived_from"))
            .unwrap()
            .pop()
            .unwrap();
        let run = DreamRun {
            id: run_id,
            profile_id: "test".into(),
            profile_revision: "r1".into(),
            source_hash: "edited-edge".into(),
            source_ids: vec![source_id],
            created_ids: vec![created_id],
            created_revisions: BTreeMap::from([(
                created_id,
                db.get(created_id).unwrap().unwrap().revision,
            )]),
            created_fingerprints: BTreeMap::new(),
            added_edges: vec![dreamed_edge.clone()],
            superseded: Vec::new(),
            explanation: "edited edge guard".into(),
            status: DreamRunStatus::Completed,
            created_at_ms: crate::now_ms(),
            completed_at_ms: Some(crate::now_ms()),
            error: None,
        };
        db.runtime_store.put_dream_run(0, run_id, &run).unwrap();

        let mut edited_edge = dreamed_edge;
        edited_edge.weight = 0.25;
        db.add_edge(edited_edge).unwrap();

        let error = db.revert_dream(run_id, Default::default()).unwrap_err();
        assert!(error.to_string().contains("changed after compaction"));
        let preserved = db.neighbours_out(created_id, Some("derived_from")).unwrap();
        assert_eq!(preserved.len(), 1);
        assert_eq!(preserved[0].weight, 0.25);
        assert_eq!(preserved[0].revision, 2);
        assert_eq!(
            db.dream_run(run_id).unwrap().unwrap().status,
            DreamRunStatus::Completed
        );
    }

    #[test]
    fn dream_outputs_remain_pending_through_projection_and_changed_staging_is_preserved() {
        let dir = tempdir().unwrap();
        let entered = Arc::new(Barrier::new(2));
        let release = Arc::new(Barrier::new(2));
        let clients = ClientRegistry::new();
        clients
            .register_embedding(
                "blocking-embedding",
                Arc::new(BlockingEmbedding {
                    vector: vec![1.0, 0.0],
                    projection: None,
                    entered: Arc::clone(&entered),
                    release: Arc::clone(&release),
                }),
            )
            .unwrap();
        clients
            .register_agent(
                "dream-client",
                Arc::new(DynamicAgent {
                    handler: Arc::new(|request| {
                        let source = request.payload["sources"][0]["id"].clone();
                        serde_json::json!({
                            "nodes": [{
                                "temporary_id": "summary",
                                "kind": "Fact",
                                "content": {"Text": "staged summary"},
                                "source_citations": [source],
                                "metadata": {}
                            }],
                            "edges": [],
                            "supersede": [],
                            "explanation": "staging integrity"
                        })
                    }),
                }),
            )
            .unwrap();
        let profile = MemoryProfile {
            version: 1,
            revision: 1,
            embedding_profiles: vec![embedding_profile(
                "blocking-profile",
                "blocking-embedding",
                "blocking-model",
                2,
            )],
            dreamer: Some(DreamProfile {
                id: "nightly".into(),
                revision: "r1".into(),
                client_id: "dream-client".into(),
                agent_id: "dream-agent".into(),
                model_id: "dream-model".into(),
                prompt_version: "v1".into(),
                response_schema: serde_json::json!({"type": "object"}),
                turn_end_threshold: 32,
                max_nodes: 128,
                max_input_bytes: 256 * 1024,
            }),
            lawyer: None,
        };
        let db = Arc::new(
            Database::builder(dir.path())
                .clients(clients)
                .profile(profile)
                .build()
                .unwrap(),
        );
        let source_id = db
            .insert(
                NodeBuilder::new(NodeKind::Episode)
                    .text("raw source remains available")
                    .build(),
            )
            .unwrap();
        let maintenance = {
            let db = Arc::clone(&db);
            std::thread::spawn(move || {
                block_on(db.maintain(MaintenanceTrigger::Manual, Default::default()))
            })
        };

        entered.wait();
        let staged_run = db.dream_runs().unwrap().pop().unwrap();
        assert_eq!(staged_run.status, DreamRunStatus::Pending);
        let staged_id = staged_run.created_ids[0];
        assert_eq!(
            db.get(staged_id).unwrap().unwrap().state,
            MemoryState::Pending
        );
        assert_eq!(
            db.get(source_id).unwrap().unwrap().state,
            MemoryState::Active
        );

        let mut edited = db.get(staged_id).unwrap().unwrap();
        edited.content = mmdb_core::Content::Text("external edit during projection".into());
        db.insert(edited).unwrap();
        release.wait();

        let error = maintenance.join().unwrap().unwrap_err();
        assert!(error.to_string().contains("changed during projection"));
        let run = db.dream_run(staged_run.id).unwrap().unwrap();
        assert_eq!(run.status, DreamRunStatus::Failed);
        assert!(run
            .error
            .as_deref()
            .is_some_and(|error| error.contains("preserved changed staging")));
        let preserved = db.get(staged_id).unwrap().unwrap();
        assert_eq!(preserved.state, MemoryState::Pending);
        assert!(matches!(
            preserved.content,
            mmdb_core::Content::Text(ref text) if text == "external edit during projection"
        ));
        assert_eq!(
            db.get(source_id).unwrap().unwrap().state,
            MemoryState::Active
        );
    }

    #[test]
    fn concurrent_dream_maintenance_serializes_validation_apply_and_checkpoints() {
        let dir = tempdir().unwrap();
        let client_barrier = Arc::new(Barrier::new(2));
        let clients = ClientRegistry::new();
        clients
            .register_agent(
                "dream-client",
                Arc::new(DynamicAgent {
                    handler: Arc::new({
                        let client_barrier = Arc::clone(&client_barrier);
                        move |request| {
                            let source = request.payload["sources"][0]["id"].clone();
                            client_barrier.wait();
                            serde_json::json!({
                                "nodes": [{
                                    "temporary_id": "summary",
                                    "kind": "Fact",
                                    "content": {"Text": "one serialized summary"},
                                    "source_citations": [source],
                                    "metadata": {}
                                }],
                                "edges": [],
                                "supersede": [],
                                "explanation": "concurrent maintenance"
                            })
                        }
                    }),
                }),
            )
            .unwrap();
        let profile = MemoryProfile {
            version: 1,
            revision: 1,
            embedding_profiles: Vec::new(),
            dreamer: Some(DreamProfile {
                id: "nightly".into(),
                revision: "r1".into(),
                client_id: "dream-client".into(),
                agent_id: "dream-agent".into(),
                model_id: "dream-model".into(),
                prompt_version: "v1".into(),
                response_schema: serde_json::json!({"type": "object"}),
                turn_end_threshold: 32,
                max_nodes: 128,
                max_input_bytes: 256 * 1024,
            }),
            lawyer: None,
        };
        let db = Arc::new(
            Database::builder(dir.path())
                .clients(clients)
                .profile(profile)
                .build()
                .unwrap(),
        );
        db.insert(
            NodeBuilder::new(NodeKind::Episode)
                .text("concurrent source")
                .build(),
        )
        .unwrap();

        let runs = (0..2)
            .map(|_| {
                let db = Arc::clone(&db);
                std::thread::spawn(move || {
                    block_on(db.maintain(MaintenanceTrigger::Manual, Default::default()))
                })
            })
            .collect::<Vec<_>>();
        let results = runs
            .into_iter()
            .map(|run| run.join().unwrap().unwrap())
            .collect::<Vec<_>>();
        assert_eq!(results.iter().filter(|run| run.is_some()).count(), 1);
        let persisted = db.dream_runs().unwrap();
        assert_eq!(persisted.len(), 1);
        assert_eq!(persisted[0].status, DreamRunStatus::Completed);
        assert_eq!(persisted[0].created_ids.len(), 1);
    }

    #[test]
    fn concurrent_dream_reverts_have_one_terminal_winner() {
        let dir = tempdir().unwrap();
        let db = Arc::new(Database::open(dir.path()).unwrap());
        for index in 0..8 {
            let run_id = Ulid::new();
            let created_id = db
                .insert(
                    NodeBuilder::new(NodeKind::Fact)
                        .text(format!("dream output {index}"))
                        .metadata("dream_run_id", serde_json::json!(run_id))
                        .build(),
                )
                .unwrap();
            let run = DreamRun {
                id: run_id,
                profile_id: "test".into(),
                profile_revision: "r1".into(),
                source_hash: format!("revert-race-{index}"),
                source_ids: Vec::new(),
                created_ids: vec![created_id],
                created_revisions: BTreeMap::from([(
                    created_id,
                    db.get(created_id).unwrap().unwrap().revision,
                )]),
                created_fingerprints: BTreeMap::new(),
                added_edges: Vec::new(),
                superseded: Vec::new(),
                explanation: "revert race".into(),
                status: DreamRunStatus::Completed,
                created_at_ms: crate::now_ms(),
                completed_at_ms: Some(crate::now_ms()),
                error: None,
            };
            db.runtime_store.put_dream_run(0, run_id, &run).unwrap();

            let barrier = Arc::new(Barrier::new(3));
            let reverts = (0..2)
                .map(|_| {
                    let db = Arc::clone(&db);
                    let barrier = Arc::clone(&barrier);
                    std::thread::spawn(move || {
                        barrier.wait();
                        db.revert_dream(run_id, Default::default())
                    })
                })
                .collect::<Vec<_>>();
            barrier.wait();
            let results = reverts
                .into_iter()
                .map(|revert| revert.join().unwrap())
                .collect::<Vec<_>>();
            assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 1);
            assert_eq!(
                db.dream_run(run_id).unwrap().unwrap().status,
                DreamRunStatus::Reverted
            );
            assert_eq!(
                db.get(created_id).unwrap().unwrap().state,
                MemoryState::Retracted
            );
        }
    }

    #[test]
    fn reopen_repairs_incomplete_dream_staging() {
        let dir = tempdir().unwrap();
        let run_id = Ulid::new();
        let staged_id;
        {
            let db = Database::open(dir.path()).unwrap();
            let mut staged = NodeBuilder::new(NodeKind::Fact)
                .text("never published")
                .build();
            staged.state = MemoryState::Pending;
            staged
                .metadata
                .insert("dream_run_id".into(), serde_json::json!(run_id));
            staged_id = db.insert(staged).unwrap();
            let run = DreamRun {
                id: run_id,
                profile_id: "test".into(),
                profile_revision: "r1".into(),
                source_hash: "incomplete".into(),
                source_ids: Vec::new(),
                created_ids: vec![staged_id],
                created_revisions: BTreeMap::new(),
                created_fingerprints: BTreeMap::from([(
                    staged_id,
                    crate::dream::dream_output_fingerprint(&db.get(staged_id).unwrap().unwrap())
                        .unwrap(),
                )]),
                added_edges: Vec::new(),
                superseded: Vec::new(),
                explanation: "simulate interrupted staging".into(),
                status: DreamRunStatus::Pending,
                created_at_ms: crate::now_ms(),
                completed_at_ms: None,
                error: None,
            };
            db.runtime_store.put_dream_run(0, run_id, &run).unwrap();
        }

        let db = Database::open(dir.path()).unwrap();
        let staged = db.get(staged_id).unwrap().unwrap();
        assert_eq!(staged.state, MemoryState::Retracted);
        assert!(!staged.is_valid_at(crate::now_ms()));
        assert_eq!(
            db.dream_run(run_id).unwrap().unwrap().status,
            DreamRunStatus::Repaired
        );
        assert!(db
            .audit_records(AuditFilter {
                action: Some(AuditAction::Repair),
                ..AuditFilter::default()
            })
            .unwrap()
            .iter()
            .any(|record| record.name == "repair_dream"));
    }

    #[test]
    fn reopen_migrates_legacy_schema_without_rewriting_raw_nodes() {
        let dir = tempdir().unwrap();
        let node = NodeBuilder::new(NodeKind::Fact)
            .text("legacy")
            .embedding("old-model", vec![1.0, 0.0])
            .created_at(10)
            .build();
        let id = node.id;
        {
            let storage = mmdb_storage::Storage::open(dir.path()).unwrap();
            storage.put_node(&node).unwrap();
            let vectors = mmdb_vector::VectorStore::open(storage.keyspace.clone()).unwrap();
            vectors.insert(0, "old-model", id, &[1.0, 0.0]).unwrap();
            let key = mmdb_storage::keys::node_key(0, id);
            let value = storage.nodes.get(&key).unwrap().unwrap();
            let mut value: serde_json::Value = serde_json::from_slice(&value).unwrap();
            let object = value.as_object_mut().unwrap();
            object.remove("revision");
            object.remove("state");
            object.remove("valid_from_ms");
            object.remove("valid_to_ms");
            storage
                .nodes
                .insert(key, serde_json::to_vec(&value).unwrap())
                .unwrap();
            storage.keyspace.persist(PersistMode::SyncAll).unwrap();
        }
        let db = Database::open(dir.path()).unwrap();
        let node = db.get(id).unwrap().unwrap();
        assert_eq!(node.revision, 0);
        assert_eq!(node.state, MemoryState::Active);
        assert!(node.is_valid_at(10));
        assert!(db
            .memory_profile()
            .unwrap()
            .embedding_profiles
            .iter()
            .any(|profile| profile.id == "legacy:old-model"));

        let key = mmdb_storage::keys::node_key(0, id);
        let raw = db.storage.nodes.get(key).unwrap().unwrap();
        let raw: serde_json::Value = serde_json::from_slice(&raw).unwrap();
        assert!(raw.get("revision").is_none());
    }
}

#[test]
fn deleting_blob_node_releases_ref_and_gc_removes_bytes() {
    use std::io::Cursor;

    let dir = tempdir().unwrap();
    let db = Database::open(dir.path()).unwrap();
    let id = db
        .insert_blob(
            NodeKind::Artifact,
            Cursor::new(b"temporary payload".to_vec()),
            "text/plain",
        )
        .unwrap();
    let hash = match db.get(id).unwrap().unwrap().content {
        Content::Blob { hash, .. } => hash,
        _ => panic!("expected blob content"),
    };

    db.delete(id).unwrap();
    assert_eq!(db.blob_refcount(&hash).unwrap(), Some(0));
    assert_eq!(db.gc_blobs().unwrap(), 1);
    assert_eq!(db.blob_refcount(&hash).unwrap(), None);
    assert!(db.get_blob_stream(&hash).is_err());
}

#[test]
fn inserting_node_with_existing_blob_reference_increments_refcount() {
    use std::io::Cursor;

    let dir = tempdir().unwrap();
    let db = Database::open(dir.path()).unwrap();
    let first = db
        .insert_blob(
            NodeKind::Artifact,
            Cursor::new(b"shared payload".to_vec()),
            "text/plain",
        )
        .unwrap();
    let (hash, size, mime) = match db.get(first).unwrap().unwrap().content {
        Content::Blob {
            hash, size, mime, ..
        } => (hash, size, mime),
        _ => panic!("expected blob content"),
    };

    let second = db
        .insert(
            NodeBuilder::new(NodeKind::Artifact)
                .blob(hash, size, mime)
                .build(),
        )
        .unwrap();

    assert_eq!(db.blob_refcount(&hash).unwrap(), Some(2));
    db.delete(first).unwrap();
    assert_eq!(db.blob_refcount(&hash).unwrap(), Some(1));
    assert_eq!(db.gc_blobs().unwrap(), 0);
    assert!(db.get_blob_stream(&hash).is_ok());
    db.delete(second).unwrap();
    assert_eq!(db.blob_refcount(&hash).unwrap(), Some(0));
}

#[test]
fn inlined_small_blob_refcount_works_uniformly_and_get_shortcircuits() {
    use mmdb_blob::INLINE_THRESHOLD;
    use std::io::Cursor;

    let dir = tempdir().unwrap();
    let db = Database::open(dir.path()).unwrap();

    // A small (<=INLINE_THRESHOLD) payload — must be inlined into the node.
    let small = vec![7u8; 1024];
    let id = db
        .insert_blob(
            NodeKind::Artifact,
            Cursor::new(small.clone()),
            "application/octet-stream",
        )
        .unwrap();
    let node = db.get(id).unwrap().unwrap();
    match node.content {
        Content::Blob {
            hash,
            size,
            inline: Some(bytes),
            ..
        } => {
            assert_eq!(size as usize, small.len());
            assert_eq!(bytes, small);
            assert!(size as usize <= INLINE_THRESHOLD);
            // Refcount still tracked (uniform accounting) even though
            // the bytes are embedded in the node record.
            assert_eq!(db.blob_refcount(&hash).unwrap(), Some(1));
            // get_blob_stream_for short-circuits to the inlined bytes.
            let mut out = Vec::new();
            db.get_blob_stream_for(&hash, id)
                .unwrap()
                .read_to_end(&mut out)
                .unwrap();
            assert_eq!(out, small);
        }
        other => panic!("expected inlined Content::Blob, got {other:?}"),
    }

    // A large (>INLINE_THRESHOLD) payload — must NOT be inlined.
    let big = vec![9u8; INLINE_THRESHOLD + 1];
    let id2 = db
        .insert_blob(
            NodeKind::Artifact,
            Cursor::new(big.clone()),
            "application/octet-stream",
        )
        .unwrap();
    let node2 = db.get(id2).unwrap().unwrap();
    match node2.content {
        Content::Blob { inline, size, .. } => {
            assert!(inline.is_none());
            assert_eq!(size as usize, big.len());
        }
        other => panic!("expected on-disk Content::Blob, got {other:?}"),
    }
}

// Suppress unused warning for now_ms import
#[allow(dead_code)]
fn _unused_now_ms() {
    let _ = now_ms();
}

// Suppress unused warning for Hit import
#[allow(dead_code)]
fn _unused_hit(_h: Hit) {}
