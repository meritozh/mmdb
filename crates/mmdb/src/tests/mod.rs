use crate::builder::{now_ms, NodeBuilder};
use crate::db::Database;
use crate::embedder::{DatabaseConfig, Embedder, DEFAULT_MODEL, DEFAULT_TENANT, EmbedFuture};
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
    let db = Database::open_with_embedder(
        dir.path(),
        cfg,
        Box::new(HashEmbedder::new("hash-32", 32)),
    )
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
    let db = Database::open_with_embedder(
        dir.path(),
        cfg,
        Box::new(HashEmbedder::new("hash-32", 32)),
    )
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
    let db = Database::open_with_embedder(
        dir.path(),
        cfg,
        Box::new(HashEmbedder::new("hash-32", 32)),
    )
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

    let result = Database::open_with_embedder(
        dir.path(),
        cfg,
        Box::new(HashEmbedder::new("actual", 32)),
    );
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
    let db =
        Database::open_with_embedder(dir.path(), cfg, Box::new(AsyncOnlyEmbedder)).unwrap();

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
    let Content::Blob { hash, size, mime, inline } = node.content else {
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
        Content::Blob { hash, size, mime, .. } => (hash, size, mime),
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
        .insert_blob(NodeKind::Artifact, Cursor::new(small.clone()), "application/octet-stream")
        .unwrap();
    let node = db.get(id).unwrap().unwrap();
    match node.content {
        Content::Blob { hash, size, inline: Some(bytes), .. } => {
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
        .insert_blob(NodeKind::Artifact, Cursor::new(big.clone()), "application/octet-stream")
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
