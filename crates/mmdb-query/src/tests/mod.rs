use super::*;
use crate::executor::{materialize_operator, Record};
use crate::ir::{
    AggregateExpr, FieldRef, JoinKey, JoinOrderCandidate, JoinStrategy, Literal, LogicalPlan,
    ModelId, OrderedF32, Predicate, ScoreExpr, SortKey, Stats, TableId, VectorRef,
};
use mmdb_core::{NodeKind, Result};
use std::cell::RefCell;
use std::collections::BTreeMap;

#[test]
fn recall_builder_lowers_to_filtered_vector_topk() {
    let plan = Query::recall()
        .tenant(7)
        .filter(Predicate::kind_in([NodeKind::Episode, NodeKind::Fact]))
        .filter(Predicate::created_after_ms(1_000))
        .similar_to(vec![1.0, 0.0, 0.0])
        .using_model("text")
        .topk(20)
        .limit(5)
        .build();

    assert_eq!(
        plan,
        LogicalPlan::TopK {
            input: Box::new(LogicalPlan::VectorSearch {
                query: VectorRef::Vector(vec![1.0, 0.0, 0.0]),
                k: 20,
                filter: Some(Predicate::and([
                    Predicate::eq(FieldRef::Tenant, Literal::U32(7)),
                    Predicate::kind_in([NodeKind::Episode, NodeKind::Fact]),
                    Predicate::created_after_ms(1_000),
                ])),
                model: ModelId::from("text"),
            }),
            k: 5,
            by: SortKey::ScoreDesc,
        }
    );
}

#[test]
fn optimizer_pushes_filter_into_scan() {
    let plan = LogicalPlan::Filter {
        input: Box::new(LogicalPlan::Scan {
            table: TableId::Nodes,
            filter: Some(Predicate::eq(FieldRef::Tenant, Literal::U32(1))),
        }),
        pred: Predicate::kind_eq(NodeKind::Fact),
    };

    let optimized = Optimizer::default().optimize(plan);

    assert_eq!(
        optimized,
        LogicalPlan::Scan {
            table: TableId::Nodes,
            filter: Some(Predicate::and([
                Predicate::eq(FieldRef::Tenant, Literal::U32(1)),
                Predicate::kind_eq(NodeKind::Fact),
            ])),
        }
    );
}

#[test]
fn executor_expands_graph_scores_with_udf_and_topks() {
    let mut ctx = ExecutionContext {
        nodes: vec![
            Record::new("a", 1, NodeKind::Fact, 100).with_score(0.6),
            Record::new("b", 1, NodeKind::Fact, 110).with_score(0.4),
            Record::new("c", 1, NodeKind::Fact, 120).with_score(0.1),
            Record::new("x", 2, NodeKind::Fact, 100).with_score(1.0),
        ],
        edges: vec![EdgeRecord::new("a", "c", "related")],
        ..Default::default()
    };
    ctx.register_udf("boost", |record, _args| {
        if record.node_id == "c" {
            2.0
        } else {
            record.score
        }
    });

    let plan = LogicalPlan::TopK {
        input: Box::new(LogicalPlan::Udf {
            input: Box::new(LogicalPlan::GraphExpand {
                from: Box::new(LogicalPlan::Scan {
                    table: TableId::Nodes,
                    filter: Some(Predicate::and([
                        Predicate::eq(FieldRef::Tenant, Literal::U32(1)),
                        Predicate::kind_eq(NodeKind::Fact),
                    ])),
                }),
                relation: Some("related".to_string()),
                depth: 1,
            }),
            name: "boost".to_string(),
            args: vec![Expr::Field(FieldRef::Score)],
        }),
        k: 2,
        by: SortKey::ScoreDesc,
    };

    let rows = Executor::new(&ctx).execute(&plan).unwrap();

    assert_eq!(
        rows.iter().map(|r| r.node_id.as_str()).collect::<Vec<_>>(),
        vec!["c", "a"]
    );
    assert_eq!(rows[0].score, 2.0);
}

#[test]
fn cost_optimizer_places_selective_filter_before_graph_expand() {
    let plan = LogicalPlan::Filter {
        input: Box::new(LogicalPlan::GraphExpand {
            from: Box::new(LogicalPlan::Scan {
                table: TableId::Nodes,
                filter: None,
            }),
            relation: Some("related".to_string()),
            depth: 1,
        }),
        pred: Predicate::eq(FieldRef::Tenant, Literal::U32(1)),
    };

    let optimized = Optimizer::with_stats(Stats {
        node_rows: 1_000,
        estimated_filter_selectivity: 0.01,
        ..Default::default()
    })
    .optimize(plan);

    assert_eq!(
        optimized,
        LogicalPlan::GraphExpand {
            from: Box::new(LogicalPlan::Scan {
                table: TableId::Nodes,
                filter: Some(Predicate::eq(FieldRef::Tenant, Literal::U32(1))),
            }),
            relation: Some("related".to_string()),
            depth: 1,
        }
    );
}

#[test]
fn histograms_estimate_filter_selectivity() {
    let histogram = FieldHistogram::from_counts([
        (Literal::NodeKind(NodeKind::Fact), 10),
        (Literal::NodeKind(NodeKind::Episode), 30),
        (Literal::NodeKind(NodeKind::Entity), 60),
    ]);
    let stats = Stats::default().with_histogram(FieldRef::Kind, histogram);

    assert_eq!(
        stats.estimate_selectivity(&Predicate::kind_eq(NodeKind::Fact)),
        0.1
    );
    assert_eq!(
        stats.estimate_selectivity(&Predicate::kind_in([NodeKind::Fact, NodeKind::Episode,])),
        0.4
    );
}

#[test]
fn histogram_optimizer_pushes_selective_filter_before_graph_expand() {
    let plan = LogicalPlan::Filter {
        input: Box::new(LogicalPlan::GraphExpand {
            from: Box::new(LogicalPlan::Scan {
                table: TableId::Nodes,
                filter: None,
            }),
            relation: Some("related".to_string()),
            depth: 1,
        }),
        pred: Predicate::kind_eq(NodeKind::Fact),
    };

    let optimized = Optimizer::with_stats(Stats::default().with_histogram(
        FieldRef::Kind,
        FieldHistogram::from_counts([
            (Literal::NodeKind(NodeKind::Fact), 10),
            (Literal::NodeKind(NodeKind::Episode), 90),
        ]),
    ))
    .optimize(plan);

    assert_eq!(
        optimized,
        LogicalPlan::GraphExpand {
            from: Box::new(LogicalPlan::Scan {
                table: TableId::Nodes,
                filter: Some(Predicate::kind_eq(NodeKind::Fact)),
            }),
            relation: Some("related".to_string()),
            depth: 1,
        }
    );
}

#[test]
fn optimizer_costs_hash_vs_merge_join_strategy() {
    let optimizer = Optimizer::with_stats(Stats {
        node_rows: 10_000,
        ..Default::default()
    });

    assert_eq!(
        optimizer.choose_join_strategy(Some(40), Some(10_000), &JoinKey::NodeId),
        JoinStrategy::HashBuildLeft
    );
    assert_eq!(
        optimizer.choose_join_strategy(Some(10_000), Some(40), &JoinKey::NodeId),
        JoinStrategy::HashBuildRight
    );
    assert_eq!(
        optimizer.choose_join_strategy(Some(10_000), Some(10_000), &JoinKey::NodeId),
        JoinStrategy::Merge
    );
}

#[test]
fn optimizer_enumerates_join_order_candidates_by_estimated_cost() {
    let fact_scan = LogicalPlan::Scan {
        table: TableId::Nodes,
        filter: Some(Predicate::kind_eq(NodeKind::Fact)),
    };
    let episode_scan = LogicalPlan::Scan {
        table: TableId::Nodes,
        filter: Some(Predicate::kind_eq(NodeKind::Episode)),
    };
    let entity_scan = LogicalPlan::Scan {
        table: TableId::Nodes,
        filter: Some(Predicate::kind_eq(NodeKind::Entity)),
    };
    let plan = LogicalPlan::Join {
        left: Box::new(LogicalPlan::Join {
            left: Box::new(fact_scan),
            right: Box::new(episode_scan),
            on: JoinKey::NodeId,
        }),
        right: Box::new(entity_scan),
        on: JoinKey::NodeId,
    };
    let stats = Stats {
        node_rows: 1_000,
        ..Default::default()
    }
    .with_histogram(
        FieldRef::Kind,
        FieldHistogram::from_counts([
            (Literal::NodeKind(NodeKind::Fact), 900),
            (Literal::NodeKind(NodeKind::Episode), 90),
            (Literal::NodeKind(NodeKind::Entity), 10),
        ]),
    );

    let candidates = Optimizer::with_stats(stats).join_order_candidates(&plan);

    assert_eq!(candidates.len(), 6);
    assert_eq!(candidates[0].leaf_order, vec![2, 1, 0]);
    assert_eq!(
        candidates[0].leaf_estimates,
        vec![Some(10), Some(90), Some(900)]
    );
    assert!(candidates[0].estimated_cost < candidates.last().unwrap().estimated_cost);
}

#[test]
fn optimizer_applies_join_order_for_scan_only_node_id_chain() {
    let fact_scan = LogicalPlan::Scan {
        table: TableId::Nodes,
        filter: Some(Predicate::kind_eq(NodeKind::Fact)),
    };
    let episode_scan = LogicalPlan::Scan {
        table: TableId::Nodes,
        filter: Some(Predicate::kind_eq(NodeKind::Episode)),
    };
    let entity_scan = LogicalPlan::Scan {
        table: TableId::Nodes,
        filter: Some(Predicate::kind_eq(NodeKind::Entity)),
    };
    let plan = LogicalPlan::Join {
        left: Box::new(LogicalPlan::Join {
            left: Box::new(fact_scan.clone()),
            right: Box::new(episode_scan.clone()),
            on: JoinKey::NodeId,
        }),
        right: Box::new(entity_scan.clone()),
        on: JoinKey::NodeId,
    };
    let stats = Stats {
        node_rows: 1_000,
        ..Default::default()
    }
    .with_histogram(
        FieldRef::Kind,
        FieldHistogram::from_counts([
            (Literal::NodeKind(NodeKind::Fact), 900),
            (Literal::NodeKind(NodeKind::Episode), 90),
            (Literal::NodeKind(NodeKind::Entity), 10),
        ]),
    );

    let optimized = Optimizer::with_stats(stats).optimize(plan);

    assert_eq!(
        optimized,
        LogicalPlan::Join {
            left: Box::new(LogicalPlan::Join {
                left: Box::new(entity_scan),
                right: Box::new(episode_scan),
                on: JoinKey::NodeId,
            }),
            right: Box::new(fact_scan),
            on: JoinKey::NodeId,
        }
    );
}

#[test]
fn optimizer_reorders_scan_filters_after_score_preserving_vector_leaf() {
    let vector = LogicalPlan::VectorSearch {
        query: VectorRef::Vector(vec![1.0, 0.0]),
        k: 10,
        filter: Some(Predicate::kind_eq(NodeKind::Fact)),
        model: ModelId::from("text"),
    };
    let episode_scan = LogicalPlan::Scan {
        table: TableId::Nodes,
        filter: Some(Predicate::kind_eq(NodeKind::Episode)),
    };
    let entity_scan = LogicalPlan::Scan {
        table: TableId::Nodes,
        filter: Some(Predicate::kind_eq(NodeKind::Entity)),
    };
    let plan = LogicalPlan::Join {
        left: Box::new(LogicalPlan::Join {
            left: Box::new(vector),
            right: Box::new(episode_scan.clone()),
            on: JoinKey::NodeId,
        }),
        right: Box::new(entity_scan.clone()),
        on: JoinKey::NodeId,
    };
    let stats = Stats {
        node_rows: 1_000,
        ..Default::default()
    }
    .with_histogram(
        FieldRef::Kind,
        FieldHistogram::from_counts([
            (Literal::NodeKind(NodeKind::Fact), 900),
            (Literal::NodeKind(NodeKind::Episode), 90),
            (Literal::NodeKind(NodeKind::Entity), 10),
        ]),
    );

    let optimized = Optimizer::with_stats(stats).optimize(plan);

    assert_eq!(
        optimized,
        LogicalPlan::Join {
            left: Box::new(LogicalPlan::Join {
                left: Box::new(LogicalPlan::VectorSearch {
                    query: VectorRef::Vector(vec![1.0, 0.0]),
                    k: 10,
                    filter: Some(Predicate::kind_eq(NodeKind::Fact)),
                    model: ModelId::from("text"),
                }),
                right: Box::new(entity_scan),
                on: JoinKey::NodeId,
            }),
            right: Box::new(episode_scan),
            on: JoinKey::NodeId,
        }
    );
}

#[test]
fn physical_scan_filter_batches_records() {
    let ctx = ExecutionContext {
        nodes: vec![
            Record::new("a", 1, NodeKind::Fact, 100),
            Record::new("b", 2, NodeKind::Fact, 110),
            Record::new("c", 1, NodeKind::Episode, 120),
            Record::new("d", 1, NodeKind::Entity, 130),
        ],
        ..Default::default()
    };
    let plan = LogicalPlan::Filter {
        input: Box::new(LogicalPlan::Scan {
            table: TableId::Nodes,
            filter: None,
        }),
        pred: Predicate::eq(FieldRef::Tenant, Literal::U32(1)),
    };

    let executor = Executor::new(&ctx);
    let mut op = executor.compile(&plan, 2).unwrap();

    let first = op.next_batch().unwrap().unwrap();
    assert_eq!(
        first
            .rows
            .iter()
            .map(|row| row.node_id.as_str())
            .collect::<Vec<_>>(),
        vec!["a", "c"]
    );
    let second = op.next_batch().unwrap().unwrap();
    assert_eq!(
        second
            .rows
            .iter()
            .map(|row| row.node_id.as_str())
            .collect::<Vec<_>>(),
        vec!["d"]
    );
    assert!(op.next_batch().unwrap().is_none());
}

#[test]
fn executor_projects_requested_fields() {
    let ctx = ExecutionContext {
        nodes: vec![Record::new("a", 1, NodeKind::Fact, 100)
            .with_score(0.75)
            .with_field("topic", Literal::String("alpha".to_string()))
            .with_field("unused", Literal::Bool(true))],
        ..Default::default()
    };
    let plan = LogicalPlan::Project {
        input: Box::new(LogicalPlan::Scan {
            table: TableId::Nodes,
            filter: None,
        }),
        fields: vec![
            FieldRef::NodeId,
            FieldRef::Metadata("topic".to_string()),
            FieldRef::Score,
        ],
    };
    let expected_fields = BTreeMap::from([
        ("node_id".to_string(), Literal::String("a".to_string())),
        ("topic".to_string(), Literal::String("alpha".to_string())),
        (
            "score".to_string(),
            Literal::F32(OrderedF32(0.75)),
        ),
    ]);

    let executor = Executor::new(&ctx);
    let rows = executor.execute(&plan).unwrap();
    let mut op = executor.compile(&plan, 1).unwrap();
    let physical_rows = collect_physical(&mut *op).unwrap();

    assert_eq!(rows[0].fields, expected_fields);
    assert_eq!(physical_rows[0].fields, expected_fields);
}

#[test]
fn topk_sorts_by_requested_field() {
    let ctx = ExecutionContext {
        nodes: vec![
            Record::new("slow", 1, NodeKind::Fact, 100)
                .with_score(10.0)
                .with_field("rank", Literal::I64(3)),
            Record::new("fast", 1, NodeKind::Fact, 110)
                .with_score(1.0)
                .with_field("rank", Literal::I64(1)),
            Record::new("middle", 1, NodeKind::Fact, 120)
                .with_score(5.0)
                .with_field("rank", Literal::I64(2)),
        ],
        ..Default::default()
    };
    let plan = LogicalPlan::TopK {
        input: Box::new(LogicalPlan::Scan {
            table: TableId::Nodes,
            filter: None,
        }),
        k: 2,
        by: SortKey::Field(FieldRef::Metadata("rank".to_string())),
    };

    let rows = Executor::new(&ctx).execute(&plan).unwrap();

    assert_eq!(
        rows.iter()
            .map(|row| row.node_id.as_str())
            .collect::<Vec<_>>(),
        vec!["fast", "middle"]
    );
}

#[test]
fn executor_filters_updated_at_field() {
    let ctx = ExecutionContext {
        nodes: vec![
            Record::new("old", 1, NodeKind::Fact, 100).with_updated_at_ms(200),
            Record::new("fresh", 1, NodeKind::Fact, 100).with_updated_at_ms(900),
        ],
        ..Default::default()
    };
    let plan = LogicalPlan::Scan {
        table: TableId::Nodes,
        filter: Some(Predicate::Gte(FieldRef::UpdatedAtMs, Literal::I64(800))),
    };

    let executor = Executor::new(&ctx);
    let rows = executor.execute(&plan).unwrap();
    let mut op = executor.compile(&plan, 1).unwrap();
    let physical_rows = collect_physical(&mut *op).unwrap();

    assert_eq!(
        rows.iter()
            .map(|row| row.node_id.as_str())
            .collect::<Vec<_>>(),
        vec!["fresh"]
    );
    assert_eq!(physical_rows, rows);
}

#[test]
fn executor_counts_rows_grouped_by_kind() {
    let ctx = ExecutionContext {
        nodes: vec![
            Record::new("a", 1, NodeKind::Fact, 100),
            Record::new("b", 1, NodeKind::Fact, 110),
            Record::new("c", 1, NodeKind::Episode, 120),
        ],
        ..Default::default()
    };
    let plan = LogicalPlan::Aggregate {
        input: Box::new(LogicalPlan::Scan {
            table: TableId::Nodes,
            filter: None,
        }),
        group_by: vec![FieldRef::Kind],
        aggregate: AggregateExpr::Count,
    };

    let executor = Executor::new(&ctx);
    let rows = executor.execute(&plan).unwrap();
    let mut op = executor.compile(&plan, 1).unwrap();
    let physical_rows = collect_physical(&mut *op).unwrap();

    assert_eq!(physical_rows, rows);
    assert_eq!(rows.len(), 2);
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
fn physical_graph_udf_topk_matches_recursive_executor() {
    let mut ctx = ExecutionContext {
        nodes: vec![
            Record::new("a", 1, NodeKind::Fact, 100).with_score(0.6),
            Record::new("b", 1, NodeKind::Fact, 110).with_score(0.4),
            Record::new("c", 1, NodeKind::Fact, 120).with_score(0.1),
            Record::new("x", 2, NodeKind::Fact, 100).with_score(1.0),
        ],
        edges: vec![EdgeRecord::new("a", "c", "related")],
        ..Default::default()
    };
    ctx.register_udf("boost", |record, _args| {
        if record.node_id == "c" {
            2.0
        } else {
            record.score
        }
    });
    let plan = LogicalPlan::TopK {
        input: Box::new(LogicalPlan::Udf {
            input: Box::new(LogicalPlan::GraphExpand {
                from: Box::new(LogicalPlan::Scan {
                    table: TableId::Nodes,
                    filter: Some(Predicate::eq(FieldRef::Tenant, Literal::U32(1))),
                }),
                relation: Some("related".to_string()),
                depth: 1,
            }),
            name: "boost".to_string(),
            args: vec![Expr::Field(FieldRef::Score)],
        }),
        k: 2,
        by: SortKey::ScoreDesc,
    };

    let executor = Executor::new(&ctx);
    let recursive_rows = executor.execute(&plan).unwrap();
    let mut op = executor.compile(&plan, 1).unwrap();
    let physical_rows = collect_physical(&mut *op).unwrap();

    assert_eq!(physical_rows, recursive_rows);
    assert_eq!(
        physical_rows
            .iter()
            .map(|row| row.node_id.as_str())
            .collect::<Vec<_>>(),
        vec!["c", "a"]
    );
}

#[test]
fn explain_reports_physical_plan_estimates_and_actual_rows() {
    let ctx = ExecutionContext {
        nodes: vec![
            Record::new("a", 1, NodeKind::Fact, 100),
            Record::new("b", 2, NodeKind::Fact, 110),
            Record::new("c", 1, NodeKind::Episode, 120),
            Record::new("d", 1, NodeKind::Entity, 130),
        ],
        ..Default::default()
    };
    let plan = LogicalPlan::TopK {
        input: Box::new(LogicalPlan::Filter {
            input: Box::new(LogicalPlan::Scan {
                table: TableId::Nodes,
                filter: None,
            }),
            pred: Predicate::eq(FieldRef::Tenant, Literal::U32(1)),
        }),
        k: 2,
        by: SortKey::CreatedAtDesc,
    };
    let stats = Stats {
        node_rows: 4,
        estimated_filter_selectivity: 0.5,
        ..Default::default()
    };

    let explain = Executor::new(&ctx).explain(&plan, &stats).unwrap();

    assert_eq!(explain.operator, "TopKOp");
    assert_eq!(explain.estimated_rows, Some(2));
    assert_eq!(explain.actual_rows, Some(2));
    assert_eq!(explain.children[0].operator, "FilterOp");
    assert_eq!(explain.children[0].estimated_rows, Some(2));
    assert_eq!(explain.children[0].actual_rows, Some(3));
    assert_eq!(explain.children[0].children[0].operator, "ScanOp");
    assert_eq!(explain.children[0].children[0].estimated_rows, Some(4));
    assert_eq!(explain.children[0].children[0].actual_rows, Some(4));
}

#[test]
fn explain_uses_histograms_for_filter_estimates() {
    let ctx = ExecutionContext {
        nodes: vec![
            Record::new("a", 1, NodeKind::Fact, 100),
            Record::new("b", 1, NodeKind::Fact, 110),
            Record::new("c", 1, NodeKind::Episode, 120),
        ],
        ..Default::default()
    };
    let plan = LogicalPlan::Filter {
        input: Box::new(LogicalPlan::Scan {
            table: TableId::Nodes,
            filter: None,
        }),
        pred: Predicate::kind_eq(NodeKind::Fact),
    };
    let stats = Stats {
        node_rows: 100,
        ..Default::default()
    }
    .with_histogram(
        FieldRef::Kind,
        FieldHistogram::from_counts([
            (Literal::NodeKind(NodeKind::Fact), 25),
            (Literal::NodeKind(NodeKind::Episode), 75),
        ]),
    );

    let explain = Executor::new(&ctx).explain(&plan, &stats).unwrap();

    assert_eq!(explain.operator, "FilterOp");
    assert_eq!(explain.estimated_rows, Some(25));
    assert_eq!(explain.actual_rows, Some(2));
}

#[test]
fn explain_uses_costed_merge_join_operator_name() {
    let ctx = ExecutionContext {
        nodes: vec![
            Record::new("a", 1, NodeKind::Fact, 100),
            Record::new("b", 1, NodeKind::Fact, 110),
        ],
        ..Default::default()
    };
    let plan = LogicalPlan::Join {
        left: Box::new(LogicalPlan::Scan {
            table: TableId::Nodes,
            filter: None,
        }),
        right: Box::new(LogicalPlan::Scan {
            table: TableId::Nodes,
            filter: None,
        }),
        on: JoinKey::NodeId,
    };
    let stats = Stats {
        node_rows: 10_000,
        ..Default::default()
    };

    let explain = Executor::new(&ctx).explain(&plan, &stats).unwrap();

    assert_eq!(explain.operator, "MergeJoinOp");
    assert_eq!(explain.estimated_rows, Some(10_000));
    assert_eq!(explain.actual_rows, Some(2));
}

#[test]
fn source_executor_uses_range_scan_source_operator() {
    let source = RecordingSource {
        range_rows: vec![
            Record::new("a", 1, NodeKind::Fact, 100),
            Record::new("b", 1, NodeKind::Episode, 110),
        ],
        ..Default::default()
    };
    let plan = LogicalPlan::Scan {
        table: TableId::Nodes,
        filter: Some(Predicate::kind_eq(NodeKind::Fact)),
    };

    let mut op = SourceExecutor::new(&source).compile(&plan, 1).unwrap();
    let rows = collect_physical(&mut *op).unwrap();

    assert_eq!(
        rows.iter()
            .map(|row| row.node_id.as_str())
            .collect::<Vec<_>>(),
        vec!["a"]
    );
    assert_eq!(source.calls.borrow().as_slice(), ["range_scan"]);
}

#[test]
fn source_executor_uses_hnsw_and_graph_source_operators() {
    let source = RecordingSource {
        vector_rows: vec![Record::new("seed", 1, NodeKind::Fact, 100).with_score(0.9)],
        graph_rows: vec![
            Record::new("seed", 1, NodeKind::Fact, 100).with_score(0.9),
            Record::new("related", 1, NodeKind::Fact, 110).with_score(0.1),
        ],
        ..Default::default()
    };
    let plan = LogicalPlan::TopK {
        input: Box::new(LogicalPlan::GraphExpand {
            from: Box::new(LogicalPlan::VectorSearch {
                query: VectorRef::Vector(vec![1.0, 0.0]),
                k: 1,
                filter: None,
                model: ModelId::from("text"),
            }),
            relation: Some("related".to_string()),
            depth: 1,
        }),
        k: 2,
        by: SortKey::ScoreDesc,
    };

    let mut op = SourceExecutor::new(&source).compile(&plan, 4).unwrap();
    let rows = collect_physical(&mut *op).unwrap();

    assert_eq!(
        rows.iter()
            .map(|row| row.node_id.as_str())
            .collect::<Vec<_>>(),
        vec!["seed", "related"]
    );
    assert_eq!(
        source.calls.borrow().as_slice(),
        ["hnsw_search", "graph_expand"]
    );
}

#[test]
fn source_explain_uses_operator_instrumentation_without_reexecuting_sources() {
    let source = RecordingSource {
        range_rows: vec![
            Record::new("a", 1, NodeKind::Fact, 100),
            Record::new("b", 2, NodeKind::Fact, 110),
            Record::new("c", 1, NodeKind::Episode, 120),
        ],
        ..Default::default()
    };
    let plan = LogicalPlan::TopK {
        input: Box::new(LogicalPlan::Filter {
            input: Box::new(LogicalPlan::Scan {
                table: TableId::Nodes,
                filter: None,
            }),
            pred: Predicate::eq(FieldRef::Tenant, Literal::U32(1)),
        }),
        k: 1,
        by: SortKey::CreatedAtDesc,
    };
    let stats = Stats {
        node_rows: 3,
        estimated_filter_selectivity: 0.5,
        ..Default::default()
    };

    let explain = SourceExecutor::new(&source)
        .explain(&plan, &stats, 2)
        .unwrap();

    assert_eq!(source.calls.borrow().as_slice(), ["range_scan"]);
    assert_eq!(explain.operator, "TopKOp");
    assert_eq!(explain.estimated_rows, Some(1));
    assert_eq!(explain.actual_rows, Some(1));
    assert_eq!(explain.children[0].operator, "FilterOp");
    assert_eq!(explain.children[0].actual_rows, Some(2));
    assert_eq!(explain.children[0].children[0].operator, "RangeScanOp");
    assert_eq!(explain.children[0].children[0].actual_rows, Some(3));
}

#[derive(Default)]
struct RecordingSource {
    range_rows: Vec<Record>,
    vector_rows: Vec<Record>,
    graph_rows: Vec<Record>,
    calls: RefCell<Vec<&'static str>>,
}

impl QuerySource for RecordingSource {
    fn range_scan(&self, _table: &TableId, filter: Option<&Predicate>) -> Result<Vec<Record>> {
        self.calls.borrow_mut().push("range_scan");
        Ok(self
            .range_rows
            .iter()
            .filter(|record| {
                filter
                    .map(|pred| {
                        crate::eval::predicate_matches(record, pred)
                    })
                    .unwrap_or(true)
            })
            .cloned()
            .collect())
    }

    fn hnsw_search(
        &self,
        _query: &VectorRef,
        _model: &ModelId,
        _k: usize,
        _filter: Option<&Predicate>,
    ) -> Result<Vec<Record>> {
        self.calls.borrow_mut().push("hnsw_search");
        Ok(self.vector_rows.clone())
    }

    fn graph_expand(
        &self,
        _seeds: Vec<Record>,
        _relation: Option<&str>,
        _depth: u8,
    ) -> Result<Vec<Record>> {
        self.calls.borrow_mut().push("graph_expand");
        Ok(self.graph_rows.clone())
    }
}

fn collect_physical(op: &mut dyn PhysicalOperator) -> Result<Vec<Record>> {
    materialize_operator(op)
}

// Suppress unused imports/variables for things used only in some test paths
#[allow(dead_code)]
fn _join_order_candidate_type(_: JoinOrderCandidate) {}
#[allow(dead_code)]
fn _score_expr_type(_: ScoreExpr) {}
