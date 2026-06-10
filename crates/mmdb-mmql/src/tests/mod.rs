use super::*;
use crate::parser::duration_ms;
use crate::util::current_time_ms;
use mmdb_core::NodeKind;
use mmdb_query::{FieldRef, Literal, LogicalPlan, ModelId, Predicate, SortKey, VectorRef};

#[test]
fn parses_recall_vector_query() {
    let plan = parse(
        r#"
        recall n: Node
          where n.tenant = 7 and n.kind in (Episode, Fact)
          similar to [1.0, 0.0, 0.0] using model "text" topk 20
          limit 5
        "#,
    )
    .unwrap();

    assert_eq!(
        plan,
        LogicalPlan::TopK {
            input: Box::new(LogicalPlan::VectorSearch {
                query: VectorRef::Vector(vec![1.0, 0.0, 0.0]),
                k: 20,
                filter: Some(Predicate::and([
                    Predicate::eq(FieldRef::Tenant, Literal::U32(7)),
                    Predicate::kind_in([NodeKind::Episode, NodeKind::Fact]),
                ])),
                model: ModelId::from("text"),
            }),
            k: 5,
            by: SortKey::ScoreDesc,
        }
    );
}

#[test]
fn parses_embed_text_query() {
    let plan = parse(
        r#"
        recall n: Node
          where n.tenant = 7 and n.kind in (Fact)
          similar to embed("quant backtest") using model "text" topk 20
          limit 5
        "#,
    )
    .unwrap();

    assert_eq!(
        plan,
        LogicalPlan::TopK {
            input: Box::new(LogicalPlan::VectorSearch {
                query: VectorRef::Text("quant backtest".to_string()),
                k: 20,
                filter: Some(Predicate::and([
                    Predicate::eq(FieldRef::Tenant, Literal::U32(7)),
                    Predicate::kind_in([NodeKind::Fact]),
                ])),
                model: ModelId::from("text"),
            }),
            k: 5,
            by: SortKey::ScoreDesc,
        }
    );
}

#[test]
fn parses_relative_created_at_predicate() {
    let before = current_time_ms() - duration_ms("7d").unwrap();
    let plan = parse(
        r#"
        recall n: Node
          where n.tenant = 7 and n.created_at > now() - 7d and n.kind in (Fact)
          similar to [1.0, 0.0, 0.0] using model "text" topk 20
          limit 5
        "#,
    )
    .unwrap();
    let after = current_time_ms() - duration_ms("7d").unwrap();

    let LogicalPlan::TopK { input, .. } = plan else {
        panic!("expected topk");
    };
    let LogicalPlan::VectorSearch {
        filter: Some(Predicate::And(predicates)),
        ..
    } = *input
    else {
        panic!("expected vector search with filter");
    };
    let cutoff = predicates
        .iter()
        .find_map(|predicate| match predicate {
            Predicate::Gt(FieldRef::CreatedAtMs, Literal::I64(value)) => Some(*value),
            _ => None,
        })
        .expect("created_at cutoff");

    assert!((before..=after).contains(&cutoff));
}

#[test]
fn parses_created_at_where_predicates() {
    let plan = parse(
        r#"
        recall n: Node
          where n.tenant = 7 and n.created_at > 1000 and n.created_at <= 5000 and n.kind in (Fact)
          similar to [1.0, 0.0, 0.0] using model "text" topk 20
          limit 5
        "#,
    )
    .unwrap();

    assert_eq!(
        plan,
        LogicalPlan::TopK {
            input: Box::new(LogicalPlan::VectorSearch {
                query: VectorRef::Vector(vec![1.0, 0.0, 0.0]),
                k: 20,
                filter: Some(Predicate::and([
                    Predicate::eq(FieldRef::Tenant, Literal::U32(7)),
                    Predicate::kind_in([NodeKind::Fact]),
                    Predicate::Gt(FieldRef::CreatedAtMs, Literal::I64(1000)),
                    Predicate::Lte(FieldRef::CreatedAtMs, Literal::I64(5000)),
                ])),
                model: ModelId::from("text"),
            }),
            k: 5,
            by: SortKey::ScoreDesc,
        }
    );
}

#[test]
fn parses_metadata_string_equality_predicates() {
    let plan = parse(
        r#"
        recall n: Node
          where n.tenant = 7 and n.name = "X" and n.kind in (Fact)
          similar to [1.0, 0.0, 0.0] using model "text" topk 20
          limit 5
        "#,
    )
    .unwrap();

    assert_eq!(
        plan,
        LogicalPlan::TopK {
            input: Box::new(LogicalPlan::VectorSearch {
                query: VectorRef::Vector(vec![1.0, 0.0, 0.0]),
                k: 20,
                filter: Some(Predicate::and([
                    Predicate::eq(FieldRef::Tenant, Literal::U32(7)),
                    Predicate::kind_in([NodeKind::Fact]),
                    Predicate::eq(
                        FieldRef::Metadata("name".to_string()),
                        Literal::String("X".to_string()),
                    ),
                ])),
                model: ModelId::from("text"),
            }),
            k: 5,
            by: SortKey::ScoreDesc,
        }
    );
}

#[test]
fn parses_boolean_where_predicate_expression() {
    let plan = parse(
        r#"
        recall n: Node
          where n.tenant = 7 and (n.kind in (Fact) or n.topic = "revenue") and not n.archived = "true" and n.created_at >= 1000
          similar to [1.0, 0.0, 0.0] using model "text" topk 20
          limit 5
        "#,
    )
    .unwrap();

    assert_eq!(
        plan,
        LogicalPlan::TopK {
            input: Box::new(LogicalPlan::VectorSearch {
                query: VectorRef::Vector(vec![1.0, 0.0, 0.0]),
                k: 20,
                filter: Some(Predicate::and([
                    Predicate::eq(FieldRef::Tenant, Literal::U32(7)),
                    Predicate::Or(vec![
                        Predicate::kind_in([NodeKind::Fact]),
                        Predicate::eq(
                            FieldRef::Metadata("topic".to_string()),
                            Literal::String("revenue".to_string()),
                        ),
                    ]),
                    Predicate::Not(Box::new(Predicate::eq(
                        FieldRef::Metadata("archived".to_string()),
                        Literal::String("true".to_string()),
                    ))),
                    Predicate::Gte(FieldRef::CreatedAtMs, Literal::I64(1000)),
                ])),
                model: ModelId::from("text"),
            }),
            k: 5,
            by: SortKey::ScoreDesc,
        }
    );
}

#[test]
fn parses_graph_expand_and_udf_score_clause() {
    let plan = parse(
        r#"
        recall n: Node
          where n.tenant = 7 and n.kind in (Fact)
          similar to [1.0, 0.0, 0.0] using model "text" topk 20
          connected via related depth 2
          score by udf "boost"
          limit 5
        "#,
    )
    .unwrap();

    assert_eq!(
        plan,
        LogicalPlan::TopK {
            input: Box::new(LogicalPlan::Udf {
                input: Box::new(LogicalPlan::GraphExpand {
                    from: Box::new(LogicalPlan::VectorSearch {
                        query: VectorRef::Vector(vec![1.0, 0.0, 0.0]),
                        k: 20,
                        filter: Some(Predicate::and([
                            Predicate::eq(FieldRef::Tenant, Literal::U32(7)),
                            Predicate::kind_in([NodeKind::Fact]),
                        ])),
                        model: ModelId::from("text"),
                    }),
                    relation: Some("related".to_string()),
                    depth: 2,
                }),
                name: "boost".to_string(),
                args: vec![mmdb_query::Expr::Field(FieldRef::Score)],
            }),
            k: 5,
            by: SortKey::ScoreDesc,
        }
    );
}

#[test]
fn parses_connected_from_subquery_as_graph_filter() {
    let plan = parse(
        r#"
        recall n: Node
          where n.tenant = 7 and n.kind in (Fact)
          similar to [1.0, 0.0, 0.0] using model "text" topk 20
          connected from (u: Node where u.kind in (Entity) and u.name = "X") via mentions depth 1
          limit 5
        "#,
    )
    .unwrap();

    assert_eq!(
        plan,
        LogicalPlan::TopK {
            input: Box::new(LogicalPlan::Join {
                left: Box::new(LogicalPlan::VectorSearch {
                    query: VectorRef::Vector(vec![1.0, 0.0, 0.0]),
                    k: 20,
                    filter: Some(Predicate::and([
                        Predicate::eq(FieldRef::Tenant, Literal::U32(7)),
                        Predicate::kind_in([NodeKind::Fact]),
                    ])),
                    model: ModelId::from("text"),
                }),
                right: Box::new(LogicalPlan::GraphExpand {
                    from: Box::new(LogicalPlan::Scan {
                        table: mmdb_query::TableId::Nodes,
                        filter: Some(Predicate::and([
                            Predicate::eq(FieldRef::Tenant, Literal::U32(7)),
                            Predicate::kind_in([NodeKind::Entity]),
                            Predicate::eq(
                                FieldRef::Metadata("name".to_string()),
                                Literal::String("X".to_string()),
                            ),
                        ])),
                    }),
                    relation: Some("mentions".to_string()),
                    depth: 1,
                }),
                on: mmdb_query::JoinKey::NodeId,
            }),
            k: 5,
            by: SortKey::ScoreDesc,
        }
    );
}

#[test]
fn parse_ast_preserves_recall_clauses_before_lowering() {
    let query = parse_ast(
        r#"
        recall n: Node
          where n.tenant = 7 and n.kind in (Episode, Fact)
          similar to [1.0, 0.0, 0.0] using model "text" topk 20
          connected via related depth 2
          score by udf "boost"
          limit 5
        "#,
    )
    .unwrap();

    assert_eq!(
        query,
        RecallQuery {
            tenant: 7,
            kinds: vec![NodeKind::Episode, NodeKind::Fact],
            created_at_predicates: Vec::new(),
            metadata_predicates: Vec::new(),
            where_predicate: None,
            query: VectorRef::Vector(vec![1.0, 0.0, 0.0]),
            model: "text".to_string(),
            topk: 20,
            limit: 5,
            graph: Some(GraphClause {
                relation: "related".to_string(),
                depth: 2,
                from: None,
            }),
            udf: Some(UdfClause {
                name: "boost".to_string(),
            }),
            score: None,
            joins: Vec::new(),
            aggregate: None,
            return_fields: Vec::new(),
        }
    );
}

#[test]
fn parses_similarity_decay_score_expression() {
    let plan = parse(
        r#"
        recall n: Node
          where n.tenant = 7 and n.kind in (Fact)
          similar to [1.0, 0.0, 0.0] using model "text" topk 20
          score by similarity * decay(n.created_at, half_life = 3d)
          limit 5
        "#,
    )
    .unwrap();

    assert_eq!(
        plan,
        LogicalPlan::TopK {
            input: Box::new(LogicalPlan::Score {
                input: Box::new(LogicalPlan::VectorSearch {
                    query: VectorRef::Vector(vec![1.0, 0.0, 0.0]),
                    k: 20,
                    filter: Some(Predicate::and([
                        Predicate::eq(FieldRef::Tenant, Literal::U32(7)),
                        Predicate::kind_in([NodeKind::Fact]),
                    ])),
                    model: ModelId::from("text"),
                }),
                expr: mmdb_query::ScoreExpr::Mul(
                    Box::new(mmdb_query::ScoreExpr::Similarity),
                    Box::new(mmdb_query::ScoreExpr::Decay {
                        field: FieldRef::CreatedAtMs,
                        half_life_ms: 3 * 24 * 60 * 60 * 1_000,
                    }),
                ),
            }),
            k: 5,
            by: SortKey::ScoreDesc,
        }
    );
}

#[test]
fn parses_parenthesized_score_expression_with_literal() {
    let plan = parse(
        r#"
        recall n: Node
          where n.tenant = 7 and n.kind in (Fact)
          similar to [1.0, 0.0, 0.0] using model "text" topk 20
          score by (similarity + 0.25) * decay(n.created_at, half_life = 3d)
          limit 5
        "#,
    )
    .unwrap();

    assert_eq!(
        plan,
        LogicalPlan::TopK {
            input: Box::new(LogicalPlan::Score {
                input: Box::new(LogicalPlan::VectorSearch {
                    query: VectorRef::Vector(vec![1.0, 0.0, 0.0]),
                    k: 20,
                    filter: Some(Predicate::and([
                        Predicate::eq(FieldRef::Tenant, Literal::U32(7)),
                        Predicate::kind_in([NodeKind::Fact]),
                    ])),
                    model: ModelId::from("text"),
                }),
                expr: mmdb_query::ScoreExpr::Mul(
                    Box::new(mmdb_query::ScoreExpr::Add(
                        Box::new(mmdb_query::ScoreExpr::Similarity),
                        Box::new(mmdb_query::ScoreExpr::Literal(0.25)),
                    )),
                    Box::new(mmdb_query::ScoreExpr::Decay {
                        field: FieldRef::CreatedAtMs,
                        half_life_ms: 3 * 24 * 60 * 60 * 1_000,
                    }),
                ),
            }),
            k: 5,
            by: SortKey::ScoreDesc,
        }
    );
}

#[test]
fn parses_count_by_kind_aggregation() {
    let plan = parse(
        r#"
        recall n: Node
          where n.tenant = 7 and n.kind in (Fact, Episode)
          similar to [1.0, 0.0, 0.0] using model "text" topk 20
          limit 5
          count by kind
        "#,
    )
    .unwrap();

    assert_eq!(
        plan,
        LogicalPlan::Aggregate {
            input: Box::new(LogicalPlan::TopK {
                input: Box::new(LogicalPlan::VectorSearch {
                    query: VectorRef::Vector(vec![1.0, 0.0, 0.0]),
                    k: 20,
                    filter: Some(Predicate::and([
                        Predicate::eq(FieldRef::Tenant, Literal::U32(7)),
                        Predicate::kind_in([NodeKind::Fact, NodeKind::Episode]),
                    ])),
                    model: ModelId::from("text"),
                }),
                k: 5,
                by: SortKey::ScoreDesc,
            }),
            group_by: vec![FieldRef::Kind],
            aggregate: mmdb_query::AggregateExpr::Count,
        }
    );
}

#[test]
fn parses_join_clause_on_node_id() {
    let plan = parse(
        r#"
        recall n: Node
          where n.tenant = 7 and n.kind in (Fact, Episode)
          similar to [1.0, 0.0, 0.0] using model "text" topk 20
          limit 5
          join m: Node where m.kind in (Fact) on node_id
        "#,
    )
    .unwrap();

    assert_eq!(
        plan,
        LogicalPlan::Join {
            left: Box::new(LogicalPlan::TopK {
                input: Box::new(LogicalPlan::VectorSearch {
                    query: VectorRef::Vector(vec![1.0, 0.0, 0.0]),
                    k: 20,
                    filter: Some(Predicate::and([
                        Predicate::eq(FieldRef::Tenant, Literal::U32(7)),
                        Predicate::kind_in([NodeKind::Fact, NodeKind::Episode]),
                    ])),
                    model: ModelId::from("text"),
                }),
                k: 5,
                by: SortKey::ScoreDesc,
            }),
            right: Box::new(LogicalPlan::Scan {
                table: mmdb_query::TableId::Nodes,
                filter: Some(Predicate::and([
                    Predicate::eq(FieldRef::Tenant, Literal::U32(7)),
                    Predicate::kind_in([NodeKind::Fact]),
                ])),
            }),
            on: mmdb_query::JoinKey::NodeId,
        }
    );
}

#[test]
fn parses_join_clause_on_matching_metadata_field() {
    let plan = parse(
        r#"
        recall n: Node
          where n.tenant = 7 and n.kind in (Fact)
          similar to [1.0, 0.0, 0.0] using model "text" topk 20
          limit 5
          join m: Node where m.kind in (Entity) and m.topic = "revenue" on n.topic = m.topic
        "#,
    )
    .unwrap();

    assert_eq!(
        plan,
        LogicalPlan::Join {
            left: Box::new(LogicalPlan::TopK {
                input: Box::new(LogicalPlan::VectorSearch {
                    query: VectorRef::Vector(vec![1.0, 0.0, 0.0]),
                    k: 20,
                    filter: Some(Predicate::and([
                        Predicate::eq(FieldRef::Tenant, Literal::U32(7)),
                        Predicate::kind_in([NodeKind::Fact]),
                    ])),
                    model: ModelId::from("text"),
                }),
                k: 5,
                by: SortKey::ScoreDesc,
            }),
            right: Box::new(LogicalPlan::Scan {
                table: mmdb_query::TableId::Nodes,
                filter: Some(Predicate::and([
                    Predicate::eq(FieldRef::Tenant, Literal::U32(7)),
                    Predicate::kind_in([NodeKind::Entity]),
                    Predicate::eq(
                        FieldRef::Metadata("topic".to_string()),
                        Literal::String("revenue".to_string()),
                    ),
                ])),
            }),
            on: mmdb_query::JoinKey::Field(FieldRef::Metadata("topic".to_string())),
        }
    );
}

#[test]
fn parses_multiple_join_clauses_in_order() {
    let plan = parse(
        r#"
        recall n: Node
          where n.tenant = 7 and n.kind in (Fact)
          similar to [1.0, 0.0, 0.0] using model "text" topk 20
          limit 5
          join m: Node where m.kind in (Entity) on n.topic = m.topic
          join p: Node where p.kind in (Artifact) on node_id
        "#,
    )
    .unwrap();

    let left = LogicalPlan::TopK {
        input: Box::new(LogicalPlan::VectorSearch {
            query: VectorRef::Vector(vec![1.0, 0.0, 0.0]),
            k: 20,
            filter: Some(Predicate::and([
                Predicate::eq(FieldRef::Tenant, Literal::U32(7)),
                Predicate::kind_in([NodeKind::Fact]),
            ])),
            model: ModelId::from("text"),
        }),
        k: 5,
        by: SortKey::ScoreDesc,
    };

    let first_join = LogicalPlan::Join {
        left: Box::new(left),
        right: Box::new(LogicalPlan::Scan {
            table: mmdb_query::TableId::Nodes,
            filter: Some(Predicate::and([
                Predicate::eq(FieldRef::Tenant, Literal::U32(7)),
                Predicate::kind_in([NodeKind::Entity]),
            ])),
        }),
        on: mmdb_query::JoinKey::Field(FieldRef::Metadata("topic".to_string())),
    };

    assert_eq!(
        plan,
        LogicalPlan::Join {
            left: Box::new(first_join),
            right: Box::new(LogicalPlan::Scan {
                table: mmdb_query::TableId::Nodes,
                filter: Some(Predicate::and([
                    Predicate::eq(FieldRef::Tenant, Literal::U32(7)),
                    Predicate::kind_in([NodeKind::Artifact]),
                ])),
            }),
            on: mmdb_query::JoinKey::NodeId,
        }
    );
}

#[test]
fn parses_return_projection_clause() {
    let plan = parse(
        r#"
        recall n: Node
          where n.tenant = 7 and n.kind in (Fact)
          similar to [1.0, 0.0, 0.0] using model "text" topk 20
          limit 5
          return n.id, n.content, score
        "#,
    )
    .unwrap();

    assert_eq!(
        plan,
        LogicalPlan::Project {
            input: Box::new(LogicalPlan::TopK {
                input: Box::new(LogicalPlan::VectorSearch {
                    query: VectorRef::Vector(vec![1.0, 0.0, 0.0]),
                    k: 20,
                    filter: Some(Predicate::and([
                        Predicate::eq(FieldRef::Tenant, Literal::U32(7)),
                        Predicate::kind_in([NodeKind::Fact]),
                    ])),
                    model: ModelId::from("text"),
                }),
                k: 5,
                by: SortKey::ScoreDesc,
            }),
            fields: vec![FieldRef::NodeId, FieldRef::Content, FieldRef::Score],
        }
    );
}

#[test]
fn resolver_rejects_unknown_model_or_udf_before_lowering() {
    let input = r#"
        recall n: Node
          where n.tenant = 7 and n.kind in (Fact)
          similar to [1.0, 0.0, 0.0] using model "image" topk 20
          score by udf "boost"
          limit 5
        "#;
    let resolver = Resolver::default().allow_model("text").allow_udf("boost");

    let err = parse_with_resolver(input, &resolver).unwrap_err();

    assert!(format!("{err}").contains("unknown model"));

    let input = r#"
        recall n: Node
          where n.tenant = 7 and n.kind in (Fact)
          similar to [1.0, 0.0, 0.0] using model "text" topk 20
          score by udf "boost"
          limit 5
        "#;
    assert!(parse_with_resolver(input, &resolver).is_ok());
}

#[test]
fn diagnostic_parser_reports_missing_clause_span() {
    let input = r#"
        recall n: Node
          where n.tenant = 7 and n.kind in (Fact)
          similar to [1.0, 0.0, 0.0] topk 20
          limit 5
        "#;

    let err = parse_ast_diagnostic(input).unwrap_err();

    assert!(err.message.contains("using model"));
    assert_eq!(&input[err.span.clone()], "similar to [1.0, 0.0, 0.0]");
}

#[test]
fn diagnostic_parser_reports_bad_kind_span() {
    let input = r#"
        recall n: Node
          where n.tenant = 7 and n.kind in (Memory)
          similar to [1.0, 0.0, 0.0] using model "text" topk 20
          limit 5
        "#;

    let err = parse_ast_diagnostic(input).unwrap_err();

    assert!(err.message.contains("unknown node kind"));
    assert_eq!(&input[err.span], "Memory");
}
