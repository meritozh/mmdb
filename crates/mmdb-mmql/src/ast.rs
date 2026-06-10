use mmdb_core::{NodeKind, Result};
use mmdb_query::{
    AggregateExpr, Expr, FieldRef, JoinKey, Literal, LogicalPlan, ModelId, Predicate, ScoreExpr,
    SortKey, TableId, VectorRef,
};
use std::collections::BTreeSet;

use crate::util::invalid;

#[derive(Debug, Clone, PartialEq)]
pub struct RecallQuery {
    pub tenant: u32,
    pub kinds: Vec<NodeKind>,
    pub created_at_predicates: Vec<Predicate>,
    pub metadata_predicates: Vec<Predicate>,
    pub where_predicate: Option<Predicate>,
    pub query: VectorRef,
    pub model: String,
    pub topk: usize,
    pub limit: usize,
    pub graph: Option<GraphClause>,
    pub udf: Option<UdfClause>,
    pub score: Option<ScoreExpr>,
    pub joins: Vec<JoinClause>,
    pub aggregate: Option<AggregateClause>,
    pub return_fields: Vec<FieldRef>,
}

impl RecallQuery {
    pub fn lower(&self) -> LogicalPlan {
        let filter = self.where_predicate.clone().unwrap_or_else(|| {
            let mut predicates = vec![
                Predicate::eq(FieldRef::Tenant, Literal::U32(self.tenant)),
                Predicate::kind_in(self.kinds.clone()),
            ];
            predicates.extend(self.created_at_predicates.clone());
            predicates.extend(self.metadata_predicates.clone());
            Predicate::and(predicates)
        });

        let mut plan = LogicalPlan::VectorSearch {
            query: self.query.clone(),
            k: self.topk,
            filter: Some(filter),
            model: ModelId::from(self.model.clone()),
        };

        if let Some(graph) = &self.graph {
            if let Some(seed) = &graph.from {
                plan = LogicalPlan::Join {
                    left: Box::new(plan),
                    right: Box::new(LogicalPlan::GraphExpand {
                        from: Box::new(LogicalPlan::Scan {
                            table: TableId::Nodes,
                            filter: Some(Predicate::and([
                                Predicate::eq(FieldRef::Tenant, Literal::U32(self.tenant)),
                                seed.filter.clone(),
                            ])),
                        }),
                        relation: Some(graph.relation.clone()),
                        depth: graph.depth,
                    }),
                    on: JoinKey::NodeId,
                };
            } else {
                plan = LogicalPlan::GraphExpand {
                    from: Box::new(plan),
                    relation: Some(graph.relation.clone()),
                    depth: graph.depth,
                };
            }
        }

        if let Some(udf) = &self.udf {
            plan = LogicalPlan::Udf {
                input: Box::new(plan),
                name: udf.name.clone(),
                args: vec![Expr::Field(FieldRef::Score)],
            };
        } else if let Some(expr) = &self.score {
            plan = LogicalPlan::Score {
                input: Box::new(plan),
                expr: expr.clone(),
            };
        }

        plan = LogicalPlan::TopK {
            input: Box::new(plan),
            k: self.limit,
            by: SortKey::ScoreDesc,
        };

        for join in &self.joins {
            plan = LogicalPlan::Join {
                left: Box::new(plan),
                right: Box::new(LogicalPlan::Scan {
                    table: TableId::Nodes,
                    filter: Some(Predicate::and([
                        Predicate::eq(FieldRef::Tenant, Literal::U32(self.tenant)),
                        join.right_filter.clone(),
                    ])),
                }),
                on: join.on.clone(),
            };
        }

        if let Some(aggregate) = &self.aggregate {
            plan = LogicalPlan::Aggregate {
                input: Box::new(plan),
                group_by: aggregate.group_by.clone(),
                aggregate: aggregate.aggregate.clone(),
            };
        }

        if !self.return_fields.is_empty() {
            plan = LogicalPlan::Project {
                input: Box::new(plan),
                fields: self.return_fields.clone(),
            };
        }

        plan
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct GraphClause {
    pub relation: String,
    pub depth: u8,
    pub from: Option<GraphSeed>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct GraphSeed {
    pub alias: String,
    pub filter: Predicate,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UdfClause {
    pub name: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct JoinClause {
    pub alias: String,
    pub kinds: Vec<NodeKind>,
    pub right_filter: Predicate,
    pub on: JoinKey,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AggregateClause {
    pub group_by: Vec<FieldRef>,
    pub aggregate: AggregateExpr,
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct Resolver {
    models: BTreeSet<String>,
    udfs: BTreeSet<String>,
}

impl Resolver {
    pub fn allow_model(mut self, model: impl Into<String>) -> Self {
        self.models.insert(model.into());
        self
    }

    pub fn allow_udf(mut self, udf: impl Into<String>) -> Self {
        self.udfs.insert(udf.into());
        self
    }

    pub fn resolve(&self, query: &RecallQuery) -> Result<()> {
        if !self.models.is_empty() && !self.models.contains(&query.model) {
            return Err(invalid(format!("unknown model `{}`", query.model)));
        }
        if let Some(udf) = &query.udf {
            if !self.udfs.is_empty() && !self.udfs.contains(&udf.name) {
                return Err(invalid(format!("unknown UDF `{}`", udf.name)));
            }
        }
        Ok(())
    }
}
