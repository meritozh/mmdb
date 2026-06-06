//! LogicalPlan IR + recall builder + rule optimizer + batch physical executor.

use mmdb_core::{Error, NodeKind, Result};
use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet, HashSet, VecDeque};
use std::rc::Rc;
use std::sync::Arc;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TableId {
    Nodes,
    Edges,
    Blobs,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ModelId(pub String);

impl From<&str> for ModelId {
    fn from(value: &str) -> Self {
        Self(value.to_string())
    }
}

impl From<String> for ModelId {
    fn from(value: String) -> Self {
        Self(value)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum VectorRef {
    Vector(Vec<f32>),
    Text(String),
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum FieldRef {
    Tenant,
    Kind,
    CreatedAtMs,
    UpdatedAtMs,
    Metadata(String),
    Content,
    Score,
    NodeId,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum Literal {
    U32(u32),
    I64(i64),
    F32(OrderedF32),
    String(String),
    NodeKind(NodeKind),
    NodeKinds(Vec<NodeKind>),
    Bool(bool),
}

#[derive(Debug, Clone, Copy, Default)]
pub struct OrderedF32(pub f32);

impl PartialEq for OrderedF32 {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == std::cmp::Ordering::Equal
    }
}

impl Eq for OrderedF32 {}

impl PartialOrd for OrderedF32 {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for OrderedF32 {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0.total_cmp(&other.0)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum Predicate {
    Eq(FieldRef, Literal),
    Gt(FieldRef, Literal),
    Gte(FieldRef, Literal),
    Lt(FieldRef, Literal),
    Lte(FieldRef, Literal),
    In(FieldRef, Vec<Literal>),
    And(Vec<Predicate>),
    Or(Vec<Predicate>),
    Not(Box<Predicate>),
}

impl Predicate {
    pub fn eq(field: FieldRef, value: Literal) -> Self {
        Self::Eq(field, value)
    }

    pub fn kind_eq(kind: NodeKind) -> Self {
        Self::Eq(FieldRef::Kind, Literal::NodeKind(kind))
    }

    pub fn kind_in(kinds: impl IntoIterator<Item = NodeKind>) -> Self {
        Self::In(
            FieldRef::Kind,
            kinds.into_iter().map(Literal::NodeKind).collect(),
        )
    }

    pub fn created_after_ms(ts_ms: i64) -> Self {
        Self::Gt(FieldRef::CreatedAtMs, Literal::I64(ts_ms))
    }

    pub fn and(preds: impl IntoIterator<Item = Predicate>) -> Self {
        let mut out = Vec::new();
        for pred in preds {
            match pred {
                Predicate::And(children) => out.extend(children),
                other => out.push(other),
            }
        }
        Self::And(out)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SortKey {
    ScoreDesc,
    CreatedAtDesc,
    Field(FieldRef),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JoinKey {
    NodeId,
    Field(FieldRef),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinStrategy {
    HashBuildLeft,
    HashBuildRight,
    Merge,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JoinOrderCandidate {
    pub leaf_order: Vec<usize>,
    pub leaf_estimates: Vec<Option<usize>>,
    pub estimated_cost: usize,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ScoreExpr {
    Similarity,
    Decay { field: FieldRef, half_life_ms: i64 },
    Literal(f32),
    Mul(Box<ScoreExpr>, Box<ScoreExpr>),
    Add(Box<ScoreExpr>, Box<ScoreExpr>),
}

#[derive(Debug, Clone, PartialEq)]
pub enum Expr {
    Field(FieldRef),
    Literal(Literal),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AggregateExpr {
    Count,
}

#[derive(Debug, Clone, PartialEq)]
pub enum LogicalPlan {
    Scan {
        table: TableId,
        filter: Option<Predicate>,
    },
    VectorSearch {
        query: VectorRef,
        k: usize,
        filter: Option<Predicate>,
        model: ModelId,
    },
    GraphExpand {
        from: Box<LogicalPlan>,
        relation: Option<String>,
        depth: u8,
    },
    Filter {
        input: Box<LogicalPlan>,
        pred: Predicate,
    },
    Score {
        input: Box<LogicalPlan>,
        expr: ScoreExpr,
    },
    TopK {
        input: Box<LogicalPlan>,
        k: usize,
        by: SortKey,
    },
    Join {
        left: Box<LogicalPlan>,
        right: Box<LogicalPlan>,
        on: JoinKey,
    },
    Aggregate {
        input: Box<LogicalPlan>,
        group_by: Vec<FieldRef>,
        aggregate: AggregateExpr,
    },
    Project {
        input: Box<LogicalPlan>,
        fields: Vec<FieldRef>,
    },
    Udf {
        input: Box<LogicalPlan>,
        name: String,
        args: Vec<Expr>,
    },
}

pub struct Query;

impl Query {
    pub fn recall() -> RecallBuilder {
        RecallBuilder::default()
    }
}

#[derive(Default)]
pub struct RecallBuilder {
    tenant: Option<u32>,
    filters: Vec<Predicate>,
    limit: Option<usize>,
}

impl RecallBuilder {
    pub fn tenant(mut self, tenant: u32) -> Self {
        self.tenant = Some(tenant);
        self
    }

    pub fn filter(mut self, pred: Predicate) -> Self {
        self.filters.push(pred);
        self
    }

    pub fn limit(mut self, limit: usize) -> Self {
        self.limit = Some(limit);
        self
    }

    pub fn similar_to(self, vector: Vec<f32>) -> VectorRecallBuilder {
        VectorRecallBuilder {
            recall: self,
            query: VectorRef::Vector(vector),
            model: ModelId::from("default"),
            topk: 10,
        }
    }

    pub fn build(self) -> LogicalPlan {
        let mut plan = LogicalPlan::Scan {
            table: TableId::Nodes,
            filter: combined_recall_filter(self.tenant, self.filters),
        };
        if let Some(limit) = self.limit {
            plan = LogicalPlan::TopK {
                input: Box::new(plan),
                k: limit,
                by: SortKey::CreatedAtDesc,
            };
        }
        plan
    }
}

pub struct VectorRecallBuilder {
    recall: RecallBuilder,
    query: VectorRef,
    model: ModelId,
    topk: usize,
}

impl VectorRecallBuilder {
    pub fn using_model(mut self, model: impl Into<ModelId>) -> Self {
        self.model = model.into();
        self
    }

    pub fn topk(mut self, k: usize) -> Self {
        self.topk = k;
        self
    }

    pub fn limit(mut self, limit: usize) -> Self {
        self.recall.limit = Some(limit);
        self
    }

    pub fn build(self) -> LogicalPlan {
        let mut plan = LogicalPlan::VectorSearch {
            query: self.query,
            k: self.topk,
            filter: combined_recall_filter(self.recall.tenant, self.recall.filters),
            model: self.model,
        };
        if let Some(limit) = self.recall.limit {
            plan = LogicalPlan::TopK {
                input: Box::new(plan),
                k: limit,
                by: SortKey::ScoreDesc,
            };
        }
        plan
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct FieldHistogram {
    counts: BTreeMap<Literal, u64>,
    total_count: u64,
}

impl FieldHistogram {
    pub fn from_counts(counts: impl IntoIterator<Item = (Literal, u64)>) -> Self {
        let mut merged = BTreeMap::new();
        let mut total_count = 0_u64;
        for (literal, count) in counts {
            if count == 0 {
                continue;
            }
            *merged.entry(literal).or_default() += count;
            total_count += count;
        }
        Self {
            counts: merged,
            total_count,
        }
    }

    pub fn total_count(&self) -> u64 {
        self.total_count
    }

    pub fn count(&self, literal: &Literal) -> u64 {
        self.counts.get(literal).copied().unwrap_or(0)
    }

    fn selectivity_for_literals<'a>(
        &self,
        literals: impl IntoIterator<Item = &'a Literal>,
    ) -> Option<f32> {
        if self.total_count == 0 {
            return None;
        }

        let mut seen = BTreeSet::new();
        let mut matching = 0_u64;
        for literal in literals {
            if seen.insert(literal) {
                matching += self.count(literal);
            }
        }
        Some((matching as f32 / self.total_count as f32).clamp(0.0, 1.0))
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Stats {
    pub node_rows: usize,
    pub estimated_filter_selectivity: f32,
    pub histograms: BTreeMap<FieldRef, FieldHistogram>,
}

impl Default for Stats {
    fn default() -> Self {
        Self {
            node_rows: 0,
            estimated_filter_selectivity: 1.0,
            histograms: BTreeMap::new(),
        }
    }
}

impl Stats {
    pub fn with_histogram(mut self, field: FieldRef, histogram: FieldHistogram) -> Self {
        self.histograms.insert(field, histogram);
        self
    }

    pub fn estimate_selectivity(&self, pred: &Predicate) -> f32 {
        let fallback = self.estimated_filter_selectivity.clamp(0.0, 1.0);
        match pred {
            Predicate::Eq(field, literal) => self
                .histograms
                .get(field)
                .and_then(|hist| hist.selectivity_for_literals(std::iter::once(literal)))
                .unwrap_or(fallback),
            Predicate::In(field, literals) => self
                .histograms
                .get(field)
                .and_then(|hist| hist.selectivity_for_literals(literals))
                .unwrap_or(fallback),
            Predicate::And(preds) => preds
                .iter()
                .map(|pred| self.estimate_selectivity(pred))
                .product::<f32>()
                .clamp(0.0, 1.0),
            Predicate::Or(preds) => {
                let none_match = preds
                    .iter()
                    .map(|pred| 1.0 - self.estimate_selectivity(pred))
                    .product::<f32>();
                (1.0 - none_match).clamp(0.0, 1.0)
            }
            Predicate::Not(pred) => (1.0 - self.estimate_selectivity(pred)).clamp(0.0, 1.0),
            Predicate::Gt(_, _)
            | Predicate::Gte(_, _)
            | Predicate::Lt(_, _)
            | Predicate::Lte(_, _) => fallback,
        }
    }
}

#[derive(Debug, Default, Clone)]
pub struct Optimizer {
    stats: Stats,
}

impl Optimizer {
    pub fn with_stats(stats: Stats) -> Self {
        Self { stats }
    }

    pub fn choose_join_strategy(
        &self,
        left_rows: Option<usize>,
        right_rows: Option<usize>,
        on: &JoinKey,
    ) -> JoinStrategy {
        match (left_rows, right_rows) {
            (Some(left), Some(right)) => choose_join_strategy(left, right, on),
            (Some(_), None) => JoinStrategy::HashBuildLeft,
            (None, Some(_)) => JoinStrategy::HashBuildRight,
            (None, None) => JoinStrategy::HashBuildRight,
        }
    }

    pub fn join_order_candidates(&self, plan: &LogicalPlan) -> Vec<JoinOrderCandidate> {
        let LogicalPlan::Join { on, .. } = plan else {
            return Vec::new();
        };
        let mut leaves = Vec::new();
        if !collect_same_key_join_leaves(plan, on, &mut leaves)
            || leaves.len() < 2
            || leaves.len() > MAX_JOIN_ORDER_LEAVES
        {
            return Vec::new();
        }

        let estimates = leaves
            .iter()
            .map(|leaf| estimated_rows_for_plan(leaf, &self.stats))
            .collect::<Vec<_>>();
        let mut orders = Vec::new();
        let mut order = (0..leaves.len()).collect::<Vec<_>>();
        permute_join_orders(0, &mut order, &mut orders);

        let mut candidates = orders
            .into_iter()
            .map(|leaf_order| {
                let leaf_estimates = leaf_order
                    .iter()
                    .map(|index| estimates[*index])
                    .collect::<Vec<_>>();
                let estimated_cost = join_order_cost(&leaf_estimates);
                JoinOrderCandidate {
                    leaf_order,
                    leaf_estimates,
                    estimated_cost,
                }
            })
            .collect::<Vec<_>>();
        candidates.sort_by(|left, right| {
            left.estimated_cost
                .cmp(&right.estimated_cost)
                .then_with(|| left.leaf_estimates.cmp(&right.leaf_estimates))
                .then_with(|| left.leaf_order.cmp(&right.leaf_order))
        });
        candidates
    }

    pub fn optimize(&self, plan: LogicalPlan) -> LogicalPlan {
        match plan {
            LogicalPlan::Filter { input, pred } => {
                let input = self.optimize(*input);
                match input {
                    LogicalPlan::Scan { table, filter } => LogicalPlan::Scan {
                        table,
                        filter: Some(combine_optional_filter(filter, pred)),
                    },
                    LogicalPlan::VectorSearch {
                        query,
                        k,
                        filter,
                        model,
                    } => LogicalPlan::VectorSearch {
                        query,
                        k,
                        filter: Some(combine_optional_filter(filter, pred)),
                        model,
                    },
                    LogicalPlan::GraphExpand {
                        from,
                        relation,
                        depth,
                    } if self.stats.estimate_selectivity(&pred) <= 0.5 => {
                        LogicalPlan::GraphExpand {
                            from: Box::new(
                                self.optimize(LogicalPlan::Filter { input: from, pred }),
                            ),
                            relation,
                            depth,
                        }
                    }
                    other => LogicalPlan::Filter {
                        input: Box::new(other),
                        pred,
                    },
                }
            }
            LogicalPlan::TopK { input, k, by } => LogicalPlan::TopK {
                input: Box::new(self.optimize(*input)),
                k,
                by,
            },
            LogicalPlan::Score { input, expr } => LogicalPlan::Score {
                input: Box::new(self.optimize(*input)),
                expr,
            },
            LogicalPlan::GraphExpand {
                from,
                relation,
                depth,
            } => LogicalPlan::GraphExpand {
                from: Box::new(self.optimize(*from)),
                relation,
                depth,
            },
            LogicalPlan::Join { left, right, on } => {
                let plan = LogicalPlan::Join {
                    left: Box::new(self.optimize(*left)),
                    right: Box::new(self.optimize(*right)),
                    on,
                };
                self.reorder_safe_join_chain(plan)
            }
            LogicalPlan::Aggregate {
                input,
                group_by,
                aggregate,
            } => LogicalPlan::Aggregate {
                input: Box::new(self.optimize(*input)),
                group_by,
                aggregate,
            },
            LogicalPlan::Project { input, fields } => LogicalPlan::Project {
                input: Box::new(self.optimize(*input)),
                fields,
            },
            LogicalPlan::Udf { input, name, args } => LogicalPlan::Udf {
                input: Box::new(self.optimize(*input)),
                name,
                args,
            },
            leaf => leaf,
        }
    }

    fn reorder_safe_join_chain(&self, plan: LogicalPlan) -> LogicalPlan {
        let LogicalPlan::Join { on, .. } = &plan else {
            return plan;
        };
        if on != &JoinKey::NodeId {
            return plan;
        }

        let mut leaves = Vec::new();
        if !collect_same_key_join_leaves(&plan, on, &mut leaves) || leaves.len() < 3 {
            return plan;
        }

        let all_scan_chain = leaves.iter().all(is_safe_scan_join_leaf);
        let fixed_score_anchor_chain = is_score_preserving_join_anchor(&leaves[0])
            && leaves[1..].iter().all(is_safe_scan_join_leaf);
        if !all_scan_chain && !fixed_score_anchor_chain {
            return plan;
        };

        let mut candidates = self.join_order_candidates(&plan).into_iter();
        let best = if all_scan_chain {
            candidates.next()
        } else {
            candidates.find(|candidate| candidate.leaf_order.first() == Some(&0))
        };
        let Some(best) = best else { return plan };
        if best.leaf_order == (0..leaves.len()).collect::<Vec<_>>() {
            return plan;
        }

        let ordered = best
            .leaf_order
            .into_iter()
            .map(|index| leaves[index].clone())
            .collect::<Vec<_>>();
        build_left_deep_join(ordered, on.clone()).unwrap_or(plan)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Record {
    pub node_id: String,
    pub tenant: u32,
    pub kind: NodeKind,
    pub created_at_ms: i64,
    pub updated_at_ms: i64,
    pub score: f32,
    pub fields: BTreeMap<String, Literal>,
}

impl Record {
    pub fn new(
        node_id: impl Into<String>,
        tenant: u32,
        kind: NodeKind,
        created_at_ms: i64,
    ) -> Self {
        Self {
            node_id: node_id.into(),
            tenant,
            kind,
            created_at_ms,
            updated_at_ms: created_at_ms,
            score: 0.0,
            fields: BTreeMap::new(),
        }
    }

    pub fn with_updated_at_ms(mut self, updated_at_ms: i64) -> Self {
        self.updated_at_ms = updated_at_ms;
        self
    }

    pub fn with_score(mut self, score: f32) -> Self {
        self.score = score;
        self
    }

    pub fn with_field(mut self, key: impl Into<String>, value: Literal) -> Self {
        self.fields.insert(key.into(), value);
        self
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EdgeRecord {
    pub src: String,
    pub dst: String,
    pub relation: String,
}

impl EdgeRecord {
    pub fn new(
        src: impl Into<String>,
        dst: impl Into<String>,
        relation: impl Into<String>,
    ) -> Self {
        Self {
            src: src.into(),
            dst: dst.into(),
            relation: relation.into(),
        }
    }
}

pub type UdfFn = dyn Fn(&Record, &[Expr]) -> f32 + Send + Sync;

#[derive(Default)]
pub struct ExecutionContext {
    pub nodes: Vec<Record>,
    pub edges: Vec<EdgeRecord>,
    udfs: BTreeMap<String, Arc<UdfFn>>,
}

impl ExecutionContext {
    pub fn register_udf(
        &mut self,
        name: impl Into<String>,
        udf: impl Fn(&Record, &[Expr]) -> f32 + Send + Sync + 'static,
    ) {
        self.udfs.insert(name.into(), Arc::new(udf));
    }
}

pub struct Executor<'a> {
    ctx: &'a ExecutionContext,
}

impl<'a> Executor<'a> {
    pub fn new(ctx: &'a ExecutionContext) -> Self {
        Self { ctx }
    }

    pub fn explain(&self, plan: &LogicalPlan, stats: &Stats) -> Result<ExplainNode> {
        self.explain_with_estimate(plan, stats)
            .map(|(node, _)| node)
    }

    fn explain_with_estimate(
        &self,
        plan: &LogicalPlan,
        stats: &Stats,
    ) -> Result<(ExplainNode, Option<usize>)> {
        let mut children = Vec::new();
        let estimated_rows = match plan {
            LogicalPlan::Scan { table, filter } => {
                let base_rows = match table {
                    TableId::Nodes => Some(stats.node_rows),
                    TableId::Edges | TableId::Blobs => None,
                };
                estimate_filter_rows(base_rows, filter.as_ref(), stats)
            }
            LogicalPlan::VectorSearch { k, filter, .. } => {
                let estimated = estimate_filter_rows(Some(stats.node_rows), filter.as_ref(), stats);
                estimated.map(|rows| rows.min(*k))
            }
            LogicalPlan::GraphExpand { from, depth, .. } => {
                let (child, child_estimate) = self.explain_with_estimate(from, stats)?;
                children.push(child);
                child_estimate.map(|rows| rows.saturating_mul((*depth as usize).saturating_add(1)))
            }
            LogicalPlan::Filter { input, pred } => {
                let (child, child_estimate) = self.explain_with_estimate(input, stats)?;
                children.push(child);
                estimate_rows_by_selectivity(child_estimate, stats.estimate_selectivity(pred))
            }
            LogicalPlan::Score { input, .. }
            | LogicalPlan::Project { input, .. }
            | LogicalPlan::Udf { input, .. } => {
                let (child, child_estimate) = self.explain_with_estimate(input, stats)?;
                children.push(child);
                child_estimate
            }
            LogicalPlan::Aggregate { input, .. } => {
                let (child, child_estimate) = self.explain_with_estimate(input, stats)?;
                children.push(child);
                child_estimate
            }
            LogicalPlan::TopK { input, k, .. } => {
                let (child, child_estimate) = self.explain_with_estimate(input, stats)?;
                children.push(child);
                child_estimate.map(|rows| rows.min(*k))
            }
            LogicalPlan::Join { left, right, .. } => {
                let (left_child, left_estimate) = self.explain_with_estimate(left, stats)?;
                let (right_child, right_estimate) = self.explain_with_estimate(right, stats)?;
                let strategy = Optimizer::with_stats(stats.clone()).choose_join_strategy(
                    left_estimate,
                    right_estimate,
                    match plan {
                        LogicalPlan::Join { on, .. } => on,
                        _ => unreachable!(),
                    },
                );
                children.push(left_child);
                children.push(right_child);
                let estimated = left_estimate.zip(right_estimate).map(|(l, r)| l.min(r));
                let actual_rows = Some(self.execute(plan)?.len());
                let node = ExplainNode {
                    operator: join_operator_name(strategy).to_string(),
                    estimated_rows: estimated,
                    actual_rows,
                    children,
                };
                return Ok((node, estimated));
            }
        };
        let actual_rows = Some(self.execute(plan)?.len());
        let node = ExplainNode {
            operator: physical_operator_name(plan).to_string(),
            estimated_rows,
            actual_rows,
            children,
        };
        Ok((node, estimated_rows))
    }

    pub fn compile(
        &self,
        plan: &LogicalPlan,
        batch_size: usize,
    ) -> Result<Box<dyn PhysicalOperator + 'a>> {
        let batch_size = batch_size.max(1);
        match plan {
            LogicalPlan::Scan { table, filter } => {
                if table != &TableId::Nodes {
                    return Err(Error::InvalidArgument(format!(
                        "in-memory executor does not support scanning {table:?}"
                    )));
                }
                Ok(Box::new(ScanOperator {
                    ctx: self.ctx,
                    filter: filter.clone(),
                    cursor: 0,
                    batch_size,
                }))
            }
            LogicalPlan::VectorSearch { k, filter, .. } => Ok(Box::new(MaterializedOperator::new(
                vector_search_rows(self.ctx, *k, filter.as_ref()),
                batch_size,
            ))),
            LogicalPlan::GraphExpand {
                from,
                relation,
                depth,
            } => Ok(Box::new(GraphExpandOperator {
                ctx: self.ctx,
                input: Some(self.compile(from, batch_size)?),
                relation: relation.clone(),
                depth: *depth,
                output: None,
                batch_size,
            })),
            LogicalPlan::Filter { input, pred } => Ok(Box::new(FilterOperator {
                input: self.compile(input, batch_size)?,
                pred: pred.clone(),
                pending: VecDeque::new(),
                batch_size,
            })),
            LogicalPlan::Score { input, expr } => Ok(Box::new(ScoreOperator {
                input: self.compile(input, batch_size)?,
                expr: expr.clone(),
            })),
            LogicalPlan::TopK { input, k, by } => Ok(Box::new(TopKOperator {
                input: Some(self.compile(input, batch_size)?),
                output: None,
                k: *k,
                by: by.clone(),
                batch_size,
            })),
            LogicalPlan::Join { left, right, on } => Ok(Box::new(JoinOperator {
                left: Some(self.compile(left, batch_size)?),
                right: Some(self.compile(right, batch_size)?),
                output: None,
                on: on.clone(),
                batch_size,
            })),
            LogicalPlan::Aggregate {
                input,
                group_by,
                aggregate,
            } => Ok(Box::new(AggregateOperator {
                input: Some(self.compile(input, batch_size)?),
                output: None,
                group_by: group_by.clone(),
                aggregate: aggregate.clone(),
                batch_size,
            })),
            LogicalPlan::Project { input, fields } => Ok(Box::new(ProjectOperator {
                input: self.compile(input, batch_size)?,
                fields: fields.clone(),
            })),
            LogicalPlan::Udf { input, name, args } => {
                let udf = self.ctx.udfs.get(name).cloned().ok_or_else(|| {
                    Error::InvalidArgument(format!("UDF `{name}` is not registered"))
                })?;
                Ok(Box::new(UdfOperator {
                    input: self.compile(input, batch_size)?,
                    udf,
                    args: args.clone(),
                }))
            }
        }
    }

    pub fn execute(&self, plan: &LogicalPlan) -> Result<Vec<Record>> {
        match plan {
            LogicalPlan::Scan { table, filter } => match table {
                TableId::Nodes => Ok(self
                    .ctx
                    .nodes
                    .iter()
                    .filter(|record| {
                        filter
                            .as_ref()
                            .map(|pred| predicate_matches(record, pred))
                            .unwrap_or(true)
                    })
                    .cloned()
                    .collect()),
                other => Err(Error::InvalidArgument(format!(
                    "in-memory executor does not support scanning {other:?}"
                ))),
            },
            LogicalPlan::VectorSearch { k, filter, .. } => {
                Ok(vector_search_rows(self.ctx, *k, filter.as_ref()))
            }
            LogicalPlan::GraphExpand {
                from,
                relation,
                depth,
            } => self.execute_graph_expand(from, relation.as_deref(), *depth),
            LogicalPlan::Filter { input, pred } => Ok(self
                .execute(input)?
                .into_iter()
                .filter(|record| predicate_matches(record, pred))
                .collect()),
            LogicalPlan::Score { input, expr } => Ok(self
                .execute(input)?
                .into_iter()
                .map(|mut record| {
                    record.score = eval_score(&record, expr);
                    record
                })
                .collect()),
            LogicalPlan::TopK { input, k, by } => {
                let mut rows = self.execute(input)?;
                sort_rows(&mut rows, by);
                rows.truncate(*k);
                Ok(rows)
            }
            LogicalPlan::Join { left, right, on } => self.execute_join(left, right, on),
            LogicalPlan::Aggregate {
                input,
                group_by,
                aggregate,
            } => Ok(aggregate_records(self.execute(input)?, group_by, aggregate)),
            LogicalPlan::Project { input, fields } => {
                Ok(project_records(self.execute(input)?, fields))
            }
            LogicalPlan::Udf { input, name, args } => {
                let udf = self.ctx.udfs.get(name).ok_or_else(|| {
                    Error::InvalidArgument(format!("UDF `{name}` is not registered"))
                })?;
                Ok(self
                    .execute(input)?
                    .into_iter()
                    .map(|mut record| {
                        record.score = udf(&record, args);
                        record
                    })
                    .collect())
            }
        }
    }

    fn execute_graph_expand(
        &self,
        from: &LogicalPlan,
        relation: Option<&str>,
        depth: u8,
    ) -> Result<Vec<Record>> {
        let seeds = self.execute(from)?;
        if depth == 0 {
            return Ok(seeds);
        }
        Ok(graph_expand_rows(self.ctx, seeds, relation, depth))
    }

    fn execute_join(
        &self,
        left: &LogicalPlan,
        right: &LogicalPlan,
        on: &JoinKey,
    ) -> Result<Vec<Record>> {
        let left_rows = self.execute(left)?;
        let right_rows = self.execute(right)?;
        Ok(join_rows(left_rows, right_rows, on))
    }
}

pub trait QuerySource {
    fn range_scan(&self, table: &TableId, filter: Option<&Predicate>) -> Result<Vec<Record>>;

    fn hnsw_search(
        &self,
        query: &VectorRef,
        model: &ModelId,
        k: usize,
        filter: Option<&Predicate>,
    ) -> Result<Vec<Record>>;

    fn graph_expand(
        &self,
        seeds: Vec<Record>,
        relation: Option<&str>,
        depth: u8,
    ) -> Result<Vec<Record>>;
}

pub struct SourceExecutor<'a, S: QuerySource + ?Sized> {
    source: &'a S,
    udfs: BTreeMap<String, Arc<UdfFn>>,
}

impl<'a, S: QuerySource + ?Sized> SourceExecutor<'a, S> {
    pub fn new(source: &'a S) -> Self {
        Self {
            source,
            udfs: BTreeMap::new(),
        }
    }

    pub fn with_udf(mut self, name: impl Into<String>, udf: Arc<UdfFn>) -> Self {
        self.udfs.insert(name.into(), udf);
        self
    }

    pub fn explain(
        &self,
        plan: &LogicalPlan,
        stats: &Stats,
        batch_size: usize,
    ) -> Result<ExplainNode> {
        let metrics = Rc::new(RefCell::new(Vec::new()));
        let mut next_id = 0;
        let mut op =
            self.compile_instrumented(plan, batch_size.max(1), metrics.clone(), &mut next_id)?;
        materialize_operator(&mut *op)?;

        let (mut explain, _) = explain_shape_with_estimate(plan, stats, true);
        let actual_rows = metrics.borrow();
        let mut cursor = 0;
        attach_actual_rows(&mut explain, &actual_rows, &mut cursor);
        Ok(explain)
    }

    pub fn compile(
        &self,
        plan: &LogicalPlan,
        batch_size: usize,
    ) -> Result<Box<dyn PhysicalOperator + 'a>> {
        let batch_size = batch_size.max(1);
        match plan {
            LogicalPlan::Scan { table, filter } => Ok(Box::new(MaterializedOperator::new(
                self.source.range_scan(table, filter.as_ref())?,
                batch_size,
            ))),
            LogicalPlan::VectorSearch {
                query,
                k,
                filter,
                model,
            } => Ok(Box::new(MaterializedOperator::new(
                self.source.hnsw_search(query, model, *k, filter.as_ref())?,
                batch_size,
            ))),
            LogicalPlan::GraphExpand {
                from,
                relation,
                depth,
            } => Ok(Box::new(SourceGraphExpandOperator {
                source: self.source,
                input: Some(self.compile(from, batch_size)?),
                relation: relation.clone(),
                depth: *depth,
                output: None,
                batch_size,
            })),
            LogicalPlan::Filter { input, pred } => Ok(Box::new(FilterOperator {
                input: self.compile(input, batch_size)?,
                pred: pred.clone(),
                pending: VecDeque::new(),
                batch_size,
            })),
            LogicalPlan::Score { input, expr } => Ok(Box::new(ScoreOperator {
                input: self.compile(input, batch_size)?,
                expr: expr.clone(),
            })),
            LogicalPlan::TopK { input, k, by } => Ok(Box::new(TopKOperator {
                input: Some(self.compile(input, batch_size)?),
                output: None,
                k: *k,
                by: by.clone(),
                batch_size,
            })),
            LogicalPlan::Join { left, right, on } => Ok(Box::new(JoinOperator {
                left: Some(self.compile(left, batch_size)?),
                right: Some(self.compile(right, batch_size)?),
                output: None,
                on: on.clone(),
                batch_size,
            })),
            LogicalPlan::Aggregate {
                input,
                group_by,
                aggregate,
            } => Ok(Box::new(AggregateOperator {
                input: Some(self.compile(input, batch_size)?),
                output: None,
                group_by: group_by.clone(),
                aggregate: aggregate.clone(),
                batch_size,
            })),
            LogicalPlan::Project { input, fields } => Ok(Box::new(ProjectOperator {
                input: self.compile(input, batch_size)?,
                fields: fields.clone(),
            })),
            LogicalPlan::Udf { input, name, args } => {
                let udf = self.udfs.get(name).cloned().ok_or_else(|| {
                    Error::InvalidArgument(format!("source executor UDF `{name}` is not bound"))
                })?;
                Ok(Box::new(UdfOperator {
                    input: self.compile(input, batch_size)?,
                    udf,
                    args: args.clone(),
                }))
            }
        }
    }

    fn compile_instrumented(
        &self,
        plan: &LogicalPlan,
        batch_size: usize,
        metrics: RowMetrics,
        next_id: &mut usize,
    ) -> Result<Box<dyn PhysicalOperator + 'a>> {
        let id = reserve_metric_id(&metrics, next_id);
        let input: Box<dyn PhysicalOperator + 'a> = match plan {
            LogicalPlan::Scan { table, filter } => Box::new(MaterializedOperator::new(
                self.source.range_scan(table, filter.as_ref())?,
                batch_size,
            )),
            LogicalPlan::VectorSearch {
                query,
                k,
                filter,
                model,
            } => Box::new(MaterializedOperator::new(
                self.source.hnsw_search(query, model, *k, filter.as_ref())?,
                batch_size,
            )),
            LogicalPlan::GraphExpand {
                from,
                relation,
                depth,
            } => Box::new(SourceGraphExpandOperator {
                source: self.source,
                input: Some(self.compile_instrumented(
                    from,
                    batch_size,
                    metrics.clone(),
                    next_id,
                )?),
                relation: relation.clone(),
                depth: *depth,
                output: None,
                batch_size,
            }),
            LogicalPlan::Filter { input, pred } => Box::new(FilterOperator {
                input: self.compile_instrumented(input, batch_size, metrics.clone(), next_id)?,
                pred: pred.clone(),
                pending: VecDeque::new(),
                batch_size,
            }),
            LogicalPlan::Score { input, expr } => Box::new(ScoreOperator {
                input: self.compile_instrumented(input, batch_size, metrics.clone(), next_id)?,
                expr: expr.clone(),
            }),
            LogicalPlan::TopK { input, k, by } => Box::new(TopKOperator {
                input: Some(self.compile_instrumented(
                    input,
                    batch_size,
                    metrics.clone(),
                    next_id,
                )?),
                output: None,
                k: *k,
                by: by.clone(),
                batch_size,
            }),
            LogicalPlan::Join { left, right, on } => Box::new(JoinOperator {
                left: Some(self.compile_instrumented(
                    left,
                    batch_size,
                    metrics.clone(),
                    next_id,
                )?),
                right: Some(self.compile_instrumented(
                    right,
                    batch_size,
                    metrics.clone(),
                    next_id,
                )?),
                output: None,
                on: on.clone(),
                batch_size,
            }),
            LogicalPlan::Aggregate {
                input,
                group_by,
                aggregate,
            } => Box::new(AggregateOperator {
                input: Some(self.compile_instrumented(
                    input,
                    batch_size,
                    metrics.clone(),
                    next_id,
                )?),
                output: None,
                group_by: group_by.clone(),
                aggregate: aggregate.clone(),
                batch_size,
            }),
            LogicalPlan::Project { input, fields } => Box::new(ProjectOperator {
                input: self.compile_instrumented(input, batch_size, metrics.clone(), next_id)?,
                fields: fields.clone(),
            }),
            LogicalPlan::Udf { input, name, args } => {
                let udf = self.udfs.get(name).cloned().ok_or_else(|| {
                    Error::InvalidArgument(format!("source executor UDF `{name}` is not bound"))
                })?;
                Box::new(UdfOperator {
                    input: self.compile_instrumented(
                        input,
                        batch_size,
                        metrics.clone(),
                        next_id,
                    )?,
                    udf,
                    args: args.clone(),
                })
            }
        };
        Ok(Box::new(InstrumentedOperator { id, input, metrics }))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExplainNode {
    pub operator: String,
    pub estimated_rows: Option<usize>,
    pub actual_rows: Option<usize>,
    pub children: Vec<ExplainNode>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct RecordBatch {
    pub rows: Vec<Record>,
}

impl RecordBatch {
    pub fn new(rows: Vec<Record>) -> Self {
        Self { rows }
    }
}

pub trait PhysicalOperator {
    fn next_batch(&mut self) -> Result<Option<RecordBatch>>;
}

type RowMetrics = Rc<RefCell<Vec<usize>>>;

struct InstrumentedOperator<'a> {
    id: usize,
    input: Box<dyn PhysicalOperator + 'a>,
    metrics: RowMetrics,
}

impl PhysicalOperator for InstrumentedOperator<'_> {
    fn next_batch(&mut self) -> Result<Option<RecordBatch>> {
        let batch = self.input.next_batch()?;
        if let Some(batch) = batch.as_ref() {
            self.metrics.borrow_mut()[self.id] += batch.rows.len();
        }
        Ok(batch)
    }
}

struct ScanOperator<'a> {
    ctx: &'a ExecutionContext,
    filter: Option<Predicate>,
    cursor: usize,
    batch_size: usize,
}

impl PhysicalOperator for ScanOperator<'_> {
    fn next_batch(&mut self) -> Result<Option<RecordBatch>> {
        let mut rows = Vec::with_capacity(self.batch_size);
        while self.cursor < self.ctx.nodes.len() && rows.len() < self.batch_size {
            let record = &self.ctx.nodes[self.cursor];
            self.cursor += 1;
            if self
                .filter
                .as_ref()
                .map(|pred| predicate_matches(record, pred))
                .unwrap_or(true)
            {
                rows.push(record.clone());
            }
        }

        if rows.is_empty() {
            Ok(None)
        } else {
            Ok(Some(RecordBatch::new(rows)))
        }
    }
}

struct FilterOperator<'a> {
    input: Box<dyn PhysicalOperator + 'a>,
    pred: Predicate,
    pending: VecDeque<Record>,
    batch_size: usize,
}

impl PhysicalOperator for FilterOperator<'_> {
    fn next_batch(&mut self) -> Result<Option<RecordBatch>> {
        let mut rows = Vec::with_capacity(self.batch_size);
        while rows.len() < self.batch_size {
            if let Some(record) = self.pending.pop_front() {
                rows.push(record);
                continue;
            }

            let Some(batch) = self.input.next_batch()? else {
                break;
            };
            for record in batch.rows {
                if predicate_matches(&record, &self.pred) {
                    if rows.len() < self.batch_size {
                        rows.push(record);
                    } else {
                        self.pending.push_back(record);
                    }
                }
            }
        }

        if rows.is_empty() {
            Ok(None)
        } else {
            Ok(Some(RecordBatch::new(rows)))
        }
    }
}

struct ScoreOperator<'a> {
    input: Box<dyn PhysicalOperator + 'a>,
    expr: ScoreExpr,
}

impl PhysicalOperator for ScoreOperator<'_> {
    fn next_batch(&mut self) -> Result<Option<RecordBatch>> {
        let Some(batch) = self.input.next_batch()? else {
            return Ok(None);
        };
        Ok(Some(RecordBatch::new(
            batch
                .rows
                .into_iter()
                .map(|mut record| {
                    record.score = eval_score(&record, &self.expr);
                    record
                })
                .collect(),
        )))
    }
}

struct UdfOperator<'a> {
    input: Box<dyn PhysicalOperator + 'a>,
    udf: Arc<UdfFn>,
    args: Vec<Expr>,
}

impl PhysicalOperator for UdfOperator<'_> {
    fn next_batch(&mut self) -> Result<Option<RecordBatch>> {
        let Some(batch) = self.input.next_batch()? else {
            return Ok(None);
        };
        Ok(Some(RecordBatch::new(
            batch
                .rows
                .into_iter()
                .map(|mut record| {
                    record.score = (self.udf)(&record, &self.args);
                    record
                })
                .collect(),
        )))
    }
}

struct ProjectOperator<'a> {
    input: Box<dyn PhysicalOperator + 'a>,
    fields: Vec<FieldRef>,
}

impl PhysicalOperator for ProjectOperator<'_> {
    fn next_batch(&mut self) -> Result<Option<RecordBatch>> {
        let Some(batch) = self.input.next_batch()? else {
            return Ok(None);
        };
        Ok(Some(RecordBatch::new(project_records(
            batch.rows,
            &self.fields,
        ))))
    }
}

struct GraphExpandOperator<'a> {
    ctx: &'a ExecutionContext,
    input: Option<Box<dyn PhysicalOperator + 'a>>,
    relation: Option<String>,
    depth: u8,
    output: Option<MaterializedOperator>,
    batch_size: usize,
}

impl PhysicalOperator for GraphExpandOperator<'_> {
    fn next_batch(&mut self) -> Result<Option<RecordBatch>> {
        if self.output.is_none() {
            let mut input = self
                .input
                .take()
                .ok_or_else(|| Error::InvalidArgument("graph input already consumed".into()))?;
            let seeds = materialize_operator(&mut *input)?;
            let rows = if self.depth == 0 {
                seeds
            } else {
                graph_expand_rows(self.ctx, seeds, self.relation.as_deref(), self.depth)
            };
            self.output = Some(MaterializedOperator::new(rows, self.batch_size));
        }
        self.output
            .as_mut()
            .expect("output initialized")
            .next_batch()
    }
}

struct SourceGraphExpandOperator<'a, S: QuerySource + ?Sized> {
    source: &'a S,
    input: Option<Box<dyn PhysicalOperator + 'a>>,
    relation: Option<String>,
    depth: u8,
    output: Option<MaterializedOperator>,
    batch_size: usize,
}

impl<S: QuerySource + ?Sized> PhysicalOperator for SourceGraphExpandOperator<'_, S> {
    fn next_batch(&mut self) -> Result<Option<RecordBatch>> {
        if self.output.is_none() {
            let mut input = self.input.take().ok_or_else(|| {
                Error::InvalidArgument("source graph input already consumed".into())
            })?;
            let seeds = materialize_operator(&mut *input)?;
            let rows = if self.depth == 0 {
                seeds
            } else {
                self.source
                    .graph_expand(seeds, self.relation.as_deref(), self.depth)?
            };
            self.output = Some(MaterializedOperator::new(rows, self.batch_size));
        }
        self.output
            .as_mut()
            .expect("output initialized")
            .next_batch()
    }
}

struct TopKOperator<'a> {
    input: Option<Box<dyn PhysicalOperator + 'a>>,
    output: Option<MaterializedOperator>,
    k: usize,
    by: SortKey,
    batch_size: usize,
}

impl PhysicalOperator for TopKOperator<'_> {
    fn next_batch(&mut self) -> Result<Option<RecordBatch>> {
        if self.output.is_none() {
            let mut input = self
                .input
                .take()
                .ok_or_else(|| Error::InvalidArgument("top-k input already consumed".into()))?;
            let mut rows = materialize_operator(&mut *input)?;
            sort_rows(&mut rows, &self.by);
            rows.truncate(self.k);
            self.output = Some(MaterializedOperator::new(rows, self.batch_size));
        }
        self.output
            .as_mut()
            .expect("output initialized")
            .next_batch()
    }
}

struct JoinOperator<'a> {
    left: Option<Box<dyn PhysicalOperator + 'a>>,
    right: Option<Box<dyn PhysicalOperator + 'a>>,
    output: Option<MaterializedOperator>,
    on: JoinKey,
    batch_size: usize,
}

impl PhysicalOperator for JoinOperator<'_> {
    fn next_batch(&mut self) -> Result<Option<RecordBatch>> {
        if self.output.is_none() {
            let mut left = self
                .left
                .take()
                .ok_or_else(|| Error::InvalidArgument("join left input already consumed".into()))?;
            let mut right = self.right.take().ok_or_else(|| {
                Error::InvalidArgument("join right input already consumed".into())
            })?;
            let rows = join_rows(
                materialize_operator(&mut *left)?,
                materialize_operator(&mut *right)?,
                &self.on,
            );
            self.output = Some(MaterializedOperator::new(rows, self.batch_size));
        }
        self.output
            .as_mut()
            .expect("output initialized")
            .next_batch()
    }
}

struct AggregateOperator<'a> {
    input: Option<Box<dyn PhysicalOperator + 'a>>,
    output: Option<MaterializedOperator>,
    group_by: Vec<FieldRef>,
    aggregate: AggregateExpr,
    batch_size: usize,
}

impl PhysicalOperator for AggregateOperator<'_> {
    fn next_batch(&mut self) -> Result<Option<RecordBatch>> {
        if self.output.is_none() {
            let mut input = self
                .input
                .take()
                .ok_or_else(|| Error::InvalidArgument("aggregate input already consumed".into()))?;
            let rows = aggregate_records(
                materialize_operator(&mut *input)?,
                &self.group_by,
                &self.aggregate,
            );
            self.output = Some(MaterializedOperator::new(rows, self.batch_size));
        }
        self.output
            .as_mut()
            .expect("output initialized")
            .next_batch()
    }
}

struct MaterializedOperator {
    rows: Vec<Record>,
    cursor: usize,
    batch_size: usize,
}

impl MaterializedOperator {
    fn new(rows: Vec<Record>, batch_size: usize) -> Self {
        Self {
            rows,
            cursor: 0,
            batch_size: batch_size.max(1),
        }
    }
}

impl PhysicalOperator for MaterializedOperator {
    fn next_batch(&mut self) -> Result<Option<RecordBatch>> {
        if self.cursor >= self.rows.len() {
            return Ok(None);
        }

        let end = (self.cursor + self.batch_size).min(self.rows.len());
        let batch = self.rows[self.cursor..end].to_vec();
        self.cursor = end;
        Ok(Some(RecordBatch::new(batch)))
    }
}

fn materialize_operator(op: &mut dyn PhysicalOperator) -> Result<Vec<Record>> {
    let mut rows = Vec::new();
    while let Some(batch) = op.next_batch()? {
        rows.extend(batch.rows);
    }
    Ok(rows)
}

fn vector_search_rows(ctx: &ExecutionContext, k: usize, filter: Option<&Predicate>) -> Vec<Record> {
    let mut rows: Vec<_> = ctx
        .nodes
        .iter()
        .filter(|record| {
            filter
                .map(|pred| predicate_matches(record, pred))
                .unwrap_or(true)
        })
        .cloned()
        .collect();
    rows.sort_by(score_desc);
    rows.truncate(k);
    rows
}

fn graph_expand_rows(
    ctx: &ExecutionContext,
    seeds: Vec<Record>,
    relation: Option<&str>,
    depth: u8,
) -> Vec<Record> {
    let node_by_id: BTreeMap<_, _> = ctx
        .nodes
        .iter()
        .map(|record| (record.node_id.as_str(), record))
        .collect();
    let mut out = Vec::new();
    let mut emitted = HashSet::new();

    for seed in seeds {
        if emitted.insert(seed.node_id.clone()) {
            out.push(seed.clone());
        }

        let mut frontier = VecDeque::from([(seed.node_id.clone(), 0_u8)]);
        let mut local_seen = HashSet::from([seed.node_id]);
        while let Some((current, hop)) = frontier.pop_front() {
            if hop >= depth {
                continue;
            }
            for edge in ctx.edges.iter().filter(|edge| edge.src == current) {
                if relation.is_some_and(|want| edge.relation != want) {
                    continue;
                }
                if !local_seen.insert(edge.dst.clone()) {
                    continue;
                }
                if let Some(record) = node_by_id.get(edge.dst.as_str()) {
                    if emitted.insert(record.node_id.clone()) {
                        out.push((*record).clone());
                    }
                }
                frontier.push_back((edge.dst.clone(), hop + 1));
            }
        }
    }

    out
}

fn join_rows(left_rows: Vec<Record>, right_rows: Vec<Record>, on: &JoinKey) -> Vec<Record> {
    let right_keys: HashSet<String> = right_rows
        .iter()
        .filter_map(|record| join_value(record, on))
        .collect();
    left_rows
        .into_iter()
        .filter(|record| {
            join_value(record, on)
                .map(|key| right_keys.contains(&key))
                .unwrap_or(false)
        })
        .collect()
}

pub fn aggregate_records(
    rows: Vec<Record>,
    group_by: &[FieldRef],
    aggregate: &AggregateExpr,
) -> Vec<Record> {
    let mut groups: BTreeMap<Vec<Literal>, i64> = BTreeMap::new();
    for record in &rows {
        let Some(key) = aggregate_group_key(record, group_by) else {
            continue;
        };
        *groups.entry(key).or_default() += 1;
    }

    groups
        .into_iter()
        .map(|(key, count)| {
            let mut fields = BTreeMap::new();
            for (field, literal) in group_by.iter().zip(key.iter()) {
                fields.insert(field_label(field), literal.clone());
            }
            match aggregate {
                AggregateExpr::Count => {
                    fields.insert("count".to_string(), Literal::I64(count));
                }
            }
            Record {
                node_id: aggregate_node_id(group_by, &key),
                tenant: 0,
                kind: NodeKind::Fact,
                created_at_ms: 0,
                updated_at_ms: 0,
                score: count as f32,
                fields,
            }
        })
        .collect()
}

fn aggregate_group_key(record: &Record, group_by: &[FieldRef]) -> Option<Vec<Literal>> {
    group_by
        .iter()
        .map(|field| field_literal(record, field))
        .collect()
}

fn project_records(rows: Vec<Record>, fields: &[FieldRef]) -> Vec<Record> {
    rows.into_iter()
        .map(|record| project_record(record, fields))
        .collect()
}

fn project_record(mut record: Record, fields: &[FieldRef]) -> Record {
    let mut projected = BTreeMap::new();
    for field in fields {
        if let Some(literal) = field_literal(&record, field) {
            projected.insert(field_label(field), literal);
        }
    }
    record.fields = projected;
    record
}

fn aggregate_node_id(group_by: &[FieldRef], key: &[Literal]) -> String {
    if group_by.is_empty() {
        return "__aggregate__".to_string();
    }
    group_by
        .iter()
        .zip(key.iter())
        .map(|(field, literal)| format!("{}={literal:?}", field_label(field)))
        .collect::<Vec<_>>()
        .join("|")
}

fn field_label(field: &FieldRef) -> String {
    match field {
        FieldRef::Tenant => "tenant".to_string(),
        FieldRef::Kind => "kind".to_string(),
        FieldRef::CreatedAtMs => "created_at_ms".to_string(),
        FieldRef::UpdatedAtMs => "updated_at_ms".to_string(),
        FieldRef::Metadata(key) => key.clone(),
        FieldRef::Content => "content".to_string(),
        FieldRef::Score => "score".to_string(),
        FieldRef::NodeId => "node_id".to_string(),
    }
}

fn sort_rows(rows: &mut [Record], by: &SortKey) {
    match by {
        SortKey::ScoreDesc => rows.sort_by(score_desc),
        SortKey::CreatedAtDesc => rows.sort_by_key(|row| std::cmp::Reverse(row.created_at_ms)),
        SortKey::Field(field) => rows.sort_by(|left, right| {
            match (field_literal(left, field), field_literal(right, field)) {
                (Some(left), Some(right)) => left.cmp(&right),
                (Some(_), None) => std::cmp::Ordering::Less,
                (None, Some(_)) => std::cmp::Ordering::Greater,
                (None, None) => std::cmp::Ordering::Equal,
            }
        }),
    }
}

fn reserve_metric_id(metrics: &RowMetrics, next_id: &mut usize) -> usize {
    let id = *next_id;
    *next_id += 1;
    let mut rows = metrics.borrow_mut();
    if rows.len() <= id {
        rows.resize(id + 1, 0);
    }
    id
}

fn attach_actual_rows(node: &mut ExplainNode, actual_rows: &[usize], cursor: &mut usize) {
    let id = *cursor;
    *cursor += 1;
    node.actual_rows = actual_rows.get(id).copied();
    for child in &mut node.children {
        attach_actual_rows(child, actual_rows, cursor);
    }
}

fn physical_operator_name(plan: &LogicalPlan) -> &'static str {
    match plan {
        LogicalPlan::Scan { table, .. } => match table {
            TableId::Nodes => "ScanOp",
            TableId::Edges | TableId::Blobs => "IndexLookupOp",
        },
        LogicalPlan::VectorSearch { .. } => "HnswSearchOp",
        LogicalPlan::GraphExpand { .. } => "GraphExpandOp",
        LogicalPlan::Filter { .. } => "FilterOp",
        LogicalPlan::Score { .. } => "ScoreOp",
        LogicalPlan::TopK { .. } => "TopKOp",
        LogicalPlan::Join { .. } => "HashJoinOp",
        LogicalPlan::Aggregate { .. } => "AggregateOp",
        LogicalPlan::Project { .. } => "ProjectOp",
        LogicalPlan::Udf { .. } => "UdfOp",
    }
}

fn source_physical_operator_name(plan: &LogicalPlan) -> &'static str {
    match plan {
        LogicalPlan::Scan { .. } => "RangeScanOp",
        LogicalPlan::VectorSearch { .. } => "HnswSearchOp",
        LogicalPlan::GraphExpand { .. } => "GraphExpandOp",
        LogicalPlan::Filter { .. } => "FilterOp",
        LogicalPlan::Score { .. } => "ScoreOp",
        LogicalPlan::TopK { .. } => "TopKOp",
        LogicalPlan::Join { .. } => "HashJoinOp",
        LogicalPlan::Aggregate { .. } => "AggregateOp",
        LogicalPlan::Project { .. } => "ProjectOp",
        LogicalPlan::Udf { .. } => "UdfOp",
    }
}

fn explain_shape_with_estimate(
    plan: &LogicalPlan,
    stats: &Stats,
    source_names: bool,
) -> (ExplainNode, Option<usize>) {
    let mut children = Vec::new();
    let estimated_rows = match plan {
        LogicalPlan::Scan { table, filter } => {
            let base_rows = match table {
                TableId::Nodes => Some(stats.node_rows),
                TableId::Edges | TableId::Blobs => None,
            };
            estimate_filter_rows(base_rows, filter.as_ref(), stats)
        }
        LogicalPlan::VectorSearch { k, filter, .. } => {
            let estimated = estimate_filter_rows(Some(stats.node_rows), filter.as_ref(), stats);
            estimated.map(|rows| rows.min(*k))
        }
        LogicalPlan::GraphExpand { from, depth, .. } => {
            let (child, child_estimate) = explain_shape_with_estimate(from, stats, source_names);
            children.push(child);
            child_estimate.map(|rows| rows.saturating_mul((*depth as usize).saturating_add(1)))
        }
        LogicalPlan::Filter { input, pred } => {
            let (child, child_estimate) = explain_shape_with_estimate(input, stats, source_names);
            children.push(child);
            estimate_rows_by_selectivity(child_estimate, stats.estimate_selectivity(pred))
        }
        LogicalPlan::Score { input, .. }
        | LogicalPlan::Project { input, .. }
        | LogicalPlan::Udf { input, .. } => {
            let (child, child_estimate) = explain_shape_with_estimate(input, stats, source_names);
            children.push(child);
            child_estimate
        }
        LogicalPlan::Aggregate { input, .. } => {
            let (child, child_estimate) = explain_shape_with_estimate(input, stats, source_names);
            children.push(child);
            child_estimate
        }
        LogicalPlan::TopK { input, k, .. } => {
            let (child, child_estimate) = explain_shape_with_estimate(input, stats, source_names);
            children.push(child);
            child_estimate.map(|rows| rows.min(*k))
        }
        LogicalPlan::Join { left, right, on } => {
            let (left_child, left_estimate) =
                explain_shape_with_estimate(left, stats, source_names);
            let (right_child, right_estimate) =
                explain_shape_with_estimate(right, stats, source_names);
            children.push(left_child);
            children.push(right_child);
            let strategy = Optimizer::with_stats(stats.clone()).choose_join_strategy(
                left_estimate,
                right_estimate,
                on,
            );
            let estimated_rows = left_estimate.zip(right_estimate).map(|(l, r)| l.min(r));
            return (
                ExplainNode {
                    operator: join_operator_name(strategy).to_string(),
                    estimated_rows,
                    actual_rows: None,
                    children,
                },
                estimated_rows,
            );
        }
    };
    let operator = if source_names {
        source_physical_operator_name(plan)
    } else {
        physical_operator_name(plan)
    };
    (
        ExplainNode {
            operator: operator.to_string(),
            estimated_rows,
            actual_rows: None,
            children,
        },
        estimated_rows,
    )
}

const MAX_JOIN_ORDER_LEAVES: usize = 6;

fn collect_same_key_join_leaves(
    plan: &LogicalPlan,
    join_key: &JoinKey,
    leaves: &mut Vec<LogicalPlan>,
) -> bool {
    match plan {
        LogicalPlan::Join { left, right, on } if on == join_key => {
            collect_same_key_join_leaves(left, join_key, leaves)
                && collect_same_key_join_leaves(right, join_key, leaves)
        }
        LogicalPlan::Join { .. } => false,
        other => {
            leaves.push(other.clone());
            true
        }
    }
}

fn estimated_rows_for_plan(plan: &LogicalPlan, stats: &Stats) -> Option<usize> {
    explain_shape_with_estimate(plan, stats, false).1
}

fn is_safe_scan_join_leaf(plan: &LogicalPlan) -> bool {
    matches!(
        plan,
        LogicalPlan::Scan {
            table: TableId::Nodes,
            ..
        }
    )
}

fn is_score_preserving_join_anchor(plan: &LogicalPlan) -> bool {
    match plan {
        LogicalPlan::VectorSearch { .. } | LogicalPlan::GraphExpand { .. } => true,
        LogicalPlan::Score { .. } | LogicalPlan::Udf { .. } => true,
        LogicalPlan::TopK { input, .. }
        | LogicalPlan::Project { input, .. }
        | LogicalPlan::Filter { input, .. } => is_score_preserving_join_anchor(input),
        LogicalPlan::Scan { .. } | LogicalPlan::Join { .. } | LogicalPlan::Aggregate { .. } => {
            false
        }
    }
}

fn build_left_deep_join(mut leaves: Vec<LogicalPlan>, on: JoinKey) -> Option<LogicalPlan> {
    let mut iter = leaves.drain(..);
    let first = iter.next()?;
    let second = iter.next()?;
    let mut plan = LogicalPlan::Join {
        left: Box::new(first),
        right: Box::new(second),
        on: on.clone(),
    };
    for leaf in iter {
        plan = LogicalPlan::Join {
            left: Box::new(plan),
            right: Box::new(leaf),
            on: on.clone(),
        };
    }
    Some(plan)
}

fn permute_join_orders(start: usize, order: &mut [usize], out: &mut Vec<Vec<usize>>) {
    if start == order.len() {
        out.push(order.to_vec());
        return;
    }
    for i in start..order.len() {
        order.swap(start, i);
        permute_join_orders(start + 1, order, out);
        order.swap(start, i);
    }
}

fn join_order_cost(estimates: &[Option<usize>]) -> usize {
    let Some(Some(mut current_rows)) = estimates.first().copied() else {
        return usize::MAX;
    };
    let mut cost = 0_usize;
    for estimate in &estimates[1..] {
        let Some(next_rows) = estimate else {
            return usize::MAX;
        };
        cost = cost.saturating_add(current_rows).saturating_add(*next_rows);
        current_rows = current_rows.min(*next_rows);
    }
    cost
}

fn choose_join_strategy(left_rows: usize, right_rows: usize, on: &JoinKey) -> JoinStrategy {
    let (small, large, small_is_left) = if left_rows <= right_rows {
        (left_rows, right_rows, true)
    } else {
        (right_rows, left_rows, false)
    };

    if small.saturating_mul(4) <= large || small <= 128 {
        if small_is_left {
            JoinStrategy::HashBuildLeft
        } else {
            JoinStrategy::HashBuildRight
        }
    } else if matches!(on, JoinKey::NodeId | JoinKey::Field(_)) && left_rows + right_rows >= 1_000 {
        JoinStrategy::Merge
    } else {
        JoinStrategy::HashBuildRight
    }
}

fn join_operator_name(strategy: JoinStrategy) -> &'static str {
    match strategy {
        JoinStrategy::HashBuildLeft | JoinStrategy::HashBuildRight => "HashJoinOp",
        JoinStrategy::Merge => "MergeJoinOp",
    }
}

fn estimate_filter_rows(
    base_rows: Option<usize>,
    filter: Option<&Predicate>,
    stats: &Stats,
) -> Option<usize> {
    match filter {
        Some(pred) => estimate_rows_by_selectivity(base_rows, stats.estimate_selectivity(pred)),
        None => base_rows,
    }
}

fn estimate_rows_by_selectivity(base_rows: Option<usize>, selectivity: f32) -> Option<usize> {
    base_rows.map(|rows| ((rows as f32) * selectivity.clamp(0.0, 1.0)).round() as usize)
}

fn predicate_matches(record: &Record, pred: &Predicate) -> bool {
    match pred {
        Predicate::Eq(field, literal) => field_literal(record, field).as_ref() == Some(literal),
        Predicate::Gt(field, literal) => compare_i64(record, field, literal, |a, b| a > b),
        Predicate::Gte(field, literal) => compare_i64(record, field, literal, |a, b| a >= b),
        Predicate::Lt(field, literal) => compare_i64(record, field, literal, |a, b| a < b),
        Predicate::Lte(field, literal) => compare_i64(record, field, literal, |a, b| a <= b),
        Predicate::In(field, literals) => field_literal(record, field)
            .map(|value| literals.contains(&value))
            .unwrap_or(false),
        Predicate::And(preds) => preds.iter().all(|pred| predicate_matches(record, pred)),
        Predicate::Or(preds) => preds.iter().any(|pred| predicate_matches(record, pred)),
        Predicate::Not(pred) => !predicate_matches(record, pred),
    }
}

fn field_literal(record: &Record, field: &FieldRef) -> Option<Literal> {
    match field {
        FieldRef::Tenant => Some(Literal::U32(record.tenant)),
        FieldRef::Kind => Some(Literal::NodeKind(record.kind)),
        FieldRef::CreatedAtMs => Some(Literal::I64(record.created_at_ms)),
        FieldRef::Score => Some(Literal::F32(OrderedF32(record.score))),
        FieldRef::NodeId => Some(Literal::String(record.node_id.clone())),
        FieldRef::UpdatedAtMs => Some(Literal::I64(record.updated_at_ms)),
        FieldRef::Content => record.fields.get("content").cloned(),
        FieldRef::Metadata(key) => record.fields.get(key).cloned(),
    }
}

fn compare_i64(
    record: &Record,
    field: &FieldRef,
    literal: &Literal,
    cmp: impl FnOnce(i64, i64) -> bool,
) -> bool {
    if let (FieldRef::Score, Literal::F32(rhs)) = (field, literal) {
        return cmp_f32(record.score, rhs.0, cmp);
    }
    let Some(Literal::I64(lhs)) = field_literal(record, field) else {
        return false;
    };
    let Literal::I64(rhs) = literal else {
        return false;
    };
    cmp(lhs, *rhs)
}

fn cmp_f32(lhs: f32, rhs: f32, cmp: impl FnOnce(i64, i64) -> bool) -> bool {
    match lhs.total_cmp(&rhs) {
        std::cmp::Ordering::Less => cmp(0, 1),
        std::cmp::Ordering::Equal => cmp(0, 0),
        std::cmp::Ordering::Greater => cmp(1, 0),
    }
}

fn eval_score(record: &Record, expr: &ScoreExpr) -> f32 {
    match expr {
        ScoreExpr::Similarity => record.score,
        ScoreExpr::Decay { .. } => record.score,
        ScoreExpr::Literal(v) => *v,
        ScoreExpr::Mul(left, right) => eval_score(record, left) * eval_score(record, right),
        ScoreExpr::Add(left, right) => eval_score(record, left) + eval_score(record, right),
    }
}

fn join_value(record: &Record, on: &JoinKey) -> Option<String> {
    match on {
        JoinKey::NodeId => Some(record.node_id.clone()),
        JoinKey::Field(field) => field_literal(record, field).map(|literal| format!("{literal:?}")),
    }
}

fn score_desc(a: &Record, b: &Record) -> std::cmp::Ordering {
    b.score
        .partial_cmp(&a.score)
        .unwrap_or(std::cmp::Ordering::Equal)
}

fn combined_recall_filter(tenant: Option<u32>, filters: Vec<Predicate>) -> Option<Predicate> {
    let mut predicates = Vec::with_capacity(filters.len() + usize::from(tenant.is_some()));
    if let Some(tenant) = tenant {
        predicates.push(Predicate::eq(FieldRef::Tenant, Literal::U32(tenant)));
    }
    predicates.extend(filters);
    match predicates.len() {
        0 => None,
        1 => predicates.into_iter().next(),
        _ => Some(Predicate::and(predicates)),
    }
}

fn combine_optional_filter(existing: Option<Predicate>, pred: Predicate) -> Predicate {
    match existing {
        Some(existing) => Predicate::and([existing, pred]),
        None => pred,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mmdb_core::NodeKind;

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
            ("score".to_string(), Literal::F32(OrderedF32(0.75))),
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
        calls: std::cell::RefCell<Vec<&'static str>>,
    }

    impl QuerySource for RecordingSource {
        fn range_scan(&self, _table: &TableId, filter: Option<&Predicate>) -> Result<Vec<Record>> {
            self.calls.borrow_mut().push("range_scan");
            Ok(self
                .range_rows
                .iter()
                .filter(|record| {
                    filter
                        .map(|pred| predicate_matches(record, pred))
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
        let mut rows = Vec::new();
        while let Some(batch) = op.next_batch()? {
            rows.extend(batch.rows);
        }
        Ok(rows)
    }
}
