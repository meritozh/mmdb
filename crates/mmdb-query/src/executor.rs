use crate::eval::{
    aggregate_records, eval_score, join_value, predicate_matches, project_records, score_desc,
    sort_rows,
};
use crate::explain::{explain_shape_with_estimate, physical_operator_name, ExplainNode};
use crate::ir::{
    AggregateExpr, Expr, FieldRef, JoinKey, Literal, LogicalPlan, ModelId, Predicate, ScoreExpr,
    SortKey, Stats, TableId, VectorRef,
};
use crate::optimizer::{estimate_filter_rows, join_operator_name, Optimizer};
use mmdb_core::{Error, NodeKind, Result};
use std::cell::RefCell;
use std::collections::{BTreeMap, HashSet, VecDeque};
use std::rc::Rc;
use std::sync::Arc;

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
    pub(crate) udfs: BTreeMap<String, Arc<UdfFn>>,
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
                crate::optimizer::estimate_rows_by_selectivity(
                    child_estimate,
                    stats.estimate_selectivity(pred),
                )
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

pub(crate) fn materialize_operator(op: &mut dyn PhysicalOperator) -> Result<Vec<Record>> {
    let mut rows = Vec::new();
    while let Some(batch) = op.next_batch()? {
        rows.extend(batch.rows);
    }
    Ok(rows)
}

pub(crate) fn vector_search_rows(
    ctx: &ExecutionContext,
    k: usize,
    filter: Option<&Predicate>,
) -> Vec<Record> {
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

pub(crate) fn graph_expand_rows(
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

pub(crate) fn join_rows(
    left_rows: Vec<Record>,
    right_rows: Vec<Record>,
    on: &JoinKey,
) -> Vec<Record> {
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

pub(crate) fn reserve_metric_id(metrics: &RowMetrics, next_id: &mut usize) -> usize {
    let id = *next_id;
    *next_id += 1;
    let mut rows = metrics.borrow_mut();
    if rows.len() <= id {
        rows.resize(id + 1, 0);
    }
    id
}

pub(crate) fn attach_actual_rows(
    node: &mut ExplainNode,
    actual_rows: &[usize],
    cursor: &mut usize,
) {
    let id = *cursor;
    *cursor += 1;
    node.actual_rows = actual_rows.get(id).copied();
    for child in &mut node.children {
        attach_actual_rows(child, actual_rows, cursor);
    }
}

