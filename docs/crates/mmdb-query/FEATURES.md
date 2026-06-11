# mmdb-query Features

`mmdb-query` defines the shared query IR, optimizer, in-memory executor, and
source-backed physical execution boundary. It intentionally depends only on
`mmdb-core`.

## Responsibilities

- Represent scans, vector recall, graph expansion, joins, scores, aggregates,
  projections, and UDF calls as `LogicalPlan`.
- Provide a Rust recall builder.
- Optimize plans with rule-based rewrites and small cost/statistics hooks.
- Execute plans in memory for semantic tests.
- Execute plans against external sources through `QuerySource`.
- Produce explain trees with estimated and actual row counts.

## Logical Plan

Core plan variants:

- `Scan`
- `VectorSearch`
- `GraphExpand`
- `Filter`
- `Score`
- `TopK`
- `Join`
- `Aggregate`
- `Project`
- `Udf`

Supporting expression types include:

- `Predicate`
- `FieldRef`
- `Literal`
- `VectorRef`
- `ScoreExpr`
- `AggregateExpr`
- `JoinKey`
- `SortKey`

## Builder API

`Query::recall()` builds common vector-recall plans fluently:

```rust
let plan = mmdb_query::Query::recall()
    .tenant(0)
    .kind(mmdb_core::NodeKind::Fact)
    .vector(vec![1.0, 0.0, 0.0])
    .model("text")
    .topk(20)
    .limit(5)
    .build();
```

The builder lowers into the same `LogicalPlan` used by MMQL.

## Optimizer

`Optimizer` supports:

- filter pushdown into `Scan` and `VectorSearch`;
- predicate selectivity estimation through `Stats`;
- histogram-backed selectivity via `FieldHistogram`;
- join strategy choice (`HashBuildLeft`, `HashBuildRight`, `Merge`);
- bounded left-deep join-order candidates;
- preserving vector/graph/score anchors while reordering scan-filter suffixes.

## Execution

The crate has two execution modes:

1. In-memory `Executor` over `ExecutionContext`, useful for deterministic
   semantic tests.
2. Source-backed `SourceExecutor` over `QuerySource`, used by the facade to bind
   `Scan`, `VectorSearch`, and `GraphExpand` to real stores.

Physical execution uses `RecordBatch` and `PhysicalOperator::next_batch()`.
Streaming operators include scan, filter, score, and UDF. Blocking operators
include graph expand, top-k, and join.

## Records

`Record` is the lightweight row shape used by query execution. It carries node
identity, kind, timestamps, content/metadata fields, and optional score. It is
not the durable storage format; `MemoryNode` remains the persistent data model.

## EXPLAIN

`ExplainNode` records:

- physical operator name;
- estimated row count;
- actual row count after draining;
- child explain nodes.

Both in-memory and source-backed paths can produce explain output.

## Public Surface

- `LogicalPlan` and supporting IR types.
- `Query`, `RecallBuilder`, `VectorRecallBuilder`.
- `Optimizer`.
- `Executor`.
- `SourceExecutor`.
- `QuerySource`.
- `Record`, `RecordBatch`, `PhysicalOperator`.
- `ExplainNode`.

## Source Files

- `crates/mmdb-query/src/ir.rs`: IR and stats.
- `crates/mmdb-query/src/builder.rs`: Rust builder.
- `crates/mmdb-query/src/optimizer.rs`: rewrites and costing.
- `crates/mmdb-query/src/executor.rs`: in-memory and source-backed execution.
- `crates/mmdb-query/src/eval.rs`: expression and aggregate evaluation.
- `crates/mmdb-query/src/explain.rs`: explain tree.

