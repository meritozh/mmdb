//! LogicalPlan IR + recall builder + rule optimizer + batch physical executor.

mod ir;
mod builder;
mod optimizer;
mod eval;
mod executor;
mod explain;

#[cfg(test)]
mod tests;

// --- IR types ---
pub use ir::{
    AggregateExpr, Expr, FieldHistogram, FieldRef, JoinKey, JoinOrderCandidate, JoinStrategy,
    Literal, LogicalPlan, ModelId, OrderedF32, Predicate, ScoreExpr, SortKey, Stats, TableId,
    VectorRef,
};

// --- Builder types ---
pub use builder::{Query, RecallBuilder, VectorRecallBuilder};

// --- Optimizer ---
pub use optimizer::Optimizer;

// --- Evaluator public helpers ---
pub use eval::aggregate_records;

// --- Executor types and traits ---
pub use executor::{
    EdgeRecord, ExecutionContext, Executor, PhysicalOperator, QuerySource, Record, RecordBatch,
    SourceExecutor, UdfFn,
};

// --- Explain types ---
pub use explain::ExplainNode;
