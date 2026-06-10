use crate::ir::{LogicalPlan, Stats, TableId};
use crate::optimizer::{
    estimate_filter_rows, estimate_rows_by_selectivity, join_operator_name, Optimizer,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExplainNode {
    pub operator: String,
    pub estimated_rows: Option<usize>,
    pub actual_rows: Option<usize>,
    pub children: Vec<ExplainNode>,
}

pub(crate) fn physical_operator_name(plan: &LogicalPlan) -> &'static str {
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

pub(crate) fn source_physical_operator_name(plan: &LogicalPlan) -> &'static str {
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

pub(crate) fn explain_shape_with_estimate(
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

