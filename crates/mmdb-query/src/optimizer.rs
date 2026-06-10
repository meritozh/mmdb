use crate::builder::combine_optional_filter;
use crate::explain::explain_shape_with_estimate;
use crate::ir::{
    JoinKey, JoinOrderCandidate, JoinStrategy, LogicalPlan, Predicate, Stats, TableId,
};

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

const MAX_JOIN_ORDER_LEAVES: usize = 6;

pub(crate) fn collect_same_key_join_leaves(
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

pub(crate) fn estimated_rows_for_plan(plan: &LogicalPlan, stats: &Stats) -> Option<usize> {
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

pub(crate) fn join_operator_name(strategy: JoinStrategy) -> &'static str {
    match strategy {
        JoinStrategy::HashBuildLeft | JoinStrategy::HashBuildRight => "HashJoinOp",
        JoinStrategy::Merge => "MergeJoinOp",
    }
}

pub(crate) fn estimate_filter_rows(
    base_rows: Option<usize>,
    filter: Option<&Predicate>,
    stats: &Stats,
) -> Option<usize> {
    match filter {
        Some(pred) => estimate_rows_by_selectivity(base_rows, stats.estimate_selectivity(pred)),
        None => base_rows,
    }
}

pub(crate) fn estimate_rows_by_selectivity(
    base_rows: Option<usize>,
    selectivity: f32,
) -> Option<usize> {
    base_rows.map(|rows| ((rows as f32) * selectivity.clamp(0.0, 1.0)).round() as usize)
}

