use mmdb_core::NodeKind;
use std::collections::{BTreeMap, BTreeSet};

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
