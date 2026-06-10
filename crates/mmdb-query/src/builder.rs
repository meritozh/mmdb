use crate::ir::{
    FieldRef, Literal, LogicalPlan, ModelId, Predicate, SortKey, TableId, VectorRef,
};

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

pub(crate) fn combined_recall_filter(
    tenant: Option<u32>,
    filters: Vec<Predicate>,
) -> Option<Predicate> {
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

pub(crate) fn combine_optional_filter(existing: Option<Predicate>, pred: Predicate) -> Predicate {
    match existing {
        Some(existing) => Predicate::and([existing, pred]),
        None => pred,
    }
}
