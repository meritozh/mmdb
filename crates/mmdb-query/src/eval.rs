use crate::executor::Record;
use crate::ir::{
    AggregateExpr, FieldRef, JoinKey, Literal, OrderedF32, Predicate, ScoreExpr, SortKey,
};
use mmdb_core::NodeKind;
use std::collections::BTreeMap;

pub(crate) fn predicate_matches(record: &Record, pred: &Predicate) -> bool {
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

pub(crate) fn field_literal(record: &Record, field: &FieldRef) -> Option<Literal> {
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

pub(crate) fn eval_score(record: &Record, expr: &ScoreExpr) -> f32 {
    match expr {
        ScoreExpr::Similarity => record.score,
        ScoreExpr::Decay { .. } => record.score,
        ScoreExpr::Literal(v) => *v,
        ScoreExpr::Mul(left, right) => eval_score(record, left) * eval_score(record, right),
        ScoreExpr::Add(left, right) => eval_score(record, left) + eval_score(record, right),
    }
}

pub(crate) fn join_value(record: &Record, on: &JoinKey) -> Option<String> {
    match on {
        JoinKey::NodeId => Some(record.node_id.clone()),
        JoinKey::Field(field) => field_literal(record, field).map(|literal| format!("{literal:?}")),
    }
}

pub(crate) fn score_desc(a: &Record, b: &Record) -> std::cmp::Ordering {
    b.score
        .partial_cmp(&a.score)
        .unwrap_or(std::cmp::Ordering::Equal)
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

pub(crate) fn project_records(rows: Vec<Record>, fields: &[FieldRef]) -> Vec<Record> {
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

pub(crate) fn field_label(field: &FieldRef) -> String {
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

pub(crate) fn sort_rows(rows: &mut [Record], by: &SortKey) {
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
