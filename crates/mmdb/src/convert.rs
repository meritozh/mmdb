use crate::embedder::Embedder;
use mmdb_core::{Content, MemoryNode, Result};

pub(crate) fn resolve_query_vector(
    query: &mmdb_query::VectorRef,
    model: &mmdb_query::ModelId,
    embedder: Option<&dyn Embedder>,
) -> Result<Vec<f32>> {
    match query {
        mmdb_query::VectorRef::Vector(vector) => Ok(vector.clone()),
        mmdb_query::VectorRef::Text(text) => {
            let embedder = embedder.ok_or_else(|| {
                mmdb_core::Error::InvalidArgument(
                    "text vector query requires an embedder (use Database::open_with_embedder)"
                        .into(),
                )
            })?;
            if embedder.model_name() != model.0 {
                return Err(mmdb_core::Error::InvalidArgument(format!(
                    "text vector query requested model `{}`, but configured embedder is `{}`",
                    model.0,
                    embedder.model_name()
                )));
            }
            embedder.embed(text)
        }
    }
}

pub(crate) fn node_to_query_record(node: MemoryNode) -> mmdb_query::Record {
    let mut record = mmdb_query::Record::new(
        node.id.to_string(),
        node.tenant,
        node.kind,
        node.created_at_ms,
    )
    .with_updated_at_ms(node.updated_at_ms);
    if let Some(literal) = content_to_query_literal(&node.content) {
        record = record.with_field("content", literal);
    }
    for (key, value) in node.metadata {
        if let Some(literal) = json_to_query_literal(value) {
            record = record.with_field(key, literal);
        }
    }
    record
}

pub(crate) fn content_to_query_literal(content: &Content) -> Option<mmdb_query::Literal> {
    match content {
        Content::Text(value) => Some(mmdb_query::Literal::String(value.clone())),
        Content::Structured(value) => json_to_query_literal(value.clone())
            .or_else(|| Some(mmdb_query::Literal::String(value.to_string()))),
        Content::Blob { size, mime, .. } => {
            Some(mmdb_query::Literal::String(format!("blob:{mime}:{size}")))
        }
    }
}

fn json_to_query_literal(value: serde_json::Value) -> Option<mmdb_query::Literal> {
    match value {
        serde_json::Value::String(value) => Some(mmdb_query::Literal::String(value)),
        serde_json::Value::Bool(value) => Some(mmdb_query::Literal::Bool(value)),
        serde_json::Value::Number(value) => {
            value.as_i64().map(mmdb_query::Literal::I64).or_else(|| {
                value
                    .as_u64()
                    .and_then(|v| u32::try_from(v).ok())
                    .map(mmdb_query::Literal::U32)
            })
        }
        serde_json::Value::Null | serde_json::Value::Array(_) | serde_json::Value::Object(_) => {
            None
        }
    }
}

pub(crate) fn query_predicate_matches(
    record: &mmdb_query::Record,
    pred: &mmdb_query::Predicate,
) -> bool {
    match pred {
        mmdb_query::Predicate::Eq(field, literal) => {
            query_field_literal(record, field).as_ref() == Some(literal)
        }
        mmdb_query::Predicate::Gt(field, literal) => {
            query_compare_i64(record, field, literal, |a, b| a > b)
        }
        mmdb_query::Predicate::Gte(field, literal) => {
            query_compare_i64(record, field, literal, |a, b| a >= b)
        }
        mmdb_query::Predicate::Lt(field, literal) => {
            query_compare_i64(record, field, literal, |a, b| a < b)
        }
        mmdb_query::Predicate::Lte(field, literal) => {
            query_compare_i64(record, field, literal, |a, b| a <= b)
        }
        mmdb_query::Predicate::In(field, literals) => query_field_literal(record, field)
            .map(|value| literals.contains(&value))
            .unwrap_or(false),
        mmdb_query::Predicate::And(preds) => preds
            .iter()
            .all(|pred| query_predicate_matches(record, pred)),
        mmdb_query::Predicate::Or(preds) => preds
            .iter()
            .any(|pred| query_predicate_matches(record, pred)),
        mmdb_query::Predicate::Not(pred) => !query_predicate_matches(record, pred),
    }
}

fn query_field_literal(
    record: &mmdb_query::Record,
    field: &mmdb_query::FieldRef,
) -> Option<mmdb_query::Literal> {
    match field {
        mmdb_query::FieldRef::Tenant => Some(mmdb_query::Literal::U32(record.tenant)),
        mmdb_query::FieldRef::Kind => Some(mmdb_query::Literal::NodeKind(record.kind)),
        mmdb_query::FieldRef::CreatedAtMs => Some(mmdb_query::Literal::I64(record.created_at_ms)),
        mmdb_query::FieldRef::Score => Some(mmdb_query::Literal::F32(mmdb_query::OrderedF32(
            record.score,
        ))),
        mmdb_query::FieldRef::NodeId => Some(mmdb_query::Literal::String(record.node_id.clone())),
        mmdb_query::FieldRef::UpdatedAtMs => Some(mmdb_query::Literal::I64(record.updated_at_ms)),
        mmdb_query::FieldRef::Content => record.fields.get("content").cloned(),
        mmdb_query::FieldRef::Metadata(key) => record.fields.get(key).cloned(),
    }
}

fn query_compare_i64(
    record: &mmdb_query::Record,
    field: &mmdb_query::FieldRef,
    literal: &mmdb_query::Literal,
    cmp: impl FnOnce(i64, i64) -> bool,
) -> bool {
    if let (mmdb_query::FieldRef::Score, mmdb_query::Literal::F32(rhs)) = (field, literal) {
        return cmp_f32(record.score, rhs.0, cmp);
    }
    let Some(mmdb_query::Literal::I64(lhs)) = query_field_literal(record, field) else {
        return false;
    };
    let mmdb_query::Literal::I64(rhs) = literal else {
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
