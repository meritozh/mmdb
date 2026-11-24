use crate::records::*;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;

pub type Properties = BTreeMap<String, Value>;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum PropertyType {
    Text,
    Boolean,
    Integer,
    Number,
    Timestamp,
    Reference { target_type: Option<ContextRef> },
    List { element: Box<PropertyType> },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PropertyDefinition {
    pub value_type: PropertyType,
    pub required: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum TypeKind {
    Object,
    Relation {
        from_types: Vec<ContextRef>,
        to_types: Vec<ContextRef>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TypeDefinition {
    pub name: String,
    pub scope: Scope,
    pub kind: TypeKind,
    pub properties: BTreeMap<String, PropertyDefinition>,
}

impl TypeDefinition {
    /// An optional starting schema, registered and versioned through `define_type`
    /// exactly like application-defined types. Creating a handle does not write it.
    pub fn note(scope: Scope) -> Self {
        Self {
            name: "mmdb.note".into(),
            scope,
            kind: TypeKind::Object,
            properties: BTreeMap::from([
                (
                    "title".into(),
                    PropertyDefinition {
                        value_type: PropertyType::Text,
                        required: false,
                    },
                ),
                (
                    "text".into(),
                    PropertyDefinition {
                        value_type: PropertyType::Text,
                        required: true,
                    },
                ),
            ]),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ObjectInput {
    pub type_pin: RecordPin,
    pub scope: Scope,
    pub properties: Properties,
    pub sources: Vec<RecordPin>,
    pub information: InformationKind,
    pub temporal: TemporalFacts,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RelationInput {
    pub type_pin: RecordPin,
    pub scope: Scope,
    pub from: ContextRef,
    pub to: ContextRef,
    pub properties: Properties,
    pub sources: Vec<RecordPin>,
    pub information: InformationKind,
    pub temporal: TemporalFacts,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
pub enum RecordBody {
    Type(TypeDefinition),
    Object(Properties),
    Relation(Properties),
    Action(ActionDefinition),
    ActionExecution(ActionExecution),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ContextRecord {
    pub header: RecordHeader,
    pub current_state: RecordState,
    pub body: RecordBody,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TypePage {
    pub definitions: Vec<(RecordPin, TypeDefinition)>,
    pub next: Option<ContextRef>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RecallBudget {
    pub max_candidates: usize,
    pub max_edges: usize,
    pub max_depth: usize,
    pub max_results: usize,
}

impl Default for RecallBudget {
    fn default() -> Self {
        Self {
            max_candidates: 4096,
            max_edges: 512,
            max_depth: 2,
            max_results: 12,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContextQuery {
    pub text: String,
    pub seeds: Vec<ContextRef>,
    pub valid_at_ms: Option<i64>,
    pub budget: RecallBudget,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ContextHit {
    pub record: ContextRecord,
    pub matched_terms: usize,
    pub path: Vec<RecordPin>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ContextRecall {
    pub hits: Vec<ContextHit>,
    pub candidates_examined: usize,
    pub edges_examined: usize,
    pub truncated: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RelationDirection {
    Incoming,
    Outgoing,
    Both,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RelationCursor {
    pub(super) query_digest: [u8; 32],
    pub(super) key: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RelationQuery {
    pub endpoint: ContextRef,
    pub direction: RelationDirection,
    pub type_filter: Option<ContextRef>,
    pub valid_at_ms: Option<i64>,
    pub cursor: Option<RelationCursor>,
    pub max_edges: usize,
    pub limit: usize,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RelationPage {
    pub relations: Vec<ContextRecord>,
    pub cursor: Option<RelationCursor>,
    pub edges_examined: usize,
}

/// Business semantics only. The harness owns tool bindings and authorization.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ActionDefinition {
    pub name: String,
    pub description: String,
    pub scope: Scope,
    pub target_type: RecordPin,
    pub inputs: BTreeMap<String, PropertyDefinition>,
    pub preconditions: Vec<ActionCondition>,
    pub sources: Vec<RecordPin>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ActionCondition {
    Equals { property: String, value: Value },
    Exists { property: String },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ActionOffer {
    pub pin: RecordPin,
    pub definition: ActionDefinition,
    pub preconditions_met: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ActionPage {
    pub actions: Vec<ActionOffer>,
    pub next: Option<ContextRef>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ActionInvocation {
    pub action: RecordPin,
    pub target: RecordPin,
    pub inputs: Properties,
    pub sources: Vec<RecordPin>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ActionStatus {
    Prepared,
    Succeeded,
    RejectedBeforeDispatch,
    FailedNoEffect,
    EffectUnknown,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ActionExecution {
    pub invocation: ActionInvocation,
    pub status: ActionStatus,
    pub result: Value,
    pub result_sources: Vec<RecordPin>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ActionPreparation {
    pub receipt: WriteReceipt,
    pub already_present: bool,
}
