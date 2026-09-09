//! A deterministic pair of harnesses sharing a context store across reopen.
use mmdb::context::*;
use mmdb::native_memory::{Actor, MemoryDatabase, OperationId, Scope, TemporalFacts};
use serde_json::json;
use std::collections::BTreeMap;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let temporary = tempfile::tempdir()?;
    let path = temporary.path().join("context");
    let (saved, relation) = {
        let db = MemoryDatabase::create_context(&path)?;
        let mut access = ContextAccess::new("owner", Actor::System);
        access.agent = Some("research".into());
        let context = db.context(access)?;
        let evidence = context.append_history(
            OperationId::new(),
            HistoryInput {
                sources: Vec::new(),
                session: "research-session".into(),
                event_id: "user-message-1".into(),
                scope: Scope::Personal,
                kind: HistoryKind::UserMessage,
                media_type: "text/plain".into(),
                occurred_at_ms: 1,
                expected_sequence: Some(0),
            },
            b"A capacity constraint affects the next trading decision.".as_slice(),
        )?;
        let note_type = context.define_type(
            OperationId::new(),
            TypeDefinition {
                name: "research.observation".into(),
                scope: Scope::Personal,
                kind: TypeKind::Object,
                properties: BTreeMap::from([(
                    "text".into(),
                    PropertyDefinition {
                        value_type: PropertyType::Text,
                        required: true,
                    },
                )]),
            },
        )?;
        let observation = context.save_object(
            OperationId::new(),
            ObjectInput {
                type_pin: note_type.pin.clone(),
                scope: Scope::Personal,
                properties: BTreeMap::from([(
                    "text".into(),
                    json!("Check the capacity constraint before planning the trade."),
                )]),
                sources: vec![evidence.pin.clone()],
                information: InformationKind::Inference,
                temporal: TemporalFacts::observed_at(1),
            },
        )?;
        let saved = context.save_object(
            OperationId::new(),
            ObjectInput {
                type_pin: note_type.pin.clone(),
                scope: Scope::Personal,
                properties: BTreeMap::from([(
                    "text".into(),
                    json!("Reconsider the target allocation."),
                )]),
                sources: vec![evidence.pin.clone()],
                information: InformationKind::Inference,
                temporal: TemporalFacts::observed_at(1),
            },
        )?;
        let relation_type = context.define_type(
            OperationId::new(),
            TypeDefinition {
                name: "research.informs".into(),
                scope: Scope::Personal,
                kind: TypeKind::Relation {
                    from_types: vec![note_type.pin.record.clone()],
                    to_types: vec![note_type.pin.record],
                },
                properties: BTreeMap::new(),
            },
        )?;
        let relation = context.save_relation(
            OperationId::new(),
            RelationInput {
                type_pin: relation_type.pin,
                scope: Scope::Personal,
                from: observation.pin.record,
                to: saved.pin.record.clone(),
                properties: BTreeMap::new(),
                sources: vec![evidence.pin],
                information: InformationKind::Inference,
                temporal: TemporalFacts::observed_at(1),
            },
        )?;
        (saved, relation)
    };
    let db = MemoryDatabase::open_context(&path)?;
    let mut access = ContextAccess::new("owner", Actor::Assistant);
    access.agent = Some("trading".into());
    access.session = Some("trading-session".into());
    let context = db.context(access)?;
    let (_, definition) = context
        .find_type(&Scope::Personal, "research.observation")?
        .ok_or("shared type was not found")?;
    let recall = context.recall(ContextQuery {
        text: "capacity".into(),
        seeds: Vec::new(),
        valid_at_ms: None,
        budget: RecallBudget::default(),
    })?;
    let hit = recall
        .hits
        .iter()
        .find(|hit| hit.record.header.pin == saved.pin)
        .ok_or("saved context was not recalled after reopen")?;
    if hit.path != vec![relation.pin] {
        return Err("indirect record did not retain its relation path".into());
    }
    let raw = context.read_payload(&hit.record.header.sources[0], 0, 4096)?;
    println!(
        "Trading recalled record {} at revision {} from agent {:?}.",
        saved.pin.record.id, saved.pin.revision, hit.record.header.provenance.agent
    );
    println!("Source: {}", String::from_utf8(raw.bytes)?);
    println!(
        "Reused type: {}; relation hops: {}",
        definition.name,
        hit.path.len()
    );
    println!(
        "Original session history position: {}",
        context.history_position("research-session")?
    );
    Ok(())
}
