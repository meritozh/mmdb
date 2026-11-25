//! Built as an mmdb example only in the exported Fjall 2 baseline.
use anyhow::{ensure, Context, Result};
use mmdb::context::{
    ObjectInput, Properties, PropertyDefinition, PropertyType, RecordBody, TypeDefinition, TypeKind,
};
use mmdb::session::EventInput;
use mmdb::{
    Actor, ContextAccess, EventKind, InformationKind, OperationId, Scope, StateStore, Store,
    StoreFormatDescriptor, TemporalFacts, WriteReceipt,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::{collections::BTreeMap, env, fs, path::Path};

const OWNER: &str = "fixture-owner";
const SESSION: &str = "session-1";
const SOURCE: &[u8] = b"The sample was collected at noon.";
const STATE_FORMAT: &str = "fixture.runtime-v1";
const TENANT: u32 = 7;

#[derive(Serialize, Deserialize)]
struct Operation {
    id: OperationId,
    receipt: WriteReceipt,
}

#[derive(Serialize, Deserialize)]
struct Memory {
    source: Operation,
    object: Operation,
}

#[derive(Serialize, Deserialize)]
struct Row {
    namespace: String,
    key: String,
    value: Value,
    revision: u64,
}

#[derive(Serialize, Deserialize)]
struct Identities {
    memory: Memory,
    state: Vec<Row>,
}

fn properties() -> Properties {
    BTreeMap::from([("text".into(), json!("Use the noon sample."))])
}

fn write_memory(root: &Path) -> Result<Memory> {
    let store = Store::create(root)?;
    let access = ContextAccess::new(OWNER, Actor::System);
    let log = store.session_log(access.clone())?;
    let context = store.context(access)?;
    let source_id = OperationId::new();
    let source = log.append(
        source_id,
        EventInput {
            sources: vec![],
            session: SESSION.into(),
            event_id: "message-1".into(),
            scope: Scope::Personal,
            kind: EventKind::UserMessage,
            media_type: "text/plain".into(),
            occurred_at_ms: 1,
            expected_sequence: Some(0),
        },
        SOURCE,
    )?;
    let schema = context.define_type(
        OperationId::new(),
        TypeDefinition {
            name: "fixture.observation".into(),
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
    let object_id = OperationId::new();
    let object = context.save_object(
        object_id,
        ObjectInput {
            type_pin: schema.pin,
            scope: Scope::Personal,
            properties: properties(),
            sources: vec![source.pin.clone()],
            information: InformationKind::Inference,
            temporal: TemporalFacts::observed_at(1),
        },
    )?;
    Ok(Memory {
        source: Operation {
            id: source_id,
            receipt: source,
        },
        object: Operation {
            id: object_id,
            receipt: object,
        },
    })
}

fn read_memory(root: &Path, expected: Memory) -> Result<()> {
    let store = Store::open(root)?;
    let access = ContextAccess::new(OWNER, Actor::System);
    let log = store.session_log(access.clone())?;
    let context = store.context(access)?;
    let object = context.read(&expected.object.receipt.pin)?;
    ensure!(
        object.header.pin == expected.object.receipt.pin && object.header.pin.record.owner == OWNER,
        "object identity changed"
    );
    ensure!(
        object.body == RecordBody::Object(properties()),
        "object payload changed"
    );
    ensure!(
        object.header.sources == [expected.source.receipt.pin.clone()],
        "object source changed"
    );
    let source = &object.header.sources[0];
    let header = log.inspect_header(source)?;
    ensure!(
        &header.pin == source && header.pin.record.owner == OWNER,
        "source identity changed"
    );
    let event = header.event.context("missing source event")?;
    ensure!(
        event.session == SESSION && event.event_id == "message-1",
        "session event changed"
    );
    let payload = log.read_payload(source, 0, SOURCE.len())?;
    ensure!(
        payload.bytes == SOURCE && payload.next_offset.is_none(),
        "source payload changed"
    );
    ensure!(
        context.operation_receipt(expected.object.id)? == Some(expected.object.receipt),
        "object operation receipt changed"
    );
    ensure!(
        log.operation_receipt(expected.source.id)? == Some(expected.source.receipt),
        "source operation receipt changed"
    );
    Ok(())
}

fn write_state(root: &Path) -> Result<Vec<Row>> {
    let state = StateStore::open_path(root, TENANT, &StoreFormatDescriptor::new(STATE_FORMAT)?)?;
    state.put("sessions", SESSION, json!({"status": "running"}))?;
    let mut rows = vec![];
    for (namespace, key, value) in [
        (
            "sessions",
            SESSION,
            json!({"status": "complete", "work": "work-1"}),
        ),
        (
            "work-receipts",
            "work-1",
            json!({"session": SESSION, "result": "ok"}),
        ),
    ] {
        let revision = state.put(namespace, key, value.clone())?;
        rows.push(Row {
            namespace: namespace.into(),
            key: key.into(),
            value,
            revision,
        });
    }
    Ok(rows)
}

fn read_state(root: &Path, rows: Vec<Row>) -> Result<()> {
    let state = StateStore::open_path(root, TENANT, &StoreFormatDescriptor::new(STATE_FORMAT)?)?;
    for row in rows {
        let entry = state
            .get(&row.namespace, &row.key)?
            .with_context(|| format!("missing {}/{}", row.namespace, row.key))?;
        ensure!(
            entry.key == row.key.as_bytes()
                && entry.value == row.value
                && entry.revision == row.revision,
            "state mismatch at {}/{}",
            row.namespace,
            row.key
        );
    }
    Ok(())
}

fn main() -> Result<()> {
    let args: Vec<_> = env::args_os().skip(1).collect();
    let [command, identities, managed, state] = args.as_slice() else {
        anyhow::bail!("usage: fixture <write|read> <identities.json> <managed-root> <state-root>");
    };
    let managed = Path::new(managed);
    let state = Path::new(state);
    match command.to_str() {
        Some("write") => {
            ensure!(
                !managed.try_exists()? && !state.try_exists()?,
                "fixture roots must be new"
            );
            let output = fs::File::create_new(identities)?;
            let records = Identities {
                memory: write_memory(managed)?,
                state: write_state(state)?,
            };
            serde_json::to_writer_pretty(output, &records)?;
            Ok(())
        }
        Some("read") => {
            let records: Identities = serde_json::from_reader(fs::File::open(identities)?)?;
            // StateStore::open_path can initialize a missing or empty root.
            ensure!(state.join("version").is_file(), "missing state engine");
            read_memory(managed, records.memory)?;
            read_state(state, records.state)
        }
        _ => anyhow::bail!("expected write or read"),
    }
}
