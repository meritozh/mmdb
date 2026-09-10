//! Native acceptance on a temporary copy of a closed real-task context store.
//! Usage: verify_context_lifecycle STORE BUSINESS_PINS_JSON REPORT_JSON
use mmdb::context::*;
use mmdb::native_memory::{Actor, MemoryDatabase, OperationId, Scope, TemporalFacts};
use serde_json::json;
use std::collections::BTreeMap;
use std::path::Path;

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

fn copy_store(
    source: &Path,
    destination: &Path,
    entries: &mut usize,
    bytes: &mut u64,
) -> Result<()> {
    std::fs::create_dir(destination)?;
    for item in std::fs::read_dir(source)? {
        let item = item?;
        let metadata = item.file_type()?;
        *entries += 1;
        if *entries > 10_000 || metadata.is_symlink() {
            return Err("store copy entry limit or symlink".into());
        }
        let target = destination.join(item.file_name());
        if metadata.is_dir() {
            copy_store(&item.path(), &target, entries, bytes)?;
        } else if metadata.is_file() {
            *bytes = bytes
                .checked_add(item.metadata()?.len())
                .ok_or("store copy size overflow")?;
            if *bytes > 1024 * 1024 * 1024 {
                return Err("store copy exceeds 1 GiB".into());
            }
            std::fs::copy(item.path(), target)?;
        } else {
            return Err("non-regular store entry".into());
        }
    }
    Ok(())
}

fn query(text: &str, seeds: Vec<ContextRef>, depth: usize) -> ContextQuery {
    ContextQuery {
        text: text.into(),
        seeds,
        valid_at_ms: None,
        budget: RecallBudget {
            max_candidates: 4096,
            max_edges: 256,
            max_depth: depth,
            max_results: 100,
        },
    }
}

fn main() -> Result<()> {
    let args = std::env::args_os().skip(1).collect::<Vec<_>>();
    if args.len() != 3 {
        return Err("expected STORE BUSINESS_PINS_JSON REPORT_JSON".into());
    }
    let pins: BTreeMap<String, RecordPin> = serde_json::from_slice(&std::fs::read(&args[1])?)?;
    let target = pins.get("target").ok_or("missing target")?.clone();
    let dependency = pins.get("dependency").ok_or("missing dependency")?.clone();
    let relation = pins.get("relation").ok_or("missing relation")?.clone();
    let action = pins.get("action").ok_or("missing action")?.clone();
    let execution = pins
        .get("execution")
        .ok_or("missing actual action execution")?
        .clone();
    let scratch = tempfile::tempdir()?;
    let copied = scratch.path().join("context");
    let (mut copied_entries, mut copied_bytes) = (0, 0);
    copy_store(
        Path::new(&args[0]),
        &copied,
        &mut copied_entries,
        &mut copied_bytes,
    )?;
    let report = {
        let db = MemoryDatabase::open_context(&copied)?;
        let store = db.context(ContextAccess::new("local-user", Actor::System))?;
        for pin in pins.values() {
            store.read(pin)?;
        }
        let original = store.read(&target)?;
        let RecordBody::Object(properties) = original.body.clone() else {
            return Err("target is not an object".into());
        };
        if properties.get("status") != Some(&json!("pending")) {
            return Err("action unexpectedly changed gate state".into());
        }
        let connected = store.read(&relation)?;
        let endpoints = connected
            .header
            .endpoints
            .ok_or("relation has no endpoints")?;
        if endpoints.from != target.record || endpoints.to != dependency.record {
            return Err("wrong graph endpoints".into());
        }
        let lexical = store.recall(query("ECB February window release", vec![], 0))?;
        if lexical
            .hits
            .iter()
            .any(|hit| hit.record.header.pin.record == dependency.record)
        {
            return Err("dependency was a lexical match; graph test is inconclusive".into());
        }
        let graph = store.recall(query(
            "ECB February window release",
            vec![target.record.clone()],
            2,
        ))?;
        let indirect = graph
            .hits
            .iter()
            .find(|hit| hit.record.header.pin.record == dependency.record)
            .ok_or("relation did not retrieve dependency")?;
        if indirect.matched_terms != 0 || !indirect.path.contains(&relation) {
            return Err("dependency lacks an explicit relation-only path".into());
        }
        let mut bounded = query("ECB", vec![target.record.clone()], 1);
        bounded.budget = RecallBudget {
            max_candidates: 1,
            max_edges: 1,
            max_depth: 1,
            max_results: 1,
        };
        let limited = store.recall(bounded)?;
        if limited.hits.len() > 1 || limited.edges_examined > 1 || limited.candidates_examined > 1 {
            return Err("recall exceeded its budget".into());
        }
        let RecordBody::ActionExecution(completed) = store.read(&execution)?.body else {
            return Err("missing action execution body".into());
        };
        if completed.status != ActionStatus::Succeeded
            || completed.invocation.action != action
            || completed.invocation.target != target
        {
            return Err("action ledger does not match the actual request".into());
        }
        if completed.invocation.inputs.contains_key("comment") {
            return Err("optional null was not normalized to absent".into());
        }
        if completed.result_sources.is_empty() {
            return Err("action completion lacks original result sources".into());
        }
        let source = original
            .header
            .sources
            .first()
            .ok_or("real task object lacks provenance")?
            .clone();
        let anchor = store
            .append_history(
                OperationId::new(),
                HistoryInput {
                    sources: vec![source.clone()],
                    session: "independent-lifecycle".into(),
                    event_id: "lifecycle-anchor".into(),
                    scope: Scope::Personal,
                    kind: HistoryKind::Notice,
                    media_type: "text/plain".into(),
                    occurred_at_ms: 1,
                    expected_sequence: Some(0),
                },
                b"Native verifier operating on a temporary copy".as_slice(),
            )?
            .pin;
        let checkpoint=store.save_checkpoint(OperationId::new(),CheckpointInput {
            scope:CheckpointScope::Session,session:"independent-lifecycle".into(),run:"native-verifier".into(),history_sequence:1,
            expected_previous:None,expected_availability_epoch:store.availability_epoch()?,sources:vec![target.clone(),anchor],
            payload:json!({"goal":"verify actual task records on a disposable copy","state":"pending"}),
        })?.pin;
        let mut revised = properties;
        revised.insert("status".into(), json!("held"));
        let newer = store
            .revise_object(
                OperationId::new(),
                target.clone(),
                ObjectInput {
                    type_pin: original
                        .header
                        .type_pin
                        .clone()
                        .ok_or("target type missing")?,
                    scope: Scope::Personal,
                    properties: revised,
                    sources: vec![source.clone()],
                    information: InformationKind::Inference,
                    temporal: TemporalFacts::observed_at(2),
                },
            )?
            .pin;
        if newer.revision != target.revision + 1 || store.head(&target.record)?.pin != newer {
            return Err("revision identity drift".into());
        }
        store.read(&target)?;
        store.read(&checkpoint)?;
        if store
            .validate_current(&[target.clone(), checkpoint.clone()], None)
            .is_ok()
        {
            return Err("stale inputs remained current".into());
        }
        store.validate_current(std::slice::from_ref(&newer), None)?;
        let offers = store.actions_for(&newer, 10, None)?;
        let offer = offers
            .actions
            .iter()
            .find(|offer| offer.pin == action)
            .ok_or("action descriptor disappeared instead of explaining applicability")?;
        if offer.preconditions_met {
            return Err("failed precondition was reported as satisfied".into());
        }
        let mut rejected_invocation = completed.invocation.clone();
        rejected_invocation.target = newer.clone();
        if !matches!(
            store.prepare_action(OperationId::new(), rejected_invocation),
            Err(ContextError::InvalidInput(_))
        ) {
            return Err("failed precondition did not reject native action preparation".into());
        }
        let private_scope = Scope::Workspace(ulid::Ulid::new());
        let mut private_access = ContextAccess::new("local-user", Actor::System);
        private_access.scopes.push(private_scope.clone());
        let private = db.context(private_access)?;
        let private_pin = private
            .save_object(
                OperationId::new(),
                ObjectInput {
                    type_pin: original.header.type_pin.ok_or("target type missing")?,
                    scope: private_scope,
                    properties: BTreeMap::from([
                        ("title".into(), json!("SCOPEONLYVERIFIERQZ9")),
                        ("status".into(), json!("pending")),
                    ]),
                    sources: vec![source.clone()],
                    information: InformationKind::Inference,
                    temporal: TemporalFacts::observed_at(3),
                },
            )?
            .pin;
        if !matches!(store.read(&private_pin), Err(ContextError::AccessDenied))
            || !store
                .recall(query("SCOPEONLYVERIFIERQZ9", vec![], 0))?
                .hits
                .is_empty()
        {
            return Err("private workspace escaped its authorized scope".into());
        }
        let foreign = db.context(ContextAccess::new("other-owner", Actor::System))?;
        if !matches!(foreign.read(&target), Err(ContextError::AccessDenied))
            || !foreign.recall(query("ECB", vec![], 0))?.hits.is_empty()
        {
            return Err("owner boundary leaked real task data".into());
        }
        db.context(ContextAccess::new("local-user", Actor::Operator))?
            .purge(OperationId::new(), source.clone())?;
        for pin in [
            &source,
            &target,
            &newer,
            &dependency,
            &relation,
            &action,
            &execution,
            &checkpoint,
            &private_pin,
        ] {
            let reader = if pin == &private_pin {
                &private
            } else {
                &store
            };
            if !matches!(reader.read(pin), Err(ContextError::Unavailable(_))) {
                return Err(format!("purged source did not hide {}", pin.record.id).into());
            }
        }
        json!({"ok":true,"sourceStore":Path::new(&args[0]),"mutations":"temporary copy only","copiedEntries":copied_entries,"copiedBytes":copied_bytes,
            "relationOnlyHit":indirect,"boundedRecall":limited,"actionExecution":completed,
            "historicalRevision":target,"currentRevisionBeforePurge":newer,"staleCheckpointRejected":true,
            "privateWorkspaceDenied":true,"foreignOwnerDenied":true,"purgedSource":source,"derivedRecordsHidden":9})
    };
    // A second independent open must observe the committed purge.
    let db = MemoryDatabase::open_context(&copied)?;
    let store = db.context(ContextAccess::new("local-user", Actor::System))?;
    if !matches!(store.read(&target), Err(ContextError::Unavailable(_))) {
        return Err("purge was not durable after reopen".into());
    }
    std::fs::write(&args[2], serde_json::to_vec_pretty(&report)?)?;
    println!(
        "{}",
        json!({"ok":true,"report":Path::new(&args[2]),"originalStoreUnchanged":true})
    );
    Ok(())
}
