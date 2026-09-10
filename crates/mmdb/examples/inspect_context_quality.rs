//! Independent, read-only acceptance of a real harness-created context store.
//! Usage: cargo run -p mmdb --example inspect_context_quality -- STORE REPORT
use mmdb::context::*;
use mmdb::native_memory::{Actor, MemoryDatabase};
use serde_json::json;
use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args_os().skip(1);
    let path = PathBuf::from(args.next().ok_or("missing context store path")?);
    let report = PathBuf::from(args.next().ok_or("missing report path")?);
    if args.next().is_some() {
        return Err("expected STORE REPORT".into());
    }
    let database = MemoryDatabase::open_context(&path)?;
    let mut access = ContextAccess::new("local-user", Actor::System);
    access.agent = Some("independent-mmdb-quality-reader".into());
    access.session = Some("independent-quality-session".into());
    let store = database.context(access)?;
    let mut sessions = Vec::new();
    let mut after = None;
    loop {
        let page = store.history_sessions(100, after.as_deref())?;
        sessions.extend(page.sessions);
        after = page.next;
        if after.is_none() {
            break;
        }
        if sessions.len() > 1000 {
            return Err("acceptance session budget exceeded".into());
        }
    }
    let mut session_reports = Vec::new();
    let mut total_bytes = 0u64;
    let mut total_events = 0usize;
    for session in sessions {
        let mut sequence = 0;
        let mut event_ids = BTreeSet::new();
        let mut calls = BTreeMap::new();
        let mut results = BTreeSet::new();
        let mut agents = BTreeSet::new();
        let mut runs = BTreeSet::new();
        let mut records = Vec::new();
        loop {
            let page = store.history(&session, sequence, 100)?;
            if page.entries.is_empty() {
                break;
            }
            for header in page.entries {
                let history = header.history.as_ref().ok_or("missing history metadata")?;
                if history.sequence != sequence + 1 || !event_ids.insert(history.event_id.clone()) {
                    return Err("non-contiguous or duplicate history".into());
                }
                sequence = history.sequence;
                agents.insert(header.provenance.agent.clone());
                runs.insert(header.provenance.run.clone());
                match &history.kind {
                    HistoryKind::ToolCall { call_id, .. } => {
                        if calls.insert(call_id.clone(), header.pin.clone()).is_some() {
                            return Err("duplicate tool call".into());
                        }
                    }
                    HistoryKind::ToolResult { call_id, .. }
                        if !calls.contains_key(call_id) || !results.insert(call_id.clone()) =>
                    {
                        return Err("orphan or duplicate tool result".into());
                    }
                    _ => {}
                }
                let mut offset = 0;
                let mut hash = blake3::Hasher::new();
                let mut read_bytes = 0u64;
                loop {
                    let payload = store.read_payload(&header.pin, offset, 65536)?;
                    hash.update(&payload.bytes);
                    read_bytes += payload.bytes.len() as u64;
                    total_bytes += payload.bytes.len() as u64;
                    match payload.next_offset {
                        Some(next) if next > offset => offset = next,
                        Some(_) => return Err("payload cursor did not advance".into()),
                        None => break,
                    }
                }
                // Native reads verify the store's keyed chunk digests. This
                // unkeyed export hash is for comparing independent readbacks;
                // it is deliberately not compared to the private keyed MAC.
                if read_bytes != header.payload.bytes {
                    return Err("original payload length mismatch".into());
                }
                for source in &header.sources {
                    store.read(source)?;
                }
                total_events += 1;
                if total_events > 100_000 || total_bytes > 1024 * 1024 * 1024 {
                    return Err("acceptance export budget exceeded".into());
                }
                records.push(json!({"pin":header.pin,"sequence":sequence,"bytes":header.payload.bytes,"exportBlake3":hash.finalize().to_hex().to_string(),"sources":header.sources}));
            }
        }
        if calls.len() != results.len() {
            return Err(format!("session {session} has unresolved tool calls").into());
        }
        let checkpoint = store.latest_checkpoint(&session)?;
        let mut checkpoints = Vec::new();
        let mut cursor = checkpoint.clone();
        let mut expected_window = None;
        while let Some(header) = cursor {
            if checkpoints.len() >= 1000 {
                return Err("checkpoint chain budget exceeded".into());
            }
            let metadata = header
                .checkpoint
                .as_ref()
                .ok_or("checkpoint metadata missing")?;
            if expected_window.is_some_and(|window| metadata.window != window) {
                return Err("checkpoint window chain is not contiguous".into());
            }
            expected_window = metadata.window.checked_sub(1);
            cursor = metadata
                .previous
                .as_ref()
                .map(|pin| store.inspect_header(pin))
                .transpose()?;
            checkpoints.push(header);
        }
        session_reports.push(json!({"session":session,"events":sequence,"toolCalls":calls.len(),"agents":agents,"runs":runs,"latestCheckpoint":checkpoint,"checkpoints":checkpoints,"records":records}));
    }
    let query = ContextQuery {
        text: "SQLite WAL ECB PMO checkpoint".into(),
        seeds: Vec::new(),
        valid_at_ms: None,
        budget: RecallBudget {
            max_candidates: 4096,
            max_edges: 512,
            max_depth: 2,
            max_results: 100,
        },
    };
    let recalled = store.recall(query.clone())?;
    if recalled.hits.is_empty() {
        return Err("no real task knowledge recalled by the independent harness".into());
    }
    for hit in &recalled.hits {
        store.read(&hit.record.header.pin)?;
        for source in &hit.record.header.sources {
            store.read(source)?;
        }
    }
    let foreign = database.context(ContextAccess::new("other-owner", Actor::System))?;
    if !foreign.recall(query)?.hits.is_empty() {
        return Err("owner boundary leaked recalled knowledge".into());
    }
    let value = json!({"store":path,"reader":"native mmdb API without MiuMiu","events":total_events,"originalBytesVerified":total_bytes,"sessions":session_reports,"recalled":recalled,"foreignOwnerHits":0});
    std::fs::write(&report, serde_json::to_vec_pretty(&value)?)?;
    println!(
        "{}",
        json!({"ok":true,"events":total_events,"originalBytesVerified":total_bytes,"savedKnowledgeHits":value["recalled"]["hits"].as_array().map(Vec::len),"report":report})
    );
    Ok(())
}
