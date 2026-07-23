use crate::audit::{sanitize_value, AuditAction, AuditContext};
use crate::recall::validate_relation;
use crate::runtime::{AgentRequest, AgentRole, DreamProfile};
use crate::Database;
use mmdb_core::{Content, Edge, Error, MemoryNode, MemoryState, NodeKind, Result};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use smallvec::SmallVec;
use std::collections::{BTreeMap, HashSet};
use ulid::Ulid;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MaintenanceTrigger {
    TurnEnd,
    ContextCompaction,
    Manual,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DreamNode {
    pub temporary_id: String,
    pub kind: NodeKind,
    pub content: Content,
    #[serde(default)]
    pub valid_from_ms: Option<i64>,
    #[serde(default)]
    pub valid_to_ms: Option<i64>,
    #[serde(default)]
    pub source_citations: Vec<Ulid>,
    #[serde(default)]
    pub metadata: BTreeMap<String, Value>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", content = "id", rename_all = "snake_case")]
pub enum DreamEndpoint {
    Existing(Ulid),
    Proposed(String),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DreamEdge {
    pub src: DreamEndpoint,
    pub dst: DreamEndpoint,
    pub relation: String,
    pub weight: f32,
    #[serde(default)]
    pub valid_from_ms: Option<i64>,
    #[serde(default)]
    pub valid_to_ms: Option<i64>,
    #[serde(default)]
    pub evidence: Vec<Ulid>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DreamSupersession {
    pub node_id: Ulid,
    pub expected_revision: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DreamProposal {
    pub nodes: Vec<DreamNode>,
    pub edges: Vec<DreamEdge>,
    pub supersede: Vec<DreamSupersession>,
    pub explanation: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DreamRunStatus {
    Pending,
    Completed,
    Reverting,
    Failed,
    Reverted,
    Repaired,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SupersededSnapshot {
    pub node_id: Ulid,
    pub previous_state: MemoryState,
    pub previous_valid_to_ms: Option<i64>,
    pub revision_after: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DreamRun {
    pub id: Ulid,
    pub profile_id: String,
    pub profile_revision: String,
    pub source_hash: String,
    pub source_ids: Vec<Ulid>,
    pub created_ids: Vec<Ulid>,
    pub added_edges: Vec<Edge>,
    pub superseded: Vec<SupersededSnapshot>,
    pub explanation: String,
    pub status: DreamRunStatus,
    pub created_at_ms: i64,
    pub completed_at_ms: Option<i64>,
    pub error: Option<String>,
}

impl Database {
    pub async fn maintain(
        &self,
        trigger: MaintenanceTrigger,
        context: AuditContext,
    ) -> Result<Option<DreamRun>> {
        let operation_id = Ulid::new();
        let profile = self
            .memory_profile()?
            .dreamer
            .ok_or_else(|| Error::InvalidArgument("no dreamer profile is configured".into()))?;
        let sources = self.select_dream_sources(trigger, &profile)?;
        let pending_count = sources
            .iter()
            .filter(|node| !node.metadata.contains_key("dream_run_id"))
            .count();
        if sources.is_empty()
            || (trigger == MaintenanceTrigger::TurnEnd
                && pending_count < profile.turn_end_threshold)
        {
            self.append_audit(
                operation_id,
                AuditAction::Compaction,
                "maintain",
                true,
                context,
                json!({
                    "trigger": trigger,
                    "outcome": "no_op",
                    "pending_count": pending_count,
                    "threshold": profile.turn_end_threshold,
                }),
                None,
            )?;
            return Ok(None);
        }
        let source_hash = dream_source_hash(&sources, &profile);
        if self.dream_runs()?.iter().any(|run| {
            run.source_hash == source_hash
                && matches!(
                    run.status,
                    DreamRunStatus::Completed
                        | DreamRunStatus::Reverting
                        | DreamRunStatus::Reverted
                )
        }) {
            self.append_audit(
                operation_id,
                AuditAction::Compaction,
                "maintain",
                true,
                context,
                json!({
                    "trigger": trigger,
                    "outcome": "deduplicated",
                    "source_hash": source_hash,
                }),
                None,
            )?;
            return Ok(None);
        }
        let client = self.clients.agent(&profile.client_id)?.ok_or_else(|| {
            Error::InvalidArgument(format!(
                "agent client `{}` is not registered",
                profile.client_id
            ))
        })?;
        let request = AgentRequest {
            role: AgentRole::Dreamer,
            agent_id: profile.agent_id.clone(),
            model_id: profile.model_id.clone(),
            prompt_version: profile.prompt_version.clone(),
            payload: json!({
                "trigger": trigger,
                "sources": sources.iter().map(dream_source_snapshot).collect::<Vec<_>>(),
                "constraints": {
                    "allowed_node_kinds": ["Fact", "Entity"],
                    "allowed_relations": ["contains", "derived_from", "causes", "enables", "prevents", "contradicts", "supersedes"],
                    "raw_episodes_are_immutable": true,
                },
                "response_schema": profile.response_schema,
            }),
        };
        let response = client.call(request.clone()).await;
        self.append_audit(
            operation_id,
            AuditAction::ClientCall,
            "dreamer",
            response.is_ok(),
            context.clone(),
            sanitize_value(&json!({"request": request, "response": response.as_ref().ok()})),
            response.as_ref().err().map(ToString::to_string),
        )?;
        let result = (|| {
            let response = response?;
            let proposal: DreamProposal =
                serde_json::from_value(response.payload).map_err(|error| {
                    Error::InvalidArgument(format!("malformed dreamer response: {error}"))
                })?;
            self.validate_dream_proposal(&proposal, &sources)?;
            self.apply_dream_proposal(&profile, source_hash, &sources, proposal)
        })();
        self.append_audit(
            operation_id,
            AuditAction::Compaction,
            "maintain",
            result.is_ok(),
            context,
            json!({
                "trigger": trigger,
                "source_ids": sources.iter().map(|node| node.id).collect::<Vec<_>>(),
                "run": result.as_ref().ok(),
            }),
            result.as_ref().err().map(ToString::to_string),
        )?;
        let run = result?;
        for id in &run.created_ids {
            let _ = self.project_configured(*id).await;
        }
        Ok(Some(run))
    }

    pub fn dream_runs(&self) -> Result<Vec<DreamRun>> {
        self.runtime_store.dream_runs(self.config.tenant)
    }

    pub fn dream_run(&self, id: Ulid) -> Result<Option<DreamRun>> {
        self.runtime_store.dream_run(self.config.tenant, id)
    }

    pub fn revert_dream(&self, id: Ulid, context: AuditContext) -> Result<()> {
        let mut run = self.dream_run(id)?.ok_or(Error::NotFound)?;
        if run.status != DreamRunStatus::Completed {
            return Err(Error::InvalidArgument(format!(
                "dream run {id} is not completed"
            )));
        }
        for snapshot in &run.superseded {
            let node = self.get(snapshot.node_id)?.ok_or(Error::NotFound)?;
            if node.revision != snapshot.revision_after || node.state != MemoryState::Superseded {
                return Err(Error::InvalidArgument(format!(
                    "cannot revert dream {id}: node {} changed after compaction",
                    snapshot.node_id
                )));
            }
        }
        run.status = DreamRunStatus::Reverting;
        self.runtime_store
            .put_dream_run(self.config.tenant, id, &run)?;
        self.retract_dream_outputs(&run, true)?;
        run.status = DreamRunStatus::Reverted;
        run.completed_at_ms = Some(crate::now_ms());
        self.runtime_store
            .put_dream_run(self.config.tenant, id, &run)?;
        self.append_audit(
            Ulid::new(),
            AuditAction::Compaction,
            "revert_dream",
            true,
            context,
            json!({"run": run}),
            None,
        )
    }

    pub(crate) fn repair_dream_runs(&self) -> Result<()> {
        for mut run in self.dream_runs()? {
            if !matches!(
                run.status,
                DreamRunStatus::Pending | DreamRunStatus::Reverting
            ) {
                continue;
            }
            let was_reverting = run.status == DreamRunStatus::Reverting;
            self.retract_dream_outputs(&run, true)?;
            run.status = if was_reverting {
                DreamRunStatus::Reverted
            } else {
                DreamRunStatus::Repaired
            };
            run.completed_at_ms = Some(crate::now_ms());
            run.error = Some("incomplete dream operation repaired on reopen".into());
            self.runtime_store
                .put_dream_run(self.config.tenant, run.id, &run)?;
            self.append_audit(
                Ulid::new(),
                AuditAction::Repair,
                "repair_dream",
                true,
                AuditContext::default(),
                json!({"run": run}),
                None,
            )?;
        }
        Ok(())
    }

    fn select_dream_sources(
        &self,
        _trigger: MaintenanceTrigger,
        profile: &DreamProfile,
    ) -> Result<Vec<MemoryNode>> {
        let processed: HashSet<_> = self
            .dream_runs()?
            .into_iter()
            .filter(|run| {
                matches!(
                    run.status,
                    DreamRunStatus::Completed
                        | DreamRunStatus::Reverting
                        | DreamRunStatus::Reverted
                )
            })
            .flat_map(|run| run.source_ids)
            .collect();
        let max_nodes = profile.max_nodes.min(128);
        let max_bytes = profile.max_input_bytes.min(256 * 1024);
        let mut pending = Vec::new();
        let mut derived = Vec::new();
        for node in self
            .storage
            .scan_by_time(self.config.tenant, 0, i64::MAX, usize::MAX)?
        {
            if node.state != MemoryState::Active
                || !matches!(node.kind, NodeKind::Episode | NodeKind::Fact)
            {
                continue;
            }
            if node.metadata.contains_key("dream_run_id") {
                derived.push(node);
            } else if !processed.contains(&node.id) {
                pending.push(node);
            }
        }
        if pending.is_empty() {
            return Ok(Vec::new());
        }
        pending.extend(derived);
        let mut selected = Vec::new();
        let mut bytes = 0;
        for node in pending {
            let size = dream_source_snapshot(&node).to_string().len();
            if selected.len() >= max_nodes || bytes + size > max_bytes {
                break;
            }
            bytes += size;
            selected.push(node);
        }
        Ok(selected)
    }

    fn validate_dream_proposal(
        &self,
        proposal: &DreamProposal,
        sources: &[MemoryNode],
    ) -> Result<()> {
        let source_ids: HashSet<_> = sources.iter().map(|node| node.id).collect();
        for source in sources {
            let current = self.get(source.id)?.ok_or(Error::NotFound)?;
            if current.revision != source.revision || current.state != MemoryState::Active {
                return Err(Error::InvalidArgument(format!(
                    "dream source {} changed during the client call",
                    source.id
                )));
            }
        }
        let mut temporary_ids = HashSet::new();
        for node in &proposal.nodes {
            if !matches!(node.kind, NodeKind::Fact | NodeKind::Entity) {
                return Err(Error::InvalidArgument(
                    "dreamer may only create fact or entity nodes".into(),
                ));
            }
            if matches!(node.content, Content::Blob { .. }) {
                return Err(Error::InvalidArgument(
                    "dreamer may not synthesize blob payloads".into(),
                ));
            }
            if node.temporary_id.is_empty() || !temporary_ids.insert(node.temporary_id.clone()) {
                return Err(Error::InvalidArgument(
                    "dreamer temporary node IDs must be non-empty and unique".into(),
                ));
            }
            if node.source_citations.is_empty()
                || node
                    .source_citations
                    .iter()
                    .any(|id| !source_ids.contains(id))
            {
                return Err(Error::InvalidArgument(
                    "every dreamed node must cite selected source memories".into(),
                ));
            }
            validate_interval(node.valid_from_ms, node.valid_to_ms)?;
        }
        for edge in &proposal.edges {
            validate_endpoint(self, &edge.src, &temporary_ids, &source_ids)?;
            validate_endpoint(self, &edge.dst, &temporary_ids, &source_ids)?;
            let synthetic = Edge {
                src: Ulid::new(),
                dst: Ulid::new(),
                label: edge.relation.clone(),
                weight: edge.weight,
                created_at_ms: crate::now_ms(),
                metadata: BTreeMap::new(),
                revision: 1,
                valid_from_ms: edge.valid_from_ms,
                valid_to_ms: edge.valid_to_ms,
                evidence: edge.evidence.clone(),
            };
            validate_relation(&synthetic)?;
            if edge.evidence.iter().any(|id| !source_ids.contains(id)) {
                return Err(Error::InvalidArgument(
                    "dream edge cited evidence outside the source batch".into(),
                ));
            }
        }
        for supersession in &proposal.supersede {
            let node = self.get(supersession.node_id)?.ok_or(Error::NotFound)?;
            if node.revision != supersession.expected_revision
                || !node.metadata.contains_key("dream_run_id")
            {
                return Err(Error::InvalidArgument(format!(
                    "dreamer may only supersede an unchanged derived memory: {}",
                    node.id
                )));
            }
        }
        Ok(())
    }

    fn apply_dream_proposal(
        &self,
        profile: &DreamProfile,
        source_hash: String,
        sources: &[MemoryNode],
        proposal: DreamProposal,
    ) -> Result<DreamRun> {
        let now = crate::now_ms();
        let mut run = DreamRun {
            id: Ulid::new(),
            profile_id: profile.id.clone(),
            profile_revision: profile.revision.clone(),
            source_hash,
            source_ids: sources.iter().map(|node| node.id).collect(),
            created_ids: Vec::new(),
            added_edges: Vec::new(),
            superseded: Vec::new(),
            explanation: proposal.explanation.clone(),
            status: DreamRunStatus::Pending,
            created_at_ms: now,
            completed_at_ms: None,
            error: None,
        };
        self.runtime_store
            .put_dream_run(self.config.tenant, run.id, &run)?;
        let result: Result<()> = (|| {
            let mut ids = BTreeMap::new();
            for dreamed in &proposal.nodes {
                let id = Ulid::new();
                ids.insert(dreamed.temporary_id.clone(), id);
                run.created_ids.push(id);
                self.runtime_store
                    .put_dream_run(self.config.tenant, run.id, &run)?;
                let mut metadata = dreamed.metadata.clone();
                metadata.insert("dream_run_id".into(), json!(run.id));
                metadata.insert("source_citations".into(), json!(dreamed.source_citations));
                let node = MemoryNode {
                    id,
                    tenant: self.config.tenant,
                    kind: dreamed.kind,
                    created_at_ms: now,
                    updated_at_ms: now,
                    content: dreamed.content.clone(),
                    embeddings: SmallVec::new(),
                    metadata,
                    revision: 1,
                    state: MemoryState::Pending,
                    valid_from_ms: dreamed.valid_from_ms.or(Some(now)),
                    valid_to_ms: dreamed.valid_to_ms,
                };
                self.insert_inner(node, false, false)?;
            }
            for dreamed in &proposal.nodes {
                let derived_id = ids[&dreamed.temporary_id];
                for source in &dreamed.source_citations {
                    let edge = Edge {
                        src: derived_id,
                        dst: *source,
                        label: "derived_from".into(),
                        weight: 1.0,
                        created_at_ms: now,
                        metadata: BTreeMap::new(),
                        revision: 1,
                        valid_from_ms: Some(now),
                        valid_to_ms: None,
                        evidence: dreamed.source_citations.clone(),
                    };
                    run.added_edges.push(edge.clone());
                    self.runtime_store
                        .put_dream_run(self.config.tenant, run.id, &run)?;
                    self.add_edge(edge)?;
                }
            }
            for dreamed in proposal.edges {
                let edge = Edge {
                    src: resolve_endpoint(&dreamed.src, &ids),
                    dst: resolve_endpoint(&dreamed.dst, &ids),
                    label: dreamed.relation,
                    weight: dreamed.weight,
                    created_at_ms: now,
                    metadata: BTreeMap::new(),
                    revision: 1,
                    valid_from_ms: dreamed.valid_from_ms.or(Some(now)),
                    valid_to_ms: dreamed.valid_to_ms,
                    evidence: dreamed.evidence,
                };
                run.added_edges.push(edge.clone());
                self.runtime_store
                    .put_dream_run(self.config.tenant, run.id, &run)?;
                self.add_edge(edge)?;
            }
            for supersession in proposal.supersede {
                let mut node = self.get(supersession.node_id)?.ok_or(Error::NotFound)?;
                let previous_state = node.state;
                let previous_valid_to_ms = node.valid_to_ms;
                let revision_after = node.revision.max(1).saturating_add(1);
                run.superseded.push(SupersededSnapshot {
                    node_id: supersession.node_id,
                    previous_state,
                    previous_valid_to_ms,
                    revision_after,
                });
                self.runtime_store
                    .put_dream_run(self.config.tenant, run.id, &run)?;
                node.state = MemoryState::Superseded;
                node.valid_to_ms = Some(now);
                self.insert_inner(node, false, false)?;
            }
            for id in &run.created_ids {
                let mut node = self.get(*id)?.ok_or(Error::NotFound)?;
                node.state = MemoryState::Active;
                self.insert_inner(node, false, false)?;
            }
            Ok(())
        })();
        match result {
            Ok(()) => {
                run.status = DreamRunStatus::Completed;
                run.completed_at_ms = Some(crate::now_ms());
                self.runtime_store
                    .put_dream_run(self.config.tenant, run.id, &run)?;
                Ok(run)
            }
            Err(error) => {
                run.error = Some(error.to_string());
                if let Err(rollback_error) = self.retract_dream_outputs(&run, true) {
                    run.error = Some(format!("{error}; rollback failed: {rollback_error}"));
                    self.runtime_store
                        .put_dream_run(self.config.tenant, run.id, &run)?;
                    return Err(error);
                }
                run.status = DreamRunStatus::Failed;
                self.runtime_store
                    .put_dream_run(self.config.tenant, run.id, &run)?;
                Err(error)
            }
        }
    }

    fn retract_dream_outputs(&self, run: &DreamRun, restore_superseded: bool) -> Result<()> {
        for edge in &run.added_edges {
            self.graph_store
                .remove_edge(self.config.tenant, edge.src, edge.dst, &edge.label)?;
        }
        for id in &run.created_ids {
            if let Some(mut node) = self.get(*id)? {
                let retracted_at = crate::now_ms();
                node.state = MemoryState::Retracted;
                if !matches!(
                    run.status,
                    DreamRunStatus::Completed | DreamRunStatus::Reverting
                ) {
                    node.valid_from_ms = Some(retracted_at);
                }
                node.valid_to_ms = Some(retracted_at);
                self.insert_inner(node, false, false)?;
            }
        }
        if restore_superseded {
            for snapshot in &run.superseded {
                if let Some(mut node) = self.get(snapshot.node_id)? {
                    node.state = snapshot.previous_state;
                    node.valid_to_ms = snapshot.previous_valid_to_ms;
                    self.insert_inner(node, false, false)?;
                }
            }
        }
        Ok(())
    }
}

fn dream_source_hash(sources: &[MemoryNode], profile: &DreamProfile) -> String {
    let mut hasher = blake3::Hasher::new();
    hasher.update(profile.revision.as_bytes());
    for source in sources {
        hasher.update(&source.id.0.to_be_bytes());
        hasher.update(&source.revision.to_be_bytes());
    }
    hasher.finalize().to_hex().to_string()
}

fn dream_source_snapshot(node: &MemoryNode) -> Value {
    let content = match &node.content {
        Content::Text(text) => json!({
            "type": "text",
            "excerpt": excerpt(text, 1024),
            "truncated": text.len() > 1024,
        }),
        Content::Structured(value) => {
            let encoded = value.to_string();
            json!({
                "type": "structured",
                "excerpt": excerpt(&encoded, 1024),
                "truncated": encoded.len() > 1024,
            })
        }
        Content::Blob {
            hash, size, mime, ..
        } => json!({
            "type": "blob",
            "hash": hash.iter().map(|byte| format!("{byte:02x}")).collect::<String>(),
            "size": size,
            "mime": mime,
        }),
    };
    json!({
        "id": node.id,
        "revision": node.revision,
        "kind": node.kind,
        "created_at_ms": node.created_at_ms,
        "updated_at_ms": node.updated_at_ms,
        "valid_from_ms": node.valid_from_ms,
        "valid_to_ms": node.valid_to_ms,
        "content": content,
        "metadata_keys": node.metadata.keys().take(32).map(|key| excerpt(key, 64)).collect::<Vec<_>>(),
    })
}

fn excerpt(text: &str, max_bytes: usize) -> String {
    if text.len() <= max_bytes {
        return text.to_string();
    }
    let mut end = max_bytes;
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    text[..end].to_string()
}

fn validate_endpoint(
    database: &Database,
    endpoint: &DreamEndpoint,
    proposed: &HashSet<String>,
    sources: &HashSet<Ulid>,
) -> Result<()> {
    match endpoint {
        DreamEndpoint::Existing(id) if sources.contains(id) && database.get(*id)?.is_some() => {
            Ok(())
        }
        DreamEndpoint::Proposed(id) if proposed.contains(id) => Ok(()),
        _ => Err(Error::InvalidArgument(
            "dream edge has an unknown endpoint".into(),
        )),
    }
}

fn resolve_endpoint(endpoint: &DreamEndpoint, proposed: &BTreeMap<String, Ulid>) -> Ulid {
    match endpoint {
        DreamEndpoint::Existing(id) => *id,
        DreamEndpoint::Proposed(id) => proposed[id],
    }
}

fn validate_interval(from: Option<i64>, to: Option<i64>) -> Result<()> {
    if from.zip(to).is_some_and(|(from, to)| from >= to) {
        return Err(Error::InvalidArgument("invalid validity interval".into()));
    }
    Ok(())
}
