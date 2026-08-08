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

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
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
    Completing,
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
    /// Revisions captured only after projection work has finished and the run
    /// is ready to publish. Revert refuses to erase an output that changed
    /// after this checkpoint.
    #[serde(default)]
    pub created_revisions: BTreeMap<Ulid, u64>,
    /// Stable semantic fingerprints captured before projection work. They
    /// deliberately exclude embeddings and revisions so projection writes do
    /// not look like external edits.
    #[serde(default)]
    pub created_fingerprints: BTreeMap<Ulid, String>,
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
                    DreamRunStatus::Pending
                        | DreamRunStatus::Completing
                        | DreamRunStatus::Completed
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
        let staged: Result<Option<DreamRun>> = (|| {
            let response = response?;
            let proposal: DreamProposal =
                serde_json::from_value(response.payload).map_err(|error| {
                    Error::InvalidArgument(format!("malformed dreamer response: {error}"))
                })?;
            let _guard = self.node_mutation_lock.lock();
            if self.dream_runs()?.iter().any(|run| {
                run.source_hash == source_hash
                    && matches!(
                        run.status,
                        DreamRunStatus::Pending
                            | DreamRunStatus::Completing
                            | DreamRunStatus::Completed
                            | DreamRunStatus::Reverting
                            | DreamRunStatus::Reverted
                    )
            }) {
                return Ok(None);
            }
            self.validate_dream_proposal(&proposal, &sources)?;
            self.apply_dream_proposal_unlocked(&profile, source_hash, &sources, proposal)
                .map(Some)
        })();
        let result = match staged {
            Ok(Some(run)) => {
                for id in &run.created_ids {
                    let _ = self.project_configured(*id).await;
                }
                let _guard = self.node_mutation_lock.lock();
                self.complete_dream_run_unlocked(run.id).map(Some)
            }
            Ok(None) => Ok(None),
            Err(error) => Err(error),
        };
        self.append_audit(
            operation_id,
            AuditAction::Compaction,
            "maintain",
            result.is_ok(),
            context,
            json!({
                "trigger": trigger,
                "source_ids": sources.iter().map(|node| node.id).collect::<Vec<_>>(),
                "run": result.as_ref().ok().and_then(|run| run.as_ref()),
                "outcome": result.as_ref().ok().map(|run| if run.is_some() { "completed" } else { "deduplicated_after_client" }),
            }),
            result.as_ref().err().map(ToString::to_string),
        )?;
        result
    }

    pub fn dream_runs(&self) -> Result<Vec<DreamRun>> {
        self.runtime_store.dream_runs(self.config.tenant)
    }

    pub fn dream_run(&self, id: Ulid) -> Result<Option<DreamRun>> {
        self.runtime_store.dream_run(self.config.tenant, id)
    }

    fn complete_dream_run_unlocked(&self, id: Ulid) -> Result<DreamRun> {
        let mut run = self.dream_run(id)?.ok_or(Error::NotFound)?;
        if run.status != DreamRunStatus::Pending {
            return Err(Error::InvalidArgument(format!(
                "dream run {id} is not pending"
            )));
        }
        let validation: Result<()> = (|| {
            if run.created_fingerprints.len() != run.created_ids.len() {
                return Err(Error::InvalidArgument(format!(
                    "cannot complete dream {id}: output fingerprint checkpoint is missing"
                )));
            }
            for created_id in &run.created_ids {
                let node = self.get(*created_id)?.ok_or_else(|| {
                    Error::InvalidArgument(format!(
                        "cannot complete dream {id}: output {created_id} is missing"
                    ))
                })?;
                let expected = run.created_fingerprints.get(created_id).ok_or_else(|| {
                    Error::InvalidArgument(format!(
                        "cannot complete dream {id}: output {created_id} lacks a fingerprint checkpoint"
                    ))
                })?;
                if node.state != MemoryState::Pending
                    || node.metadata.get("dream_run_id") != Some(&json!(id))
                    || dream_output_fingerprint(&node)? != *expected
                {
                    return Err(Error::InvalidArgument(format!(
                        "cannot complete dream {id}: staged output {created_id} changed during projection"
                    )));
                }
            }
            for planned in &run.added_edges {
                if self.dream_edge(planned)?.is_some() {
                    return Err(Error::InvalidArgument(format!(
                        "cannot complete dream {id}: edge {} -[{}]-> {} was created during projection",
                        planned.src, planned.label, planned.dst
                    )));
                }
            }
            for snapshot in &run.superseded {
                let node = self.get(snapshot.node_id)?.ok_or(Error::NotFound)?;
                if node.revision.max(1).saturating_add(1) != snapshot.revision_after
                    || node.state != snapshot.previous_state
                    || node.valid_to_ms != snapshot.previous_valid_to_ms
                {
                    return Err(Error::InvalidArgument(format!(
                        "cannot complete dream {id}: supersession target {} changed during projection",
                        snapshot.node_id
                    )));
                }
            }
            Ok(())
        })();
        if let Err(error) = validation {
            let cleanup = self.cleanup_pending_dream_unlocked(&run);
            run.error = Some(match &cleanup {
                Ok(conflicts) if conflicts.is_empty() => error.to_string(),
                Ok(conflicts) => format!(
                    "{error}; preserved changed staging: {}",
                    conflicts.join(", ")
                ),
                Err(cleanup_error) => format!("{error}; cleanup failed: {cleanup_error}"),
            });
            if cleanup.is_ok() {
                run.status = DreamRunStatus::Failed;
                run.completed_at_ms = Some(crate::now_ms());
            }
            self.runtime_store
                .put_dream_run(self.config.tenant, id, &run)?;
            return Err(error);
        }

        run.status = DreamRunStatus::Completing;
        self.runtime_store
            .put_dream_run(self.config.tenant, id, &run)?;
        let result: Result<()> = (|| {
            for edge in &run.added_edges {
                self.add_edge_unlocked(edge.clone())?;
            }
            let superseded_at = crate::now_ms();
            for snapshot in &run.superseded {
                let mut node = self.get(snapshot.node_id)?.ok_or(Error::NotFound)?;
                node.state = MemoryState::Superseded;
                node.valid_to_ms = Some(superseded_at);
                self.insert_inner_unlocked(node, false, false)?;
                let current = self.get(snapshot.node_id)?.ok_or(Error::NotFound)?;
                if current.revision != snapshot.revision_after {
                    return Err(Error::InvalidArgument(format!(
                        "dream {id} supersession checkpoint drifted for {}",
                        snapshot.node_id
                    )));
                }
            }
            for created_id in &run.created_ids {
                let mut node = self.get(*created_id)?.ok_or(Error::NotFound)?;
                node.state = MemoryState::Active;
                self.insert_inner_unlocked(node, false, false)?;
                let revision = self.get(*created_id)?.ok_or(Error::NotFound)?.revision;
                run.created_revisions.insert(*created_id, revision);
                self.runtime_store
                    .put_dream_run(self.config.tenant, id, &run)?;
            }
            Ok(())
        })();
        if let Err(error) = result {
            let rollback = self.rollback_completing_dream_unlocked(&run);
            run.error = Some(match &rollback {
                Ok(conflicts) if conflicts.is_empty() => error.to_string(),
                Ok(conflicts) => {
                    format!(
                        "{error}; preserved changed outputs: {}",
                        conflicts.join(", ")
                    )
                }
                Err(rollback_error) => format!("{error}; rollback failed: {rollback_error}"),
            });
            if let Err(rollback_error) = rollback {
                run.error = Some(format!("{error}; rollback failed: {rollback_error}"));
                self.runtime_store
                    .put_dream_run(self.config.tenant, id, &run)?;
                return Err(error);
            }
            run.status = DreamRunStatus::Failed;
            run.completed_at_ms = Some(crate::now_ms());
            self.runtime_store
                .put_dream_run(self.config.tenant, id, &run)?;
            return Err(error);
        }

        run.status = DreamRunStatus::Completed;
        run.completed_at_ms = Some(crate::now_ms());
        self.runtime_store
            .put_dream_run(self.config.tenant, id, &run)?;
        Ok(run)
    }

    pub fn revert_dream(&self, id: Ulid, context: AuditContext) -> Result<()> {
        let _guard = self.node_mutation_lock.lock();
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
        if run.created_revisions.len() != run.created_ids.len() {
            return Err(Error::InvalidArgument(format!(
                "cannot revert dream {id}: output revision checkpoint is missing"
            )));
        }
        for created_id in &run.created_ids {
            let expected_revision = run.created_revisions.get(created_id).ok_or_else(|| {
                Error::InvalidArgument(format!(
                    "cannot revert dream {id}: output {created_id} lacks a revision checkpoint"
                ))
            })?;
            let node = self.get(*created_id)?.ok_or_else(|| {
                Error::InvalidArgument(format!(
                    "cannot revert dream {id}: output {created_id} is missing"
                ))
            })?;
            if node.revision != *expected_revision
                || node.state != MemoryState::Active
                || node.metadata.get("dream_run_id") != Some(&json!(id))
            {
                return Err(Error::InvalidArgument(format!(
                    "cannot revert dream {id}: output {created_id} changed after compaction"
                )));
            }
        }
        for expected in &run.added_edges {
            let current = self.dream_edge(expected)?.ok_or_else(|| {
                Error::InvalidArgument(format!(
                    "cannot revert dream {id}: edge {} -[{}]-> {} is missing",
                    expected.src, expected.label, expected.dst
                ))
            })?;
            if !same_edge_revision(&current, expected) {
                return Err(Error::InvalidArgument(format!(
                    "cannot revert dream {id}: edge {} -[{}]-> {} changed after compaction",
                    expected.src, expected.label, expected.dst
                )));
            }
        }
        run.status = DreamRunStatus::Reverting;
        self.runtime_store
            .put_dream_run(self.config.tenant, id, &run)?;
        self.retract_dream_outputs_unlocked(&run, true)?;
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

    fn dream_edge(&self, expected: &Edge) -> Result<Option<Edge>> {
        Ok(self
            .graph_store
            .neighbours_out(self.config.tenant, expected.src, Some(&expected.label))?
            .into_iter()
            .find(|edge| edge.dst == expected.dst))
    }

    fn cleanup_pending_dream_unlocked(&self, run: &DreamRun) -> Result<Vec<String>> {
        let mut preserved = Vec::new();
        for created_id in &run.created_ids {
            let Some(mut node) = self.get(*created_id)? else {
                preserved.push(format!("missing output {created_id}"));
                continue;
            };
            let Some(expected) = run.created_fingerprints.get(created_id) else {
                preserved.push(format!("uncheckpointed output {created_id}"));
                continue;
            };
            if node.state != MemoryState::Pending
                || node.metadata.get("dream_run_id") != Some(&json!(run.id))
                || dream_output_fingerprint(&node)? != *expected
            {
                preserved.push(format!("edited output {created_id}"));
                continue;
            }
            let retracted_at = crate::now_ms();
            node.state = MemoryState::Retracted;
            node.valid_from_ms = Some(retracted_at);
            node.valid_to_ms = Some(retracted_at);
            self.insert_inner_unlocked(node, false, false)?;
        }
        Ok(preserved)
    }

    fn rollback_completing_dream_unlocked(&self, run: &DreamRun) -> Result<Vec<String>> {
        let mut preserved = Vec::new();
        for expected in &run.added_edges {
            match self.dream_edge(expected)? {
                Some(current) if same_edge_revision(&current, expected) => {
                    self.graph_store.remove_edge(
                        self.config.tenant,
                        expected.src,
                        expected.dst,
                        &expected.label,
                    )?;
                }
                Some(_) => preserved.push(format!(
                    "edited edge {} -[{}]-> {}",
                    expected.src, expected.label, expected.dst
                )),
                None => {}
            }
        }
        for created_id in &run.created_ids {
            let Some(mut node) = self.get(*created_id)? else {
                preserved.push(format!("missing output {created_id}"));
                continue;
            };
            let Some(expected) = run.created_fingerprints.get(created_id) else {
                preserved.push(format!("uncheckpointed output {created_id}"));
                continue;
            };
            if !matches!(node.state, MemoryState::Pending | MemoryState::Active)
                || node.metadata.get("dream_run_id") != Some(&json!(run.id))
                || dream_output_fingerprint(&node)? != *expected
            {
                preserved.push(format!("edited output {created_id}"));
                continue;
            }
            let retracted_at = crate::now_ms();
            node.state = MemoryState::Retracted;
            node.valid_from_ms = Some(retracted_at);
            node.valid_to_ms = Some(retracted_at);
            self.insert_inner_unlocked(node, false, false)?;
        }
        for snapshot in &run.superseded {
            let Some(mut node) = self.get(snapshot.node_id)? else {
                preserved.push(format!("missing supersession target {}", snapshot.node_id));
                continue;
            };
            if node.revision == snapshot.revision_after && node.state == MemoryState::Superseded {
                node.state = snapshot.previous_state;
                node.valid_to_ms = snapshot.previous_valid_to_ms;
                self.insert_inner_unlocked(node, false, false)?;
            } else if node.revision != snapshot.revision_after.saturating_sub(1)
                || node.state != snapshot.previous_state
                || node.valid_to_ms != snapshot.previous_valid_to_ms
            {
                preserved.push(format!("edited supersession target {}", snapshot.node_id));
            }
        }
        Ok(preserved)
    }

    pub(crate) fn repair_dream_runs(&self) -> Result<()> {
        let _guard = self.node_mutation_lock.lock();
        for mut run in self.dream_runs()? {
            if !matches!(
                run.status,
                DreamRunStatus::Pending | DreamRunStatus::Completing | DreamRunStatus::Reverting
            ) {
                continue;
            }
            let was_reverting = run.status == DreamRunStatus::Reverting;
            let preserved = match run.status {
                DreamRunStatus::Pending => self.cleanup_pending_dream_unlocked(&run)?,
                DreamRunStatus::Completing => self.rollback_completing_dream_unlocked(&run)?,
                DreamRunStatus::Reverting => {
                    self.retract_dream_outputs_unlocked(&run, true)?;
                    Vec::new()
                }
                _ => unreachable!("filtered above"),
            };
            run.status = if was_reverting {
                DreamRunStatus::Reverted
            } else {
                DreamRunStatus::Repaired
            };
            run.completed_at_ms = Some(crate::now_ms());
            run.error = Some(if preserved.is_empty() {
                "incomplete dream operation repaired on reopen".into()
            } else {
                format!(
                    "incomplete dream operation repaired on reopen; preserved changed staging: {}",
                    preserved.join(", ")
                )
            });
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
                    DreamRunStatus::Pending
                        | DreamRunStatus::Completing
                        | DreamRunStatus::Completed
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
        let mut edge_keys = HashSet::new();
        for node in &proposal.nodes {
            for source in &node.source_citations {
                if !edge_keys.insert((
                    DreamEndpoint::Proposed(node.temporary_id.clone()),
                    DreamEndpoint::Existing(*source),
                    "derived_from".to_string(),
                )) {
                    return Err(Error::InvalidArgument(
                        "dream proposal contains duplicate derived_from provenance".into(),
                    ));
                }
            }
        }
        for edge in &proposal.edges {
            validate_endpoint(self, &edge.src, &temporary_ids, &source_ids)?;
            validate_endpoint(self, &edge.dst, &temporary_ids, &source_ids)?;
            if edge.relation == "derived_from" && edge.src == edge.dst {
                return Err(Error::InvalidArgument(
                    "derived_from provenance may not be a self-loop".into(),
                ));
            }
            if !edge_keys.insert((edge.src.clone(), edge.dst.clone(), edge.relation.clone())) {
                return Err(Error::InvalidArgument(format!(
                    "dream proposal contains a duplicate `{}` edge",
                    edge.relation
                )));
            }
            if let (DreamEndpoint::Existing(src), DreamEndpoint::Existing(dst)) =
                (&edge.src, &edge.dst)
            {
                let exists = self
                    .graph_store
                    .neighbours_out(self.config.tenant, *src, Some(&edge.relation))?
                    .into_iter()
                    .any(|current| current.dst == *dst);
                if exists {
                    return Err(Error::InvalidArgument(format!(
                        "dream proposal may not overwrite an existing `{}` edge",
                        edge.relation
                    )));
                }
            }
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

    fn apply_dream_proposal_unlocked(
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
            created_revisions: BTreeMap::new(),
            created_fingerprints: BTreeMap::new(),
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
                run.created_fingerprints
                    .insert(id, dream_output_fingerprint(&node)?);
                self.runtime_store
                    .put_dream_run(self.config.tenant, run.id, &run)?;
                self.insert_inner_unlocked(node, false, false)?;
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
                    run.added_edges.push(edge);
                    self.runtime_store
                        .put_dream_run(self.config.tenant, run.id, &run)?;
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
                run.added_edges.push(edge);
                self.runtime_store
                    .put_dream_run(self.config.tenant, run.id, &run)?;
            }
            for supersession in proposal.supersede {
                let node = self.get(supersession.node_id)?.ok_or(Error::NotFound)?;
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
            }
            Ok(())
        })();
        match result {
            Ok(()) => Ok(run),
            Err(error) => {
                let cleanup = self.cleanup_pending_dream_unlocked(&run);
                run.error = Some(match &cleanup {
                    Ok(conflicts) if conflicts.is_empty() => error.to_string(),
                    Ok(conflicts) => format!(
                        "{error}; preserved changed staging: {}",
                        conflicts.join(", ")
                    ),
                    Err(cleanup_error) => format!("{error}; cleanup failed: {cleanup_error}"),
                });
                if cleanup.is_err() {
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

    fn retract_dream_outputs_unlocked(
        &self,
        run: &DreamRun,
        restore_superseded: bool,
    ) -> Result<()> {
        for edge in &run.added_edges {
            self.graph_store
                .remove_edge(self.config.tenant, edge.src, edge.dst, &edge.label)?;
        }
        for id in &run.created_ids {
            if let Some(mut node) = self.get(*id)? {
                if node.state == MemoryState::Retracted && node.valid_to_ms.is_some() {
                    continue;
                }
                let retracted_at = crate::now_ms();
                node.state = MemoryState::Retracted;
                if !matches!(
                    run.status,
                    DreamRunStatus::Completed | DreamRunStatus::Reverting
                ) {
                    node.valid_from_ms = Some(retracted_at);
                }
                node.valid_to_ms = Some(retracted_at);
                self.insert_inner_unlocked(node, false, false)?;
            }
        }
        if restore_superseded {
            for snapshot in &run.superseded {
                if let Some(mut node) = self.get(snapshot.node_id)? {
                    if node.state == snapshot.previous_state
                        && node.valid_to_ms == snapshot.previous_valid_to_ms
                    {
                        self.reconcile_node_indexes_unlocked(&node)?;
                        continue;
                    }
                    node.state = snapshot.previous_state;
                    node.valid_to_ms = snapshot.previous_valid_to_ms;
                    self.insert_inner_unlocked(node, false, false)?;
                }
            }
        }
        Ok(())
    }
}

pub(crate) fn dream_output_fingerprint(node: &MemoryNode) -> Result<String> {
    let encoded = serde_json::to_vec(&json!({
        "id": node.id,
        "tenant": node.tenant,
        "kind": node.kind,
        "created_at_ms": node.created_at_ms,
        "content": &node.content,
        "metadata": &node.metadata,
        "valid_from_ms": node.valid_from_ms,
        "valid_to_ms": node.valid_to_ms,
    }))?;
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"mmdb.dream-output.v1\0");
    hasher.update(&encoded);
    Ok(hasher.finalize().to_hex().to_string())
}

fn same_edge_revision(current: &Edge, expected: &Edge) -> bool {
    current.src == expected.src
        && current.dst == expected.dst
        && current.label == expected.label
        && current.weight.to_bits() == expected.weight.to_bits()
        && current.created_at_ms == expected.created_at_ms
        && current.metadata == expected.metadata
        && current.revision == expected.revision
        && current.valid_from_ms == expected.valid_from_ms
        && current.valid_to_ms == expected.valid_to_ms
        && current.evidence == expected.evidence
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

#[cfg(test)]
mod tests {
    use super::{
        DreamEdge, DreamEndpoint, DreamNode, DreamProposal, DreamRun, DreamRunStatus,
        SupersededSnapshot,
    };
    use crate::{Database, NodeBuilder};
    use mmdb_core::{Content, MemoryState, NodeKind};
    use std::collections::BTreeMap;
    use tempfile::tempdir;

    #[test]
    fn dream_proposal_rejects_self_referential_provenance() {
        let dir = tempdir().unwrap();
        let db = Database::open(dir.path()).unwrap();
        let source_id = db
            .insert(NodeBuilder::new(NodeKind::Episode).text("source").build())
            .unwrap();
        let sources = vec![db.get(source_id).unwrap().unwrap()];
        let proposal = DreamProposal {
            nodes: vec![DreamNode {
                temporary_id: "summary".into(),
                kind: NodeKind::Fact,
                content: Content::Text("summary".into()),
                valid_from_ms: None,
                valid_to_ms: None,
                source_citations: vec![source_id],
                metadata: BTreeMap::new(),
            }],
            edges: vec![DreamEdge {
                src: DreamEndpoint::Proposed("summary".into()),
                dst: DreamEndpoint::Proposed("summary".into()),
                relation: "derived_from".into(),
                weight: 1.0,
                valid_from_ms: None,
                valid_to_ms: None,
                evidence: vec![source_id],
            }],
            supersede: Vec::new(),
            explanation: "invalid provenance".into(),
        };

        let error = db.validate_dream_proposal(&proposal, &sources).unwrap_err();
        assert!(error.to_string().contains("self-loop"));
    }

    #[test]
    fn reverting_reopen_repair_preserves_already_closed_output_and_restored_source() {
        let dir = tempdir().unwrap();
        let run_id = ulid::Ulid::new();
        let output_id;
        let source_id;
        let output_revision_after_partial_revert;
        let source_revision_after_partial_revert;
        let closed_at = crate::now_ms();
        {
            let db = Database::open(dir.path()).unwrap();
            output_id = db
                .insert(
                    NodeBuilder::new(NodeKind::Fact)
                        .text("dream output")
                        .metadata("dream_run_id", serde_json::json!(run_id))
                        .build(),
                )
                .unwrap();
            source_id = db
                .insert(
                    NodeBuilder::new(NodeKind::Fact)
                        .text("superseded dream source")
                        .embedding("dream-recovery-model", vec![1.0, 0.0])
                        .metadata("dream_run_id", serde_json::json!(ulid::Ulid::new()))
                        .build(),
                )
                .unwrap();

            let mut source = db.get(source_id).unwrap().unwrap();
            let source_previous_state = source.state;
            let source_previous_valid_to_ms = source.valid_to_ms;
            source.state = MemoryState::Superseded;
            source.valid_to_ms = Some(closed_at);
            db.insert(source).unwrap();
            let source_revision_after = db.get(source_id).unwrap().unwrap().revision;

            // Simulate a crash after the output was retracted and the source
            // restored, but before the run's Reverted terminal checkpoint.
            let mut output = db.get(output_id).unwrap().unwrap();
            output.state = MemoryState::Retracted;
            output.valid_to_ms = Some(closed_at);
            db.insert(output).unwrap();
            output_revision_after_partial_revert = db.get(output_id).unwrap().unwrap().revision;

            let mut source = db.get(source_id).unwrap().unwrap();
            source.state = source_previous_state;
            source.valid_to_ms = source_previous_valid_to_ms;
            db.insert(source).unwrap();
            source_revision_after_partial_revert = db.get(source_id).unwrap().unwrap().revision;
            db.vector_store
                .delete(0, "dream-recovery-model", source_id)
                .unwrap();
            assert!(db
                .vector_store
                .search(0, "dream-recovery-model", &[1.0, 0.0], 4)
                .unwrap()
                .is_empty());

            let run = DreamRun {
                id: run_id,
                profile_id: "test".into(),
                profile_revision: "r1".into(),
                source_hash: "partial-revert".into(),
                source_ids: vec![source_id],
                created_ids: vec![output_id],
                created_revisions: BTreeMap::new(),
                created_fingerprints: BTreeMap::new(),
                added_edges: Vec::new(),
                superseded: vec![SupersededSnapshot {
                    node_id: source_id,
                    previous_state: source_previous_state,
                    previous_valid_to_ms: source_previous_valid_to_ms,
                    revision_after: source_revision_after,
                }],
                explanation: "simulate interrupted revert".into(),
                status: DreamRunStatus::Reverting,
                created_at_ms: closed_at,
                completed_at_ms: None,
                error: None,
            };
            db.runtime_store.put_dream_run(0, run_id, &run).unwrap();
        }

        let db = Database::open(dir.path()).unwrap();
        let output = db.get(output_id).unwrap().unwrap();
        assert_eq!(output.state, MemoryState::Retracted);
        assert_eq!(output.valid_to_ms, Some(closed_at));
        assert_eq!(output.revision, output_revision_after_partial_revert);
        let source = db.get(source_id).unwrap().unwrap();
        assert_eq!(source.state, MemoryState::Active);
        assert_eq!(source.valid_to_ms, None);
        assert_eq!(source.revision, source_revision_after_partial_revert);
        assert!(db
            .vector_store
            .search(0, "dream-recovery-model", &[1.0, 0.0], 4)
            .unwrap()
            .iter()
            .any(|hit| hit.node_id == source_id));
        assert_eq!(
            db.dream_run(run_id).unwrap().unwrap().status,
            DreamRunStatus::Reverted
        );
    }
}
