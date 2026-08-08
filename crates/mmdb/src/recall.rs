use crate::audit::{node_snapshot, sanitize_value, AuditAction, AuditContext};
use crate::runtime::{
    AgentRequest, AgentRole, EmbeddingInput, EmbeddingProfile, LawyerFailureMode, LawyerProfile,
};
use crate::{Database, MemoryProfile};
use mmdb_core::{Edge, Error, MemoryNode, MemoryState, NodeKind, Result};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet, HashSet, VecDeque};
use ulid::Ulid;

const RRF_K: f32 = 60.0;
const MAX_LAWYER_CANDIDATES: usize = 50;
const MAX_LAWYER_BYTES: usize = 128 * 1024;

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RecallFilter {
    pub kinds: Vec<NodeKind>,
    pub metadata: BTreeMap<String, Value>,
    /// Exclude facts without durable provenance before any candidate limit is
    /// applied. Non-fact memories are trusted by construction.
    #[serde(default)]
    pub require_verified: bool,
    /// Metadata keys whose presence marks an otherwise unverified fact as an
    /// explicitly trusted application-owned record.
    #[serde(default)]
    pub allow_unverified_metadata_keys: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RecallRequest {
    pub query: String,
    pub as_of_ms: i64,
    pub limit: usize,
    pub candidate_limit: usize,
    pub vector_profiles: Vec<String>,
    #[serde(default)]
    pub min_vector_similarity: Option<f32>,
    pub lexical: bool,
    pub graph_depth: usize,
    pub lawyer_profile: Option<String>,
    pub filter: RecallFilter,
    #[serde(default)]
    pub audit: AuditContext,
}

impl RecallRequest {
    pub fn new(query: impl Into<String>) -> Self {
        Self {
            query: query.into(),
            as_of_ms: crate::now_ms(),
            limit: 10,
            candidate_limit: 50,
            vector_profiles: Vec::new(),
            min_vector_similarity: None,
            lexical: true,
            graph_depth: 1,
            lawyer_profile: None,
            filter: RecallFilter::default(),
            audit: AuditContext::default(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VectorEvidence {
    pub profile_id: String,
    pub model: String,
    pub rank: usize,
    pub similarity: f32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GraphPath {
    pub nodes: Vec<Ulid>,
    pub edges: Vec<Edge>,
    pub weight: f32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RecallEvidence {
    pub node: MemoryNode,
    pub score: f32,
    pub lexical_score: Option<f32>,
    pub lexical_rank: Option<usize>,
    pub lexical_terms: Vec<String>,
    pub vectors: Vec<VectorEvidence>,
    pub graph_paths: Vec<GraphPath>,
    pub provenance: Vec<Ulid>,
    pub conflicts: Vec<Ulid>,
    pub verified: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RecallStatus {
    Deterministic,
    Adjudicated,
    Degraded { reason: String },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AdjudicatedRecall {
    pub operation_id: Ulid,
    pub evidence: Vec<RecallEvidence>,
    pub verdict: Option<LawyerVerdict>,
    pub status: RecallStatus,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CausalAnnotation {
    pub candidate_id: Ulid,
    pub note: String,
    pub evidence_ids: Vec<Ulid>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LawyerVerdict {
    pub accepted_candidate_ids: Vec<Ulid>,
    pub rejected_candidate_ids: Vec<Ulid>,
    pub final_order: Vec<Ulid>,
    pub annotations: Vec<CausalAnnotation>,
    pub cited_evidence_ids: Vec<Ulid>,
    pub unresolved_conflicts: Vec<[Ulid; 2]>,
    pub proposals: Vec<ChangeProposal>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ChangeProposalStatus {
    Pending,
    Applying,
    Applied,
    Rejected,
    Stale,
}

fn default_proposal_status() -> ChangeProposalStatus {
    ChangeProposalStatus::Pending
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ProposedChange {
    SetValidity {
        node_id: Ulid,
        expected_revision: u64,
        valid_from_ms: Option<i64>,
        valid_to_ms: Option<i64>,
    },
    SetState {
        node_id: Ulid,
        expected_revision: u64,
        state: MemoryState,
    },
    AddEdge {
        edge: Edge,
        expected_src_revision: u64,
        expected_dst_revision: u64,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChangeProposal {
    #[serde(default = "Ulid::new")]
    pub id: Ulid,
    pub reason: String,
    #[serde(default)]
    pub changes: Vec<ProposedChange>,
    #[serde(default = "default_proposal_status")]
    pub status: ChangeProposalStatus,
    #[serde(default)]
    pub source_operation: Option<Ulid>,
    #[serde(default)]
    pub created_at_ms: i64,
    #[serde(default)]
    pub applied_at_ms: Option<i64>,
    /// Number of changes durably applied. `Applying` proposals resume at this
    /// cursor after an interrupted process or retry.
    #[serde(default)]
    pub next_change: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GraphSlice {
    pub as_of_ms: i64,
    pub nodes: Vec<MemoryNode>,
    pub edges: Vec<Edge>,
    pub paths: Vec<GraphPath>,
}

#[derive(Default)]
struct EvidenceBuilder {
    score: f32,
    lexical_score: Option<f32>,
    lexical_rank: Option<usize>,
    lexical_terms: Vec<String>,
    vectors: Vec<VectorEvidence>,
    graph_paths: Vec<GraphPath>,
}

struct PreparedVectorQuery {
    profile: EmbeddingProfile,
    profile_fingerprint: String,
    vector: Vec<f32>,
}

impl Database {
    pub async fn recall(&self, request: RecallRequest) -> Result<AdjudicatedRecall> {
        let operation_id = Ulid::new();
        let context = request.audit.clone();
        let result = self.recall_inner(operation_id, &request).await;
        let details = json!({
            "request": request,
            "result": result.as_ref().ok().map(|recall| recall.evidence.iter().map(evidence_audit_value).collect::<Vec<_>>()),
            "status": result.as_ref().ok().map(|recall| &recall.status),
        });
        self.append_audit(
            operation_id,
            AuditAction::Query,
            "recall",
            result.is_ok(),
            context,
            details,
            result.as_ref().err().map(ToString::to_string),
        )?;
        result
    }

    async fn recall_inner(
        &self,
        operation_id: Ulid,
        request: &RecallRequest,
    ) -> Result<AdjudicatedRecall> {
        if request
            .min_vector_similarity
            .is_some_and(|threshold| !threshold.is_finite() || !(0.0..=1.0).contains(&threshold))
        {
            return Err(Error::InvalidArgument(
                "min_vector_similarity must be finite and between 0 and 1".into(),
            ));
        }
        if request.limit == 0 || request.candidate_limit == 0 {
            return Ok(AdjudicatedRecall {
                operation_id,
                evidence: Vec::new(),
                verdict: None,
                status: RecallStatus::Deterministic,
            });
        }
        let initial_profile = self.memory_profile()?;
        let selected_profiles = select_vector_profiles(&initial_profile, &request.vector_profiles)?
            .into_iter()
            .cloned()
            .collect::<Vec<_>>();
        let mut degraded = Vec::new();
        let mut prepared = Vec::new();
        for embedding_profile in selected_profiles {
            let query_vector = match self
                .embed_recall_query(operation_id, &request.query, &embedding_profile)
                .await
            {
                Ok(vector) => vector,
                Err(error) => {
                    degraded.push(error.to_string());
                    continue;
                }
            };
            prepared.push(PreparedVectorQuery {
                profile_fingerprint: embedding_profile.fingerprint(),
                profile: embedding_profile,
                vector: query_vector,
            });
        }
        let (profile, deterministic) = {
            let _guard = self.node_mutation_lock.lock();
            let profile = self.memory_profile()?;
            prepared.retain(|query| {
                let current = profile
                    .embedding_profiles
                    .iter()
                    .find(|candidate| candidate.id == query.profile.id);
                let current = current
                    .is_some_and(|candidate| candidate.fingerprint() == query.profile_fingerprint);
                if !current {
                    degraded.push(format!(
                        "embedding profile `{}` changed while preparing recall",
                        query.profile.id
                    ));
                }
                current
            });
            let evidence = self.deterministic_recall(request, &prepared)?;
            (profile, evidence)
        };
        let Some(lawyer_id) = request.lawyer_profile.as_deref() else {
            let mut evidence = deterministic;
            evidence.truncate(request.limit);
            return Ok(AdjudicatedRecall {
                operation_id,
                evidence,
                verdict: None,
                status: if degraded.is_empty() {
                    RecallStatus::Deterministic
                } else {
                    RecallStatus::Degraded {
                        reason: degraded.join("; "),
                    }
                },
            });
        };
        let lawyer = resolve_lawyer(&profile, lawyer_id)?;
        match self
            .adjudicate(operation_id, &request.query, &deterministic, lawyer)
            .await
        {
            Ok((evidence, verdict)) => Ok(AdjudicatedRecall {
                operation_id,
                evidence: evidence.into_iter().take(request.limit).collect(),
                verdict: Some(verdict),
                status: RecallStatus::Adjudicated,
            }),
            Err(error) if lawyer.failure_mode == LawyerFailureMode::ReturnDeterministic => {
                let mut evidence = deterministic;
                evidence.truncate(request.limit);
                Ok(AdjudicatedRecall {
                    operation_id,
                    evidence,
                    verdict: None,
                    status: RecallStatus::Degraded {
                        reason: error.to_string(),
                    },
                })
            }
            Err(error) => Err(error),
        }
    }

    /// Build all index, lifecycle, graph, and hydration evidence while the
    /// caller holds `node_mutation_lock`. Remote embedding and lawyer calls
    /// deliberately live outside this synchronous snapshot.
    fn deterministic_recall(
        &self,
        request: &RecallRequest,
        prepared: &[PreparedVectorQuery],
    ) -> Result<Vec<RecallEvidence>> {
        let candidate_limit = request.candidate_limit.max(request.limit);
        let mut builders: BTreeMap<Ulid, EvidenceBuilder> = BTreeMap::new();
        let candidate_error = parking_lot::Mutex::new(None);

        if request.lexical {
            let filter = |id| match self.recall_candidate_matches(id, request, None) {
                Ok(matches) => matches,
                Err(error) => {
                    let mut deferred = candidate_error.lock();
                    if deferred.is_none() {
                        *deferred = Some(error);
                    }
                    false
                }
            };
            let lexical_hits = self.lexical_index.search_with_filter(
                self.config.tenant,
                &request.query,
                candidate_limit,
                filter,
            );
            if let Some(error) = candidate_error.lock().take() {
                return Err(error);
            }
            for (rank, hit) in lexical_hits?.into_iter().enumerate() {
                let entry = builders.entry(hit.node_id).or_default();
                entry.score += 1.0 / (RRF_K + rank as f32 + 1.0);
                entry.lexical_score = Some(hit.score);
                entry.lexical_rank = Some(rank + 1);
                entry.lexical_terms = hit.terms;
            }
        }

        for query in prepared {
            let filter = |id| match self.recall_candidate_matches(id, request, Some(&query.profile))
            {
                Ok(matches) => matches,
                Err(error) => {
                    let mut deferred = candidate_error.lock();
                    if deferred.is_none() {
                        *deferred = Some(error);
                    }
                    false
                }
            };
            let hits = self.vector_store.search_with_filter(
                self.config.tenant,
                &query.profile.model,
                &query.vector,
                candidate_limit,
                Some(&filter),
            );
            if let Some(error) = candidate_error.lock().take() {
                return Err(error);
            }
            for (rank, hit) in hits?
                .into_iter()
                .filter(|hit| {
                    request
                        .min_vector_similarity
                        .is_none_or(|minimum| hit.score >= minimum)
                })
                .enumerate()
            {
                let entry = builders.entry(hit.node_id).or_default();
                entry.score += query.profile.weight / (RRF_K + rank as f32 + 1.0);
                entry.vectors.push(VectorEvidence {
                    profile_id: query.profile.id.clone(),
                    model: query.profile.model.clone(),
                    rank: rank + 1,
                    similarity: hit.score,
                });
            }
        }

        let mut invalid = Vec::new();
        for id in builders.keys() {
            if !self.recall_candidate_matches(*id, request, None)? {
                invalid.push(*id);
            }
        }
        for id in invalid {
            builders.remove(&id);
        }

        let mut seeds: Vec<(Ulid, f32)> = builders
            .iter()
            .map(|(id, evidence)| (*id, evidence.score))
            .collect();
        seeds.sort_by(|a, b| b.1.total_cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
        seeds.truncate(candidate_limit);
        if request.graph_depth > 0 {
            for (seed, seed_score) in seeds {
                for path in self.graph_paths(seed, request.as_of_ms, request.graph_depth)? {
                    let Some(node_id) = path.nodes.last().copied() else {
                        continue;
                    };
                    let entry = builders.entry(node_id).or_default();
                    entry.score += seed_score * path.weight * 0.25;
                    entry.graph_paths.push(path);
                }
            }
        }

        let mut evidence = Vec::new();
        for (id, builder) in builders {
            let Some(node) = self.storage.get_node(self.config.tenant, id)? else {
                continue;
            };
            if !node.is_valid_at(request.as_of_ms) || !request.filter.matches(&node) {
                continue;
            }
            let (provenance, conflicts) = self.provenance_and_conflicts(id, request.as_of_ms)?;
            let verified = node.kind != NodeKind::Fact || !provenance.is_empty();
            evidence.push(RecallEvidence {
                node,
                score: builder.score,
                lexical_score: builder.lexical_score,
                lexical_rank: builder.lexical_rank,
                lexical_terms: builder.lexical_terms,
                vectors: builder.vectors,
                graph_paths: builder.graph_paths,
                provenance,
                conflicts,
                verified,
            });
        }
        self.surface_conflicts(&mut evidence, request)?;
        evidence.retain(|candidate| request.filter.accepts_trust(candidate));
        evidence.sort_by(|a, b| {
            b.score
                .total_cmp(&a.score)
                .then_with(|| a.node.id.cmp(&b.node.id))
        });
        evidence.truncate(candidate_limit);
        Ok(evidence)
    }

    fn recall_candidate_matches(
        &self,
        id: Ulid,
        request: &RecallRequest,
        embedding_profile: Option<&EmbeddingProfile>,
    ) -> Result<bool> {
        let Some(node) = self.storage.get_node(self.config.tenant, id)? else {
            return Ok(false);
        };
        if !node.is_valid_at(request.as_of_ms) || !request.filter.matches(&node) {
            return Ok(false);
        }
        if request.filter.requires_provenance(&node) {
            let (provenance, _) = self.provenance_and_conflicts(id, request.as_of_ms)?;
            if provenance.is_empty() {
                return Ok(false);
            }
        }
        if let Some(profile) = embedding_profile {
            if let Some(status) =
                self.runtime_store
                    .projection(self.config.tenant, id, &profile.id)?
            {
                return Ok(status.is_current_for(profile, &node));
            }
        }
        Ok(true)
    }

    async fn embed_recall_query(
        &self,
        operation_id: Ulid,
        query: &str,
        profile: &crate::EmbeddingProfile,
    ) -> Result<Vec<f32>> {
        let result = if profile.client_id == "legacy" {
            let embedder = self.embedder.as_ref().ok_or_else(|| {
                Error::InvalidArgument(format!(
                    "embedding client `{}` is not registered",
                    profile.client_id
                ))
            })?;
            if embedder.model_name() != profile.model {
                return Err(Error::InvalidArgument(format!(
                    "legacy embedder does not provide model `{}`",
                    profile.model
                )));
            }
            embedder.embed_async(query).await
        } else {
            let client = self.clients.embedding(&profile.client_id)?.ok_or_else(|| {
                Error::InvalidArgument(format!(
                    "embedding client `{}` is not registered",
                    profile.client_id
                ))
            })?;
            client
                .embed(EmbeddingInput::Text(query.to_string()), profile)
                .await
                .map(|output| output.vector)
        };
        let valid = result.as_ref().is_ok_and(|vector| {
            vector.len() == profile.dimension as usize && vector.iter().all(|v| v.is_finite())
        });
        let error = match &result {
            Ok(vector) if !valid => Some(format!(
                "invalid embedding output: expected {} finite values, got {}",
                profile.dimension,
                vector.len()
            )),
            Err(error) => Some(error.to_string()),
            _ => None,
        };
        self.append_audit(
            operation_id,
            AuditAction::ClientCall,
            "recall_embedding",
            valid,
            AuditContext::default(),
            json!({
                "profile": profile,
                "request": {"type": "text", "text": query},
                "response": result.as_ref().ok().map(|vector| json!({"dimension": vector.len()})),
            }),
            error.clone(),
        )?;
        let vector = result?;
        if let Some(error) = error {
            return Err(Error::InvalidArgument(error));
        }
        Ok(vector)
    }

    async fn adjudicate(
        &self,
        operation_id: Ulid,
        query: &str,
        evidence: &[RecallEvidence],
        profile: &LawyerProfile,
    ) -> Result<(Vec<RecallEvidence>, LawyerVerdict)> {
        let client = self.clients.agent(&profile.client_id)?.ok_or_else(|| {
            Error::InvalidArgument(format!(
                "agent client `{}` is not registered",
                profile.client_id
            ))
        })?;
        let candidate_limit = profile.evidence_limit.min(MAX_LAWYER_CANDIDATES);
        let payload_evidence = bounded_evidence_payload(evidence, candidate_limit);
        let request = AgentRequest {
            role: AgentRole::Lawyer,
            agent_id: profile.agent_id.clone(),
            model_id: profile.model_id.clone(),
            prompt_version: profile.prompt_version.clone(),
            payload: json!({
                "query": query,
                "rule_set": profile.rule_set,
                "evidence": payload_evidence,
                "authority": "gate_and_propose",
            }),
        };
        let response = client.call(request.clone()).await;
        self.append_audit(
            operation_id,
            AuditAction::ClientCall,
            "lawyer",
            response.is_ok(),
            AuditContext::default(),
            sanitize_value(&json!({"request": request, "response": response.as_ref().ok()})),
            response.as_ref().err().map(ToString::to_string),
        )?;
        let mut verdict: LawyerVerdict =
            serde_json::from_value(response?.payload).map_err(|error| {
                Error::InvalidArgument(format!("malformed lawyer response: {error}"))
            })?;
        validate_verdict(&verdict, evidence)?;
        let allowed_proposal_ids = allowed_evidence_ids(evidence);
        for proposal in &mut verdict.proposals {
            proposal.id = Ulid::new();
            proposal.status = ChangeProposalStatus::Pending;
            proposal.source_operation = Some(operation_id);
            proposal.created_at_ms = crate::now_ms();
            proposal.applied_at_ms = None;
            proposal.next_change = 0;
            validate_proposal_scope(proposal, &allowed_proposal_ids)?;
            self.validate_proposal(proposal)?;
            self.runtime_store
                .put_proposal(self.config.tenant, proposal.id, proposal)?;
        }
        let accepted: HashSet<_> = verdict.accepted_candidate_ids.iter().copied().collect();
        let mut by_id: BTreeMap<_, _> = evidence
            .iter()
            .filter(|candidate| accepted.contains(&candidate.node.id))
            .map(|candidate| (candidate.node.id, candidate.clone()))
            .collect();
        let mut ordered = Vec::new();
        for id in &verdict.final_order {
            if let Some(candidate) = by_id.remove(id) {
                ordered.push(candidate);
            }
        }
        for candidate in evidence {
            if let Some(candidate) = by_id.remove(&candidate.node.id) {
                ordered.push(candidate);
            }
        }
        Ok((ordered, verdict))
    }

    pub fn proposals(&self) -> Result<Vec<ChangeProposal>> {
        self.runtime_store.proposals(self.config.tenant)
    }

    pub fn proposal(&self, id: Ulid) -> Result<Option<ChangeProposal>> {
        self.runtime_store.proposal(self.config.tenant, id)
    }

    pub fn apply_proposal(&self, id: Ulid, context: AuditContext) -> Result<()> {
        let _guard = self.node_mutation_lock.lock();
        let mut proposal: ChangeProposal = self.proposal(id)?.ok_or(Error::NotFound)?;
        match proposal.status {
            ChangeProposalStatus::Pending => {
                if let Err(error) = self.validate_proposal(&proposal) {
                    proposal.status = ChangeProposalStatus::Stale;
                    self.runtime_store
                        .put_proposal(self.config.tenant, id, &proposal)?;
                    self.append_audit(
                        Ulid::new(),
                        AuditAction::Proposal,
                        "apply_proposal",
                        false,
                        context,
                        json!({"proposal": proposal}),
                        Some(error.to_string()),
                    )?;
                    return Err(error);
                }
                proposal.status = ChangeProposalStatus::Applying;
                proposal.next_change = 0;
                self.runtime_store
                    .put_proposal(self.config.tenant, id, &proposal)?;
            }
            ChangeProposalStatus::Applying => {}
            _ => {
                return Err(Error::InvalidArgument(format!(
                    "proposal {id} is not pending or applying"
                )));
            }
        }

        let result = self.resume_proposal_unlocked(&mut proposal);
        self.append_audit(
            Ulid::new(),
            AuditAction::Proposal,
            "apply_proposal",
            result.is_ok(),
            context,
            json!({"proposal": proposal}),
            result.as_ref().err().map(ToString::to_string),
        )?;
        result
    }

    fn resume_proposal_unlocked(&self, proposal: &mut ChangeProposal) -> Result<()> {
        if proposal.status != ChangeProposalStatus::Applying {
            return Err(Error::InvalidArgument(format!(
                "proposal {} is not applying",
                proposal.id
            )));
        }
        if proposal.next_change > proposal.changes.len() {
            return Err(Error::InvalidArgument(format!(
                "proposal {} has an invalid apply cursor {} for {} changes",
                proposal.id,
                proposal.next_change,
                proposal.changes.len()
            )));
        }
        while proposal.next_change < proposal.changes.len() {
            let index = proposal.next_change;
            let change = proposal.changes[index].clone();
            self.apply_proposed_change_unlocked(proposal, index, change)?;
            proposal.next_change = index + 1;
            self.runtime_store
                .put_proposal(self.config.tenant, proposal.id, proposal)?;
        }
        proposal.status = ChangeProposalStatus::Applied;
        proposal.applied_at_ms = Some(crate::now_ms());
        self.runtime_store
            .put_proposal(self.config.tenant, proposal.id, proposal)
    }

    fn apply_proposed_change_unlocked(
        &self,
        proposal: &ChangeProposal,
        index: usize,
        change: ProposedChange,
    ) -> Result<()> {
        match change {
            ProposedChange::SetValidity {
                node_id,
                expected_revision,
                valid_from_ms,
                valid_to_ms,
            } => {
                let prior = prior_node_changes(&proposal.changes[..index], node_id);
                let before_revision = expected_revision.saturating_add(prior);
                let after_revision = before_revision.saturating_add(1);
                let mut node = self.get(node_id)?.ok_or(Error::NotFound)?;
                if node.revision == after_revision
                    && node.valid_from_ms == valid_from_ms
                    && node.valid_to_ms == valid_to_ms
                {
                    return self.reconcile_proposal_node_indexes_unlocked(&node);
                }
                if node.revision != before_revision {
                    return Err(proposal_resume_conflict(
                        proposal.id,
                        node_id,
                        before_revision,
                        after_revision,
                        node.revision,
                    ));
                }
                node.valid_from_ms = valid_from_ms;
                node.valid_to_ms = valid_to_ms;
                self.insert_inner_unlocked(node, false, false)?;
                Ok(())
            }
            ProposedChange::SetState {
                node_id,
                expected_revision,
                state,
            } => {
                let prior = prior_node_changes(&proposal.changes[..index], node_id);
                let before_revision = expected_revision.saturating_add(prior);
                let after_revision = before_revision.saturating_add(1);
                let mut node = self.get(node_id)?.ok_or(Error::NotFound)?;
                let terminal_validity_is_set =
                    !matches!(state, MemoryState::Superseded | MemoryState::Retracted)
                        || node.valid_to_ms.is_some();
                if node.revision == after_revision
                    && node.state == state
                    && terminal_validity_is_set
                {
                    return self.reconcile_proposal_node_indexes_unlocked(&node);
                }
                if node.revision != before_revision {
                    return Err(proposal_resume_conflict(
                        proposal.id,
                        node_id,
                        before_revision,
                        after_revision,
                        node.revision,
                    ));
                }
                node.state = state;
                if matches!(state, MemoryState::Superseded | MemoryState::Retracted)
                    && node.valid_to_ms.is_none()
                {
                    node.valid_to_ms = Some(crate::now_ms());
                }
                self.insert_inner_unlocked(node, false, false)?;
                Ok(())
            }
            ProposedChange::AddEdge {
                edge,
                expected_src_revision,
                expected_dst_revision,
            } => {
                let expected = normalized_new_edge(edge);
                match self.proposal_edge(&expected)? {
                    Some(current) if same_proposal_edge(&current, &expected) => Ok(()),
                    Some(_) => Err(Error::InvalidArgument(format!(
                        "cannot resume proposal {}: edge {} -[{}]-> {} changed while applying",
                        proposal.id, expected.src, expected.label, expected.dst
                    ))),
                    None => {
                        for (role, node_id, base_revision) in [
                            ("source", expected.src, expected_src_revision),
                            ("destination", expected.dst, expected_dst_revision),
                        ] {
                            let expected_revision = base_revision.saturating_add(
                                prior_node_changes(&proposal.changes[..index], node_id),
                            );
                            let actual_revision =
                                self.get(node_id)?.ok_or(Error::NotFound)?.revision;
                            if actual_revision != expected_revision {
                                return Err(Error::InvalidArgument(format!(
                                    "cannot resume proposal {}: edge {role} {node_id} expected revision {expected_revision}, found {actual_revision}",
                                    proposal.id
                                )));
                            }
                        }
                        self.add_edge_unlocked(expected)
                    }
                }
            }
        }
    }

    fn reconcile_proposal_node_indexes_unlocked(&self, node: &MemoryNode) -> Result<()> {
        self.reconcile_node_indexes_unlocked(node)
    }

    fn proposal_edge(&self, expected: &Edge) -> Result<Option<Edge>> {
        Ok(self
            .graph_store
            .neighbours_out(self.config.tenant, expected.src, Some(&expected.label))?
            .into_iter()
            .find(|edge| edge.dst == expected.dst))
    }

    pub(crate) fn repair_applying_proposals(&self) -> Result<()> {
        let _guard = self.node_mutation_lock.lock();
        for mut proposal in self.proposals()? {
            if proposal.status != ChangeProposalStatus::Applying {
                continue;
            }
            let result = self.resume_proposal_unlocked(&mut proposal);
            self.append_audit(
                Ulid::new(),
                AuditAction::Repair,
                "repair_proposal",
                result.is_ok(),
                AuditContext::default(),
                json!({"proposal": proposal}),
                result.as_ref().err().map(ToString::to_string),
            )?;
            result?;
        }
        Ok(())
    }

    pub fn reject_proposal(&self, id: Ulid, context: AuditContext) -> Result<()> {
        let _guard = self.node_mutation_lock.lock();
        let mut proposal: ChangeProposal = self.proposal(id)?.ok_or(Error::NotFound)?;
        if proposal.status != ChangeProposalStatus::Pending {
            return Err(Error::InvalidArgument(format!(
                "proposal {id} is not pending"
            )));
        }
        proposal.status = ChangeProposalStatus::Rejected;
        self.runtime_store
            .put_proposal(self.config.tenant, id, &proposal)?;
        self.append_audit(
            Ulid::new(),
            AuditAction::Proposal,
            "reject_proposal",
            true,
            context,
            json!({"proposal": proposal}),
            None,
        )
    }

    fn validate_proposal(&self, proposal: &ChangeProposal) -> Result<()> {
        let mut added_edges = HashSet::new();
        for change in &proposal.changes {
            match change {
                ProposedChange::SetValidity {
                    node_id,
                    expected_revision,
                    valid_from_ms,
                    valid_to_ms,
                } => {
                    if valid_from_ms
                        .zip(*valid_to_ms)
                        .is_some_and(|(from, to)| from >= to)
                    {
                        return Err(Error::InvalidArgument(
                            "invalid proposed validity interval".into(),
                        ));
                    }
                    require_revision(self, *node_id, *expected_revision)?;
                }
                ProposedChange::SetState {
                    node_id,
                    expected_revision,
                    ..
                } => require_revision(self, *node_id, *expected_revision)?,
                ProposedChange::AddEdge {
                    edge,
                    expected_src_revision,
                    expected_dst_revision,
                } => {
                    require_revision(self, edge.src, *expected_src_revision)?;
                    require_revision(self, edge.dst, *expected_dst_revision)?;
                    validate_relation(edge)?;
                    if !added_edges.insert((edge.src, edge.dst, edge.label.clone())) {
                        return Err(Error::InvalidArgument(format!(
                            "proposal contains duplicate `{}` edge",
                            edge.label
                        )));
                    }
                    if self.proposal_edge(edge)?.is_some() {
                        return Err(Error::InvalidArgument(format!(
                            "proposal may not overwrite existing edge {} -[{}]-> {}",
                            edge.src, edge.label, edge.dst
                        )));
                    }
                    for evidence in &edge.evidence {
                        if self.get(*evidence)?.is_none() {
                            return Err(Error::InvalidArgument(format!(
                                "proposed edge evidence node {evidence} does not exist"
                            )));
                        }
                    }
                }
            }
        }
        Ok(())
    }

    pub fn graph_slice(&self, seed: Ulid, as_of_ms: i64, depth: usize) -> Result<GraphSlice> {
        let paths = self.graph_paths(seed, as_of_ms, depth)?;
        let mut ids = BTreeSet::from([seed]);
        let mut edges = BTreeMap::new();
        for path in &paths {
            ids.extend(path.nodes.iter().copied());
            for edge in &path.edges {
                edges.insert((edge.src, edge.dst, edge.label.clone()), edge.clone());
            }
        }
        let nodes = ids
            .into_iter()
            .filter_map(|id| self.get(id).transpose())
            .collect::<Result<Vec<_>>>()?
            .into_iter()
            .filter(|node| node.is_valid_at(as_of_ms))
            .collect();
        let slice = GraphSlice {
            as_of_ms,
            nodes,
            edges: edges.into_values().collect(),
            paths,
        };
        self.append_audit(
            Ulid::new(),
            AuditAction::Query,
            "graph_slice",
            true,
            AuditContext::default(),
            json!({"seed": seed, "as_of_ms": as_of_ms, "depth": depth, "node_ids": slice.nodes.iter().map(|node| node.id).collect::<Vec<_>>() }),
            None,
        )?;
        Ok(slice)
    }

    fn graph_paths(&self, seed: Ulid, as_of_ms: i64, depth: usize) -> Result<Vec<GraphPath>> {
        if self
            .get(seed)?
            .is_none_or(|node| !node.is_valid_at(as_of_ms))
        {
            return Ok(Vec::new());
        }
        let mut paths = Vec::new();
        let mut frontier = VecDeque::from([(seed, vec![seed], Vec::new(), 1.0_f32)]);
        let mut best_depth = BTreeMap::from([(seed, 0_usize)]);
        while let Some((current, nodes, edges, weight)) = frontier.pop_front() {
            if edges.len() >= depth {
                continue;
            }
            let mut adjacent =
                self.graph_store
                    .neighbours_out(self.config.tenant, current, None)?;
            adjacent.extend(
                self.graph_store
                    .neighbours_in(self.config.tenant, current, None)?
                    .into_iter()
                    .filter(|edge| !is_causal(&edge.label)),
            );
            for edge in adjacent {
                if !edge.is_valid_at(as_of_ms) {
                    continue;
                }
                let next = if edge.src == current {
                    edge.dst
                } else {
                    edge.src
                };
                let Some(next_node) = self.get(next)? else {
                    continue;
                };
                if !next_node.is_valid_at(as_of_ms) || nodes.contains(&next) {
                    continue;
                }
                if is_causal(&edge.label) {
                    let Some(source) = self.get(edge.src)? else {
                        continue;
                    };
                    let Some(destination) = self.get(edge.dst)? else {
                        continue;
                    };
                    if valid_from(&source) > valid_from(&destination) {
                        continue;
                    }
                }
                let next_depth = edges.len() + 1;
                if best_depth.get(&next).is_some_and(|seen| *seen < next_depth) {
                    continue;
                }
                best_depth.insert(next, next_depth);
                let mut next_nodes = nodes.clone();
                next_nodes.push(next);
                let mut next_edges = edges.clone();
                next_edges.push(edge.clone());
                let next_weight = weight * edge.weight;
                let path = GraphPath {
                    nodes: next_nodes.clone(),
                    edges: next_edges.clone(),
                    weight: next_weight,
                };
                paths.push(path);
                frontier.push_back((next, next_nodes, next_edges, next_weight));
            }
        }
        Ok(paths)
    }

    fn provenance_and_conflicts(&self, id: Ulid, as_of_ms: i64) -> Result<(Vec<Ulid>, Vec<Ulid>)> {
        let mut provenance = BTreeSet::new();
        let mut conflicts = BTreeSet::new();
        let mut edges = self
            .graph_store
            .neighbours_out(self.config.tenant, id, None)?;
        edges.extend(
            self.graph_store
                .neighbours_in(self.config.tenant, id, None)?,
        );
        for edge in edges.into_iter().filter(|edge| edge.is_valid_at(as_of_ms)) {
            if edge.label == "derived_from" && (edge.src != id || edge.dst == id) {
                continue;
            }
            let other = if edge.src == id { edge.dst } else { edge.src };
            if self
                .get(other)?
                .is_none_or(|node| !node.is_valid_at(as_of_ms))
            {
                continue;
            }
            if edge.label == "derived_from" || edge.evidence.contains(&other) {
                provenance.insert(other);
            }
            for evidence in &edge.evidence {
                if *evidence != id
                    && self
                        .get(*evidence)?
                        .is_some_and(|node| node.is_valid_at(as_of_ms))
                {
                    provenance.insert(*evidence);
                }
            }
            if edge.label == "contradicts" {
                conflicts.insert(other);
            }
        }
        Ok((
            provenance.into_iter().collect(),
            conflicts.into_iter().collect(),
        ))
    }

    fn surface_conflicts(
        &self,
        evidence: &mut Vec<RecallEvidence>,
        request: &RecallRequest,
    ) -> Result<()> {
        let existing: HashSet<_> = evidence.iter().map(|item| item.node.id).collect();
        let mut conflict_scores = BTreeMap::new();
        for item in evidence.iter() {
            for conflict in &item.conflicts {
                if !existing.contains(conflict) {
                    conflict_scores
                        .entry(*conflict)
                        .and_modify(|score: &mut f32| *score = score.max(item.score))
                        .or_insert(item.score);
                }
            }
        }
        for (id, score) in conflict_scores {
            let Some(node) = self.get(id)? else {
                continue;
            };
            if !node.is_valid_at(request.as_of_ms) || !request.filter.matches(&node) {
                continue;
            }
            let (provenance, conflicts) = self.provenance_and_conflicts(id, request.as_of_ms)?;
            evidence.push(RecallEvidence {
                verified: node.kind != NodeKind::Fact || !provenance.is_empty(),
                node,
                score,
                lexical_score: None,
                lexical_rank: None,
                lexical_terms: Vec::new(),
                vectors: Vec::new(),
                graph_paths: Vec::new(),
                provenance,
                conflicts,
            });
        }
        Ok(())
    }
}

impl RecallFilter {
    fn matches(&self, node: &MemoryNode) -> bool {
        (self.kinds.is_empty() || self.kinds.contains(&node.kind))
            && self
                .metadata
                .iter()
                .all(|(key, value)| node.metadata.get(key) == Some(value))
    }

    fn allows_unverified(&self, node: &MemoryNode) -> bool {
        self.allow_unverified_metadata_keys
            .iter()
            .any(|key| node.metadata.contains_key(key))
    }

    fn requires_provenance(&self, node: &MemoryNode) -> bool {
        self.require_verified && node.kind == NodeKind::Fact && !self.allows_unverified(node)
    }

    fn accepts_trust(&self, evidence: &RecallEvidence) -> bool {
        !self.require_verified || evidence.verified || self.allows_unverified(&evidence.node)
    }
}

fn select_vector_profiles<'a>(
    profile: &'a MemoryProfile,
    requested: &[String],
) -> Result<Vec<&'a crate::EmbeddingProfile>> {
    if requested.is_empty() {
        return Ok(profile
            .embedding_profiles
            .iter()
            .filter(|profile| {
                profile
                    .supported_content
                    .contains(&crate::SupportedContent::Text)
            })
            .collect());
    }
    let mut profiles = Vec::new();
    for id in requested {
        let selected = profile
            .embedding_profiles
            .iter()
            .find(|profile| &profile.id == id)
            .ok_or_else(|| Error::InvalidArgument(format!("unknown embedding profile `{id}`")))?;
        if !selected
            .supported_content
            .contains(&crate::SupportedContent::Text)
        {
            return Err(Error::InvalidArgument(format!(
                "embedding profile `{id}` cannot embed text queries"
            )));
        }
        profiles.push(selected);
    }
    Ok(profiles)
}

fn resolve_lawyer<'a>(profile: &'a MemoryProfile, requested: &str) -> Result<&'a LawyerProfile> {
    profile
        .lawyer
        .as_ref()
        .filter(|lawyer| lawyer.id == requested)
        .ok_or_else(|| Error::InvalidArgument(format!("unknown lawyer profile `{requested}`")))
}

fn bounded_evidence_payload(evidence: &[RecallEvidence], limit: usize) -> Vec<Value> {
    let mut values = Vec::new();
    let mut bytes = 2;
    for candidate in evidence.iter().take(limit) {
        let value = evidence_audit_value(candidate);
        let value_bytes = value.to_string().len() + usize::from(!values.is_empty());
        if bytes + value_bytes > MAX_LAWYER_BYTES {
            break;
        }
        bytes += value_bytes;
        values.push(value);
    }
    values
}

fn evidence_audit_value(evidence: &RecallEvidence) -> Value {
    json!({
        "node": node_snapshot(&evidence.node),
        "score": evidence.score,
        "lexical_score": evidence.lexical_score,
        "lexical_rank": evidence.lexical_rank,
        "lexical_terms": evidence.lexical_terms,
        "vector_evidence": evidence.vectors,
        "graph_paths": evidence.graph_paths,
        "provenance": evidence.provenance,
        "conflicts": evidence.conflicts,
        "verified": evidence.verified,
    })
}

fn validate_verdict(verdict: &LawyerVerdict, evidence: &[RecallEvidence]) -> Result<()> {
    let candidates: HashSet<_> = evidence.iter().map(|candidate| candidate.node.id).collect();
    let mut cited = candidates.clone();
    for candidate in evidence {
        cited.extend(candidate.provenance.iter().copied());
        cited.extend(candidate.conflicts.iter().copied());
    }
    let all_candidate_references = verdict
        .accepted_candidate_ids
        .iter()
        .chain(&verdict.rejected_candidate_ids)
        .chain(&verdict.final_order)
        .chain(
            verdict
                .annotations
                .iter()
                .map(|annotation| &annotation.candidate_id),
        );
    if all_candidate_references
        .into_iter()
        .any(|id| !candidates.contains(id))
    {
        return Err(Error::InvalidArgument(
            "lawyer referenced a candidate outside its evidence set".into(),
        ));
    }
    if verdict
        .cited_evidence_ids
        .iter()
        .chain(
            verdict
                .annotations
                .iter()
                .flat_map(|annotation| &annotation.evidence_ids),
        )
        .any(|id| !cited.contains(id))
    {
        return Err(Error::InvalidArgument(
            "lawyer cited evidence outside its evidence set".into(),
        ));
    }
    let conflicts: HashSet<_> = evidence
        .iter()
        .flat_map(|candidate| {
            candidate
                .conflicts
                .iter()
                .map(move |other| ordered_pair(candidate.node.id, *other))
        })
        .collect();
    if verdict
        .unresolved_conflicts
        .iter()
        .any(|pair| !conflicts.contains(&ordered_pair(pair[0], pair[1])))
    {
        return Err(Error::InvalidArgument(
            "lawyer reported a conflict outside its evidence set".into(),
        ));
    }
    let accepted: HashSet<_> = verdict.accepted_candidate_ids.iter().copied().collect();
    let rejected: HashSet<_> = verdict.rejected_candidate_ids.iter().copied().collect();
    if !accepted.is_disjoint(&rejected)
        || verdict.final_order.iter().any(|id| !accepted.contains(id))
        || verdict
            .final_order
            .iter()
            .copied()
            .collect::<HashSet<_>>()
            .len()
            != verdict.final_order.len()
    {
        return Err(Error::InvalidArgument(
            "inconsistent lawyer candidate sets".into(),
        ));
    }
    Ok(())
}

fn allowed_evidence_ids(evidence: &[RecallEvidence]) -> HashSet<Ulid> {
    let mut allowed = HashSet::new();
    for candidate in evidence {
        allowed.insert(candidate.node.id);
        allowed.extend(candidate.provenance.iter().copied());
        allowed.extend(candidate.conflicts.iter().copied());
        for path in &candidate.graph_paths {
            allowed.extend(path.nodes.iter().copied());
            allowed.extend(
                path.edges
                    .iter()
                    .flat_map(|edge| edge.evidence.iter().copied()),
            );
        }
    }
    allowed
}

fn validate_proposal_scope(proposal: &ChangeProposal, allowed: &HashSet<Ulid>) -> Result<()> {
    for change in &proposal.changes {
        let in_scope = match change {
            ProposedChange::SetValidity { node_id, .. }
            | ProposedChange::SetState { node_id, .. } => allowed.contains(node_id),
            ProposedChange::AddEdge { edge, .. } => {
                allowed.contains(&edge.src)
                    && allowed.contains(&edge.dst)
                    && edge.evidence.iter().all(|id| allowed.contains(id))
            }
        };
        if !in_scope {
            return Err(Error::InvalidArgument(
                "lawyer proposal referenced memory outside its evidence set".into(),
            ));
        }
    }
    Ok(())
}

fn ordered_pair(left: Ulid, right: Ulid) -> (Ulid, Ulid) {
    if left <= right {
        (left, right)
    } else {
        (right, left)
    }
}

fn require_revision(database: &Database, id: Ulid, expected: u64) -> Result<()> {
    let node = database.get(id)?.ok_or(Error::NotFound)?;
    if node.revision != expected {
        return Err(Error::InvalidArgument(format!(
            "stale proposal for node {id}: expected revision {expected}, found {}",
            node.revision
        )));
    }
    Ok(())
}

fn prior_node_changes(changes: &[ProposedChange], node_id: Ulid) -> u64 {
    changes
        .iter()
        .filter(|change| match change {
            ProposedChange::SetValidity { node_id: id, .. }
            | ProposedChange::SetState { node_id: id, .. } => *id == node_id,
            ProposedChange::AddEdge { .. } => false,
        })
        .count() as u64
}

fn proposal_resume_conflict(
    proposal_id: Ulid,
    node_id: Ulid,
    before_revision: u64,
    after_revision: u64,
    actual_revision: u64,
) -> Error {
    Error::InvalidArgument(format!(
        "cannot resume proposal {proposal_id}: node {node_id} expected revision {before_revision} before or {after_revision} after the change, found {actual_revision}"
    ))
}

fn normalized_new_edge(mut edge: Edge) -> Edge {
    edge.revision = edge.revision.max(1);
    if edge.valid_from_ms.is_none() {
        edge.valid_from_ms = Some(edge.created_at_ms);
    }
    edge
}

fn same_proposal_edge(current: &Edge, expected: &Edge) -> bool {
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

pub(crate) fn validate_relation(edge: &Edge) -> Result<()> {
    const RELATIONS: [&str; 7] = [
        "contains",
        "derived_from",
        "causes",
        "enables",
        "prevents",
        "contradicts",
        "supersedes",
    ];
    if !RELATIONS.contains(&edge.label.as_str()) {
        return Err(Error::InvalidArgument(format!(
            "unsupported agent relation `{}`",
            edge.label
        )));
    }
    if edge.label == "derived_from" && edge.src == edge.dst {
        return Err(Error::InvalidArgument(
            "derived_from relation cannot reference itself".into(),
        ));
    }
    if !edge.weight.is_finite() || !(0.0..=1.0).contains(&edge.weight) {
        return Err(Error::InvalidArgument("invalid relation weight".into()));
    }
    if edge
        .valid_from_ms
        .zip(edge.valid_to_ms)
        .is_some_and(|(from, to)| from >= to)
    {
        return Err(Error::InvalidArgument(
            "invalid edge validity interval".into(),
        ));
    }
    Ok(())
}

fn is_causal(label: &str) -> bool {
    matches!(label, "causes" | "enables" | "prevents")
}

fn valid_from(node: &MemoryNode) -> i64 {
    node.valid_from_ms.unwrap_or(node.created_at_ms)
}
