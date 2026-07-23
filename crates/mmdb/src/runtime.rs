use crate::audit::{AuditAction, AuditContext};
use crate::lexical::searchable_text;
use crate::{Database, DatabaseConfig, Embedder};
use fjall::{Keyspace, PartitionCreateOptions, PartitionHandle, PersistMode};
use mmdb_core::{Content, Embedding, MemoryNode, NodeKind};
use mmdb_core::{Error, Result};
use serde::{Deserialize, Serialize};
use serde_json::json;
use serde_json::Value;
use smallvec::SmallVec;
use std::collections::BTreeMap;
use std::fmt;
use std::future::Future;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::{Arc, RwLock};
use ulid::Ulid;

const PART_PROFILES: &str = "memory_profiles_v1";
const PART_PROJECTIONS: &str = "projection_status_v1";
const PART_PROPOSALS: &str = "change_proposals_v1";
const PART_DREAM_RUNS: &str = "dream_runs_v1";

pub type ClientFuture<'a, T> = Pin<Box<dyn Future<Output = Result<T>> + Send + 'a>>;

#[derive(Clone)]
pub struct BlobInput {
    pub mime: String,
    pub size: u64,
    opener: Arc<dyn Fn() -> Result<Box<dyn Read + Send>> + Send + Sync>,
}

impl BlobInput {
    pub(crate) fn new(
        mime: String,
        size: u64,
        opener: impl Fn() -> Result<Box<dyn Read + Send>> + Send + Sync + 'static,
    ) -> Self {
        Self {
            mime,
            size,
            opener: Arc::new(opener),
        }
    }

    pub fn open(&self) -> Result<Box<dyn Read + Send>> {
        (self.opener)()
    }
}

impl fmt::Debug for BlobInput {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BlobInput")
            .field("mime", &self.mime)
            .field("size", &self.size)
            .finish_non_exhaustive()
    }
}

#[derive(Debug, Clone)]
pub enum EmbeddingInput {
    Text(String),
    Json(Value),
    Blob(BlobInput),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EmbeddingOutput {
    pub vector: Vec<f32>,
    #[serde(default)]
    pub searchable_text: Option<String>,
}

pub trait EmbeddingClient: Send + Sync {
    fn embed<'a>(
        &'a self,
        input: EmbeddingInput,
        profile: &'a EmbeddingProfile,
    ) -> ClientFuture<'a, EmbeddingOutput>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AgentRole {
    Dreamer,
    Lawyer,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentRequest {
    pub role: AgentRole,
    pub agent_id: String,
    pub model_id: String,
    pub prompt_version: String,
    pub payload: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentResponse {
    pub payload: Value,
}

pub trait AgentClient: Send + Sync {
    fn call<'a>(&'a self, request: AgentRequest) -> ClientFuture<'a, AgentResponse>;
}

#[derive(Clone, Default)]
pub struct ClientRegistry {
    embeddings: Arc<RwLock<BTreeMap<String, Arc<dyn EmbeddingClient>>>>,
    agents: Arc<RwLock<BTreeMap<String, Arc<dyn AgentClient>>>>,
}

impl ClientRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register_embedding(
        &self,
        id: impl Into<String>,
        client: Arc<dyn EmbeddingClient>,
    ) -> Result<()> {
        self.embeddings
            .write()
            .map_err(|_| Error::Storage("embedding client registry lock poisoned".into()))?
            .insert(id.into(), client);
        Ok(())
    }

    pub fn register_agent(
        &self,
        id: impl Into<String>,
        client: Arc<dyn AgentClient>,
    ) -> Result<()> {
        self.agents
            .write()
            .map_err(|_| Error::Storage("agent client registry lock poisoned".into()))?
            .insert(id.into(), client);
        Ok(())
    }

    pub(crate) fn embedding(&self, id: &str) -> Result<Option<Arc<dyn EmbeddingClient>>> {
        Ok(self
            .embeddings
            .read()
            .map_err(|_| Error::Storage("embedding client registry lock poisoned".into()))?
            .get(id)
            .cloned())
    }

    pub(crate) fn agent(&self, id: &str) -> Result<Option<Arc<dyn AgentClient>>> {
        Ok(self
            .agents
            .read()
            .map_err(|_| Error::Storage("agent client registry lock poisoned".into()))?
            .get(id)
            .cloned())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EmbeddingDistance {
    Cosine,
    Dot,
    Euclidean,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SupportedContent {
    Text,
    Json,
    Blob,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EmbeddingProfile {
    pub id: String,
    pub client_id: String,
    pub model: String,
    pub model_revision: String,
    pub dimension: u32,
    pub distance: EmbeddingDistance,
    pub supported_content: Vec<SupportedContent>,
    #[serde(default)]
    pub supported_mime_types: Vec<String>,
    #[serde(default = "default_weight")]
    pub weight: f32,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LawyerFailureMode {
    #[default]
    ReturnDeterministic,
    FailClosed,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LawyerProfile {
    pub id: String,
    pub client_id: String,
    pub agent_id: String,
    pub model_id: String,
    pub prompt_version: String,
    pub rule_set: String,
    #[serde(default = "default_evidence_limit")]
    pub evidence_limit: usize,
    #[serde(default)]
    pub failure_mode: LawyerFailureMode,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DreamProfile {
    pub id: String,
    pub revision: String,
    pub client_id: String,
    pub agent_id: String,
    pub model_id: String,
    pub prompt_version: String,
    #[serde(default)]
    pub response_schema: Value,
    #[serde(default = "default_turn_threshold")]
    pub turn_end_threshold: usize,
    #[serde(default = "default_dream_nodes")]
    pub max_nodes: usize,
    #[serde(default = "default_dream_bytes")]
    pub max_input_bytes: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemoryProfile {
    #[serde(default = "default_profile_version")]
    pub version: u32,
    #[serde(default)]
    pub revision: u64,
    #[serde(default)]
    pub embedding_profiles: Vec<EmbeddingProfile>,
    #[serde(default)]
    pub dreamer: Option<DreamProfile>,
    #[serde(default)]
    pub lawyer: Option<LawyerProfile>,
}

impl Default for MemoryProfile {
    fn default() -> Self {
        Self {
            version: default_profile_version(),
            revision: 1,
            embedding_profiles: Vec::new(),
            dreamer: None,
            lawyer: None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProjectionState {
    Pending,
    Ready,
    Failed,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProjectionStatus {
    pub node_id: Ulid,
    pub node_revision: u64,
    pub profile_id: String,
    pub state: ProjectionState,
    pub attempts: u32,
    pub updated_at_ms: i64,
    pub last_error: Option<String>,
    pub searchable_text: Option<String>,
}

#[derive(Debug, Clone)]
pub struct IngestReport {
    pub node_id: Ulid,
    pub projections: Vec<ProjectionStatus>,
}

pub struct DatabaseBuilder {
    path: PathBuf,
    config: DatabaseConfig,
    clients: ClientRegistry,
    profile: Option<MemoryProfile>,
    legacy_embedder: Option<Box<dyn Embedder>>,
}

impl DatabaseBuilder {
    pub(crate) fn new(path: impl AsRef<Path>) -> Self {
        Self {
            path: path.as_ref().to_path_buf(),
            config: DatabaseConfig::default(),
            clients: ClientRegistry::new(),
            profile: None,
            legacy_embedder: None,
        }
    }

    pub fn config(mut self, config: DatabaseConfig) -> Self {
        self.config = config;
        self
    }

    pub fn clients(mut self, clients: ClientRegistry) -> Self {
        self.clients = clients;
        self
    }

    pub fn profile(mut self, profile: MemoryProfile) -> Self {
        self.profile = Some(profile);
        self
    }

    pub fn legacy_embedder(mut self, embedder: Box<dyn Embedder>) -> Self {
        self.legacy_embedder = Some(embedder);
        self
    }

    pub fn build(self) -> Result<Database> {
        Database::open_runtime(
            &self.path,
            self.config,
            self.clients,
            self.profile,
            self.legacy_embedder,
        )
    }
}

impl Database {
    /// Persist the raw memory first, then build every compatible configured
    /// projection. Per-profile failures are returned as retryable status and do
    /// not roll back the source memory.
    pub async fn ingest(&self, node: MemoryNode) -> Result<IngestReport> {
        let node_id = self.insert_inner(node, false, false)?;
        let statuses = self.project_configured(node_id).await?;
        Ok(IngestReport {
            node_id,
            projections: statuses,
        })
    }

    /// Persist a blob in the content-addressed store, then pass reopenable
    /// readers to every compatible embedding profile.
    pub async fn ingest_blob(
        &self,
        kind: NodeKind,
        reader: impl Read,
        mime: impl Into<String>,
    ) -> Result<IngestReport> {
        let node_id = self.insert_blob(kind, reader, mime)?;
        let projections = self.project_configured(node_id).await?;
        Ok(IngestReport {
            node_id,
            projections,
        })
    }

    pub async fn retry_projection(
        &self,
        node_id: Ulid,
        profile_id: &str,
    ) -> Result<ProjectionStatus> {
        let node = self.get(node_id)?.ok_or(Error::NotFound)?;
        let profile = self
            .memory_profile()?
            .embedding_profiles
            .into_iter()
            .find(|profile| profile.id == profile_id)
            .ok_or_else(|| {
                Error::InvalidArgument(format!("unknown embedding profile `{profile_id}`"))
            })?;
        if !supports(&profile, &node.content) {
            return Err(Error::InvalidArgument(format!(
                "profile `{profile_id}` does not support this content"
            )));
        }
        self.project_node(node, &profile).await
    }

    pub fn projection_statuses(&self, node_id: Ulid) -> Result<Vec<ProjectionStatus>> {
        self.runtime_store
            .projection_statuses(self.config.tenant, node_id)
    }

    pub fn set_memory_profile(
        &self,
        mut profile: MemoryProfile,
        context: AuditContext,
    ) -> Result<()> {
        if let Err(error) = validate_profile(&profile) {
            self.append_audit(
                Ulid::new(),
                AuditAction::Configuration,
                "memory_profile",
                false,
                context,
                json!({"attempted": profile}),
                Some(error.to_string()),
            )?;
            return Err(error);
        }
        let previous = self.memory_profile()?;
        profile.revision = previous
            .revision
            .max(profile.revision)
            .saturating_add(1)
            .max(1);
        self.runtime_store
            .put_profile(self.config.tenant, &profile)?;
        *self
            .profile
            .write()
            .map_err(|_| Error::Storage("memory profile lock poisoned".into()))? = profile.clone();
        for node in self
            .storage
            .scan_by_time(self.config.tenant, 0, i64::MAX, usize::MAX)?
        {
            self.reindex_node(&node)?;
        }
        self.append_audit(
            Ulid::new(),
            AuditAction::Configuration,
            "memory_profile",
            true,
            context,
            json!({"before": previous, "after": profile}),
            None,
        )
    }

    pub(crate) async fn project_node(
        &self,
        node: MemoryNode,
        profile: &EmbeddingProfile,
    ) -> Result<ProjectionStatus> {
        let previous = self
            .runtime_store
            .projection(self.config.tenant, node.id, &profile.id)?;
        let mut status = ProjectionStatus {
            node_id: node.id,
            node_revision: node.revision,
            profile_id: profile.id.clone(),
            state: ProjectionState::Pending,
            attempts: previous.as_ref().map_or(1, |old| old.attempts + 1),
            updated_at_ms: crate::now_ms(),
            last_error: None,
            searchable_text: None,
        };
        self.runtime_store
            .put_projection(self.config.tenant, &status)?;
        self.reindex_node(&node)?;
        let operation_id = Ulid::new();
        let Some(client) = self.clients.embedding(&profile.client_id)? else {
            status.state = ProjectionState::Failed;
            status.last_error = Some(format!(
                "embedding client `{}` is not registered",
                profile.client_id
            ));
            status.updated_at_ms = crate::now_ms();
            self.runtime_store
                .put_projection(self.config.tenant, &status)?;
            self.append_audit(
                operation_id,
                AuditAction::ClientCall,
                "embedding",
                false,
                AuditContext::default(),
                json!({"node_id": node.id, "profile": profile, "input": input_summary(&node.content)}),
                status.last_error.clone(),
            )?;
            return Ok(status);
        };
        let input = self.embedding_input(&node.content)?;
        let response = client.embed(input, profile).await;
        match response {
            Ok(output)
                if output.vector.len() == profile.dimension as usize
                    && output.vector.iter().all(|value| value.is_finite()) =>
            {
                let mut current = self.get(node.id)?.ok_or(Error::NotFound)?;
                current
                    .embeddings
                    .retain(|embedding| embedding.model != profile.model);
                current.embeddings.push(Embedding {
                    model: profile.model.clone(),
                    dim: profile.dimension,
                    vector: SmallVec::from_vec(output.vector),
                });
                self.insert_inner(current, false, false)?;
                let current = self.get(node.id)?.ok_or(Error::NotFound)?;
                status.node_revision = current.revision;
                status.state = ProjectionState::Ready;
                status.updated_at_ms = crate::now_ms();
                status.searchable_text = output.searchable_text;
                self.runtime_store
                    .put_projection(self.config.tenant, &status)?;
                self.reindex_node(&current)?;
                self.append_audit(
                    operation_id,
                    AuditAction::ClientCall,
                    "embedding",
                    true,
                    AuditContext::default(),
                    json!({
                        "node_id": node.id,
                        "profile": profile,
                        "input": input_summary(&node.content),
                        "response": {
                            "dimension": profile.dimension,
                            "searchable_text_bytes": status.searchable_text.as_ref().map_or(0, |text| text.len()),
                        },
                    }),
                    None,
                )?;
            }
            Ok(output) => {
                status.state = ProjectionState::Failed;
                status.updated_at_ms = crate::now_ms();
                status.last_error = Some(format!(
                    "invalid embedding output: expected {} finite values, got {}",
                    profile.dimension,
                    output.vector.len()
                ));
                self.runtime_store
                    .put_projection(self.config.tenant, &status)?;
                self.append_audit(
                    operation_id,
                    AuditAction::ClientCall,
                    "embedding",
                    false,
                    AuditContext::default(),
                    json!({"node_id": node.id, "profile": profile, "input": input_summary(&node.content)}),
                    status.last_error.clone(),
                )?;
            }
            Err(error) => {
                status.state = ProjectionState::Failed;
                status.updated_at_ms = crate::now_ms();
                status.last_error = Some(error.to_string());
                self.runtime_store
                    .put_projection(self.config.tenant, &status)?;
                self.append_audit(
                    operation_id,
                    AuditAction::ClientCall,
                    "embedding",
                    false,
                    AuditContext::default(),
                    json!({"node_id": node.id, "profile": profile, "input": input_summary(&node.content)}),
                    status.last_error.clone(),
                )?;
            }
        }
        Ok(status)
    }

    fn embedding_input(&self, content: &Content) -> Result<EmbeddingInput> {
        match content {
            Content::Text(text) => Ok(EmbeddingInput::Text(text.clone())),
            Content::Structured(value) => Ok(EmbeddingInput::Json(value.clone())),
            Content::Blob {
                hash, size, mime, ..
            } => {
                let blob_store = self.blob_store.clone();
                let hash = *hash;
                Ok(EmbeddingInput::Blob(BlobInput::new(
                    mime.clone(),
                    *size,
                    move || blob_store.get_stream(&hash),
                )))
            }
        }
    }

    fn reindex_node(&self, node: &MemoryNode) -> Result<()> {
        let active_profiles: Vec<_> = self
            .memory_profile()?
            .embedding_profiles
            .into_iter()
            .map(|profile| profile.id)
            .collect();
        let projections = self
            .runtime_store
            .projection_statuses(self.config.tenant, node.id)?
            .into_iter()
            .filter(|status| active_profiles.contains(&status.profile_id))
            .filter(|status| status.state == ProjectionState::Ready)
            .filter_map(|status| status.searchable_text)
            .collect::<Vec<_>>();
        self.lexical_index.upsert(
            self.config.tenant,
            node.id,
            &searchable_text(node, &projections),
        )
    }

    pub(crate) async fn project_configured(&self, node_id: Ulid) -> Result<Vec<ProjectionStatus>> {
        let profiles = self.memory_profile()?.embedding_profiles;
        let node = self.get(node_id)?.ok_or(Error::NotFound)?;
        for profile in profiles
            .iter()
            .filter(|profile| supports(profile, &node.content))
        {
            let previous =
                self.runtime_store
                    .projection(self.config.tenant, node_id, &profile.id)?;
            self.runtime_store.put_projection(
                self.config.tenant,
                &ProjectionStatus {
                    node_id,
                    node_revision: node.revision,
                    profile_id: profile.id.clone(),
                    state: ProjectionState::Pending,
                    attempts: previous.map_or(0, |status| status.attempts),
                    updated_at_ms: crate::now_ms(),
                    last_error: None,
                    searchable_text: None,
                },
            )?;
        }
        self.reindex_node(&node)?;
        let mut statuses = Vec::new();
        for profile in profiles {
            let node = self.get(node_id)?.ok_or(Error::NotFound)?;
            if supports(&profile, &node.content) {
                statuses.push(self.project_node(node, &profile).await?);
            }
        }
        Ok(statuses)
    }
}

pub(crate) struct RuntimeStore {
    keyspace: Keyspace,
    profiles: PartitionHandle,
    projections: PartitionHandle,
    proposals: PartitionHandle,
    dream_runs: PartitionHandle,
}

impl RuntimeStore {
    pub(crate) fn open(keyspace: Keyspace) -> Result<Self> {
        Ok(Self {
            profiles: open_partition(&keyspace, PART_PROFILES)?,
            projections: open_partition(&keyspace, PART_PROJECTIONS)?,
            proposals: open_partition(&keyspace, PART_PROPOSALS)?,
            dream_runs: open_partition(&keyspace, PART_DREAM_RUNS)?,
            keyspace,
        })
    }

    pub(crate) fn load_profile(&self, tenant: u32) -> Result<Option<MemoryProfile>> {
        self.get_json(&self.profiles, &tenant.to_be_bytes())
    }

    pub(crate) fn put_profile(&self, tenant: u32, profile: &MemoryProfile) -> Result<()> {
        self.put_json(&self.profiles, &tenant.to_be_bytes(), profile)
    }

    pub(crate) fn projection(
        &self,
        tenant: u32,
        node: Ulid,
        profile: &str,
    ) -> Result<Option<ProjectionStatus>> {
        self.get_json(&self.projections, &projection_key(tenant, node, profile))
    }

    pub(crate) fn put_projection(&self, tenant: u32, status: &ProjectionStatus) -> Result<()> {
        self.put_json(
            &self.projections,
            &projection_key(tenant, status.node_id, &status.profile_id),
            status,
        )
    }

    pub(crate) fn projection_statuses(
        &self,
        tenant: u32,
        node: Ulid,
    ) -> Result<Vec<ProjectionStatus>> {
        let mut prefix = Vec::with_capacity(20);
        prefix.extend_from_slice(&tenant.to_be_bytes());
        prefix.extend_from_slice(&node.0.to_be_bytes());
        self.scan_prefix(&self.projections, &prefix)
    }

    pub(crate) fn put_proposal<T: Serialize>(
        &self,
        tenant: u32,
        id: Ulid,
        value: &T,
    ) -> Result<()> {
        self.put_json(&self.proposals, &tenant_ulid_key(tenant, id), value)
    }

    pub(crate) fn proposal<T: for<'de> Deserialize<'de>>(
        &self,
        tenant: u32,
        id: Ulid,
    ) -> Result<Option<T>> {
        self.get_json(&self.proposals, &tenant_ulid_key(tenant, id))
    }

    pub(crate) fn proposals<T: for<'de> Deserialize<'de>>(&self, tenant: u32) -> Result<Vec<T>> {
        self.scan_prefix(&self.proposals, &tenant.to_be_bytes())
    }

    pub(crate) fn put_dream_run<T: Serialize>(
        &self,
        tenant: u32,
        id: Ulid,
        value: &T,
    ) -> Result<()> {
        self.put_json(&self.dream_runs, &tenant_ulid_key(tenant, id), value)
    }

    pub(crate) fn dream_run<T: for<'de> Deserialize<'de>>(
        &self,
        tenant: u32,
        id: Ulid,
    ) -> Result<Option<T>> {
        self.get_json(&self.dream_runs, &tenant_ulid_key(tenant, id))
    }

    pub(crate) fn dream_runs<T: for<'de> Deserialize<'de>>(&self, tenant: u32) -> Result<Vec<T>> {
        self.scan_prefix(&self.dream_runs, &tenant.to_be_bytes())
    }

    fn get_json<T: for<'de> Deserialize<'de>>(
        &self,
        partition: &PartitionHandle,
        key: &[u8],
    ) -> Result<Option<T>> {
        match partition
            .get(key)
            .map_err(|e| Error::Storage(e.to_string()))?
        {
            Some(value) => Ok(Some(serde_json::from_slice(&value)?)),
            None => Ok(None),
        }
    }

    fn put_json<T: Serialize>(
        &self,
        partition: &PartitionHandle,
        key: &[u8],
        value: &T,
    ) -> Result<()> {
        partition
            .insert(key, serde_json::to_vec(value)?)
            .map_err(|e| Error::Storage(e.to_string()))?;
        self.keyspace
            .persist(PersistMode::SyncAll)
            .map_err(|e| Error::Storage(e.to_string()))
    }

    fn scan_prefix<T: for<'de> Deserialize<'de>>(
        &self,
        partition: &PartitionHandle,
        prefix: &[u8],
    ) -> Result<Vec<T>> {
        let mut hi = prefix.to_vec();
        hi.extend_from_slice(&[0xff; 512]);
        let mut values = Vec::new();
        for item in partition.range(prefix.to_vec()..hi) {
            let (_, value) = item.map_err(|e| Error::Storage(e.to_string()))?;
            values.push(serde_json::from_slice(&value)?);
        }
        Ok(values)
    }
}

fn open_partition(keyspace: &Keyspace, name: &str) -> Result<PartitionHandle> {
    keyspace
        .open_partition(name, PartitionCreateOptions::default())
        .map_err(|e| Error::Storage(e.to_string()))
}

fn projection_key(tenant: u32, node: Ulid, profile: &str) -> Vec<u8> {
    let mut key = Vec::with_capacity(20 + profile.len());
    key.extend_from_slice(&tenant.to_be_bytes());
    key.extend_from_slice(&node.0.to_be_bytes());
    key.extend_from_slice(profile.as_bytes());
    key
}

fn tenant_ulid_key(tenant: u32, id: Ulid) -> Vec<u8> {
    let mut key = Vec::with_capacity(20);
    key.extend_from_slice(&tenant.to_be_bytes());
    key.extend_from_slice(&id.0.to_be_bytes());
    key
}

fn default_weight() -> f32 {
    1.0
}

fn default_profile_version() -> u32 {
    1
}

fn default_evidence_limit() -> usize {
    50
}

fn default_turn_threshold() -> usize {
    32
}

fn default_dream_nodes() -> usize {
    128
}

fn default_dream_bytes() -> usize {
    256 * 1024
}

fn supports(profile: &EmbeddingProfile, content: &Content) -> bool {
    match content {
        Content::Text(_) => profile.supported_content.contains(&SupportedContent::Text),
        Content::Structured(_) => profile.supported_content.contains(&SupportedContent::Json),
        Content::Blob { mime, .. } => {
            profile.supported_content.contains(&SupportedContent::Blob)
                && (profile.supported_mime_types.is_empty()
                    || profile
                        .supported_mime_types
                        .iter()
                        .any(|supported| supported == mime))
        }
    }
}

fn input_summary(content: &Content) -> Value {
    match content {
        Content::Text(text) => json!({"type": "text", "bytes": text.len()}),
        Content::Structured(value) => {
            json!({"type": "json", "bytes": value.to_string().len()})
        }
        Content::Blob { size, mime, .. } => json!({"type": "blob", "size": size, "mime": mime}),
    }
}

pub(crate) fn validate_profile(profile: &MemoryProfile) -> Result<()> {
    if profile.version != 1 {
        return Err(Error::InvalidArgument(format!(
            "unsupported memory profile version {}",
            profile.version
        )));
    }
    let mut ids = BTreeMap::new();
    let mut models = BTreeMap::new();
    for embedding in &profile.embedding_profiles {
        if embedding.id.is_empty()
            || embedding.client_id.is_empty()
            || embedding.model.is_empty()
            || embedding.dimension == 0
            || !embedding.weight.is_finite()
            || embedding.weight < 0.0
            || embedding.distance != EmbeddingDistance::Cosine
        {
            return Err(Error::InvalidArgument(format!(
                "invalid embedding profile `{}`",
                embedding.id
            )));
        }
        if ids.insert(&embedding.id, ()).is_some() {
            return Err(Error::InvalidArgument(format!(
                "duplicate embedding profile id `{}`",
                embedding.id
            )));
        }
        if models
            .insert(&embedding.model, embedding.dimension)
            .is_some()
        {
            return Err(Error::InvalidArgument(format!(
                "duplicate embedding model `{}`",
                embedding.model
            )));
        }
    }
    if let Some(dreamer) = &profile.dreamer {
        if dreamer.id.is_empty()
            || dreamer.revision.is_empty()
            || dreamer.client_id.is_empty()
            || dreamer.agent_id.is_empty()
            || dreamer.model_id.is_empty()
            || dreamer.prompt_version.is_empty()
            || dreamer.turn_end_threshold == 0
            || dreamer.turn_end_threshold > 128
            || dreamer.max_nodes == 0
            || dreamer.max_input_bytes < 4096
        {
            return Err(Error::InvalidArgument("invalid dreamer profile".into()));
        }
    }
    if let Some(lawyer) = &profile.lawyer {
        if lawyer.id.is_empty()
            || lawyer.client_id.is_empty()
            || lawyer.agent_id.is_empty()
            || lawyer.model_id.is_empty()
            || lawyer.prompt_version.is_empty()
            || lawyer.rule_set.is_empty()
            || lawyer.evidence_limit == 0
        {
            return Err(Error::InvalidArgument("invalid lawyer profile".into()));
        }
    }
    Ok(())
}
