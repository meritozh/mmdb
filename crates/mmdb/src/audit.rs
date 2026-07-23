use fjall::{Keyspace, PartitionCreateOptions, PartitionHandle, PersistMode};
use mmdb_core::{Content, Error, MemoryNode, Result};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::BTreeMap;
use ulid::Ulid;

const PART_AUDIT: &str = "audit_journal_v1";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AuditAction {
    Query,
    Mutation,
    ClientCall,
    Compaction,
    Proposal,
    Configuration,
    Failure,
    Repair,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuditContext {
    pub actor: Option<String>,
    pub session_id: Option<String>,
    pub request_id: Option<String>,
    pub metadata: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuditRecord {
    pub id: Ulid,
    pub operation_id: Ulid,
    pub tenant: u32,
    pub at_ms: i64,
    pub action: AuditAction,
    pub name: String,
    pub success: bool,
    pub context: AuditContext,
    pub details: Value,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Default)]
pub struct AuditFilter {
    pub operation_id: Option<Ulid>,
    pub action: Option<AuditAction>,
    pub from_ms: Option<i64>,
    pub to_ms: Option<i64>,
    pub limit: Option<usize>,
}

pub(crate) struct AuditStore {
    keyspace: Keyspace,
    records: PartitionHandle,
}

impl AuditStore {
    pub(crate) fn open(keyspace: Keyspace) -> Result<Self> {
        let records = keyspace
            .open_partition(PART_AUDIT, PartitionCreateOptions::default())
            .map_err(|e| Error::Storage(e.to_string()))?;
        Ok(Self { keyspace, records })
    }

    pub(crate) fn append(&self, record: &AuditRecord) -> Result<()> {
        let key = audit_key(record.tenant, record.id);
        if self
            .records
            .get(&key)
            .map_err(|e| Error::Storage(e.to_string()))?
            .is_some()
        {
            return Err(Error::Storage("audit record id collision".into()));
        }
        let mut persisted = record.clone();
        persisted.details = sanitize_value(&persisted.details);
        persisted.error = persisted.error.as_deref().map(sanitize_error);
        let value = serde_json::to_vec(&persisted)?;
        self.records
            .insert(key, value)
            .map_err(|e| Error::Storage(e.to_string()))?;
        self.keyspace
            .persist(PersistMode::SyncAll)
            .map_err(|e| Error::Storage(e.to_string()))
    }

    pub(crate) fn list(&self, tenant: u32, filter: &AuditFilter) -> Result<Vec<AuditRecord>> {
        let (lo, hi) = tenant_range(tenant);
        let mut records = Vec::new();
        for item in self.records.range(lo..hi) {
            let (_, value) = item.map_err(|e| Error::Storage(e.to_string()))?;
            let record: AuditRecord = serde_json::from_slice(&value)?;
            if filter
                .operation_id
                .is_some_and(|id| record.operation_id != id)
                || filter.action.is_some_and(|action| record.action != action)
                || filter.from_ms.is_some_and(|from| record.at_ms < from)
                || filter.to_ms.is_some_and(|to| record.at_ms > to)
            {
                continue;
            }
            records.push(record);
        }
        records.sort_by_key(|record| record.id);
        if let Some(limit) = filter.limit {
            let drain = records.len().saturating_sub(limit);
            records.drain(..drain);
        }
        Ok(records)
    }
}

pub(crate) fn node_snapshot(node: &MemoryNode) -> Value {
    let content = match &node.content {
        Content::Text(text) => json!({"type": "text", "text": text}),
        Content::Structured(value) => {
            json!({"type": "structured", "value": sanitize_value(value)})
        }
        Content::Blob {
            hash, size, mime, ..
        } => json!({
            "type": "blob",
            "hash": hex(hash),
            "size": size,
            "mime": mime,
        }),
    };
    json!({
        "id": node.id,
        "kind": node.kind,
        "revision": node.revision,
        "state": node.state,
        "created_at_ms": node.created_at_ms,
        "updated_at_ms": node.updated_at_ms,
        "valid_from_ms": node.valid_from_ms,
        "valid_to_ms": node.valid_to_ms,
        "content": content,
        "embeddings": node.embeddings.iter().map(|embedding| json!({
            "model": embedding.model,
            "dim": embedding.dim,
        })).collect::<Vec<_>>(),
        "metadata": sanitize_value(&json!(node.metadata)),
    })
}

pub(crate) fn sanitize_value(value: &Value) -> Value {
    match value {
        Value::Array(values) => Value::Array(values.iter().map(sanitize_value).collect()),
        Value::Object(values) => Value::Object(
            values
                .iter()
                .filter(|(key, _)| !is_sensitive_key(key))
                .map(|(key, value)| (key.clone(), sanitize_value(value)))
                .collect(),
        ),
        other => other.clone(),
    }
}

fn is_sensitive_key(key: &str) -> bool {
    matches!(
        key.to_ascii_lowercase().as_str(),
        "api_key"
            | "apikey"
            | "access_key"
            | "authorization"
            | "blob_bytes"
            | "bytes"
            | "credential"
            | "credentials"
            | "endpoint"
            | "inline"
            | "password"
            | "secret"
            | "token"
            | "vector"
            | "vectors"
    )
}

fn sanitize_error(error: &str) -> String {
    let mut redact_next = false;
    error
        .split_whitespace()
        .map(|word| {
            let lower = word.to_ascii_lowercase();
            if redact_next {
                redact_next = false;
                return "[redacted]";
            }
            if lower == "bearer" {
                redact_next = true;
                return "Bearer";
            }
            if lower.contains("api_key")
                || lower.contains("apikey")
                || lower.contains("authorization")
                || lower.contains("credential")
                || lower.contains("endpoint")
                || lower.contains("password")
                || lower.contains("secret")
                || lower.contains("token")
                || lower.starts_with("http://")
                || lower.starts_with("https://")
            {
                "[redacted]"
            } else {
                word
            }
        })
        .collect::<Vec<_>>()
        .join(" ")
}

fn audit_key(tenant: u32, id: Ulid) -> Vec<u8> {
    let mut key = Vec::with_capacity(20);
    key.extend_from_slice(&tenant.to_be_bytes());
    key.extend_from_slice(&id.0.to_be_bytes());
    key
}

fn tenant_range(tenant: u32) -> (Vec<u8>, Vec<u8>) {
    let lo = tenant.to_be_bytes().to_vec();
    let mut hi = lo.clone();
    hi.extend_from_slice(&[0xff; 16]);
    (lo, hi)
}

fn hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0f) as usize] as char);
    }
    out
}
