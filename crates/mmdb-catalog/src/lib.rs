//! Schema, stats, and named snapshot catalog.

use mmdb_core::{Error, NodeKind, Result};
use std::collections::BTreeMap;
use std::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DistanceMetric {
    Cosine,
    Dot,
    L2,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EmbeddingModel {
    pub name: String,
    pub dim: u32,
    pub distance: DistanceMetric,
}

impl EmbeddingModel {
    pub fn new(name: impl Into<String>, dim: u32, distance: DistanceMetric) -> Self {
        Self {
            name: name.into(),
            dim,
            distance,
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TenantStats {
    pub total_nodes: u64,
    pub nodes_by_kind: BTreeMap<NodeKind, u64>,
    pub edge_count: u64,
    pub blob_count: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NamedSnapshot {
    pub name: String,
    pub seq_no: u64,
    pub created_at_ms: i64,
}

#[derive(Default)]
pub struct Catalog {
    inner: RwLock<CatalogState>,
}

#[derive(Default)]
struct CatalogState {
    models: BTreeMap<String, EmbeddingModel>,
    tenant_stats: BTreeMap<u32, TenantStats>,
    snapshots: BTreeMap<String, NamedSnapshot>,
}

impl Catalog {
    pub fn register_model(&self, model: EmbeddingModel) -> Result<()> {
        let mut state = self.write()?;
        if let Some(existing) = state.models.get(&model.name) {
            if existing != &model {
                return Err(Error::InvalidArgument(format!(
                    "embedding model `{}` already registered with dim {}",
                    model.name, existing.dim
                )));
            }
            return Ok(());
        }
        state.models.insert(model.name.clone(), model);
        Ok(())
    }

    pub fn model(&self, name: &str) -> Result<Option<EmbeddingModel>> {
        Ok(self.read()?.models.get(name).cloned())
    }

    pub fn record_node_insert(&self, tenant: u32, kind: NodeKind) {
        if let Ok(mut state) = self.write() {
            let stats = state.tenant_stats.entry(tenant).or_default();
            stats.total_nodes += 1;
            *stats.nodes_by_kind.entry(kind).or_default() += 1;
        }
    }

    pub fn record_node_delete(&self, tenant: u32, kind: NodeKind) {
        if let Ok(mut state) = self.write() {
            let stats = state.tenant_stats.entry(tenant).or_default();
            stats.total_nodes = stats.total_nodes.saturating_sub(1);
            if let Some(count) = stats.nodes_by_kind.get_mut(&kind) {
                *count = count.saturating_sub(1);
                if *count == 0 {
                    stats.nodes_by_kind.remove(&kind);
                }
            }
        }
    }

    pub fn tenant_stats(&self, tenant: u32) -> TenantStats {
        self.read()
            .ok()
            .and_then(|state| state.tenant_stats.get(&tenant).cloned())
            .unwrap_or_default()
    }

    pub fn create_snapshot(&self, name: impl Into<String>, seq_no: u64) -> Result<NamedSnapshot> {
        let name = name.into();
        let mut state = self.write()?;
        if state.snapshots.contains_key(&name) {
            return Err(Error::InvalidArgument(format!(
                "snapshot `{name}` already exists"
            )));
        }
        let snapshot = NamedSnapshot {
            name: name.clone(),
            seq_no,
            created_at_ms: now_ms(),
        };
        state.snapshots.insert(name, snapshot.clone());
        Ok(snapshot)
    }

    pub fn snapshot(&self, name: &str) -> Result<NamedSnapshot> {
        self.read()?
            .snapshots
            .get(name)
            .cloned()
            .ok_or(Error::NotFound)
    }

    pub fn snapshot_names(&self) -> Vec<String> {
        self.read()
            .map(|state| state.snapshots.keys().cloned().collect())
            .unwrap_or_default()
    }

    fn read(&self) -> Result<RwLockReadGuard<'_, CatalogState>> {
        self.inner
            .read()
            .map_err(|_| Error::Storage("catalog read lock poisoned".into()))
    }

    fn write(&self) -> Result<RwLockWriteGuard<'_, CatalogState>> {
        self.inner
            .write()
            .map_err(|_| Error::Storage("catalog write lock poisoned".into()))
    }
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use mmdb_core::NodeKind;

    #[test]
    fn model_registry_rejects_dimension_changes() {
        let catalog = Catalog::default();
        catalog
            .register_model(EmbeddingModel::new("text", 384, DistanceMetric::Cosine))
            .unwrap();
        assert_eq!(catalog.model("text").unwrap().unwrap().dim, 384);

        let err = catalog
            .register_model(EmbeddingModel::new("text", 768, DistanceMetric::Cosine))
            .unwrap_err();
        assert!(format!("{err}").contains("already registered"));
    }

    #[test]
    fn tenant_stats_track_node_kinds() {
        let catalog = Catalog::default();
        catalog.record_node_insert(0, NodeKind::Fact);
        catalog.record_node_insert(0, NodeKind::Fact);
        catalog.record_node_insert(0, NodeKind::Episode);
        catalog.record_node_delete(0, NodeKind::Fact);

        let stats = catalog.tenant_stats(0);
        assert_eq!(stats.total_nodes, 2);
        assert_eq!(stats.nodes_by_kind.get(&NodeKind::Fact), Some(&1));
        assert_eq!(stats.nodes_by_kind.get(&NodeKind::Episode), Some(&1));
    }

    #[test]
    fn named_snapshots_are_unique_and_listed_by_name() {
        let catalog = Catalog::default();
        catalog.create_snapshot("before-risky-action", 42).unwrap();
        let err = catalog
            .create_snapshot("before-risky-action", 43)
            .unwrap_err();
        assert!(format!("{err}").contains("already exists"));

        let names = catalog.snapshot_names();
        assert_eq!(names, vec!["before-risky-action".to_string()]);
        assert_eq!(catalog.snapshot("before-risky-action").unwrap().seq_no, 42);
    }
}
