use crate::convert::{node_to_query_record, query_predicate_matches, resolve_query_vector};
use crate::embedder::Embedder;
use crate::Database;
use mmdb_catalog::Catalog;
use mmdb_core::Result;
use mmdb_graph::{Direction, GraphStore};
use mmdb_storage::Storage;
use mmdb_vector::VectorStore;
use std::collections::{BTreeMap, HashSet};
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};
use ulid::Ulid;

pub(crate) const QUERY_BATCH_SIZE: usize = 1024;

pub(crate) type QueryUdfFn = mmdb_query::UdfFn;

#[derive(Clone)]
pub(crate) struct QuerySourceHandle {
    pub(crate) storage: Arc<Storage>,
    pub(crate) vector_store: Arc<VectorStore>,
    pub(crate) graph_store: Arc<GraphStore>,
    pub(crate) embedder: Option<Arc<dyn Embedder>>,
    pub(crate) config: crate::embedder::DatabaseConfig,
}

pub(crate) struct AsyncQueryRequest {
    pub(crate) source: QuerySourceHandle,
    pub(crate) udfs: Result<BTreeMap<String, Arc<QueryUdfFn>>>,
    pub(crate) plan: mmdb_query::LogicalPlan,
}

pub(crate) struct AsyncQueryFuture {
    request: Option<AsyncQueryRequest>,
    state: Arc<Mutex<AsyncQueryState>>,
}

pub(crate) struct AsyncQueryState {
    pub(crate) result: Option<Result<Vec<mmdb_query::Record>>>,
    pub(crate) waker: Option<Waker>,
}

impl AsyncQueryFuture {
    pub(crate) fn new(request: AsyncQueryRequest) -> Self {
        Self {
            request: Some(request),
            state: Arc::new(Mutex::new(AsyncQueryState {
                result: None,
                waker: None,
            })),
        }
    }
}

impl Future for AsyncQueryFuture {
    type Output = Result<Vec<mmdb_query::Record>>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        {
            let mut state = self.state.lock().expect("async query state mutex poisoned");
            if let Some(result) = state.result.take() {
                return Poll::Ready(result);
            }
            state.waker = Some(cx.waker().clone());
        }

        if let Some(request) = self.request.take() {
            let state = self.state.clone();
            std::thread::spawn(move || {
                let result = execute_async_query_request(request);
                let waker = {
                    let mut state = state.lock().expect("async query state mutex poisoned");
                    state.result = Some(result);
                    state.waker.take()
                };
                if let Some(waker) = waker {
                    waker.wake();
                }
            });
        }

        Poll::Pending
    }
}

fn execute_async_query_request(request: AsyncQueryRequest) -> Result<Vec<mmdb_query::Record>> {
    let udfs = request.udfs?;
    let mut executor = mmdb_query::SourceExecutor::new(&request.source);
    for (name, udf) in udfs {
        executor = executor.with_udf(name, udf);
    }
    let mut op = executor.compile(&request.plan, QUERY_BATCH_SIZE)?;
    collect_query_operator(&mut *op)
}

impl mmdb_query::QuerySource for QuerySourceHandle {
    fn range_scan(
        &self,
        table: &mmdb_query::TableId,
        filter: Option<&mmdb_query::Predicate>,
    ) -> Result<Vec<mmdb_query::Record>> {
        if table != &mmdb_query::TableId::Nodes {
            return Err(mmdb_core::Error::InvalidArgument(format!(
                "database query source does not support scanning {table:?}"
            )));
        }
        Ok(self
            .storage
            .scan_by_time(self.config.tenant, 0, i64::MAX, usize::MAX)?
            .into_iter()
            .map(node_to_query_record)
            .filter(|record| {
                filter
                    .map(|pred| query_predicate_matches(record, pred))
                    .unwrap_or(true)
            })
            .collect())
    }

    fn hnsw_search(
        &self,
        query: &mmdb_query::VectorRef,
        model: &mmdb_query::ModelId,
        k: usize,
        filter: Option<&mmdb_query::Predicate>,
    ) -> Result<Vec<mmdb_query::Record>> {
        let vector = resolve_query_vector(query, model, self.embedder.as_deref())?;
        let hits = self
            .vector_store
            .search(self.config.tenant, &model.0, &vector, k)?;
        let mut rows = Vec::with_capacity(hits.len());
        for hit in hits {
            let Some(node) = self.storage.get_node(self.config.tenant, hit.node_id)? else {
                continue;
            };
            let record = node_to_query_record(node).with_score(hit.score);
            if filter
                .map(|pred| query_predicate_matches(&record, pred))
                .unwrap_or(true)
            {
                rows.push(record);
            }
        }
        Ok(rows)
    }

    fn graph_expand(
        &self,
        seeds: Vec<mmdb_query::Record>,
        relation: Option<&str>,
        depth: u8,
    ) -> Result<Vec<mmdb_query::Record>> {
        graph_expand_query_rows_from(
            &self.storage,
            &self.graph_store,
            self.config.tenant,
            seeds,
            relation,
            depth,
        )
    }
}

impl mmdb_query::QuerySource for Database {
    fn range_scan(
        &self,
        table: &mmdb_query::TableId,
        filter: Option<&mmdb_query::Predicate>,
    ) -> Result<Vec<mmdb_query::Record>> {
        if table != &mmdb_query::TableId::Nodes {
            return Err(mmdb_core::Error::InvalidArgument(format!(
                "database query source does not support scanning {table:?}"
            )));
        }
        Ok(self
            .storage
            .scan_by_time(self.config.tenant, 0, i64::MAX, usize::MAX)?
            .into_iter()
            .map(node_to_query_record)
            .filter(|record| {
                filter
                    .map(|pred| query_predicate_matches(record, pred))
                    .unwrap_or(true)
            })
            .collect())
    }

    fn hnsw_search(
        &self,
        query: &mmdb_query::VectorRef,
        model: &mmdb_query::ModelId,
        k: usize,
        filter: Option<&mmdb_query::Predicate>,
    ) -> Result<Vec<mmdb_query::Record>> {
        let vector = resolve_query_vector(query, model, self.embedder.as_deref())?;
        let hits = self
            .vector_store
            .search(self.config.tenant, &model.0, &vector, k)?;
        let mut rows = Vec::with_capacity(hits.len());
        for hit in hits {
            let Some(node) = self.storage.get_node(self.config.tenant, hit.node_id)? else {
                continue;
            };
            let record = node_to_query_record(node).with_score(hit.score);
            if filter
                .map(|pred| query_predicate_matches(&record, pred))
                .unwrap_or(true)
            {
                rows.push(record);
            }
        }
        Ok(rows)
    }

    fn graph_expand(
        &self,
        seeds: Vec<mmdb_query::Record>,
        relation: Option<&str>,
        depth: u8,
    ) -> Result<Vec<mmdb_query::Record>> {
        self.graph_expand_query_rows(seeds, relation, depth)
    }
}

pub(crate) fn graph_expand_query_rows_from(
    storage: &Storage,
    graph_store: &GraphStore,
    tenant: u32,
    seeds: Vec<mmdb_query::Record>,
    relation: Option<&str>,
    depth: u8,
) -> Result<Vec<mmdb_query::Record>> {
    if depth == 0 {
        return Ok(seeds);
    }

    let mut out = Vec::new();
    let mut emitted = HashSet::new();
    for seed in seeds {
        if emitted.insert(seed.node_id.clone()) {
            out.push(seed.clone());
        }
        let Ok(seed_id) = seed.node_id.parse::<Ulid>() else {
            continue;
        };
        for id in graph_store.bfs(tenant, seed_id, depth as usize, Direction::Out, relation)? {
            let id_string = id.to_string();
            if !emitted.insert(id_string) {
                continue;
            }
            if let Some(node) = storage.get_node(tenant, id)? {
                out.push(node_to_query_record(node));
            }
        }
    }
    Ok(out)
}

pub(crate) fn collect_query_operator(
    op: &mut dyn mmdb_query::PhysicalOperator,
) -> Result<Vec<mmdb_query::Record>> {
    let mut rows = Vec::new();
    while let Some(batch) = op.next_batch()? {
        rows.extend(batch.rows);
    }
    Ok(rows)
}

pub(crate) fn query_stats_from_catalog(
    stats: mmdb_catalog::TenantStats,
) -> mmdb_query::Stats {
    let mut histograms = BTreeMap::new();
    histograms.insert(
        mmdb_query::FieldRef::Kind,
        mmdb_query::FieldHistogram::from_counts(
            stats
                .nodes_by_kind
                .into_iter()
                .map(|(kind, count)| (mmdb_query::Literal::NodeKind(kind), count)),
        ),
    );
    mmdb_query::Stats {
        node_rows: stats.total_nodes.min(usize::MAX as u64) as usize,
        estimated_filter_selectivity: 1.0,
        histograms,
    }
}

pub(crate) fn rebuild_catalog(storage: &Storage, tenant: u32) -> Result<Catalog> {
    let catalog = Catalog::default();
    for node in storage.scan_by_time(tenant, 0, i64::MAX, usize::MAX)? {
        catalog.record_node_insert(tenant, node.kind);
    }
    Ok(catalog)
}
