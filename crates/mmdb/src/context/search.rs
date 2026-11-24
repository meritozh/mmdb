use super::storage::*;
use super::*;
use crate::{RecordState, Scope};
use std::collections::{BTreeMap, BTreeSet, VecDeque};

fn is_cjk(c: char) -> bool {
    matches!(c as u32, 0x3400..=0x4dbf | 0x4e00..=0x9fff | 0x20000..=0x2fa1f)
}

fn terms(text: &str) -> BTreeSet<String> {
    let mut terms = BTreeSet::new();
    let mut word = String::new();
    let mut previous_cjk = None;
    for c in text.chars().flat_map(char::to_lowercase) {
        if is_cjk(c) {
            if !word.is_empty() {
                terms.insert(std::mem::take(&mut word));
            }
            terms.insert(c.to_string());
            if let Some(previous) = previous_cjk {
                terms.insert(format!("{previous}{c}"));
            }
            previous_cjk = Some(c);
        } else {
            previous_cjk = None;
            if c.is_alphanumeric() {
                word.push(c);
            } else if !word.is_empty() {
                terms.insert(std::mem::take(&mut word));
            }
        }
    }
    if !word.is_empty() {
        terms.insert(word);
    }
    terms
}

fn posting_prefix(owner: &str, scope: &Scope, term: [u8; 32]) -> ContextResult<Vec<u8>> {
    let mut key = owner_key(owner);
    segment(&mut key, &encode(scope)?);
    key.extend_from_slice(&term);
    Ok(key)
}

fn posting_key(
    owner: &str,
    scope: &Scope,
    term: [u8; 32],
    reference: &ContextRef,
) -> ContextResult<Vec<u8>> {
    let mut key = posting_prefix(owner, scope, term)?;
    key.push(kind_byte(reference.kind));
    key.extend_from_slice(&reference.id.0.to_be_bytes());
    Ok(key)
}

fn adjacency_key(endpoint: &ContextRef, relation: &ContextRef) -> Vec<u8> {
    let mut key = record_key(endpoint);
    key.extend_from_slice(&relation.id.0.to_be_bytes());
    key
}

impl ContextStore<'_> {
    pub(crate) fn replace_postings(
        &self,
        batch: &mut mmdb_kernel::Batch,
        header: &RecordHeader,
        properties: Option<&Properties>,
    ) -> ContextResult<()> {
        let key = record_key(&header.pin.record);
        if let Some(previous) = self.parts.record_terms.get(&key)? {
            for term in decode::<Vec<[u8; 32]>>(&previous)? {
                batch.remove(
                    &self.parts.postings,
                    posting_key(
                        &self.scoped.access().owner,
                        &header.scope,
                        term,
                        &header.pin.record,
                    )?,
                );
            }
        }
        if let Some(properties) = properties {
            let text = serde_json::to_string(properties)?;
            let hashes: BTreeSet<_> = terms(&text)
                .into_iter()
                .map(|term| self.scoped.content_digest(term.as_bytes()))
                .collect();
            for hash in &hashes {
                batch.insert(
                    &self.parts.postings,
                    posting_key(
                        &self.scoped.access().owner,
                        &header.scope,
                        *hash,
                        &header.pin.record,
                    )?,
                    encode(&header.pin)?,
                );
            }
            batch.insert(&self.parts.record_terms, key, encode(&hashes)?);
        } else {
            batch.remove(&self.parts.record_terms, key);
        }
        Ok(())
    }

    pub(crate) fn replace_adjacency(
        &self,
        batch: &mut mmdb_kernel::Batch,
        previous: Option<&RecordHeader>,
        header: &RecordHeader,
    ) -> ContextResult<()> {
        if let Some(previous) = previous {
            if let Some(endpoints) = &previous.endpoints {
                for endpoint in [&endpoints.from, &endpoints.to] {
                    batch.remove(
                        &self.parts.adjacency,
                        adjacency_key(endpoint, &previous.pin.record),
                    );
                }
            }
        }
        if header.state == RecordState::Active {
            if let Some(endpoints) = &header.endpoints {
                for endpoint in [&endpoints.from, &endpoints.to] {
                    batch.insert(
                        &self.parts.adjacency,
                        adjacency_key(endpoint, &header.pin.record),
                        encode(&header.pin)?,
                    );
                }
            }
        }
        Ok(())
    }

    fn current_candidate(
        &self,
        pin: &RecordPin,
        at: Option<i64>,
        memo: &mut crate::records::Memo,
    ) -> ContextResult<Option<RecordHeader>> {
        let header = match self.scoped.header_locked(pin) {
            Ok(header) => header,
            Err(ContextError::AccessDenied | ContextError::NotFound(_)) => return Ok(None),
            Err(e) => return Err(e),
        };
        if at.is_some_and(|at| !header.temporal.is_valid_at(at))
            || !self.scoped.available_memoized(pin, true, memo)?
        {
            return Ok(None);
        }
        if let Some(endpoints) = &header.endpoints {
            for endpoint in [&endpoints.from, &endpoints.to] {
                let endpoint = match self.scoped.head_locked(endpoint) {
                    Ok(header) => header,
                    Err(ContextError::AccessDenied | ContextError::NotFound(_)) => return Ok(None),
                    Err(error) => return Err(error),
                };
                if at.is_some_and(|at| !endpoint.temporal.is_valid_at(at))
                    || !self.scoped.available_memoized(&endpoint.pin, true, memo)?
                {
                    return Ok(None);
                }
            }
        }
        Ok(Some(header))
    }

    /// Lists current relations touching an endpoint, with direction, type and
    /// time filters. The cursor also advances across inaccessible or stale edges.
    pub fn relations(&self, query: RelationQuery) -> ContextResult<RelationPage> {
        if query.max_edges == 0 || query.max_edges > 4096 || query.limit == 0 || query.limit > 100 {
            return Err(ContextError::InvalidInput(
                "invalid bounded relation query".into(),
            ));
        }
        let mut memo = BTreeMap::new();
        let _guard = self.scoped.lock();
        let endpoint = self.scoped.head_locked(&query.endpoint)?;
        if self
            .current_candidate(&endpoint.pin, query.valid_at_ms, &mut memo)?
            .is_none()
        {
            return Err(ContextError::Unavailable(query.endpoint));
        }
        if let Some(filter) = &query.type_filter {
            let header = self.scoped.head_locked(filter)?;
            if !matches!(
                self.type_definition_locked(&header.pin)?.kind,
                TypeKind::Relation { .. }
            ) {
                return Err(ContextError::InvalidInput(
                    "relation filter must name a relation type".into(),
                ));
            }
        }
        let prefix = record_key(&query.endpoint);
        let digest = self.scoped.digest(
            "relation-query-cursor",
            &(
                &query.endpoint,
                query.direction,
                &query.type_filter,
                query.valid_at_ms,
                &self.scoped.access().scopes,
            ),
        )?;
        let start = if let Some(cursor) = &query.cursor {
            if cursor.query_digest != digest
                || !cursor.key.starts_with(&prefix)
                || cursor.key.len() != prefix.len() + 16
            {
                return Err(ContextError::InvalidInput(
                    "relation cursor belongs to a different query".into(),
                ));
            }
            cursor.key.clone()
        } else {
            prefix.clone()
        };
        let mut page = RelationPage {
            relations: Vec::new(),
            cursor: None,
            edges_examined: 0,
        };
        for entry in self.parts.adjacency.range(start..) {
            let (key, bytes) = entry?;
            if !key.starts_with(&prefix) {
                break;
            }
            if page.edges_examined == query.max_edges || page.relations.len() == query.limit {
                page.cursor = Some(RelationCursor {
                    query_digest: digest,
                    key: key.to_vec(),
                });
                break;
            }
            page.edges_examined += 1;
            let pin: RecordPin = decode(&bytes)?;
            let Some(header) = self.current_candidate(&pin, query.valid_at_ms, &mut memo)? else {
                continue;
            };
            let endpoints = header.endpoints.as_ref().ok_or_else(|| {
                ContextError::Corrupt("adjacency points at a non-relation".into())
            })?;
            if endpoints.from != query.endpoint && endpoints.to != query.endpoint {
                return Err(ContextError::Corrupt("adjacency endpoint mismatch".into()));
            }
            let direction_matches = match query.direction {
                RelationDirection::Incoming => endpoints.to == query.endpoint,
                RelationDirection::Outgoing => endpoints.from == query.endpoint,
                RelationDirection::Both => true,
            };
            let type_matches = query.type_filter.as_ref().is_none_or(|filter| {
                header
                    .type_pin
                    .as_ref()
                    .is_some_and(|pin| pin.record == *filter)
            });
            if direction_matches && type_matches {
                page.relations.push(self.read_locked(&pin)?);
            }
        }
        Ok(page)
    }

    pub fn recall(&self, query: ContextQuery) -> ContextResult<ContextRecall> {
        let budget = &query.budget;
        if query.text.len() > 4096
            || query.seeds.len() > 64
            || budget.max_candidates == 0
            || budget.max_candidates > 4096
            || budget.max_edges > 4096
            || budget.max_depth > 8
            || budget.max_results == 0
            || budget.max_results > 100
        {
            return Err(ContextError::InvalidInput(
                "invalid bounded context query".into(),
            ));
        }
        let mut memo = BTreeMap::new();
        let _guard = self.scoped.lock();
        let mut candidates: BTreeMap<ContextRef, (RecordPin, usize, Vec<RecordPin>)> =
            BTreeMap::new();
        let mut candidates_examined = 0;
        let mut edges_examined = 0;
        let mut truncated = false;
        for reference in &query.seeds {
            self.scoped.check_ref(reference)?;
            if candidates_examined >= budget.max_candidates {
                truncated = true;
                break;
            }
            let head = self.scoped.head_locked(reference)?;
            candidates_examined += 1;
            if self
                .current_candidate(&head.pin, query.valid_at_ms, &mut memo)?
                .is_some()
            {
                candidates.insert(reference.clone(), (head.pin, 0, Vec::new()));
            }
        }
        'postings: for term in terms(&query.text) {
            for scope in &self.scoped.access().scopes {
                for item in self.parts.postings.prefix_values(posting_prefix(
                    &self.scoped.access().owner,
                    scope,
                    self.scoped.content_digest(term.as_bytes()),
                )?) {
                    let value = item?;
                    if candidates_examined >= budget.max_candidates {
                        truncated = true;
                        break 'postings;
                    }
                    let pin: RecordPin = decode(&value)?;
                    candidates_examined += 1;
                    if self
                        .current_candidate(&pin, query.valid_at_ms, &mut memo)?
                        .is_none()
                    {
                        continue;
                    }
                    let entry =
                        candidates
                            .entry(pin.record.clone())
                            .or_insert((pin, 0, Vec::new()));
                    entry.1 += 1;
                }
            }
        }
        let mut starts: Vec<_> = candidates
            .iter()
            .map(|(reference, (_, score, _))| (reference.clone(), *score))
            .collect();
        starts.sort_by(|a, b| b.1.cmp(&a.1).then(a.0.cmp(&b.0)));
        let mut queue: VecDeque<_> = starts
            .into_iter()
            .map(|(reference, _)| (reference, Vec::<RecordPin>::new()))
            .collect();
        let mut visited: BTreeSet<_> = candidates.keys().cloned().collect();
        'expand: while let Some((reference, path)) = queue.pop_front() {
            if path.len() >= budget.max_depth {
                continue;
            }
            for item in self.parts.adjacency.prefix_values(record_key(&reference)) {
                let bytes = item?;
                if edges_examined == budget.max_edges {
                    truncated = true;
                    break 'expand;
                }
                edges_examined += 1;
                let edge: RecordPin = decode(&bytes)?;
                let Some(edge_header) =
                    self.current_candidate(&edge, query.valid_at_ms, &mut memo)?
                else {
                    continue;
                };
                let endpoints = edge_header.endpoints.ok_or_else(|| {
                    ContextError::Corrupt("adjacency points at a non-relation".into())
                })?;
                let other = if endpoints.from == reference {
                    endpoints.to
                } else if endpoints.to == reference {
                    endpoints.from
                } else {
                    return Err(ContextError::Corrupt("adjacency endpoint mismatch".into()));
                };
                if !visited.insert(other.clone()) {
                    continue;
                }
                if candidates_examined >= budget.max_candidates {
                    truncated = true;
                    break 'expand;
                }
                candidates_examined += 1;
                let head = self.scoped.head_locked(&other)?;
                if self
                    .current_candidate(&head.pin, query.valid_at_ms, &mut memo)?
                    .is_none()
                {
                    continue;
                }
                let mut next_path = path.clone();
                next_path.push(edge);
                candidates.insert(other.clone(), (head.pin, 0, next_path.clone()));
                queue.push_back((other, next_path));
            }
        }
        let mut ranked: Vec<_> = candidates
            .into_values()
            .filter(|(pin, _, _)| pin.record.kind.module() == StoreModule::Memory)
            .map(|(pin, score, path)| {
                let header = self.scoped.header_locked(&pin)?;
                Ok((pin, score, path, header.recorded_at_ms))
            })
            .collect::<ContextResult<_>>()?;
        ranked.sort_by(|a, b| {
            b.1.cmp(&a.1)
                .then(a.2.len().cmp(&b.2.len()))
                .then(b.3.cmp(&a.3))
                .then(a.0.cmp(&b.0))
        });
        truncated |= ranked.len() > budget.max_results;
        let hits = ranked
            .into_iter()
            .take(budget.max_results)
            .map(|(pin, matched_terms, path, _)| {
                Ok(ContextHit {
                    record: self.read_locked(&pin)?,
                    matched_terms,
                    path,
                })
            })
            .collect::<ContextResult<_>>()?;
        Ok(ContextRecall {
            hits,
            candidates_examined,
            edges_examined,
            truncated,
        })
    }
}
