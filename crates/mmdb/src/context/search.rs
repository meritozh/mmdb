use super::super::{RecordState, Scope};
use super::storage::*;
use super::*;
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

fn prefix_table(needle: &[u8]) -> Vec<usize> {
    let mut table = vec![0; needle.len()];
    let mut matched = 0;
    for at in 1..needle.len() {
        while matched > 0 && needle[at] != needle[matched] {
            matched = table[matched - 1];
        }
        if needle[at] == needle[matched] {
            matched += 1;
        }
        table[at] = matched;
    }
    table
}

fn find_bytes(haystack: &[u8], needle: &[u8], table: &[usize]) -> Option<usize> {
    let mut matched = 0;
    for (at, byte) in haystack.iter().enumerate() {
        let byte = byte.to_ascii_lowercase();
        while matched > 0 && byte != needle[matched] {
            matched = table[matched - 1];
        }
        if byte == needle[matched] {
            matched += 1;
        }
        if matched == needle.len() {
            return Some(at + 1 - needle.len());
        }
    }
    None
}

impl ContextStore<'_> {
    pub(crate) fn replace_postings(
        &self,
        batch: &mut fjall::Batch,
        header: &RecordHeader,
        properties: Option<&Properties>,
    ) -> ContextResult<()> {
        let key = record_key(&header.pin.record);
        if let Some(previous) = self.parts.record_terms.get(&key)? {
            for term in decode::<Vec<[u8; 32]>>(&previous)? {
                batch.remove(
                    &self.parts.postings,
                    posting_key(&self.access.owner, &header.scope, term, &header.pin.record)?,
                );
            }
        }
        if let Some(properties) = properties {
            let text = serde_json::to_string(properties)?;
            let hashes: BTreeSet<_> = terms(&text)
                .into_iter()
                .map(|term| self.content_digest(term.as_bytes()))
                .collect();
            for hash in &hashes {
                batch.insert(
                    &self.parts.postings,
                    posting_key(&self.access.owner, &header.scope, *hash, &header.pin.record)?,
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
        batch: &mut fjall::Batch,
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
    ) -> ContextResult<Option<RecordHeader>> {
        let header = match self.header_locked(pin) {
            Ok(header) => header,
            Err(ContextError::AccessDenied | ContextError::NotFound(_)) => return Ok(None),
            Err(e) => return Err(e),
        };
        if at.is_some_and(|at| !header.temporal.contains_valid_time(at))
            || !self.available_locked(pin, true)?
        {
            return Ok(None);
        }
        if let Some(endpoints) = &header.endpoints {
            for endpoint in [&endpoints.from, &endpoints.to] {
                let endpoint = match self.head_locked(endpoint) {
                    Ok(header) => header,
                    Err(ContextError::AccessDenied | ContextError::NotFound(_)) => return Ok(None),
                    Err(error) => return Err(error),
                };
                if at.is_some_and(|at| !endpoint.temporal.contains_valid_time(at))
                    || !self.available_locked(&endpoint.pin, true)?
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
        let _guard = self.db.write_lock.lock();
        let endpoint = self.head_locked(&query.endpoint)?;
        if self
            .current_candidate(&endpoint.pin, query.valid_at_ms)?
            .is_none()
        {
            return Err(ContextError::Unavailable(query.endpoint));
        }
        if let Some(filter) = &query.type_filter {
            let header = self.head_locked(filter)?;
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
        let digest = self.digest(
            "relation-query-cursor",
            &(
                &query.endpoint,
                query.direction,
                &query.type_filter,
                query.valid_at_ms,
                &self.access.scopes,
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
            let Some(header) = self.current_candidate(&pin, query.valid_at_ms)? else {
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
        let _guard = self.db.write_lock.lock();
        let mut candidates: BTreeMap<ContextRef, (RecordPin, usize, Vec<RecordPin>)> =
            BTreeMap::new();
        let mut candidates_examined = 0;
        let mut edges_examined = 0;
        let mut truncated = false;
        for reference in &query.seeds {
            self.check_ref(reference)?;
            if candidates_examined >= budget.max_candidates {
                truncated = true;
                break;
            }
            let head = self.head_locked(reference)?;
            candidates_examined += 1;
            if self
                .current_candidate(&head.pin, query.valid_at_ms)?
                .is_some()
            {
                candidates.insert(reference.clone(), (head.pin, 0, Vec::new()));
            }
        }
        'postings: for term in terms(&query.text) {
            for scope in &self.access.scopes {
                for item in self.parts.postings.prefix(posting_prefix(
                    &self.access.owner,
                    scope,
                    self.content_digest(term.as_bytes()),
                )?) {
                    if candidates_examined >= budget.max_candidates {
                        truncated = true;
                        break 'postings;
                    }
                    let (_, value) = item?;
                    let pin: RecordPin = decode(&value)?;
                    candidates_examined += 1;
                    if self.current_candidate(&pin, query.valid_at_ms)?.is_none() {
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
            for item in self.parts.adjacency.prefix(record_key(&reference)) {
                if edges_examined == budget.max_edges {
                    truncated = true;
                    break 'expand;
                }
                let (_, bytes) = item?;
                edges_examined += 1;
                let edge: RecordPin = decode(&bytes)?;
                let Some(edge_header) = self.current_candidate(&edge, query.valid_at_ms)? else {
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
                let head = self.head_locked(&other)?;
                if self
                    .current_candidate(&head.pin, query.valid_at_ms)?
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
            .map(|(pin, score, path)| {
                let header = self.header_locked(&pin)?;
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

    /// Exact substring search over raw bytes, ASCII case-insensitive. A cursor
    /// resumes bounded chunk scans, including matches across chunk boundaries.
    pub fn search_history(&self, query: HistoryQuery) -> ContextResult<HistorySearch> {
        if query.text.is_empty()
            || query.text.len() > 4096
            || query.limit == 0
            || query.limit > 100
            || query.max_events == 0
            || query.max_events > 4096
            || query.max_bytes < query.text.len() * 2
            || query.max_bytes > 16 * 1024 * 1024
        {
            return Err(ContextError::InvalidInput(
                "invalid bounded history query".into(),
            ));
        }
        if let Some(session) = &query.session {
            validate_name(session, "session")?;
        }
        let _guard = self.db.write_lock.lock();
        let needle = query.text.as_bytes().to_ascii_lowercase();
        let table = prefix_table(&needle);
        let digest = self.digest(
            "history-search-cursor",
            &(&query.text, &query.session, &self.access.scopes),
        )?;
        let prefix = query.session.as_ref().map_or_else(
            || owner_key(&self.access.owner),
            |session| session_key(&self.access.owner, session),
        );
        let start = if let Some(cursor) = &query.cursor {
            if cursor.query_digest != digest || !cursor.key.starts_with(&prefix) {
                return Err(ContextError::InvalidInput(
                    "history cursor belongs to a different query".into(),
                ));
            }
            cursor.key.clone()
        } else {
            prefix.clone()
        };
        let mut result = HistorySearch {
            matches: Vec::new(),
            cursor: None,
            bytes_examined: 0,
            events_examined: 0,
        };
        for entry in self.parts.history_order.range(start.clone()..) {
            let (key, reference) = entry?;
            if !key.starts_with(&prefix) {
                break;
            }
            let mut offset = query
                .cursor
                .as_ref()
                .filter(|cursor| cursor.key.as_slice() == key.as_ref())
                .map_or(0, |cursor| cursor.offset);
            if result.events_examined == query.max_events || result.matches.len() == query.limit {
                result.cursor = Some(HistorySearchCursor {
                    query_digest: digest,
                    key: key.to_vec(),
                    offset,
                });
                return Ok(result);
            }
            result.events_examined += 1;
            let header = match self.head_locked(&decode(&reference)?) {
                Ok(header) => header,
                Err(ContextError::AccessDenied) => continue,
                Err(error) => return Err(error),
            };
            if !self.available_locked(&header.pin, false)? {
                continue;
            }
            if offset > header.payload.bytes {
                return Err(ContextError::InvalidInput(
                    "history cursor exceeds payload length".into(),
                ));
            }
            while header.payload.bytes - offset >= needle.len() as u64 {
                let remaining = query.max_bytes - result.bytes_examined;
                if remaining < needle.len() {
                    result.cursor = Some(HistorySearchCursor {
                        query_digest: digest,
                        key: key.to_vec(),
                        offset,
                    });
                    return Ok(result);
                }
                let page = self.payload_slice_locked(
                    &header,
                    offset,
                    remaining.min(MAX_PAYLOAD_PAGE_BYTES),
                )?;
                result.bytes_examined += page.bytes.len();
                if let Some(at) = find_bytes(&page.bytes, &needle, &table) {
                    result.matches.push(HistoryMatch {
                        header: header.clone(),
                        byte_offset: offset + at as u64,
                    });
                    break;
                }
                if page.next_offset.is_none() {
                    break;
                }
                offset += (page.bytes.len() - needle.len() + 1) as u64;
            }
        }
        Ok(result)
    }
}
