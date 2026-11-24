use super::*;
use crate::records::codec::*;

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

impl SessionLog<'_> {
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
        let _guard = self.scoped.lock();
        let needle = query.text.as_bytes().to_ascii_lowercase();
        let table = prefix_table(&needle);
        let digest = self.scoped.digest(
            "history-search-cursor",
            &(&query.text, &query.session, &self.scoped.access().scopes),
        )?;
        let prefix = query.session.as_ref().map_or_else(
            || owner_key(&self.scoped.access().owner),
            |session| session_key(&self.scoped.access().owner, session),
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
            let header = match self.scoped.head_locked(&decode(&reference)?) {
                Ok(header) => header,
                Err(ContextError::AccessDenied) => continue,
                Err(error) => return Err(error),
            };
            if !self.scoped.available_locked(&header.pin, false)? {
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
                let page = self.scoped.payload_slice_locked(
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
