use fjall::{Keyspace, PartitionCreateOptions, PartitionHandle, PersistMode};
use mmdb_core::{Content, Error, MemoryNode, Result};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use ulid::Ulid;

const PART_DOCS: &str = "lexical_docs_v1";
const PART_POSTINGS: &str = "lexical_postings_v1";

#[derive(Debug, Clone)]
pub(crate) struct LexicalHit {
    pub node_id: Ulid,
    pub score: f32,
    pub terms: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct LexicalDocument {
    length: usize,
    terms: BTreeMap<String, u32>,
}

pub(crate) struct LexicalIndex {
    keyspace: Keyspace,
    docs: PartitionHandle,
    postings: PartitionHandle,
}

impl LexicalIndex {
    pub(crate) fn open(keyspace: Keyspace) -> Result<Self> {
        let docs = keyspace
            .open_partition(PART_DOCS, PartitionCreateOptions::default())
            .map_err(|e| Error::Storage(e.to_string()))?;
        let postings = keyspace
            .open_partition(PART_POSTINGS, PartitionCreateOptions::default())
            .map_err(|e| Error::Storage(e.to_string()))?;
        Ok(Self {
            keyspace,
            docs,
            postings,
        })
    }

    pub(crate) fn upsert(&self, tenant: u32, node: Ulid, text: &str) -> Result<()> {
        let key = doc_key(tenant, node);
        let old = self
            .docs
            .get(&key)
            .map_err(|e| Error::Storage(e.to_string()))?
            .map(|value| serde_json::from_slice::<LexicalDocument>(&value))
            .transpose()?;
        let tokens = tokenize(text);
        let mut terms = BTreeMap::new();
        for token in &tokens {
            *terms.entry(token.clone()).or_insert(0) += 1;
        }
        let document = LexicalDocument {
            length: tokens.len(),
            terms,
        };
        let mut batch = self.keyspace.batch();
        if let Some(old) = old {
            for term in old.terms.keys() {
                batch.remove(&self.postings, posting_key(tenant, term, node));
            }
        }
        if document.terms.is_empty() {
            batch.remove(&self.docs, key);
        } else {
            batch.insert(&self.docs, key, serde_json::to_vec(&document)?);
            for (term, frequency) in &document.terms {
                batch.insert(
                    &self.postings,
                    posting_key(tenant, term, node),
                    frequency.to_be_bytes(),
                );
            }
        }
        batch.commit().map_err(|e| Error::Storage(e.to_string()))?;
        self.persist()
    }

    pub(crate) fn delete(&self, tenant: u32, node: Ulid) -> Result<()> {
        let key = doc_key(tenant, node);
        let old = self
            .docs
            .get(&key)
            .map_err(|e| Error::Storage(e.to_string()))?
            .map(|value| serde_json::from_slice::<LexicalDocument>(&value))
            .transpose()?;
        let mut batch = self.keyspace.batch();
        batch.remove(&self.docs, key);
        if let Some(old) = old {
            for term in old.terms.keys() {
                batch.remove(&self.postings, posting_key(tenant, term, node));
            }
        }
        batch.commit().map_err(|e| Error::Storage(e.to_string()))?;
        self.persist()
    }

    pub(crate) fn search_with_filter(
        &self,
        tenant: u32,
        query: &str,
        limit: usize,
        filter: impl Fn(Ulid) -> bool,
    ) -> Result<Vec<LexicalHit>> {
        let query_terms: BTreeSet<String> = tokenize(query).into_iter().collect();
        if query_terms.is_empty() || limit == 0 {
            return Ok(Vec::new());
        }
        let (document_count, total_length) = self.corpus_stats(tenant)?;
        if document_count == 0 {
            return Ok(Vec::new());
        }
        let average_length = total_length as f32 / document_count as f32;
        let mut scores: BTreeMap<Ulid, (f32, Vec<String>)> = BTreeMap::new();
        for term in query_terms {
            let postings = self.term_postings(tenant, &term)?;
            let document_frequency = postings.len() as f32;
            if document_frequency == 0.0 {
                continue;
            }
            let inverse_document_frequency = (1.0
                + (document_count as f32 - document_frequency + 0.5) / (document_frequency + 0.5))
                .ln();
            for (node, frequency) in postings {
                let Some(document) = self.document(tenant, node)? else {
                    continue;
                };
                let frequency = frequency as f32;
                let length_ratio = document.length as f32 / average_length.max(1.0);
                let score = inverse_document_frequency * frequency * 2.2
                    / (frequency + 1.2 * (0.25 + 0.75 * length_ratio));
                let entry = scores.entry(node).or_default();
                entry.0 += score;
                entry.1.push(term.clone());
            }
        }
        let mut hits: Vec<_> = scores
            .into_iter()
            .filter(|(node_id, _)| filter(*node_id))
            .map(|(node_id, (score, terms))| LexicalHit {
                node_id,
                score,
                terms,
            })
            .collect();
        hits.sort_by(|a, b| {
            b.score
                .total_cmp(&a.score)
                .then_with(|| a.node_id.cmp(&b.node_id))
        });
        hits.truncate(limit);
        Ok(hits)
    }

    fn corpus_stats(&self, tenant: u32) -> Result<(usize, usize)> {
        let (lo, hi) = tenant_range(tenant);
        let mut count = 0;
        let mut length = 0;
        for item in self.docs.range(lo..hi) {
            let (_, value) = item.map_err(|e| Error::Storage(e.to_string()))?;
            let document: LexicalDocument = serde_json::from_slice(&value)?;
            count += 1;
            length += document.length;
        }
        Ok((count, length))
    }

    fn document(&self, tenant: u32, node: Ulid) -> Result<Option<LexicalDocument>> {
        self.docs
            .get(doc_key(tenant, node))
            .map_err(|e| Error::Storage(e.to_string()))?
            .map(|value| serde_json::from_slice(&value).map_err(Error::from))
            .transpose()
    }

    fn term_postings(&self, tenant: u32, term: &str) -> Result<Vec<(Ulid, u32)>> {
        let prefix = posting_prefix(tenant, term);
        let mut hi = prefix.clone();
        hi.extend_from_slice(&[0xff; 16]);
        let mut postings = Vec::new();
        for item in self.postings.range(prefix.clone()..hi) {
            let (key, value) = item.map_err(|e| Error::Storage(e.to_string()))?;
            if key.len() != prefix.len() + 16 || value.len() != 4 {
                continue;
            }
            let mut id = [0; 16];
            id.copy_from_slice(&key[prefix.len()..]);
            let mut frequency = [0; 4];
            frequency.copy_from_slice(&value);
            postings.push((Ulid(u128::from_be_bytes(id)), u32::from_be_bytes(frequency)));
        }
        Ok(postings)
    }

    fn persist(&self) -> Result<()> {
        self.keyspace
            .persist(PersistMode::SyncAll)
            .map_err(|e| Error::Storage(e.to_string()))
    }
}

pub(crate) fn searchable_text(node: &MemoryNode, projections: &[String]) -> String {
    let mut output = String::new();
    match &node.content {
        Content::Text(text) => output.push_str(text),
        Content::Structured(value) => collect_strings(value, &mut output),
        Content::Blob { .. } => {}
    }
    for projection in projections {
        output.push('\n');
        output.push_str(projection);
    }
    output
}

fn collect_strings(value: &serde_json::Value, output: &mut String) {
    match value {
        serde_json::Value::String(text) => {
            output.push(' ');
            output.push_str(text);
        }
        serde_json::Value::Array(values) => {
            for value in values {
                collect_strings(value, output);
            }
        }
        serde_json::Value::Object(values) => {
            for (key, value) in values {
                output.push(' ');
                output.push_str(key);
                collect_strings(value, output);
            }
        }
        _ => {}
    }
}

fn tokenize(input: &str) -> Vec<String> {
    let mut tokens = Vec::new();
    let mut word = String::new();
    let mut cjk = Vec::new();
    let flush_word = |word: &mut String, tokens: &mut Vec<String>| {
        if !word.is_empty() {
            let mut token = std::mem::take(word);
            token.truncate(token.len().min(256));
            tokens.push(token);
        }
    };
    let flush_cjk = |cjk: &mut Vec<char>, tokens: &mut Vec<String>| {
        for character in cjk.iter() {
            tokens.push(character.to_string());
        }
        for pair in cjk.windows(2) {
            tokens.push(pair.iter().collect());
        }
        cjk.clear();
    };
    for character in input.chars().flat_map(char::to_lowercase) {
        if character.is_ascii_alphanumeric() || character == '_' {
            flush_cjk(&mut cjk, &mut tokens);
            word.push(character);
        } else if is_cjk(character) {
            flush_word(&mut word, &mut tokens);
            cjk.push(character);
        } else {
            flush_word(&mut word, &mut tokens);
            flush_cjk(&mut cjk, &mut tokens);
        }
    }
    flush_word(&mut word, &mut tokens);
    flush_cjk(&mut cjk, &mut tokens);
    tokens
}

fn is_cjk(character: char) -> bool {
    matches!(character as u32, 0x3400..=0x4dbf | 0x4e00..=0x9fff | 0xf900..=0xfaff)
}

fn doc_key(tenant: u32, node: Ulid) -> Vec<u8> {
    let mut key = Vec::with_capacity(20);
    key.extend_from_slice(&tenant.to_be_bytes());
    key.extend_from_slice(&node.0.to_be_bytes());
    key
}

fn posting_prefix(tenant: u32, term: &str) -> Vec<u8> {
    let bytes = term.as_bytes();
    let length = u16::try_from(bytes.len()).unwrap_or(u16::MAX);
    let mut key = Vec::with_capacity(6 + bytes.len());
    key.extend_from_slice(&tenant.to_be_bytes());
    key.extend_from_slice(&length.to_be_bytes());
    key.extend_from_slice(&bytes[..usize::from(length)]);
    key
}

fn posting_key(tenant: u32, term: &str, node: Ulid) -> Vec<u8> {
    let mut key = posting_prefix(tenant, term);
    key.extend_from_slice(&node.0.to_be_bytes());
    key
}

fn tenant_range(tenant: u32) -> (Vec<u8>, Vec<u8>) {
    let lo = tenant.to_be_bytes().to_vec();
    let mut hi = lo.clone();
    hi.extend_from_slice(&[0xff; 16]);
    (lo, hi)
}
