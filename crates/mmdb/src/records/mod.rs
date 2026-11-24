use serde::{Deserialize, Serialize};
use std::fmt;
use ulid::Ulid;

pub const MAX_ACCESS_SCOPES: usize = 32;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Actor {
    User,
    Assistant,
    System,
    Operator,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum Scope {
    Personal,
    Workspace(Ulid),
    Session(Ulid),
    Artifact(Ulid),
    System,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct TemporalFacts {
    pub observed_at_ms: i64,
    pub source_at_ms: Option<i64>,
    pub valid_from_ms: Option<i64>,
    pub valid_to_ms: Option<i64>,
}

impl TemporalFacts {
    pub fn observed_at(observed_at_ms: i64) -> Self {
        Self {
            observed_at_ms,
            source_at_ms: None,
            valid_from_ms: None,
            valid_to_ms: None,
        }
    }

    pub(crate) fn validate(self) -> Result<(), InvalidTemporalFacts> {
        if matches!((self.valid_from_ms, self.valid_to_ms), (Some(from), Some(to)) if from >= to) {
            return Err(InvalidTemporalFacts);
        }
        Ok(())
    }

    /// Whether `at_ms` falls inside the record's explicit valid-time window.
    ///
    /// This deliberately only reasons about *valid time* (when the fact holds in
    /// the world). It is distinct from `observed_at_ms`, which records when the
    /// observation was captured, and from `recorded_at_ms` on the header, which
    /// is when the row was written to the store. The three concepts are kept on
    /// separate fields so they can never be conflated.
    pub fn is_valid_at(&self, at_ms: i64) -> bool {
        self.valid_from_ms.is_none_or(|from| at_ms >= from)
            && self.valid_to_ms.is_none_or(|to| at_ms < to)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RecordState {
    Active,
    Retracted,
    Superseded,
    Unsupported,
    Purged,
}

#[derive(Debug)]
pub(crate) struct InvalidTemporalFacts;

impl fmt::Display for InvalidTemporalFacts {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("valid time must use a non-empty [from, to) interval")
    }
}
