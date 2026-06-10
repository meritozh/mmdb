use mmdb_core::Error;
use std::ops::Range;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MmqlError {
    pub message: String,
    pub span: Range<usize>,
}

pub(crate) fn split_top_level_keyword<'a>(s: &'a str, keyword: &str) -> Vec<&'a str> {
    let mut parts = Vec::new();
    let mut depth = 0_i32;
    let mut in_quote = false;
    let mut start = 0;
    let mut idx = 0;
    while idx < s.len() {
        let ch = s[idx..].chars().next().unwrap();
        match ch {
            '"' => in_quote = !in_quote,
            '(' if !in_quote => depth += 1,
            ')' if !in_quote => depth -= 1,
            _ => {}
        }
        if !in_quote && depth == 0 && s[idx..].starts_with(keyword) {
            parts.push(s[start..idx].trim());
            idx += keyword.len();
            start = idx;
            continue;
        }
        idx += ch.len_utf8();
    }
    parts.push(s[start..].trim());
    parts
}

pub(crate) fn split_once_top_level_keyword<'a>(
    s: &'a str,
    keyword: &str,
) -> Option<(&'a str, &'a str)> {
    let parts = split_top_level_keyword(s, keyword);
    if parts.len() == 2 {
        Some((parts[0], parts[1]))
    } else {
        None
    }
}

pub(crate) fn matching_close_paren(s: &str, open: usize) -> Option<usize> {
    let mut depth = 0_i32;
    let mut in_quote = false;
    for (idx, ch) in s[open..].char_indices() {
        let absolute = open + idx;
        match ch {
            '"' => in_quote = !in_quote,
            '(' if !in_quote => depth += 1,
            ')' if !in_quote => {
                depth -= 1;
                if depth == 0 {
                    return Some(absolute);
                }
            }
            _ => {}
        }
        if depth < 0 {
            return None;
        }
    }
    None
}

pub(crate) fn split_digits_unit(s: &str) -> Option<(&str, &str)> {
    let split = s
        .char_indices()
        .find(|(_, ch)| !ch.is_ascii_digit())
        .map(|(idx, _)| idx)?;
    Some((&s[..split], &s[split..]))
}

pub(crate) fn strip_outer_parens(s: &str) -> Option<&str> {
    let inner = s.strip_prefix('(')?.strip_suffix(')')?;
    let mut depth = 0_i32;
    for (idx, ch) in s.char_indices() {
        match ch {
            '(' => depth += 1,
            ')' => {
                depth -= 1;
                if depth == 0 && idx != s.len() - ch.len_utf8() {
                    return None;
                }
            }
            _ => {}
        }
        if depth < 0 {
            return None;
        }
    }
    if depth == 0 {
        Some(inner.trim())
    } else {
        None
    }
}

pub(crate) fn split_once_top_level(s: &str, needle: char) -> Option<(&str, &str)> {
    let mut depth = 0_i32;
    for (idx, ch) in s.char_indices() {
        match ch {
            '(' => depth += 1,
            ')' => depth -= 1,
            c if c == needle && depth == 0 => {
                return Some((&s[..idx], &s[idx + ch.len_utf8()..]))
            }
            _ => {}
        }
    }
    None
}

pub(crate) fn invalid(msg: impl Into<String>) -> Error {
    Error::InvalidArgument(msg.into())
}

pub(crate) fn diagnostic(
    _input: &str,
    message: impl Into<String>,
    span: Range<usize>,
) -> MmqlError {
    MmqlError {
        message: message.into(),
        span,
    }
}

pub(crate) fn marker_span(input: &str, marker: &str) -> Option<Range<usize>> {
    let start = input.find(marker)?;
    Some(start..start + marker.len())
}

pub(crate) fn bracketed_span(
    input: &str,
    marker_start: usize,
    open: char,
    close: char,
) -> Option<Range<usize>> {
    let rest = input.get(marker_start..)?;
    let open_offset = rest.find(open)?;
    let start = marker_start + open_offset;
    let end = start + input.get(start..)?.find(close)? + close.len_utf8();
    Some(marker_start..end)
}

pub(crate) fn fallback_span(input: &str) -> Range<usize> {
    0..input.len().min(1)
}

use std::time::{SystemTime, UNIX_EPOCH};

pub(crate) fn current_time_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as i64)
        .unwrap_or(0)
}
