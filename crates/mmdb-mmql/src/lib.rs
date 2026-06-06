//! Minimal MMQL parser, resolver, and lowering to `mmdb-query::LogicalPlan`.

use mmdb_core::{Error, NodeKind, Result};
use mmdb_query::{
    AggregateExpr, Expr, FieldRef, JoinKey, Literal, LogicalPlan, ModelId, Predicate, ScoreExpr,
    SortKey, TableId, VectorRef,
};
use std::collections::BTreeSet;
use std::ops::Range;
use std::time::{SystemTime, UNIX_EPOCH};

pub fn parse(input: &str) -> Result<mmdb_query::LogicalPlan> {
    Ok(parse_ast(input)?.lower())
}

pub fn parse_with_resolver(input: &str, resolver: &Resolver) -> Result<mmdb_query::LogicalPlan> {
    let query = parse_ast(input)?;
    resolver.resolve(&query)?;
    Ok(query.lower())
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MmqlError {
    pub message: String,
    pub span: Range<usize>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct RecallQuery {
    pub tenant: u32,
    pub kinds: Vec<NodeKind>,
    pub created_at_predicates: Vec<Predicate>,
    pub metadata_predicates: Vec<Predicate>,
    pub where_predicate: Option<Predicate>,
    pub query: VectorRef,
    pub model: String,
    pub topk: usize,
    pub limit: usize,
    pub graph: Option<GraphClause>,
    pub udf: Option<UdfClause>,
    pub score: Option<ScoreExpr>,
    pub joins: Vec<JoinClause>,
    pub aggregate: Option<AggregateClause>,
    pub return_fields: Vec<FieldRef>,
}

impl RecallQuery {
    pub fn lower(&self) -> LogicalPlan {
        let filter = self.where_predicate.clone().unwrap_or_else(|| {
            let mut predicates = vec![
                Predicate::eq(FieldRef::Tenant, Literal::U32(self.tenant)),
                Predicate::kind_in(self.kinds.clone()),
            ];
            predicates.extend(self.created_at_predicates.clone());
            predicates.extend(self.metadata_predicates.clone());
            Predicate::and(predicates)
        });

        let mut plan = LogicalPlan::VectorSearch {
            query: self.query.clone(),
            k: self.topk,
            filter: Some(filter),
            model: ModelId::from(self.model.clone()),
        };

        if let Some(graph) = &self.graph {
            if let Some(seed) = &graph.from {
                plan = LogicalPlan::Join {
                    left: Box::new(plan),
                    right: Box::new(LogicalPlan::GraphExpand {
                        from: Box::new(LogicalPlan::Scan {
                            table: TableId::Nodes,
                            filter: Some(Predicate::and([
                                Predicate::eq(FieldRef::Tenant, Literal::U32(self.tenant)),
                                seed.filter.clone(),
                            ])),
                        }),
                        relation: Some(graph.relation.clone()),
                        depth: graph.depth,
                    }),
                    on: JoinKey::NodeId,
                };
            } else {
                plan = LogicalPlan::GraphExpand {
                    from: Box::new(plan),
                    relation: Some(graph.relation.clone()),
                    depth: graph.depth,
                };
            }
        }

        if let Some(udf) = &self.udf {
            plan = LogicalPlan::Udf {
                input: Box::new(plan),
                name: udf.name.clone(),
                args: vec![Expr::Field(FieldRef::Score)],
            };
        } else if let Some(expr) = &self.score {
            plan = LogicalPlan::Score {
                input: Box::new(plan),
                expr: expr.clone(),
            };
        }

        plan = LogicalPlan::TopK {
            input: Box::new(plan),
            k: self.limit,
            by: SortKey::ScoreDesc,
        };

        for join in &self.joins {
            plan = LogicalPlan::Join {
                left: Box::new(plan),
                right: Box::new(LogicalPlan::Scan {
                    table: TableId::Nodes,
                    filter: Some(Predicate::and([
                        Predicate::eq(FieldRef::Tenant, Literal::U32(self.tenant)),
                        join.right_filter.clone(),
                    ])),
                }),
                on: join.on.clone(),
            };
        }

        if let Some(aggregate) = &self.aggregate {
            plan = LogicalPlan::Aggregate {
                input: Box::new(plan),
                group_by: aggregate.group_by.clone(),
                aggregate: aggregate.aggregate.clone(),
            };
        }

        if !self.return_fields.is_empty() {
            plan = LogicalPlan::Project {
                input: Box::new(plan),
                fields: self.return_fields.clone(),
            };
        }

        plan
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct GraphClause {
    pub relation: String,
    pub depth: u8,
    pub from: Option<GraphSeed>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct GraphSeed {
    pub alias: String,
    pub filter: Predicate,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UdfClause {
    pub name: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct JoinClause {
    pub alias: String,
    pub kinds: Vec<NodeKind>,
    pub right_filter: Predicate,
    pub on: JoinKey,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AggregateClause {
    pub group_by: Vec<FieldRef>,
    pub aggregate: AggregateExpr,
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct Resolver {
    models: BTreeSet<String>,
    udfs: BTreeSet<String>,
}

impl Resolver {
    pub fn allow_model(mut self, model: impl Into<String>) -> Self {
        self.models.insert(model.into());
        self
    }

    pub fn allow_udf(mut self, udf: impl Into<String>) -> Self {
        self.udfs.insert(udf.into());
        self
    }

    pub fn resolve(&self, query: &RecallQuery) -> Result<()> {
        if !self.models.is_empty() && !self.models.contains(&query.model) {
            return Err(invalid(format!("unknown model `{}`", query.model)));
        }
        if let Some(udf) = &query.udf {
            if !self.udfs.is_empty() && !self.udfs.contains(&udf.name) {
                return Err(invalid(format!("unknown UDF `{}`", udf.name)));
            }
        }
        Ok(())
    }
}

pub fn parse_ast(input: &str) -> Result<RecallQuery> {
    parse_ast_diagnostic(input).map_err(|err| invalid(err.message))
}

pub fn parse_ast_diagnostic(input: &str) -> std::result::Result<RecallQuery, MmqlError> {
    let normalized = input.split_whitespace().collect::<Vec<_>>().join(" ");
    if !normalized.starts_with("recall ") {
        return Err(diagnostic(
            input,
            "recall query must start with `recall`",
            fallback_span(input),
        ));
    }

    let tenant = parse_u32_after(&normalized, "n.tenant = ").ok_or_else(|| {
        diagnostic(
            input,
            "recall query must contain `n.tenant = <u32>`",
            marker_span(input, "where").unwrap_or_else(|| fallback_span(input)),
        )
    })?;
    let kinds = parse_kind_list_diagnostic(input, &normalized, "n.kind in (")?;
    let created_at_predicates = parse_created_at_predicates(&normalized);
    let metadata_predicates = parse_metadata_predicates(&normalized);
    let where_predicate = parse_where_predicate_expression(input, &normalized)?;
    let query = parse_vector_ref(&normalized).ok_or_else(|| {
        diagnostic(
            input,
            "recall query must contain `similar to [..]` or `similar to embed(\"...\")`",
            marker_span(input, "similar to").unwrap_or_else(|| fallback_span(input)),
        )
    })?;
    let model = parse_quoted_after(&normalized, "using model ").ok_or_else(|| {
        diagnostic(
            input,
            "recall query must contain `using model \"...\"`",
            marker_span(input, "similar to [")
                .and_then(|start| bracketed_span(input, start.start, '[', ']'))
                .unwrap_or_else(|| {
                    marker_span(input, "similar to").unwrap_or_else(|| fallback_span(input))
                }),
        )
    })?;
    let topk = parse_usize_after(&normalized, "topk ").ok_or_else(|| {
        diagnostic(
            input,
            "recall query must contain `topk <usize>`",
            marker_span(input, "topk").unwrap_or_else(|| fallback_span(input)),
        )
    })?;
    let limit = parse_usize_after(&normalized, "limit ").ok_or_else(|| {
        diagnostic(
            input,
            "recall query must contain `limit <usize>`",
            marker_span(input, "limit").unwrap_or_else(|| fallback_span(input)),
        )
    })?;

    let graph = parse_connected_clause(&normalized);
    let udf = parse_quoted_after(&normalized, "score by udf ").map(|name| UdfClause { name });
    let score = if udf.is_none() {
        parse_score_expr(&normalized)
    } else {
        None
    };
    let aggregate = parse_aggregate_clause(&normalized);
    let joins = parse_join_clauses_diagnostic(input, &normalized)?;
    let return_fields = parse_return_clause_diagnostic(input, &normalized)?;

    Ok(RecallQuery {
        tenant,
        kinds,
        created_at_predicates,
        metadata_predicates,
        where_predicate,
        query,
        model,
        topk,
        limit,
        graph,
        udf,
        score,
        joins,
        aggregate,
        return_fields,
    })
}

fn parse_u32_after(s: &str, marker: &str) -> Option<u32> {
    parse_digits_after(s, marker)?.parse().ok()
}

fn parse_usize_after(s: &str, marker: &str) -> Option<usize> {
    parse_digits_after(s, marker)?.parse().ok()
}

fn parse_i64_after(s: &str, marker: &str) -> Option<(usize, i64)> {
    let start = s.find(marker)? + marker.len();
    let rest = &s[start..];
    let mut end = 0;
    for (idx, ch) in rest.char_indices() {
        if idx == 0 && ch == '-' {
            end = ch.len_utf8();
            continue;
        }
        if ch.is_ascii_digit() {
            end = idx + ch.len_utf8();
        } else {
            break;
        }
    }
    if end == 0 || rest[..end].trim() == "-" {
        return None;
    }
    Some((start, rest[..end].parse().ok()?))
}

fn parse_created_at_relative_after(s: &str, marker: &str) -> Option<(usize, i64)> {
    let start = s.find(marker)? + marker.len();
    let rest = &s[start..];
    let duration = rest.strip_prefix("now() - ")?;
    let end = duration
        .char_indices()
        .take_while(|(_, ch)| ch.is_ascii_alphanumeric())
        .map(|(idx, ch)| idx + ch.len_utf8())
        .last()?;
    Some((start, current_time_ms() - duration_ms(&duration[..end])?))
}

fn parse_digits_after<'a>(s: &'a str, marker: &str) -> Option<&'a str> {
    let rest = &s[s.find(marker)? + marker.len()..];
    let len = rest
        .char_indices()
        .take_while(|(_, c)| c.is_ascii_digit())
        .map(|(idx, c)| idx + c.len_utf8())
        .last()?;
    Some(&rest[..len])
}

fn parse_created_at_predicates(s: &str) -> Vec<Predicate> {
    let mut predicates = Vec::new();
    for (marker, op) in [
        ("n.created_at >= ", ">="),
        ("n.created_at <= ", "<="),
        ("n.created_at > ", ">"),
        ("n.created_at < ", "<"),
    ] {
        if let Some((pos, value)) =
            parse_i64_after(s, marker).or_else(|| parse_created_at_relative_after(s, marker))
        {
            let predicate = match op {
                ">=" => Predicate::Gte(FieldRef::CreatedAtMs, Literal::I64(value)),
                "<=" => Predicate::Lte(FieldRef::CreatedAtMs, Literal::I64(value)),
                ">" => Predicate::Gt(FieldRef::CreatedAtMs, Literal::I64(value)),
                "<" => Predicate::Lt(FieldRef::CreatedAtMs, Literal::I64(value)),
                _ => unreachable!(),
            };
            predicates.push((pos, predicate));
        }
    }
    predicates.sort_by_key(|(pos, _)| *pos);
    predicates
        .into_iter()
        .map(|(_, predicate)| predicate)
        .collect()
}

fn parse_metadata_predicates(s: &str) -> Vec<Predicate> {
    let mut predicates = Vec::new();
    let Some(where_clause) = where_clause(s) else {
        return predicates;
    };
    for part in where_clause.split(" and ") {
        let part = part.trim();
        let Some(rest) = part.strip_prefix("n.") else {
            continue;
        };
        let Some((field, value)) = rest.split_once(" = ") else {
            continue;
        };
        if matches!(field, "tenant" | "kind" | "created_at" | "updated_at") {
            continue;
        }
        let Some(value) = value.strip_prefix('"').and_then(|v| v.strip_suffix('"')) else {
            continue;
        };
        predicates.push(Predicate::eq(
            FieldRef::Metadata(field.to_string()),
            Literal::String(value.to_string()),
        ));
    }
    predicates
}

fn where_clause(s: &str) -> Option<&str> {
    let where_start = s.find(" where ")?;
    let end = [
        " similar to ",
        " connected ",
        " score by ",
        " limit ",
        " join ",
        " count ",
        " return ",
    ]
    .iter()
    .filter_map(|marker| s[where_start..].find(marker).map(|idx| where_start + idx))
    .min()
    .unwrap_or(s.len());
    Some(s[where_start + " where ".len()..end].trim())
}

fn parse_where_predicate_expression(
    input: &str,
    normalized: &str,
) -> std::result::Result<Option<Predicate>, MmqlError> {
    let Some(where_clause) = where_clause(normalized) else {
        return Ok(None);
    };
    if !uses_boolean_predicate_syntax(where_clause) {
        return Ok(None);
    }
    parse_predicate_expr(where_clause).map(Some).ok_or_else(|| {
        diagnostic(
            input,
            "could not parse boolean where predicate expression",
            marker_span(input, "where").unwrap_or_else(|| fallback_span(input)),
        )
    })
}

fn uses_boolean_predicate_syntax(where_clause: &str) -> bool {
    where_clause.contains(" or ")
        || where_clause.starts_with("not ")
        || where_clause.contains(" not ")
}

fn parse_predicate_expr(s: &str) -> Option<Predicate> {
    parse_predicate_or(s.trim())
}

fn parse_predicate_or(s: &str) -> Option<Predicate> {
    let parts = split_top_level_keyword(s, " or ");
    if parts.len() > 1 {
        return Some(Predicate::Or(
            parts
                .into_iter()
                .map(parse_predicate_and)
                .collect::<Option<Vec<_>>>()?,
        ));
    }
    parse_predicate_and(s)
}

fn parse_predicate_and(s: &str) -> Option<Predicate> {
    let parts = split_top_level_keyword(s, " and ");
    if parts.len() > 1 {
        return Some(Predicate::and(
            parts
                .into_iter()
                .map(parse_predicate_not)
                .collect::<Option<Vec<_>>>()?,
        ));
    }
    parse_predicate_not(s)
}

fn parse_predicate_not(s: &str) -> Option<Predicate> {
    let s = s.trim();
    if let Some(rest) = s.strip_prefix("not ") {
        return Some(Predicate::Not(Box::new(parse_predicate_not(rest)?)));
    }
    parse_predicate_primary(s)
}

fn parse_predicate_primary(s: &str) -> Option<Predicate> {
    let s = s.trim();
    if let Some(inner) = strip_outer_parens(s) {
        return parse_predicate_expr(inner);
    }
    parse_predicate_comparison(s)
}

fn parse_predicate_comparison(s: &str) -> Option<Predicate> {
    if let Some((field, raw_values)) = split_once_top_level_keyword(s, " in ") {
        let field = parse_predicate_field(field.trim())?;
        let values = parse_predicate_in_values(&field, raw_values.trim())?;
        return Some(Predicate::In(field, values));
    }

    for (op, build) in [
        (" >= ", Predicate::Gte as fn(FieldRef, Literal) -> Predicate),
        (" <= ", Predicate::Lte as fn(FieldRef, Literal) -> Predicate),
        (" > ", Predicate::Gt as fn(FieldRef, Literal) -> Predicate),
        (" < ", Predicate::Lt as fn(FieldRef, Literal) -> Predicate),
        (" = ", Predicate::Eq as fn(FieldRef, Literal) -> Predicate),
    ] {
        if let Some((field, raw_value)) = split_once_top_level_keyword(s, op) {
            let field = parse_predicate_field(field.trim())?;
            let literal = parse_predicate_literal(&field, raw_value.trim())?;
            return Some(build(field, literal));
        }
    }
    None
}

fn parse_predicate_field(s: &str) -> Option<FieldRef> {
    let (_alias, field) = s.split_once('.')?;
    field_ref_from_name(field)
}

fn field_ref_from_name(field: &str) -> Option<FieldRef> {
    match field {
        "tenant" => Some(FieldRef::Tenant),
        "kind" => Some(FieldRef::Kind),
        "created_at" => Some(FieldRef::CreatedAtMs),
        "updated_at" => Some(FieldRef::UpdatedAtMs),
        "content" => Some(FieldRef::Content),
        "score" => Some(FieldRef::Score),
        "node_id" => Some(FieldRef::NodeId),
        name if !name.is_empty() => Some(FieldRef::Metadata(name.to_string())),
        _ => None,
    }
}

fn parse_predicate_literal(field: &FieldRef, raw: &str) -> Option<Literal> {
    match field {
        FieldRef::Tenant => raw.parse::<u32>().ok().map(Literal::U32),
        FieldRef::Kind => parse_kind(raw).map(Literal::NodeKind),
        FieldRef::CreatedAtMs | FieldRef::UpdatedAtMs => raw.parse::<i64>().ok().map(Literal::I64),
        FieldRef::Metadata(_) | FieldRef::NodeId | FieldRef::Content => {
            parse_string_literal(raw).map(Literal::String)
        }
        FieldRef::Score => raw
            .parse::<f32>()
            .ok()
            .map(|value| Literal::F32(mmdb_query::OrderedF32(value))),
    }
}

fn parse_predicate_in_values(field: &FieldRef, raw: &str) -> Option<Vec<Literal>> {
    let inner = raw.strip_prefix('(')?.strip_suffix(')')?;
    inner
        .split(',')
        .map(|part| parse_predicate_literal(field, part.trim()))
        .collect()
}

fn parse_string_literal(raw: &str) -> Option<String> {
    raw.strip_prefix('"')
        .and_then(|value| value.strip_suffix('"'))
        .map(ToString::to_string)
}

fn split_top_level_keyword<'a>(s: &'a str, keyword: &str) -> Vec<&'a str> {
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

fn split_once_top_level_keyword<'a>(s: &'a str, keyword: &str) -> Option<(&'a str, &'a str)> {
    let parts = split_top_level_keyword(s, keyword);
    if parts.len() == 2 {
        Some((parts[0], parts[1]))
    } else {
        None
    }
}

fn parse_quoted_after(s: &str, marker: &str) -> Option<String> {
    let rest = &s[s.find(marker)? + marker.len()..];
    let rest = rest.strip_prefix('"')?;
    let end = rest.find('"')?;
    Some(rest[..end].to_string())
}

fn parse_connected_clause(s: &str) -> Option<GraphClause> {
    parse_connected_from_clause(s).or_else(|| {
        let relation = parse_word_after(s, "connected via ")?;
        let depth = parse_usize_after(s, "depth ")?;
        Some(GraphClause {
            relation,
            depth: depth.try_into().ok()?,
            from: None,
        })
    })
}

fn parse_connected_from_clause(s: &str) -> Option<GraphClause> {
    let marker = "connected from ";
    let open = s.find(marker)? + marker.len();
    if s.get(open..)?.chars().next()? != '(' {
        return None;
    }
    let close = matching_close_paren(s, open)?;
    let seed = parse_graph_seed(&s[open + 1..close])?;
    let tail = &s[close + 1..];
    let relation = parse_word_after(tail, " via ")?;
    let depth = parse_usize_after(tail, "depth ")?;
    Some(GraphClause {
        relation,
        depth: depth.try_into().ok()?,
        from: Some(seed),
    })
}

fn parse_graph_seed(s: &str) -> Option<GraphSeed> {
    let (alias, rest) = s.split_once(": Node where ")?;
    let alias = alias.trim();
    if alias.is_empty() {
        return None;
    }
    Some(GraphSeed {
        alias: alias.to_string(),
        filter: parse_predicate_expr(rest.trim())?,
    })
}

fn matching_close_paren(s: &str, open: usize) -> Option<usize> {
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

fn parse_aggregate_clause(s: &str) -> Option<AggregateClause> {
    if s.contains("count by kind") || s.contains("count by n.kind") {
        return Some(AggregateClause {
            group_by: vec![FieldRef::Kind],
            aggregate: AggregateExpr::Count,
        });
    }
    None
}

fn parse_return_clause_diagnostic(
    input: &str,
    normalized: &str,
) -> std::result::Result<Vec<FieldRef>, MmqlError> {
    let Some(start) = normalized.find(" return ") else {
        return Ok(Vec::new());
    };
    let raw = normalized[start + " return ".len()..].trim();
    if raw.is_empty() {
        return Err(diagnostic(
            input,
            "return clause must list at least one field",
            marker_span(input, "return").unwrap_or_else(|| fallback_span(input)),
        ));
    }
    raw.split(',')
        .map(|field| {
            parse_return_field(field.trim()).ok_or_else(|| {
                diagnostic(
                    input,
                    format!("unsupported return field `{}`", field.trim()),
                    marker_span(input, field.trim()).unwrap_or_else(|| fallback_span(input)),
                )
            })
        })
        .collect()
}

fn parse_return_field(raw: &str) -> Option<FieldRef> {
    match raw {
        "score" => return Some(FieldRef::Score),
        "n.id" | "n.node_id" => return Some(FieldRef::NodeId),
        _ => {}
    }
    let field = raw.strip_prefix("n.")?;
    field_ref_from_name(field)
}

fn parse_join_clauses_diagnostic(
    input: &str,
    normalized: &str,
) -> std::result::Result<Vec<JoinClause>, MmqlError> {
    let mut joins = Vec::new();
    let mut search_from = 0;
    while let Some(relative_start) = normalized[search_from..].find(" join ") {
        let join_start = search_from + relative_start;
        let body_start = join_start + " join ".len();
        let body_end = [" join ", " count ", " return "]
            .iter()
            .filter_map(|marker| {
                normalized[body_start..]
                    .find(marker)
                    .map(|idx| body_start + idx)
            })
            .min()
            .unwrap_or(normalized.len());
        joins.push(parse_join_clause_body(
            input,
            &normalized[body_start..body_end],
        )?);
        search_from = body_end;
    }
    Ok(joins)
}

fn parse_join_clause_body(input: &str, rest: &str) -> std::result::Result<JoinClause, MmqlError> {
    let alias_end = rest.find(": Node").ok_or_else(|| {
        diagnostic(
            input,
            "join clause must look like `join m: Node where m.kind in (...) on node_id`",
            marker_span(input, "join").unwrap_or_else(|| fallback_span(input)),
        )
    })?;
    let alias = rest[..alias_end].trim();
    if alias.is_empty() {
        return Err(diagnostic(
            input,
            "join clause must name a right-side alias",
            marker_span(input, "join").unwrap_or_else(|| fallback_span(input)),
        ));
    }
    let where_start = rest.find(" where ").ok_or_else(|| {
        diagnostic(
            input,
            "join clause must include a right-side `where` predicate",
            marker_span(input, "join").unwrap_or_else(|| fallback_span(input)),
        )
    })?;
    let on_start = rest.rfind(" on ").ok_or_else(|| {
        diagnostic(
            input,
            "join clause must include `on node_id` or `on n.field = alias.field`",
            marker_span(input, "join").unwrap_or_else(|| fallback_span(input)),
        )
    })?;
    if on_start <= where_start {
        return Err(diagnostic(
            input,
            "join clause `on` must follow the right-side `where` predicate",
            marker_span(input, " on ").unwrap_or_else(|| fallback_span(input)),
        ));
    }
    let kind_marker = format!("{alias}.kind in (");
    let kinds = parse_kind_list_diagnostic(input, rest, &kind_marker)?;
    let right_where = rest[where_start + " where ".len()..on_start].trim();
    let right_filter = parse_predicate_expr(right_where).ok_or_else(|| {
        diagnostic(
            input,
            "could not parse join right-side where predicate",
            marker_span(input, " where ").unwrap_or_else(|| fallback_span(input)),
        )
    })?;
    let on = parse_join_on(input, alias, rest[on_start + " on ".len()..].trim())?;
    Ok(JoinClause {
        alias: alias.to_string(),
        kinds,
        right_filter,
        on,
    })
}

fn parse_join_on(input: &str, alias: &str, raw: &str) -> std::result::Result<JoinKey, MmqlError> {
    if raw == "node_id" {
        return Ok(JoinKey::NodeId);
    }
    let Some((left, right)) = split_once_top_level_keyword(raw, " = ") else {
        return Err(diagnostic(
            input,
            "join clause must use `on node_id` or `on n.field = alias.field`",
            marker_span(input, " on ").unwrap_or_else(|| fallback_span(input)),
        ));
    };
    let left = parse_join_field_for_alias(left.trim(), "n").ok_or_else(|| {
        diagnostic(
            input,
            "left join field must use the recall alias `n`",
            marker_span(input, left.trim()).unwrap_or_else(|| fallback_span(input)),
        )
    })?;
    let right = parse_join_field_for_alias(right.trim(), alias).ok_or_else(|| {
        diagnostic(
            input,
            format!("right join field must use alias `{alias}`"),
            marker_span(input, right.trim()).unwrap_or_else(|| fallback_span(input)),
        )
    })?;
    if left != right {
        return Err(diagnostic(
            input,
            "field joins require matching left/right field names",
            marker_span(input, raw).unwrap_or_else(|| fallback_span(input)),
        ));
    }
    Ok(JoinKey::Field(left))
}

fn parse_join_field_for_alias(raw: &str, alias: &str) -> Option<FieldRef> {
    let field = raw.strip_prefix(&format!("{alias}."))?;
    field_ref_from_name(field)
}

fn parse_score_expr(s: &str) -> Option<ScoreExpr> {
    let rest = s.get(s.find("score by ")? + "score by ".len()..)?;
    if rest.starts_with("udf ") {
        return None;
    }
    let end = rest.find(" limit ").unwrap_or(rest.len());
    parse_score_expr_inner(rest[..end].trim())
}

fn parse_score_expr_inner(s: &str) -> Option<ScoreExpr> {
    let s = s.trim();
    if let Some(inner) = strip_outer_parens(s) {
        return parse_score_expr_inner(inner);
    }
    if let Some((left, right)) = split_once_top_level(s, '+') {
        return Some(ScoreExpr::Add(
            Box::new(parse_score_expr_inner(left.trim())?),
            Box::new(parse_score_expr_inner(right.trim())?),
        ));
    }
    if let Some((left, right)) = split_once_top_level(s, '*') {
        return Some(ScoreExpr::Mul(
            Box::new(parse_score_expr_inner(left.trim())?),
            Box::new(parse_score_expr_inner(right.trim())?),
        ));
    }
    match s.trim() {
        "similarity" => Some(ScoreExpr::Similarity),
        raw if raw.starts_with("decay(") => parse_decay_expr(raw),
        raw => raw.parse::<f32>().ok().map(ScoreExpr::Literal),
    }
}

fn strip_outer_parens(s: &str) -> Option<&str> {
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

fn parse_decay_expr(s: &str) -> Option<ScoreExpr> {
    let inner = s.strip_prefix("decay(")?.strip_suffix(')')?;
    let mut parts = inner.split(',').map(str::trim);
    let field = match parts.next()? {
        "n.created_at" => FieldRef::CreatedAtMs,
        "n.updated_at" => FieldRef::UpdatedAtMs,
        _ => return None,
    };
    let half_life = parts.next()?.strip_prefix("half_life = ")?.trim();
    Some(ScoreExpr::Decay {
        field,
        half_life_ms: parse_duration_ms(half_life)?,
    })
}

fn parse_duration_ms(s: &str) -> Option<i64> {
    let (digits, unit) = split_digits_unit(s)?;
    let value: i64 = digits.parse().ok()?;
    match unit {
        "ms" => Some(value),
        "s" => Some(value * 1_000),
        "m" => Some(value * 60 * 1_000),
        "h" => Some(value * 60 * 60 * 1_000),
        "d" => Some(value * 24 * 60 * 60 * 1_000),
        _ => None,
    }
}

fn split_digits_unit(s: &str) -> Option<(&str, &str)> {
    let split = s
        .char_indices()
        .find(|(_, ch)| !ch.is_ascii_digit())
        .map(|(idx, _)| idx)?;
    Some((&s[..split], &s[split..]))
}

fn split_once_top_level(s: &str, needle: char) -> Option<(&str, &str)> {
    let mut depth = 0_i32;
    for (idx, ch) in s.char_indices() {
        match ch {
            '(' => depth += 1,
            ')' => depth -= 1,
            c if c == needle && depth == 0 => return Some((&s[..idx], &s[idx + ch.len_utf8()..])),
            _ => {}
        }
    }
    None
}

fn parse_word_after(s: &str, marker: &str) -> Option<String> {
    let rest = &s[s.find(marker)? + marker.len()..];
    let end = rest
        .char_indices()
        .find(|(_, c)| !(c.is_ascii_alphanumeric() || *c == '_' || *c == '-'))
        .map(|(idx, _)| idx)
        .unwrap_or(rest.len());
    if end == 0 {
        None
    } else {
        Some(rest[..end].to_string())
    }
}

fn parse_kind_list_diagnostic(
    input: &str,
    normalized: &str,
    marker: &str,
) -> std::result::Result<Vec<NodeKind>, MmqlError> {
    let marker_start = normalized.find(marker).ok_or_else(|| {
        diagnostic(
            input,
            format!("query must contain `{marker}...)`"),
            marker_span(input, marker.trim_end_matches('(').trim())
                .or_else(|| marker_span(input, "n.kind"))
                .unwrap_or_else(|| fallback_span(input)),
        )
    })?;
    let rest = normalized
        .get(marker_start + marker.len()..)
        .ok_or_else(|| {
            diagnostic(
                input,
                "recall query must contain `n.kind in (...)`",
                marker_span(input, "n.kind").unwrap_or_else(|| fallback_span(input)),
            )
        })?;
    let end = rest.find(')').ok_or_else(|| {
        diagnostic(
            input,
            "recall query must contain a closed `n.kind in (...)` list",
            marker_span(input, "n.kind").unwrap_or_else(|| fallback_span(input)),
        )
    })?;
    let mut kinds = Vec::new();
    for raw in rest[..end].split(',') {
        let name = raw.trim();
        let kind = parse_kind(name).ok_or_else(|| {
            diagnostic(
                input,
                format!("unknown node kind `{name}`"),
                marker_span(input, name).unwrap_or_else(|| fallback_span(input)),
            )
        })?;
        kinds.push(kind);
    }
    Ok(kinds)
}

fn parse_kind(s: &str) -> Option<NodeKind> {
    match s {
        "Episode" => Some(NodeKind::Episode),
        "Fact" => Some(NodeKind::Fact),
        "Entity" => Some(NodeKind::Entity),
        "Artifact" => Some(NodeKind::Artifact),
        _ => None,
    }
}

fn parse_vector_ref(s: &str) -> Option<VectorRef> {
    parse_vector(s)
        .map(VectorRef::Vector)
        .or_else(|| parse_embed_text(s).map(VectorRef::Text))
}

fn parse_embed_text(s: &str) -> Option<String> {
    let marker = "similar to embed(";
    let rest = &s[s.find(marker)? + marker.len()..];
    let rest = rest.strip_prefix('"')?;
    let end = rest.find('"')?;
    Some(rest[..end].to_string())
}

fn parse_vector(s: &str) -> Option<Vec<f32>> {
    let marker = "similar to [";
    let rest = &s[s.find(marker)? + marker.len()..];
    let end = rest.find(']')?;
    rest[..end]
        .split(',')
        .map(|raw| raw.trim().parse::<f32>().ok())
        .collect()
}

fn current_time_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as i64)
        .unwrap_or(0)
}

fn duration_ms(raw: &str) -> Option<i64> {
    parse_duration_ms(raw)
}

fn invalid(msg: impl Into<String>) -> Error {
    Error::InvalidArgument(msg.into())
}

fn diagnostic(_input: &str, message: impl Into<String>, span: Range<usize>) -> MmqlError {
    MmqlError {
        message: message.into(),
        span,
    }
}

fn marker_span(input: &str, marker: &str) -> Option<Range<usize>> {
    let start = input.find(marker)?;
    Some(start..start + marker.len())
}

fn bracketed_span(
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

fn fallback_span(input: &str) -> Range<usize> {
    0..input.len().min(1)
}

#[cfg(test)]
mod tests {
    use super::*;
    use mmdb_core::NodeKind;
    use mmdb_query::{FieldRef, Literal, LogicalPlan, ModelId, Predicate, SortKey, VectorRef};

    #[test]
    fn parses_recall_vector_query() {
        let plan = parse(
            r#"
            recall n: Node
              where n.tenant = 7 and n.kind in (Episode, Fact)
              similar to [1.0, 0.0, 0.0] using model "text" topk 20
              limit 5
            "#,
        )
        .unwrap();

        assert_eq!(
            plan,
            LogicalPlan::TopK {
                input: Box::new(LogicalPlan::VectorSearch {
                    query: VectorRef::Vector(vec![1.0, 0.0, 0.0]),
                    k: 20,
                    filter: Some(Predicate::and([
                        Predicate::eq(FieldRef::Tenant, Literal::U32(7)),
                        Predicate::kind_in([NodeKind::Episode, NodeKind::Fact]),
                    ])),
                    model: ModelId::from("text"),
                }),
                k: 5,
                by: SortKey::ScoreDesc,
            }
        );
    }

    #[test]
    fn parses_embed_text_query() {
        let plan = parse(
            r#"
            recall n: Node
              where n.tenant = 7 and n.kind in (Fact)
              similar to embed("quant backtest") using model "text" topk 20
              limit 5
            "#,
        )
        .unwrap();

        assert_eq!(
            plan,
            LogicalPlan::TopK {
                input: Box::new(LogicalPlan::VectorSearch {
                    query: VectorRef::Text("quant backtest".to_string()),
                    k: 20,
                    filter: Some(Predicate::and([
                        Predicate::eq(FieldRef::Tenant, Literal::U32(7)),
                        Predicate::kind_in([NodeKind::Fact]),
                    ])),
                    model: ModelId::from("text"),
                }),
                k: 5,
                by: SortKey::ScoreDesc,
            }
        );
    }

    #[test]
    fn parses_relative_created_at_predicate() {
        let before = current_time_ms() - duration_ms("7d").unwrap();
        let plan = parse(
            r#"
            recall n: Node
              where n.tenant = 7 and n.created_at > now() - 7d and n.kind in (Fact)
              similar to [1.0, 0.0, 0.0] using model "text" topk 20
              limit 5
            "#,
        )
        .unwrap();
        let after = current_time_ms() - duration_ms("7d").unwrap();

        let LogicalPlan::TopK { input, .. } = plan else {
            panic!("expected topk");
        };
        let LogicalPlan::VectorSearch {
            filter: Some(Predicate::And(predicates)),
            ..
        } = *input
        else {
            panic!("expected vector search with filter");
        };
        let cutoff = predicates
            .iter()
            .find_map(|predicate| match predicate {
                Predicate::Gt(FieldRef::CreatedAtMs, Literal::I64(value)) => Some(*value),
                _ => None,
            })
            .expect("created_at cutoff");

        assert!((before..=after).contains(&cutoff));
    }

    #[test]
    fn parses_created_at_where_predicates() {
        let plan = parse(
            r#"
            recall n: Node
              where n.tenant = 7 and n.created_at > 1000 and n.created_at <= 5000 and n.kind in (Fact)
              similar to [1.0, 0.0, 0.0] using model "text" topk 20
              limit 5
            "#,
        )
        .unwrap();

        assert_eq!(
            plan,
            LogicalPlan::TopK {
                input: Box::new(LogicalPlan::VectorSearch {
                    query: VectorRef::Vector(vec![1.0, 0.0, 0.0]),
                    k: 20,
                    filter: Some(Predicate::and([
                        Predicate::eq(FieldRef::Tenant, Literal::U32(7)),
                        Predicate::kind_in([NodeKind::Fact]),
                        Predicate::Gt(FieldRef::CreatedAtMs, Literal::I64(1000)),
                        Predicate::Lte(FieldRef::CreatedAtMs, Literal::I64(5000)),
                    ])),
                    model: ModelId::from("text"),
                }),
                k: 5,
                by: SortKey::ScoreDesc,
            }
        );
    }

    #[test]
    fn parses_metadata_string_equality_predicates() {
        let plan = parse(
            r#"
            recall n: Node
              where n.tenant = 7 and n.name = "X" and n.kind in (Fact)
              similar to [1.0, 0.0, 0.0] using model "text" topk 20
              limit 5
            "#,
        )
        .unwrap();

        assert_eq!(
            plan,
            LogicalPlan::TopK {
                input: Box::new(LogicalPlan::VectorSearch {
                    query: VectorRef::Vector(vec![1.0, 0.0, 0.0]),
                    k: 20,
                    filter: Some(Predicate::and([
                        Predicate::eq(FieldRef::Tenant, Literal::U32(7)),
                        Predicate::kind_in([NodeKind::Fact]),
                        Predicate::eq(
                            FieldRef::Metadata("name".to_string()),
                            Literal::String("X".to_string()),
                        ),
                    ])),
                    model: ModelId::from("text"),
                }),
                k: 5,
                by: SortKey::ScoreDesc,
            }
        );
    }

    #[test]
    fn parses_boolean_where_predicate_expression() {
        let plan = parse(
            r#"
            recall n: Node
              where n.tenant = 7 and (n.kind in (Fact) or n.topic = "revenue") and not n.archived = "true" and n.created_at >= 1000
              similar to [1.0, 0.0, 0.0] using model "text" topk 20
              limit 5
            "#,
        )
        .unwrap();

        assert_eq!(
            plan,
            LogicalPlan::TopK {
                input: Box::new(LogicalPlan::VectorSearch {
                    query: VectorRef::Vector(vec![1.0, 0.0, 0.0]),
                    k: 20,
                    filter: Some(Predicate::and([
                        Predicate::eq(FieldRef::Tenant, Literal::U32(7)),
                        Predicate::Or(vec![
                            Predicate::kind_in([NodeKind::Fact]),
                            Predicate::eq(
                                FieldRef::Metadata("topic".to_string()),
                                Literal::String("revenue".to_string()),
                            ),
                        ]),
                        Predicate::Not(Box::new(Predicate::eq(
                            FieldRef::Metadata("archived".to_string()),
                            Literal::String("true".to_string()),
                        ))),
                        Predicate::Gte(FieldRef::CreatedAtMs, Literal::I64(1000)),
                    ])),
                    model: ModelId::from("text"),
                }),
                k: 5,
                by: SortKey::ScoreDesc,
            }
        );
    }

    #[test]
    fn parses_graph_expand_and_udf_score_clause() {
        let plan = parse(
            r#"
            recall n: Node
              where n.tenant = 7 and n.kind in (Fact)
              similar to [1.0, 0.0, 0.0] using model "text" topk 20
              connected via related depth 2
              score by udf "boost"
              limit 5
            "#,
        )
        .unwrap();

        assert_eq!(
            plan,
            LogicalPlan::TopK {
                input: Box::new(LogicalPlan::Udf {
                    input: Box::new(LogicalPlan::GraphExpand {
                        from: Box::new(LogicalPlan::VectorSearch {
                            query: VectorRef::Vector(vec![1.0, 0.0, 0.0]),
                            k: 20,
                            filter: Some(Predicate::and([
                                Predicate::eq(FieldRef::Tenant, Literal::U32(7)),
                                Predicate::kind_in([NodeKind::Fact]),
                            ])),
                            model: ModelId::from("text"),
                        }),
                        relation: Some("related".to_string()),
                        depth: 2,
                    }),
                    name: "boost".to_string(),
                    args: vec![mmdb_query::Expr::Field(FieldRef::Score)],
                }),
                k: 5,
                by: SortKey::ScoreDesc,
            }
        );
    }

    #[test]
    fn parses_connected_from_subquery_as_graph_filter() {
        let plan = parse(
            r#"
            recall n: Node
              where n.tenant = 7 and n.kind in (Fact)
              similar to [1.0, 0.0, 0.0] using model "text" topk 20
              connected from (u: Node where u.kind in (Entity) and u.name = "X") via mentions depth 1
              limit 5
            "#,
        )
        .unwrap();

        assert_eq!(
            plan,
            LogicalPlan::TopK {
                input: Box::new(LogicalPlan::Join {
                    left: Box::new(LogicalPlan::VectorSearch {
                        query: VectorRef::Vector(vec![1.0, 0.0, 0.0]),
                        k: 20,
                        filter: Some(Predicate::and([
                            Predicate::eq(FieldRef::Tenant, Literal::U32(7)),
                            Predicate::kind_in([NodeKind::Fact]),
                        ])),
                        model: ModelId::from("text"),
                    }),
                    right: Box::new(LogicalPlan::GraphExpand {
                        from: Box::new(LogicalPlan::Scan {
                            table: mmdb_query::TableId::Nodes,
                            filter: Some(Predicate::and([
                                Predicate::eq(FieldRef::Tenant, Literal::U32(7)),
                                Predicate::kind_in([NodeKind::Entity]),
                                Predicate::eq(
                                    FieldRef::Metadata("name".to_string()),
                                    Literal::String("X".to_string()),
                                ),
                            ])),
                        }),
                        relation: Some("mentions".to_string()),
                        depth: 1,
                    }),
                    on: mmdb_query::JoinKey::NodeId,
                }),
                k: 5,
                by: SortKey::ScoreDesc,
            }
        );
    }

    #[test]
    fn parse_ast_preserves_recall_clauses_before_lowering() {
        let query = parse_ast(
            r#"
            recall n: Node
              where n.tenant = 7 and n.kind in (Episode, Fact)
              similar to [1.0, 0.0, 0.0] using model "text" topk 20
              connected via related depth 2
              score by udf "boost"
              limit 5
            "#,
        )
        .unwrap();

        assert_eq!(
            query,
            RecallQuery {
                tenant: 7,
                kinds: vec![NodeKind::Episode, NodeKind::Fact],
                created_at_predicates: Vec::new(),
                metadata_predicates: Vec::new(),
                where_predicate: None,
                query: VectorRef::Vector(vec![1.0, 0.0, 0.0]),
                model: "text".to_string(),
                topk: 20,
                limit: 5,
                graph: Some(GraphClause {
                    relation: "related".to_string(),
                    depth: 2,
                    from: None,
                }),
                udf: Some(UdfClause {
                    name: "boost".to_string(),
                }),
                score: None,
                joins: Vec::new(),
                aggregate: None,
                return_fields: Vec::new(),
            }
        );
    }

    #[test]
    fn parses_similarity_decay_score_expression() {
        let plan = parse(
            r#"
            recall n: Node
              where n.tenant = 7 and n.kind in (Fact)
              similar to [1.0, 0.0, 0.0] using model "text" topk 20
              score by similarity * decay(n.created_at, half_life = 3d)
              limit 5
            "#,
        )
        .unwrap();

        assert_eq!(
            plan,
            LogicalPlan::TopK {
                input: Box::new(LogicalPlan::Score {
                    input: Box::new(LogicalPlan::VectorSearch {
                        query: VectorRef::Vector(vec![1.0, 0.0, 0.0]),
                        k: 20,
                        filter: Some(Predicate::and([
                            Predicate::eq(FieldRef::Tenant, Literal::U32(7)),
                            Predicate::kind_in([NodeKind::Fact]),
                        ])),
                        model: ModelId::from("text"),
                    }),
                    expr: mmdb_query::ScoreExpr::Mul(
                        Box::new(mmdb_query::ScoreExpr::Similarity),
                        Box::new(mmdb_query::ScoreExpr::Decay {
                            field: FieldRef::CreatedAtMs,
                            half_life_ms: 3 * 24 * 60 * 60 * 1_000,
                        }),
                    ),
                }),
                k: 5,
                by: SortKey::ScoreDesc,
            }
        );
    }

    #[test]
    fn parses_parenthesized_score_expression_with_literal() {
        let plan = parse(
            r#"
            recall n: Node
              where n.tenant = 7 and n.kind in (Fact)
              similar to [1.0, 0.0, 0.0] using model "text" topk 20
              score by (similarity + 0.25) * decay(n.created_at, half_life = 3d)
              limit 5
            "#,
        )
        .unwrap();

        assert_eq!(
            plan,
            LogicalPlan::TopK {
                input: Box::new(LogicalPlan::Score {
                    input: Box::new(LogicalPlan::VectorSearch {
                        query: VectorRef::Vector(vec![1.0, 0.0, 0.0]),
                        k: 20,
                        filter: Some(Predicate::and([
                            Predicate::eq(FieldRef::Tenant, Literal::U32(7)),
                            Predicate::kind_in([NodeKind::Fact]),
                        ])),
                        model: ModelId::from("text"),
                    }),
                    expr: mmdb_query::ScoreExpr::Mul(
                        Box::new(mmdb_query::ScoreExpr::Add(
                            Box::new(mmdb_query::ScoreExpr::Similarity),
                            Box::new(mmdb_query::ScoreExpr::Literal(0.25)),
                        )),
                        Box::new(mmdb_query::ScoreExpr::Decay {
                            field: FieldRef::CreatedAtMs,
                            half_life_ms: 3 * 24 * 60 * 60 * 1_000,
                        }),
                    ),
                }),
                k: 5,
                by: SortKey::ScoreDesc,
            }
        );
    }

    #[test]
    fn parses_count_by_kind_aggregation() {
        let plan = parse(
            r#"
            recall n: Node
              where n.tenant = 7 and n.kind in (Fact, Episode)
              similar to [1.0, 0.0, 0.0] using model "text" topk 20
              limit 5
              count by kind
            "#,
        )
        .unwrap();

        assert_eq!(
            plan,
            LogicalPlan::Aggregate {
                input: Box::new(LogicalPlan::TopK {
                    input: Box::new(LogicalPlan::VectorSearch {
                        query: VectorRef::Vector(vec![1.0, 0.0, 0.0]),
                        k: 20,
                        filter: Some(Predicate::and([
                            Predicate::eq(FieldRef::Tenant, Literal::U32(7)),
                            Predicate::kind_in([NodeKind::Fact, NodeKind::Episode]),
                        ])),
                        model: ModelId::from("text"),
                    }),
                    k: 5,
                    by: SortKey::ScoreDesc,
                }),
                group_by: vec![FieldRef::Kind],
                aggregate: mmdb_query::AggregateExpr::Count,
            }
        );
    }

    #[test]
    fn parses_join_clause_on_node_id() {
        let plan = parse(
            r#"
            recall n: Node
              where n.tenant = 7 and n.kind in (Fact, Episode)
              similar to [1.0, 0.0, 0.0] using model "text" topk 20
              limit 5
              join m: Node where m.kind in (Fact) on node_id
            "#,
        )
        .unwrap();

        assert_eq!(
            plan,
            LogicalPlan::Join {
                left: Box::new(LogicalPlan::TopK {
                    input: Box::new(LogicalPlan::VectorSearch {
                        query: VectorRef::Vector(vec![1.0, 0.0, 0.0]),
                        k: 20,
                        filter: Some(Predicate::and([
                            Predicate::eq(FieldRef::Tenant, Literal::U32(7)),
                            Predicate::kind_in([NodeKind::Fact, NodeKind::Episode]),
                        ])),
                        model: ModelId::from("text"),
                    }),
                    k: 5,
                    by: SortKey::ScoreDesc,
                }),
                right: Box::new(LogicalPlan::Scan {
                    table: mmdb_query::TableId::Nodes,
                    filter: Some(Predicate::and([
                        Predicate::eq(FieldRef::Tenant, Literal::U32(7)),
                        Predicate::kind_in([NodeKind::Fact]),
                    ])),
                }),
                on: mmdb_query::JoinKey::NodeId,
            }
        );
    }

    #[test]
    fn parses_join_clause_on_matching_metadata_field() {
        let plan = parse(
            r#"
            recall n: Node
              where n.tenant = 7 and n.kind in (Fact)
              similar to [1.0, 0.0, 0.0] using model "text" topk 20
              limit 5
              join m: Node where m.kind in (Entity) and m.topic = "revenue" on n.topic = m.topic
            "#,
        )
        .unwrap();

        assert_eq!(
            plan,
            LogicalPlan::Join {
                left: Box::new(LogicalPlan::TopK {
                    input: Box::new(LogicalPlan::VectorSearch {
                        query: VectorRef::Vector(vec![1.0, 0.0, 0.0]),
                        k: 20,
                        filter: Some(Predicate::and([
                            Predicate::eq(FieldRef::Tenant, Literal::U32(7)),
                            Predicate::kind_in([NodeKind::Fact]),
                        ])),
                        model: ModelId::from("text"),
                    }),
                    k: 5,
                    by: SortKey::ScoreDesc,
                }),
                right: Box::new(LogicalPlan::Scan {
                    table: mmdb_query::TableId::Nodes,
                    filter: Some(Predicate::and([
                        Predicate::eq(FieldRef::Tenant, Literal::U32(7)),
                        Predicate::kind_in([NodeKind::Entity]),
                        Predicate::eq(
                            FieldRef::Metadata("topic".to_string()),
                            Literal::String("revenue".to_string()),
                        ),
                    ])),
                }),
                on: mmdb_query::JoinKey::Field(FieldRef::Metadata("topic".to_string())),
            }
        );
    }

    #[test]
    fn parses_multiple_join_clauses_in_order() {
        let plan = parse(
            r#"
            recall n: Node
              where n.tenant = 7 and n.kind in (Fact)
              similar to [1.0, 0.0, 0.0] using model "text" topk 20
              limit 5
              join m: Node where m.kind in (Entity) on n.topic = m.topic
              join p: Node where p.kind in (Artifact) on node_id
            "#,
        )
        .unwrap();

        let left = LogicalPlan::TopK {
            input: Box::new(LogicalPlan::VectorSearch {
                query: VectorRef::Vector(vec![1.0, 0.0, 0.0]),
                k: 20,
                filter: Some(Predicate::and([
                    Predicate::eq(FieldRef::Tenant, Literal::U32(7)),
                    Predicate::kind_in([NodeKind::Fact]),
                ])),
                model: ModelId::from("text"),
            }),
            k: 5,
            by: SortKey::ScoreDesc,
        };

        let first_join = LogicalPlan::Join {
            left: Box::new(left),
            right: Box::new(LogicalPlan::Scan {
                table: mmdb_query::TableId::Nodes,
                filter: Some(Predicate::and([
                    Predicate::eq(FieldRef::Tenant, Literal::U32(7)),
                    Predicate::kind_in([NodeKind::Entity]),
                ])),
            }),
            on: mmdb_query::JoinKey::Field(FieldRef::Metadata("topic".to_string())),
        };

        assert_eq!(
            plan,
            LogicalPlan::Join {
                left: Box::new(first_join),
                right: Box::new(LogicalPlan::Scan {
                    table: mmdb_query::TableId::Nodes,
                    filter: Some(Predicate::and([
                        Predicate::eq(FieldRef::Tenant, Literal::U32(7)),
                        Predicate::kind_in([NodeKind::Artifact]),
                    ])),
                }),
                on: mmdb_query::JoinKey::NodeId,
            }
        );
    }

    #[test]
    fn parses_return_projection_clause() {
        let plan = parse(
            r#"
            recall n: Node
              where n.tenant = 7 and n.kind in (Fact)
              similar to [1.0, 0.0, 0.0] using model "text" topk 20
              limit 5
              return n.id, n.content, score
            "#,
        )
        .unwrap();

        assert_eq!(
            plan,
            LogicalPlan::Project {
                input: Box::new(LogicalPlan::TopK {
                    input: Box::new(LogicalPlan::VectorSearch {
                        query: VectorRef::Vector(vec![1.0, 0.0, 0.0]),
                        k: 20,
                        filter: Some(Predicate::and([
                            Predicate::eq(FieldRef::Tenant, Literal::U32(7)),
                            Predicate::kind_in([NodeKind::Fact]),
                        ])),
                        model: ModelId::from("text"),
                    }),
                    k: 5,
                    by: SortKey::ScoreDesc,
                }),
                fields: vec![FieldRef::NodeId, FieldRef::Content, FieldRef::Score],
            }
        );
    }

    #[test]
    fn resolver_rejects_unknown_model_or_udf_before_lowering() {
        let input = r#"
            recall n: Node
              where n.tenant = 7 and n.kind in (Fact)
              similar to [1.0, 0.0, 0.0] using model "image" topk 20
              score by udf "boost"
              limit 5
            "#;
        let resolver = Resolver::default().allow_model("text").allow_udf("boost");

        let err = parse_with_resolver(input, &resolver).unwrap_err();

        assert!(format!("{err}").contains("unknown model"));

        let input = r#"
            recall n: Node
              where n.tenant = 7 and n.kind in (Fact)
              similar to [1.0, 0.0, 0.0] using model "text" topk 20
              score by udf "boost"
              limit 5
            "#;
        assert!(parse_with_resolver(input, &resolver).is_ok());
    }

    #[test]
    fn diagnostic_parser_reports_missing_clause_span() {
        let input = r#"
            recall n: Node
              where n.tenant = 7 and n.kind in (Fact)
              similar to [1.0, 0.0, 0.0] topk 20
              limit 5
            "#;

        let err = parse_ast_diagnostic(input).unwrap_err();

        assert!(err.message.contains("using model"));
        assert_eq!(&input[err.span.clone()], "similar to [1.0, 0.0, 0.0]");
    }

    #[test]
    fn diagnostic_parser_reports_bad_kind_span() {
        let input = r#"
            recall n: Node
              where n.tenant = 7 and n.kind in (Memory)
              similar to [1.0, 0.0, 0.0] using model "text" topk 20
              limit 5
            "#;

        let err = parse_ast_diagnostic(input).unwrap_err();

        assert!(err.message.contains("unknown node kind"));
        assert_eq!(&input[err.span], "Memory");
    }
}
