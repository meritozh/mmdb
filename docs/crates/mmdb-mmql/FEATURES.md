# mmdb-mmql Features

`mmdb-mmql` parses a compact memory-recall DSL and lowers it into
`mmdb-query::LogicalPlan`.

## Responsibilities

- Parse recall queries into a typed AST.
- Validate optional model and UDF names through `Resolver`.
- Lower AST to `LogicalPlan`.
- Return diagnostic errors with byte spans for common parse failures.

## Entry Points

- `parse`
- `parse_with_resolver`
- `parse_ast`
- `parse_ast_diagnostic`

`parse_ast` returns `RecallQuery`. `RecallQuery::lower` produces a
`LogicalPlan`.

## Supported Query Shape

MMQL supports vector recall:

```mmql
recall n: Node
  where n.tenant = 0 and n.kind in (Episode, Fact)
  similar to [1.0, 0.0, 0.0] using model "text" topk 20
  limit 5
  return n.id, n.content, score
```

It also supports embedding text:

```mmql
similar to embed("quarterly revenue") using model "text" topk 20
```

The facade resolves `VectorRef::Text` through its configured embedder during
source-backed execution.

## Where Clauses

Supported predicates include:

- `n.tenant = <u32>`
- `n.kind in (...)`
- `n.created_at >/>=/</<= <epoch_ms>`
- `now() - <duration>` relative time
- metadata string equality such as `n.session = "s-001"`
- boolean `and`, `or`, and `not`

## Graph, Score, Aggregate, Join, Project

Graph expansion:

```mmql
connected via related depth 2
```

Graph seed filtering:

```mmql
connected from (u: Node where u.name = "Alice") via mentions depth 1
```

Score expressions:

```mmql
score by (similarity + 0.25) * decay(n.created_at, half_life = 3d)
```

UDF score hook:

```mmql
score by udf "boost"
```

Aggregation:

```mmql
count by kind
```

Joins:

```mmql
join m: Node where m.kind in (Entity) on node_id
join p: Node where p.kind in (Fact) on n.topic = p.topic
```

Projection:

```mmql
return n.id, n.content, score
```

## Resolver

`Resolver` can allow-list known model names and UDF names before lowering. This
keeps parser output separate from environment-specific availability checks.

## Source Files

- `crates/mmdb-mmql/src/parser.rs`: parser and diagnostics.
- `crates/mmdb-mmql/src/ast.rs`: AST, resolver, lowering.
- `crates/mmdb-mmql/src/util.rs`: error helper.

