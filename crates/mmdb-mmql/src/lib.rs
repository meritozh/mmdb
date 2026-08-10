//! Minimal MMQL parser, resolver, and lowering to `mmdb-query::LogicalPlan`.

mod ast;
mod parser;
#[cfg(test)]
mod tests;
mod util;

pub use ast::{
    AggregateClause, GraphClause, GraphSeed, JoinClause, RecallQuery, Resolver, UdfClause,
};
pub use parser::{parse, parse_ast, parse_ast_diagnostic, parse_with_resolver};
pub use util::MmqlError;
