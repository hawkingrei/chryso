pub mod config;
pub mod adapter {
    pub use chryso_adapter::*;
}
#[cfg(feature = "duckdb")]
pub mod adapter_duckdb {
    pub use chryso_adapter_duckdb::*;
}
#[cfg(feature = "velox")]
pub mod adapter_velox {
    pub use chryso_adapter_velox::*;
}
#[cfg(feature = "duckdb-ops")]
pub mod adapter_duckdb_ops {
    pub use chryso_adapter_duckdb_ops::*;
}
pub mod ast {
    pub use chryso_core::ast::*;
}
pub mod diagnostics {
    pub use chryso_core::diagnostics::*;
}
pub mod error {
    pub use chryso_core::error::*;
}
pub mod metadata {
    pub use chryso_metadata::*;
}
pub mod optimizer {
    pub use chryso_optimizer::*;
}
pub mod parser {
    pub use chryso_parser::*;
}
pub mod planner {
    pub use chryso_planner::*;
}
pub mod plan_diff {
    pub use chryso_planner::plan_diff::*;
}
pub mod serde {
    pub use chryso_planner::serde::*;
}
pub mod sql_format {
    pub use chryso_core::sql_format::*;
}
pub mod statement {
    pub use chryso_core::statement::*;
}
pub mod pipeline;
pub mod session;
pub mod sql_utils;
#[cfg(any(test, feature = "test-utils"))]
pub mod test_support;

pub use adapter::{AdapterCapabilities, ExecutorAdapter, MockAdapter, ParamValue, QueryResult};
#[cfg(feature = "duckdb")]
pub use adapter_duckdb::DuckDbAdapter;
#[cfg(feature = "duckdb-ops")]
pub use adapter_duckdb_ops::DuckDbOpsAdapter;
#[cfg(feature = "velox")]
pub use adapter_velox::VeloxAdapter;
pub use ast::{Expr, Statement, StatementCategory};
pub use error::{ChrysoError, ChrysoResult};
pub use optimizer::{CascadesOptimizer, OptimizerConfig};
pub use parser::{Dialect, ParserConfig, SqlParser};
pub use pipeline::{Authorizer, DdlHandler, DdlResult, PlanOutcome, plan_with_hooks};
pub use planner::{LogicalPlan, PhysicalPlan, PlanBuilder};
pub use session::SessionContext;
pub use statement::{NoExtension, StatementContext, StatementEnvelope};
