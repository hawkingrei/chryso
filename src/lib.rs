pub mod config;
pub mod adapter {
    pub use corundum_adapter::*;
}
pub mod ast {
    pub use corundum_core::ast::*;
}
pub mod diagnostics {
    pub use corundum_core::diagnostics::*;
}
pub mod error {
    pub use corundum_core::error::*;
}
pub mod metadata {
    pub use corundum_metadata::*;
}
pub mod optimizer {
    pub use corundum_optimizer::*;
}
pub mod parser {
    pub use corundum_parser::*;
}
pub mod planner {
    pub use corundum_planner::*;
}
pub mod plan_diff {
    pub use corundum_planner::plan_diff::*;
}
pub mod serde {
    pub use corundum_planner::serde::*;
}
pub mod sql_format {
    pub use corundum_core::sql_format::*;
}
#[cfg(any(test, feature = "test-utils"))]
pub mod test_support;

pub use adapter::{
    AdapterCapabilities, DuckDbAdapter, ExecutorAdapter, MockAdapter, ParamValue, QueryResult,
};
pub use ast::{Expr, Statement};
pub use error::{CorundumError, CorundumResult};
pub use optimizer::{CascadesOptimizer, OptimizerConfig};
pub use parser::{Dialect, ParserConfig, SqlParser};
pub use planner::{LogicalPlan, PhysicalPlan, PlanBuilder};
