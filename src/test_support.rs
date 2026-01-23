use crate::adapter::{ExecutorAdapter, MockAdapter, QueryResult};
use crate::error::ChrysoResult;
use crate::metadata::StatsCache;
use crate::optimizer::{CascadesOptimizer, OptimizerConfig};
use crate::parser::{Dialect, ParserConfig, SimpleParser, SqlParser};
use crate::planner::PlanBuilder;

pub struct TestRun {
    pub logical_explain: String,
    pub physical_explain: String,
    pub result: QueryResult,
}

pub fn execute_with_adapter<A: ExecutorAdapter>(
    sql: &str,
    dialect: Dialect,
    adapter: &A,
) -> ChrysoResult<TestRun> {
    let parser = SimpleParser::new(ParserConfig { dialect });
    let statement = parser.parse(sql)?;
    let logical = PlanBuilder::build(statement)?;
    let optimizer = CascadesOptimizer::new(OptimizerConfig::default());
    let mut stats = StatsCache::new();
    let physical = optimizer.optimize(&logical, &mut stats);
    let result = adapter.execute(&physical)?;
    Ok(TestRun {
        logical_explain: logical.explain(0),
        physical_explain: physical.explain(0),
        result,
    })
}

pub fn execute(sql: &str, dialect: Dialect) -> ChrysoResult<TestRun> {
    let adapter = MockAdapter::new();
    execute_with_adapter(sql, dialect, &adapter)
}

pub fn explain(sql: &str, dialect: Dialect) -> ChrysoResult<(String, String)> {
    let parser = SimpleParser::new(ParserConfig { dialect });
    let statement = parser.parse(sql)?;
    let logical = PlanBuilder::build(statement)?;
    let optimizer = CascadesOptimizer::new(OptimizerConfig::default());
    let mut stats = StatsCache::new();
    let physical = optimizer.optimize(&logical, &mut stats);
    Ok((logical.explain(0), physical.explain(0)))
}

#[cfg(test)]
mod tests {
    use super::{execute, explain};
    use crate::parser::Dialect;

    #[test]
    fn pipeline_executes_and_explains() {
        let sql = "select id from users where id = 1";
        let run = execute(sql, Dialect::Postgres).expect("execute");
        assert!(run.logical_explain.contains("LogicalProject"));
        assert!(run.physical_explain.contains("Project"));
        assert_eq!(run.result.rows.len(), 1);

        let (logical, physical) = explain(sql, Dialect::Postgres).expect("explain");
        assert!(logical.contains("LogicalScan"));
        assert!(physical.contains("TableScan"));
    }
}
