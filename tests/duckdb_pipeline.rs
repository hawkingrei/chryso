#[cfg(feature = "duckdb")]
mod tests {
    use chryso::metadata::StatsCache;
    use chryso::optimizer::{CascadesOptimizer, OptimizerConfig};
    use chryso::parser::{Dialect, ParserConfig, SimpleParser, SqlParser};
    use chryso::planner::PlanBuilder;
    use chryso::{DuckDbAdapter, ExecutorAdapter};

    #[test]
    fn duckdb_pipeline_executes_optimized_plan() {
        let adapter = DuckDbAdapter::try_new().expect("duckdb adapter");
        adapter
            .execute_sql("create table sales (id integer, amount integer, region varchar)")
            .expect("create table");
        adapter
            .execute_sql(
                "insert into sales values (1, 10, 'us'), (1, 5, 'us'), (2, 7, 'us'), (2, 1, 'eu')",
            )
            .expect("insert");

        let sql = "select id from sales where region = 'us' order by id limit 2";
        let parser = SimpleParser::new(ParserConfig {
            dialect: Dialect::Postgres,
        });
        let stmt = parser.parse(sql).expect("parse");
        let logical = PlanBuilder::build(stmt).expect("plan");
        let optimizer = CascadesOptimizer::new(OptimizerConfig::default());
        let mut stats = StatsCache::new();
        let physical = optimizer.optimize(&logical, &mut stats);
        let result = adapter.execute(&physical).expect("execute");

        assert_eq!(result.columns.len(), 1);
        assert_eq!(result.rows.len(), 2);
        assert_eq!(result.rows[0][0], "1");
        assert_eq!(result.rows[1][0], "1");
    }
}
