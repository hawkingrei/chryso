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

    #[test]
    fn cost_config_allows_deterministic_preference() {
        let sql = "select t1.id from t1 join t2 on t1.id = t2.id";
        let parser = SimpleParser::new(ParserConfig {
            dialect: Dialect::Postgres,
        });
        let stmt = parser.parse(sql).expect("parse");
        let logical = PlanBuilder::build(stmt).expect("plan");
        let mut prefer_hash = OptimizerConfig::default();
        prefer_hash.cost_config = Some(chryso::optimizer::CostModelConfig {
            scan: 1.0,
            filter: 1.0,
            projection: 0.1,
            join: 10.0,
            sort: 0.2,
            aggregate: 1.0,
            limit: 0.05,
            derived: 0.1,
            dml: 1.0,
            join_hash_multiplier: 1.0,
            join_nested_multiplier: 10.0,
            max_cost: 1.0e9,
        });
        let optimizer = CascadesOptimizer::new(prefer_hash);
        let mut stats = StatsCache::new();
        let physical = optimizer.optimize(&logical, &mut stats);
        let explain = physical.explain(0);
        assert!(explain.contains("algorithm=Hash"));

        let mut prefer_nested = OptimizerConfig::default();
        prefer_nested.cost_config = Some(chryso::optimizer::CostModelConfig {
            scan: 1.0,
            filter: 1.0,
            projection: 0.1,
            join: 10.0,
            sort: 0.2,
            aggregate: 1.0,
            limit: 0.05,
            derived: 0.1,
            dml: 1.0,
            join_hash_multiplier: 10.0,
            join_nested_multiplier: 1.0,
            max_cost: 1.0e9,
        });
        let optimizer = CascadesOptimizer::new(prefer_nested);
        let physical = optimizer.optimize(&logical, &mut stats);
        let explain = physical.explain(0);
        assert!(explain.contains("algorithm=NestedLoop"));
    }
}
