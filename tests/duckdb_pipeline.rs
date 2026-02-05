#[cfg(feature = "duckdb")]
mod tests {
    use chryso::metadata::StatsCache;
    use chryso::optimizer::{CascadesOptimizer, OptimizerConfig};
    use chryso::parser::{Dialect, ParserConfig, SimpleParser, SqlParser};
    use chryso::planner::PlanBuilder;
    use chryso::{DuckDbAdapter, ExecutorAdapter};
    use chryso_parser_yacc::YaccParser;

    fn build_physical(parser: &dyn SqlParser, sql: &str) -> chryso::planner::PhysicalPlan {
        let stmt = parser.parse(sql).expect("parse");
        let logical = PlanBuilder::build(stmt).expect("plan");
        let optimizer = CascadesOptimizer::new(OptimizerConfig::default());
        let mut stats = StatsCache::new();
        optimizer.optimize(&logical, &mut stats)
    }

    fn setup_sales_table(adapter: &DuckDbAdapter) {
        adapter
            .execute_sql("create table sales (id integer, amount integer, region varchar)")
            .expect("create table");
        adapter
            .execute_sql(
                "insert into sales values (1, 10, 'us'), (1, 5, 'us'), (2, 7, 'us'), (2, 1, 'eu')",
            )
            .expect("insert");
    }

    fn execute_with_parser(
        adapter: &DuckDbAdapter,
        parser: &dyn SqlParser,
        sql: &str,
    ) -> chryso::QueryResult {
        let physical = build_physical(parser, sql);
        adapter.execute(&physical).expect("execute")
    }

    fn execute_dml_roundtrip(parser: &dyn SqlParser) -> chryso::QueryResult {
        let adapter = DuckDbAdapter::try_new().expect("duckdb adapter");
        adapter
            .execute_sql("create table t1 (id integer, amount integer)")
            .expect("create table");
        let insert_sql = "insert into t1 values (1, 10)";
        let select_sql = "select count(*) from t1";
        let _ = execute_with_parser(&adapter, parser, insert_sql);
        execute_with_parser(&adapter, parser, select_sql)
    }

    #[test]
    fn duckdb_pipeline_executes_optimized_plan() {
        let adapter = DuckDbAdapter::try_new().expect("duckdb adapter");
        setup_sales_table(&adapter);

        let sql = "select id from sales where region = 'us' order by id limit 2";
        let parser = SimpleParser::new(ParserConfig {
            dialect: Dialect::Postgres,
        });
        let result = execute_with_parser(&adapter, &parser, sql);

        assert_eq!(result.columns.len(), 1);
        assert_eq!(result.rows.len(), 2);
        assert_eq!(result.rows[0][0], "1");
        assert_eq!(result.rows[1][0], "1");

        let yacc_parser = YaccParser::new(ParserConfig {
            dialect: Dialect::Postgres,
        });
        let result = execute_with_parser(&adapter, &yacc_parser, sql);

        assert_eq!(result.columns.len(), 1);
        assert_eq!(result.rows.len(), 2);
        assert_eq!(result.rows[0][0], "1");
        assert_eq!(result.rows[1][0], "1");
    }

    #[test]
    fn cost_config_allows_deterministic_preference() {
        let sql = "select t1.id from t1 join t2 on t1.id = t2.id";
        let make_config = |join_hash_multiplier: f64, join_nested_multiplier: f64| {
            let mut config = OptimizerConfig::default();
            config.cost_config = Some(chryso::optimizer::CostModelConfig {
                scan: 1.0,
                filter: 1.0,
                projection: 0.1,
                join: 10.0,
                sort: 0.2,
                aggregate: 1.0,
                limit: 0.05,
                derived: 0.1,
                dml: 1.0,
                join_hash_multiplier,
                join_nested_multiplier,
                max_cost: 1.0e9,
            });
            config
        };
        let parser = SimpleParser::new(ParserConfig {
            dialect: Dialect::Postgres,
        });
        let stmt = parser.parse(sql).expect("parse");
        let logical = PlanBuilder::build(stmt).expect("plan");
        let optimizer = CascadesOptimizer::new(make_config(1.0, 10.0));
        let mut stats = StatsCache::new();
        let physical = optimizer.optimize(&logical, &mut stats);
        let explain = physical.explain(0);
        assert!(explain.contains("algorithm=Hash"));

        let optimizer = CascadesOptimizer::new(make_config(10.0, 1.0));
        let physical = optimizer.optimize(&logical, &mut stats);
        let explain = physical.explain(0);
        assert!(explain.contains("algorithm=NestedLoop"));

        let yacc_parser = YaccParser::new(ParserConfig {
            dialect: Dialect::Postgres,
        });
        let stmt = yacc_parser.parse(sql).expect("parse");
        let logical = PlanBuilder::build(stmt).expect("plan");
        let optimizer = CascadesOptimizer::new(make_config(10.0, 1.0));
        let physical = optimizer.optimize(&logical, &mut stats);
        let explain = physical.explain(0);
        assert!(explain.contains("algorithm=NestedLoop"));
    }

    #[test]
    fn duckdb_pipeline_executes_derived_subquery() {
        let adapter = DuckDbAdapter::try_new().expect("duckdb adapter");
        setup_sales_table(&adapter);
        let sql =
            "select id from (select id from sales where region = 'us') as t order by id limit 1";

        let parser = SimpleParser::new(ParserConfig {
            dialect: Dialect::Postgres,
        });
        let result = execute_with_parser(&adapter, &parser, sql);
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0][0], "1");

        let yacc_parser = YaccParser::new(ParserConfig {
            dialect: Dialect::Postgres,
        });
        let result = execute_with_parser(&adapter, &yacc_parser, sql);
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0][0], "1");
    }

    #[test]
    fn duckdb_pipeline_executes_distinct() {
        let adapter = DuckDbAdapter::try_new().expect("duckdb adapter");
        setup_sales_table(&adapter);
        let sql = "select distinct region from sales order by region";

        let parser = SimpleParser::new(ParserConfig {
            dialect: Dialect::Postgres,
        });
        let result = execute_with_parser(&adapter, &parser, sql);
        assert_eq!(result.rows.len(), 2);
        assert_eq!(result.rows[0][0], "eu");
        assert_eq!(result.rows[1][0], "us");

        let yacc_parser = YaccParser::new(ParserConfig {
            dialect: Dialect::Postgres,
        });
        let result = execute_with_parser(&adapter, &yacc_parser, sql);
        assert_eq!(result.rows.len(), 2);
        assert_eq!(result.rows[0][0], "eu");
        assert_eq!(result.rows[1][0], "us");
    }

    #[test]
    fn duckdb_pipeline_executes_set_op_suffix() {
        let adapter = DuckDbAdapter::try_new().expect("duckdb adapter");
        setup_sales_table(&adapter);
        let sql = "select id from sales where region = 'us' union all select id from sales where region = 'eu' order by id limit 3";

        let parser = SimpleParser::new(ParserConfig {
            dialect: Dialect::Postgres,
        });
        let result = execute_with_parser(&adapter, &parser, sql);
        assert_eq!(result.rows.len(), 3);

        let yacc_parser = YaccParser::new(ParserConfig {
            dialect: Dialect::Postgres,
        });
        let result = execute_with_parser(&adapter, &yacc_parser, sql);
        assert_eq!(result.rows.len(), 3);
    }

    #[test]
    fn duckdb_pipeline_executes_dml_roundtrip() {
        let parser = SimpleParser::new(ParserConfig {
            dialect: Dialect::Postgres,
        });
        let result = execute_dml_roundtrip(&parser);
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0][0], "1");

        let yacc_parser = YaccParser::new(ParserConfig {
            dialect: Dialect::Postgres,
        });
        let result = execute_dml_roundtrip(&yacc_parser);
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0][0], "1");
    }
}
