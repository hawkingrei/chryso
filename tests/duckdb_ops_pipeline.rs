#[cfg(feature = "duckdb-ops-ffi")]
mod tests {
    use chryso::metadata::StatsCache;
    use chryso::optimizer::{CascadesOptimizer, OptimizerConfig};
    use chryso::parser::{Dialect, ParserConfig, SimpleParser, SqlParser};
    use chryso::planner::PlanBuilder;
    use chryso::{DuckDbOpsAdapter, ExecutorAdapter};

    fn build_physical(parser: &dyn SqlParser, sql: &str) -> chryso::planner::PhysicalPlan {
        let stmt = parser.parse(sql).expect("parse");
        let logical = PlanBuilder::build(stmt).expect("plan");
        let optimizer = CascadesOptimizer::new(OptimizerConfig::default());
        let mut stats = StatsCache::new();
        optimizer.optimize(&logical, &mut stats)
    }

    fn execute(
        adapter: &DuckDbOpsAdapter,
        parser: &dyn SqlParser,
        sql: &str,
    ) -> chryso::QueryResult {
        let physical = build_physical(parser, sql);
        adapter.execute(&physical).expect("execute")
    }

    #[test]
    fn ops_pipeline_roundtrip() {
        let adapter = DuckDbOpsAdapter::try_new().expect("duckdb ops adapter");
        let parser = SimpleParser::new(ParserConfig {
            dialect: Dialect::Postgres,
        });

        execute(
            &adapter,
            &parser,
            "create table sales (id integer, amount integer, region varchar)",
        );
        execute(
            &adapter,
            &parser,
            "insert into sales values (1, 10, 'us'), (2, 7, 'eu'), (1, 5, 'us')",
        );

        let result = execute(
            &adapter,
            &parser,
            "select id from sales where region = 'us' order by id limit 2",
        );

        assert_eq!(result.columns, vec!["id".to_string()]);
        assert_eq!(result.rows.len(), 2);
        assert_eq!(result.rows[0][0], "1");
        assert_eq!(result.rows[1][0], "1");
    }

    #[test]
    fn ops_pipeline_join_and_aggregate() {
        let adapter = DuckDbOpsAdapter::try_new().expect("duckdb ops adapter");
        let parser = SimpleParser::new(ParserConfig {
            dialect: Dialect::Postgres,
        });

        execute(
            &adapter,
            &parser,
            "create table regions (id integer, name varchar)",
        );
        execute(
            &adapter,
            &parser,
            "create table sales (id integer, amount integer, region_id integer)",
        );
        execute(
            &adapter,
            &parser,
            "insert into regions values (1, 'us'), (2, 'eu')",
        );
        execute(
            &adapter,
            &parser,
            "insert into sales values (1, 10, 1), (1, 5, 1), (2, 7, 2)",
        );

        let result = execute(
            &adapter,
            &parser,
            "select regions.name, sum(sales.amount) as total from sales join regions on sales.region_id = regions.id group by regions.name order by regions.name",
        );

        assert_eq!(
            result.columns,
            vec!["name".to_string(), "total".to_string()]
        );
        assert_eq!(result.rows.len(), 2);
        assert_eq!(result.rows[0][0], "eu");
        assert_eq!(result.rows[0][1], "7");
        assert_eq!(result.rows[1][0], "us");
        assert_eq!(result.rows[1][1], "15");
    }

    #[test]
    fn ops_pipeline_derived_column_aliases() {
        let adapter = DuckDbOpsAdapter::try_new().expect("duckdb ops adapter");
        let parser = SimpleParser::new(ParserConfig {
            dialect: Dialect::Postgres,
        });

        execute(
            &adapter,
            &parser,
            "create table users (id integer, name varchar)",
        );
        execute(
            &adapter,
            &parser,
            "insert into users values (1, 'alice'), (2, 'bob')",
        );

        let result = execute(
            &adapter,
            &parser,
            "select * from (select id, name from users) as u(uid, uname) order by uid",
        );

        assert_eq!(result.columns, vec!["uid".to_string(), "uname".to_string()]);
        assert_eq!(result.rows.len(), 2);
        assert_eq!(result.rows[0][0], "1");
        assert_eq!(result.rows[0][1], "alice");
    }
}
