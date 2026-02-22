#[cfg(feature = "duckdb")]
mod tests {
    use chryso::metadata::StatsCache;
    use chryso::optimizer::{CascadesOptimizer, OptimizerConfig};
    use chryso::parser::{Dialect, ParserConfig, SimpleParser, SqlParser};
    use chryso::planner::PlanBuilder;
    use chryso::{DuckDbAdapter, ExecutorAdapter, QueryResult};
    use chryso_parser_yacc::YaccParser;

    struct ExecCase {
        name: &'static str,
        setup_sql: Vec<&'static str>,
        query_sql: &'static str,
        expected_rows: Vec<Vec<&'static str>>,
    }

    fn build_physical(parser: &dyn SqlParser, sql: &str) -> chryso::planner::PhysicalPlan {
        let stmt = parser.parse(sql).expect("parse");
        let logical = PlanBuilder::build(stmt).expect("plan");
        let optimizer = CascadesOptimizer::new(OptimizerConfig::default());
        let mut stats = StatsCache::new();
        optimizer.optimize(&logical, &mut stats)
    }

    fn execute_with_parser(
        adapter: &DuckDbAdapter,
        parser: &dyn SqlParser,
        sql: &str,
    ) -> QueryResult {
        let physical = build_physical(parser, sql);
        adapter.execute(&physical).expect("execute")
    }

    fn run_case_with_parser(case: &ExecCase, parser: &dyn SqlParser) {
        let adapter = DuckDbAdapter::try_new().expect("duckdb adapter");
        for sql in &case.setup_sql {
            adapter.execute_sql(sql).expect("setup sql");
        }
        let result = execute_with_parser(&adapter, parser, case.query_sql);
        assert_result_rows(case.name, &result, &case.expected_rows);
    }

    fn assert_result_rows(case_name: &str, result: &QueryResult, expected: &[Vec<&str>]) {
        let actual = result.rows.clone();
        let expected: Vec<Vec<String>> = expected
            .iter()
            .map(|row| row.iter().map(|v| (*v).to_string()).collect())
            .collect();
        assert_eq!(
            actual, expected,
            "case={} row mismatch, columns={:?}",
            case_name, result.columns
        );
    }

    fn regress_exec_cases() -> Vec<ExecCase> {
        vec![
            ExecCase {
                name: "select_distinct_regions",
                setup_sql: vec![
                    "create table reg_sales (id integer, amount integer, region varchar)",
                    "insert into reg_sales values (1, 10, 'us'), (1, 5, 'us'), (2, 7, 'us'), (2, 1, 'eu')",
                ],
                query_sql: "select distinct region from reg_sales order by region",
                expected_rows: vec![vec!["eu"], vec!["us"]],
            },
            ExecCase {
                name: "select_filter_order",
                setup_sql: vec![
                    "create table reg_metrics (id integer, grp varchar)",
                    "insert into reg_metrics values (1, 'a'), (1, 'b'), (2, 'x'), (2, 'y'), (3, 'z')",
                ],
                query_sql: "select grp from reg_metrics where id = 2 order by grp",
                expected_rows: vec![vec!["x"], vec!["y"]],
            },
            ExecCase {
                name: "join_on_and_order",
                setup_sql: vec![
                    "create table reg_left (id integer, v integer)",
                    "create table reg_right (id integer, w integer)",
                    "insert into reg_left values (1, 10), (2, 20), (3, 30)",
                    "insert into reg_right values (2, 200), (3, 300), (4, 400)",
                ],
                query_sql: "select l.id from reg_left l join reg_right r on l.id = r.id order by l.id",
                expected_rows: vec![vec!["2"], vec!["3"]],
            },
            ExecCase {
                name: "derived_subquery_with_limit",
                setup_sql: vec![
                    "create table reg_orders (id integer, amount integer, region varchar)",
                    "insert into reg_orders values (1, 10, 'us'), (2, 20, 'eu'), (3, 5, 'us')",
                ],
                query_sql: "select id from (select id from reg_orders where region = 'us') as t order by id limit 1",
                expected_rows: vec![vec!["1"]],
            },
            ExecCase {
                name: "set_op_union_all_with_suffix",
                setup_sql: vec![
                    "create table reg_union (id integer, region varchar)",
                    "insert into reg_union values (1, 'us'), (1, 'us'), (2, 'us'), (3, 'eu')",
                ],
                query_sql: "select id from reg_union where region = 'us' union all select id from reg_union where region = 'eu' order by id limit 3",
                expected_rows: vec![vec!["1"], vec!["1"], vec!["2"]],
            },
        ]
    }

    #[test]
    fn postgres_regress_exec_subset_matches_expected_rows() {
        let simple_parser = SimpleParser::new(ParserConfig {
            dialect: Dialect::Postgres,
        });
        let yacc_parser = YaccParser::new(ParserConfig {
            dialect: Dialect::Postgres,
        });

        for case in regress_exec_cases() {
            run_case_with_parser(&case, &simple_parser);
            run_case_with_parser(&case, &yacc_parser);
        }
    }
}
