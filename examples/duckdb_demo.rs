#[cfg(feature = "duckdb")]
fn main() {
    use corundum::adapter::{DuckDbAdapter, ExecutorAdapter};
    use corundum::metadata::StatsCache;
    use corundum::optimizer::{CascadesOptimizer, OptimizerConfig};
    use corundum::parser::{Dialect, ParserConfig, SimpleParser, SqlParser};
    use corundum::planner::PlanBuilder;

    let adapter = std::sync::Arc::new(DuckDbAdapter::new());
    let mut stats = StatsCache::new();
    adapter
        .execute_sql("create table users(id integer, name varchar)")
        .expect("create");

    let parser = SimpleParser::new(ParserConfig {
        dialect: Dialect::Postgres,
    });
    let mut config = OptimizerConfig::default();
    config.stats_provider = Some(adapter.clone());
    let optimizer = CascadesOptimizer::new(config);

    let dml = [
        "insert into users (id, name) values (1, 'alice'), (2, 'bob')",
        "update users set name = 'bobby' where id = 2",
        "delete from users where id = 1",
    ];
    for sql in dml {
        let stmt = parser.parse(sql).expect("parse");
        let logical = PlanBuilder::build(stmt).expect("plan");
        let physical = optimizer.optimize(&logical, &mut stats);
        let result = adapter.execute(&physical).expect("execute");
        println!("dml result: {:?}", result.rows);
    }

    adapter.analyze_table("users", &mut stats).expect("analyze");

    let sql = "select id, name from users order by id";
    let stmt = parser.parse(sql).expect("parse");
    let logical = PlanBuilder::build(stmt).expect("plan");
    let physical = optimizer.optimize(&logical, &mut stats);
    let result = adapter.execute(&physical).expect("execute");
    for row in result.rows {
        println!("{} {}", row[0], row[1]);
    }
}

#[cfg(not(feature = "duckdb"))]
fn main() {
    eprintln!("duckdb feature disabled; run with --features duckdb");
}
