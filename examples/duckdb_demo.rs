#[cfg(feature = "duckdb")]
fn main() {
    use chryso::metadata::StatsCache;
    use chryso::optimizer::{CascadesOptimizer, OptimizerConfig};
    use chryso::parser::{Dialect, ParserConfig, SimpleParser, SqlParser};
    use chryso::planner::PlanBuilder;
    use chryso::{DuckDbAdapter, ExecutorAdapter};

    let adapter = std::sync::Arc::new(DuckDbAdapter::new());
    let mut stats = StatsCache::new();
    adapter
        .execute_sql("create table if not exists users(id integer, name varchar)")
        .expect("create");
    adapter
        .execute_sql("create table if not exists items(id integer, user_id integer)")
        .expect("create");

    let parser = SimpleParser::new(ParserConfig {
        dialect: Dialect::Postgres,
    });
    let mut config = OptimizerConfig::default();
    config.stats_provider = Some(adapter.clone());
    let optimizer = CascadesOptimizer::new(config);

    let dml = [
        "insert into users (id, name) values (1, 'alice'), (2, 'bob')",
        "insert into items (id, user_id) values (10, 1), (11, 2)",
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

    let sql = "select 1 where 1 = 1 order by 1 limit 1";
    let stmt = parser.parse(sql).expect("parse");
    let logical = PlanBuilder::build(stmt).expect("plan");
    let physical = optimizer.optimize(&logical, &mut stats);
    let result = adapter.execute(&physical).expect("execute");
    println!("select without from: {:?}", result.rows);

    let sql = "select u.user_id, i.id from (select id as user_id from users) as u(user_id) join items as i using (user_id)";
    let stmt = parser.parse(sql).expect("parse");
    let logical = PlanBuilder::build(stmt).expect("plan");
    let physical = optimizer.optimize(&logical, &mut stats);
    let result = adapter.execute(&physical).expect("execute");
    println!("derived join: {:?}", result.rows);
}

#[cfg(not(feature = "duckdb"))]
fn main() {
    eprintln!("duckdb feature disabled; run with --features duckdb");
}
