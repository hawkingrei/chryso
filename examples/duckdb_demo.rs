#[cfg(feature = "duckdb")]
fn main() {
    use corundum::adapter::duckdb::connect;
    use corundum::adapter::{physical_to_sql, DuckDbAdapter, ExecutorAdapter};
    use corundum::metadata::StatsCache;
    use corundum::optimizer::{CascadesOptimizer, OptimizerConfig};
    use corundum::parser::{Dialect, ParserConfig, SimpleParser, SqlParser};
    use corundum::planner::PlanBuilder;

    let conn = connect().expect("connect");
    conn.execute("create table users(id integer, name varchar)", [])
        .expect("create");

    let parser = SimpleParser::new(ParserConfig {
        dialect: Dialect::Postgres,
    });
    let optimizer = CascadesOptimizer::new(OptimizerConfig::default());

    let dml = [
        "insert into users (id, name) values (1, 'alice'), (2, 'bob')",
        "update users set name = 'bobby' where id = 2",
        "delete from users where id = 1",
    ];
    for sql in dml {
        let stmt = parser.parse(sql).expect("parse");
        let logical = PlanBuilder::build(stmt).expect("plan");
        let physical = optimizer.optimize(&logical, &StatsCache::new());
        let _ = physical_to_sql(&physical);
        let adapter = DuckDbAdapter::new();
        let result = adapter.execute(&physical).expect("execute");
        println!("dml result: {:?}", result.rows);
    }

    let sql = "select id, name from users order by id";
    let stmt = parser.parse(sql).expect("parse");
    let logical = PlanBuilder::build(stmt).expect("plan");
    let physical = optimizer.optimize(&logical, &StatsCache::new());
    let query = physical_to_sql(&physical);
    let mut stmt = conn.prepare(&query).expect("prepare");
    let mut rows = stmt.query([]).expect("query");
    while let Some(row) = rows.next().expect("row") {
        let id: i64 = row.get(0).expect("id");
        let name: String = row.get(1).expect("name");
        println!("{id} {name}");
    }

    let adapter = DuckDbAdapter::new();
    let _ = adapter.execute(&physical);
}

#[cfg(not(feature = "duckdb"))]
fn main() {
    eprintln!("duckdb feature disabled; run with --features duckdb");
}
