use corundum::adapter::ExecutorAdapter;
use corundum::{
    metadata::StatsCache, parser::SimpleParser, CascadesOptimizer, Dialect, MockAdapter,
    OptimizerConfig, ParserConfig, PlanBuilder, SqlParser,
};

fn main() {
    let sql = "SELECT id, name FROM users WHERE id = 42;";
    let parser = SimpleParser::new(ParserConfig {
        dialect: Dialect::Postgres,
    });
    let statement = parser.parse(sql).expect("parse failed");
    let logical = PlanBuilder::build(statement).expect("plan build failed");

    let optimizer = CascadesOptimizer::new(OptimizerConfig::default());
    let stats = StatsCache::new();
    let physical = optimizer.optimize(&logical, &stats);

    println!("Logical Plan:\n{}", logical.explain(0));
    println!("Physical Plan:\n{}", physical.explain(0));

    let adapter = MockAdapter::new();
    let result = adapter.execute(&physical).expect("execution failed");
    println!("Result columns: {:?}", result.columns);
    println!("Result rows: {:?}", result.rows);
}
