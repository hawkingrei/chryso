#![feature(test)]

extern crate test;

use corundum::metadata::StatsCache;
use corundum::optimizer::{CascadesOptimizer, OptimizerConfig};
use corundum::parser::{Dialect, ParserConfig, SimpleParser, SqlParser};
use corundum::planner::PlanBuilder;
use test::Bencher;

#[bench]
fn bench_optimize_select(b: &mut Bencher) {
    let sql = "select id, sum(amount) from sales where region = 'us' group by id order by id limit 50";
    let parser = SimpleParser::new(ParserConfig {
        dialect: Dialect::Postgres,
    });
    let statement = parser.parse(sql).expect("parse");
    let logical = PlanBuilder::build(statement).expect("plan");
    let optimizer = CascadesOptimizer::new(OptimizerConfig::default());
    let stats = StatsCache::new();
    b.iter(|| {
        let _ = optimizer.optimize(&logical, &stats);
    });
}
