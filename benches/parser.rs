#![feature(test)]

extern crate test;

use chryso::parser::{Dialect, ParserConfig, SimpleParser, SqlParser};
use test::Bencher;

#[bench]
fn bench_parse_select(b: &mut Bencher) {
    let sql = "select id, sum(amount) from sales where region = 'us' group by id order by id limit 50";
    let parser = SimpleParser::new(ParserConfig {
        dialect: Dialect::Postgres,
    });
    b.iter(|| {
        let _ = parser.parse(sql).expect("parse");
    });
}
