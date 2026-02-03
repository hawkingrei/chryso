use chryso_core::sql_format::format_statement;
use chryso_parser::{Dialect, ParserConfig, SimpleParser, SqlParser};
use std::fs;
use std::path::Path;

fn run_golden(dir: &Path, dialect: Dialect) {
    for entry in fs::read_dir(dir).expect("read golden dir") {
        let entry = entry.expect("read entry");
        let path = entry.path();
        if path.extension().and_then(|ext| ext.to_str()) != Some("sql") {
            continue;
        }
        let sql = fs::read_to_string(&path).expect("read sql");
        let parser = SimpleParser::new(ParserConfig { dialect });
        let stmt = parser.parse(sql.trim()).expect("parse");
        let formatted = format_statement(&stmt);
        let out_path = path.with_extension("out");
        let expected = fs::read_to_string(&out_path).expect("read expected");
        assert_eq!(
            formatted.trim(),
            expected.trim(),
            "golden mismatch for {}",
            path.display()
        );
    }
}

#[test]
fn golden_postgres() {
    let dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/testdata/golden/pg");
    run_golden(&dir, Dialect::Postgres);
}

#[test]
fn golden_mysql() {
    let dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/testdata/golden/mysql");
    run_golden(&dir, Dialect::MySql);
}
