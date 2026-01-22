use corundum::metadata::StatsCache;
use corundum::optimizer::{CascadesOptimizer, OptimizerConfig};
use corundum::parser::{Dialect, ParserConfig, SimpleParser, SqlParser};
use corundum::planner::PlanBuilder;

#[derive(serde::Deserialize)]
struct PlanTestSuite {
    cases: Vec<PlanTestCaseInput>,
}

#[derive(serde::Deserialize)]
struct PlanTestCaseInput {
    name: String,
    sql: String,
    dialect: String,
}

#[derive(serde::Deserialize, serde::Serialize, Default)]
struct PlanTestOutputSuite {
    cases: std::collections::BTreeMap<String, PlanTestOutput>,
}

#[derive(serde::Deserialize, serde::Serialize, Clone)]
struct PlanTestOutput {
    logical_explain: Vec<String>,
    physical_explain: Vec<String>,
}

#[test]
fn snapshot_plan_explain() {
    let base = std::path::Path::new("tests/testdata/plan");
    let mut found = false;
    for entry in std::fs::read_dir(base).expect("read plan testdata") {
        let entry = entry.expect("read entry");
        if !entry.file_type().expect("file type").is_dir() {
            continue;
        }
        let dir = entry.path();
        let in_path = dir.join("in.json");
        let out_path = dir.join("out.json");
        if !in_path.exists() {
            continue;
        }
        found = true;
        let input = load_json::<PlanTestSuite>(&in_path);
        let expected = if should_record() {
            PlanTestOutputSuite::default()
        } else {
            load_json::<PlanTestOutputSuite>(&out_path)
        };
        let mut actual = PlanTestOutputSuite::default();

        for case in input.cases {
            let dialect = match case.dialect.as_str() {
                "postgres" => Dialect::Postgres,
                "mysql" => Dialect::MySql,
                other => panic!("unknown dialect: {other}"),
            };
            let parser = SimpleParser::new(ParserConfig { dialect });
            let stmt = parser.parse(&case.sql).expect("parse");
            let logical = PlanBuilder::build(stmt).expect("plan");
            let optimizer = CascadesOptimizer::new(OptimizerConfig::default());
            let mut stats = StatsCache::new();
            let physical = optimizer.optimize(&logical, &mut stats);
            let output = PlanTestOutput {
                logical_explain: logical.explain(0).lines().map(|line| line.to_string()).collect(),
                physical_explain: physical.explain(0).lines().map(|line| line.to_string()).collect(),
            };
            actual.cases.insert(case.name, output);
        }

        if should_record() {
            write_json(&out_path, &actual);
            continue;
        }

        assert_eq!(actual.cases.len(), expected.cases.len());
        for (name, output) in &expected.cases {
            let actual_output = actual
                .cases
                .get(name)
                .unwrap_or_else(|| panic!("missing case {name}"));
            assert_eq!(actual_output.logical_explain, output.logical_explain);
            assert_eq!(actual_output.physical_explain, output.physical_explain);
        }
    }
    assert!(found, "no plan test cases found");
}

fn load_json<T: serde::de::DeserializeOwned>(path: &std::path::Path) -> T {
    let content = std::fs::read_to_string(path).expect("read testdata");
    serde_json::from_str(&content).expect("parse json")
}

fn write_json<T: serde::Serialize>(path: &std::path::Path, value: &T) {
    let content = serde_json::to_string_pretty(value).expect("serialize json");
    std::fs::write(path, format!("{content}\n")).expect("write testdata");
}

fn should_record() -> bool {
    std::env::var("CORUNDUM_RECORD").map(|value| value == "1").unwrap_or(false)
}
