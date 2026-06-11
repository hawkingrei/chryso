use chryso::optimizer::cost::StatsCostModel;
use chryso::planner::CostModel;
use chryso::{
    CascadesOptimizer, Dialect, OptimizerConfig, ParserConfig, PlanBuilder, SqlParser,
    metadata::StatsCache, parser::SimpleParser, sql_utils::split_sql_with_tail,
};
use std::fs;
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::time::Instant;

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let tune_args = match TuneArgs::parse(args) {
        Ok(parsed) => parsed,
        Err(err) => {
            eprintln!("{err}");
            std::process::exit(1);
        }
    };

    let query_dir = tune_args.query_dir;
    let mut writer: Box<dyn Write> = if let Some(path) = tune_args.out_path {
        let file = fs::File::create(path).unwrap_or_else(|err| {
            eprintln!("failed to open output file: {err}");
            std::process::exit(1);
        });
        Box::new(file)
    } else {
        Box::new(io::stdout())
    };

    writeln!(writer, "query,cost,time_ms").ok();

    let mut entries = match fs::read_dir(&query_dir) {
        Ok(entries) => entries
            .filter_map(|entry| entry.ok())
            .filter(|entry| entry.path().extension().and_then(|ext| ext.to_str()) == Some("sql"))
            .map(|entry| entry.path())
            .collect::<Vec<_>>(),
        Err(err) => {
            eprintln!("failed to read directory: {err}");
            std::process::exit(1);
        }
    };
    entries.sort();

    let mut runner = TuneRunner::new(tune_args.dialect, tune_args.enable_join_reorder);
    for path in entries {
        if let Some(name) = path.file_stem().and_then(|stem| stem.to_str()) {
            match runner.run_query_file(&path) {
                Ok((cost, elapsed)) => {
                    writeln!(writer, "{},{:.3},{}", name, cost, elapsed).ok();
                }
                Err(err) => {
                    eprintln!("failed to run {}: {err}", path.display());
                }
            }
        }
    }
}

#[derive(Debug)]
struct TuneArgs {
    query_dir: PathBuf,
    dialect: Dialect,
    out_path: Option<PathBuf>,
    enable_join_reorder: bool,
}

impl TuneArgs {
    fn parse(args: Vec<String>) -> chryso::ChrysoResult<Self> {
        let mut query_dir = None;
        let mut dialect = Dialect::Postgres;
        let mut out_path: Option<PathBuf> = None;
        let mut enable_join_reorder = true;
        let mut iter = args.into_iter();
        while let Some(arg) = iter.next() {
            match arg.as_str() {
                "--queries" => {
                    if let Some(path) = iter.next() {
                        query_dir = Some(PathBuf::from(path));
                    }
                }
                "--dialect" => {
                    if let Some(value) = iter.next() {
                        dialect = parse_dialect(&value)?;
                    }
                }
                "--out" => {
                    if let Some(path) = iter.next() {
                        out_path = Some(PathBuf::from(path));
                    }
                }
                "--no-reorder" => {
                    enable_join_reorder = false;
                }
                other => {
                    return Err(chryso::ChrysoError::new(format!(
                        "unknown argument: {other}"
                    )));
                }
            }
        }
        let Some(query_dir) = query_dir else {
            return Err(chryso::ChrysoError::new("missing --queries <dir>"));
        };
        Ok(Self {
            query_dir,
            dialect,
            out_path,
            enable_join_reorder,
        })
    }
}

fn parse_dialect(value: &str) -> chryso::ChrysoResult<Dialect> {
    match value {
        "postgres" => Ok(Dialect::Postgres),
        "mysql" => Ok(Dialect::MySql),
        other => Err(chryso::ChrysoError::new(format!(
            "unknown dialect: {other}"
        ))),
    }
}

struct TuneRunner {
    parser: SimpleParser,
    optimizer: CascadesOptimizer,
    stats: StatsCache,
}

impl TuneRunner {
    fn new(dialect: Dialect, enable_join_reorder: bool) -> Self {
        let mut config = OptimizerConfig::default();
        config.enable_join_reorder = enable_join_reorder;
        Self {
            parser: SimpleParser::new(ParserConfig { dialect }),
            optimizer: CascadesOptimizer::new(config),
            stats: StatsCache::new(),
        }
    }

    fn run_query_file(&mut self, path: &Path) -> chryso::ChrysoResult<(f64, u128)> {
        let sql =
            fs::read_to_string(path).map_err(|err| chryso::ChrysoError::new(err.to_string()))?;
        let start = Instant::now();
        let mut total_cost = 0.0;
        let (statements, tail) = split_sql_with_tail(&sql);
        for stmt in statements {
            total_cost += self.run_statement(&stmt)?;
        }
        if !tail.trim().is_empty() {
            total_cost += self.run_statement(&tail)?;
        }
        Ok((total_cost, start.elapsed().as_millis()))
    }

    fn run_statement(&mut self, sql: &str) -> chryso::ChrysoResult<f64> {
        let statement = self.parser.parse(sql)?;
        let logical = PlanBuilder::build(statement)?;
        let physical = self.optimizer.optimize(&logical, &mut self.stats);
        let cost_model = StatsCostModel::new(&self.stats);
        Ok(cost_model.cost(&physical).0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_tune_args_supports_no_reorder() {
        let parsed = TuneArgs::parse(vec![
            "--queries".to_string(),
            "queries".to_string(),
            "--dialect".to_string(),
            "mysql".to_string(),
            "--no-reorder".to_string(),
            "--out".to_string(),
            "costs.csv".to_string(),
        ])
        .expect("parse");

        assert_eq!(parsed.query_dir, PathBuf::from("queries"));
        assert!(matches!(parsed.dialect, Dialect::MySql));
        assert_eq!(parsed.out_path, Some(PathBuf::from("costs.csv")));
        assert!(!parsed.enable_join_reorder);
    }

    #[test]
    fn parse_tune_args_requires_queries() {
        let err = TuneArgs::parse(vec!["--no-reorder".to_string()]).expect_err("missing queries");
        assert!(err.to_string().contains("missing --queries"));
    }
}
