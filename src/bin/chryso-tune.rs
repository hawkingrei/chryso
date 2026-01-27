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
    let mut query_dir = None;
    let mut dialect = Dialect::Postgres;
    let mut out_path: Option<PathBuf> = None;
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
                    dialect = match value.as_str() {
                        "postgres" => Dialect::Postgres,
                        "mysql" => Dialect::MySql,
                        other => {
                            eprintln!("unknown dialect: {other}");
                            std::process::exit(1);
                        }
                    };
                }
            }
            "--out" => {
                if let Some(path) = iter.next() {
                    out_path = Some(PathBuf::from(path));
                }
            }
            other => {
                eprintln!("unknown argument: {other}");
                std::process::exit(1);
            }
        }
    }

    let query_dir = match query_dir {
        Some(dir) => dir,
        None => {
            eprintln!("missing --queries <dir>");
            std::process::exit(1);
        }
    };

    let mut writer: Box<dyn Write> = if let Some(path) = out_path {
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

    let mut runner = TuneRunner::new(dialect);
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

struct TuneRunner {
    parser: SimpleParser,
    optimizer: CascadesOptimizer,
    stats: StatsCache,
}

impl TuneRunner {
    fn new(dialect: Dialect) -> Self {
        Self {
            parser: SimpleParser::new(ParserConfig { dialect }),
            optimizer: CascadesOptimizer::new(OptimizerConfig::default()),
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
