#[cfg(feature = "duckdb")]
use chryso::DuckDbAdapter;
#[cfg(feature = "duckdb")]
use chryso::adapter::ExecutorAdapter;
#[cfg(feature = "duckdb")]
use chryso::metadata::StatsCache;
#[cfg(feature = "duckdb")]
use chryso::parser::SimpleParser;
#[cfg(feature = "duckdb")]
use chryso::{CascadesOptimizer, Dialect, OptimizerConfig, ParserConfig, PlanBuilder, SqlParser};
use chryso::{ChrysoError, ChrysoResult};
use std::fs::{self, File};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
#[cfg(feature = "duckdb")]
use std::time::{Duration, Instant};
use tpchgen::csv::{
    CustomerCsv, LineItemCsv, NationCsv, OrderCsv, PartCsv, PartSuppCsv, RegionCsv, SupplierCsv,
};
use tpchgen::generators::{
    CustomerGenerator, LineItemGenerator, NationGenerator, OrderGenerator, PartGenerator,
    PartSuppGenerator, RegionGenerator, SupplierGenerator,
};

const DEFAULT_SCALE_FACTOR: f64 = 0.01;
const DEFAULT_THREADS: usize = 4;
const DEFAULT_ITERATIONS: usize = 1;

fn main() {
    if let Err(err) = run(std::env::args().skip(1).collect()) {
        eprintln!("error: {err}");
        std::process::exit(1);
    }
}

fn run(args: Vec<String>) -> ChrysoResult<()> {
    let command = CliCommand::parse(args)?;
    match command {
        CliCommand::Tpch(TpchCommand::Generate(config)) => generate_tpch(&config),
        CliCommand::Tpch(TpchCommand::Run(config)) => run_tpch(&config),
        CliCommand::Tpch(TpchCommand::All(config)) => {
            generate_tpch(&config.generate)?;
            run_tpch(&config.run)
        }
        CliCommand::Tpcc => Err(ChrysoError::new(
            "TPC-C is not implemented yet; no mature embedded Rust TPC-C generator was found",
        )),
        CliCommand::Help => {
            print_usage();
            Ok(())
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
enum CliCommand {
    Tpch(TpchCommand),
    Tpcc,
    Help,
}

impl CliCommand {
    fn parse(args: Vec<String>) -> ChrysoResult<Self> {
        let mut args = ArgCursor::new(args);
        let Some(command) = args.next() else {
            return Ok(Self::Help);
        };
        match command.as_str() {
            "-h" | "--help" | "help" => Ok(Self::Help),
            "tpcc" => {
                args.reject_remaining()?;
                Ok(Self::Tpcc)
            }
            "tpch" => TpchCommand::parse(args).map(Self::Tpch),
            _ => Err(ChrysoError::new(format!(
                "unknown command '{command}', expected tpch or tpcc"
            ))),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
enum TpchCommand {
    Generate(GenerateConfig),
    Run(RunConfig),
    All(AllConfig),
}

impl TpchCommand {
    fn parse(mut args: ArgCursor) -> ChrysoResult<Self> {
        let Some(command) = args.next() else {
            return Err(ChrysoError::new("tpch expects generate, run, or all"));
        };
        match command.as_str() {
            "generate" => GenerateConfig::parse(args).map(Self::Generate),
            "run" => RunConfig::parse(args).map(Self::Run),
            "all" => AllConfig::parse(args).map(Self::All),
            _ => Err(ChrysoError::new(format!(
                "unknown tpch command '{command}', expected generate, run, or all"
            ))),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
struct GenerateConfig {
    scale_factor: f64,
    out_dir: PathBuf,
    threads: usize,
    tables: Vec<BenchTable>,
}

impl GenerateConfig {
    fn parse(mut args: ArgCursor) -> ChrysoResult<Self> {
        let mut config = Self {
            scale_factor: DEFAULT_SCALE_FACTOR,
            out_dir: PathBuf::from("target/chryso-bench/tpch"),
            threads: DEFAULT_THREADS,
            tables: BenchTable::all(),
        };
        while let Some(arg) = args.next() {
            match arg.as_str() {
                "--scale-factor" => config.scale_factor = parse_value(&mut args, "--scale-factor")?,
                "--out" | "--out-dir" => {
                    config.out_dir = PathBuf::from(expect_value(&mut args, &arg)?)
                }
                "--threads" => config.threads = parse_value(&mut args, "--threads")?,
                "--tables" => {
                    config.tables = BenchTable::parse_list(&expect_value(&mut args, "--tables")?)?
                }
                _ => return Err(unknown_arg(&arg)),
            }
        }
        config.validate()?;
        Ok(config)
    }

    fn validate(&self) -> ChrysoResult<()> {
        if self.scale_factor <= 0.0 {
            return Err(ChrysoError::new("--scale-factor must be greater than 0"));
        }
        if self.threads == 0 {
            return Err(ChrysoError::new("--threads must be greater than 0"));
        }
        if self.tables.is_empty() {
            return Err(ChrysoError::new("--tables must not be empty"));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq)]
struct RunConfig {
    data_dir: PathBuf,
    iterations: usize,
    query_suite: QuerySuite,
}

impl RunConfig {
    fn parse(mut args: ArgCursor) -> ChrysoResult<Self> {
        let mut config = Self {
            data_dir: PathBuf::from("target/chryso-bench/tpch"),
            iterations: DEFAULT_ITERATIONS,
            query_suite: QuerySuite::All,
        };
        while let Some(arg) = args.next() {
            match arg.as_str() {
                "--data-dir" => config.data_dir = PathBuf::from(expect_value(&mut args, &arg)?),
                "--iterations" => config.iterations = parse_value(&mut args, "--iterations")?,
                "--queries" => {
                    config.query_suite = QuerySuite::parse(&expect_value(&mut args, "--queries")?)?
                }
                _ => return Err(unknown_arg(&arg)),
            }
        }
        config.validate()?;
        Ok(config)
    }

    fn validate(&self) -> ChrysoResult<()> {
        if self.iterations == 0 {
            return Err(ChrysoError::new("--iterations must be greater than 0"));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq)]
struct AllConfig {
    generate: GenerateConfig,
    run: RunConfig,
}

impl AllConfig {
    fn parse(mut args: ArgCursor) -> ChrysoResult<Self> {
        let mut scale_factor = DEFAULT_SCALE_FACTOR;
        let mut work_dir = PathBuf::from("target/chryso-bench/tpch");
        let mut threads = DEFAULT_THREADS;
        let mut iterations = DEFAULT_ITERATIONS;
        let mut tables = BenchTable::all();
        let mut query_suite = QuerySuite::All;

        while let Some(arg) = args.next() {
            match arg.as_str() {
                "--scale-factor" => scale_factor = parse_value(&mut args, "--scale-factor")?,
                "--work-dir" | "--out" | "--out-dir" => {
                    work_dir = PathBuf::from(expect_value(&mut args, &arg)?)
                }
                "--threads" => threads = parse_value(&mut args, "--threads")?,
                "--iterations" => iterations = parse_value(&mut args, "--iterations")?,
                "--tables" => {
                    tables = BenchTable::parse_list(&expect_value(&mut args, "--tables")?)?
                }
                "--queries" => {
                    query_suite = QuerySuite::parse(&expect_value(&mut args, "--queries")?)?
                }
                _ => return Err(unknown_arg(&arg)),
            }
        }

        let generate = GenerateConfig {
            scale_factor,
            out_dir: work_dir.clone(),
            threads,
            tables,
        };
        generate.validate()?;
        let run = RunConfig {
            data_dir: work_dir,
            iterations,
            query_suite,
        };
        run.validate()?;
        Ok(Self { generate, run })
    }
}

#[derive(Debug, Clone)]
struct ArgCursor {
    args: std::vec::IntoIter<String>,
}

impl ArgCursor {
    fn new(args: Vec<String>) -> Self {
        Self {
            args: args.into_iter(),
        }
    }

    fn next(&mut self) -> Option<String> {
        self.args.next()
    }

    fn reject_remaining(mut self) -> ChrysoResult<()> {
        if let Some(arg) = self.next() {
            Err(unknown_arg(&arg))
        } else {
            Ok(())
        }
    }
}

fn expect_value(args: &mut ArgCursor, flag: &str) -> ChrysoResult<String> {
    args.next()
        .ok_or_else(|| ChrysoError::new(format!("{flag} expects a value")))
}

fn parse_value<T>(args: &mut ArgCursor, flag: &str) -> ChrysoResult<T>
where
    T: std::str::FromStr,
    T::Err: std::fmt::Display,
{
    let value = expect_value(args, flag)?;
    value
        .parse()
        .map_err(|err| ChrysoError::new(format!("invalid value for {flag}: {err}")))
}

fn unknown_arg(arg: &str) -> ChrysoError {
    ChrysoError::new(format!("unknown argument '{arg}'"))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BenchTable {
    Nation,
    Region,
    Part,
    Supplier,
    Partsupp,
    Customer,
    Orders,
    Lineitem,
}

impl BenchTable {
    fn all() -> Vec<Self> {
        vec![
            Self::Nation,
            Self::Region,
            Self::Part,
            Self::Supplier,
            Self::Partsupp,
            Self::Customer,
            Self::Orders,
            Self::Lineitem,
        ]
    }

    fn parse_list(value: &str) -> ChrysoResult<Vec<Self>> {
        value
            .split(',')
            .map(|item| Self::parse(item.trim()))
            .collect()
    }

    fn parse(value: &str) -> ChrysoResult<Self> {
        match value {
            "nation" => Ok(Self::Nation),
            "region" => Ok(Self::Region),
            "part" => Ok(Self::Part),
            "supplier" => Ok(Self::Supplier),
            "partsupp" => Ok(Self::Partsupp),
            "customer" => Ok(Self::Customer),
            "orders" => Ok(Self::Orders),
            "lineitem" => Ok(Self::Lineitem),
            _ => Err(ChrysoError::new(format!("unknown TPC-H table '{value}'"))),
        }
    }

    #[cfg(feature = "duckdb")]
    fn as_str(self) -> &'static str {
        match self {
            Self::Nation => "nation",
            Self::Region => "region",
            Self::Part => "part",
            Self::Supplier => "supplier",
            Self::Partsupp => "partsupp",
            Self::Customer => "customer",
            Self::Orders => "orders",
            Self::Lineitem => "lineitem",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum QuerySuite {
    Smoke,
    All,
}

impl QuerySuite {
    fn parse(value: &str) -> ChrysoResult<Self> {
        match value {
            "smoke" => Ok(Self::Smoke),
            "all" => Ok(Self::All),
            _ => Err(ChrysoError::new("--queries expects smoke or all")),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(not(any(feature = "duckdb", test)), allow(dead_code))]
struct BenchmarkQuery {
    name: &'static str,
    sql: &'static str,
}

#[cfg_attr(not(any(feature = "duckdb", test)), allow(dead_code))]
fn queries(suite: QuerySuite) -> Vec<BenchmarkQuery> {
    let smoke = vec![BenchmarkQuery {
        name: "q_lineitem_group",
        sql: "select l_returnflag, l_linestatus, count(*) as count_order, sum(l_quantity) as sum_qty from lineitem group by l_returnflag, l_linestatus order by l_returnflag, l_linestatus",
    }];
    match suite {
        QuerySuite::Smoke => smoke,
        QuerySuite::All => {
            let mut all = smoke;
            all.extend([
                BenchmarkQuery {
                    name: "q_customer_orders",
                    sql: "select c_custkey, count(*) as order_count from customer join orders on c_custkey = o_custkey group by c_custkey order by c_custkey limit 10",
                },
                BenchmarkQuery {
                    name: "q_nation_supplier",
                    sql: "select n_name, count(*) as supplier_count from nation join supplier on n_nationkey = s_nationkey group by n_name order by n_name",
                },
                BenchmarkQuery {
                    name: "q_order_priority",
                    sql: "select o_orderpriority, count(*) as order_count from orders group by o_orderpriority order by o_orderpriority",
                },
            ]);
            all
        }
    }
}

fn generate_tpch(config: &GenerateConfig) -> ChrysoResult<()> {
    println!(
        "generating TPC-H data: scale_factor={}, out={}, threads={}",
        config.scale_factor,
        config.out_dir.display(),
        config.threads
    );
    fs::create_dir_all(&config.out_dir)
        .map_err(|err| ChrysoError::new(format!("failed to create output directory: {err}")))?;
    for table in &config.tables {
        write_tpch_table(*table, config.scale_factor, &config.out_dir)?;
    }
    Ok(())
}

fn write_tpch_table(table: BenchTable, scale_factor: f64, out_dir: &Path) -> ChrysoResult<()> {
    match table {
        BenchTable::Nation => write_csv(
            out_dir.join("nation.csv"),
            NationCsv::header(),
            NationGenerator::new(scale_factor, 1, 1).iter(),
            |writer, row| writeln!(writer, "{}", NationCsv::new(row)),
        ),
        BenchTable::Region => write_csv(
            out_dir.join("region.csv"),
            RegionCsv::header(),
            RegionGenerator::new(scale_factor, 1, 1).iter(),
            |writer, row| writeln!(writer, "{}", RegionCsv::new(row)),
        ),
        BenchTable::Part => write_csv(
            out_dir.join("part.csv"),
            PartCsv::header(),
            PartGenerator::new(scale_factor, 1, 1).iter(),
            |writer, row| writeln!(writer, "{}", PartCsv::new(row)),
        ),
        BenchTable::Supplier => write_csv(
            out_dir.join("supplier.csv"),
            SupplierCsv::header(),
            SupplierGenerator::new(scale_factor, 1, 1).iter(),
            |writer, row| writeln!(writer, "{}", SupplierCsv::new(row)),
        ),
        BenchTable::Partsupp => write_csv(
            out_dir.join("partsupp.csv"),
            PartSuppCsv::header(),
            PartSuppGenerator::new(scale_factor, 1, 1).iter(),
            |writer, row| writeln!(writer, "{}", PartSuppCsv::new(row)),
        ),
        BenchTable::Customer => write_csv(
            out_dir.join("customer.csv"),
            CustomerCsv::header(),
            CustomerGenerator::new(scale_factor, 1, 1).iter(),
            |writer, row| writeln!(writer, "{}", CustomerCsv::new(row)),
        ),
        BenchTable::Orders => write_csv(
            out_dir.join("orders.csv"),
            OrderCsv::header(),
            OrderGenerator::new(scale_factor, 1, 1).iter(),
            |writer, row| writeln!(writer, "{}", OrderCsv::new(row)),
        ),
        BenchTable::Lineitem => write_csv(
            out_dir.join("lineitem.csv"),
            LineItemCsv::header(),
            LineItemGenerator::new(scale_factor, 1, 1).iter(),
            |writer, row| writeln!(writer, "{}", LineItemCsv::new(row)),
        ),
    }
}

fn write_csv<T, I, F>(path: PathBuf, header: &str, rows: I, mut write_row: F) -> ChrysoResult<()>
where
    I: IntoIterator<Item = T>,
    F: FnMut(&mut BufWriter<File>, T) -> std::io::Result<()>,
{
    let file = File::create(&path)
        .map_err(|err| ChrysoError::new(format!("failed to create {}: {err}", path.display())))?;
    let mut writer = BufWriter::new(file);
    writeln!(writer, "{header}").map_err(|err| {
        ChrysoError::new(format!(
            "failed to write header to {}: {err}",
            path.display()
        ))
    })?;
    for row in rows {
        write_row(&mut writer, row).map_err(|err| {
            ChrysoError::new(format!("failed to write row to {}: {err}", path.display()))
        })?;
    }
    writer
        .flush()
        .map_err(|err| ChrysoError::new(format!("failed to flush {}: {err}", path.display())))
}

#[cfg(feature = "duckdb")]
fn run_tpch(config: &RunConfig) -> ChrysoResult<()> {
    let adapter = DuckDbAdapter::try_new()?;
    load_tpch_csv(&adapter, &config.data_dir)?;

    let mut stats = StatsCache::new();
    for table in BenchTable::all() {
        adapter.analyze_table(table.as_str(), &mut stats)?;
    }

    let parser = SimpleParser::new(ParserConfig {
        dialect: Dialect::Postgres,
    });
    let optimizer = CascadesOptimizer::new(OptimizerConfig::default());
    let benchmark_queries = queries(config.query_suite);

    println!("query,iteration,rows,plan_ms,execute_ms,total_ms");
    for query in benchmark_queries {
        for iteration in 1..=config.iterations {
            let result = run_query(&adapter, &parser, &optimizer, &mut stats, query.sql)?;
            println!(
                "{},{},{},{:.3},{:.3},{:.3}",
                query.name,
                iteration,
                result.rows,
                duration_ms(result.plan_elapsed),
                duration_ms(result.execute_elapsed),
                duration_ms(result.total_elapsed)
            );
        }
    }

    Ok(())
}

#[cfg(not(feature = "duckdb"))]
fn run_tpch(_config: &RunConfig) -> ChrysoResult<()> {
    Err(ChrysoError::new(
        "TPC-H execution requires the duckdb feature; rerun with --features duckdb",
    ))
}

#[cfg(feature = "duckdb")]
fn load_tpch_csv(adapter: &DuckDbAdapter, data_dir: &Path) -> ChrysoResult<()> {
    for table in BenchTable::all() {
        let file = data_dir.join(format!("{}.csv", table.as_str()));
        if !file.exists() {
            return Err(ChrysoError::new(format!(
                "missing TPC-H CSV file: {}",
                file.display()
            )));
        }
        let sql = format!(
            "create or replace table {} as select * from read_csv_auto('{}', header = true)",
            table.as_str(),
            escape_sql_path(&file)
        );
        adapter.execute_sql(&sql)?;
    }
    Ok(())
}

#[cfg_attr(not(any(feature = "duckdb", test)), allow(dead_code))]
fn escape_sql_path(path: &Path) -> String {
    path.to_string_lossy().replace('\'', "''")
}

#[cfg(feature = "duckdb")]
#[derive(Debug, Clone, Copy)]
struct QueryRunResult {
    rows: usize,
    plan_elapsed: Duration,
    execute_elapsed: Duration,
    total_elapsed: Duration,
}

#[cfg(feature = "duckdb")]
fn run_query(
    adapter: &DuckDbAdapter,
    parser: &SimpleParser,
    optimizer: &CascadesOptimizer,
    stats: &mut StatsCache,
    sql: &str,
) -> ChrysoResult<QueryRunResult> {
    let total_start = Instant::now();

    let plan_start = Instant::now();
    let statement = parser.parse(sql)?;
    let logical = PlanBuilder::build(statement)?;
    let physical = optimizer.optimize(&logical, stats);
    let plan_elapsed = plan_start.elapsed();

    let execute_start = Instant::now();
    let result = adapter.execute(&physical)?;
    let execute_elapsed = execute_start.elapsed();

    Ok(QueryRunResult {
        rows: result.rows.len(),
        plan_elapsed,
        execute_elapsed,
        total_elapsed: total_start.elapsed(),
    })
}

#[cfg(feature = "duckdb")]
fn duration_ms(duration: Duration) -> f64 {
    duration.as_secs_f64() * 1000.0
}

fn print_usage() {
    println!(
        "usage:
  chryso-bench tpch generate [--scale-factor N] [--out DIR] [--threads N] [--tables a,b]
  chryso-bench tpch run [--data-dir DIR] [--iterations N] [--queries smoke|all]
  chryso-bench tpch all [--scale-factor N] [--work-dir DIR] [--iterations N]
  chryso-bench tpcc"
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(args: &[&str]) -> ChrysoResult<CliCommand> {
        CliCommand::parse(args.iter().map(|arg| arg.to_string()).collect())
    }

    #[test]
    fn parse_tpch_generate_defaults() {
        let command = parse(&["tpch", "generate"]).expect("parse");
        let CliCommand::Tpch(TpchCommand::Generate(config)) = command else {
            panic!("expected tpch generate");
        };
        assert_eq!(config.scale_factor, DEFAULT_SCALE_FACTOR);
        assert_eq!(config.out_dir, PathBuf::from("target/chryso-bench/tpch"));
        assert_eq!(config.threads, DEFAULT_THREADS);
        assert_eq!(config.tables, BenchTable::all());
    }

    #[test]
    fn parse_tpch_generate_overrides() {
        let command = parse(&[
            "tpch",
            "generate",
            "--scale-factor",
            "0.1",
            "--out",
            "target/tpch-small",
            "--threads",
            "2",
            "--tables",
            "nation,region",
        ])
        .expect("parse");
        let CliCommand::Tpch(TpchCommand::Generate(config)) = command else {
            panic!("expected tpch generate");
        };
        assert_eq!(config.scale_factor, 0.1);
        assert_eq!(config.out_dir, PathBuf::from("target/tpch-small"));
        assert_eq!(config.threads, 2);
        assert_eq!(config.tables, vec![BenchTable::Nation, BenchTable::Region]);
    }

    #[test]
    fn parse_tpch_run_overrides() {
        let command = parse(&[
            "tpch",
            "run",
            "--data-dir",
            "target/tpch-small",
            "--iterations",
            "5",
            "--queries",
            "smoke",
        ])
        .expect("parse");
        let CliCommand::Tpch(TpchCommand::Run(config)) = command else {
            panic!("expected tpch run");
        };
        assert_eq!(config.data_dir, PathBuf::from("target/tpch-small"));
        assert_eq!(config.iterations, 5);
        assert_eq!(config.query_suite, QuerySuite::Smoke);
    }

    #[test]
    fn parse_tpch_all_maps_work_dir_to_generate_and_run() {
        let command = parse(&[
            "tpch",
            "all",
            "--scale-factor",
            "0.02",
            "--work-dir",
            "target/tpch-all",
            "--iterations",
            "3",
        ])
        .expect("parse");
        let CliCommand::Tpch(TpchCommand::All(config)) = command else {
            panic!("expected tpch all");
        };
        assert_eq!(config.generate.scale_factor, 0.02);
        assert_eq!(config.generate.out_dir, PathBuf::from("target/tpch-all"));
        assert_eq!(config.run.data_dir, PathBuf::from("target/tpch-all"));
        assert_eq!(config.run.iterations, 3);
    }

    #[test]
    fn parse_tpcc_placeholder() {
        assert_eq!(parse(&["tpcc"]).expect("parse"), CliCommand::Tpcc);
    }

    #[test]
    fn rejects_invalid_iterations() {
        let err = parse(&["tpch", "run", "--iterations", "0"]).expect_err("invalid");
        assert!(err.to_string().contains("iterations"));
    }

    #[test]
    fn query_suites_are_not_empty() {
        assert_eq!(queries(QuerySuite::Smoke).len(), 1);
        assert!(queries(QuerySuite::All).len() > queries(QuerySuite::Smoke).len());
    }

    #[test]
    fn generate_tpch_writes_selected_csv_files() {
        let out_dir = std::env::temp_dir().join(format!(
            "chryso-bench-test-{}-{}",
            std::process::id(),
            std::thread::current().name().unwrap_or("unnamed")
        ));
        let _ = fs::remove_dir_all(&out_dir);
        let config = GenerateConfig {
            scale_factor: 0.001,
            out_dir: out_dir.clone(),
            threads: 1,
            tables: vec![BenchTable::Nation, BenchTable::Region],
        };

        generate_tpch(&config).expect("generate");

        let nation = fs::read_to_string(out_dir.join("nation.csv")).expect("nation");
        let region = fs::read_to_string(out_dir.join("region.csv")).expect("region");
        assert!(nation.starts_with(NationCsv::header()));
        assert!(region.starts_with(RegionCsv::header()));
        assert!(!out_dir.join("lineitem.csv").exists());

        let _ = fs::remove_dir_all(&out_dir);
    }

    #[test]
    fn sql_path_escaping_doubles_single_quotes() {
        assert_eq!(
            escape_sql_path(Path::new("/tmp/chryso's/tpch.csv")),
            "/tmp/chryso''s/tpch.csv"
        );
    }
}
