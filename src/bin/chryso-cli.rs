use chryso::adapter::ExecutorAdapter;
use chryso::planner::{ExplainConfig, ExplainFormatter};
use chryso::{
    Authorizer, CascadesOptimizer, DdlHandler, DdlResult, Dialect, DuckDbAdapter, MockAdapter,
    NoExtension, OptimizerConfig, ParserConfig, PlanBuilder, PlanOutcome, SessionContext,
    SqlParser, Statement, StatementCategory, StatementContext, StatementEnvelope,
    metadata::StatsCache, parser::SimpleParser, plan_with_hooks, sql_utils::split_sql_with_tail,
};
use crossterm::ExecutableCommand;
use crossterm::event::{self, Event, KeyCode, KeyEvent, KeyModifiers};
use crossterm::terminal::{
    EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode,
};
use ratatui::Terminal;
use ratatui::layout::{Constraint, Direction, Layout};
use ratatui::style::{Color, Style};
use ratatui::text::{Line, Text};
use ratatui::widgets::{Block, Borders, Paragraph};
use std::io::IsTerminal;
use std::io::{self, Read};
use std::time::{Duration, Instant};

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let cli_args = match CliArgs::parse(args) {
        Ok(parsed) => parsed,
        Err(err) => {
            eprintln!("error: {err}");
            return;
        }
    };
    let mut runner = match PipelineRunner::new(cli_args.engine) {
        Ok(runner) => runner,
        Err(err) => {
            eprintln!("error: {err}");
            return;
        }
    };
    runner.context = SessionContext::new(cli_args.database, cli_args.user, cli_args.default_schema);
    if !cli_args.sql_parts.is_empty() {
        let sql = cli_args.sql_parts.join(" ");
        if let Err(err) = execute_non_interactive_with_memo(
            &sql,
            &mut runner,
            cli_args.dump_memo,
            cli_args.memo_best_only,
        ) {
            eprintln!("error: {err}");
        }
        return;
    }

    if !io::stdout().is_terminal() {
        let mut input = String::new();
        if io::stdin().read_to_string(&mut input).is_err() {
            return;
        }
        if let Err(err) = execute_non_interactive(&input, &mut runner) {
            eprintln!("error: {err}");
        }
        return;
    }

    if let Err(err) = run_tui(&mut runner) {
        eprintln!("error: {err}");
    }
}

#[derive(Debug, Clone)]
struct CliArgs {
    dump_memo: bool,
    memo_best_only: bool,
    engine: EngineMode,
    database: String,
    user: String,
    default_schema: String,
    sql_parts: Vec<String>,
}

impl CliArgs {
    fn parse(args: Vec<String>) -> chryso::ChrysoResult<Self> {
        let mut dump_memo = false;
        let mut memo_best_only = false;
        let mut engine = EngineMode::Sql;
        let mut database = "default".to_string();
        let mut user = "default".to_string();
        let mut default_schema = "main".to_string();
        let mut sql_parts = Vec::new();

        let mut iter = args.into_iter().peekable();
        while let Some(arg) = iter.next() {
            match arg.as_str() {
                "--dump-memo" => dump_memo = true,
                "--memo-best-only" => memo_best_only = true,
                "--engine" => {
                    let Some(value) = iter.next() else {
                        return Err(chryso::ChrysoError::new("--engine expects a value"));
                    };
                    engine = EngineMode::parse(&value)?;
                }
                "--db" | "--database" => {
                    let Some(value) = iter.next() else {
                        return Err(chryso::ChrysoError::new("--db expects a value"));
                    };
                    database = value;
                }
                "--user" => {
                    let Some(value) = iter.next() else {
                        return Err(chryso::ChrysoError::new("--user expects a value"));
                    };
                    user = value;
                }
                "--schema" => {
                    let Some(value) = iter.next() else {
                        return Err(chryso::ChrysoError::new("--schema expects a value"));
                    };
                    default_schema = value;
                }
                _ => {
                    if let Some(value) = arg.strip_prefix("--db=") {
                        database = value.to_string();
                    } else if let Some(value) = arg.strip_prefix("--engine=") {
                        engine = EngineMode::parse(value)?;
                    } else if let Some(value) = arg.strip_prefix("--user=") {
                        user = value.to_string();
                    } else if let Some(value) = arg.strip_prefix("--schema=") {
                        default_schema = value.to_string();
                    } else {
                        sql_parts.push(arg);
                    }
                }
            }
        }

        Ok(Self {
            dump_memo,
            memo_best_only,
            engine,
            database,
            user,
            default_schema,
            sql_parts,
        })
    }
}

#[derive(Debug, Clone, Copy)]
enum EngineMode {
    Sql,
    Mock,
}

impl EngineMode {
    fn parse(value: &str) -> chryso::ChrysoResult<Self> {
        match value.to_ascii_lowercase().as_str() {
            "sql" => Ok(Self::Sql),
            "mock" => Ok(Self::Mock),
            _ => Err(chryso::ChrysoError::new("--engine expects sql or mock")),
        }
    }
}

fn execute_non_interactive(sql: &str, runner: &mut PipelineRunner) -> chryso::ChrysoResult<()> {
    execute_non_interactive_with_memo(sql, runner, false, false)
}

fn execute_non_interactive_with_memo(
    sql: &str,
    runner: &mut PipelineRunner,
    dump_memo: bool,
    memo_best_only: bool,
) -> chryso::ChrysoResult<()> {
    let (statements, tail) = split_sql_with_tail(sql);
    for stmt in statements {
        print_statement_output(&stmt, runner, dump_memo, memo_best_only)?;
    }
    if !tail.trim().is_empty() {
        print_statement_output(&tail, runner, dump_memo, memo_best_only)?;
    }
    Ok(())
}

fn print_statement_output(
    sql: &str,
    runner: &mut PipelineRunner,
    dump_memo: bool,
    memo_best_only: bool,
) -> chryso::ChrysoResult<()> {
    let lines = if dump_memo {
        runner.execute_line_with_memo(sql, memo_best_only)?
    } else {
        runner.execute_line(sql)?
    };
    for line in lines {
        println!("{line}");
    }
    Ok(())
}

fn run_tui(runner: &mut PipelineRunner) -> chryso::ChrysoResult<()> {
    enable_raw_mode().map_err(|err| chryso::ChrysoError::new(err.to_string()))?;
    let mut stdout = io::stdout();
    stdout
        .execute(EnterAlternateScreen)
        .map_err(|err| chryso::ChrysoError::new(err.to_string()))?;
    let backend = ratatui::backend::CrosstermBackend::new(stdout);
    let mut terminal =
        Terminal::new(backend).map_err(|err| chryso::ChrysoError::new(err.to_string()))?;

    let mut app = AppState::default();
    let mut last_tick = Instant::now();

    loop {
        terminal
            .draw(|frame| draw_ui(frame, &app))
            .map_err(|err| chryso::ChrysoError::new(err.to_string()))?;

        let timeout = Duration::from_millis(200);
        if event::poll(timeout).map_err(|err| chryso::ChrysoError::new(err.to_string()))? {
            if let Event::Key(key) =
                event::read().map_err(|err| chryso::ChrysoError::new(err.to_string()))?
            {
                if handle_key_event(key, &mut app, runner)? == Action::Exit {
                    break;
                }
            }
        }

        if last_tick.elapsed() >= Duration::from_millis(200) {
            last_tick = Instant::now();
        }
    }

    disable_raw_mode().map_err(|err| chryso::ChrysoError::new(err.to_string()))?;
    let mut stdout = io::stdout();
    stdout
        .execute(LeaveAlternateScreen)
        .map_err(|err| chryso::ChrysoError::new(err.to_string()))?;
    Ok(())
}

fn draw_ui(frame: &mut ratatui::Frame, app: &AppState) {
    let layout = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Min(1),
            Constraint::Length(3),
            Constraint::Length(1),
        ])
        .split(frame.area());

    let output = Text::from(
        app.output
            .iter()
            .map(|line| Line::from(line.clone()))
            .collect::<Vec<_>>(),
    );
    let output_block = Paragraph::new(output)
        .block(Block::default().borders(Borders::ALL).title("Output"))
        .style(Style::default().fg(Color::White));
    frame.render_widget(output_block, layout[0]);

    let prompt = format!("> {}", app.input);
    let input_block =
        Paragraph::new(prompt).block(Block::default().borders(Borders::ALL).title("SQL"));
    frame.render_widget(input_block, layout[1]);

    let cursor_x = layout[1].x + 2 + app.cursor as u16;
    let cursor_y = layout[1].y + 1;
    frame.set_cursor_position((cursor_x, cursor_y));

    let status = format!(
        "mode={} headers={} timing={}  F1 help  Ctrl+E explain  Ctrl+T timing",
        app.state.mode.label(),
        if app.state.headers { "on" } else { "off" },
        if app.state.timing { "on" } else { "off" }
    );
    let status_block = Paragraph::new(status).style(Style::default().fg(Color::Gray));
    frame.render_widget(status_block, layout[2]);
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Action {
    Continue,
    Exit,
}

fn handle_key_event(
    key: KeyEvent,
    app: &mut AppState,
    runner: &mut PipelineRunner,
) -> chryso::ChrysoResult<Action> {
    match (key.code, key.modifiers) {
        (KeyCode::Char('c'), KeyModifiers::CONTROL) => return Ok(Action::Exit),
        (KeyCode::Char('d'), KeyModifiers::CONTROL) => return Ok(Action::Exit),
        (KeyCode::Enter, _) => {
            let line = app.input.trim().to_string();
            app.input.clear();
            app.cursor = 0;
            app.history_pos = None;
            if line.is_empty() {
                return Ok(Action::Continue);
            }
            app.history.push(line.clone());
            let mut statement = line.clone();
            if app.state.explain_next {
                statement = format!("explain {statement}");
                app.state.explain_next = false;
            }
            for output in execute_line_with_meta(&statement, runner, &mut app.state)? {
                app.push_output(output);
            }
        }
        (KeyCode::F(1), _) => {
            app.state.show_help = !app.state.show_help;
            if app.state.show_help {
                for line in help_lines() {
                    app.push_output(line);
                }
            }
        }
        (KeyCode::Char('e'), KeyModifiers::CONTROL) => {
            app.state.explain_next = true;
        }
        (KeyCode::Char('t'), KeyModifiers::CONTROL) => {
            app.state.timing = !app.state.timing;
        }
        (KeyCode::Backspace, _) => {
            if app.cursor > 0 && app.cursor <= app.input.len() {
                app.input.remove(app.cursor - 1);
                app.cursor -= 1;
            }
        }
        (KeyCode::Left, _) => {
            if app.cursor > 0 {
                app.cursor -= 1;
            }
        }
        (KeyCode::Right, _) => {
            if app.cursor < app.input.len() {
                app.cursor += 1;
            }
        }
        (KeyCode::Up, _) => {
            if app.history.is_empty() {
                return Ok(Action::Continue);
            }
            let next = match app.history_pos {
                Some(pos) if pos > 0 => pos - 1,
                Some(pos) => pos,
                None => app.history.len().saturating_sub(1),
            };
            app.history_pos = Some(next);
            app.input = app.history[next].clone();
            app.cursor = app.input.len();
        }
        (KeyCode::Down, _) => {
            if app.history.is_empty() {
                return Ok(Action::Continue);
            }
            let next = match app.history_pos {
                Some(pos) if pos + 1 < app.history.len() => pos + 1,
                _ => {
                    app.history_pos = None;
                    app.input.clear();
                    app.cursor = 0;
                    return Ok(Action::Continue);
                }
            };
            app.history_pos = Some(next);
            app.input = app.history[next].clone();
            app.cursor = app.input.len();
        }
        (KeyCode::Char(ch), KeyModifiers::NONE | KeyModifiers::SHIFT) => {
            app.input.insert(app.cursor, ch);
            app.cursor += 1;
        }
        _ => {}
    }
    Ok(Action::Continue)
}

#[derive(Default)]
struct AppState {
    input: String,
    cursor: usize,
    output: Vec<String>,
    history: Vec<String>,
    history_pos: Option<usize>,
    state: CliState,
}

impl AppState {
    fn push_output(&mut self, line: String) {
        self.output.push(line);
        if self.output.len() > 2000 {
            let drain = self.output.len() - 2000;
            self.output.drain(0..drain);
        }
    }
}

#[derive(Debug, Clone, Copy)]
enum OutputMode {
    Table,
    Csv,
    Tsv,
}

impl OutputMode {
    fn label(self) -> &'static str {
        match self {
            OutputMode::Table => "table",
            OutputMode::Csv => "csv",
            OutputMode::Tsv => "tsv",
        }
    }
}

#[derive(Debug, Clone)]
struct CliState {
    headers: bool,
    mode: OutputMode,
    timing: bool,
    explain_next: bool,
    show_help: bool,
    explain_verbose: bool,
}

impl Default for CliState {
    fn default() -> Self {
        Self {
            headers: true,
            mode: OutputMode::Table,
            timing: false,
            explain_next: false,
            show_help: false,
            explain_verbose: false,
        }
    }
}

fn execute_line_with_meta(
    line: &str,
    runner: &mut PipelineRunner,
    state: &mut CliState,
) -> chryso::ChrysoResult<Vec<String>> {
    if line.starts_with('.') {
        return handle_meta_command(line, runner, state);
    }
    runner.execute_line_with_state(line, state)
}

fn handle_meta_command(
    command: &str,
    runner: &mut PipelineRunner,
    state: &mut CliState,
) -> chryso::ChrysoResult<Vec<String>> {
    let mut parts = command.split_whitespace();
    let Some(name) = parts.next() else {
        return Ok(Vec::new());
    };
    match name {
        ".tables" => runner.execute_line_with_state(
            "select table_name from information_schema.tables where table_schema = 'main' order by table_name",
            state,
        ),
        ".schema" => {
            if let Some(table) = parts.next() {
                print_table_schema(table, runner, state)
            } else {
                let list = runner.execute_line_with_state(
                    "select table_name from information_schema.tables where table_schema = 'main' order by table_name",
                    state,
                )?;
                let mut output = Vec::new();
                for line in list {
                    if line.is_empty() || line.contains("|") {
                        continue;
                    }
                    for schema_line in print_table_schema(&line, runner, state)? {
                        output.push(schema_line);
                    }
                }
                Ok(output)
            }
        }
        ".read" => {
            let Some(path) = parts.next() else {
                return Err(chryso::ChrysoError::new(".read expects a file path"));
            };
            let content = std::fs::read_to_string(path)
                .map_err(|err| chryso::ChrysoError::new(format!("read failed: {err}")))?;
            execute_script(&content, runner, state)
        }
        ".explain" => {
            let rest = parts.collect::<Vec<_>>().join(" ");
            if rest.trim().is_empty() {
                return Err(chryso::ChrysoError::new(".explain expects SQL"));
            }
            runner.execute_line_with_state(&format!("explain {rest}"), state)
        }
        ".timing" => {
            let value = parts.next().unwrap_or("");
            match value {
                "on" => {
                    state.timing = true;
                    Ok(Vec::new())
                }
                "off" => {
                    state.timing = false;
                    Ok(Vec::new())
                }
                _ => Err(chryso::ChrysoError::new(".timing expects on or off")),
            }
        }
        ".headers" => {
            let value = parts.next().unwrap_or("");
            match value {
                "on" => {
                    state.headers = true;
                    Ok(Vec::new())
                }
                "off" => {
                    state.headers = false;
                    Ok(Vec::new())
                }
                _ => Err(chryso::ChrysoError::new(".headers expects on or off")),
            }
        }
        ".mode" => {
            let value = parts.next().unwrap_or("");
            match value {
                "table" => {
                    state.mode = OutputMode::Table;
                    Ok(Vec::new())
                }
                "csv" => {
                    state.mode = OutputMode::Csv;
                    Ok(Vec::new())
                }
                "tsv" => {
                    state.mode = OutputMode::Tsv;
                    Ok(Vec::new())
                }
                _ => Err(chryso::ChrysoError::new(".mode expects table, csv, or tsv")),
            }
        }
        ".explain-mode" => {
            let value = parts.next().unwrap_or("");
            match value {
                "brief" => {
                    state.explain_verbose = false;
                    Ok(vec!["explain mode: brief (default)".to_string()])
                }
                "verbose" => {
                    state.explain_verbose = true;
                    Ok(vec!["explain mode: verbose (with cardinality)".to_string()])
                }
                _ => Err(chryso::ChrysoError::new(".explain-mode expects brief or verbose")),
            }
        }
        ".help" => Ok(help_lines()),
        ".exit" | "\\q" => Ok(vec!["exit".to_string()]),
        _ => Err(chryso::ChrysoError::new("unknown meta command")),
    }
}

fn execute_script(
    script: &str,
    runner: &mut PipelineRunner,
    state: &mut CliState,
) -> chryso::ChrysoResult<Vec<String>> {
    let (statements, tail) = split_sql_with_tail(script);
    let mut output = Vec::new();
    for stmt in statements {
        output.extend(runner.execute_line_with_state(&stmt, state)?);
    }
    if !tail.trim().is_empty() {
        output.extend(runner.execute_line_with_state(&tail, state)?);
    }
    Ok(output)
}

fn print_table_schema(
    table: &str,
    runner: &mut PipelineRunner,
    state: &mut CliState,
) -> chryso::ChrysoResult<Vec<String>> {
    let sql = format!(
        "select column_name, data_type from information_schema.columns where table_schema = 'main' and table_name = '{table}' order by ordinal_position"
    );
    let mut output = vec![table.to_string()];
    output.extend(runner.execute_line_with_state(&sql, state)?);
    Ok(output)
}

struct PipelineRunner {
    adapter: Adapter,
    parser: SimpleParser,
    optimizer: CascadesOptimizer,
    stats: StatsCache,
    context: SessionContext,
}

impl PipelineRunner {
    const BRIEF_EXPLAIN_MAX_EXPR_LENGTH: usize = 60;
    const VERBOSE_EXPLAIN_MAX_EXPR_LENGTH: usize = 80;

    fn new(engine: EngineMode) -> chryso::ChrysoResult<Self> {
        let adapter = build_adapter(engine)?;
        let parser = SimpleParser::new(ParserConfig {
            dialect: Dialect::Postgres,
        });
        let mut config = OptimizerConfig::default();
        if let Adapter::Duck(adapter) = &adapter {
            config.stats_provider = Some(adapter.clone());
        }
        let optimizer = CascadesOptimizer::new(config);
        Ok(Self {
            adapter,
            parser,
            optimizer,
            stats: StatsCache::new(),
            context: SessionContext::default(),
        })
    }

    fn execute_line(&mut self, sql: &str) -> chryso::ChrysoResult<Vec<String>> {
        let state = CliState::default();
        self.execute_line_with_state(sql, &state)
    }

    fn execute_line_with_state(
        &mut self,
        sql: &str,
        state: &CliState,
    ) -> chryso::ChrysoResult<Vec<String>> {
        let start = Instant::now();
        let statement = self.parser.parse(sql)?;
        let mut output = match statement {
            Statement::Explain(inner) => {
                let logical = PlanBuilder::build(*inner)?;
                let physical = self.optimizer.optimize(&logical, &mut self.stats);

                // Use the new formatted explain output with real cost model and stats
                let config = ExplainConfig {
                    show_types: true,
                    show_costs: true,
                    show_cardinality: state.explain_verbose, // Only show cardinality in verbose mode
                    compact: !state.explain_verbose,         // Use compact format unless verbose
                    max_expr_length: if state.explain_verbose {
                        Self::VERBOSE_EXPLAIN_MAX_EXPR_LENGTH
                    } else {
                        Self::BRIEF_EXPLAIN_MAX_EXPR_LENGTH
                    },
                };
                let formatter = ExplainFormatter::new(config);
                let logical_output = if state.explain_verbose {
                    formatter.format_logical_plan_with_stats(
                        &logical,
                        &chryso_metadata::type_inference::SimpleTypeInferencer,
                        &self.stats,
                    )
                } else {
                    formatter.format_logical_plan(
                        &logical,
                        &chryso_metadata::type_inference::SimpleTypeInferencer,
                    )
                };

                let cost_model = chryso::optimizer::cost::StatsCostModel::new(&self.stats);
                let physical_output = if state.explain_verbose {
                    formatter.format_physical_plan_with_stats(&physical, &cost_model, &self.stats)
                } else {
                    formatter.format_physical_plan(&physical, &cost_model)
                };

                let mut lines = Vec::new();
                lines.extend(logical_output.lines().map(|line: &str| line.to_string()));
                lines.push(String::new());
                lines.extend(physical_output.lines().map(|line: &str| line.to_string()));
                Ok(lines)
            }
            Statement::Analyze(analyze) => {
                self.adapter
                    .analyze_table(&analyze.table, &mut self.stats)?;
                Ok(vec!["ok".to_string()])
            }
            _ => {
                let env = StatementEnvelope::new(statement, StatementContext::new(sql));
                let authorizer = CliAuthorizer;
                let ddl_handler = ddl_handler(&self.adapter);
                match plan_with_hooks(&self.context, &env, &authorizer, ddl_handler)? {
                    PlanOutcome::DdlHandled { result } => Ok(format_ddl_result(&result)),
                    PlanOutcome::Planned { logical } => {
                        let physical = self.optimizer.optimize(&logical, &mut self.stats);
                        let result = self.adapter.execute(&physical)?;
                        Ok(format_result(&result, state))
                    }
                }
            }
        }?;
        if state.timing {
            let elapsed = start.elapsed().as_millis();
            output.push(format!("time_ms={elapsed}"));
        }
        Ok(output)
    }

    fn execute_line_with_memo(
        &mut self,
        sql: &str,
        best_only: bool,
    ) -> chryso::ChrysoResult<Vec<String>> {
        let statement = self.parser.parse(sql)?;
        match statement {
            Statement::Explain(inner) => {
                let logical = PlanBuilder::build(*inner)?;
                let physical = self.optimizer.optimize(&logical, &mut self.stats);
                let config = ExplainConfig {
                    show_types: true,
                    show_costs: true,
                    show_cardinality: false,
                    compact: true,
                    max_expr_length: Self::BRIEF_EXPLAIN_MAX_EXPR_LENGTH,
                };
                let formatter = ExplainFormatter::new(config);
                let logical_output = formatter.format_logical_plan(
                    &logical,
                    &chryso_metadata::type_inference::SimpleTypeInferencer,
                );
                let cost_model = chryso::optimizer::cost::StatsCostModel::new(&self.stats);
                let physical_output = formatter.format_physical_plan(&physical, &cost_model);
                let mut lines = Vec::new();
                lines.extend(logical_output.lines().map(|line| line.to_string()));
                lines.push(String::new());
                lines.extend(physical_output.lines().map(|line| line.to_string()));
                Ok(lines)
            }
            Statement::Analyze(analyze) => {
                self.adapter
                    .analyze_table(&analyze.table, &mut self.stats)?;
                Ok(vec!["ok".to_string()])
            }
            other => {
                let env = StatementEnvelope::new(other, StatementContext::new(sql));
                let authorizer = CliAuthorizer;
                let ddl_handler = ddl_handler(&self.adapter);
                match plan_with_hooks(&self.context, &env, &authorizer, ddl_handler)? {
                    PlanOutcome::DdlHandled { result } => Ok(format_ddl_result(&result)),
                    PlanOutcome::Planned { logical } => {
                        let (physical, memo) = self
                            .optimizer
                            .optimize_with_memo_trace(&logical, &mut self.stats);
                        let trace = if best_only {
                            memo.format_best_only()
                        } else {
                            memo.format_full()
                        };
                        let cost_model = chryso::optimizer::cost::StatsCostModel::new(&self.stats);
                        let physical_output = physical.explain_costed(0, &cost_model);
                        let mut lines = Vec::new();
                        lines.extend(trace.lines().map(|line| line.to_string()));
                        if !physical_output.is_empty() {
                            lines.push(String::new());
                            lines.extend(physical_output.lines().map(|line| line.to_string()));
                        }
                        Ok(lines)
                    }
                }
            }
        }
    }
}

fn build_adapter(engine: EngineMode) -> chryso::ChrysoResult<Adapter> {
    match engine {
        EngineMode::Sql => {
            DuckDbAdapter::try_new().map(|duck| Adapter::Duck(std::sync::Arc::new(duck)))
        }
        EngineMode::Mock => Ok(Adapter::Mock(MockAdapter::new())),
    }
}

enum Adapter {
    Duck(std::sync::Arc<DuckDbAdapter>),
    Mock(MockAdapter),
}

impl Adapter {
    fn execute(&self, plan: &chryso::PhysicalPlan) -> chryso::ChrysoResult<chryso::QueryResult> {
        match self {
            Adapter::Duck(adapter) => adapter.execute(plan),
            Adapter::Mock(adapter) => adapter.execute(plan),
        }
    }

    fn analyze_table(
        &self,
        table: &str,
        stats: &mut StatsCache,
    ) -> chryso::ChrysoResult<()> {
        match self {
            Adapter::Duck(adapter) => adapter.analyze_table(table, stats),
            Adapter::Mock(_) => Err(chryso::ChrysoError::new(
                "analyze requires duckdb adapter",
            )),
        }
    }
}

impl DdlHandler<SessionContext, NoExtension> for Adapter {
    fn supports(&self, _ctx: &SessionContext, env: &StatementEnvelope<NoExtension>) -> bool {
        if env.category != StatementCategory::Ddl {
            return false;
        }
        match self {
            Adapter::Duck(_) => true,
            Adapter::Mock(_) => false,
        }
    }

    fn handle(
        &self,
        _ctx: &SessionContext,
        env: &StatementEnvelope<NoExtension>,
    ) -> chryso::ChrysoResult<DdlResult> {
        let sql = chryso::sql_format::format_statement(&env.statement);
        match self {
            Adapter::Duck(_) => {
                let plan = chryso::PhysicalPlan::Dml { sql: sql.clone() };
                let _ = self.execute(&plan)?;
                Ok(DdlResult { detail: None })
            }
            Adapter::Mock(_) => Err(chryso::ChrysoError::new("ddl requires duckdb adapter")),
        }
    }
}

fn ddl_handler(adapter: &Adapter) -> Option<&dyn DdlHandler<SessionContext, NoExtension>> {
    Some(adapter)
}

struct CliAuthorizer;

impl Authorizer<SessionContext, NoExtension> for CliAuthorizer {
    fn authorize_statement(
        &self,
        _ctx: &SessionContext,
        _env: &StatementEnvelope<NoExtension>,
    ) -> chryso::ChrysoResult<()> {
        Ok(())
    }
}

fn format_ddl_result(result: &DdlResult) -> Vec<String> {
    match result.detail.as_deref() {
        Some(detail) if !detail.is_empty() => vec![detail.to_string()],
        _ => vec!["ok".to_string()],
    }
}

fn format_result(result: &chryso::QueryResult, state: &CliState) -> Vec<String> {
    if result.columns.is_empty() {
        return vec!["ok".to_string()];
    }
    let mut output = Vec::new();
    match state.mode {
        OutputMode::Table => {
            if state.headers {
                output.push(result.columns.join(" | "));
            }
            for row in &result.rows {
                output.push(row.join(" | "));
            }
        }
        OutputMode::Csv => {
            if state.headers {
                output.push(
                    result
                        .columns
                        .iter()
                        .map(|value| csv_escape(value))
                        .collect::<Vec<_>>()
                        .join(","),
                );
            }
            for row in &result.rows {
                output.push(
                    row.iter()
                        .map(|value| csv_escape(value))
                        .collect::<Vec<_>>()
                        .join(","),
                );
            }
        }
        OutputMode::Tsv => {
            if state.headers {
                output.push(result.columns.join("\t"));
            }
            for row in &result.rows {
                output.push(row.join("\t"));
            }
        }
    }
    output
}

fn csv_escape(value: &str) -> String {
    if value.contains(['"', ',', '\n', '\r']) {
        let escaped = value.replace('"', "\"\"");
        format!("\"{escaped}\"")
    } else {
        value.to_string()
    }
}

fn help_lines() -> Vec<String> {
    vec![
        ".tables".to_string(),
        ".schema [table]".to_string(),
        ".read <path>".to_string(),
        ".explain <sql>".to_string(),
        ".explain-mode brief|verbose".to_string(),
        ".headers on|off".to_string(),
        ".mode table|csv|tsv".to_string(),
        ".timing on|off".to_string(),
        ".exit|\\q".to_string(),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    struct MockRunner {
        executed: Vec<String>,
    }

    impl MockRunner {
        fn new() -> Self {
            Self {
                executed: Vec::new(),
            }
        }

        fn execute_line(&mut self, sql: &str) -> chryso::ChrysoResult<Vec<String>> {
            self.executed.push(sql.to_string());
            Ok(vec![format!("ok: {sql}")])
        }
    }

    #[test]
    fn split_sql_handles_comments_and_strings() {
        let input = "select 1; -- comment\nselect ';' as x; /* block */ select 2";
        let (statements, tail) = split_sql_with_tail(input);
        assert_eq!(statements.len(), 2);
        assert!(statements[1].contains("';'"));
        assert_eq!(tail, "select 2");
    }

    #[test]
    fn execute_script_runs_all_statements() {
        let script = "select 1; select 2;";
        let mut runner = MockRunner::new();
        let state = CliState::default();
        let (stmts, tail) = split_sql_with_tail(script);
        let mut output = Vec::new();
        for stmt in stmts {
            output.extend(runner.execute_line(&stmt).unwrap());
        }
        if !tail.is_empty() {
            output.extend(runner.execute_line(&tail).unwrap());
        }
        assert_eq!(runner.executed.len(), 2);
        assert_eq!(state.headers, true);
    }

    #[test]
    fn parse_engine_flag() {
        let args = vec![
            "--engine".to_string(),
            "sql".to_string(),
            "select".to_string(),
            "1".to_string(),
        ];
        let parsed = CliArgs::parse(args).expect("parse");
        match parsed.engine {
            EngineMode::Sql => {}
            other => panic!("unexpected engine: {other:?}"),
        }
    }

    #[test]
    fn parse_engine_default_sql() {
        let args = vec!["select".to_string(), "1".to_string()];
        let parsed = CliArgs::parse(args).expect("parse");
        match parsed.engine {
            EngineMode::Sql => {}
            other => panic!("unexpected engine: {other:?}"),
        }
    }
}
