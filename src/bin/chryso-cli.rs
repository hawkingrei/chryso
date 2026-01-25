use chryso::adapter::ExecutorAdapter;
use chryso::{
    metadata::StatsCache, parser::SimpleParser, CascadesOptimizer, Dialect, DuckDbAdapter,
    MockAdapter, OptimizerConfig, ParserConfig, PlanBuilder, SqlParser, Statement,
};
use chryso::planner::{ExplainConfig, ExplainFormatter};
use crossterm::event::{self, Event, KeyCode, KeyEvent, KeyModifiers};
use crossterm::terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen};
use crossterm::ExecutableCommand;
use ratatui::layout::{Constraint, Direction, Layout};
use ratatui::style::{Color, Style};
use ratatui::text::{Line, Text};
use ratatui::widgets::{Block, Borders, Paragraph};
use ratatui::Terminal;
use std::io::{self, Read};
use std::io::IsTerminal;
use std::time::{Duration, Instant};

fn main() {
    let mut runner = PipelineRunner::new();
    let args: Vec<String> = std::env::args().skip(1).collect();
    if !args.is_empty() {
        let sql = args.join(" ");
        if let Err(err) = execute_non_interactive(&sql, &mut runner) {
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

fn execute_non_interactive(sql: &str, runner: &mut PipelineRunner) -> chryso::ChrysoResult<()> {
    let (statements, tail) = split_sql_with_tail(sql);
    for stmt in statements {
        for line in runner.execute_line(&stmt)? {
            println!("{line}");
        }
    }
    if !tail.trim().is_empty() {
        for line in runner.execute_line(&tail)? {
            println!("{line}");
        }
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
    let mut terminal = Terminal::new(backend)
        .map_err(|err| chryso::ChrysoError::new(err.to_string()))?;

    let mut app = AppState::default();
    let mut last_tick = Instant::now();

    loop {
        terminal
            .draw(|frame| draw_ui(frame, &app))
            .map_err(|err| chryso::ChrysoError::new(err.to_string()))?;

        let timeout = Duration::from_millis(200);
        if event::poll(timeout).map_err(|err| chryso::ChrysoError::new(err.to_string()))? {
            if let Event::Key(key) = event::read()
                .map_err(|err| chryso::ChrysoError::new(err.to_string()))?
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
        .constraints([Constraint::Min(1), Constraint::Length(3), Constraint::Length(1)])
        .split(frame.area());

    let output = Text::from(app.output.iter().map(|line| Line::from(line.clone())).collect::<Vec<_>>());
    let output_block = Paragraph::new(output)
        .block(Block::default().borders(Borders::ALL).title("Output"))
        .style(Style::default().fg(Color::White));
    frame.render_widget(output_block, layout[0]);

    let prompt = format!("> {}", app.input);
    let input_block = Paragraph::new(prompt)
        .block(Block::default().borders(Borders::ALL).title("SQL"));
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
}

impl PipelineRunner {
    const BRIEF_EXPLAIN_MAX_EXPR_LENGTH: usize = 60;
    const VERBOSE_EXPLAIN_MAX_EXPR_LENGTH: usize = 80;

    fn new() -> Self {
        let adapter = DuckDbAdapter::try_new()
            .map(Adapter::Duck)
            .unwrap_or_else(|_| Adapter::Mock(MockAdapter::new()));
        let parser = SimpleParser::new(ParserConfig {
            dialect: Dialect::Postgres,
        });
        let optimizer = CascadesOptimizer::new(OptimizerConfig::default());
        Self {
            adapter,
            parser,
            optimizer,
            stats: StatsCache::new(),
        }
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
                    show_cardinality: state.explain_verbose,  // Only show cardinality in verbose mode
                    compact: !state.explain_verbose,          // Use compact format unless verbose
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
            _ => {
                let logical = PlanBuilder::build(statement)?;
                let physical = self.optimizer.optimize(&logical, &mut self.stats);
                let result = self.adapter.execute(&physical)?;
                Ok(format_result(&result, state))
            }
        }?;
        if state.timing {
            let elapsed = start.elapsed().as_millis();
            output.push(format!("time_ms={elapsed}"));
        }
        Ok(output)
    }
}

enum Adapter {
    Duck(DuckDbAdapter),
    Mock(MockAdapter),
}

impl Adapter {
    fn execute(
        &self,
        plan: &chryso::PhysicalPlan,
    ) -> chryso::ChrysoResult<chryso::QueryResult> {
        match self {
            Adapter::Duck(adapter) => adapter.execute(plan),
            Adapter::Mock(adapter) => adapter.execute(plan),
        }
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

fn split_sql_with_tail(input: &str) -> (Vec<String>, String) {
    let mut statements = Vec::new();
    let mut current = String::new();
    let mut chars = input.chars().peekable();
    let mut in_single = false;
    let mut in_double = false;
    let mut in_line_comment = false;
    let mut in_block_comment = false;

    while let Some(ch) = chars.next() {
        if in_line_comment {
            if ch == '\n' {
                in_line_comment = false;
                current.push(ch);
            }
            continue;
        }
        if in_block_comment {
            if ch == '*' && matches!(chars.peek(), Some('/')) {
                chars.next();
                in_block_comment = false;
            }
            continue;
        }
        if !in_single && !in_double {
            if ch == '-' && matches!(chars.peek(), Some('-')) {
                chars.next();
                in_line_comment = true;
                continue;
            }
            if ch == '#' {
                in_line_comment = true;
                continue;
            }
            if ch == '/' && matches!(chars.peek(), Some('*')) {
                chars.next();
                in_block_comment = true;
                continue;
            }
        }
        if ch == '\'' && !in_double {
            if in_single && matches!(chars.peek(), Some('\'')) {
                current.push(ch);
                current.push('\'');
                chars.next();
                continue;
            }
            in_single = !in_single;
            current.push(ch);
            continue;
        }
        if ch == '"' && !in_single {
            if in_double && matches!(chars.peek(), Some('"')) {
                current.push(ch);
                current.push('"');
                chars.next();
                continue;
            }
            in_double = !in_double;
            current.push(ch);
            continue;
        }
        if ch == ';' && !in_single && !in_double {
            let stmt = current.trim();
            if !stmt.is_empty() {
                statements.push(stmt.to_string());
            }
            current.clear();
            continue;
        }
        current.push(ch);
    }

    (statements, current.trim().to_string())
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
            Self { executed: Vec::new() }
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
}
