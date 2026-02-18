use chryso_parser::{Dialect, ParserConfig, SimpleParser, SqlParser};
use chryso_parser_yacc::YaccParser;
use std::fs;
use std::path::Path;

const FILE_LIST: &str = "crates/parser/tests/testdata/postgres_regress/files.txt";
const SQL_DIR: &str = "crates/parser/tests/testdata/postgres_regress/sql";
const MIN_SELECTED_STATEMENTS: usize = 60;
const MIN_SIMPLE_PARSED: usize = 40;
const MIN_YACC_PARSED: usize = 30;

const UNSUPPORTED_SNIPPETS: &[&str] = &[
    "::",
    " using ",
    " natural join ",
    " fetch first ",
    " for update",
    " for no key",
    " skip locked",
    " distinct on ",
    " row(",
    " row_to_json(",
    " array[",
    "generate_series(",
    "pg_typeof(",
    "\\",
];

#[test]
fn postgres_regress_subset_select_with_parseable() {
    let simple_parser = SimpleParser::new(ParserConfig {
        dialect: Dialect::Postgres,
    });
    let yacc_parser = YaccParser::new(ParserConfig {
        dialect: Dialect::Postgres,
    });

    let files = load_file_list(Path::new(FILE_LIST));
    let mut selected = 0usize;
    let mut simple_parsed = 0usize;
    let mut yacc_parsed = 0usize;
    let mut simple_failures = Vec::new();
    let mut yacc_failures = Vec::new();

    for file in files {
        let path = Path::new(SQL_DIR).join(&file);
        let sql = fs::read_to_string(&path)
            .unwrap_or_else(|err| panic!("read {} failed: {err}", path.display()));
        for (idx, statement) in split_sql_statements(&sql).into_iter().enumerate() {
            if !is_candidate_statement(&statement) {
                continue;
            }
            selected += 1;
            let statement = statement.trim();

            if let Err(err) = simple_parser.parse(statement) {
                simple_failures.push(format!(
                    "{}#{} simple parse failed: {err} | {}",
                    file,
                    idx + 1,
                    abbreviate(statement)
                ));
                continue;
            }
            simple_parsed += 1;

            if let Err(err) = yacc_parser.parse(statement) {
                yacc_failures.push(format!(
                    "{}#{} yacc parse failed: {err} | {}",
                    file,
                    idx + 1,
                    abbreviate(statement)
                ));
            } else {
                yacc_parsed += 1;
            }
        }
    }

    println!(
        "postgres_regress_subset: selected={selected}, simple_parsed={simple_parsed}, yacc_parsed={yacc_parsed}"
    );

    assert!(
        selected >= MIN_SELECTED_STATEMENTS,
        "postgres regress subset selected only {selected} statements, expected at least {MIN_SELECTED_STATEMENTS}"
    );
    assert!(
        simple_parsed >= MIN_SIMPLE_PARSED,
        "postgres regress subset simple parser coverage too low: simple_parsed={simple_parsed}, expected at least {MIN_SIMPLE_PARSED}\n{}",
        simple_failures
            .iter()
            .take(20)
            .cloned()
            .collect::<Vec<_>>()
            .join("\n")
    );
    assert!(
        yacc_parsed >= MIN_YACC_PARSED,
        "postgres regress subset yacc parser coverage too low: yacc_parsed={yacc_parsed}, expected at least {MIN_YACC_PARSED}\n{}",
        yacc_failures
            .iter()
            .take(20)
            .cloned()
            .collect::<Vec<_>>()
            .join("\n")
    );
}

fn load_file_list(path: &Path) -> Vec<String> {
    let content = fs::read_to_string(path)
        .unwrap_or_else(|err| panic!("read {} failed: {err}", path.display()));
    content
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .map(|line| line.to_string())
        .collect()
}

fn is_candidate_statement(statement: &str) -> bool {
    let trimmed = statement.trim();
    if trimmed.is_empty() {
        return false;
    }
    let first = first_keyword(trimmed);
    if !matches!(first.as_deref(), Some("select") | Some("with")) {
        return false;
    }

    let lowered = trimmed.to_ascii_lowercase();
    !UNSUPPORTED_SNIPPETS
        .iter()
        .any(|snippet| lowered.contains(snippet))
}

fn first_keyword(sql: &str) -> Option<String> {
    let mut chars = sql.chars().peekable();
    while matches!(chars.peek(), Some(ch) if ch.is_whitespace()) {
        chars.next();
    }
    let mut keyword = String::new();
    while let Some(ch) = chars.peek().copied() {
        if ch.is_ascii_alphabetic() || ch == '_' {
            keyword.push(ch.to_ascii_lowercase());
            chars.next();
        } else {
            break;
        }
    }
    if keyword.is_empty() {
        None
    } else {
        Some(keyword)
    }
}

fn split_sql_statements(sql: &str) -> Vec<String> {
    let mut statements = Vec::new();
    let mut current = String::new();
    let mut chars = sql.chars().peekable();
    let mut in_single_quote = false;
    let mut in_double_quote = false;
    let mut in_line_comment = false;
    let mut in_block_comment = false;

    while let Some(ch) = chars.next() {
        if in_line_comment {
            if ch == '\n' {
                in_line_comment = false;
                current.push('\n');
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
        if !in_single_quote && !in_double_quote {
            if ch == '-' && matches!(chars.peek(), Some('-')) {
                chars.next();
                in_line_comment = true;
                continue;
            }
            if ch == '/' && matches!(chars.peek(), Some('*')) {
                chars.next();
                in_block_comment = true;
                continue;
            }
        }
        if !in_double_quote && ch == '\'' {
            current.push(ch);
            if in_single_quote && matches!(chars.peek(), Some('\'')) {
                current.push(chars.next().expect("peeked quote"));
            } else {
                in_single_quote = !in_single_quote;
            }
            continue;
        }
        if !in_single_quote && ch == '"' {
            current.push(ch);
            if in_double_quote && matches!(chars.peek(), Some('"')) {
                current.push(chars.next().expect("peeked quote"));
            } else {
                in_double_quote = !in_double_quote;
            }
            continue;
        }
        if !in_single_quote && !in_double_quote && ch == ';' {
            let trimmed = current.trim();
            if !trimmed.is_empty() {
                statements.push(trimmed.to_string());
            }
            current.clear();
            continue;
        }
        current.push(ch);
    }

    let trimmed = current.trim();
    if !trimmed.is_empty() {
        statements.push(trimmed.to_string());
    }
    statements
}

fn abbreviate(statement: &str) -> String {
    const MAX_LEN: usize = 160;
    let compact = statement.split_whitespace().collect::<Vec<_>>().join(" ");
    if compact.len() <= MAX_LEN {
        compact
    } else {
        format!("{}...", &compact[..MAX_LEN])
    }
}
