use chryso_parser::{Dialect, ParserConfig, SimpleParser, SqlParser};
use chryso_parser_yacc::YaccParser;
use std::fs;
use std::path::Path;

const FILE_LIST: &str = "crates/parser/tests/testdata/postgres_regress/files.txt";
const SQL_DIR: &str = "crates/parser/tests/testdata/postgres_regress/sql";

// These thresholds keep the test stable while still enforcing corpus value.
const MIN_SELECTED_STATEMENTS: usize = 680;
const MIN_SIMPLE_PARSED: usize = 560;
const MIN_YACC_PARSED: usize = 190;

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

            if let Err(err) = simple_parser.parse(&statement) {
                simple_failures.push(format!(
                    "{}#{} simple parse failed: {err} | {}",
                    file,
                    idx + 1,
                    abbreviate(&statement)
                ));
                continue;
            }
            simple_parsed += 1;

            if let Err(err) = yacc_parser.parse(&statement) {
                yacc_failures.push(format!(
                    "{}#{} yacc parse failed: {err} | {}",
                    file,
                    idx + 1,
                    abbreviate(&statement)
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
            .take(40)
            .cloned()
            .collect::<Vec<_>>()
            .join("\n")
    );
    assert!(
        yacc_parsed >= MIN_YACC_PARSED,
        "postgres regress subset yacc parser coverage too low: yacc_parsed={yacc_parsed}, expected at least {MIN_YACC_PARSED}\n{}",
        yacc_failures
            .iter()
            .take(40)
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
        .filter(|line| !line.is_empty() && !line.starts_with('#'))
        .map(ToString::to_string)
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

    // Current AST keeps LIMIT/OFFSET as numeric values only.
    if lowered.contains("limit (") || lowered.contains("offset (") {
        return false;
    }

    true
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

    let chars: Vec<char> = sql.chars().collect();
    let mut i = 0usize;

    let mut in_single_quote = false;
    let mut in_double_quote = false;
    let mut in_line_comment = false;
    let mut in_block_comment = false;
    let mut dollar_quote_tag: Option<String> = None;

    while i < chars.len() {
        let ch = chars[i];

        if in_line_comment {
            if ch == '\n' {
                in_line_comment = false;
            }
            i += 1;
            continue;
        }

        if in_block_comment {
            if ch == '*' && i + 1 < chars.len() && chars[i + 1] == '/' {
                in_block_comment = false;
                i += 2;
            } else {
                i += 1;
            }
            continue;
        }

        if let Some(tag) = &dollar_quote_tag {
            if starts_with_at(&chars, i, tag) {
                current.push_str(tag);
                i += tag.len();
                dollar_quote_tag = None;
            } else {
                current.push(ch);
                i += 1;
            }
            continue;
        }

        if !in_single_quote && !in_double_quote {
            if ch == '-' && i + 1 < chars.len() && chars[i + 1] == '-' {
                in_line_comment = true;
                i += 2;
                continue;
            }

            if ch == '/' && i + 1 < chars.len() && chars[i + 1] == '*' {
                in_block_comment = true;
                i += 2;
                continue;
            }

            if ch == '$' {
                if let Some(tag) = read_dollar_tag(&chars, i) {
                    current.push_str(&tag);
                    i += tag.len();
                    dollar_quote_tag = Some(tag);
                    continue;
                }
            }
        }

        if ch == '\'' && !in_double_quote {
            current.push(ch);
            if in_single_quote && i + 1 < chars.len() && chars[i + 1] == '\'' {
                current.push(chars[i + 1]);
                i += 2;
            } else {
                in_single_quote = !in_single_quote;
                i += 1;
            }
            continue;
        }

        if ch == '"' && !in_single_quote {
            current.push(ch);
            if in_double_quote && i + 1 < chars.len() && chars[i + 1] == '"' {
                current.push(chars[i + 1]);
                i += 2;
            } else {
                in_double_quote = !in_double_quote;
                i += 1;
            }
            continue;
        }

        if !in_single_quote && !in_double_quote && ch == ';' {
            let trimmed = current.trim();
            if !trimmed.is_empty() {
                statements.push(trimmed.to_string());
            }
            current.clear();
            i += 1;
            continue;
        }

        current.push(ch);
        i += 1;
    }

    let trimmed = current.trim();
    if !trimmed.is_empty() {
        statements.push(trimmed.to_string());
    }

    statements
}

fn read_dollar_tag(chars: &[char], start: usize) -> Option<String> {
    if chars[start] != '$' {
        return None;
    }

    let mut i = start + 1;
    while i < chars.len() {
        let ch = chars[i];
        if ch == '$' {
            let tag: String = chars[start..=i].iter().collect();
            return Some(tag);
        }
        if !(ch.is_ascii_alphanumeric() || ch == '_') {
            return None;
        }
        i += 1;
    }
    None
}

fn starts_with_at(chars: &[char], at: usize, pat: &str) -> bool {
    let pat_chars: Vec<char> = pat.chars().collect();
    if at + pat_chars.len() > chars.len() {
        return false;
    }
    chars[at..at + pat_chars.len()] == pat_chars
}

fn abbreviate(statement: &str) -> String {
    const MAX_LEN: usize = 140;
    let one_line = statement.replace('\n', " ").replace('\t', " ");
    let collapsed = one_line.split_whitespace().collect::<Vec<_>>().join(" ");
    if collapsed.len() <= MAX_LEN {
        collapsed
    } else {
        format!("{}...", &collapsed[..MAX_LEN])
    }
}
