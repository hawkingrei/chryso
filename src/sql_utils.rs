pub fn split_sql_with_tail(input: &str) -> (Vec<String>, String) {
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
