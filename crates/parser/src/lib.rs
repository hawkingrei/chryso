use corundum_core::ast::{
    BinaryOperator, Expr, Join, JoinType, Literal, OrderByExpr, SelectItem, SelectStatement,
    Statement, TableRef, UnaryOperator,
};
use corundum_core::{CorundumError, CorundumResult};

#[derive(Debug, Clone, Copy)]
pub enum Dialect {
    Postgres,
    MySql,
}

#[derive(Debug, Clone)]
pub struct ParserConfig {
    pub dialect: Dialect,
}

impl Default for ParserConfig {
    fn default() -> Self {
        Self {
            dialect: Dialect::Postgres,
        }
    }
}

pub trait SqlParser {
    fn parse(&self, sql: &str) -> CorundumResult<Statement>;
}

pub struct SimpleParser {
    config: ParserConfig,
}

impl SimpleParser {
    pub fn new(config: ParserConfig) -> Self {
        Self { config }
    }
}

impl SqlParser for SimpleParser {
    fn parse(&self, sql: &str) -> CorundumResult<Statement> {
        let tokens = tokenize(sql, self.config.dialect)?;
        let mut parser = Parser::new(tokens, self.config.dialect);
        let stmt = parser.parse_statement()?;
        Ok(corundum_core::ast::normalize_statement(&stmt))
    }
}

#[derive(Debug, Clone, PartialEq)]
enum Token {
    Ident(String),
    Number(String),
    String(String),
    Comma,
    Dot,
    Star,
    LParen,
    RParen,
    Eq,
    NotEq,
    Lt,
    LtEq,
    Gt,
    GtEq,
    Plus,
    Minus,
    Slash,
    Keyword(Keyword),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Keyword {
    Select,
    Explain,
    Create,
    Table,
    Analyze,
    Insert,
    Into,
    Values,
    Update,
    Set,
    Delete,
    Over,
    Partition,
    Exists,
    In,
    From,
    Where,
    And,
    Or,
    Not,
    As,
    Join,
    Left,
    On,
    Group,
    By,
    Having,
    Order,
    Offset,
    Limit,
    Asc,
    Desc,
    Distinct,
    True,
    False,
}

struct Parser {
    tokens: Vec<Token>,
    pos: usize,
    _dialect: Dialect,
}

impl Parser {
    fn new(tokens: Vec<Token>, dialect: Dialect) -> Self {
        Self {
            tokens,
            pos: 0,
            _dialect: dialect,
        }
    }

    fn parse_statement(&mut self) -> CorundumResult<Statement> {
        if self.consume_keyword(Keyword::Explain) {
            let statement = self.parse_explain_statement()?;
            Ok(Statement::Explain(Box::new(statement)))
        } else if self.consume_keyword(Keyword::Analyze) {
            let statement = self.parse_analyze_statement()?;
            Ok(statement)
        } else if self.consume_keyword(Keyword::Insert) {
            let statement = self.parse_insert_statement()?;
            Ok(statement)
        } else if self.consume_keyword(Keyword::Update) {
            let statement = self.parse_update_statement()?;
            Ok(statement)
        } else if self.consume_keyword(Keyword::Delete) {
            let statement = self.parse_delete_statement()?;
            Ok(statement)
        } else if self.consume_keyword(Keyword::Create) {
            let statement = self.parse_create_statement()?;
            Ok(statement)
        } else if self.consume_keyword(Keyword::Select) {
            let select = self.parse_select()?;
            Ok(Statement::Select(select))
        } else {
            Err(CorundumError::new("only SELECT is supported"))
        }
    }

    fn parse_explain_statement(&mut self) -> CorundumResult<Statement> {
        if self.consume_keyword(Keyword::Select) {
            let select = self.parse_select()?;
            Ok(Statement::Select(select))
        } else {
            Err(CorundumError::new("EXPLAIN expects SELECT"))
        }
    }

    fn parse_create_statement(&mut self) -> CorundumResult<Statement> {
        if self.consume_keyword(Keyword::Table) {
            let name = self.expect_identifier()?;
            Ok(Statement::CreateTable(corundum_core::ast::CreateTableStatement { name }))
        } else {
            Err(CorundumError::new("only CREATE TABLE is supported"))
        }
    }

    fn parse_analyze_statement(&mut self) -> CorundumResult<Statement> {
        let name = self.expect_identifier()?;
        Ok(Statement::Analyze(corundum_core::ast::AnalyzeStatement { table: name }))
    }

    fn parse_insert_statement(&mut self) -> CorundumResult<Statement> {
        self.expect_keyword(Keyword::Into)?;
        let table = self.expect_identifier()?;
        let columns = if self.consume_token(&Token::LParen) {
            let columns = self.parse_identifier_list()?;
            self.expect_token(Token::RParen)?;
            columns
        } else {
            Vec::new()
        };
        self.expect_keyword(Keyword::Values)?;
        let mut values = Vec::new();
        loop {
            self.expect_token(Token::LParen)?;
            let row = self.parse_expr_list()?;
            self.expect_token(Token::RParen)?;
            values.push(row);
            if !self.consume_token(&Token::Comma) {
                break;
            }
        }
        Ok(Statement::Insert(corundum_core::ast::InsertStatement {
            table,
            columns,
            values,
        }))
    }

    fn parse_update_statement(&mut self) -> CorundumResult<Statement> {
        let table = self.expect_identifier()?;
        self.expect_keyword(Keyword::Set)?;
        let assignments = self.parse_assignments()?;
        let selection = if self.consume_keyword(Keyword::Where) {
            Some(self.parse_expr()?)
        } else {
            None
        };
        Ok(Statement::Update(corundum_core::ast::UpdateStatement {
            table,
            assignments,
            selection,
        }))
    }

    fn parse_delete_statement(&mut self) -> CorundumResult<Statement> {
        self.expect_keyword(Keyword::From)?;
        let table = self.expect_identifier()?;
        let selection = if self.consume_keyword(Keyword::Where) {
            Some(self.parse_expr()?)
        } else {
            None
        };
        Ok(Statement::Delete(corundum_core::ast::DeleteStatement { table, selection }))
    }

    fn parse_select(&mut self) -> CorundumResult<SelectStatement> {
        let distinct = self.consume_keyword(Keyword::Distinct);
        let projection = self.parse_projection()?;
        self.expect_keyword(Keyword::From)?;
        let from = self.parse_table_ref()?;
        let selection = if self.consume_keyword(Keyword::Where) {
            Some(self.parse_expr()?)
        } else {
            None
        };
        let group_by = if self.consume_keyword(Keyword::Group) {
            self.expect_keyword(Keyword::By)?;
            self.parse_expr_list()?
        } else {
            Vec::new()
        };
        let having = if self.consume_keyword(Keyword::Having) {
            Some(self.parse_expr()?)
        } else {
            None
        };
        let order_by = if self.consume_keyword(Keyword::Order) {
            self.expect_keyword(Keyword::By)?;
            self.parse_order_by_list()?
        } else {
            Vec::new()
        };
        let limit = if self.consume_keyword(Keyword::Limit) {
            Some(self.parse_limit_value()?)
        } else {
            None
        };
        let offset = if self.consume_keyword(Keyword::Offset) {
            Some(self.parse_limit_value()?)
        } else {
            None
        };
        Ok(SelectStatement {
            distinct,
            projection,
            from,
            selection,
            group_by,
            having,
            order_by,
            limit,
            offset,
        })
    }

    fn parse_projection(&mut self) -> CorundumResult<Vec<SelectItem>> {
        let mut items = Vec::new();
        loop {
            let expr = if self.consume_token(&Token::Star) {
                Expr::Wildcard
            } else {
                self.parse_expr()?
            };
            let alias = if self.consume_keyword(Keyword::As) {
                Some(self.expect_identifier()?)
            } else if let Some(Token::Ident(name)) = self.peek().cloned() {
                if !self.is_clause_boundary() {
                    self.next();
                    Some(name)
                } else {
                    None
                }
            } else {
                None
            };
            items.push(SelectItem { expr, alias });
            if !self.consume_token(&Token::Comma) {
                break;
            }
        }
        Ok(items)
    }

    fn parse_table_ref(&mut self) -> CorundumResult<TableRef> {
        let name = self.expect_identifier()?;
        let alias = if self.consume_keyword(Keyword::As) {
            Some(self.expect_identifier()?)
        } else if let Some(Token::Ident(_)) = self.peek() {
            if !self.is_join_boundary() {
                Some(self.expect_identifier()?)
            } else {
                None
            }
        } else {
            None
        };
        let mut table = TableRef {
            name,
            alias,
            joins: Vec::new(),
        };
        loop {
            let join_type = if self.consume_keyword(Keyword::Join) {
                JoinType::Inner
            } else if self.consume_keyword(Keyword::Left) {
                self.expect_keyword(Keyword::Join)?;
                JoinType::Left
            } else {
                break;
            };
            let right = self.parse_table_ref()?;
            self.expect_keyword(Keyword::On)?;
            let on = self.parse_expr()?;
            table.joins.push(Join {
                join_type,
                right,
                on,
            });
        }
        Ok(table)
    }

    fn parse_order_by_list(&mut self) -> CorundumResult<Vec<OrderByExpr>> {
        let mut items = Vec::new();
        loop {
            let expr = self.parse_expr()?;
            let asc = if self.consume_keyword(Keyword::Asc) {
                true
            } else if self.consume_keyword(Keyword::Desc) {
                false
            } else {
                true
            };
            items.push(OrderByExpr { expr, asc });
            if !self.consume_token(&Token::Comma) {
                break;
            }
        }
        Ok(items)
    }

    fn parse_limit_value(&mut self) -> CorundumResult<u64> {
        if let Some(Token::Number(value)) = self.next() {
            value
                .parse()
                .map_err(|_| CorundumError::new("invalid LIMIT value"))
        } else {
            Err(CorundumError::new("LIMIT expects a number"))
        }
    }

    fn parse_expr_list(&mut self) -> CorundumResult<Vec<Expr>> {
        let mut items = Vec::new();
        loop {
            items.push(self.parse_expr()?);
            if !self.consume_token(&Token::Comma) {
                break;
            }
        }
        Ok(items)
    }

    fn parse_expr(&mut self) -> CorundumResult<Expr> {
        self.parse_or()
    }

    fn parse_or(&mut self) -> CorundumResult<Expr> {
        let mut expr = self.parse_and()?;
        while self.consume_keyword(Keyword::Or) {
            let rhs = self.parse_and()?;
            expr = Expr::BinaryOp {
                left: Box::new(expr),
                op: BinaryOperator::Or,
                right: Box::new(rhs),
            };
        }
        Ok(expr)
    }

    fn parse_and(&mut self) -> CorundumResult<Expr> {
        let mut expr = self.parse_comparison()?;
        while self.consume_keyword(Keyword::And) {
            let rhs = self.parse_comparison()?;
            expr = Expr::BinaryOp {
                left: Box::new(expr),
                op: BinaryOperator::And,
                right: Box::new(rhs),
            };
        }
        Ok(expr)
    }

    fn parse_comparison(&mut self) -> CorundumResult<Expr> {
        let mut expr = self.parse_additive()?;
        loop {
            let op = if self.consume_token(&Token::Eq) {
                Some(BinaryOperator::Eq)
            } else if self.consume_token(&Token::NotEq) {
                Some(BinaryOperator::NotEq)
            } else if self.consume_token(&Token::LtEq) {
                Some(BinaryOperator::LtEq)
            } else if self.consume_token(&Token::Lt) {
                Some(BinaryOperator::Lt)
            } else if self.consume_token(&Token::GtEq) {
                Some(BinaryOperator::GtEq)
            } else if self.consume_token(&Token::Gt) {
                Some(BinaryOperator::Gt)
            } else if self.consume_keyword(Keyword::In) {
                let subquery = self.parse_subquery_select()?;
                return Ok(Expr::InSubquery {
                    expr: Box::new(expr),
                    subquery: Box::new(subquery),
                });
            } else {
                None
            };
            let Some(op) = op else {
                break;
            };
            let rhs = self.parse_additive()?;
            expr = Expr::BinaryOp {
                left: Box::new(expr),
                op,
                right: Box::new(rhs),
            };
        }
        Ok(expr)
    }

    fn parse_additive(&mut self) -> CorundumResult<Expr> {
        let mut expr = self.parse_multiplicative()?;
        loop {
            let op = if self.consume_token(&Token::Plus) {
                Some(BinaryOperator::Add)
            } else if self.consume_token(&Token::Minus) {
                Some(BinaryOperator::Sub)
            } else {
                None
            };
            let Some(op) = op else {
                break;
            };
            let rhs = self.parse_multiplicative()?;
            expr = Expr::BinaryOp {
                left: Box::new(expr),
                op,
                right: Box::new(rhs),
            };
        }
        Ok(expr)
    }

    fn parse_multiplicative(&mut self) -> CorundumResult<Expr> {
        let mut expr = self.parse_unary()?;
        loop {
            let op = if self.consume_token(&Token::Star) {
                Some(BinaryOperator::Mul)
            } else if self.consume_token(&Token::Slash) {
                Some(BinaryOperator::Div)
            } else {
                None
            };
            let Some(op) = op else {
                break;
            };
            let rhs = self.parse_unary()?;
            expr = Expr::BinaryOp {
                left: Box::new(expr),
                op,
                right: Box::new(rhs),
            };
        }
        Ok(expr)
    }

    fn parse_unary(&mut self) -> CorundumResult<Expr> {
        if self.consume_keyword(Keyword::Exists) {
            let subquery = self.parse_subquery_select()?;
            return Ok(Expr::Exists(Box::new(subquery)));
        }
        if self.consume_keyword(Keyword::Not) {
            let expr = self.parse_unary()?;
            return Ok(Expr::UnaryOp {
                op: UnaryOperator::Not,
                expr: Box::new(expr),
            });
        }
        if self.consume_token(&Token::Minus) {
            let expr = self.parse_unary()?;
            return Ok(Expr::UnaryOp {
                op: UnaryOperator::Neg,
                expr: Box::new(expr),
            });
        }
        self.parse_primary()
    }

    fn parse_primary(&mut self) -> CorundumResult<Expr> {
        match self.next() {
            Some(Token::Ident(name)) => {
                if self.consume_token(&Token::LParen) {
                    let args = if self.consume_token(&Token::RParen) {
                        Vec::new()
                    } else {
                        let args = self.parse_expr_list()?;
                        self.expect_token(Token::RParen)?;
                        args
                    };
                    let function = Expr::FunctionCall { name, args };
                    if self.consume_keyword(Keyword::Over) {
                        let spec = self.parse_window_spec()?;
                        Ok(Expr::WindowFunction {
                            function: Box::new(function),
                            spec,
                        })
                    } else {
                        Ok(function)
                    }
                } else if self.consume_token(&Token::Dot) {
                    let rhs = self.expect_identifier()?;
                    Ok(Expr::Identifier(format!("{}.{}", name, rhs)))
                } else {
                    Ok(Expr::Identifier(name))
                }
            }
            Some(Token::Keyword(Keyword::True)) => Ok(Expr::Literal(Literal::Bool(true))),
            Some(Token::Keyword(Keyword::False)) => Ok(Expr::Literal(Literal::Bool(false))),
            Some(Token::Number(value)) => Ok(Expr::Literal(Literal::Number(
                value
                    .parse()
                    .map_err(|_| CorundumError::new("invalid number"))?,
            ))),
            Some(Token::String(value)) => Ok(Expr::Literal(Literal::String(value))),
            Some(Token::Star) => Ok(Expr::Wildcard),
            Some(Token::LParen) => {
                if self.peek_is_keyword(Keyword::Select) {
                    let select = self.parse_subquery_select_after_lparen()?;
                    Ok(Expr::Subquery(Box::new(select)))
                } else {
                    let expr = self.parse_expr()?;
                    self.expect_token(Token::RParen)?;
                    Ok(expr)
                }
            }
            _ => Err(CorundumError::new("unexpected token in expression")),
        }
    }

    fn expect_keyword(&mut self, keyword: Keyword) -> CorundumResult<()> {
        if self.consume_keyword(keyword) {
            Ok(())
        } else {
            let found = self.peek().map(token_label).unwrap_or_else(|| "end of input".to_string());
            Err(CorundumError::new(format!(
                "expected keyword {} but found {found}",
                keyword_label(keyword)
            )))
        }
    }

    fn parse_subquery_select(&mut self) -> CorundumResult<SelectStatement> {
        self.expect_token(Token::LParen)?;
        self.expect_keyword(Keyword::Select)?;
        let select = self.parse_select()?;
        self.expect_token(Token::RParen)?;
        Ok(select)
    }

    fn parse_subquery_select_after_lparen(&mut self) -> CorundumResult<SelectStatement> {
        self.expect_keyword(Keyword::Select)?;
        let select = self.parse_select()?;
        self.expect_token(Token::RParen)?;
        Ok(select)
    }

    fn parse_window_spec(&mut self) -> CorundumResult<corundum_core::ast::WindowSpec> {
        self.expect_token(Token::LParen)?;
        let mut partition_by = Vec::new();
        let mut order_by = Vec::new();
        if self.consume_keyword(Keyword::Partition) {
            self.expect_keyword(Keyword::By)?;
            partition_by = self.parse_expr_list()?;
        }
        if self.consume_keyword(Keyword::Order) {
            self.expect_keyword(Keyword::By)?;
            order_by = self.parse_order_by_list()?;
        }
        self.expect_token(Token::RParen)?;
        Ok(corundum_core::ast::WindowSpec {
            partition_by,
            order_by,
        })
    }

    fn parse_identifier_list(&mut self) -> CorundumResult<Vec<String>> {
        let mut items = Vec::new();
        loop {
            items.push(self.expect_identifier()?);
            if !self.consume_token(&Token::Comma) {
                break;
            }
        }
        Ok(items)
    }

    fn parse_assignments(&mut self) -> CorundumResult<Vec<corundum_core::ast::Assignment>> {
        let mut items = Vec::new();
        loop {
            let column = self.expect_identifier()?;
            self.expect_token(Token::Eq)?;
            let value = self.parse_expr()?;
            items.push(corundum_core::ast::Assignment { column, value });
            if !self.consume_token(&Token::Comma) {
                break;
            }
        }
        Ok(items)
    }

    fn consume_keyword(&mut self, keyword: Keyword) -> bool {
        match self.peek() {
            Some(Token::Keyword(kw)) if *kw == keyword => {
                self.pos += 1;
                true
            }
            _ => false,
        }
    }

    fn peek_is_keyword(&self, keyword: Keyword) -> bool {
        matches!(self.peek(), Some(Token::Keyword(kw)) if *kw == keyword)
    }

    fn consume_token(&mut self, token: &Token) -> bool {
        match self.peek() {
            Some(next) if next == token => {
                self.pos += 1;
                true
            }
            _ => false,
        }
    }

    fn expect_token(&mut self, token: Token) -> CorundumResult<()> {
        if self.consume_token(&token) {
            Ok(())
        } else {
            let found = self.peek().map(token_label).unwrap_or_else(|| "end of input".to_string());
            Err(CorundumError::new(format!(
                "expected token {} but found {found}",
                token_label(&token)
            )))
        }
    }

    fn expect_identifier(&mut self) -> CorundumResult<String> {
        match self.next() {
            Some(Token::Ident(name)) => Ok(name),
            other => Err(CorundumError::new(format!(
                "expected identifier but found {}",
                other.as_ref().map(token_label).unwrap_or_else(|| "end of input".to_string())
            ))),
        }
    }

    fn is_clause_boundary(&self) -> bool {
        matches!(
            self.peek(),
            Some(Token::Keyword(
                Keyword::From
                    | Keyword::Where
                    | Keyword::Group
                    | Keyword::Having
                    | Keyword::Order
                    | Keyword::Offset
                    | Keyword::Limit
                    | Keyword::Join
                    | Keyword::Left
            ))
        )
    }

    fn is_join_boundary(&self) -> bool {
        matches!(
            self.peek(),
            Some(Token::Keyword(
                Keyword::Join
                    | Keyword::Left
                    | Keyword::Where
                    | Keyword::Group
                    | Keyword::Having
                    | Keyword::Order
                    | Keyword::Offset
                    | Keyword::Limit
            ))
        )
    }

    fn peek(&self) -> Option<&Token> {
        self.tokens.get(self.pos)
    }

    fn next(&mut self) -> Option<Token> {
        if self.pos >= self.tokens.len() {
            None
        } else {
            let token = self.tokens[self.pos].clone();
            self.pos += 1;
            Some(token)
        }
    }
}

fn token_label(token: &Token) -> String {
    match token {
        Token::Ident(value) => format!("identifier({value})"),
        Token::Number(value) => format!("number({value})"),
        Token::String(value) => format!("string('{value}')"),
        Token::Comma => ",".to_string(),
        Token::Dot => ".".to_string(),
        Token::Star => "*".to_string(),
        Token::LParen => "(".to_string(),
        Token::RParen => ")".to_string(),
        Token::Eq => "=".to_string(),
        Token::NotEq => "!=".to_string(),
        Token::Lt => "<".to_string(),
        Token::LtEq => "<=".to_string(),
        Token::Gt => ">".to_string(),
        Token::GtEq => ">=".to_string(),
        Token::Plus => "+".to_string(),
        Token::Minus => "-".to_string(),
        Token::Slash => "/".to_string(),
        Token::Keyword(keyword) => keyword_label(*keyword).to_string(),
    }
}

fn keyword_label(keyword: Keyword) -> &'static str {
    match keyword {
        Keyword::Select => "select",
        Keyword::Explain => "explain",
        Keyword::Create => "create",
        Keyword::Table => "table",
        Keyword::Insert => "insert",
        Keyword::Into => "into",
        Keyword::Values => "values",
        Keyword::Update => "update",
        Keyword::Set => "set",
        Keyword::Delete => "delete",
        Keyword::From => "from",
        Keyword::Where => "where",
        Keyword::And => "and",
        Keyword::Or => "or",
        Keyword::Not => "not",
        Keyword::As => "as",
        Keyword::Join => "join",
        Keyword::Left => "left",
        Keyword::On => "on",
        Keyword::Group => "group",
        Keyword::By => "by",
        Keyword::Having => "having",
        Keyword::Order => "order",
        Keyword::Offset => "offset",
        Keyword::Limit => "limit",
        Keyword::Asc => "asc",
        Keyword::Desc => "desc",
        Keyword::Distinct => "distinct",
        Keyword::Analyze => "analyze",
        Keyword::Over => "over",
        Keyword::Partition => "partition",
        Keyword::Exists => "exists",
        Keyword::In => "in",
        Keyword::True => "true",
        Keyword::False => "false",
    }
}

fn tokenize(input: &str, _dialect: Dialect) -> CorundumResult<Vec<Token>> {
    let mut tokens = Vec::new();
    let chars: Vec<char> = input.trim().chars().collect();
    let mut index = 0;
    while index < chars.len() {
        let c = chars[index];
        if c.is_whitespace() {
            index += 1;
            continue;
        }
        if c == ',' {
            tokens.push(Token::Comma);
            index += 1;
            continue;
        }
        if c == '.' {
            tokens.push(Token::Dot);
            index += 1;
            continue;
        }
        if c == '(' {
            tokens.push(Token::LParen);
            index += 1;
            continue;
        }
        if c == ')' {
            tokens.push(Token::RParen);
            index += 1;
            continue;
        }
        if c == '*' {
            tokens.push(Token::Star);
            index += 1;
            continue;
        }
        if c == '+' {
            tokens.push(Token::Plus);
            index += 1;
            continue;
        }
        if c == '-' {
            tokens.push(Token::Minus);
            index += 1;
            continue;
        }
        if c == '/' {
            tokens.push(Token::Slash);
            index += 1;
            continue;
        }
        if c == '=' {
            tokens.push(Token::Eq);
            index += 1;
            continue;
        }
        if c == '!' && index + 1 < chars.len() && chars[index + 1] == '=' {
            tokens.push(Token::NotEq);
            index += 2;
            continue;
        }
        if c == '<' {
            if index + 1 < chars.len() && chars[index + 1] == '=' {
                tokens.push(Token::LtEq);
                index += 2;
            } else {
                tokens.push(Token::Lt);
                index += 1;
            }
            continue;
        }
        if c == '>' {
            if index + 1 < chars.len() && chars[index + 1] == '=' {
                tokens.push(Token::GtEq);
                index += 2;
            } else {
                tokens.push(Token::Gt);
                index += 1;
            }
            continue;
        }
        if c == '\'' {
            let start = index;
            let mut end = index + 1;
            while end < chars.len() && chars[end] != '\'' {
                end += 1;
            }
            if end >= chars.len() {
                return Err(CorundumError::with_span(
                    "unterminated string literal",
                    corundum_core::error::Span { start, end },
                )
                .with_code(corundum_core::error::ErrorCode::ParserError));
            }
            let value: String = chars[index + 1..end].iter().collect();
            tokens.push(Token::String(value));
            index = end + 1;
            continue;
        }
        if c == '"' || c == '`' {
            let quote = c;
            let start = index;
            let mut end = index + 1;
            while end < chars.len() && chars[end] != quote {
                end += 1;
            }
            if end >= chars.len() {
                return Err(CorundumError::with_span(
                    "unterminated quoted identifier",
                    corundum_core::error::Span { start, end },
                )
                .with_code(corundum_core::error::ErrorCode::ParserError));
            }
            let value: String = chars[index + 1..end].iter().collect();
            tokens.push(Token::Ident(value));
            index = end + 1;
            continue;
        }
        if c.is_ascii_digit() {
            let mut end = index + 1;
            while end < chars.len() && (chars[end].is_ascii_digit() || chars[end] == '.') {
                end += 1;
            }
            let value: String = chars[index..end].iter().collect();
            tokens.push(Token::Number(value));
            index = end;
            continue;
        }
        if is_ident_start(c) {
            let mut end = index + 1;
            while end < chars.len() && is_ident_part(chars[end]) {
                end += 1;
            }
            let raw: String = chars[index..end].iter().collect();
            if let Some(keyword) = keyword_from(&raw) {
                tokens.push(Token::Keyword(keyword));
            } else {
                tokens.push(Token::Ident(raw));
            }
            index = end;
            continue;
        }
        if c == ';' {
            index += 1;
            continue;
        }
        return Err(CorundumError::with_span(
            "unsupported character in SQL",
            corundum_core::error::Span {
                start: index,
                end: index + 1,
            },
        )
        .with_code(corundum_core::error::ErrorCode::ParserError));
    }
    Ok(tokens)
}

fn is_ident_start(c: char) -> bool {
    c.is_ascii_alphabetic() || c == '_'
}

fn is_ident_part(c: char) -> bool {
    c.is_ascii_alphanumeric() || c == '_' || c == '$'
}

fn keyword_from(raw: &str) -> Option<Keyword> {
    match raw.to_ascii_lowercase().as_str() {
        "select" => Some(Keyword::Select),
        "explain" => Some(Keyword::Explain),
        "create" => Some(Keyword::Create),
        "table" => Some(Keyword::Table),
        "analyze" => Some(Keyword::Analyze),
        "insert" => Some(Keyword::Insert),
        "into" => Some(Keyword::Into),
        "values" => Some(Keyword::Values),
        "update" => Some(Keyword::Update),
        "set" => Some(Keyword::Set),
        "delete" => Some(Keyword::Delete),
        "over" => Some(Keyword::Over),
        "partition" => Some(Keyword::Partition),
        "exists" => Some(Keyword::Exists),
        "from" => Some(Keyword::From),
        "where" => Some(Keyword::Where),
        "and" => Some(Keyword::And),
        "or" => Some(Keyword::Or),
        "not" => Some(Keyword::Not),
        "as" => Some(Keyword::As),
        "join" => Some(Keyword::Join),
        "left" => Some(Keyword::Left),
        "on" => Some(Keyword::On),
        "group" => Some(Keyword::Group),
        "by" => Some(Keyword::By),
        "having" => Some(Keyword::Having),
        "order" => Some(Keyword::Order),
        "offset" => Some(Keyword::Offset),
        "limit" => Some(Keyword::Limit),
        "asc" => Some(Keyword::Asc),
        "desc" => Some(Keyword::Desc),
        "distinct" => Some(Keyword::Distinct),
        "in" => Some(Keyword::In),
        "true" => Some(Keyword::True),
        "false" => Some(Keyword::False),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::{Dialect, ParserConfig, SimpleParser, SqlParser};
    use corundum_core::ast::{BinaryOperator, Expr, JoinType, Statement};

    #[test]
    fn parse_select_with_and_or() {
        let sql = "select id from users where id = 1 and name = 'alice' or age > 2";
        let parser = SimpleParser::new(ParserConfig {
            dialect: Dialect::Postgres,
        });
        let stmt = parser.parse(sql).expect("parse");
        let Statement::Select(select) = stmt else {
            panic!("expected select");
        };
        let Expr::BinaryOp { op, .. } = select.selection.expect("selection") else {
            panic!("expected binary op");
        };
        assert!(matches!(op, BinaryOperator::Or));
    }

    #[test]
    fn parse_select_with_group_order_limit() {
        let sql = "select sum(amount) as total from sales group by region order by total desc limit 10";
        let parser = SimpleParser::new(ParserConfig {
            dialect: Dialect::Postgres,
        });
        let stmt = parser.parse(sql).expect("parse");
        let Statement::Select(select) = stmt else {
            panic!("expected select");
        };
        assert_eq!(select.group_by.len(), 1);
        assert_eq!(select.order_by.len(), 1);
        assert_eq!(select.limit, Some(10));
    }

    #[test]
    fn parse_select_with_distinct_offset() {
        let sql = "select distinct id from users order by id offset 5";
        let parser = SimpleParser::new(ParserConfig {
            dialect: Dialect::Postgres,
        });
        let stmt = parser.parse(sql).expect("parse");
        let Statement::Select(select) = stmt else {
            panic!("expected select");
        };
        assert!(select.distinct);
        assert_eq!(select.offset, Some(5));
    }

    #[test]
    fn parse_predicate_precedence() {
        let sql = "select id from t where a = 1 or b = 2 and c = 3";
        let parser = SimpleParser::new(ParserConfig {
            dialect: Dialect::Postgres,
        });
        let stmt = parser.parse(sql).expect("parse");
        let Statement::Select(select) = stmt else {
            panic!("expected select");
        };
        let Expr::BinaryOp { op, right, .. } = select.selection.expect("selection") else {
            panic!("expected binary op");
        };
        assert!(matches!(op, BinaryOperator::Or));
        let Expr::BinaryOp { op, .. } = right.as_ref() else {
            panic!("expected nested binary op");
        };
        assert!(matches!(op, BinaryOperator::And));
    }

    #[test]
    fn parse_join() {
        let sql = "select * from t1 join t2 on t1.id = t2.id";
        let parser = SimpleParser::new(ParserConfig {
            dialect: Dialect::Postgres,
        });
        let stmt = parser.parse(sql).expect("parse");
        let Statement::Select(select) = stmt else {
            panic!("expected select");
        };
        assert_eq!(select.from.joins.len(), 1);
        assert!(matches!(select.from.joins[0].join_type, JoinType::Inner));
    }

    #[test]
    fn parse_explain_select() {
        let sql = "explain select id from users";
        let parser = SimpleParser::new(ParserConfig {
            dialect: Dialect::Postgres,
        });
        let stmt = parser.parse(sql).expect("parse");
        let Statement::Explain(inner) = stmt else {
            panic!("expected explain");
        };
        let Statement::Select(_) = inner.as_ref() else {
            panic!("expected select");
        };
    }

    #[test]
    fn parse_create_table() {
        let sql = "create table users";
        let parser = SimpleParser::new(ParserConfig {
            dialect: Dialect::Postgres,
        });
        let stmt = parser.parse(sql).expect("parse");
        let Statement::CreateTable(create) = stmt else {
            panic!("expected create table");
        };
        assert_eq!(create.name, "users");
    }

    #[test]
    fn parse_quoted_identifiers() {
        let sql = "select \"user\" from `users`";
        let parser = SimpleParser::new(ParserConfig {
            dialect: Dialect::MySql,
        });
        let stmt = parser.parse(sql).expect("parse");
        let Statement::Select(select) = stmt else {
            panic!("expected select");
        };
        let first = select.projection.first().expect("projection");
        match &first.expr {
            Expr::Identifier(name) => assert_eq!(name, "user"),
            _ => panic!("expected identifier"),
        }
        assert_eq!(select.from.name, "users");
    }

    #[test]
    fn parse_analyze() {
        let sql = "analyze users";
        let parser = SimpleParser::new(ParserConfig {
            dialect: Dialect::Postgres,
        });
        let stmt = parser.parse(sql).expect("parse");
        let Statement::Analyze(analyze) = stmt else {
            panic!("expected analyze");
        };
        assert_eq!(analyze.table, "users");
    }

    #[test]
    fn parse_window_function() {
        let sql = "select sum(amount) over (partition by id order by ts) from sales";
        let parser = SimpleParser::new(ParserConfig {
            dialect: Dialect::Postgres,
        });
        let stmt = parser.parse(sql).expect("parse");
        let Statement::Select(select) = stmt else {
            panic!("expected select");
        };
        let first = select.projection.first().expect("projection");
        match &first.expr {
            Expr::WindowFunction { .. } => {}
            _ => panic!("expected window function"),
        }
    }

    #[test]
    fn parse_subquery_expression() {
        let sql = "select (select id from t) from users";
        let parser = SimpleParser::new(ParserConfig {
            dialect: Dialect::Postgres,
        });
        let stmt = parser.parse(sql).expect("parse");
        let Statement::Select(select) = stmt else {
            panic!("expected select");
        };
        let first = select.projection.first().expect("projection");
        match &first.expr {
            Expr::Subquery(_) => {}
            _ => panic!("expected subquery"),
        }
    }

    #[test]
    fn parse_exists_subquery() {
        let sql = "select id from users where exists (select id from t)";
        let parser = SimpleParser::new(ParserConfig {
            dialect: Dialect::Postgres,
        });
        let stmt = parser.parse(sql).expect("parse");
        let Statement::Select(select) = stmt else {
            panic!("expected select");
        };
        match select.selection {
            Some(Expr::Exists(_)) => {}
            _ => panic!("expected exists"),
        }
    }

    #[test]
    fn parse_in_subquery() {
        let sql = "select id from users where id in (select id from t)";
        let parser = SimpleParser::new(ParserConfig {
            dialect: Dialect::Postgres,
        });
        let stmt = parser.parse(sql).expect("parse");
        let Statement::Select(select) = stmt else {
            panic!("expected select");
        };
        match select.selection {
            Some(Expr::InSubquery { .. }) => {}
            _ => panic!("expected in subquery"),
        }
    }

    #[test]
    fn parse_boolean_literal() {
        let sql = "select * from users where active = true and deleted = false";
        let parser = SimpleParser::new(ParserConfig {
            dialect: Dialect::Postgres,
        });
        let stmt = parser.parse(sql).expect("parse");
        let Statement::Select(select) = stmt else {
            panic!("expected select");
        };
        let Expr::BinaryOp { op, .. } = select.selection.expect("selection") else {
            panic!("expected binary op");
        };
        assert!(matches!(op, BinaryOperator::And));
    }

    #[test]
    fn parse_insert() {
        let sql = "insert into users (id, name) values (1, 'alice')";
        let parser = SimpleParser::new(ParserConfig {
            dialect: Dialect::Postgres,
        });
        let stmt = parser.parse(sql).expect("parse");
        let Statement::Insert(insert) = stmt else {
            panic!("expected insert");
        };
        assert_eq!(insert.table, "users");
        assert_eq!(insert.columns.len(), 2);
        assert_eq!(insert.values.len(), 1);
        assert_eq!(insert.values[0].len(), 2);
    }

    #[test]
    fn parse_update() {
        let sql = "update users set name = 'bob' where id = 1";
        let parser = SimpleParser::new(ParserConfig {
            dialect: Dialect::Postgres,
        });
        let stmt = parser.parse(sql).expect("parse");
        let Statement::Update(update) = stmt else {
            panic!("expected update");
        };
        assert_eq!(update.table, "users");
        assert_eq!(update.assignments.len(), 1);
    }

    #[test]
    fn parse_delete() {
        let sql = "delete from users where id = 1";
        let parser = SimpleParser::new(ParserConfig {
            dialect: Dialect::Postgres,
        });
        let stmt = parser.parse(sql).expect("parse");
        let Statement::Delete(delete) = stmt else {
            panic!("expected delete");
        };
        assert_eq!(delete.table, "users");
        assert!(delete.selection.is_some());
    }

    #[test]
    fn parse_insert_multi_values() {
        let sql = "insert into users (id, name) values (1, 'alice'), (2, 'bob')";
        let parser = SimpleParser::new(ParserConfig {
            dialect: Dialect::Postgres,
        });
        let stmt = parser.parse(sql).expect("parse");
        let Statement::Insert(insert) = stmt else {
            panic!("expected insert");
        };
        assert_eq!(insert.values.len(), 2);
    }
}
