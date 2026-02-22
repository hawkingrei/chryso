use chryso_core::ast::{
    BinaryOperator, Expr, Join, JoinType, Literal, OrderByExpr, SelectItem, SelectStatement,
    Statement, TableFactor, TableRef, UnaryOperator,
};
use chryso_core::{ChrysoError, ChrysoResult};

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
    fn parse(&self, sql: &str) -> ChrysoResult<Statement>;
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
    fn parse(&self, sql: &str) -> ChrysoResult<Statement> {
        let tokens = tokenize(sql, self.config.dialect)?;
        let mut parser = Parser::new(tokens, self.config.dialect);
        let stmt = parser.parse_statement()?;
        Ok(chryso_core::ast::normalize_statement(&stmt))
    }
}

#[derive(Debug, Clone, PartialEq)]
enum Token {
    Ident(String),
    Number(String),
    String(String),
    Comma,
    Dot,
    DoubleColon,
    Star,
    LParen,
    RParen,
    LBracket,
    RBracket,
    Eq,
    NotEq,
    Lt,
    LtEq,
    Gt,
    GtEq,
    Plus,
    Minus,
    Slash,
    Tilde,
    TildeStar,
    NotTilde,
    NotTildeStar,
    Keyword(Keyword),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Keyword {
    Select,
    Explain,
    Create,
    Drop,
    Truncate,
    If,
    Exists,
    Table,
    Analyze,
    Insert,
    Into,
    Values,
    Default,
    Update,
    Set,
    Delete,
    Over,
    Partition,
    In,
    From,
    Where,
    And,
    Or,
    Not,
    As,
    Join,
    Inner,
    Cross,
    Natural,
    Left,
    Right,
    Full,
    Outer,
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
    Union,
    All,
    Any,
    Some,
    Intersect,
    Except,
    With,
    Recursive,
    Returning,
    Fetch,
    First,
    Next,
    Only,
    Lateral,
    Ties,
    Top,
    Qualify,
    True,
    False,
    Is,
    Null,
    Between,
    Like,
    ILike,
    Using,
    Case,
    When,
    Then,
    Else,
    End,
    Nulls,
    Last,
    Escape,
    Cast,
    Date,
    Time,
    Timestamp,
    Interval,
    Regexp,
    Similar,
    To,
    Rows,
    Row,
    Range,
    Groups,
    Current,
    Unbounded,
    Preceding,
    Following,
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

    fn parse_statement(&mut self) -> ChrysoResult<Statement> {
        let result = if self.consume_keyword(Keyword::With) {
            self.parse_with_statement()
        } else if self.consume_keyword(Keyword::Explain) {
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
        } else if self.consume_keyword(Keyword::Drop) {
            let statement = self.parse_drop_statement()?;
            Ok(statement)
        } else if self.consume_keyword(Keyword::Truncate) {
            let statement = self.parse_truncate_statement()?;
            Ok(statement)
        } else if self.consume_keyword(Keyword::Select) {
            let select = self.parse_select()?;
            self.parse_query_tail(select)
        } else {
            Err(ChrysoError::new("unexpected statement"))
        };
        result.map_err(|err| self.decorate_error(err))
    }

    fn parse_explain_statement(&mut self) -> ChrysoResult<Statement> {
        let result = if self.consume_keyword(Keyword::With) {
            self.parse_with_statement()
        } else if self.consume_keyword(Keyword::Select) {
            let select = self.parse_select()?;
            self.parse_query_tail(select)
        } else if self.consume_keyword(Keyword::Insert) {
            self.parse_insert_statement()
        } else if self.consume_keyword(Keyword::Update) {
            self.parse_update_statement()
        } else if self.consume_keyword(Keyword::Delete) {
            self.parse_delete_statement()
        } else if self.consume_keyword(Keyword::Create) {
            self.parse_create_statement()
        } else if self.consume_keyword(Keyword::Analyze) {
            self.parse_analyze_statement()
        } else {
            Err(ChrysoError::new("EXPLAIN expects a statement"))
        };
        result.map_err(|err| self.decorate_error(err))
    }

    fn parse_with_statement(&mut self) -> ChrysoResult<Statement> {
        let mut ctes = Vec::new();
        let mut seen_names = std::collections::HashSet::new();
        let recursive = self.consume_keyword(Keyword::Recursive);
        loop {
            let name = self.expect_identifier()?;
            if !seen_names.insert(name.clone()) {
                return Err(ChrysoError::new(format!("duplicate CTE name {name}")));
            }
            let columns = if self.consume_token(&Token::LParen) {
                if self.consume_token(&Token::RParen) {
                    return Err(ChrysoError::new("CTE column list cannot be empty"));
                }
                let cols = self.parse_identifier_list()?;
                self.expect_token(Token::RParen)?;
                let mut seen_cols = std::collections::HashSet::new();
                for col in &cols {
                    if !seen_cols.insert(col.clone()) {
                        return Err(ChrysoError::new(format!("duplicate CTE column {col}")));
                    }
                }
                cols
            } else {
                Vec::new()
            };
            self.expect_keyword(Keyword::As)?;
            self.expect_token(Token::LParen)?;
            let stmt = if self.consume_keyword(Keyword::Select) {
                let select = self.parse_select()?;
                self.parse_query_tail(select)?
            } else if self.consume_keyword(Keyword::Insert) {
                self.parse_insert_statement()?
            } else if self.consume_keyword(Keyword::Update) {
                self.parse_update_statement()?
            } else if self.consume_keyword(Keyword::Delete) {
                self.parse_delete_statement()?
            } else if self.consume_keyword(Keyword::With) {
                self.parse_with_statement()?
            } else {
                return Err(ChrysoError::new(
                    "WITH expects SELECT/INSERT/UPDATE/DELETE or WITH in CTE",
                ));
            };
            self.expect_token(Token::RParen)?;
            ctes.push(chryso_core::ast::Cte {
                name,
                columns,
                query: Box::new(stmt),
            });
            if !self.consume_token(&Token::Comma) {
                break;
            }
        }
        let statement = if self.consume_keyword(Keyword::Select) {
            let select = self.parse_select()?;
            self.parse_query_tail(select)?
        } else if self.consume_keyword(Keyword::Insert) {
            self.parse_insert_statement()?
        } else if self.consume_keyword(Keyword::Update) {
            self.parse_update_statement()?
        } else if self.consume_keyword(Keyword::Delete) {
            self.parse_delete_statement()?
        } else {
            return Err(ChrysoError::new(
                "WITH expects SELECT/INSERT/UPDATE/DELETE after CTEs",
            ));
        };
        Ok(Statement::With(chryso_core::ast::WithStatement {
            ctes,
            recursive,
            statement: Box::new(statement),
        }))
    }

    fn parse_query_tail(&mut self, left: SelectStatement) -> ChrysoResult<Statement> {
        let mut current = Statement::Select(left);
        loop {
            let op = if self.consume_keyword(Keyword::Union) {
                if self.consume_keyword(Keyword::All) {
                    chryso_core::ast::SetOperator::UnionAll
                } else {
                    chryso_core::ast::SetOperator::Union
                }
            } else if self.consume_keyword(Keyword::Intersect) {
                if self.consume_keyword(Keyword::All) {
                    chryso_core::ast::SetOperator::IntersectAll
                } else {
                    chryso_core::ast::SetOperator::Intersect
                }
            } else if self.consume_keyword(Keyword::Except) {
                if self.consume_keyword(Keyword::All) {
                    chryso_core::ast::SetOperator::ExceptAll
                } else {
                    chryso_core::ast::SetOperator::Except
                }
            } else {
                break;
            };
            self.expect_keyword(Keyword::Select)?;
            let right = self.parse_select_internal(false)?;
            current = Statement::SetOp {
                left: Box::new(current),
                op,
                right: Box::new(Statement::Select(right)),
            };
        }
        if !matches!(current, Statement::SetOp { .. }) {
            return Ok(current);
        }
        let (order_by, limit, offset) = self.parse_suffix_clauses()?;
        if order_by.is_empty() && limit.is_none() && offset.is_none() {
            return Ok(current);
        }
        Ok(self.wrap_setop_with_suffix(current, order_by, limit, offset))
    }

    fn wrap_setop_with_suffix(
        &self,
        statement: Statement,
        order_by: Vec<OrderByExpr>,
        limit: Option<u64>,
        offset: Option<u64>,
    ) -> Statement {
        let from = Some(TableRef {
            factor: TableFactor::Derived {
                query: Box::new(statement),
            },
            alias: None,
            column_aliases: Vec::new(),
            joins: Vec::new(),
        });
        Statement::Select(SelectStatement {
            distinct: false,
            distinct_on: Vec::new(),
            projection: vec![SelectItem {
                expr: Expr::Wildcard,
                alias: None,
            }],
            from,
            selection: None,
            group_by: Vec::new(),
            having: None,
            qualify: None,
            order_by,
            limit,
            offset,
        })
    }

    fn parse_create_statement(&mut self) -> ChrysoResult<Statement> {
        if self.consume_keyword(Keyword::Table) {
            let if_not_exists = if self.consume_keyword(Keyword::If) {
                self.expect_keyword(Keyword::Not)?;
                self.expect_keyword(Keyword::Exists)?;
                true
            } else {
                false
            };
            let name = self.expect_identifier()?;
            let columns = if self.consume_token(&Token::LParen) {
                let columns = self.parse_column_definitions()?;
                self.expect_token(Token::RParen)?;
                columns
            } else {
                Vec::new()
            };
            Ok(Statement::CreateTable(
                chryso_core::ast::CreateTableStatement {
                    name,
                    if_not_exists,
                    columns,
                },
            ))
        } else {
            Err(ChrysoError::new("only CREATE TABLE is supported"))
        }
    }

    fn parse_column_definitions(&mut self) -> ChrysoResult<Vec<chryso_core::ast::ColumnDef>> {
        let mut columns = Vec::new();
        loop {
            let name = self.expect_identifier()?;
            let data_type = self.parse_type_name()?;
            if data_type.is_empty() {
                return Err(ChrysoError::new("column expects a data type"));
            }
            columns.push(chryso_core::ast::ColumnDef { name, data_type });
            if !self.consume_token(&Token::Comma) {
                break;
            }
        }
        Ok(columns)
    }

    fn parse_type_name(&mut self) -> ChrysoResult<String> {
        // Heuristic type parser for now; handles common parameterized types and
        // falls back to token stitching for other cases.
        let Some(first) = self.next() else {
            return Ok(String::new());
        };
        let first_part = match first {
            Token::Ident(name) => name,
            Token::Keyword(keyword) => keyword_label(keyword).to_string(),
            other => {
                return Err(ChrysoError::new(format!(
                    "unexpected token in type: {}",
                    token_label(&other)
                )));
            }
        };
        let first_lower = first_part.to_ascii_lowercase();
        if matches!(first_lower.as_str(), "decimal" | "numeric" | "varchar") {
            let mut output = first_lower;
            if self.consume_token(&Token::LParen) {
                let mut parts = Vec::new();
                loop {
                    let part = match self.next() {
                        Some(Token::Number(value)) => value,
                        Some(Token::Ident(value)) => value,
                        Some(Token::Keyword(keyword)) => keyword_label(keyword).to_string(),
                        other => {
                            return Err(ChrysoError::new(format!(
                                "unexpected token in type parameters: {}",
                                other
                                    .as_ref()
                                    .map(token_label)
                                    .unwrap_or_else(|| "end of input".to_string())
                            )));
                        }
                    };
                    parts.push(part);
                    if self.consume_token(&Token::Comma) {
                        continue;
                    }
                    self.expect_token(Token::RParen)?;
                    break;
                }
                output.push('(');
                output.push_str(&parts.join(","));
                output.push(')');
            }
            return Ok(output);
        }

        let mut output = first_part;
        let mut depth = 0usize;
        loop {
            let token = match self.peek().cloned() {
                Some(token) => token,
                None => break,
            };
            if depth == 0 && is_type_name_terminator(&token) {
                break;
            }
            let part = match self.next() {
                Some(Token::Ident(name)) => name,
                Some(Token::Number(value)) => value,
                Some(Token::Dot) => ".".to_string(),
                Some(Token::Comma) => ",".to_string(),
                Some(Token::LParen) => {
                    depth += 1;
                    "(".to_string()
                }
                Some(Token::RParen) => {
                    if depth == 0 {
                        return Err(ChrysoError::new("unexpected ')' in type"));
                    }
                    depth -= 1;
                    ")".to_string()
                }
                Some(Token::LBracket) => "[".to_string(),
                Some(Token::RBracket) => "]".to_string(),
                Some(Token::Keyword(keyword)) => keyword_label(keyword).to_string(),
                other => {
                    return Err(ChrysoError::new(format!(
                        "unexpected token in type: {}",
                        other
                            .as_ref()
                            .map(token_label)
                            .unwrap_or_else(|| "end of input".to_string())
                    )));
                }
            };
            if output.is_empty() {
                output.push_str(&part);
            } else if part == ")"
                || part == "("
                || part == ","
                || part == "."
                || part == "["
                || part == "]"
            {
                output.push_str(&part);
            } else if output.ends_with('(') || output.ends_with('.') || output.ends_with(',') {
                output.push_str(&part);
            } else {
                output.push(' ');
                output.push_str(&part);
            }
        }
        Ok(output)
    }

    fn parse_drop_statement(&mut self) -> ChrysoResult<Statement> {
        let _ = self.consume_keyword(Keyword::Table);
        let if_exists = if self.consume_keyword(Keyword::If) {
            self.expect_keyword(Keyword::Exists)?;
            true
        } else {
            false
        };
        let name = self.expect_identifier()?;
        Ok(Statement::DropTable(chryso_core::ast::DropTableStatement {
            name,
            if_exists,
        }))
    }

    fn parse_truncate_statement(&mut self) -> ChrysoResult<Statement> {
        let _ = self.consume_keyword(Keyword::Table);
        let name = self.expect_identifier()?;
        Ok(Statement::Truncate(chryso_core::ast::TruncateStatement {
            table: name,
        }))
    }

    fn parse_analyze_statement(&mut self) -> ChrysoResult<Statement> {
        let _ = self.consume_keyword(Keyword::Table);
        let name = self.expect_identifier()?;
        Ok(Statement::Analyze(chryso_core::ast::AnalyzeStatement {
            table: name,
        }))
    }

    fn parse_insert_statement(&mut self) -> ChrysoResult<Statement> {
        self.expect_keyword(Keyword::Into)?;
        let table = self.expect_identifier()?;
        let columns = if self.consume_token(&Token::LParen) {
            let columns = self.parse_identifier_list()?;
            self.expect_token(Token::RParen)?;
            columns
        } else {
            Vec::new()
        };
        if self.consume_keyword(Keyword::Default) {
            self.expect_keyword(Keyword::Values)?;
            return Ok(Statement::Insert(chryso_core::ast::InsertStatement {
                table,
                columns,
                source: chryso_core::ast::InsertSource::DefaultValues,
                returning: self.parse_returning_clause()?,
            }));
        }
        let source = if self.consume_keyword(Keyword::Values) {
            let values = self.parse_values_rows()?;
            chryso_core::ast::InsertSource::Values(values)
        } else if self.consume_keyword(Keyword::Select) {
            let select = self.parse_select()?;
            let statement = self.parse_query_tail(select)?;
            chryso_core::ast::InsertSource::Query(Box::new(statement))
        } else if self.consume_keyword(Keyword::With) {
            let statement = self.parse_with_statement()?;
            chryso_core::ast::InsertSource::Query(Box::new(statement))
        } else {
            return Err(ChrysoError::new("INSERT expects VALUES or SELECT"));
        };
        Ok(Statement::Insert(chryso_core::ast::InsertStatement {
            table,
            columns,
            source,
            returning: self.parse_returning_clause()?,
        }))
    }

    fn parse_update_statement(&mut self) -> ChrysoResult<Statement> {
        let table = self.expect_identifier()?;
        self.expect_keyword(Keyword::Set)?;
        let assignments = self.parse_assignments()?;
        let selection = if self.consume_keyword(Keyword::Where) {
            Some(self.parse_expr()?)
        } else {
            None
        };
        Ok(Statement::Update(chryso_core::ast::UpdateStatement {
            table,
            assignments,
            selection,
            returning: self.parse_returning_clause()?,
        }))
    }

    fn parse_delete_statement(&mut self) -> ChrysoResult<Statement> {
        self.expect_keyword(Keyword::From)?;
        let table = self.expect_identifier()?;
        let selection = if self.consume_keyword(Keyword::Where) {
            Some(self.parse_expr()?)
        } else {
            None
        };
        Ok(Statement::Delete(chryso_core::ast::DeleteStatement {
            table,
            selection,
            returning: self.parse_returning_clause()?,
        }))
    }

    fn parse_select(&mut self) -> ChrysoResult<SelectStatement> {
        self.parse_select_internal(true)
    }

    fn parse_select_internal(&mut self, allow_suffix: bool) -> ChrysoResult<SelectStatement> {
        let mut top = if self.consume_keyword(Keyword::Top) {
            Some(self.parse_top_value()?)
        } else {
            None
        };
        let distinct = self.consume_keyword(Keyword::Distinct);
        let distinct_on = if distinct && self.consume_keyword(Keyword::On) {
            self.expect_token(Token::LParen)?;
            let exprs = self.parse_expr_list()?;
            self.expect_token(Token::RParen)?;
            exprs
        } else {
            Vec::new()
        };
        if self.consume_keyword(Keyword::Top) {
            if top.is_some() {
                return Err(ChrysoError::new("TOP specified more than once"));
            }
            top = Some(self.parse_top_value()?);
        }
        let projection = self.parse_projection()?;
        let from = if self.consume_keyword(Keyword::From) {
            Some(self.parse_table_ref()?)
        } else {
            None
        };
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
        let qualify = if self.consume_keyword(Keyword::Qualify) {
            Some(self.parse_expr()?)
        } else {
            None
        };
        let mut order_by = Vec::new();
        let mut limit = None;
        let mut offset = None;
        if allow_suffix {
            let suffix = self.parse_suffix_clauses()?;
            order_by = suffix.0;
            limit = suffix.1;
            offset = suffix.2;
        }
        if let Some(top_value) = top {
            if limit.is_none() {
                limit = Some(top_value);
            } else {
                return Err(ChrysoError::new(
                    "multiple limit clauses (LIMIT/FETCH/TOP) are not supported",
                ));
            }
        }
        Ok(SelectStatement {
            distinct,
            distinct_on,
            projection,
            from,
            selection,
            group_by,
            having,
            qualify,
            order_by,
            limit,
            offset,
        })
    }

    fn parse_suffix_clauses(
        &mut self,
    ) -> ChrysoResult<(Vec<OrderByExpr>, Option<u64>, Option<u64>)> {
        let order_by = if self.consume_keyword(Keyword::Order) {
            self.expect_keyword(Keyword::By)?;
            self.parse_order_by_list()?
        } else {
            Vec::new()
        };
        let mut limit = if self.consume_keyword(Keyword::Limit) {
            Some(self.parse_limit_value()?)
        } else {
            None
        };
        let offset = if self.consume_keyword(Keyword::Offset) {
            Some(self.parse_limit_value()?)
        } else {
            None
        };
        if self.consume_keyword(Keyword::Fetch) {
            let _ = if self.consume_keyword(Keyword::First) {
                true
            } else if self.consume_keyword(Keyword::Next) {
                true
            } else {
                return Err(ChrysoError::new("FETCH expects FIRST or NEXT"));
            };
            let fetch_value = if self.peek_is_limit_value() {
                self.parse_limit_value()?
            } else {
                1
            };
            let _ = if self.consume_keyword(Keyword::Row) {
                true
            } else if self.consume_keyword(Keyword::Rows) {
                true
            } else {
                return Err(ChrysoError::new("FETCH expects ROW or ROWS"));
            };
            if self.consume_keyword(Keyword::Only) {
                // no-op
            } else if self.consume_keyword(Keyword::With) {
                self.expect_keyword(Keyword::Ties)?;
            } else {
                return Err(ChrysoError::new("FETCH expects ONLY or WITH TIES"));
            }
            if limit.is_none() {
                limit = Some(fetch_value);
            } else {
                return Err(ChrysoError::new(
                    "multiple limit clauses (LIMIT/FETCH/TOP) are not supported",
                ));
            }
        }
        Ok((order_by, limit, offset))
    }

    fn parse_projection(&mut self) -> ChrysoResult<Vec<SelectItem>> {
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

    fn parse_returning_clause(&mut self) -> ChrysoResult<Vec<SelectItem>> {
        if self.consume_keyword(Keyword::Returning) {
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
        } else {
            Ok(Vec::new())
        }
    }

    fn parse_table_ref(&mut self) -> ChrysoResult<TableRef> {
        let factor = self.parse_table_factor()?;
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
        let column_aliases = if self.consume_token(&Token::LParen) {
            if alias.is_none() {
                return Err(ChrysoError::new("table alias list requires alias"));
            }
            let columns = self.parse_identifier_list()?;
            self.expect_token(Token::RParen)?;
            columns
        } else {
            Vec::new()
        };
        if matches!(
            factor,
            chryso_core::ast::TableFactor::Derived { .. }
                | chryso_core::ast::TableFactor::Values { .. }
        ) && alias.is_none()
        {
            return Err(ChrysoError::new("subquery in FROM requires alias"));
        }
        let mut table = TableRef {
            factor,
            alias,
            column_aliases,
            joins: Vec::new(),
        };
        loop {
            let mut cross_join = false;
            let mut natural_join = false;
            let join_type = if self.consume_keyword(Keyword::Join) {
                JoinType::Inner
            } else if self.consume_keyword(Keyword::Inner) {
                self.expect_keyword(Keyword::Join)?;
                JoinType::Inner
            } else if self.consume_keyword(Keyword::Cross) {
                self.expect_keyword(Keyword::Join)?;
                cross_join = true;
                JoinType::Inner
            } else if self.consume_token(&Token::Comma) {
                cross_join = true;
                JoinType::Inner
            } else if self.consume_keyword(Keyword::Natural) {
                natural_join = true;
                if let Some(join_type) = self.parse_left_right_full_join_type()? {
                    join_type
                } else if self.consume_keyword(Keyword::Inner) {
                    self.expect_keyword(Keyword::Join)?;
                    JoinType::Inner
                } else if self.consume_keyword(Keyword::Join) {
                    JoinType::Inner
                } else {
                    return Err(ChrysoError::new("NATURAL expects JOIN"));
                }
            } else if let Some(join_type) = self.parse_left_right_full_join_type()? {
                join_type
            } else {
                break;
            };
            let right = self.parse_table_ref()?;
            let on = if cross_join || natural_join {
                if self.peek_is_keyword(Keyword::On) || self.peek_is_keyword(Keyword::Using) {
                    return Err(ChrysoError::new(
                        "NATURAL/CROSS JOIN cannot use ON or USING",
                    ));
                }
                Expr::Literal(Literal::Bool(true))
            } else if self.consume_keyword(Keyword::On) {
                self.parse_expr()?
            } else if self.consume_keyword(Keyword::Using) {
                let left_name = table_ref_name(&table)?;
                let right_name = table_ref_name(&right)?;
                self.expect_token(Token::LParen)?;
                let columns = self.parse_identifier_list()?;
                self.expect_token(Token::RParen)?;
                build_using_on(left_name.as_str(), right_name.as_str(), columns)
            } else {
                return Err(ChrysoError::new("JOIN expects ON or USING"));
            };
            table.joins.push(Join {
                join_type,
                right,
                on,
            });
        }
        Ok(table)
    }

    fn parse_left_right_full_join_type(&mut self) -> ChrysoResult<Option<JoinType>> {
        let join_type = if self.consume_keyword(Keyword::Left) {
            Some(JoinType::Left)
        } else if self.consume_keyword(Keyword::Right) {
            Some(JoinType::Right)
        } else if self.consume_keyword(Keyword::Full) {
            Some(JoinType::Full)
        } else {
            None
        };

        join_type
            .map(|join_type| {
                let _ = self.consume_keyword(Keyword::Outer);
                self.expect_keyword(Keyword::Join)?;
                Ok(join_type)
            })
            .transpose()
    }

    fn parse_table_factor(&mut self) -> ChrysoResult<chryso_core::ast::TableFactor> {
        let _ = self.consume_keyword(Keyword::Lateral);
        if self.consume_token(&Token::LParen) {
            return self.parse_parenthesized_table_factor();
        }
        let first = self.expect_identifier()?;
        let mut name = self.parse_qualified_identifier_from(first)?;
        if self.consume_token(&Token::Star) {
            name.push('*');
        }
        Ok(chryso_core::ast::TableFactor::Table { name })
    }

    fn parse_parenthesized_table_factor(&mut self) -> ChrysoResult<chryso_core::ast::TableFactor> {
        if self.consume_token(&Token::LParen) {
            let factor = self.parse_parenthesized_table_factor()?;
            self.expect_token(Token::RParen)?;
            return Ok(factor);
        }
        if self.consume_keyword(Keyword::With) {
            let statement = self.parse_with_statement()?;
            self.expect_token(Token::RParen)?;
            return Ok(chryso_core::ast::TableFactor::Derived {
                query: Box::new(statement),
            });
        }
        if self.consume_keyword(Keyword::Select) {
            let select = self.parse_select()?;
            let statement = self.parse_query_tail(select)?;
            self.expect_token(Token::RParen)?;
            return Ok(chryso_core::ast::TableFactor::Derived {
                query: Box::new(statement),
            });
        }
        if self.consume_keyword(Keyword::Values) {
            let rows = self.parse_values_rows()?;
            self.expect_token(Token::RParen)?;
            return Ok(chryso_core::ast::TableFactor::Values { rows });
        }
        Err(ChrysoError::new(
            "subquery in FROM expects SELECT, WITH, or VALUES",
        ))
    }

    fn parse_array_literal(&mut self) -> ChrysoResult<Expr> {
        let args = self.parse_enclosed_expr_list(Token::RBracket)?;
        Ok(Expr::FunctionCall {
            name: "array".to_string(),
            args,
        })
    }

    fn parse_row_constructor(&mut self) -> ChrysoResult<Expr> {
        self.expect_token(Token::LParen)?;
        let args = self.parse_enclosed_expr_list(Token::RParen)?;
        Ok(Expr::FunctionCall {
            name: "row".to_string(),
            args,
        })
    }

    fn parse_enclosed_expr_list(&mut self, closing_token: Token) -> ChrysoResult<Vec<Expr>> {
        if self.consume_token(&closing_token) {
            Ok(Vec::new())
        } else {
            let args = self.parse_expr_list()?;
            self.expect_token(closing_token)?;
            Ok(args)
        }
    }

    fn parse_values_rows(&mut self) -> ChrysoResult<Vec<Vec<Expr>>> {
        let mut rows = Vec::new();
        loop {
            self.expect_token(Token::LParen)?;
            let row = self.parse_enclosed_expr_list(Token::RParen)?;
            rows.push(row);
            if !self.consume_token(&Token::Comma) {
                break;
            }
        }
        Ok(rows)
    }

    fn parse_order_by_list(&mut self) -> ChrysoResult<Vec<OrderByExpr>> {
        let mut items = Vec::new();
        loop {
            let expr = self.parse_expr()?;
            let asc = if self.consume_keyword(Keyword::Using) {
                self.parse_order_by_using_operator()?
            } else if self.consume_keyword(Keyword::Asc) {
                true
            } else if self.consume_keyword(Keyword::Desc) {
                false
            } else {
                true
            };
            let nulls_first = if self.consume_keyword(Keyword::Nulls) {
                if self.consume_keyword(Keyword::First) {
                    Some(true)
                } else if self.consume_keyword(Keyword::Last) {
                    Some(false)
                } else {
                    return Err(ChrysoError::new("NULLS expects FIRST or LAST"));
                }
            } else {
                None
            };
            items.push(OrderByExpr {
                expr,
                asc,
                nulls_first,
            });
            if !self.consume_token(&Token::Comma) {
                break;
            }
        }
        Ok(items)
    }

    fn parse_order_by_using_operator(&mut self) -> ChrysoResult<bool> {
        match self.next() {
            Some(Token::Lt) | Some(Token::LtEq) => Ok(true),
            Some(Token::Gt) | Some(Token::GtEq) => Ok(false),
            Some(Token::Ident(op)) => {
                let lower = op.to_ascii_lowercase();
                match lower.as_str() {
                    "<" | "<=" => Ok(true),
                    ">" | ">=" => Ok(false),
                    _ => Err(ChrysoError::new(format!(
                        "ORDER BY USING expects < or > operator, found {op}"
                    ))),
                }
            }
            Some(Token::Keyword(keyword)) => Err(ChrysoError::new(format!(
                "ORDER BY USING expects operator, found {}",
                keyword_label(keyword)
            ))),
            other => Err(ChrysoError::new(format!(
                "ORDER BY USING expects operator, found {}",
                other
                    .as_ref()
                    .map(token_label)
                    .unwrap_or_else(|| "end of input".to_string())
            ))),
        }
    }

    fn parse_limit_value(&mut self) -> ChrysoResult<u64> {
        if let Some(Token::Number(value)) = self.next() {
            value
                .parse()
                .map_err(|_| ChrysoError::new("invalid LIMIT value"))
        } else {
            Err(ChrysoError::new("LIMIT expects a number"))
        }
    }

    fn parse_top_value(&mut self) -> ChrysoResult<u64> {
        if self.consume_token(&Token::LParen) {
            let value = self.parse_limit_value()?;
            self.expect_token(Token::RParen)?;
            Ok(value)
        } else {
            self.parse_limit_value()
        }
    }

    fn parse_expr_list(&mut self) -> ChrysoResult<Vec<Expr>> {
        let mut items = Vec::new();
        loop {
            items.push(self.parse_expr()?);
            if !self.consume_token(&Token::Comma) {
                break;
            }
        }
        Ok(items)
    }

    fn parse_expr(&mut self) -> ChrysoResult<Expr> {
        self.parse_or()
    }

    fn parse_or(&mut self) -> ChrysoResult<Expr> {
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

    fn parse_and(&mut self) -> ChrysoResult<Expr> {
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

    fn parse_comparison(&mut self) -> ChrysoResult<Expr> {
        let mut expr = self.parse_additive()?;
        loop {
            if self.peek_is_keyword(Keyword::Not) && self.peek_is_keyword_n(1, Keyword::Between) {
                self.next();
                self.next();
                let low = self.parse_additive()?;
                self.expect_keyword(Keyword::And)?;
                let high = self.parse_additive()?;
                return Ok(Expr::BinaryOp {
                    left: Box::new(Expr::BinaryOp {
                        left: Box::new(expr.clone()),
                        op: BinaryOperator::Lt,
                        right: Box::new(low),
                    }),
                    op: BinaryOperator::Or,
                    right: Box::new(Expr::BinaryOp {
                        left: Box::new(expr),
                        op: BinaryOperator::Gt,
                        right: Box::new(high),
                    }),
                });
            }
            if self.peek_is_keyword(Keyword::Not) && self.peek_is_keyword_n(1, Keyword::In) {
                self.next();
                self.next();
                let in_expr = self.parse_in_payload(expr)?;
                return Ok(Expr::UnaryOp {
                    op: UnaryOperator::Not,
                    expr: Box::new(in_expr),
                });
            }
            if self.peek_is_keyword(Keyword::Not) && self.peek_is_keyword_n(1, Keyword::Like) {
                self.next();
                self.next();
                let like_expr = self.parse_like_payload(expr, "like")?;
                return Ok(Expr::UnaryOp {
                    op: UnaryOperator::Not,
                    expr: Box::new(like_expr),
                });
            }
            if self.peek_is_keyword(Keyword::Not) && self.peek_is_keyword_n(1, Keyword::ILike) {
                self.next();
                self.next();
                let like_expr = self.parse_like_payload(expr, "ilike")?;
                return Ok(Expr::UnaryOp {
                    op: UnaryOperator::Not,
                    expr: Box::new(like_expr),
                });
            }
            if self.peek_is_keyword(Keyword::Not) && self.peek_is_keyword_n(1, Keyword::Regexp) {
                self.next();
                self.next();
                let regexp_expr = self.parse_regexp_payload(expr, false, true)?;
                return Ok(Expr::UnaryOp {
                    op: UnaryOperator::Not,
                    expr: Box::new(regexp_expr),
                });
            }
            if self.peek_is_keyword(Keyword::Not) && self.peek_is_keyword_n(1, Keyword::Similar) {
                self.next();
                self.next();
                self.expect_keyword(Keyword::To)?;
                let similar_expr = self.parse_like_payload(expr, "similar_to")?;
                return Ok(Expr::UnaryOp {
                    op: UnaryOperator::Not,
                    expr: Box::new(similar_expr),
                });
            }
            if self.consume_keyword(Keyword::Between) {
                let low = self.parse_additive()?;
                self.expect_keyword(Keyword::And)?;
                let high = self.parse_additive()?;
                return Ok(Expr::BinaryOp {
                    left: Box::new(Expr::BinaryOp {
                        left: Box::new(expr.clone()),
                        op: BinaryOperator::GtEq,
                        right: Box::new(low),
                    }),
                    op: BinaryOperator::And,
                    right: Box::new(Expr::BinaryOp {
                        left: Box::new(expr),
                        op: BinaryOperator::LtEq,
                        right: Box::new(high),
                    }),
                });
            }
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
                return Ok(self.parse_in_payload(expr)?);
            } else if self.consume_keyword(Keyword::Is) {
                let not = self.consume_keyword(Keyword::Not);
                if self.consume_keyword(Keyword::Distinct) {
                    self.expect_keyword(Keyword::From)?;
                    let rhs = self.parse_additive()?;
                    return Ok(Expr::IsDistinctFrom {
                        left: Box::new(expr),
                        right: Box::new(rhs),
                        negated: not,
                    });
                }
                self.expect_keyword(Keyword::Null)?;
                return Ok(Expr::IsNull {
                    expr: Box::new(expr),
                    negated: not,
                });
            } else if self.consume_keyword(Keyword::Like) {
                return Ok(self.parse_like_payload(expr, "like")?);
            } else if self.consume_keyword(Keyword::ILike) {
                return Ok(self.parse_like_payload(expr, "ilike")?);
            } else if self.consume_keyword(Keyword::Regexp) {
                return Ok(self.parse_regexp_payload(expr, false, true)?);
            } else if self.consume_keyword(Keyword::Similar) {
                self.expect_keyword(Keyword::To)?;
                return Ok(self.parse_like_payload(expr, "similar_to")?);
            } else if self.consume_token(&Token::Tilde) {
                return Ok(self.parse_regexp_payload(expr, false, false)?);
            } else if self.consume_token(&Token::TildeStar) {
                return Ok(self.parse_regexp_payload(expr, true, false)?);
            } else if matches!(
                self.peek(),
                Some(Token::NotTilde) | Some(Token::NotTildeStar)
            ) {
                let token = self.next().expect("peek ensures token");
                let case_insensitive = matches!(token, Token::NotTildeStar);
                let regexp_expr = self.parse_regexp_payload(expr, case_insensitive, false)?;
                return Ok(Expr::UnaryOp {
                    op: UnaryOperator::Not,
                    expr: Box::new(regexp_expr),
                });
            } else {
                None
            };
            let Some(op) = op else {
                break;
            };
            let rhs = if let Some(quantifier) = self.consume_quantifier_name() {
                self.parse_quantified_rhs(quantifier)?
            } else {
                self.parse_additive()?
            };
            expr = Expr::BinaryOp {
                left: Box::new(expr),
                op,
                right: Box::new(rhs),
            };
        }
        Ok(expr)
    }

    fn parse_in_payload(&mut self, expr: Expr) -> ChrysoResult<Expr> {
        self.expect_token(Token::LParen)?;
        if self.peek_is_keyword(Keyword::Select) {
            let subquery = self.parse_subquery_select_after_lparen()?;
            return Ok(Expr::InSubquery {
                expr: Box::new(expr),
                subquery: Box::new(subquery),
            });
        }
        let list = self.parse_expr_list_in_parens()?;
        Ok(rewrite_in_list(expr, list))
    }

    fn consume_quantifier_name(&mut self) -> Option<&'static str> {
        if self.consume_keyword(Keyword::Any) {
            Some("any")
        } else if self.consume_keyword(Keyword::All) {
            Some("all")
        } else if self.consume_keyword(Keyword::Some) {
            Some("some")
        } else {
            None
        }
    }

    fn parse_quantified_rhs(&mut self, quantifier: &'static str) -> ChrysoResult<Expr> {
        self.expect_token(Token::LParen)?;
        let payload = if self.peek_is_keyword(Keyword::Select) {
            Expr::Subquery(Box::new(self.parse_subquery_select_after_lparen()?))
        } else {
            let expr = self.parse_expr()?;
            self.expect_token(Token::RParen)?;
            expr
        };
        Ok(Expr::FunctionCall {
            name: quantifier.to_string(),
            args: vec![payload],
        })
    }

    fn parse_like_payload(&mut self, expr: Expr, name: &str) -> ChrysoResult<Expr> {
        let pattern = self.parse_additive()?;
        let mut args = vec![expr, pattern];
        if self.consume_keyword(Keyword::Escape) {
            let escape = self.parse_additive()?;
            args.push(escape);
        }
        Ok(Expr::FunctionCall {
            name: name.to_string(),
            args,
        })
    }

    fn parse_regexp_payload(
        &mut self,
        expr: Expr,
        case_insensitive: bool,
        from_regexp_keyword: bool,
    ) -> ChrysoResult<Expr> {
        if !matches!(self._dialect, Dialect::Postgres | Dialect::MySql) {
            return Err(ChrysoError::new(
                "regex operator is not supported in this dialect",
            ));
        }
        if matches!(self._dialect, Dialect::Postgres) && from_regexp_keyword {
            return Err(ChrysoError::new(
                "REGEXP is not supported in Postgres dialect",
            ));
        }
        if matches!(self._dialect, Dialect::MySql) && !from_regexp_keyword {
            return Err(ChrysoError::new(
                "regex operators are not supported in MySQL dialect",
            ));
        }
        if matches!(self._dialect, Dialect::MySql) && case_insensitive {
            return Err(ChrysoError::new(
                "REGEXP does not support case-insensitive operator in MySQL dialect",
            ));
        }
        let pattern = self.parse_additive()?;
        let name = if case_insensitive {
            "regexp_i"
        } else {
            "regexp"
        };
        Ok(Expr::FunctionCall {
            name: name.to_string(),
            args: vec![expr, pattern],
        })
    }

    fn parse_expr_list_in_parens(&mut self) -> ChrysoResult<Vec<Expr>> {
        let mut items = Vec::new();
        if self.consume_token(&Token::RParen) {
            return Err(ChrysoError::new("IN list cannot be empty"));
        }
        loop {
            items.push(self.parse_expr()?);
            if self.consume_token(&Token::Comma) {
                continue;
            }
            self.expect_token(Token::RParen)?;
            break;
        }
        Ok(items)
    }

    fn parse_additive(&mut self) -> ChrysoResult<Expr> {
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

    fn parse_multiplicative(&mut self) -> ChrysoResult<Expr> {
        let first = self.parse_unary()?;
        let mut expr = self.parse_postfix(first)?;
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
            let rhs_unary = self.parse_unary()?;
            let rhs = self.parse_postfix(rhs_unary)?;
            expr = Expr::BinaryOp {
                left: Box::new(expr),
                op,
                right: Box::new(rhs),
            };
        }
        Ok(expr)
    }

    fn parse_unary(&mut self) -> ChrysoResult<Expr> {
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

    fn parse_postfix(&mut self, mut expr: Expr) -> ChrysoResult<Expr> {
        loop {
            if self.consume_token(&Token::DoubleColon) {
                let data_type = self.parse_type_name()?;
                if data_type.is_empty() {
                    return Err(ChrysoError::new("cast operator expects a type name"));
                }
                expr = Expr::FunctionCall {
                    name: "cast".to_string(),
                    args: vec![expr, Expr::Literal(Literal::String(data_type))],
                };
                continue;
            }
            break;
        }
        Ok(expr)
    }

    fn parse_primary(&mut self) -> ChrysoResult<Expr> {
        match self.next() {
            Some(Token::Ident(name)) => {
                if name.eq_ignore_ascii_case("array") && self.consume_token(&Token::LBracket) {
                    self.parse_array_literal()
                } else if self.consume_token(&Token::LParen) {
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
                } else {
                    Ok(Expr::Identifier(
                        self.parse_qualified_identifier_from(name)?,
                    ))
                }
            }
            Some(Token::Keyword(Keyword::True)) => Ok(Expr::Literal(Literal::Bool(true))),
            Some(Token::Keyword(Keyword::False)) => Ok(Expr::Literal(Literal::Bool(false))),
            Some(Token::Keyword(Keyword::Null)) => Ok(Expr::Literal(Literal::Null)),
            Some(Token::Keyword(Keyword::Cast)) => self.parse_cast_expr(),
            Some(Token::Keyword(Keyword::Date)) => self.parse_keyword_literal("date"),
            Some(Token::Keyword(Keyword::Time)) => self.parse_keyword_literal("time"),
            Some(Token::Keyword(Keyword::Timestamp)) => self.parse_keyword_literal("timestamp"),
            Some(Token::Keyword(Keyword::Interval)) => self.parse_keyword_literal("interval"),
            Some(Token::Number(value)) => Ok(Expr::Literal(Literal::Number(
                value
                    .parse()
                    .map_err(|_| ChrysoError::new("invalid number"))?,
            ))),
            Some(Token::String(value)) => Ok(Expr::Literal(Literal::String(value))),
            Some(Token::Keyword(Keyword::Case)) => self.parse_case_expr(),
            Some(Token::Keyword(Keyword::Row)) => {
                if self.peek() == Some(&Token::LParen) {
                    self.parse_row_constructor()
                } else {
                    Ok(Expr::Identifier("row".to_string()))
                }
            }
            Some(Token::Star) => Ok(Expr::Wildcard),
            Some(Token::LParen) => {
                if self.peek_is_keyword(Keyword::Select) {
                    let select = self.parse_subquery_select_after_lparen()?;
                    Ok(Expr::Subquery(Box::new(select)))
                } else {
                    let first = self.parse_expr()?;
                    if self.consume_token(&Token::Comma) {
                        let mut args = vec![first];
                        loop {
                            args.push(self.parse_expr()?);
                            if !self.consume_token(&Token::Comma) {
                                break;
                            }
                        }
                        self.expect_token(Token::RParen)?;
                        Ok(Expr::FunctionCall {
                            name: "row".to_string(),
                            args,
                        })
                    } else {
                        self.expect_token(Token::RParen)?;
                        Ok(first)
                    }
                }
            }
            _ => Err(ChrysoError::new("unexpected token in expression")),
        }
    }

    fn parse_case_expr(&mut self) -> ChrysoResult<Expr> {
        if self.peek_is_keyword(Keyword::End) {
            return Err(ChrysoError::new("CASE expects at least one WHEN"));
        }
        let operand = if self.peek_is_keyword(Keyword::When) {
            None
        } else {
            Some(Box::new(self.parse_expr()?))
        };
        let mut when_then = Vec::new();
        loop {
            if !self.consume_keyword(Keyword::When) {
                break;
            }
            let when_expr = self.parse_expr()?;
            self.expect_keyword(Keyword::Then)?;
            let then_expr = self.parse_expr()?;
            when_then.push((when_expr, then_expr));
        }
        if when_then.is_empty() {
            return Err(ChrysoError::new("CASE expects at least one WHEN"));
        }
        let else_expr = if self.consume_keyword(Keyword::Else) {
            Some(Box::new(self.parse_expr()?))
        } else {
            None
        };
        self.expect_keyword(Keyword::End)?;
        Ok(Expr::Case {
            operand,
            when_then,
            else_expr,
        })
    }

    fn parse_cast_expr(&mut self) -> ChrysoResult<Expr> {
        self.expect_token(Token::LParen)?;
        let expr = self.parse_expr()?;
        self.expect_keyword(Keyword::As)?;
        let data_type = self.parse_type_name()?;
        if data_type.is_empty() {
            return Err(ChrysoError::new("CAST expects a type name"));
        }
        self.expect_token(Token::RParen)?;
        Ok(Expr::FunctionCall {
            name: "cast".to_string(),
            args: vec![expr, Expr::Literal(Literal::String(data_type))],
        })
    }

    fn parse_keyword_literal(&mut self, name: &str) -> ChrysoResult<Expr> {
        match self.peek() {
            Some(Token::String(_)) | Some(Token::Number(_)) | Some(Token::LParen) => {
                let value = match self.next() {
                    Some(Token::String(value)) => Expr::Literal(Literal::String(value)),
                    Some(Token::Number(value)) => Expr::Literal(Literal::Number(
                        value
                            .parse()
                            .map_err(|_| ChrysoError::new("invalid number"))?,
                    )),
                    Some(Token::LParen) => {
                        let expr = self.parse_expr()?;
                        self.expect_token(Token::RParen)?;
                        expr
                    }
                    _ => unreachable!("expects a string, number, or expression"),
                };
                Ok(Expr::FunctionCall {
                    name: name.to_string(),
                    args: vec![value],
                })
            }
            _ => Ok(Expr::Identifier(name.to_string())),
        }
    }

    fn expect_keyword(&mut self, keyword: Keyword) -> ChrysoResult<()> {
        if self.consume_keyword(keyword) {
            Ok(())
        } else {
            let found = self
                .peek()
                .map(token_label)
                .unwrap_or_else(|| "end of input".to_string());
            Err(ChrysoError::new(format!(
                "expected keyword {} but found {found}",
                keyword_label(keyword)
            )))
        }
    }

    fn parse_subquery_select(&mut self) -> ChrysoResult<SelectStatement> {
        self.expect_token(Token::LParen)?;
        self.expect_keyword(Keyword::Select)?;
        let select = self.parse_select()?;
        self.expect_token(Token::RParen)?;
        Ok(select)
    }

    fn parse_subquery_select_after_lparen(&mut self) -> ChrysoResult<SelectStatement> {
        self.expect_keyword(Keyword::Select)?;
        let select = self.parse_select()?;
        self.expect_token(Token::RParen)?;
        Ok(select)
    }

    fn parse_window_spec(&mut self) -> ChrysoResult<chryso_core::ast::WindowSpec> {
        self.expect_token(Token::LParen)?;
        let mut partition_by = Vec::new();
        let mut order_by = Vec::new();
        let mut frame = None;
        if self.consume_keyword(Keyword::Partition) {
            self.expect_keyword(Keyword::By)?;
            partition_by = self.parse_expr_list()?;
        }
        if self.consume_keyword(Keyword::Order) {
            self.expect_keyword(Keyword::By)?;
            order_by = self.parse_order_by_list()?;
        }
        if self.consume_keyword(Keyword::Rows) {
            frame = Some(self.parse_window_frame(chryso_core::ast::WindowFrameKind::Rows)?);
        } else if self.consume_keyword(Keyword::Range) {
            frame = Some(self.parse_window_frame(chryso_core::ast::WindowFrameKind::Range)?);
        } else if self.consume_keyword(Keyword::Groups) {
            frame = Some(self.parse_window_frame(chryso_core::ast::WindowFrameKind::Groups)?);
        }
        self.expect_token(Token::RParen)?;
        Ok(chryso_core::ast::WindowSpec {
            partition_by,
            order_by,
            frame,
        })
    }

    fn parse_window_frame(
        &mut self,
        kind: chryso_core::ast::WindowFrameKind,
    ) -> ChrysoResult<chryso_core::ast::WindowFrame> {
        if self.consume_keyword(Keyword::Between) {
            let start = self.parse_window_frame_bound()?;
            self.expect_keyword(Keyword::And)?;
            let end = self.parse_window_frame_bound()?;
            Ok(chryso_core::ast::WindowFrame {
                kind,
                start,
                end: Some(end),
            })
        } else {
            let start = self.parse_window_frame_bound()?;
            Ok(chryso_core::ast::WindowFrame {
                kind,
                start,
                end: None,
            })
        }
    }

    fn parse_window_frame_bound(&mut self) -> ChrysoResult<chryso_core::ast::WindowFrameBound> {
        if self.consume_keyword(Keyword::Unbounded) {
            if self.consume_keyword(Keyword::Preceding) {
                return Ok(chryso_core::ast::WindowFrameBound::UnboundedPreceding);
            }
            if self.consume_keyword(Keyword::Following) {
                return Ok(chryso_core::ast::WindowFrameBound::UnboundedFollowing);
            }
            return Err(ChrysoError::new("UNBOUNDED expects PRECEDING or FOLLOWING"));
        }
        if self.consume_keyword(Keyword::Current) {
            self.expect_keyword(Keyword::Row)?;
            return Ok(chryso_core::ast::WindowFrameBound::CurrentRow);
        }
        let expr = self.parse_additive()?;
        if self.consume_keyword(Keyword::Preceding) {
            return Ok(chryso_core::ast::WindowFrameBound::Preceding(Box::new(
                expr,
            )));
        }
        if self.consume_keyword(Keyword::Following) {
            return Ok(chryso_core::ast::WindowFrameBound::Following(Box::new(
                expr,
            )));
        }
        Err(ChrysoError::new(
            "window frame bound expects PRECEDING or FOLLOWING",
        ))
    }

    fn parse_identifier_list(&mut self) -> ChrysoResult<Vec<String>> {
        let mut items = Vec::new();
        loop {
            items.push(self.expect_identifier()?);
            if !self.consume_token(&Token::Comma) {
                break;
            }
        }
        Ok(items)
    }

    fn parse_assignments(&mut self) -> ChrysoResult<Vec<chryso_core::ast::Assignment>> {
        let mut items = Vec::new();
        loop {
            let column = self.expect_identifier()?;
            self.expect_token(Token::Eq)?;
            let value = self.parse_expr()?;
            items.push(chryso_core::ast::Assignment { column, value });
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

    fn peek_is_limit_value(&self) -> bool {
        matches!(self.peek(), Some(Token::Number(_)))
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

    fn expect_token(&mut self, token: Token) -> ChrysoResult<()> {
        if self.consume_token(&token) {
            Ok(())
        } else {
            let found = self
                .peek()
                .map(token_label)
                .unwrap_or_else(|| "end of input".to_string());
            Err(ChrysoError::new(format!(
                "expected token {} but found {found}",
                token_label(&token)
            )))
        }
    }

    fn expect_identifier(&mut self) -> ChrysoResult<String> {
        match self.next() {
            Some(Token::Ident(name)) => Ok(name),
            other => Err(ChrysoError::new(format!(
                "expected identifier but found {}",
                other
                    .as_ref()
                    .map(token_label)
                    .unwrap_or_else(|| "end of input".to_string())
            ))),
        }
    }

    fn parse_qualified_identifier_from(&mut self, first: String) -> ChrysoResult<String> {
        let mut parts = vec![first];
        while self.consume_token(&Token::Dot) {
            if self.consume_token(&Token::Star) {
                parts.push("*".to_string());
                break;
            }
            parts.push(self.expect_identifier()?);
        }
        Ok(parts.join("."))
    }

    fn is_clause_boundary(&self) -> bool {
        matches!(
            self.peek(),
            Some(Token::Keyword(
                Keyword::From
                    | Keyword::Where
                    | Keyword::Group
                    | Keyword::Having
                    | Keyword::Qualify
                    | Keyword::Order
                    | Keyword::Offset
                    | Keyword::Limit
                    | Keyword::Fetch
                    | Keyword::Join
                    | Keyword::Inner
                    | Keyword::Left
                    | Keyword::Cross
                    | Keyword::Natural
                    | Keyword::Lateral
                    | Keyword::Right
                    | Keyword::Full
            )) | Some(Token::Comma)
        )
    }

    fn is_join_boundary(&self) -> bool {
        matches!(
            self.peek(),
            Some(Token::Keyword(
                Keyword::Join
                    | Keyword::Inner
                    | Keyword::Left
                    | Keyword::Cross
                    | Keyword::Natural
                    | Keyword::Lateral
                    | Keyword::Right
                    | Keyword::Full
                    | Keyword::Where
                    | Keyword::Group
                    | Keyword::Having
                    | Keyword::Qualify
                    | Keyword::Order
                    | Keyword::Offset
                    | Keyword::Limit
                    | Keyword::Fetch
            )) | Some(Token::Comma)
        )
    }

    fn peek(&self) -> Option<&Token> {
        self.tokens.get(self.pos)
    }

    fn peek_is_keyword_n(&self, offset: usize, keyword: Keyword) -> bool {
        matches!(
            self.tokens.get(self.pos + offset),
            Some(Token::Keyword(kw)) if *kw == keyword
        )
    }

    fn decorate_error(&self, err: ChrysoError) -> ChrysoError {
        let found = self
            .peek()
            .map(token_label)
            .unwrap_or_else(|| "end of input".to_string());
        ChrysoError::new(format!("{} at token {} ({found})", err, self.pos))
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

fn rewrite_in_list(expr: Expr, list: Vec<Expr>) -> Expr {
    let mut iter = list.into_iter();
    let first = iter.next().expect("in list should be non-empty");
    let mut combined = Expr::BinaryOp {
        left: Box::new(expr.clone()),
        op: BinaryOperator::Eq,
        right: Box::new(first),
    };
    for item in iter {
        combined = Expr::BinaryOp {
            left: Box::new(combined),
            op: BinaryOperator::Or,
            right: Box::new(Expr::BinaryOp {
                left: Box::new(expr.clone()),
                op: BinaryOperator::Eq,
                right: Box::new(item),
            }),
        };
    }
    combined
}

fn table_ref_name(table: &TableRef) -> ChrysoResult<String> {
    if let Some(alias) = &table.alias {
        return Ok(alias.clone());
    }
    match &table.factor {
        chryso_core::ast::TableFactor::Table { name } => Ok(name.clone()),
        chryso_core::ast::TableFactor::Derived { .. } => {
            Err(ChrysoError::new("subquery in FROM requires alias"))
        }
        chryso_core::ast::TableFactor::Values { .. } => {
            Err(ChrysoError::new("VALUES in FROM requires alias"))
        }
    }
}

fn is_type_name_terminator(token: &Token) -> bool {
    matches!(
        token,
        Token::Comma
            | Token::RParen
            | Token::Eq
            | Token::NotEq
            | Token::Lt
            | Token::LtEq
            | Token::Gt
            | Token::GtEq
            | Token::Plus
            | Token::Minus
            | Token::Star
            | Token::Slash
            | Token::DoubleColon
    ) || matches!(
        token,
        Token::Keyword(
            Keyword::From
                | Keyword::Where
                | Keyword::Group
                | Keyword::Having
                | Keyword::Qualify
                | Keyword::Order
                | Keyword::Offset
                | Keyword::Limit
                | Keyword::Fetch
                | Keyword::Join
                | Keyword::Inner
                | Keyword::Left
                | Keyword::Cross
                | Keyword::Natural
                | Keyword::Right
                | Keyword::Full
                | Keyword::Outer
                | Keyword::And
                | Keyword::Or
                | Keyword::When
                | Keyword::Then
                | Keyword::Else
                | Keyword::End
                | Keyword::On
        )
    )
}

fn build_using_on(left_name: &str, right_name: &str, columns: Vec<String>) -> Expr {
    let mut iter = columns.into_iter();
    let first = iter.next().expect("using columns should be non-empty");
    let mut expr = Expr::BinaryOp {
        left: Box::new(Expr::Identifier(format!("{left_name}.{first}"))),
        op: BinaryOperator::Eq,
        right: Box::new(Expr::Identifier(format!("{right_name}.{first}"))),
    };
    for column in iter {
        let next = Expr::BinaryOp {
            left: Box::new(Expr::Identifier(format!("{left_name}.{column}"))),
            op: BinaryOperator::Eq,
            right: Box::new(Expr::Identifier(format!("{right_name}.{column}"))),
        };
        expr = Expr::BinaryOp {
            left: Box::new(expr),
            op: BinaryOperator::And,
            right: Box::new(next),
        };
    }
    expr
}

fn token_label(token: &Token) -> String {
    match token {
        Token::Ident(value) => format!("identifier({value})"),
        Token::Number(value) => format!("number({value})"),
        Token::String(value) => format!("string('{value}')"),
        Token::Comma => ",".to_string(),
        Token::Dot => ".".to_string(),
        Token::DoubleColon => "::".to_string(),
        Token::Star => "*".to_string(),
        Token::LParen => "(".to_string(),
        Token::RParen => ")".to_string(),
        Token::LBracket => "[".to_string(),
        Token::RBracket => "]".to_string(),
        Token::Eq => "=".to_string(),
        Token::NotEq => "!=".to_string(),
        Token::Lt => "<".to_string(),
        Token::LtEq => "<=".to_string(),
        Token::Gt => ">".to_string(),
        Token::GtEq => ">=".to_string(),
        Token::Plus => "+".to_string(),
        Token::Minus => "-".to_string(),
        Token::Slash => "/".to_string(),
        Token::Tilde => "~".to_string(),
        Token::TildeStar => "~*".to_string(),
        Token::NotTilde => "!~".to_string(),
        Token::NotTildeStar => "!~*".to_string(),
        Token::Keyword(keyword) => keyword_label(*keyword).to_string(),
    }
}

fn keyword_label(keyword: Keyword) -> &'static str {
    match keyword {
        Keyword::Select => "select",
        Keyword::Explain => "explain",
        Keyword::Create => "create",
        Keyword::Drop => "drop",
        Keyword::Truncate => "truncate",
        Keyword::If => "if",
        Keyword::Table => "table",
        Keyword::Insert => "insert",
        Keyword::Into => "into",
        Keyword::Values => "values",
        Keyword::Default => "default",
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
        Keyword::Inner => "inner",
        Keyword::Cross => "cross",
        Keyword::Natural => "natural",
        Keyword::Left => "left",
        Keyword::Right => "right",
        Keyword::Full => "full",
        Keyword::Outer => "outer",
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
        Keyword::Union => "union",
        Keyword::All => "all",
        Keyword::Any => "any",
        Keyword::Some => "some",
        Keyword::Intersect => "intersect",
        Keyword::Except => "except",
        Keyword::With => "with",
        Keyword::Recursive => "recursive",
        Keyword::Returning => "returning",
        Keyword::Fetch => "fetch",
        Keyword::First => "first",
        Keyword::Next => "next",
        Keyword::Only => "only",
        Keyword::Lateral => "lateral",
        Keyword::Ties => "ties",
        Keyword::Top => "top",
        Keyword::Qualify => "qualify",
        Keyword::Analyze => "analyze",
        Keyword::Over => "over",
        Keyword::Partition => "partition",
        Keyword::Exists => "exists",
        Keyword::In => "in",
        Keyword::True => "true",
        Keyword::False => "false",
        Keyword::Is => "is",
        Keyword::Null => "null",
        Keyword::Between => "between",
        Keyword::Like => "like",
        Keyword::ILike => "ilike",
        Keyword::Using => "using",
        Keyword::Case => "case",
        Keyword::When => "when",
        Keyword::Then => "then",
        Keyword::Else => "else",
        Keyword::End => "end",
        Keyword::Nulls => "nulls",
        Keyword::Last => "last",
        Keyword::Escape => "escape",
        Keyword::Cast => "cast",
        Keyword::Date => "date",
        Keyword::Time => "time",
        Keyword::Timestamp => "timestamp",
        Keyword::Interval => "interval",
        Keyword::Regexp => "regexp",
        Keyword::Similar => "similar",
        Keyword::To => "to",
        Keyword::Rows => "rows",
        Keyword::Row => "row",
        Keyword::Range => "range",
        Keyword::Groups => "groups",
        Keyword::Current => "current",
        Keyword::Unbounded => "unbounded",
        Keyword::Preceding => "preceding",
        Keyword::Following => "following",
    }
}

fn tokenize(input: &str, _dialect: Dialect) -> ChrysoResult<Vec<Token>> {
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
        if c == ':' && index + 1 < chars.len() && chars[index + 1] == ':' {
            tokens.push(Token::DoubleColon);
            index += 2;
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
        if c == '[' {
            tokens.push(Token::LBracket);
            index += 1;
            continue;
        }
        if c == ']' {
            tokens.push(Token::RBracket);
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
        if c == '!' && index + 1 < chars.len() && chars[index + 1] == '~' {
            if index + 2 < chars.len() && chars[index + 2] == '*' {
                tokens.push(Token::NotTildeStar);
                index += 3;
            } else {
                tokens.push(Token::NotTilde);
                index += 2;
            }
            continue;
        }
        if c == '~' {
            if index + 1 < chars.len() && chars[index + 1] == '*' {
                tokens.push(Token::TildeStar);
                index += 2;
            } else {
                tokens.push(Token::Tilde);
                index += 1;
            }
            continue;
        }
        if c == '<' {
            if index + 1 < chars.len() && chars[index + 1] == '>' {
                tokens.push(Token::NotEq);
                index += 2;
            } else if index + 1 < chars.len() && chars[index + 1] == '=' {
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
                return Err(ChrysoError::with_span(
                    "unterminated string literal",
                    chryso_core::error::Span { start, end },
                )
                .with_code(chryso_core::error::ErrorCode::ParserError));
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
                return Err(ChrysoError::with_span(
                    "unterminated quoted identifier",
                    chryso_core::error::Span { start, end },
                )
                .with_code(chryso_core::error::ErrorCode::ParserError));
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
        return Err(ChrysoError::with_span(
            "unsupported character in SQL",
            chryso_core::error::Span {
                start: index,
                end: index + 1,
            },
        )
        .with_code(chryso_core::error::ErrorCode::ParserError));
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
        "drop" => Some(Keyword::Drop),
        "truncate" => Some(Keyword::Truncate),
        "if" => Some(Keyword::If),
        "table" => Some(Keyword::Table),
        "analyze" => Some(Keyword::Analyze),
        "insert" => Some(Keyword::Insert),
        "into" => Some(Keyword::Into),
        "values" => Some(Keyword::Values),
        "default" => Some(Keyword::Default),
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
        "inner" => Some(Keyword::Inner),
        "cross" => Some(Keyword::Cross),
        "natural" => Some(Keyword::Natural),
        "left" => Some(Keyword::Left),
        "right" => Some(Keyword::Right),
        "full" => Some(Keyword::Full),
        "outer" => Some(Keyword::Outer),
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
        "union" => Some(Keyword::Union),
        "all" => Some(Keyword::All),
        "any" => Some(Keyword::Any),
        "some" => Some(Keyword::Some),
        "intersect" => Some(Keyword::Intersect),
        "except" => Some(Keyword::Except),
        "with" => Some(Keyword::With),
        "recursive" => Some(Keyword::Recursive),
        "returning" => Some(Keyword::Returning),
        "fetch" => Some(Keyword::Fetch),
        "first" => Some(Keyword::First),
        "next" => Some(Keyword::Next),
        "only" => Some(Keyword::Only),
        "lateral" => Some(Keyword::Lateral),
        "ties" => Some(Keyword::Ties),
        "top" => Some(Keyword::Top),
        "qualify" => Some(Keyword::Qualify),
        "in" => Some(Keyword::In),
        "true" => Some(Keyword::True),
        "false" => Some(Keyword::False),
        "is" => Some(Keyword::Is),
        "null" => Some(Keyword::Null),
        "between" => Some(Keyword::Between),
        "like" => Some(Keyword::Like),
        "ilike" => Some(Keyword::ILike),
        "using" => Some(Keyword::Using),
        "case" => Some(Keyword::Case),
        "when" => Some(Keyword::When),
        "then" => Some(Keyword::Then),
        "else" => Some(Keyword::Else),
        "end" => Some(Keyword::End),
        "nulls" => Some(Keyword::Nulls),
        "last" => Some(Keyword::Last),
        "escape" => Some(Keyword::Escape),
        "cast" => Some(Keyword::Cast),
        "date" => Some(Keyword::Date),
        "time" => Some(Keyword::Time),
        "timestamp" => Some(Keyword::Timestamp),
        "interval" => Some(Keyword::Interval),
        "regexp" => Some(Keyword::Regexp),
        "similar" => Some(Keyword::Similar),
        "to" => Some(Keyword::To),
        "rows" => Some(Keyword::Rows),
        "row" => Some(Keyword::Row),
        "range" => Some(Keyword::Range),
        "groups" => Some(Keyword::Groups),
        "current" => Some(Keyword::Current),
        "unbounded" => Some(Keyword::Unbounded),
        "preceding" => Some(Keyword::Preceding),
        "following" => Some(Keyword::Following),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::{Dialect, ParserConfig, SimpleParser, SqlParser};
    use chryso_core::ast::{
        BinaryOperator, Expr, InsertSource, JoinType, Literal, SelectStatement, Statement,
        TableFactor, TableRef, UnaryOperator,
    };

    fn unwrap_from(select: &SelectStatement) -> &TableRef {
        select.from.as_ref().expect("expected from")
    }

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
        let sql =
            "select sum(amount) as total from sales group by region order by total desc limit 10";
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
    fn parse_select_without_from() {
        let sql = "select 1 + 2";
        let parser = SimpleParser::new(ParserConfig {
            dialect: Dialect::Postgres,
        });
        let stmt = parser.parse(sql).expect("parse");
        let Statement::Select(select) = stmt else {
            panic!("expected select");
        };
        assert!(select.from.is_none());
    }

    #[test]
    fn parse_select_without_from_with_where() {
        let sql = "select 1 where 1 = 1";
        let parser = SimpleParser::new(ParserConfig {
            dialect: Dialect::Postgres,
        });
        let stmt = parser.parse(sql).expect("parse");
        let Statement::Select(select) = stmt else {
            panic!("expected select");
        };
        assert!(select.from.is_none());
        assert!(select.selection.is_some());
    }

    #[test]
    fn parse_select_without_from_with_group_order_limit() {
        let sql = "select 1 group by 1 order by 1 limit 2 offset 1";
        let parser = SimpleParser::new(ParserConfig {
            dialect: Dialect::Postgres,
        });
        let stmt = parser.parse(sql).expect("parse");
        let Statement::Select(select) = stmt else {
            panic!("expected select");
        };
        assert!(select.from.is_none());
        assert_eq!(select.group_by.len(), 1);
        assert_eq!(select.order_by.len(), 1);
        assert_eq!(select.limit, Some(2));
        assert_eq!(select.offset, Some(1));
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
        assert!(select.distinct_on.is_empty());
        assert_eq!(select.offset, Some(5));
    }

    #[test]
    fn parse_select_with_distinct_top() {
        let sql = "select distinct top 5 id from users";
        let parser = SimpleParser::new(ParserConfig {
            dialect: Dialect::Postgres,
        });
        let stmt = parser.parse(sql).expect("parse");
        let Statement::Select(select) = stmt else {
            panic!("expected select");
        };
        assert!(select.distinct);
        assert_eq!(select.limit, Some(5));
    }

    #[test]
    fn parse_cast_expression() {
        let sql = "select cast(amount as decimal(10,2)) from sales";
        let parser = SimpleParser::new(ParserConfig {
            dialect: Dialect::Postgres,
        });
        let stmt = parser.parse(sql).expect("parse");
        let Statement::Select(select) = stmt else {
            panic!("expected select");
        };
        let expr = &select.projection[0].expr;
        let Expr::FunctionCall { name, args } = expr else {
            panic!("expected function call");
        };
        assert_eq!(name, "cast");
        assert_eq!(args.len(), 2);
        let Expr::Literal(Literal::String(data_type)) = &args[1] else {
            panic!("expected type literal");
        };
        assert_eq!(data_type, "decimal(10,2)");
    }

    #[test]
    fn parse_date_time_interval_literals() {
        let sql = "select date '2024-01-01', time '12:34:56', timestamp '2024-01-01 12:00:00', interval '1 day'";
        let parser = SimpleParser::new(ParserConfig {
            dialect: Dialect::Postgres,
        });
        let stmt = parser.parse(sql).expect("parse");
        let Statement::Select(select) = stmt else {
            panic!("expected select");
        };
        assert_eq!(select.projection.len(), 4);
        let names = ["date", "time", "timestamp", "interval"];
        for (item, expected_name) in select.projection.iter().zip(names.iter()) {
            let Expr::FunctionCall { name, args } = &item.expr else {
                panic!("expected function call");
            };
            assert_eq!(name, *expected_name);
            assert_eq!(args.len(), 1);
        }
    }

    #[test]
    fn parse_regexp_operator() {
        let sql = "select * from users where name regexp 'alice'";
        let parser = SimpleParser::new(ParserConfig {
            dialect: Dialect::MySql,
        });
        let stmt = parser.parse(sql).expect("parse");
        let Statement::Select(select) = stmt else {
            panic!("expected select");
        };
        let Expr::FunctionCall { name, args } = select.selection.expect("selection") else {
            panic!("expected function call");
        };
        assert_eq!(name, "regexp");
        assert_eq!(args.len(), 2);
    }

    #[test]
    fn parse_pg_regex_operators() {
        let sql = "select * from users where name ~ 'alice' and tag !~* 'bot'";
        let parser = SimpleParser::new(ParserConfig {
            dialect: Dialect::Postgres,
        });
        let stmt = parser.parse(sql).expect("parse");
        let Statement::Select(select) = stmt else {
            panic!("expected select");
        };
        let Expr::BinaryOp { op, left, right } = select.selection.expect("selection") else {
            panic!("expected binary op");
        };
        assert!(matches!(op, BinaryOperator::And));
        let is_regexp = |expr: &Expr| {
            matches!(
                expr,
                Expr::FunctionCall { name, .. } if name == "regexp"
            )
        };
        let is_not_regexp_i = |expr: &Expr| {
            matches!(
                expr,
                Expr::UnaryOp { op: UnaryOperator::Not, expr }
                    if matches!(expr.as_ref(), Expr::FunctionCall { name, .. } if name == "regexp_i")
            )
        };
        let (left_expr, right_expr) = (left.as_ref(), right.as_ref());
        assert!(
            (is_regexp(left_expr) && is_not_regexp_i(right_expr))
                || (is_regexp(right_expr) && is_not_regexp_i(left_expr))
        );
    }

    #[test]
    fn parse_pg_similar_to() {
        let sql = "select * from users where name similar to 'a%'";
        let parser = SimpleParser::new(ParserConfig {
            dialect: Dialect::Postgres,
        });
        let stmt = parser.parse(sql).expect("parse");
        let Statement::Select(select) = stmt else {
            panic!("expected select");
        };
        let Expr::FunctionCall { name, args } = select.selection.expect("selection") else {
            panic!("expected function call");
        };
        assert_eq!(name, "similar_to");
        assert_eq!(args.len(), 2);
    }

    #[test]
    fn reject_regexp_in_postgres_dialect() {
        let sql = "select * from users where name regexp 'alice'";
        let parser = SimpleParser::new(ParserConfig {
            dialect: Dialect::Postgres,
        });
        let err = parser.parse(sql).expect_err("expected error");
        assert!(err.to_string().contains("REGEXP is not supported"));
    }

    #[test]
    fn reject_case_insensitive_regex_in_mysql_dialect() {
        let sql = "select * from users where name ~* 'alice'";
        let parser = SimpleParser::new(ParserConfig {
            dialect: Dialect::MySql,
        });
        let err = parser.parse(sql).expect_err("expected error");
        assert!(
            err.to_string()
                .contains("regex operators are not supported")
        );
    }

    #[test]
    fn parse_distinct_on() {
        let sql = "select distinct on (region) region, id from users";
        let parser = SimpleParser::new(ParserConfig {
            dialect: Dialect::Postgres,
        });
        let stmt = parser.parse(sql).expect("parse");
        let Statement::Select(select) = stmt else {
            panic!("expected select");
        };
        assert!(select.distinct);
        assert_eq!(select.distinct_on.len(), 1);
    }

    #[test]
    fn parse_union_all() {
        let sql = "select id from t1 union all select id from t2";
        let parser = SimpleParser::new(ParserConfig {
            dialect: Dialect::Postgres,
        });
        let stmt = parser.parse(sql).expect("parse");
        let Statement::SetOp { op, .. } = stmt else {
            panic!("expected set op");
        };
        assert!(matches!(op, chryso_core::ast::SetOperator::UnionAll));
    }

    #[test]
    fn parse_setop_with_order_limit() {
        let sql = "select id from t1 union all select id from t2 order by id limit 1";
        let parser = SimpleParser::new(ParserConfig {
            dialect: Dialect::Postgres,
        });
        let stmt = parser.parse(sql).expect("parse");
        let Statement::Select(select) = stmt else {
            panic!("expected select");
        };
        assert_eq!(select.order_by.len(), 1);
        assert_eq!(select.limit, Some(1));
        let from = select.from.expect("from");
        let TableFactor::Derived { .. } = from.factor else {
            panic!("expected derived");
        };
    }

    #[test]
    fn parse_intersect_except() {
        let sql = "select id from t1 intersect select id from t2";
        let parser = SimpleParser::new(ParserConfig {
            dialect: Dialect::Postgres,
        });
        let stmt = parser.parse(sql).expect("parse");
        let Statement::SetOp { op, .. } = stmt else {
            panic!("expected set op");
        };
        assert!(matches!(op, chryso_core::ast::SetOperator::Intersect));

        let sql = "select id from t1 except all select id from t2";
        let stmt = parser.parse(sql).expect("parse");
        let Statement::SetOp { op, .. } = stmt else {
            panic!("expected set op");
        };
        assert!(matches!(op, chryso_core::ast::SetOperator::ExceptAll));
    }

    #[test]
    fn parse_with_cte() {
        let sql = "with t as (select id from users) select id from t";
        let parser = SimpleParser::new(ParserConfig {
            dialect: Dialect::Postgres,
        });
        let stmt = parser.parse(sql).expect("parse");
        let Statement::With(with_stmt) = stmt else {
            panic!("expected with");
        };
        assert_eq!(with_stmt.ctes.len(), 1);
    }

    #[test]
    fn parse_with_recursive_cte_columns() {
        let sql = "with recursive t(id) as (select id from users) select id from t";
        let parser = SimpleParser::new(ParserConfig {
            dialect: Dialect::Postgres,
        });
        let stmt = parser.parse(sql).expect("parse");
        let Statement::With(with_stmt) = stmt else {
            panic!("expected with");
        };
        assert!(with_stmt.recursive);
        assert_eq!(with_stmt.ctes[0].columns, vec!["id".to_string()]);
    }

    #[test]
    fn parse_with_duplicate_cte_columns() {
        let sql = "with t(id, id) as (select id from users) select id from t";
        let parser = SimpleParser::new(ParserConfig {
            dialect: Dialect::Postgres,
        });
        let err = parser.parse(sql).expect_err("expected error");
        assert!(err.to_string().contains("duplicate CTE column"));
    }

    #[test]
    fn parse_with_empty_cte_columns() {
        let sql = "with t() as (select id from users) select id from t";
        let parser = SimpleParser::new(ParserConfig {
            dialect: Dialect::Postgres,
        });
        let err = parser.parse(sql).expect_err("expected error");
        assert!(err.to_string().contains("CTE column list cannot be empty"));
    }

    #[test]
    fn parse_with_insert() {
        let sql = "with t as (select id from users) insert into audit (id) values (1)";
        let parser = SimpleParser::new(ParserConfig {
            dialect: Dialect::Postgres,
        });
        let stmt = parser.parse(sql).expect("parse");
        let Statement::With(with_stmt) = stmt else {
            panic!("expected with");
        };
        assert!(matches!(*with_stmt.statement, Statement::Insert(_)));
    }

    #[test]
    fn parse_with_delete() {
        let sql = "with t as (select id from users) delete from users where id = 1";
        let parser = SimpleParser::new(ParserConfig {
            dialect: Dialect::Postgres,
        });
        let stmt = parser.parse(sql).expect("parse");
        let Statement::With(with_stmt) = stmt else {
            panic!("expected with");
        };
        assert!(matches!(*with_stmt.statement, Statement::Delete(_)));
    }

    #[test]
    fn parse_with_nested_cte() {
        let sql = "with t as (with u as (select id from users) select id from u) select id from t";
        let parser = SimpleParser::new(ParserConfig {
            dialect: Dialect::Postgres,
        });
        let stmt = parser.parse(sql).expect("parse");
        let Statement::With(with_stmt) = stmt else {
            panic!("expected with");
        };
        assert_eq!(with_stmt.ctes.len(), 1);
    }

    #[test]
    fn parse_with_delete_returning_mixed() {
        let sql = "with t as (select id from users) delete from users returning id, users.*";
        let parser = SimpleParser::new(ParserConfig {
            dialect: Dialect::Postgres,
        });
        let stmt = parser.parse(sql).expect("parse");
        let Statement::With(with_stmt) = stmt else {
            panic!("expected with");
        };
        let Statement::Delete(delete) = with_stmt.statement.as_ref() else {
            panic!("expected delete");
        };
        assert_eq!(delete.returning.len(), 2);
        assert!(
            matches!(delete.returning[1].expr, Expr::Identifier(ref name) if name == "users.*")
        );
    }

    #[test]
    fn parse_returning_expressions() {
        let sql = "update users set name = 'bob' returning id + 1 as next_id, upper(name)";
        let parser = SimpleParser::new(ParserConfig {
            dialect: Dialect::Postgres,
        });
        let stmt = parser.parse(sql).expect("parse");
        let Statement::Update(update) = stmt else {
            panic!("expected update");
        };
        assert_eq!(update.returning.len(), 2);
        assert!(matches!(
            update.returning[0].alias.as_deref(),
            Some("next_id")
        ));
    }

    #[test]
    fn parse_with_insert_returning_mixed() {
        let sql = "with t as (select id from users) insert into users (id) values (1) returning id, users.*";
        let parser = SimpleParser::new(ParserConfig {
            dialect: Dialect::Postgres,
        });
        let stmt = parser.parse(sql).expect("parse");
        let Statement::With(with_stmt) = stmt else {
            panic!("expected with");
        };
        let Statement::Insert(insert) = with_stmt.statement.as_ref() else {
            panic!("expected insert");
        };
        assert_eq!(insert.returning.len(), 2);
    }

    #[test]
    fn parse_with_update_returning_mixed() {
        let sql =
            "with t as (select id from users) update users set name = 'bob' returning id, users.*";
        let parser = SimpleParser::new(ParserConfig {
            dialect: Dialect::Postgres,
        });
        let stmt = parser.parse(sql).expect("parse");
        let Statement::With(with_stmt) = stmt else {
            panic!("expected with");
        };
        let Statement::Update(update) = with_stmt.statement.as_ref() else {
            panic!("expected update");
        };
        assert_eq!(update.returning.len(), 2);
    }

    #[test]
    fn parse_returning_case_expr() {
        let sql =
            "update users set active = true returning case when active then 1 else 0 end as flag";
        let parser = SimpleParser::new(ParserConfig {
            dialect: Dialect::Postgres,
        });
        let stmt = parser.parse(sql).expect("parse");
        let Statement::Update(update) = stmt else {
            panic!("expected update");
        };
        assert_eq!(update.returning.len(), 1);
        assert!(matches!(update.returning[0].alias.as_deref(), Some("flag")));
    }

    #[test]
    fn parse_returning_nested_function() {
        let sql = "insert into users (name) values ('alice') returning upper(trim(name))";
        let parser = SimpleParser::new(ParserConfig {
            dialect: Dialect::Postgres,
        });
        let stmt = parser.parse(sql).expect("parse");
        let Statement::Insert(insert) = stmt else {
            panic!("expected insert");
        };
        assert_eq!(insert.returning.len(), 1);
    }

    #[test]
    fn parse_returning_multiple_aliases() {
        let sql =
            "insert into users (id, name) values (1, 'alice') returning id as id1, name as name1";
        let parser = SimpleParser::new(ParserConfig {
            dialect: Dialect::Postgres,
        });
        let stmt = parser.parse(sql).expect("parse");
        let Statement::Insert(insert) = stmt else {
            panic!("expected insert");
        };
        assert_eq!(insert.returning.len(), 2);
        assert!(matches!(insert.returning[0].alias.as_deref(), Some("id1")));
        assert!(matches!(
            insert.returning[1].alias.as_deref(),
            Some("name1")
        ));
    }

    #[test]
    fn parse_returning_with_star_and_alias() {
        let sql = "delete from users returning *, id as id1";
        let parser = SimpleParser::new(ParserConfig {
            dialect: Dialect::Postgres,
        });
        let stmt = parser.parse(sql).expect("parse");
        let Statement::Delete(delete) = stmt else {
            panic!("expected delete");
        };
        assert_eq!(delete.returning.len(), 2);
        assert!(matches!(delete.returning[0].expr, Expr::Wildcard));
        assert!(matches!(delete.returning[1].alias.as_deref(), Some("id1")));
    }

    #[test]
    fn parse_with_recursive_multiple_ctes() {
        let sql = "with recursive t(id) as (select id from users), u as (select id from t) select id from u";
        let parser = SimpleParser::new(ParserConfig {
            dialect: Dialect::Postgres,
        });
        let stmt = parser.parse(sql).expect("parse");
        let Statement::With(with_stmt) = stmt else {
            panic!("expected with");
        };
        assert!(with_stmt.recursive);
        assert_eq!(with_stmt.ctes.len(), 2);
    }

    #[test]
    fn parse_returning_complex_expr() {
        let sql = "update users set active = true returning case when active then upper(name) else lower(name) end as cname";
        let parser = SimpleParser::new(ParserConfig {
            dialect: Dialect::Postgres,
        });
        let stmt = parser.parse(sql).expect("parse");
        let Statement::Update(update) = stmt else {
            panic!("expected update");
        };
        assert_eq!(update.returning.len(), 1);
        assert!(matches!(
            update.returning[0].alias.as_deref(),
            Some("cname")
        ));
    }

    #[test]
    fn parse_with_union_returning() {
        let sql = "with t as (select id from t1 union select id from t2) update users set id = 1 returning id";
        let parser = SimpleParser::new(ParserConfig {
            dialect: Dialect::Postgres,
        });
        let stmt = parser.parse(sql).expect("parse");
        let Statement::With(with_stmt) = stmt else {
            panic!("expected with");
        };
        let Statement::SetOp { op, .. } = with_stmt.ctes[0].query.as_ref() else {
            panic!("expected set op");
        };
        assert!(matches!(op, chryso_core::ast::SetOperator::Union));
        let Statement::Update(update) = with_stmt.statement.as_ref() else {
            panic!("expected update");
        };
        assert_eq!(update.returning.len(), 1);
    }

    #[test]
    fn parse_with_intersect_returning() {
        let sql = "with t as (select id from t1 intersect select id from t2) delete from users returning id";
        let parser = SimpleParser::new(ParserConfig {
            dialect: Dialect::Postgres,
        });
        let stmt = parser.parse(sql).expect("parse");
        let Statement::With(with_stmt) = stmt else {
            panic!("expected with");
        };
        let Statement::SetOp { op, .. } = with_stmt.ctes[0].query.as_ref() else {
            panic!("expected set op");
        };
        assert!(matches!(op, chryso_core::ast::SetOperator::Intersect));
        let Statement::Delete(delete) = with_stmt.statement.as_ref() else {
            panic!("expected delete");
        };
        assert_eq!(delete.returning.len(), 1);
    }

    #[test]
    fn parse_with_except_returning() {
        let sql = "with t as (select id from t1 except select id from t2) insert into users (id) values (1) returning id";
        let parser = SimpleParser::new(ParserConfig {
            dialect: Dialect::Postgres,
        });
        let stmt = parser.parse(sql).expect("parse");
        let Statement::With(with_stmt) = stmt else {
            panic!("expected with");
        };
        let Statement::SetOp { op, .. } = with_stmt.ctes[0].query.as_ref() else {
            panic!("expected set op");
        };
        assert!(matches!(op, chryso_core::ast::SetOperator::Except));
        let Statement::Insert(insert) = with_stmt.statement.as_ref() else {
            panic!("expected insert");
        };
        assert_eq!(insert.returning.len(), 1);
    }

    #[test]
    fn parse_returning_mixed_expressions() {
        let sql = "update users set id = 1 returning id, id + 1 as next_id, users.*";
        let parser = SimpleParser::new(ParserConfig {
            dialect: Dialect::Postgres,
        });
        let stmt = parser.parse(sql).expect("parse");
        let Statement::Update(update) = stmt else {
            panic!("expected update");
        };
        assert_eq!(update.returning.len(), 3);
        assert!(matches!(
            update.returning[1].alias.as_deref(),
            Some("next_id")
        ));
        assert!(
            matches!(update.returning[2].expr, Expr::Identifier(ref name) if name == "users.*")
        );
    }

    #[test]
    fn parse_with_intersect_all_returning() {
        let sql = "with t as (select id from t1 intersect all select id from t2) delete from users returning id";
        let parser = SimpleParser::new(ParserConfig {
            dialect: Dialect::Postgres,
        });
        let stmt = parser.parse(sql).expect("parse");
        let Statement::With(with_stmt) = stmt else {
            panic!("expected with");
        };
        let Statement::SetOp { op, .. } = with_stmt.ctes[0].query.as_ref() else {
            panic!("expected set op");
        };
        assert!(matches!(op, chryso_core::ast::SetOperator::IntersectAll));
        let Statement::Delete(delete) = with_stmt.statement.as_ref() else {
            panic!("expected delete");
        };
        assert_eq!(delete.returning.len(), 1);
    }

    #[test]
    fn parse_with_except_all_returning() {
        let sql = "with t as (select id from t1 except all select id from t2) insert into users (id) values (1) returning id";
        let parser = SimpleParser::new(ParserConfig {
            dialect: Dialect::Postgres,
        });
        let stmt = parser.parse(sql).expect("parse");
        let Statement::With(with_stmt) = stmt else {
            panic!("expected with");
        };
        let Statement::SetOp { op, .. } = with_stmt.ctes[0].query.as_ref() else {
            panic!("expected set op");
        };
        assert!(matches!(op, chryso_core::ast::SetOperator::ExceptAll));
        let Statement::Insert(insert) = with_stmt.statement.as_ref() else {
            panic!("expected insert");
        };
        assert_eq!(insert.returning.len(), 1);
    }

    #[test]
    fn parse_returning_deep_expr() {
        let sql = "update users set id = 1 returning case when active then upper(trim(name)) else lower(trim(name)) end as cname";
        let parser = SimpleParser::new(ParserConfig {
            dialect: Dialect::Postgres,
        });
        let stmt = parser.parse(sql).expect("parse");
        let Statement::Update(update) = stmt else {
            panic!("expected update");
        };
        assert_eq!(update.returning.len(), 1);
        assert!(matches!(
            update.returning[0].alias.as_deref(),
            Some("cname")
        ));
    }

    #[test]
    fn parse_with_cte_insert_returning() {
        let sql = "with t as (insert into users (id) values (1) returning id) select id from t";
        let parser = SimpleParser::new(ParserConfig {
            dialect: Dialect::Postgres,
        });
        let stmt = parser.parse(sql).expect("parse");
        let Statement::With(with_stmt) = stmt else {
            panic!("expected with");
        };
        let Statement::Insert(insert) = with_stmt.ctes[0].query.as_ref() else {
            panic!("expected insert");
        };
        assert_eq!(insert.returning.len(), 1);
    }

    #[test]
    fn parse_with_cte_update_returning() {
        let sql = "with t as (update users set id = 1 returning id) select id from t";
        let parser = SimpleParser::new(ParserConfig {
            dialect: Dialect::Postgres,
        });
        let stmt = parser.parse(sql).expect("parse");
        let Statement::With(with_stmt) = stmt else {
            panic!("expected with");
        };
        let Statement::Update(update) = with_stmt.ctes[0].query.as_ref() else {
            panic!("expected update");
        };
        assert_eq!(update.returning.len(), 1);
    }

    #[test]
    fn parse_with_cte_delete_returning() {
        let sql = "with t as (delete from users returning id) select id from t";
        let parser = SimpleParser::new(ParserConfig {
            dialect: Dialect::Postgres,
        });
        let stmt = parser.parse(sql).expect("parse");
        let Statement::With(with_stmt) = stmt else {
            panic!("expected with");
        };
        let Statement::Delete(delete) = with_stmt.ctes[0].query.as_ref() else {
            panic!("expected delete");
        };
        assert_eq!(delete.returning.len(), 1);
    }

    #[test]
    fn parse_returning_table_column_expr() {
        let sql = "update users set id = 1 returning users.id, id + 1, users.*";
        let parser = SimpleParser::new(ParserConfig {
            dialect: Dialect::Postgres,
        });
        let stmt = parser.parse(sql).expect("parse");
        let Statement::Update(update) = stmt else {
            panic!("expected update");
        };
        assert_eq!(update.returning.len(), 3);
        assert!(
            matches!(update.returning[0].expr, Expr::Identifier(ref name) if name == "users.id")
        );
        assert!(
            matches!(update.returning[2].expr, Expr::Identifier(ref name) if name == "users.*")
        );
    }

    #[test]
    fn parse_with_cte_setop_insert_returning() {
        let sql = "with t as (select id from t1 union select id from t2) insert into users (id) values (1) returning id";
        let parser = SimpleParser::new(ParserConfig {
            dialect: Dialect::Postgres,
        });
        let stmt = parser.parse(sql).expect("parse");
        let Statement::With(with_stmt) = stmt else {
            panic!("expected with");
        };
        let Statement::SetOp { op, .. } = with_stmt.ctes[0].query.as_ref() else {
            panic!("expected set op");
        };
        assert!(matches!(op, chryso_core::ast::SetOperator::Union));
        let Statement::Insert(insert) = with_stmt.statement.as_ref() else {
            panic!("expected insert");
        };
        assert_eq!(insert.returning.len(), 1);
    }

    #[test]
    fn parse_with_cte_setop_update_returning() {
        let sql = "with t as (select id from t1 intersect select id from t2) update users set id = 1 returning id";
        let parser = SimpleParser::new(ParserConfig {
            dialect: Dialect::Postgres,
        });
        let stmt = parser.parse(sql).expect("parse");
        let Statement::With(with_stmt) = stmt else {
            panic!("expected with");
        };
        let Statement::SetOp { op, .. } = with_stmt.ctes[0].query.as_ref() else {
            panic!("expected set op");
        };
        assert!(matches!(op, chryso_core::ast::SetOperator::Intersect));
        let Statement::Update(update) = with_stmt.statement.as_ref() else {
            panic!("expected update");
        };
        assert_eq!(update.returning.len(), 1);
    }

    #[test]
    fn parse_with_cte_setop_delete_returning() {
        let sql =
            "with t as (select id from t1 except select id from t2) delete from users returning id";
        let parser = SimpleParser::new(ParserConfig {
            dialect: Dialect::Postgres,
        });
        let stmt = parser.parse(sql).expect("parse");
        let Statement::With(with_stmt) = stmt else {
            panic!("expected with");
        };
        let Statement::SetOp { op, .. } = with_stmt.ctes[0].query.as_ref() else {
            panic!("expected set op");
        };
        assert!(matches!(op, chryso_core::ast::SetOperator::Except));
        let Statement::Delete(delete) = with_stmt.statement.as_ref() else {
            panic!("expected delete");
        };
        assert_eq!(delete.returning.len(), 1);
    }

    #[test]
    fn parse_returning_complex_chain() {
        let sql = "update users set id = 1 returning upper(trim(lower(name))) as cname, case when id > 0 then id * 2 else id / 2 end";
        let parser = SimpleParser::new(ParserConfig {
            dialect: Dialect::Postgres,
        });
        let stmt = parser.parse(sql).expect("parse");
        let Statement::Update(update) = stmt else {
            panic!("expected update");
        };
        assert_eq!(update.returning.len(), 2);
        assert!(matches!(
            update.returning[0].alias.as_deref(),
            Some("cname")
        ));
    }

    #[test]
    fn parse_with_duplicate_cte_names() {
        let sql = "with t as (select id from t1), t as (select id from t2) select id from t";
        let parser = SimpleParser::new(ParserConfig {
            dialect: Dialect::Postgres,
        });
        let err = parser.parse(sql).expect_err("expected error");
        assert!(err.to_string().contains("duplicate CTE name"));
    }

    #[test]
    fn parse_insert_returning() {
        let sql = "insert into users (id) values (1) returning id";
        let parser = SimpleParser::new(ParserConfig {
            dialect: Dialect::Postgres,
        });
        let stmt = parser.parse(sql).expect("parse");
        let Statement::Insert(insert) = stmt else {
            panic!("expected insert");
        };
        assert_eq!(insert.returning.len(), 1);
    }

    #[test]
    fn parse_update_returning() {
        let sql = "update users set name = 'bob' returning id";
        let parser = SimpleParser::new(ParserConfig {
            dialect: Dialect::Postgres,
        });
        let stmt = parser.parse(sql).expect("parse");
        let Statement::Update(update) = stmt else {
            panic!("expected update");
        };
        assert_eq!(update.returning.len(), 1);
    }

    #[test]
    fn parse_delete_returning() {
        let sql = "delete from users returning id";
        let parser = SimpleParser::new(ParserConfig {
            dialect: Dialect::Postgres,
        });
        let stmt = parser.parse(sql).expect("parse");
        let Statement::Delete(delete) = stmt else {
            panic!("expected delete");
        };
        assert_eq!(delete.returning.len(), 1);
    }

    #[test]
    fn parse_with_union_cte() {
        let sql = "with t as (select id from t1 union all select id from t2) select id from t";
        let parser = SimpleParser::new(ParserConfig {
            dialect: Dialect::Postgres,
        });
        let stmt = parser.parse(sql).expect("parse");
        let Statement::With(with_stmt) = stmt else {
            panic!("expected with");
        };
        assert_eq!(with_stmt.ctes.len(), 1);
        let Statement::SetOp { op, .. } = with_stmt.ctes[0].query.as_ref() else {
            panic!("expected set op");
        };
        assert!(matches!(op, chryso_core::ast::SetOperator::UnionAll));
    }

    #[test]
    fn parse_with_multiple_ctes() {
        let sql = "with t as (select id from t1), u as (select id from t2) select id from t";
        let parser = SimpleParser::new(ParserConfig {
            dialect: Dialect::Postgres,
        });
        let stmt = parser.parse(sql).expect("parse");
        let Statement::With(with_stmt) = stmt else {
            panic!("expected with");
        };
        assert_eq!(with_stmt.ctes.len(), 2);
    }

    #[test]
    fn parse_order_by_nulls_last() {
        let sql = "select id from users order by id desc nulls last";
        let parser = SimpleParser::new(ParserConfig {
            dialect: Dialect::Postgres,
        });
        let stmt = parser.parse(sql).expect("parse");
        let Statement::Select(select) = stmt else {
            panic!("expected select");
        };
        assert_eq!(select.order_by.len(), 1);
        assert_eq!(select.order_by[0].nulls_first, Some(false));
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
        let from = unwrap_from(&select);
        assert_eq!(from.joins.len(), 1);
        assert!(matches!(from.joins[0].join_type, JoinType::Inner));
    }

    #[test]
    fn parse_inner_join_keyword() {
        let sql = "select * from t1 inner join t2 on t1.id = t2.id";
        let parser = SimpleParser::new(ParserConfig {
            dialect: Dialect::Postgres,
        });
        let stmt = parser.parse(sql).expect("parse");
        let Statement::Select(select) = stmt else {
            panic!("expected select");
        };
        let from = unwrap_from(&select);
        assert_eq!(from.joins.len(), 1);
        assert!(matches!(from.joins[0].join_type, JoinType::Inner));
    }

    #[test]
    fn parse_left_outer_join() {
        let sql = "select * from t1 left outer join t2 on t1.id = t2.id";
        let parser = SimpleParser::new(ParserConfig {
            dialect: Dialect::Postgres,
        });
        let stmt = parser.parse(sql).expect("parse");
        let Statement::Select(select) = stmt else {
            panic!("expected select");
        };
        let from = unwrap_from(&select);
        assert_eq!(from.joins.len(), 1);
        assert!(matches!(from.joins[0].join_type, JoinType::Left));
    }

    #[test]
    fn parse_from_subquery() {
        let sql = "select * from (select id from users) as u";
        let parser = SimpleParser::new(ParserConfig {
            dialect: Dialect::Postgres,
        });
        let stmt = parser.parse(sql).expect("parse");
        let Statement::Select(select) = stmt else {
            panic!("expected select");
        };
        let from = unwrap_from(&select);
        assert!(matches!(from.factor, TableFactor::Derived { .. }));
        assert_eq!(from.alias.as_deref(), Some("u"));
    }

    #[test]
    fn parse_from_subquery_join() {
        let sql = "select * from (select id from users) as u join items on u.id = items.user_id";
        let parser = SimpleParser::new(ParserConfig {
            dialect: Dialect::Postgres,
        });
        let stmt = parser.parse(sql).expect("parse");
        let Statement::Select(select) = stmt else {
            panic!("expected select");
        };
        let from = unwrap_from(&select);
        assert!(matches!(from.factor, TableFactor::Derived { .. }));
        assert_eq!(from.joins.len(), 1);
        assert!(matches!(
            from.joins[0].right.factor,
            TableFactor::Table { .. }
        ));
    }

    #[test]
    fn parse_from_subquery_join_using() {
        let sql = "select * from (select id from users) as u join items using (id)";
        let parser = SimpleParser::new(ParserConfig {
            dialect: Dialect::Postgres,
        });
        let stmt = parser.parse(sql).expect("parse");
        let Statement::Select(select) = stmt else {
            panic!("expected select");
        };
        let on_sql = unwrap_from(&select).joins[0].on.to_sql();
        assert!(on_sql.contains("u.id = items.id"));
    }

    #[test]
    fn parse_from_subquery_column_aliases() {
        let sql = "select * from (select id from users) as u(user_id)";
        let parser = SimpleParser::new(ParserConfig {
            dialect: Dialect::Postgres,
        });
        let stmt = parser.parse(sql).expect("parse");
        let Statement::Select(select) = stmt else {
            panic!("expected select");
        };
        let from = unwrap_from(&select);
        assert_eq!(from.column_aliases, vec!["user_id"]);
    }

    #[test]
    fn parse_from_values() {
        let sql = "select x from (values (1), (2)) as v(x)";
        let parser = SimpleParser::new(ParserConfig {
            dialect: Dialect::Postgres,
        });
        let stmt = parser.parse(sql).expect("parse");
        let Statement::Select(select) = stmt else {
            panic!("expected select");
        };
        let from = unwrap_from(&select);
        assert_eq!(from.alias.as_deref(), Some("v"));
        assert_eq!(from.column_aliases, vec!["x"]);
        let TableFactor::Values { rows } = &from.factor else {
            panic!("expected values factor");
        };
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].len(), 1);
        assert_eq!(rows[1].len(), 1);
    }

    #[test]
    fn parse_from_values_requires_alias() {
        let sql = "select * from (values (1), (2))";
        let parser = SimpleParser::new(ParserConfig {
            dialect: Dialect::Postgres,
        });
        let err = parser.parse(sql).expect_err("expected error");
        assert!(err.to_string().contains("requires alias"));
    }

    #[test]
    fn parse_lateral_values_factor() {
        let sql = "select * from users u, lateral (values (u.id)) as v(x)";
        let parser = SimpleParser::new(ParserConfig {
            dialect: Dialect::Postgres,
        });
        let stmt = parser.parse(sql).expect("parse");
        let Statement::Select(select) = stmt else {
            panic!("expected select");
        };
        let from = unwrap_from(&select);
        assert_eq!(from.joins.len(), 1);
        assert_eq!(from.joins[0].right.alias.as_deref(), Some("v"));
        let TableFactor::Values { rows } = &from.joins[0].right.factor else {
            panic!("expected lateral values factor");
        };
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].len(), 1);
    }

    #[test]
    fn parse_parenthesized_subquery_with_extra_parens() {
        let sql = "select * from ((select 1 as x)) as ss";
        let parser = SimpleParser::new(ParserConfig {
            dialect: Dialect::Postgres,
        });
        let stmt = parser.parse(sql).expect("parse");
        let Statement::Select(select) = stmt else {
            panic!("expected select");
        };
        let from = unwrap_from(&select);
        assert!(matches!(from.factor, TableFactor::Derived { .. }));
        assert_eq!(from.alias.as_deref(), Some("ss"));
    }

    #[test]
    fn parse_table_alias_list_requires_alias() {
        let sql = "select * from users (id)";
        let parser = SimpleParser::new(ParserConfig {
            dialect: Dialect::Postgres,
        });
        let err = parser.parse(sql).expect_err("expected error");
        assert!(err.to_string().contains("requires alias"));
    }

    #[test]
    fn parse_from_subquery_requires_alias() {
        let sql = "select * from (select id from users)";
        let parser = SimpleParser::new(ParserConfig {
            dialect: Dialect::Postgres,
        });
        let err = parser.parse(sql).expect_err("expected error");
        assert!(err.to_string().contains("requires alias"));
    }

    #[test]
    fn parse_comma_join() {
        let sql = "select * from t1, t2";
        let parser = SimpleParser::new(ParserConfig {
            dialect: Dialect::Postgres,
        });
        let stmt = parser.parse(sql).expect("parse");
        let Statement::Select(select) = stmt else {
            panic!("expected select");
        };
        let from = unwrap_from(&select);
        assert!(matches!(
            from.factor,
            TableFactor::Table { ref name } if name == "t1"
        ));
        assert_eq!(from.joins.len(), 1);
        assert!(matches!(from.joins[0].join_type, JoinType::Inner));
        assert!(matches!(
            from.joins[0].right.factor,
            TableFactor::Table { ref name } if name == "t2"
        ));
        assert!(matches!(
            from.joins[0].on,
            Expr::Literal(Literal::Bool(true))
        ));
    }

    #[test]
    fn parse_comma_join_precedence() {
        let sql = "select * from t1, t2 join t3 on t2.id = t3.id";
        let parser = SimpleParser::new(ParserConfig {
            dialect: Dialect::Postgres,
        });
        let stmt = parser.parse(sql).expect("parse");
        let Statement::Select(select) = stmt else {
            panic!("expected select");
        };
        let from = unwrap_from(&select);
        assert_eq!(from.joins.len(), 1);
        assert!(matches!(
            from.joins[0].right.factor,
            TableFactor::Table { ref name } if name == "t2"
        ));
        assert_eq!(from.joins[0].right.joins.len(), 1);
        assert!(matches!(
            from.joins[0].right.joins[0].right.factor,
            TableFactor::Table { ref name } if name == "t3"
        ));
    }

    #[test]
    fn parse_join_then_comma_join() {
        let sql = "select * from t1 join t2 on t1.id = t2.id, t3";
        let parser = SimpleParser::new(ParserConfig {
            dialect: Dialect::Postgres,
        });
        let stmt = parser.parse(sql).expect("parse");
        let Statement::Select(select) = stmt else {
            panic!("expected select");
        };
        let from = unwrap_from(&select);
        assert_eq!(from.joins.len(), 2);
        assert!(matches!(
            from.joins[1].on,
            Expr::Literal(Literal::Bool(true))
        ));
    }

    #[test]
    fn parse_join_without_condition_rejected() {
        let sql = "select * from t1 join t2";
        let parser = SimpleParser::new(ParserConfig {
            dialect: Dialect::Postgres,
        });
        let err = parser.parse(sql).expect_err("expected error");
        assert!(err.to_string().contains("JOIN expects ON or USING"));
    }

    #[test]
    fn parse_cross_join() {
        let sql = "select * from t1 cross join t2";
        let parser = SimpleParser::new(ParserConfig {
            dialect: Dialect::Postgres,
        });
        let stmt = parser.parse(sql).expect("parse");
        let Statement::Select(select) = stmt else {
            panic!("expected select");
        };
        let from = unwrap_from(&select);
        assert_eq!(from.joins.len(), 1);
        assert!(matches!(from.joins[0].join_type, JoinType::Inner));
        assert!(matches!(
            from.joins[0].on,
            Expr::Literal(Literal::Bool(true))
        ));
    }

    #[test]
    fn parse_natural_join() {
        let sql = "select * from t1 natural join t2";
        let parser = SimpleParser::new(ParserConfig {
            dialect: Dialect::Postgres,
        });
        let stmt = parser.parse(sql).expect("parse");
        let Statement::Select(select) = stmt else {
            panic!("expected select");
        };
        let from = unwrap_from(&select);
        assert_eq!(from.joins.len(), 1);
        assert!(matches!(
            from.joins[0].on,
            Expr::Literal(Literal::Bool(true))
        ));
    }

    #[test]
    fn parse_natural_left_join() {
        let sql = "select * from t1 natural left join t2";
        let parser = SimpleParser::new(ParserConfig {
            dialect: Dialect::Postgres,
        });
        let stmt = parser.parse(sql).expect("parse");
        let Statement::Select(select) = stmt else {
            panic!("expected select");
        };
        let from = unwrap_from(&select);
        assert_eq!(from.joins.len(), 1);
        assert!(matches!(from.joins[0].join_type, JoinType::Left));
    }

    #[test]
    fn parse_natural_left_outer_join() {
        let sql = "select * from t1 natural left outer join t2";
        let parser = SimpleParser::new(ParserConfig {
            dialect: Dialect::Postgres,
        });
        let stmt = parser.parse(sql).expect("parse");
        let Statement::Select(select) = stmt else {
            panic!("expected select");
        };
        let from = unwrap_from(&select);
        assert_eq!(from.joins.len(), 1);
        assert!(matches!(from.joins[0].join_type, JoinType::Left));
    }

    #[test]
    fn parse_natural_join_with_on_rejected() {
        let sql = "select * from t1 natural join t2 on t1.id = t2.id";
        let parser = SimpleParser::new(ParserConfig {
            dialect: Dialect::Postgres,
        });
        let err = parser.parse(sql).expect_err("expected error");
        assert!(
            err.to_string()
                .contains("NATURAL/CROSS JOIN cannot use ON or USING")
        );
    }

    #[test]
    fn parse_cross_join_with_using_rejected() {
        let sql = "select * from t1 cross join t2 using (id)";
        let parser = SimpleParser::new(ParserConfig {
            dialect: Dialect::Postgres,
        });
        let err = parser.parse(sql).expect_err("expected error");
        assert!(
            err.to_string()
                .contains("NATURAL/CROSS JOIN cannot use ON or USING")
        );
    }

    #[test]
    fn parse_join_using() {
        let sql = "select * from t1 join t2 using (id, name)";
        let parser = SimpleParser::new(ParserConfig {
            dialect: Dialect::Postgres,
        });
        let stmt = parser.parse(sql).expect("parse");
        let Statement::Select(select) = stmt else {
            panic!("expected select");
        };
        let on_sql = unwrap_from(&select).joins[0].on.to_sql();
        assert!(on_sql.contains("t1.id = t2.id"));
        assert!(on_sql.contains("t1.name = t2.name"));
    }

    #[test]
    fn parse_case_expr() {
        let sql = "select case when a = 1 then 'one' else 'other' end from t";
        let parser = SimpleParser::new(ParserConfig {
            dialect: Dialect::Postgres,
        });
        let stmt = parser.parse(sql).expect("parse");
        let Statement::Select(select) = stmt else {
            panic!("expected select");
        };
        let expr = &select.projection[0].expr;
        let Expr::Case { when_then, .. } = expr else {
            panic!("expected case");
        };
        assert_eq!(when_then.len(), 1);
    }

    #[test]
    fn parse_case_with_operand() {
        let sql = "select case status when 'ok' then 1 else 0 end from t";
        let parser = SimpleParser::new(ParserConfig {
            dialect: Dialect::Postgres,
        });
        let stmt = parser.parse(sql).expect("parse");
        let Statement::Select(select) = stmt else {
            panic!("expected select");
        };
        let expr = &select.projection[0].expr;
        let Expr::Case { operand, .. } = expr else {
            panic!("expected case");
        };
        assert!(operand.is_some());
    }

    #[test]
    fn parse_case_missing_when() {
        let sql = "select case end from t";
        let parser = SimpleParser::new(ParserConfig {
            dialect: Dialect::Postgres,
        });
        let err = parser.parse(sql).expect_err("expected error");
        assert!(err.to_string().contains("CASE expects"));
    }

    #[test]
    fn parse_right_full_join() {
        let sql = "select * from t1 right join t2 on t1.id = t2.id";
        let parser = SimpleParser::new(ParserConfig {
            dialect: Dialect::Postgres,
        });
        let stmt = parser.parse(sql).expect("parse");
        let Statement::Select(select) = stmt else {
            panic!("expected select");
        };
        assert!(matches!(
            unwrap_from(&select).joins[0].join_type,
            JoinType::Right
        ));
        let sql = "select * from t1 full join t2 on t1.id = t2.id";
        let stmt = parser.parse(sql).expect("parse");
        let Statement::Select(select) = stmt else {
            panic!("expected select");
        };
        assert!(matches!(
            unwrap_from(&select).joins[0].join_type,
            JoinType::Full
        ));
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
        assert!(!create.if_not_exists);
        assert!(create.columns.is_empty());
    }

    #[test]
    fn parse_create_table_if_not_exists() {
        let sql = "create table if not exists users";
        let parser = SimpleParser::new(ParserConfig {
            dialect: Dialect::Postgres,
        });
        let stmt = parser.parse(sql).expect("parse");
        let Statement::CreateTable(create) = stmt else {
            panic!("expected create table");
        };
        assert!(create.if_not_exists);
        assert!(create.columns.is_empty());
    }

    #[test]
    fn parse_create_table_with_columns() {
        let sql = "create table users (id integer, name varchar(20))";
        let parser = SimpleParser::new(ParserConfig {
            dialect: Dialect::Postgres,
        });
        let stmt = parser.parse(sql).expect("parse");
        let Statement::CreateTable(create) = stmt else {
            panic!("expected create table");
        };
        assert_eq!(create.columns.len(), 2);
        assert_eq!(create.columns[0].name, "id");
        assert_eq!(create.columns[0].data_type, "integer");
        assert_eq!(create.columns[1].name, "name");
        assert_eq!(create.columns[1].data_type, "varchar(20)");
    }

    #[test]
    fn parse_create_table_with_complex_types() {
        let sql = "create table metrics (price decimal(10,2), stamp timestamp with time zone, dtype pg_catalog.int4)";
        let parser = SimpleParser::new(ParserConfig {
            dialect: Dialect::Postgres,
        });
        let stmt = parser.parse(sql).expect("parse");
        let Statement::CreateTable(create) = stmt else {
            panic!("expected create table");
        };
        assert_eq!(create.columns.len(), 3);
        assert_eq!(create.columns[0].data_type, "decimal(10,2)");
        assert_eq!(create.columns[1].data_type, "timestamp with time zone");
        assert_eq!(create.columns[2].data_type, "pg_catalog.int4");
    }

    #[test]
    fn parse_create_table_with_numeric_spacing() {
        let sql = "create table metrics (price numeric ( 10 , 2 ), name varchar ( 20 ))";
        let parser = SimpleParser::new(ParserConfig {
            dialect: Dialect::Postgres,
        });
        let stmt = parser.parse(sql).expect("parse");
        let Statement::CreateTable(create) = stmt else {
            panic!("expected create table");
        };
        assert_eq!(create.columns.len(), 2);
        assert_eq!(create.columns[0].data_type, "numeric(10,2)");
        assert_eq!(create.columns[1].data_type, "varchar(20)");
    }

    #[test]
    fn parse_drop_table() {
        let sql = "drop table users";
        let parser = SimpleParser::new(ParserConfig {
            dialect: Dialect::Postgres,
        });
        let stmt = parser.parse(sql).expect("parse");
        let Statement::DropTable(drop) = stmt else {
            panic!("expected drop table");
        };
        assert_eq!(drop.name, "users");
        assert!(!drop.if_exists);
    }

    #[test]
    fn parse_drop_table_if_exists() {
        let sql = "drop table if exists users";
        let parser = SimpleParser::new(ParserConfig {
            dialect: Dialect::Postgres,
        });
        let stmt = parser.parse(sql).expect("parse");
        let Statement::DropTable(drop) = stmt else {
            panic!("expected drop table");
        };
        assert!(drop.if_exists);
    }

    #[test]
    fn parse_truncate_table() {
        let sql = "truncate table users";
        let parser = SimpleParser::new(ParserConfig {
            dialect: Dialect::Postgres,
        });
        let stmt = parser.parse(sql).expect("parse");
        let Statement::Truncate(truncate) = stmt else {
            panic!("expected truncate");
        };
        assert_eq!(truncate.table, "users");
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
        assert!(matches!(
            unwrap_from(&select).factor,
            TableFactor::Table { ref name } if name == "users"
        ));
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
    fn parse_analyze_table() {
        let sql = "analyze table users";
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
    fn parse_explain_insert() {
        let sql = "explain insert into users (id) values (1)";
        let parser = SimpleParser::new(ParserConfig {
            dialect: Dialect::Postgres,
        });
        let stmt = parser.parse(sql).expect("parse");
        let Statement::Explain(inner) = stmt else {
            panic!("expected explain");
        };
        assert!(matches!(*inner, Statement::Insert(_)));
    }

    #[test]
    fn parse_explain_delete() {
        let sql = "explain delete from users where id = 1";
        let parser = SimpleParser::new(ParserConfig {
            dialect: Dialect::Postgres,
        });
        let stmt = parser.parse(sql).expect("parse");
        let Statement::Explain(inner) = stmt else {
            panic!("expected explain");
        };
        assert!(matches!(*inner, Statement::Delete(_)));
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
    fn parse_window_frame_rows_between() {
        let sql = "select sum(amount) over (partition by id order by ts rows between 1 preceding and current row) from sales";
        let parser = SimpleParser::new(ParserConfig {
            dialect: Dialect::Postgres,
        });
        let stmt = parser.parse(sql).expect("parse");
        let Statement::Select(select) = stmt else {
            panic!("expected select");
        };
        let first = select.projection.first().expect("projection");
        let Expr::WindowFunction { spec, .. } = &first.expr else {
            panic!("expected window function");
        };
        let frame = spec.frame.as_ref().expect("frame");
        assert!(matches!(
            frame.kind,
            chryso_core::ast::WindowFrameKind::Rows
        ));
        assert!(matches!(
            frame.start,
            chryso_core::ast::WindowFrameBound::Preceding(_)
        ));
        assert!(matches!(
            frame.end,
            Some(chryso_core::ast::WindowFrameBound::CurrentRow)
        ));
    }

    #[test]
    fn parse_qualify_clause() {
        let sql = "select id from sales qualify id > 10";
        let parser = SimpleParser::new(ParserConfig {
            dialect: Dialect::Postgres,
        });
        let stmt = parser.parse(sql).expect("parse");
        let Statement::Select(select) = stmt else {
            panic!("expected select");
        };
        assert!(select.qualify.is_some());
    }

    #[test]
    fn parse_top_clause() {
        let sql = "select top 5 id from sales";
        let parser = SimpleParser::new(ParserConfig {
            dialect: Dialect::Postgres,
        });
        let stmt = parser.parse(sql).expect("parse");
        let Statement::Select(select) = stmt else {
            panic!("expected select");
        };
        assert_eq!(select.limit, Some(5));
    }

    #[test]
    fn parse_fetch_first_clause() {
        let sql = "select id from sales order by id fetch first 3 rows only";
        let parser = SimpleParser::new(ParserConfig {
            dialect: Dialect::Postgres,
        });
        let stmt = parser.parse(sql).expect("parse");
        let Statement::Select(select) = stmt else {
            panic!("expected select");
        };
        assert_eq!(select.limit, Some(3));
    }

    #[test]
    fn parse_fetch_first_without_count_defaults_to_one() {
        let sql = "select id from sales order by id fetch first row only";
        let parser = SimpleParser::new(ParserConfig {
            dialect: Dialect::Postgres,
        });
        let stmt = parser.parse(sql).expect("parse");
        let Statement::Select(select) = stmt else {
            panic!("expected select");
        };
        assert_eq!(select.limit, Some(1));
    }

    #[test]
    fn reject_fetch_without_only() {
        let sql = "select id from sales order by id fetch first row";
        let parser = SimpleParser::new(ParserConfig {
            dialect: Dialect::Postgres,
        });
        let err = parser.parse(sql).unwrap_err();
        assert!(err.to_string().contains("FETCH expects ONLY"));
    }

    #[test]
    fn parse_error_includes_token_context() {
        let sql = "select from";
        let parser = SimpleParser::new(ParserConfig {
            dialect: Dialect::Postgres,
        });
        let err = parser.parse(sql).unwrap_err();
        assert!(err.to_string().contains("token"));
    }

    #[test]
    fn reject_multiple_limit_clauses() {
        let sql = "select id from sales limit 2 fetch first 1 row only";
        let parser = SimpleParser::new(ParserConfig {
            dialect: Dialect::Postgres,
        });
        let err = parser.parse(sql).unwrap_err();
        assert!(err.to_string().contains("multiple limit clauses"));
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
    fn parse_in_list() {
        let sql = "select * from users where id in (1, 2, 3)";
        let parser = SimpleParser::new(ParserConfig {
            dialect: Dialect::Postgres,
        });
        let stmt = parser.parse(sql).expect("parse");
        let Statement::Select(select) = stmt else {
            panic!("expected select");
        };
        let expr = select.selection.expect("selection");
        assert_eq!(expr.to_sql(), "id = 1 or id = 2 or id = 3");
    }

    #[test]
    fn parse_row_tuple_in_list() {
        let sql = "select * from users where (id, age) in ((1, 2), (3, 4))";
        let parser = SimpleParser::new(ParserConfig {
            dialect: Dialect::Postgres,
        });
        let stmt = parser.parse(sql).expect("parse");
        let Statement::Select(select) = stmt else {
            panic!("expected select");
        };
        let expr = select.selection.expect("selection");
        assert_eq!(
            expr.to_sql(),
            "row(id, age) = row(1, 2) or row(id, age) = row(3, 4)"
        );
    }

    #[test]
    fn parse_quantified_subquery_comparison() {
        let sql = "select 1 = all (select 1)";
        let parser = SimpleParser::new(ParserConfig {
            dialect: Dialect::Postgres,
        });
        let stmt = parser.parse(sql).expect("parse");
        let Statement::Select(select) = stmt else {
            panic!("expected select");
        };
        let Expr::BinaryOp { right, .. } = &select.projection[0].expr else {
            panic!("expected binary comparison");
        };
        let Expr::FunctionCall { name, args } = right.as_ref() else {
            panic!("expected quantified rhs");
        };
        assert_eq!(name, "all");
        assert_eq!(args.len(), 1);
    }

    #[test]
    fn parse_null_literal_projection() {
        let sql = "select null";
        let parser = SimpleParser::new(ParserConfig {
            dialect: Dialect::Postgres,
        });
        let stmt = parser.parse(sql).expect("parse");
        let Statement::Select(select) = stmt else {
            panic!("expected select");
        };
        assert!(matches!(
            select.projection[0].expr,
            Expr::Literal(Literal::Null)
        ));
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
    fn parse_is_null() {
        let sql = "select * from users where deleted is null or deleted is not null";
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
    fn parse_between() {
        let sql = "select * from users where age between 18 and 30";
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
    fn parse_not_between() {
        let sql = "select * from users where age not between 18 and 30";
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
    fn parse_like() {
        let sql = "select * from users where name like 'al%'";
        let parser = SimpleParser::new(ParserConfig {
            dialect: Dialect::Postgres,
        });
        let stmt = parser.parse(sql).expect("parse");
        let Statement::Select(select) = stmt else {
            panic!("expected select");
        };
        let Expr::FunctionCall { name, args } = select.selection.expect("selection") else {
            panic!("expected function call");
        };
        assert_eq!(name, "like");
        assert_eq!(args.len(), 2);
    }

    #[test]
    fn parse_not_like() {
        let sql = "select * from users where name not like 'al%'";
        let parser = SimpleParser::new(ParserConfig {
            dialect: Dialect::Postgres,
        });
        let stmt = parser.parse(sql).expect("parse");
        let Statement::Select(select) = stmt else {
            panic!("expected select");
        };
        let Expr::UnaryOp { op, expr } = select.selection.expect("selection") else {
            panic!("expected unary op");
        };
        assert!(matches!(op, UnaryOperator::Not));
        let Expr::FunctionCall { name, args } = expr.as_ref() else {
            panic!("expected function call");
        };
        assert_eq!(name, "like");
        assert_eq!(args.len(), 2);
    }

    #[test]
    fn parse_like_escape() {
        let sql = "select * from users where name like 'al\\_%' escape '\\\\'";
        let parser = SimpleParser::new(ParserConfig {
            dialect: Dialect::Postgres,
        });
        let stmt = parser.parse(sql).expect("parse");
        let Statement::Select(select) = stmt else {
            panic!("expected select");
        };
        let Expr::FunctionCall { name, args } = select.selection.expect("selection") else {
            panic!("expected function call");
        };
        assert_eq!(name, "like");
        assert_eq!(args.len(), 3);
    }

    #[test]
    fn parse_not_like_escape() {
        let sql = "select * from users where name not like 'al\\_%' escape '\\\\'";
        let parser = SimpleParser::new(ParserConfig {
            dialect: Dialect::Postgres,
        });
        let stmt = parser.parse(sql).expect("parse");
        let Statement::Select(select) = stmt else {
            panic!("expected select");
        };
        let Expr::UnaryOp { op, expr } = select.selection.expect("selection") else {
            panic!("expected unary op");
        };
        assert!(matches!(op, UnaryOperator::Not));
        let Expr::FunctionCall { name, args } = expr.as_ref() else {
            panic!("expected function call");
        };
        assert_eq!(name, "like");
        assert_eq!(args.len(), 3);
    }

    #[test]
    fn parse_not_in_list() {
        let sql = "select * from users where id not in (1, 2, 3)";
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
    fn parse_ilike() {
        let sql = "select * from users where name ilike 'al%'";
        let parser = SimpleParser::new(ParserConfig {
            dialect: Dialect::Postgres,
        });
        let stmt = parser.parse(sql).expect("parse");
        let Statement::Select(select) = stmt else {
            panic!("expected select");
        };
        let Expr::FunctionCall { name, args } = select.selection.expect("selection") else {
            panic!("expected function call");
        };
        assert_eq!(name, "ilike");
        assert_eq!(args.len(), 2);
    }

    #[test]
    fn parse_not_ilike() {
        let sql = "select * from users where name not ilike 'al%'";
        let parser = SimpleParser::new(ParserConfig {
            dialect: Dialect::Postgres,
        });
        let stmt = parser.parse(sql).expect("parse");
        let Statement::Select(select) = stmt else {
            panic!("expected select");
        };
        let Expr::UnaryOp { op, expr } = select.selection.expect("selection") else {
            panic!("expected unary op");
        };
        assert!(matches!(op, UnaryOperator::Not));
        let Expr::FunctionCall { name, args } = expr.as_ref() else {
            panic!("expected function call");
        };
        assert_eq!(name, "ilike");
        assert_eq!(args.len(), 2);
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
        let InsertSource::Values(values) = insert.source else {
            panic!("expected values source");
        };
        assert_eq!(values.len(), 1);
        assert_eq!(values[0].len(), 2);
        assert!(insert.returning.is_empty());
    }

    #[test]
    fn parse_insert_select() {
        let sql = "insert into users (id) select id from staging";
        let parser = SimpleParser::new(ParserConfig {
            dialect: Dialect::Postgres,
        });
        let stmt = parser.parse(sql).expect("parse");
        let Statement::Insert(insert) = stmt else {
            panic!("expected insert");
        };
        let InsertSource::Query(statement) = insert.source else {
            panic!("expected query source");
        };
        let Statement::Select(_) = statement.as_ref() else {
            panic!("expected select query");
        };
    }

    #[test]
    fn parse_insert_select_returning() {
        let sql = "insert into users (id) select id from staging returning id";
        let parser = SimpleParser::new(ParserConfig {
            dialect: Dialect::Postgres,
        });
        let stmt = parser.parse(sql).expect("parse");
        let Statement::Insert(insert) = stmt else {
            panic!("expected insert");
        };
        assert_eq!(insert.returning.len(), 1);
        assert!(matches!(insert.source, InsertSource::Query(_)));
    }

    #[test]
    fn parse_insert_default_values() {
        let sql = "insert into users default values";
        let parser = SimpleParser::new(ParserConfig {
            dialect: Dialect::Postgres,
        });
        let stmt = parser.parse(sql).expect("parse");
        let Statement::Insert(insert) = stmt else {
            panic!("expected insert");
        };
        assert!(matches!(insert.source, InsertSource::DefaultValues));
        assert!(insert.returning.is_empty());
    }

    #[test]
    fn parse_insert_default_values_returning() {
        let sql = "insert into users default values returning id";
        let parser = SimpleParser::new(ParserConfig {
            dialect: Dialect::Postgres,
        });
        let stmt = parser.parse(sql).expect("parse");
        let Statement::Insert(insert) = stmt else {
            panic!("expected insert");
        };
        assert!(matches!(insert.source, InsertSource::DefaultValues));
        assert_eq!(insert.returning.len(), 1);
    }

    #[test]
    fn parse_returning_star() {
        let sql = "delete from users returning *";
        let parser = SimpleParser::new(ParserConfig {
            dialect: Dialect::Postgres,
        });
        let stmt = parser.parse(sql).expect("parse");
        let Statement::Delete(delete) = stmt else {
            panic!("expected delete");
        };
        assert_eq!(delete.returning.len(), 1);
        assert!(matches!(delete.returning[0].expr, Expr::Wildcard));
    }

    #[test]
    fn parse_returning_qualified_star() {
        let sql = "delete from users returning users.*";
        let parser = SimpleParser::new(ParserConfig {
            dialect: Dialect::Postgres,
        });
        let stmt = parser.parse(sql).expect("parse");
        let Statement::Delete(delete) = stmt else {
            panic!("expected delete");
        };
        assert_eq!(delete.returning.len(), 1);
        assert!(
            matches!(delete.returning[0].expr, Expr::Identifier(ref name) if name == "users.*")
        );
    }

    #[test]
    fn parse_schema_qualified_table() {
        let sql = "select * from public.users";
        let parser = SimpleParser::new(ParserConfig {
            dialect: Dialect::Postgres,
        });
        let stmt = parser.parse(sql).expect("parse");
        let Statement::Select(select) = stmt else {
            panic!("expected select");
        };
        let from = select.from.expect("from");
        let chryso_core::ast::TableFactor::Table { name } = from.factor else {
            panic!("expected table factor");
        };
        assert_eq!(name, "public.users");
    }

    #[test]
    fn parse_table_inheritance_star() {
        let sql = "select * from person* p";
        let parser = SimpleParser::new(ParserConfig {
            dialect: Dialect::Postgres,
        });
        let stmt = parser.parse(sql).expect("parse");
        let Statement::Select(select) = stmt else {
            panic!("expected select");
        };
        let from = select.from.expect("from");
        assert_eq!(from.alias.as_deref(), Some("p"));
        let TableFactor::Table { name } = from.factor else {
            panic!("expected table factor");
        };
        assert_eq!(name, "person*");
    }

    #[test]
    fn parse_angle_not_eq_operator() {
        let sql = "select * from tenk1 where four <> 0";
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
        assert!(matches!(op, BinaryOperator::NotEq));
    }

    #[test]
    fn parse_array_literal() {
        let sql = "select array[1, 2]::int[]";
        let parser = SimpleParser::new(ParserConfig {
            dialect: Dialect::Postgres,
        });
        let stmt = parser.parse(sql).expect("parse");
        let Statement::Select(select) = stmt else {
            panic!("expected select");
        };
        let Expr::FunctionCall { name, args } = &select.projection[0].expr else {
            panic!("expected cast");
        };
        assert_eq!(name, "cast");
        let Expr::FunctionCall {
            name: array_name,
            args: array_args,
        } = &args[0]
        else {
            panic!("expected array literal");
        };
        assert_eq!(array_name, "array");
        assert_eq!(array_args.len(), 2);
    }

    #[test]
    fn parse_row_constructor_expr() {
        let sql = "select row(1, 2)";
        let parser = SimpleParser::new(ParserConfig {
            dialect: Dialect::Postgres,
        });
        let stmt = parser.parse(sql).expect("parse");
        let Statement::Select(select) = stmt else {
            panic!("expected select");
        };
        let Expr::FunctionCall { name, args } = &select.projection[0].expr else {
            panic!("expected row constructor");
        };
        assert_eq!(name, "row");
        assert_eq!(args.len(), 2);
    }

    #[test]
    fn parse_schema_qualified_identifier() {
        let sql = "select public.users.id from public.users";
        let parser = SimpleParser::new(ParserConfig {
            dialect: Dialect::Postgres,
        });
        let stmt = parser.parse(sql).expect("parse");
        let Statement::Select(select) = stmt else {
            panic!("expected select");
        };
        assert_eq!(select.projection.len(), 1);
        assert!(
            matches!(select.projection[0].expr, Expr::Identifier(ref name) if name == "public.users.id")
        );
    }

    #[test]
    fn parse_schema_qualified_wildcard() {
        let sql = "select public.users.* from public.users";
        let parser = SimpleParser::new(ParserConfig {
            dialect: Dialect::Postgres,
        });
        let stmt = parser.parse(sql).expect("parse");
        let Statement::Select(select) = stmt else {
            panic!("expected select");
        };
        assert_eq!(select.projection.len(), 1);
        assert!(
            matches!(select.projection[0].expr, Expr::Identifier(ref name) if name == "public.users.*")
        );
    }

    #[test]
    fn parse_returning_alias() {
        let sql = "update users set name = 'bob' returning id as user_id";
        let parser = SimpleParser::new(ParserConfig {
            dialect: Dialect::Postgres,
        });
        let stmt = parser.parse(sql).expect("parse");
        let Statement::Update(update) = stmt else {
            panic!("expected update");
        };
        assert_eq!(update.returning.len(), 1);
        assert!(matches!(
            update.returning[0].alias.as_deref(),
            Some("user_id")
        ));
    }

    #[test]
    fn parse_returning_mixed_list() {
        let sql = "insert into users (id) values (1) returning id, users.*, name as username";
        let parser = SimpleParser::new(ParserConfig {
            dialect: Dialect::Postgres,
        });
        let stmt = parser.parse(sql).expect("parse");
        let Statement::Insert(insert) = stmt else {
            panic!("expected insert");
        };
        assert_eq!(insert.returning.len(), 3);
        assert!(matches!(insert.returning[0].expr, Expr::Identifier(ref name) if name == "id"));
        assert!(
            matches!(insert.returning[1].expr, Expr::Identifier(ref name) if name == "users.*")
        );
        assert!(matches!(
            insert.returning[2].alias.as_deref(),
            Some("username")
        ));
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
        let InsertSource::Values(values) = insert.source else {
            panic!("expected values source");
        };
        assert_eq!(values.len(), 2);
    }

    #[test]
    fn parse_postgres_double_colon_cast() {
        let sql = "select ''::text as s, 1::int4 as i";
        let parser = SimpleParser::new(ParserConfig {
            dialect: Dialect::Postgres,
        });
        let stmt = parser.parse(sql).expect("parse");
        let Statement::Select(select) = stmt else {
            panic!("expected select");
        };
        assert_eq!(select.projection.len(), 2);
        for item in &select.projection {
            let Expr::FunctionCall { name, args } = &item.expr else {
                panic!("expected cast expression");
            };
            assert_eq!(name, "cast");
            assert_eq!(args.len(), 2);
        }
    }

    #[test]
    fn parse_is_distinct_from() {
        let sql = "select f1 is distinct from 2 from disttable";
        let parser = SimpleParser::new(ParserConfig {
            dialect: Dialect::Postgres,
        });
        let stmt = parser.parse(sql).expect("parse");
        let Statement::Select(select) = stmt else {
            panic!("expected select");
        };
        let Expr::IsDistinctFrom { negated, .. } = &select.projection[0].expr else {
            panic!("expected is distinct from expression");
        };
        assert!(!negated);
    }

    #[test]
    fn parse_is_not_distinct_from() {
        let sql = "select f1 is not distinct from 2 from disttable";
        let parser = SimpleParser::new(ParserConfig {
            dialect: Dialect::Postgres,
        });
        let stmt = parser.parse(sql).expect("parse");
        let Statement::Select(select) = stmt else {
            panic!("expected select");
        };
        let Expr::IsDistinctFrom { negated, .. } = &select.projection[0].expr else {
            panic!("expected is distinct from expression");
        };
        assert!(*negated);
    }

    #[test]
    fn parse_order_by_using_operator() {
        let sql = "select two, ten from onek order by two using <, ten using >";
        let parser = SimpleParser::new(ParserConfig {
            dialect: Dialect::Postgres,
        });
        let stmt = parser.parse(sql).expect("parse");
        let Statement::Select(select) = stmt else {
            panic!("expected select");
        };
        assert_eq!(select.order_by.len(), 2);
        assert!(select.order_by[0].asc);
        assert!(!select.order_by[1].asc);
    }

    #[test]
    fn parse_fetch_with_ties() {
        let sql = "select id from sales order by id fetch first 2 rows with ties";
        let parser = SimpleParser::new(ParserConfig {
            dialect: Dialect::Postgres,
        });
        let stmt = parser.parse(sql).expect("parse");
        let Statement::Select(select) = stmt else {
            panic!("expected select");
        };
        assert_eq!(select.limit, Some(2));
    }
}
