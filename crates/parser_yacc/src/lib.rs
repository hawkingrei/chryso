use cfgrammar::TIdx;
use chryso_core::ast::{
    Assignment, BinaryOperator, ColumnDef, Cte, DeleteStatement, Expr, InsertSource,
    InsertStatement, Join, JoinType, Literal, SelectItem, SelectStatement, Statement, TableFactor,
    TableRef, TruncateStatement, UpdateStatement, WithStatement,
};
use chryso_core::{ChrysoError, ChrysoResult};
use chryso_parser::{ParserConfig, SqlParser};
use lrpar::{Lexeme as _, Node};

lrlex::lrlex_mod!("sql.l");
lrpar::lrpar_mod!("sql.y");

pub struct YaccParser {
    #[allow(dead_code)]
    config: ParserConfig,
}

impl YaccParser {
    pub fn new(config: ParserConfig) -> Self {
        Self { config }
    }
}

impl SqlParser for YaccParser {
    fn parse(&self, sql: &str) -> ChrysoResult<Statement> {
        let lexerdef = sql_l::lexerdef();
        let lexer = lexerdef.lexer(sql);
        let (result, errors) = sql_y::parse(&lexer);
        if !errors.is_empty() {
            let mut rendered = Vec::new();
            for error in errors {
                rendered.push(error.pp(&lexer, &sql_y::token_epp).to_string());
            }
            return Err(ChrysoError::new(rendered.join("\n")));
        }
        let Some(tree) = result else {
            return Err(ChrysoError::new(
                "yacc parser failed to produce a statement",
            ));
        };
        let builder = AstBuilder::new(sql);
        builder.statement(&tree)
    }
}

type Lexeme = lrlex::defaults::DefaultLexeme<u32>;

struct AstBuilder<'a> {
    input: &'a str,
}

impl<'a> AstBuilder<'a> {
    fn new(input: &'a str) -> Self {
        Self { input }
    }

    fn statement(&self, node: &Node<Lexeme, u32>) -> ChrysoResult<Statement> {
        let (_, nodes) = self.expect_nonterm(node, sql_y::R_STATEMENT)?;
        let Some(Node::Nonterm { ridx, .. }) = nodes.get(0) else {
            return Err(self.err("expected statement child"));
        };
        let ridx = ridx.as_storaget();
        if ridx == sql_y::R_WITHSTMT {
            return self.with_stmt(&nodes[0]);
        }
        if ridx == sql_y::R_SELECTSTMT {
            return self.select_stmt(&nodes[0]);
        }
        if ridx == sql_y::R_INSERTSTMT {
            return self.insert_stmt(&nodes[0]);
        }
        if ridx == sql_y::R_UPDATESTMT {
            return self.update_stmt(&nodes[0]);
        }
        if ridx == sql_y::R_DELETESTMT {
            return self.delete_stmt(&nodes[0]);
        }
        if ridx == sql_y::R_CREATESTMT {
            return self.create_stmt(&nodes[0]);
        }
        if ridx == sql_y::R_DROPSTMT {
            return self.drop_stmt(&nodes[0]);
        }
        if ridx == sql_y::R_TRUNCATESTMT {
            return self.truncate_stmt(&nodes[0]);
        }
        Err(self.err("unsupported statement"))
    }

    fn create_stmt(&self, node: &Node<Lexeme, u32>) -> ChrysoResult<Statement> {
        // CREATE TABLE IfNotExistsOpt IDENT ColumnDefsOpt
        let (_, nodes) = self.expect_nonterm(node, sql_y::R_CREATESTMT)?;
        let if_not_exists = self.if_not_exists_opt(&nodes[2])?;
        let name = self.ident_from_term(&nodes[3])?;
        let columns = self.column_defs_opt(&nodes[4])?;
        Ok(Statement::CreateTable(
            chryso_core::ast::CreateTableStatement {
                name,
                if_not_exists,
                columns,
            },
        ))
    }

    fn drop_stmt(&self, node: &Node<Lexeme, u32>) -> ChrysoResult<Statement> {
        // DROP TABLE IfExistsOpt IDENT
        let (_, nodes) = self.expect_nonterm(node, sql_y::R_DROPSTMT)?;
        let if_exists = self.if_exists_opt(&nodes[2])?;
        let name = self.ident_from_term(&nodes[3])?;
        Ok(Statement::DropTable(chryso_core::ast::DropTableStatement {
            name,
            if_exists,
        }))
    }

    fn truncate_stmt(&self, node: &Node<Lexeme, u32>) -> ChrysoResult<Statement> {
        // TRUNCATE TableOpt IDENT
        let (_, nodes) = self.expect_nonterm(node, sql_y::R_TRUNCATESTMT)?;
        let name = self.ident_from_term(&nodes[2])?;
        Ok(Statement::Truncate(TruncateStatement { table: name }))
    }

    fn with_stmt(&self, node: &Node<Lexeme, u32>) -> ChrysoResult<Statement> {
        // WITH RecursiveOpt CteList Statement
        let (_, nodes) = self.expect_nonterm(node, sql_y::R_WITHSTMT)?;
        let recursive = self.recursive_opt(&nodes[1])?;
        let ctes = self.cte_list(&nodes[2])?;
        let statement = self.statement(&nodes[3])?;
        Ok(Statement::With(WithStatement {
            ctes,
            recursive,
            statement: Box::new(statement),
        }))
    }

    fn cte_list(&self, node: &Node<Lexeme, u32>) -> ChrysoResult<Vec<Cte>> {
        let (ridx, nodes) = self.expect_nonterm(node, sql_y::R_CTELIST)?;
        if nodes.len() == 1 {
            return Ok(vec![self.cte(&nodes[0])?]);
        }
        if nodes.len() == 3 {
            let mut items = self.cte_list(&nodes[0])?;
            items.push(self.cte(&nodes[2])?);
            return Ok(items);
        }
        Err(self.err_with_rule(ridx))
    }

    fn cte(&self, node: &Node<Lexeme, u32>) -> ChrysoResult<Cte> {
        // IDENT OptColumnList AS LPAREN Statement RPAREN
        let (_, nodes) = self.expect_nonterm(node, sql_y::R_CTE)?;
        let name = self.ident_from_term(&nodes[0])?;
        let columns = self.opt_column_list(&nodes[1])?;
        let statement = self.statement(&nodes[4])?;
        Ok(Cte {
            name,
            columns,
            query: Box::new(statement),
        })
    }

    fn select_stmt(&self, node: &Node<Lexeme, u32>) -> ChrysoResult<Statement> {
        let (ridx, nodes) = self.expect_nonterm(node, sql_y::R_SELECTSTMT)?;
        if nodes.len() == 1 {
            return self.select_core(&nodes[0]);
        }
        let left = self.select_stmt(&nodes[0])?;
        let right = self.select_core(&nodes[nodes.len() - 1])?;
        let op = match nodes.len() {
            3 => self.set_op_from_term(&nodes[1], false)?,
            4 => self.set_op_from_term(&nodes[1], true)?,
            _ => return Err(self.err_with_rule(ridx)),
        };
        Ok(Statement::SetOp {
            left: Box::new(left),
            op,
            right: Box::new(right),
        })
    }

    fn select_core(&self, node: &Node<Lexeme, u32>) -> ChrysoResult<Statement> {
        // SELECT DistinctOpt SelectList FromClauseOpt WhereClause
        let (_, nodes) = self.expect_nonterm(node, sql_y::R_SELECTCORE)?;
        let (distinct, distinct_on) = self.distinct_opt(&nodes[1])?;
        let projection = self.select_list(&nodes[2])?;
        let from = self.from_clause_opt(&nodes[3])?;
        let selection = self.where_clause(&nodes[4])?;
        Ok(Statement::Select(SelectStatement {
            distinct,
            distinct_on,
            projection,
            from,
            selection,
            group_by: Vec::new(),
            having: None,
            qualify: None,
            order_by: Vec::new(),
            limit: None,
            offset: None,
        }))
    }

    fn from_clause_opt(&self, node: &Node<Lexeme, u32>) -> ChrysoResult<Option<TableRef>> {
        let (_, nodes) = self.expect_nonterm(node, sql_y::R_FROMCLAUSEOPT)?;
        if nodes.is_empty() {
            return Ok(None);
        }
        let from = self.from_clause(&nodes[0])?;
        Ok(Some(from))
    }

    fn from_clause(&self, node: &Node<Lexeme, u32>) -> ChrysoResult<TableRef> {
        let (_, nodes) = self.expect_nonterm(node, sql_y::R_FROMCLAUSE)?;
        let tables = self.table_ref_list(&nodes[1])?;
        self.fold_table_refs(tables)
    }

    fn table_ref_list(&self, node: &Node<Lexeme, u32>) -> ChrysoResult<Vec<TableRef>> {
        let (ridx, nodes) = self.expect_nonterm(node, sql_y::R_TABLEREFLIST)?;
        if nodes.len() == 1 {
            return Ok(vec![self.table_ref(&nodes[0])?]);
        }
        if nodes.len() == 3 {
            let mut items = self.table_ref_list(&nodes[0])?;
            items.push(self.table_ref(&nodes[2])?);
            return Ok(items);
        }
        Err(self.err_with_rule(ridx))
    }

    fn table_ref(&self, node: &Node<Lexeme, u32>) -> ChrysoResult<TableRef> {
        // TableFactor OptAlias JoinList
        let (_, nodes) = self.expect_nonterm(node, sql_y::R_TABLEREF)?;
        let factor = self.table_factor(&nodes[0])?;
        let (alias, column_aliases) = self.opt_alias(&nodes[1])?;
        let mut table = TableRef {
            factor,
            alias,
            column_aliases,
            joins: Vec::new(),
        };
        let joins = self.join_list(&nodes[2], &table)?;
        table.joins = joins;
        Ok(table)
    }

    fn table_factor(&self, node: &Node<Lexeme, u32>) -> ChrysoResult<TableFactor> {
        let (ridx, nodes) = self.expect_nonterm(node, sql_y::R_TABLEFACTOR)?;
        if nodes.len() == 1 {
            let name = self.ident_from_term(&nodes[0])?;
            return Ok(TableFactor::Table { name });
        }
        if nodes.len() == 3 {
            let stmt = self.statement(&nodes[1])?;
            return Ok(TableFactor::Derived {
                query: Box::new(stmt),
            });
        }
        Err(self.err_with_rule(ridx))
    }

    fn join_list(&self, node: &Node<Lexeme, u32>, left: &TableRef) -> ChrysoResult<Vec<Join>> {
        let (ridx, nodes) = self.expect_nonterm(node, sql_y::R_JOINLIST)?;
        if nodes.is_empty() {
            return Ok(Vec::new());
        }
        if nodes.len() == 2 {
            let mut joins = self.join_list(&nodes[0], left)?;
            joins.push(self.join_clause(&nodes[1], left)?);
            return Ok(joins);
        }
        Err(self.err_with_rule(ridx))
    }

    fn join_clause(&self, node: &Node<Lexeme, u32>, left: &TableRef) -> ChrysoResult<Join> {
        let (ridx, nodes) = self.expect_nonterm(node, sql_y::R_JOINCLAUSE)?;
        if nodes.len() != 1 {
            return Err(self.err_with_rule(ridx));
        }
        let child = self.expect_nonterm_child(&nodes, 0)?;
        if child.0 == sql_y::R_REGULARJOIN {
            return self.regular_join(&child.1, left);
        }
        if child.0 == sql_y::R_CROSSJOIN {
            return self.cross_join(&child.1);
        }
        if child.0 == sql_y::R_NATURALJOIN {
            return self.natural_join(&child.1);
        }
        Err(self.err("unsupported join clause"))
    }

    fn regular_join(&self, nodes: &[Node<Lexeme, u32>], left: &TableRef) -> ChrysoResult<Join> {
        let join_type = self.join_type_regular(&nodes[0])?;
        let right = self.table_ref(&nodes[1])?;
        let on = self.join_condition(&nodes[2], left, &right)?;
        Ok(Join {
            join_type,
            right,
            on,
        })
    }

    fn cross_join(&self, nodes: &[Node<Lexeme, u32>]) -> ChrysoResult<Join> {
        let right = self.table_ref(&nodes[2])?;
        Ok(Join {
            join_type: JoinType::Inner,
            right,
            on: Expr::Literal(Literal::Bool(true)),
        })
    }

    fn natural_join(&self, nodes: &[Node<Lexeme, u32>]) -> ChrysoResult<Join> {
        let join_type = self.natural_join_type(&nodes[0])?;
        let right = self.table_ref(&nodes[1])?;
        Ok(Join {
            join_type,
            right,
            on: Expr::Literal(Literal::Bool(true)),
        })
    }

    fn join_type_regular(&self, node: &Node<Lexeme, u32>) -> ChrysoResult<JoinType> {
        let (_, nodes) = self.expect_nonterm(node, sql_y::R_JOINTYPEREGULAR)?;
        let keyword = self.term_text(&nodes[0])?;
        let join_type = match keyword.to_uppercase().as_str() {
            "JOIN" => JoinType::Inner,
            "LEFT" => JoinType::Left,
            "RIGHT" => JoinType::Right,
            "FULL" => JoinType::Full,
            _ => return Err(self.err("unexpected join type")),
        };
        Ok(join_type)
    }

    fn natural_join_type(&self, node: &Node<Lexeme, u32>) -> ChrysoResult<JoinType> {
        let (_, nodes) = self.expect_nonterm(node, sql_y::R_NATURALJOINTYPE)?;
        if nodes.len() == 2 {
            return Ok(JoinType::Inner);
        }
        let keyword = self.term_text(&nodes[1])?;
        let join_type = match keyword.to_uppercase().as_str() {
            "LEFT" => JoinType::Left,
            "RIGHT" => JoinType::Right,
            "FULL" => JoinType::Full,
            _ => return Err(self.err("unexpected natural join type")),
        };
        Ok(join_type)
    }

    fn join_condition(
        &self,
        node: &Node<Lexeme, u32>,
        left: &TableRef,
        right: &TableRef,
    ) -> ChrysoResult<Expr> {
        let (_, nodes) = self.expect_nonterm(node, sql_y::R_JOINCONDITION)?;
        let keyword = self.term_text(&nodes[0])?;
        if keyword.eq_ignore_ascii_case("ON") {
            return self.expr(&nodes[1]);
        }
        if keyword.eq_ignore_ascii_case("USING") {
            let columns = self.ident_list(&nodes[2])?;
            let left_name = self.table_ref_name(left)?;
            let right_name = self.table_ref_name(right)?;
            return Ok(build_using_on(&left_name, &right_name, columns));
        }
        Err(self.err("unexpected join condition"))
    }

    fn select_list(&self, node: &Node<Lexeme, u32>) -> ChrysoResult<Vec<SelectItem>> {
        let (ridx, nodes) = self.expect_nonterm(node, sql_y::R_SELECTLIST)?;
        if nodes.len() == 1 {
            match &nodes[0] {
                Node::Term { .. } => {
                    let text = self.term_text(&nodes[0])?;
                    if text == "*" {
                        return Ok(vec![SelectItem {
                            expr: Expr::Wildcard,
                            alias: None,
                        }]);
                    }
                    return Ok(vec![SelectItem {
                        expr: self.expr(&nodes[0])?,
                        alias: None,
                    }]);
                }
                _ => {
                    return Ok(vec![SelectItem {
                        expr: self.expr(&nodes[0])?,
                        alias: None,
                    }]);
                }
            }
        }
        if nodes.len() == 3 {
            let mut items = self.select_list(&nodes[0])?;
            items.push(SelectItem {
                expr: self.expr(&nodes[2])?,
                alias: None,
            });
            return Ok(items);
        }
        Err(self.err_with_rule(ridx))
    }

    fn distinct_opt(&self, node: &Node<Lexeme, u32>) -> ChrysoResult<(bool, Vec<Expr>)> {
        let (_, nodes) = self.expect_nonterm(node, sql_y::R_DISTINCTOPT)?;
        if nodes.is_empty() {
            return Ok((false, Vec::new()));
        }
        if nodes.len() == 1 {
            return Ok((true, Vec::new()));
        }
        let exprs = self.expr_list(&nodes[3])?;
        Ok((true, exprs))
    }

    fn where_clause(&self, node: &Node<Lexeme, u32>) -> ChrysoResult<Option<Expr>> {
        let (_, nodes) = self.expect_nonterm(node, sql_y::R_WHERECLAUSE)?;
        if nodes.is_empty() {
            return Ok(None);
        }
        Ok(Some(self.expr(&nodes[1])?))
    }

    fn insert_stmt(&self, node: &Node<Lexeme, u32>) -> ChrysoResult<Statement> {
        let (_, nodes) = self.expect_nonterm(node, sql_y::R_INSERTSTMT)?;
        let table = self.ident_from_term(&nodes[2])?;
        let columns = self.opt_column_list(&nodes[3])?;
        let (source, returning) = match nodes.get(4) {
            Some(Node::Term { .. }) => {
                let keyword = self.term_text(&nodes[4])?.to_uppercase();
                if keyword == "DEFAULT" {
                    (
                        InsertSource::DefaultValues,
                        self.returning_clause(&nodes[6])?,
                    )
                } else if keyword == "VALUES" {
                    (
                        InsertSource::Values(self.values_list(&nodes[5])?),
                        self.returning_clause(&nodes[6])?,
                    )
                } else {
                    return Err(self.err("unsupported insert source"));
                }
            }
            Some(Node::Nonterm { .. }) => (
                InsertSource::Query(Box::new(self.select_stmt(&nodes[4])?)),
                self.returning_clause(&nodes[5])?,
            ),
            None => return Err(self.err("malformed insert statement")),
        };
        Ok(Statement::Insert(InsertStatement {
            table,
            columns,
            source,
            returning,
        }))
    }

    fn values_list(&self, node: &Node<Lexeme, u32>) -> ChrysoResult<Vec<Vec<Expr>>> {
        let (ridx, nodes) = self.expect_nonterm(node, sql_y::R_VALUESLIST)?;
        if nodes.len() == 3 {
            return Ok(vec![self.expr_list(&nodes[1])?]);
        }
        if nodes.len() == 5 {
            let mut items = self.values_list(&nodes[0])?;
            items.push(self.expr_list(&nodes[3])?);
            return Ok(items);
        }
        Err(self.err_with_rule(ridx))
    }

    fn update_stmt(&self, node: &Node<Lexeme, u32>) -> ChrysoResult<Statement> {
        let (_, nodes) = self.expect_nonterm(node, sql_y::R_UPDATESTMT)?;
        let table = self.ident_from_term(&nodes[1])?;
        let assignments = self.assignment_list(&nodes[3])?;
        let selection = self.where_clause(&nodes[4])?;
        let returning = self.returning_clause(&nodes[5])?;
        Ok(Statement::Update(UpdateStatement {
            table,
            assignments,
            selection,
            returning,
        }))
    }

    fn delete_stmt(&self, node: &Node<Lexeme, u32>) -> ChrysoResult<Statement> {
        let (_, nodes) = self.expect_nonterm(node, sql_y::R_DELETESTMT)?;
        let table = self.ident_from_term(&nodes[2])?;
        let selection = self.where_clause(&nodes[3])?;
        let returning = self.returning_clause(&nodes[4])?;
        Ok(Statement::Delete(DeleteStatement {
            table,
            selection,
            returning,
        }))
    }

    fn returning_clause(&self, node: &Node<Lexeme, u32>) -> ChrysoResult<Vec<SelectItem>> {
        let (_, nodes) = self.expect_nonterm(node, sql_y::R_RETURNINGCLAUSE)?;
        if nodes.is_empty() {
            return Ok(Vec::new());
        }
        self.select_list(&nodes[1])
    }

    fn assignment_list(&self, node: &Node<Lexeme, u32>) -> ChrysoResult<Vec<Assignment>> {
        let (ridx, nodes) = self.expect_nonterm(node, sql_y::R_ASSIGNMENTLIST)?;
        if nodes.len() == 1 {
            return Ok(vec![self.assignment(&nodes[0])?]);
        }
        if nodes.len() == 3 {
            let mut items = self.assignment_list(&nodes[0])?;
            items.push(self.assignment(&nodes[2])?);
            return Ok(items);
        }
        Err(self.err_with_rule(ridx))
    }

    fn assignment(&self, node: &Node<Lexeme, u32>) -> ChrysoResult<Assignment> {
        let (_, nodes) = self.expect_nonterm(node, sql_y::R_ASSIGNMENT)?;
        let column = self.ident_from_term(&nodes[0])?;
        let value = self.expr(&nodes[2])?;
        Ok(Assignment { column, value })
    }

    fn expr_list(&self, node: &Node<Lexeme, u32>) -> ChrysoResult<Vec<Expr>> {
        let (ridx, nodes) = self.expect_nonterm(node, sql_y::R_EXPRLIST)?;
        if nodes.len() == 1 {
            return Ok(vec![self.expr(&nodes[0])?]);
        }
        if nodes.len() == 3 {
            let mut items = self.expr_list(&nodes[0])?;
            items.push(self.expr(&nodes[2])?);
            return Ok(items);
        }
        Err(self.err_with_rule(ridx))
    }

    fn ident_list(&self, node: &Node<Lexeme, u32>) -> ChrysoResult<Vec<String>> {
        let (ridx, nodes) = self.expect_nonterm(node, sql_y::R_IDENTLIST)?;
        if nodes.len() == 1 {
            return Ok(vec![self.ident_from_term(&nodes[0])?]);
        }
        if nodes.len() == 3 {
            let mut items = self.ident_list(&nodes[0])?;
            items.push(self.ident_from_term(&nodes[2])?);
            return Ok(items);
        }
        Err(self.err_with_rule(ridx))
    }

    fn expr(&self, node: &Node<Lexeme, u32>) -> ChrysoResult<Expr> {
        match node {
            Node::Term { .. } => self.expr_from_term(node),
            Node::Nonterm { .. } => {
                let (ridx, nodes) = self.expect_nonterm(node, sql_y::R_EXPR)?;
                if nodes.len() == 1 {
                    return self.expr(&nodes[0]);
                }
                if nodes.len() == 3 {
                    if self.is_term_kind(&nodes[0], "IDENT")
                        && self.is_term_kind(&nodes[1], "DOT")
                        && (self.is_term_kind(&nodes[2], "IDENT")
                            || self.is_term_kind(&nodes[2], "STAR"))
                    {
                        let left = self.ident_from_term(&nodes[0])?;
                        let right = self.term_text(&nodes[2])?;
                        return Ok(Expr::Identifier(format!("{left}.{right}")));
                    }
                    if self.is_term_kind(&nodes[0], "LPAREN")
                        && self.is_term_kind(&nodes[2], "RPAREN")
                    {
                        return self.expr(&nodes[1]);
                    }
                    let op_text = self.term_text(&nodes[1])?;
                    let op = match op_text.to_uppercase().as_str() {
                        "AND" => BinaryOperator::And,
                        "OR" => BinaryOperator::Or,
                        "=" => BinaryOperator::Eq,
                        _ => return Err(self.err("unsupported binary operator")),
                    };
                    return Ok(Expr::BinaryOp {
                        left: Box::new(self.expr(&nodes[0])?),
                        op,
                        right: Box::new(self.expr(&nodes[2])?),
                    });
                }
                Err(self.err_with_rule(ridx))
            }
        }
    }

    fn expr_from_term(&self, node: &Node<Lexeme, u32>) -> ChrysoResult<Expr> {
        let text = self.term_text(node)?;
        if text == "*" {
            return Ok(Expr::Wildcard);
        }
        if let Ok(value) = text.parse::<f64>() {
            return Ok(Expr::Literal(Literal::Number(value)));
        }
        if text.starts_with('\'') {
            return Ok(Expr::Literal(Literal::String(decode_string_literal(
                text.as_str(),
            )?)));
        }
        Ok(Expr::Identifier(text))
    }

    fn opt_column_list(&self, node: &Node<Lexeme, u32>) -> ChrysoResult<Vec<String>> {
        let (_, nodes) = self.expect_nonterm(node, sql_y::R_OPTCOLUMNLIST)?;
        if nodes.is_empty() {
            return Ok(Vec::new());
        }
        self.ident_list(&nodes[1])
    }

    fn if_exists_opt(&self, node: &Node<Lexeme, u32>) -> ChrysoResult<bool> {
        let (_, nodes) = self.expect_nonterm(node, sql_y::R_IFEXISTSOPT)?;
        Ok(!nodes.is_empty())
    }

    fn if_not_exists_opt(&self, node: &Node<Lexeme, u32>) -> ChrysoResult<bool> {
        let (_, nodes) = self.expect_nonterm(node, sql_y::R_IFNOTEXISTSOPT)?;
        Ok(!nodes.is_empty())
    }

    fn recursive_opt(&self, node: &Node<Lexeme, u32>) -> ChrysoResult<bool> {
        let (_, nodes) = self.expect_nonterm(node, sql_y::R_RECURSIVEOPT)?;
        Ok(!nodes.is_empty())
    }

    fn column_defs_opt(&self, node: &Node<Lexeme, u32>) -> ChrysoResult<Vec<ColumnDef>> {
        let (_, nodes) = self.expect_nonterm(node, sql_y::R_COLUMNDEFSOPT)?;
        if nodes.is_empty() {
            return Ok(Vec::new());
        }
        self.column_def_list(&nodes[1])
    }

    fn column_def_list(&self, node: &Node<Lexeme, u32>) -> ChrysoResult<Vec<ColumnDef>> {
        let (ridx, nodes) = self.expect_nonterm(node, sql_y::R_COLUMNDEFLIST)?;
        if nodes.len() == 1 {
            return Ok(vec![self.column_def(&nodes[0])?]);
        }
        if nodes.len() == 3 {
            let mut items = self.column_def_list(&nodes[0])?;
            items.push(self.column_def(&nodes[2])?);
            return Ok(items);
        }
        Err(self.err_with_rule(ridx))
    }

    fn column_def(&self, node: &Node<Lexeme, u32>) -> ChrysoResult<ColumnDef> {
        let (_, nodes) = self.expect_nonterm(node, sql_y::R_COLUMNDEF)?;
        let name = self.ident_from_term(&nodes[0])?;
        let data_type = self.type_name(&nodes[1])?;
        Ok(ColumnDef { name, data_type })
    }

    fn type_name(&self, node: &Node<Lexeme, u32>) -> ChrysoResult<String> {
        let (ridx, nodes) = self.expect_nonterm(node, sql_y::R_TYPENAME)?;
        let parts: Vec<String> = nodes
            .iter()
            .filter_map(|node| match node {
                Node::Term { .. } => Some(self.term_text(node)),
                _ => None,
            })
            .collect::<ChrysoResult<Vec<_>>>()?;
        match parts.len() {
            1 => Ok(parts[0].clone()),
            2 => Ok(format!("{} {}", parts[0], parts[1])),
            4 => Ok(format!("{}({})", parts[0], parts[2])),
            5 => Ok(format!("{} {}({})", parts[0], parts[1], parts[3])),
            _ => Err(self.err_with_rule(ridx)),
        }
    }

    fn opt_alias(&self, node: &Node<Lexeme, u32>) -> ChrysoResult<(Option<String>, Vec<String>)> {
        let (_, nodes) = self.expect_nonterm(node, sql_y::R_OPTALIAS)?;
        if nodes.is_empty() {
            return Ok((None, Vec::new()));
        }
        if self.is_term_kind(&nodes[0], "AS") {
            let name = self.ident_from_term(&nodes[1])?;
            let column_aliases = if nodes.len() == 3 {
                self.column_alias_list_opt(&nodes[2])?
            } else {
                Vec::new()
            };
            return Ok((Some(name), column_aliases));
        }
        let name = self.ident_from_term(&nodes[0])?;
        let column_aliases = if nodes.len() == 2 {
            self.column_alias_list_opt(&nodes[1])?
        } else {
            Vec::new()
        };
        Ok((Some(name), column_aliases))
    }

    fn column_alias_list_opt(&self, node: &Node<Lexeme, u32>) -> ChrysoResult<Vec<String>> {
        let (_, nodes) = self.expect_nonterm(node, sql_y::R_COLUMNALIASLISTOPT)?;
        if nodes.is_empty() {
            return Ok(Vec::new());
        }
        self.ident_list(&nodes[1])
    }

    fn fold_table_refs(&self, mut tables: Vec<TableRef>) -> ChrysoResult<TableRef> {
        if tables.is_empty() {
            return Err(self.err("empty FROM clause"));
        }
        let mut table = tables.remove(0);
        for right in tables {
            table.joins.push(Join {
                join_type: JoinType::Inner,
                right,
                on: Expr::Literal(Literal::Bool(true)),
            });
        }
        Ok(table)
    }

    fn table_ref_name(&self, table: &TableRef) -> ChrysoResult<String> {
        if let Some(alias) = &table.alias {
            return Ok(alias.clone());
        }
        match &table.factor {
            TableFactor::Table { name } => Ok(name.clone()),
            TableFactor::Derived { .. } => Err(ChrysoError::new("subquery in FROM requires alias")),
        }
    }

    fn set_op_from_term(
        &self,
        node: &Node<Lexeme, u32>,
        all: bool,
    ) -> ChrysoResult<chryso_core::ast::SetOperator> {
        let keyword = self.term_text(node)?;
        let op = match keyword.to_uppercase().as_str() {
            "UNION" => {
                if all {
                    chryso_core::ast::SetOperator::UnionAll
                } else {
                    chryso_core::ast::SetOperator::Union
                }
            }
            "INTERSECT" => {
                if all {
                    chryso_core::ast::SetOperator::IntersectAll
                } else {
                    chryso_core::ast::SetOperator::Intersect
                }
            }
            "EXCEPT" => {
                if all {
                    chryso_core::ast::SetOperator::ExceptAll
                } else {
                    chryso_core::ast::SetOperator::Except
                }
            }
            _ => return Err(self.err("unsupported set operator")),
        };
        Ok(op)
    }

    fn expect_nonterm<'b>(
        &self,
        node: &'b Node<Lexeme, u32>,
        expected: u32,
    ) -> ChrysoResult<(u32, &'b [Node<Lexeme, u32>])> {
        match node {
            Node::Nonterm { ridx, nodes } => {
                let ridx = ridx.as_storaget();
                if ridx != expected {
                    return Err(self.err("unexpected nonterminal"));
                }
                Ok((ridx, nodes))
            }
            Node::Term { .. } => Err(self.err("expected nonterminal")),
        }
    }

    fn expect_nonterm_child<'b>(
        &self,
        nodes: &'b [Node<Lexeme, u32>],
        idx: usize,
    ) -> ChrysoResult<(u32, &'b [Node<Lexeme, u32>])> {
        match nodes.get(idx) {
            Some(Node::Nonterm { ridx, nodes }) => Ok((ridx.as_storaget(), nodes)),
            _ => Err(self.err("expected nonterminal child")),
        }
    }

    fn term_text(&self, node: &Node<Lexeme, u32>) -> ChrysoResult<String> {
        match node {
            Node::Term { lexeme } => {
                let span = lexeme.span();
                Ok(self.input[span.start()..span.end()].to_string())
            }
            _ => Err(self.err("expected terminal")),
        }
    }

    fn ident_from_term(&self, node: &Node<Lexeme, u32>) -> ChrysoResult<String> {
        self.term_text(node)
    }

    fn is_term_kind(&self, node: &Node<Lexeme, u32>, value: &str) -> bool {
        match node {
            Node::Term { lexeme } => sql_y::token_epp(TIdx(lexeme.tok_id()))
                .map(|name| name == value)
                .unwrap_or(false),
            _ => false,
        }
    }

    fn err(&self, msg: &str) -> ChrysoError {
        ChrysoError::new(msg.to_string())
    }

    fn err_with_rule(&self, ridx: u32) -> ChrysoError {
        ChrysoError::new(format!("unexpected parse tree for rule {ridx}"))
    }
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

fn decode_string_literal(input: &str) -> ChrysoResult<String> {
    let content = input
        .strip_prefix('\'')
        .and_then(|value| value.strip_suffix('\''))
        .ok_or_else(|| ChrysoError::new("invalid string literal".to_string()))?;
    let mut out = String::new();
    let mut chars = content.chars().peekable();
    while let Some(ch) = chars.next() {
        if ch == '\\' {
            if let Some(next) = chars.next() {
                out.push(next);
            }
            continue;
        }
        if ch == '\'' && chars.peek() == Some(&'\'') {
            out.push('\'');
            chars.next();
            continue;
        }
        out.push(ch);
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use chryso_parser::Dialect;

    #[test]
    fn yacc_parser_parses_select() {
        let parser = YaccParser::new(ParserConfig {
            dialect: Dialect::Postgres,
        });
        let stmt = parser.parse("select 1").expect("parse");
        match stmt {
            chryso_core::ast::Statement::Select(_) => {}
            _ => panic!("expected select"),
        }
    }

    #[test]
    fn yacc_parser_rejects_invalid_sql() {
        let parser = YaccParser::new(ParserConfig {
            dialect: Dialect::Postgres,
        });
        let err = parser.parse("select from").unwrap_err();
        assert!(!err.to_string().is_empty());
    }

    #[test]
    fn yacc_parser_parses_join_using() {
        let parser = YaccParser::new(ParserConfig {
            dialect: Dialect::Postgres,
        });
        let stmt = parser
            .parse("select t1.id from t1 join t2 using (id)")
            .expect("parse");
        let Statement::Select(select) = stmt else {
            panic!("expected select");
        };
        let table = select.from.expect("from");
        assert_eq!(table.joins.len(), 1);
        let on = &table.joins[0].on;
        let Expr::BinaryOp { op, .. } = on else {
            panic!("expected join predicate");
        };
        assert!(matches!(op, BinaryOperator::Eq | BinaryOperator::And));
    }
}
