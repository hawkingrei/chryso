use cfgrammar::TIdx;
use chryso_core::ast::{
    Assignment, BinaryOperator, ColumnDef, Cte, DeleteStatement, Expr, InsertSource,
    InsertStatement, Join, JoinType, Literal, OrderByExpr, SelectItem, SelectStatement, Statement,
    TableFactor, TableRef, TruncateStatement, UpdateStatement, WithStatement,
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
        if nodes.len() != 2 {
            return Err(self.err_with_rule(ridx));
        }
        let mut items = vec![self.cte(&nodes[0])?];
        self.cte_list_tail(&nodes[1], &mut items)?;
        Ok(items)
    }

    fn cte_list_tail(&self, node: &Node<Lexeme, u32>, items: &mut Vec<Cte>) -> ChrysoResult<()> {
        let (_, nodes) = self.expect_nonterm(node, sql_y::R_CTELISTTAIL)?;
        if nodes.is_empty() {
            return Ok(());
        }
        if nodes.len() == 3 {
            items.push(self.cte(&nodes[1])?);
            return self.cte_list_tail(&nodes[2], items);
        }
        Err(self.err("unexpected cte list tail"))
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
        if nodes.len() != 2 {
            return Err(self.err_with_rule(ridx));
        }
        let expr = self.select_expr(&nodes[0])?;
        let (order_by, limit, offset) = self.select_suffix(&nodes[1])?;
        match expr {
            Statement::Select(mut stmt) => {
                stmt.order_by = order_by;
                stmt.limit = limit;
                stmt.offset = offset;
                Ok(Statement::Select(stmt))
            }
            Statement::SetOp { .. } => {
                if order_by.is_empty() && limit.is_none() && offset.is_none() {
                    return Ok(expr);
                }
                Ok(self.wrap_setop_with_suffix(expr, order_by, limit, offset))
            }
            _ => Err(self.err("unexpected select expression")),
        }
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

    fn select_expr(&self, node: &Node<Lexeme, u32>) -> ChrysoResult<Statement> {
        let (ridx, nodes) = self.expect_nonterm(node, sql_y::R_SELECTEXPR)?;
        if nodes.len() == 1 {
            return self.select_term(&nodes[0]);
        }
        if nodes.len() == 3 {
            let left = self.select_expr(&nodes[0])?;
            let right = self.select_term(&nodes[2])?;
            let op = self.set_op_from_term(&nodes[1])?;
            return Ok(Statement::SetOp {
                left: Box::new(left),
                op,
                right: Box::new(right),
            });
        }
        Err(self.err_with_rule(ridx))
    }

    fn select_term(&self, node: &Node<Lexeme, u32>) -> ChrysoResult<Statement> {
        let (ridx, nodes) = self.expect_nonterm(node, sql_y::R_SELECTTERM)?;
        if nodes.len() != 1 {
            return Err(self.err_with_rule(ridx));
        }
        self.select_core(&nodes[0])
    }

    fn select_suffix(
        &self,
        node: &Node<Lexeme, u32>,
    ) -> ChrysoResult<(Vec<OrderByExpr>, Option<u64>, Option<u64>)> {
        let (ridx, nodes) = self.expect_nonterm(node, sql_y::R_SELECTSUFFIX)?;
        if nodes.len() != 3 {
            return Err(self.err_with_rule(ridx));
        }
        let order_by = self.order_by_clause_opt(&nodes[0])?;
        let limit = self.limit_clause_opt(&nodes[1])?;
        let offset = self.offset_clause_opt(&nodes[2])?;
        Ok((order_by, limit, offset))
    }

    fn select_core(&self, node: &Node<Lexeme, u32>) -> ChrysoResult<Statement> {
        // SELECT DistinctOpt SelectList FromClauseOpt WhereClause GroupByClauseOpt HavingClauseOpt
        let (_, nodes) = self.expect_nonterm(node, sql_y::R_SELECTCORE)?;
        let (distinct, distinct_on) = self.distinct_opt(&nodes[1])?;
        let projection = self.select_list(&nodes[2])?;
        let from = self.from_clause_opt(&nodes[3])?;
        let selection = self.where_clause(&nodes[4])?;
        let group_by = self.group_by_clause_opt(&nodes[5])?;
        let having = self.having_clause_opt(&nodes[6])?;
        Ok(Statement::Select(SelectStatement {
            distinct,
            distinct_on,
            projection,
            from,
            selection,
            group_by,
            having,
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
        if nodes.len() != 2 {
            return Err(self.err_with_rule(ridx));
        }
        let mut items = vec![self.table_ref(&nodes[0])?];
        self.table_ref_list_tail(&nodes[1], &mut items)?;
        Ok(items)
    }

    fn table_ref_list_tail(
        &self,
        node: &Node<Lexeme, u32>,
        items: &mut Vec<TableRef>,
    ) -> ChrysoResult<()> {
        let (_, nodes) = self.expect_nonterm(node, sql_y::R_TABLEREFLISTTAIL)?;
        if nodes.is_empty() {
            return Ok(());
        }
        if nodes.len() == 3 {
            items.push(self.table_ref(&nodes[1])?);
            return self.table_ref_list_tail(&nodes[2], items);
        }
        Err(self.err("unexpected table ref list tail"))
    }

    fn table_ref(&self, node: &Node<Lexeme, u32>) -> ChrysoResult<TableRef> {
        // TableFactor OptAlias | TableRef RegularJoin | TableRef CrossJoin | TableRef NaturalJoin
        let (ridx, nodes) = self.expect_nonterm(node, sql_y::R_TABLEREF)?;
        if nodes.len() != 2 {
            return Err(self.err_with_rule(ridx));
        }
        let (
            Node::Nonterm {
                ridx: left_ridx, ..
            },
            Node::Nonterm {
                ridx: right_ridx, ..
            },
        ) = (&nodes[0], &nodes[1])
        else {
            return Err(self.err("expected nonterminal table ref"));
        };
        let left_rule = left_ridx.as_storaget();
        let right_rule = right_ridx.as_storaget();
        if left_rule == sql_y::R_TABLEFACTOR && right_rule == sql_y::R_OPTALIAS {
            return self.table_ref_from_factor_alias(&nodes[0], &nodes[1]);
        }
        if left_rule == sql_y::R_TABLEREF {
            let mut table = self.table_ref(&nodes[0])?;
            let join = if right_rule == sql_y::R_REGULARJOIN {
                self.regular_join(self.expect_nonterm_child(&nodes, 1)?.1, &table)?
            } else if right_rule == sql_y::R_CROSSJOIN {
                self.cross_join(self.expect_nonterm_child(&nodes, 1)?.1)?
            } else if right_rule == sql_y::R_NATURALJOIN {
                self.natural_join(self.expect_nonterm_child(&nodes, 1)?.1)?
            } else {
                return Err(self.err("unsupported join clause"));
            };
            table.joins.push(join);
            return Ok(table);
        }
        Err(self.err("unsupported table ref"))
    }

    fn table_factor(&self, node: &Node<Lexeme, u32>) -> ChrysoResult<TableFactor> {
        let (ridx, nodes) = self.expect_nonterm(node, sql_y::R_TABLEFACTOR)?;
        if nodes.len() == 1 {
            let name = self.ident_from_term(&nodes[0])?;
            return Ok(TableFactor::Table { name });
        }
        if nodes.len() == 3 {
            let stmt = self.subquery_stmt(&nodes[1])?;
            return Ok(TableFactor::Derived {
                query: Box::new(stmt),
            });
        }
        Err(self.err_with_rule(ridx))
    }

    fn subquery_stmt(&self, node: &Node<Lexeme, u32>) -> ChrysoResult<Statement> {
        let Node::Nonterm { ridx, .. } = node else {
            return Err(self.err("expected subquery node"));
        };
        let ridx = ridx.as_storaget();
        if ridx == sql_y::R_SELECTSTMT {
            return self.select_stmt(node);
        }
        if ridx == sql_y::R_WITHSTMT {
            return self.with_stmt(node);
        }
        Err(self.err("unsupported subquery type"))
    }

    fn table_ref_from_factor_alias(
        &self,
        factor_node: &Node<Lexeme, u32>,
        alias_node: &Node<Lexeme, u32>,
    ) -> ChrysoResult<TableRef> {
        let factor = self.table_factor(factor_node)?;
        let (alias, column_aliases) = self.opt_alias(alias_node)?;
        Ok(TableRef {
            factor,
            alias,
            column_aliases,
            joins: Vec::new(),
        })
    }

    fn regular_join(&self, nodes: &[Node<Lexeme, u32>], left: &TableRef) -> ChrysoResult<Join> {
        let join_type = self.join_type_regular(&nodes[0])?;
        let right = self.table_ref_from_factor_alias(&nodes[1], &nodes[2])?;
        let on = self.join_condition(&nodes[3], left, &right)?;
        Ok(Join {
            join_type,
            right,
            on,
        })
    }

    fn cross_join(&self, nodes: &[Node<Lexeme, u32>]) -> ChrysoResult<Join> {
        let right = self.table_ref_from_factor_alias(&nodes[2], &nodes[3])?;
        Ok(Join {
            join_type: JoinType::Inner,
            right,
            on: Expr::Literal(Literal::Bool(true)),
        })
    }

    fn natural_join(&self, nodes: &[Node<Lexeme, u32>]) -> ChrysoResult<Join> {
        let join_type = self.natural_join_type(&nodes[0])?;
        let right = self.table_ref_from_factor_alias(&nodes[1], &nodes[2])?;
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
        if nodes.len() == 2 {
            let mut items = vec![self.select_item(&nodes[0])?];
            self.select_list_tail(&nodes[1], &mut items)?;
            return Ok(items);
        }
        Err(self.err_with_rule(ridx))
    }

    fn select_item(&self, node: &Node<Lexeme, u32>) -> ChrysoResult<SelectItem> {
        let (ridx, nodes) = self.expect_nonterm(node, sql_y::R_SELECTITEM)?;
        if nodes.len() != 1 {
            return Err(self.err_with_rule(ridx));
        }
        if self.is_term_kind(&nodes[0], "STAR") {
            return Ok(SelectItem {
                expr: Expr::Wildcard,
                alias: None,
            });
        }
        Ok(SelectItem {
            expr: self.expr(&nodes[0])?,
            alias: None,
        })
    }

    fn select_list_tail(
        &self,
        node: &Node<Lexeme, u32>,
        items: &mut Vec<SelectItem>,
    ) -> ChrysoResult<()> {
        let (_, nodes) = self.expect_nonterm(node, sql_y::R_SELECTLISTTAIL)?;
        if nodes.is_empty() {
            return Ok(());
        }
        if nodes.len() == 3 {
            items.push(self.select_item(&nodes[1])?);
            return self.select_list_tail(&nodes[2], items);
        }
        Err(self.err("unexpected select list tail"))
    }

    fn distinct_opt(&self, node: &Node<Lexeme, u32>) -> ChrysoResult<(bool, Vec<Expr>)> {
        let (_, nodes) = self.expect_nonterm(node, sql_y::R_DISTINCTOPT)?;
        if nodes.is_empty() {
            return Ok((false, Vec::new()));
        }
        if nodes.len() != 2 {
            return Err(self.err("unexpected distinct clause"));
        }
        let exprs = self.distinct_on_opt(&nodes[1])?;
        Ok((true, exprs))
    }

    fn distinct_on_opt(&self, node: &Node<Lexeme, u32>) -> ChrysoResult<Vec<Expr>> {
        let (_, nodes) = self.expect_nonterm(node, sql_y::R_DISTINCTONOPT)?;
        if nodes.is_empty() {
            return Ok(Vec::new());
        }
        if nodes.len() == 4 {
            return self.expr_list(&nodes[2]);
        }
        Err(self.err("unexpected distinct on clause"))
    }

    fn where_clause(&self, node: &Node<Lexeme, u32>) -> ChrysoResult<Option<Expr>> {
        let (_, nodes) = self.expect_nonterm(node, sql_y::R_WHERECLAUSE)?;
        if nodes.is_empty() {
            return Ok(None);
        }
        Ok(Some(self.expr(&nodes[1])?))
    }

    fn group_by_clause_opt(&self, node: &Node<Lexeme, u32>) -> ChrysoResult<Vec<Expr>> {
        let (_, nodes) = self.expect_nonterm(node, sql_y::R_GROUPBYCLAUSEOPT)?;
        if nodes.is_empty() {
            return Ok(Vec::new());
        }
        self.expr_list(&nodes[2])
    }

    fn having_clause_opt(&self, node: &Node<Lexeme, u32>) -> ChrysoResult<Option<Expr>> {
        let (_, nodes) = self.expect_nonterm(node, sql_y::R_HAVINGCLAUSEOPT)?;
        if nodes.is_empty() {
            return Ok(None);
        }
        Ok(Some(self.expr(&nodes[1])?))
    }

    fn order_by_clause_opt(&self, node: &Node<Lexeme, u32>) -> ChrysoResult<Vec<OrderByExpr>> {
        let (_, nodes) = self.expect_nonterm(node, sql_y::R_ORDERBYCLAUSEOPT)?;
        if nodes.is_empty() {
            return Ok(Vec::new());
        }
        self.order_by_list(&nodes[2])
    }

    fn order_by_list(&self, node: &Node<Lexeme, u32>) -> ChrysoResult<Vec<OrderByExpr>> {
        let (ridx, nodes) = self.expect_nonterm(node, sql_y::R_ORDERBYLIST)?;
        if nodes.len() != 2 {
            return Err(self.err_with_rule(ridx));
        }
        let mut items = vec![self.order_by_expr(&nodes[0])?];
        self.order_by_list_tail(&nodes[1], &mut items)?;
        Ok(items)
    }

    fn order_by_list_tail(
        &self,
        node: &Node<Lexeme, u32>,
        items: &mut Vec<OrderByExpr>,
    ) -> ChrysoResult<()> {
        let (_, nodes) = self.expect_nonterm(node, sql_y::R_ORDERBYLISTTAIL)?;
        if nodes.is_empty() {
            return Ok(());
        }
        if nodes.len() == 3 {
            items.push(self.order_by_expr(&nodes[1])?);
            return self.order_by_list_tail(&nodes[2], items);
        }
        Err(self.err("unexpected order by list tail"))
    }

    fn order_by_expr(&self, node: &Node<Lexeme, u32>) -> ChrysoResult<OrderByExpr> {
        let (_, nodes) = self.expect_nonterm(node, sql_y::R_ORDERBYEXPR)?;
        let expr = self.expr(&nodes[0])?;
        let asc = self.opt_sort_direction(&nodes[1])?;
        let nulls_first = self.opt_nulls_order(&nodes[2])?;
        Ok(OrderByExpr {
            expr,
            asc,
            nulls_first,
        })
    }

    fn opt_sort_direction(&self, node: &Node<Lexeme, u32>) -> ChrysoResult<bool> {
        let (_, nodes) = self.expect_nonterm(node, sql_y::R_OPTSORTDIRECTION)?;
        if nodes.is_empty() {
            return Ok(true);
        }
        if self.is_term_kind(&nodes[0], "DESC") {
            return Ok(false);
        }
        Ok(true)
    }

    fn opt_nulls_order(&self, node: &Node<Lexeme, u32>) -> ChrysoResult<Option<bool>> {
        let (_, nodes) = self.expect_nonterm(node, sql_y::R_OPTNULLSORDER)?;
        if nodes.is_empty() {
            return Ok(None);
        }
        if nodes.len() == 2 && self.is_term_kind(&nodes[1], "LAST") {
            return Ok(Some(false));
        }
        Ok(Some(true))
    }

    fn limit_clause_opt(&self, node: &Node<Lexeme, u32>) -> ChrysoResult<Option<u64>> {
        let (_, nodes) = self.expect_nonterm(node, sql_y::R_LIMITCLAUSEOPT)?;
        if nodes.is_empty() {
            return Ok(None);
        }
        Ok(Some(self.number_from_term(&nodes[1])?))
    }

    fn offset_clause_opt(&self, node: &Node<Lexeme, u32>) -> ChrysoResult<Option<u64>> {
        let (_, nodes) = self.expect_nonterm(node, sql_y::R_OFFSETCLAUSEOPT)?;
        if nodes.is_empty() {
            return Ok(None);
        }
        Ok(Some(self.number_from_term(&nodes[1])?))
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
        if nodes.len() != 2 {
            return Err(self.err_with_rule(ridx));
        }
        let mut items = vec![self.values_row(&nodes[0])?];
        self.values_list_tail(&nodes[1], &mut items)?;
        Ok(items)
    }

    fn values_list_tail(
        &self,
        node: &Node<Lexeme, u32>,
        items: &mut Vec<Vec<Expr>>,
    ) -> ChrysoResult<()> {
        let (_, nodes) = self.expect_nonterm(node, sql_y::R_VALUESLISTTAIL)?;
        if nodes.is_empty() {
            return Ok(());
        }
        if nodes.len() == 3 {
            items.push(self.values_row(&nodes[1])?);
            return self.values_list_tail(&nodes[2], items);
        }
        Err(self.err("unexpected values list tail"))
    }

    fn values_row(&self, node: &Node<Lexeme, u32>) -> ChrysoResult<Vec<Expr>> {
        let (ridx, nodes) = self.expect_nonterm(node, sql_y::R_VALUESROW)?;
        if nodes.len() != 3 {
            return Err(self.err_with_rule(ridx));
        }
        self.expr_list(&nodes[1])
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
        if nodes.len() != 2 {
            return Err(self.err_with_rule(ridx));
        }
        let mut items = vec![self.assignment(&nodes[0])?];
        self.assignment_list_tail(&nodes[1], &mut items)?;
        Ok(items)
    }

    fn assignment_list_tail(
        &self,
        node: &Node<Lexeme, u32>,
        items: &mut Vec<Assignment>,
    ) -> ChrysoResult<()> {
        let (_, nodes) = self.expect_nonterm(node, sql_y::R_ASSIGNMENTLISTTAIL)?;
        if nodes.is_empty() {
            return Ok(());
        }
        if nodes.len() == 3 {
            items.push(self.assignment(&nodes[1])?);
            return self.assignment_list_tail(&nodes[2], items);
        }
        Err(self.err("unexpected assignment list tail"))
    }

    fn assignment(&self, node: &Node<Lexeme, u32>) -> ChrysoResult<Assignment> {
        let (_, nodes) = self.expect_nonterm(node, sql_y::R_ASSIGNMENT)?;
        let column = self.ident_from_term(&nodes[0])?;
        let value = self.expr(&nodes[2])?;
        Ok(Assignment { column, value })
    }

    fn expr_list(&self, node: &Node<Lexeme, u32>) -> ChrysoResult<Vec<Expr>> {
        let (ridx, nodes) = self.expect_nonterm(node, sql_y::R_EXPRLIST)?;
        if nodes.len() == 2 {
            let mut items = vec![self.expr(&nodes[0])?];
            self.expr_list_tail(&nodes[1], &mut items)?;
            return Ok(items);
        }
        Err(self.err_with_rule(ridx))
    }

    fn expr_list_tail(&self, node: &Node<Lexeme, u32>, items: &mut Vec<Expr>) -> ChrysoResult<()> {
        let (_, nodes) = self.expect_nonterm(node, sql_y::R_EXPRLISTTAIL)?;
        if nodes.is_empty() {
            return Ok(());
        }
        if nodes.len() == 3 {
            items.push(self.expr(&nodes[1])?);
            return self.expr_list_tail(&nodes[2], items);
        }
        Err(self.err("unexpected expr list tail"))
    }

    fn ident_list(&self, node: &Node<Lexeme, u32>) -> ChrysoResult<Vec<String>> {
        let (ridx, nodes) = self.expect_nonterm(node, sql_y::R_IDENTLIST)?;
        if nodes.len() == 2 {
            let mut items = vec![self.ident_from_term(&nodes[0])?];
            self.ident_list_tail(&nodes[1], &mut items)?;
            return Ok(items);
        }
        Err(self.err_with_rule(ridx))
    }

    fn ident_list_tail(
        &self,
        node: &Node<Lexeme, u32>,
        items: &mut Vec<String>,
    ) -> ChrysoResult<()> {
        let (_, nodes) = self.expect_nonterm(node, sql_y::R_IDENTLISTTAIL)?;
        if nodes.is_empty() {
            return Ok(());
        }
        if nodes.len() == 3 {
            items.push(self.ident_from_term(&nodes[1])?);
            return self.ident_list_tail(&nodes[2], items);
        }
        Err(self.err("unexpected ident list tail"))
    }

    fn expr(&self, node: &Node<Lexeme, u32>) -> ChrysoResult<Expr> {
        match node {
            Node::Term { .. } => self.expr_from_term(node),
            Node::Nonterm { .. } => {
                let (ridx, nodes) = self.expect_nonterm(node, sql_y::R_EXPR)?;
                if nodes.len() != 1 {
                    return Err(self.err_with_rule(ridx));
                }
                self.or_expr(&nodes[0])
            }
        }
    }

    fn or_expr(&self, node: &Node<Lexeme, u32>) -> ChrysoResult<Expr> {
        let (ridx, nodes) = self.expect_nonterm(node, sql_y::R_OREXPR)?;
        if nodes.len() != 2 {
            return Err(self.err_with_rule(ridx));
        }
        let mut left = self.and_expr(&nodes[0])?;
        let mut current_tail = &nodes[1];
        loop {
            let (_, tail_nodes) = self.expect_nonterm(current_tail, sql_y::R_OREXPRTAIL)?;
            if tail_nodes.is_empty() {
                break;
            }
            if tail_nodes.len() == 3 {
                let right = self.and_expr(&tail_nodes[1])?;
                left = Expr::BinaryOp {
                    left: Box::new(left),
                    op: BinaryOperator::Or,
                    right: Box::new(right),
                };
                current_tail = &tail_nodes[2];
            } else {
                return Err(self.err("unexpected or expr tail"));
            }
        }
        Ok(left)
    }

    fn and_expr(&self, node: &Node<Lexeme, u32>) -> ChrysoResult<Expr> {
        let (ridx, nodes) = self.expect_nonterm(node, sql_y::R_ANDEXPR)?;
        if nodes.len() != 2 {
            return Err(self.err_with_rule(ridx));
        }
        let mut left = self.eq_expr(&nodes[0])?;
        let mut current_tail = &nodes[1];
        loop {
            let (_, tail_nodes) = self.expect_nonterm(current_tail, sql_y::R_ANDEXPRTAIL)?;
            if tail_nodes.is_empty() {
                break;
            }
            if tail_nodes.len() == 3 {
                let right = self.eq_expr(&tail_nodes[1])?;
                left = Expr::BinaryOp {
                    left: Box::new(left),
                    op: BinaryOperator::And,
                    right: Box::new(right),
                };
                current_tail = &tail_nodes[2];
            } else {
                return Err(self.err("unexpected and expr tail"));
            }
        }
        Ok(left)
    }

    fn eq_expr(&self, node: &Node<Lexeme, u32>) -> ChrysoResult<Expr> {
        let (ridx, nodes) = self.expect_nonterm(node, sql_y::R_EQEXPR)?;
        if nodes.len() != 2 {
            return Err(self.err_with_rule(ridx));
        }
        let mut left = self.primary_expr(&nodes[0])?;
        let mut current_tail = &nodes[1];
        loop {
            let (_, tail_nodes) = self.expect_nonterm(current_tail, sql_y::R_EQEXPRTAIL)?;
            if tail_nodes.is_empty() {
                break;
            }
            if tail_nodes.len() == 3 {
                let right = self.primary_expr(&tail_nodes[1])?;
                left = Expr::BinaryOp {
                    left: Box::new(left),
                    op: BinaryOperator::Eq,
                    right: Box::new(right),
                };
                current_tail = &tail_nodes[2];
            } else {
                return Err(self.err("unexpected eq expr tail"));
            }
        }
        Ok(left)
    }

    fn primary_expr(&self, node: &Node<Lexeme, u32>) -> ChrysoResult<Expr> {
        let (ridx, nodes) = self.expect_nonterm(node, sql_y::R_PRIMARYEXPR)?;
        if nodes.len() == 1 {
            if let Node::Nonterm { ridx, .. } = &nodes[0] {
                if ridx.as_storaget() == sql_y::R_FUNCTIONCALL {
                    return self.function_call(&nodes[0]);
                }
            }
            return self.expr(&nodes[0]);
        }
        if nodes.len() == 3
            && self.is_term_kind(&nodes[0], "LPAREN")
            && self.is_term_kind(&nodes[2], "RPAREN")
        {
            return self.expr(&nodes[1]);
        }
        if nodes.len() == 3
            && self.is_term_kind(&nodes[0], "IDENT")
            && self.is_term_kind(&nodes[1], "DOT")
            && (self.is_term_kind(&nodes[2], "IDENT") || self.is_term_kind(&nodes[2], "STAR"))
        {
            let left = self.ident_from_term(&nodes[0])?;
            let right = self.term_text(&nodes[2])?;
            return Ok(Expr::Identifier(format!("{left}.{right}")));
        }
        Err(self.err_with_rule(ridx))
    }

    fn function_call(&self, node: &Node<Lexeme, u32>) -> ChrysoResult<Expr> {
        let (ridx, nodes) = self.expect_nonterm(node, sql_y::R_FUNCTIONCALL)?;
        if nodes.len() != 4 {
            return Err(self.err_with_rule(ridx));
        }
        let name = self.ident_from_term(&nodes[0])?;
        let args = self.function_args_opt(&nodes[2])?;
        Ok(Expr::FunctionCall { name, args })
    }

    fn function_args_opt(&self, node: &Node<Lexeme, u32>) -> ChrysoResult<Vec<Expr>> {
        let (_, nodes) = self.expect_nonterm(node, sql_y::R_FUNCTIONARGSOPT)?;
        if nodes.is_empty() {
            return Ok(Vec::new());
        }
        if nodes.len() == 1 {
            return self.function_args(&nodes[0]);
        }
        Err(self.err("unexpected function args opt"))
    }

    fn function_args(&self, node: &Node<Lexeme, u32>) -> ChrysoResult<Vec<Expr>> {
        let (ridx, nodes) = self.expect_nonterm(node, sql_y::R_FUNCTIONARGS)?;
        if nodes.len() != 1 {
            return Err(self.err_with_rule(ridx));
        }
        if self.is_term_kind(&nodes[0], "STAR") {
            return Ok(vec![Expr::Wildcard]);
        }
        self.expr_list(&nodes[0])
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
        if nodes.len() != 2 {
            return Err(self.err_with_rule(ridx));
        }
        let mut items = vec![self.column_def(&nodes[0])?];
        self.column_def_list_tail(&nodes[1], &mut items)?;
        Ok(items)
    }

    fn column_def_list_tail(
        &self,
        node: &Node<Lexeme, u32>,
        items: &mut Vec<ColumnDef>,
    ) -> ChrysoResult<()> {
        let (_, nodes) = self.expect_nonterm(node, sql_y::R_COLUMNDEFLISTTAIL)?;
        if nodes.is_empty() {
            return Ok(());
        }
        if nodes.len() == 3 {
            items.push(self.column_def(&nodes[1])?);
            return self.column_def_list_tail(&nodes[2], items);
        }
        Err(self.err("unexpected column def list tail"))
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
    ) -> ChrysoResult<chryso_core::ast::SetOperator> {
        let keyword = self.term_text(node)?;
        let normalized = keyword
            .split_whitespace()
            .collect::<Vec<_>>()
            .join(" ")
            .to_uppercase();
        let op = match normalized.as_str() {
            "UNION" => chryso_core::ast::SetOperator::Union,
            "UNION ALL" => chryso_core::ast::SetOperator::UnionAll,
            "INTERSECT" => chryso_core::ast::SetOperator::Intersect,
            "INTERSECT ALL" => chryso_core::ast::SetOperator::IntersectAll,
            "EXCEPT" => chryso_core::ast::SetOperator::Except,
            "EXCEPT ALL" => chryso_core::ast::SetOperator::ExceptAll,
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

    fn number_from_term(&self, node: &Node<Lexeme, u32>) -> ChrysoResult<u64> {
        let text = self.term_text(node)?;
        text.parse::<u64>()
            .map_err(|_| self.err("invalid numeric literal"))
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
    use lrpar::Lexer;

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
    fn yacc_parser_parses_select_star_with_exprs() {
        let parser = YaccParser::new(ParserConfig {
            dialect: Dialect::Postgres,
        });
        let stmt = parser.parse("select *, id from users").expect("parse");
        let Statement::Select(select) = stmt else {
            panic!("expected select");
        };
        assert_eq!(select.projection.len(), 2);
    }

    #[test]
    fn yacc_parser_parses_setop_with_order_limit() {
        let parser = YaccParser::new(ParserConfig {
            dialect: Dialect::Postgres,
        });
        let stmt = parser
            .parse("select id from t1 union all select id from t2 order by id limit 1")
            .expect("parse");
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
    fn yacc_parser_parses_function_call_count_star() {
        let parser = YaccParser::new(ParserConfig {
            dialect: Dialect::Postgres,
        });
        let stmt = parser.parse("select count(*) from t1").expect("parse");
        let Statement::Select(select) = stmt else {
            panic!("expected select");
        };
        let Expr::FunctionCall { name, args } = &select.projection[0].expr else {
            panic!("expected function call");
        };
        assert_eq!(name, "count");
        assert!(matches!(args.get(0), Some(Expr::Wildcard)));
    }

    #[test]
    fn yacc_lexer_accepts_star_with_comma() {
        let lexerdef = sql_l::lexerdef();
        let lexer = lexerdef.lexer("select *, id from users");
        let mut errors = Vec::new();
        for item in lexer.iter() {
            if let Err(err) = item {
                errors.push(err);
            }
        }
        assert!(errors.is_empty(), "lexer errors: {:?}", errors);
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

    #[test]
    fn yacc_parser_parses_derived_table() {
        let parser = YaccParser::new(ParserConfig {
            dialect: Dialect::Postgres,
        });
        let stmt = parser.parse("select * from (select 1) t").expect("parse");
        let Statement::Select(select) = stmt else {
            panic!("expected select");
        };
        let table = select.from.expect("from");
        match table.factor {
            TableFactor::Derived { .. } => {}
            _ => panic!("expected derived table"),
        }
        assert_eq!(table.alias.as_deref(), Some("t"));
    }

    #[test]
    fn yacc_parser_parses_order_by_limit_offset() {
        let parser = YaccParser::new(ParserConfig {
            dialect: Dialect::Postgres,
        });
        let stmt = parser
            .parse("select id from users order by id desc nulls last limit 2 offset 1")
            .expect("parse");
        let Statement::Select(select) = stmt else {
            panic!("expected select");
        };
        assert_eq!(select.order_by.len(), 1);
        assert!(!select.order_by[0].asc);
        assert_eq!(select.order_by[0].nulls_first, Some(false));
        assert_eq!(select.limit, Some(2));
        assert_eq!(select.offset, Some(1));
    }

    #[test]
    fn yacc_parser_parses_group_by() {
        let parser = YaccParser::new(ParserConfig {
            dialect: Dialect::Postgres,
        });
        let stmt = parser
            .parse("select id from sales group by id")
            .expect("parse");
        let Statement::Select(select) = stmt else {
            panic!("expected select");
        };
        assert_eq!(select.group_by.len(), 1);
    }

    #[test]
    fn yacc_parser_parses_adapter_style_sql() {
        let parser = YaccParser::new(ParserConfig {
            dialect: Dialect::Postgres,
        });
        let sql = "select id from (select * from sales where region = 'us') as t";
        let stmt = parser.parse(sql).expect("parse");
        let Statement::Select(select) = stmt else {
            panic!("expected select");
        };
        assert!(select.from.is_some());
    }
}
