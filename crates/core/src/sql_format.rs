use crate::ast::{Expr, Statement};

pub fn format_statement(statement: &Statement) -> String {
    crate::ast::statement_to_sql(statement)
}

pub fn format_expr(expr: &Expr) -> String {
    expr.to_sql()
}

#[cfg(test)]
mod tests {
    use super::format_statement;
    use crate::ast::{InsertSource, SelectItem, SelectStatement, Statement, TableFactor, TableRef};

    #[test]
    fn format_simple_select() {
        let stmt = Statement::Select(SelectStatement {
            distinct: false,
            distinct_on: Vec::new(),
            projection: vec![SelectItem {
                expr: crate::ast::Expr::Identifier("id".to_string()),
                alias: None,
            }],
            from: Some(TableRef {
                factor: TableFactor::Table {
                    name: "users".to_string(),
                },
                alias: None,
                column_aliases: Vec::new(),
                joins: Vec::new(),
            }),
            selection: None,
            group_by: Vec::new(),
            having: None,
            order_by: Vec::new(),
            limit: None,
            offset: None,
        });
        let output = format_statement(&stmt);
        assert_eq!(output, "select id from users");
    }

    #[test]
    fn format_update_statement() {
        let stmt = Statement::Update(crate::ast::UpdateStatement {
            table: "users".to_string(),
            assignments: vec![crate::ast::Assignment {
                column: "name".to_string(),
                value: crate::ast::Expr::Literal(crate::ast::Literal::String("bob".to_string())),
            }],
            selection: Some(crate::ast::Expr::BinaryOp {
                left: Box::new(crate::ast::Expr::Identifier("id".to_string())),
                op: crate::ast::BinaryOperator::Eq,
                right: Box::new(crate::ast::Expr::Literal(crate::ast::Literal::Number(1.0))),
            }),
            returning: Vec::new(),
        });
        let output = format_statement(&stmt);
        assert_eq!(output, "update users set name = 'bob' where id = 1");
    }

    #[test]
    fn format_insert_multi_values() {
        let stmt = Statement::Insert(crate::ast::InsertStatement {
            table: "users".to_string(),
            columns: vec!["id".to_string(), "name".to_string()],
            source: InsertSource::Values(vec![
                vec![
                    crate::ast::Expr::Literal(crate::ast::Literal::Number(1.0)),
                    crate::ast::Expr::Literal(crate::ast::Literal::String("alice".to_string())),
                ],
                vec![
                    crate::ast::Expr::Literal(crate::ast::Literal::Number(2.0)),
                    crate::ast::Expr::Literal(crate::ast::Literal::String("bob".to_string())),
                ],
            ]),
            returning: Vec::new(),
        });
        let output = format_statement(&stmt);
        assert_eq!(
            output,
            "insert into users (id, name) values (1, 'alice'), (2, 'bob')"
        );
    }

    #[test]
    fn format_insert_default_values() {
        let stmt = Statement::Insert(crate::ast::InsertStatement {
            table: "users".to_string(),
            columns: Vec::new(),
            source: InsertSource::DefaultValues,
            returning: Vec::new(),
        });
        let output = format_statement(&stmt);
        assert_eq!(output, "insert into users default values");
    }

    #[test]
    fn format_insert_select() {
        let select = Statement::Select(SelectStatement {
            distinct: false,
            distinct_on: Vec::new(),
            projection: vec![SelectItem {
                expr: crate::ast::Expr::Identifier("id".to_string()),
                alias: None,
            }],
            from: Some(TableRef {
                factor: TableFactor::Table {
                    name: "staging".to_string(),
                },
                alias: None,
                column_aliases: Vec::new(),
                joins: Vec::new(),
            }),
            selection: None,
            group_by: Vec::new(),
            having: None,
            order_by: Vec::new(),
            limit: None,
            offset: None,
        });
        let stmt = Statement::Insert(crate::ast::InsertStatement {
            table: "users".to_string(),
            columns: vec!["id".to_string()],
            source: InsertSource::Query(Box::new(select)),
            returning: Vec::new(),
        });
        let output = format_statement(&stmt);
        assert_eq!(output, "insert into users (id) select id from staging");
    }

    #[test]
    fn format_select_without_from() {
        let stmt = Statement::Select(SelectStatement {
            distinct: false,
            distinct_on: Vec::new(),
            projection: vec![SelectItem {
                expr: crate::ast::Expr::Literal(crate::ast::Literal::Number(1.0)),
                alias: None,
            }],
            from: None,
            selection: None,
            group_by: Vec::new(),
            having: None,
            order_by: Vec::new(),
            limit: None,
            offset: None,
        });
        let output = format_statement(&stmt);
        assert_eq!(output, "select 1");
    }

    #[test]
    fn format_select_without_from_with_order_limit() {
        let stmt = Statement::Select(SelectStatement {
            distinct: false,
            distinct_on: Vec::new(),
            projection: vec![SelectItem {
                expr: crate::ast::Expr::Literal(crate::ast::Literal::Number(1.0)),
                alias: None,
            }],
            from: None,
            selection: None,
            group_by: Vec::new(),
            having: None,
            order_by: vec![crate::ast::OrderByExpr {
                expr: crate::ast::Expr::Literal(crate::ast::Literal::Number(1.0)),
                asc: true,
                nulls_first: None,
            }],
            limit: Some(2),
            offset: Some(1),
        });
        let output = format_statement(&stmt);
        assert_eq!(output, "select 1 order by 1 asc limit 2 offset 1");
    }

    #[test]
    fn format_create_table_if_not_exists() {
        let stmt = Statement::CreateTable(crate::ast::CreateTableStatement {
            name: "users".to_string(),
            if_not_exists: true,
            columns: Vec::new(),
        });
        let output = format_statement(&stmt);
        assert_eq!(output, "create table if not exists users");
    }

    #[test]
    fn format_create_table_with_columns() {
        let stmt = Statement::CreateTable(crate::ast::CreateTableStatement {
            name: "users".to_string(),
            if_not_exists: false,
            columns: vec![
                crate::ast::ColumnDef {
                    name: "id".to_string(),
                    data_type: "integer".to_string(),
                },
                crate::ast::ColumnDef {
                    name: "name".to_string(),
                    data_type: "varchar(20)".to_string(),
                },
            ],
        });
        let output = format_statement(&stmt);
        assert_eq!(output, "create table users (id integer, name varchar(20))");
    }
}
