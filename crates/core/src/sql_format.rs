use crate::ast::{Expr, Statement};

pub fn format_statement(statement: &Statement) -> String {
    match statement {
        Statement::Select(select) => {
            let mut output = String::from("select ");
            if select.distinct {
                output.push_str("distinct ");
            }
            let projection = select
                .projection
                .iter()
                .map(|item| item.expr.to_sql())
                .collect::<Vec<_>>()
                .join(", ");
            output.push_str(&projection);
            output.push_str(" from ");
            output.push_str(&select.from.name);
            output
        }
        Statement::Explain(inner) => format!("explain {}", format_statement(inner)),
        Statement::CreateTable(stmt) => format!("create table {}", stmt.name),
        Statement::Analyze(stmt) => format!("analyze {}", stmt.table),
        Statement::Insert(stmt) => {
            let mut output = format!("insert into {}", stmt.table);
            if !stmt.columns.is_empty() {
                output.push_str(" (");
                output.push_str(&stmt.columns.join(", "));
                output.push(')');
            }
            let rows = stmt
                .values
                .iter()
                .map(|row| {
                    let values = row
                        .iter()
                        .map(|expr| expr.to_sql())
                        .collect::<Vec<_>>()
                        .join(", ");
                    format!("({values})")
                })
                .collect::<Vec<_>>()
                .join(", ");
            output.push_str(" values ");
            output.push_str(&rows);
            output
        }
        Statement::Update(stmt) => {
            let mut output = format!("update {} set ", stmt.table);
            let assignments = stmt
                .assignments
                .iter()
                .map(|assign| format!("{} = {}", assign.column, assign.value.to_sql()))
                .collect::<Vec<_>>()
                .join(", ");
            output.push_str(&assignments);
            if let Some(selection) = &stmt.selection {
                output.push_str(" where ");
                output.push_str(&selection.to_sql());
            }
            output
        }
        Statement::Delete(stmt) => {
            let mut output = format!("delete from {}", stmt.table);
            if let Some(selection) = &stmt.selection {
                output.push_str(" where ");
                output.push_str(&selection.to_sql());
            }
            output
        }
    }
}

pub fn format_expr(expr: &Expr) -> String {
    expr.to_sql()
}

#[cfg(test)]
mod tests {
    use super::format_statement;
    use crate::ast::{SelectItem, SelectStatement, Statement, TableRef};

    #[test]
    fn format_simple_select() {
        let stmt = Statement::Select(SelectStatement {
            distinct: false,
            projection: vec![SelectItem {
                expr: crate::ast::Expr::Identifier("id".to_string()),
                alias: None,
            }],
            from: TableRef {
                name: "users".to_string(),
                alias: None,
                joins: Vec::new(),
            },
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
        });
        let output = format_statement(&stmt);
        assert_eq!(output, "update users set name = 'bob' where id = 1");
    }

    #[test]
    fn format_insert_multi_values() {
        let stmt = Statement::Insert(crate::ast::InsertStatement {
            table: "users".to_string(),
            columns: vec!["id".to_string(), "name".to_string()],
            values: vec![
                vec![
                    crate::ast::Expr::Literal(crate::ast::Literal::Number(1.0)),
                    crate::ast::Expr::Literal(crate::ast::Literal::String("alice".to_string())),
                ],
                vec![
                    crate::ast::Expr::Literal(crate::ast::Literal::Number(2.0)),
                    crate::ast::Expr::Literal(crate::ast::Literal::String("bob".to_string())),
                ],
            ],
        });
        let output = format_statement(&stmt);
        assert_eq!(
            output,
            "insert into users (id, name) values (1, 'alice'), (2, 'bob')"
        );
    }
}
