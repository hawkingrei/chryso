use crate::{LogicalPlan, PhysicalPlan};
use chryso_core::ast::{BinaryOperator, Expr, Literal, Statement, TableFactor, TableRef};

pub fn serialize_statement(statement: &Statement) -> String {
    match statement {
        Statement::Select(select) => format!(
            "select|distinct={}|from={}",
            select.distinct,
            select
                .from
                .as_ref()
                .map(table_ref_label)
                .unwrap_or_else(|| "none".to_string())
        ),
        Statement::With(stmt) => format!("with|ctes={}", stmt.ctes.len()),
        Statement::SetOp { op, .. } => format!("setop|op={op:?}"),
        Statement::Explain(_) => "explain".to_string(),
        Statement::CreateTable(stmt) => format!("create_table|name={}", stmt.name),
        Statement::DropTable(stmt) => format!("drop_table|name={}", stmt.name),
        Statement::Truncate(stmt) => format!("truncate|table={}", stmt.table),
        Statement::Analyze(stmt) => format!("analyze|table={}", stmt.table),
        Statement::Insert(stmt) => format!("insert|table={}", stmt.table),
        Statement::Update(stmt) => format!("update|table={}", stmt.table),
        Statement::Delete(stmt) => format!("delete|table={}", stmt.table),
    }
}

pub fn serialize_logical_plan(plan: &LogicalPlan) -> String {
    plan.explain(0)
}

pub fn serialize_physical_plan(plan: &PhysicalPlan) -> String {
    plan.explain(0)
}

pub fn serialize_expr(expr: &Expr) -> String {
    match expr {
        Expr::Identifier(name) => format!("ident:{name}"),
        Expr::Literal(Literal::String(value)) => format!("string:{value}"),
        Expr::Literal(Literal::Number(value)) => format!("number:{value}"),
        Expr::Literal(Literal::Bool(value)) => format!("bool:{value}"),
        Expr::IsNull { expr, negated } => {
            format!("isnull:{}({})", negated, serialize_expr(expr))
        }
        Expr::IsDistinctFrom {
            left,
            right,
            negated,
        } => format!(
            "isdistinct:{}({},{})",
            negated,
            serialize_expr(left),
            serialize_expr(right)
        ),
        Expr::BinaryOp { left, op, right } => format!(
            "binop:{:?}({},{})",
            op,
            serialize_expr(left),
            serialize_expr(right)
        ),
        Expr::UnaryOp { op, expr } => format!("unary:{:?}({})", op, serialize_expr(expr)),
        Expr::FunctionCall { name, args } => {
            let args = args
                .iter()
                .map(serialize_expr)
                .collect::<Vec<_>>()
                .join(",");
            format!("call:{name}({args})")
        }
        Expr::WindowFunction { function, spec } => format!(
            "window:{}:partition={}:order={}",
            serialize_expr(function),
            spec.partition_by
                .iter()
                .map(serialize_expr)
                .collect::<Vec<_>>()
                .join(","),
            spec.order_by
                .iter()
                .map(|item| item.expr.to_sql())
                .collect::<Vec<_>>()
                .join(",")
        ),
        Expr::Exists(select) => format!("exists:{}", select_to_marker(select)),
        Expr::InSubquery { expr, subquery } => {
            format!("in:{}:{}", serialize_expr(expr), select_to_marker(subquery))
        }
        Expr::Subquery(select) => format!("subquery:{}", select_to_marker(select)),
        Expr::Case {
            operand,
            when_then,
            else_expr,
        } => {
            let mut output = String::from("case");
            if let Some(expr) = operand {
                output.push(':');
                output.push_str(&serialize_expr(expr));
            }
            for (when_expr, then_expr) in when_then {
                output.push_str(":when=");
                output.push_str(&serialize_expr(when_expr));
                output.push_str(":then=");
                output.push_str(&serialize_expr(then_expr));
            }
            if let Some(expr) = else_expr {
                output.push_str(":else=");
                output.push_str(&serialize_expr(expr));
            }
            output
        }
        Expr::Wildcard => "wildcard".to_string(),
    }
}

fn select_to_marker(select: &chryso_core::ast::SelectStatement) -> String {
    let from = select
        .from
        .as_ref()
        .map(table_ref_label)
        .unwrap_or_else(|| "none".to_string());
    format!("select:{from}")
}

fn table_ref_label(table: &TableRef) -> String {
    match &table.factor {
        TableFactor::Table { name } => name.clone(),
        TableFactor::Derived { .. } => "subquery".to_string(),
        TableFactor::Values { .. } => "values".to_string(),
    }
}

pub fn deserialize_expr(input: &str) -> Option<Expr> {
    let mut parts = input.splitn(2, ':');
    let kind = parts.next()?;
    let payload = parts.next().unwrap_or("");
    match kind {
        "ident" => Some(Expr::Identifier(payload.to_string())),
        "string" => Some(Expr::Literal(Literal::String(payload.to_string()))),
        "number" => payload
            .parse()
            .ok()
            .map(|value| Expr::Literal(Literal::Number(value))),
        "wildcard" => Some(Expr::Wildcard),
        _ => None,
    }
}

pub fn deserialize_binary_operator(input: &str) -> Option<BinaryOperator> {
    match input {
        "Eq" => Some(BinaryOperator::Eq),
        "NotEq" => Some(BinaryOperator::NotEq),
        "Lt" => Some(BinaryOperator::Lt),
        "LtEq" => Some(BinaryOperator::LtEq),
        "Gt" => Some(BinaryOperator::Gt),
        "GtEq" => Some(BinaryOperator::GtEq),
        "And" => Some(BinaryOperator::And),
        "Or" => Some(BinaryOperator::Or),
        "Add" => Some(BinaryOperator::Add),
        "Sub" => Some(BinaryOperator::Sub),
        "Mul" => Some(BinaryOperator::Mul),
        "Div" => Some(BinaryOperator::Div),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::{deserialize_expr, serialize_expr, serialize_statement};
    use chryso_core::ast::{Expr, Statement};

    #[test]
    fn serialize_expr_roundtrip() {
        let expr = Expr::Identifier("id".to_string());
        let encoded = serialize_expr(&expr);
        let decoded = deserialize_expr(&encoded).expect("decode");
        assert_eq!(encoded, serialize_expr(&decoded));
    }

    #[test]
    fn serialize_statement_marker() {
        let stmt = Statement::Explain(Box::new(Statement::Analyze(
            chryso_core::ast::AnalyzeStatement {
                table: "t".to_string(),
            },
        )));
        let encoded = serialize_statement(&stmt);
        assert!(encoded.contains("explain"));
    }
}
