use corundum_core::ast::{BinaryOperator, Expr};
use corundum_planner::LogicalPlan;
use std::collections::HashSet;

pub fn collect_identifiers(expr: &Expr) -> HashSet<String> {
    let mut idents = HashSet::new();
    collect_identifiers_inner(expr, &mut idents);
    idents
}

pub fn collect_tables(plan: &LogicalPlan) -> HashSet<String> {
    let mut tables = HashSet::new();
    collect_tables_inner(plan, &mut tables);
    tables
}

pub fn split_conjuncts(expr: &Expr) -> Vec<Expr> {
    match expr {
        Expr::BinaryOp { left, op, right } if matches!(op, BinaryOperator::And) => {
            let mut out = split_conjuncts(left);
            out.extend(split_conjuncts(right));
            out
        }
        _ => vec![expr.clone()],
    }
}

pub fn combine_conjuncts(exprs: Vec<Expr>) -> Option<Expr> {
    let mut iter = exprs.into_iter();
    let first = iter.next()?;
    Some(iter.fold(first, |left, right| Expr::BinaryOp {
        left: Box::new(left),
        op: BinaryOperator::And,
        right: Box::new(right),
    }))
}

pub fn table_prefix(ident: &str) -> Option<&str> {
    ident.split_once('.').map(|(prefix, _)| prefix)
}

fn collect_identifiers_inner(expr: &Expr, idents: &mut HashSet<String>) {
    match expr {
        Expr::Identifier(name) => {
            idents.insert(name.clone());
        }
        Expr::BinaryOp { left, right, .. } => {
            collect_identifiers_inner(left, idents);
            collect_identifiers_inner(right, idents);
        }
        Expr::IsNull { expr, .. } => {
            collect_identifiers_inner(expr, idents);
        }
        Expr::UnaryOp { expr, .. } => {
            collect_identifiers_inner(expr, idents);
        }
        Expr::FunctionCall { args, .. } => {
            for arg in args {
                collect_identifiers_inner(arg, idents);
            }
        }
        Expr::WindowFunction { function, spec } => {
            collect_identifiers_inner(function, idents);
            for expr in &spec.partition_by {
                collect_identifiers_inner(expr, idents);
            }
            for expr in &spec.order_by {
                collect_identifiers_inner(&expr.expr, idents);
            }
        }
        Expr::Subquery(select) | Expr::Exists(select) => {
            for item in &select.projection {
                collect_identifiers_inner(&item.expr, idents);
            }
        }
        Expr::InSubquery { expr, subquery } => {
            collect_identifiers_inner(expr, idents);
            for item in &subquery.projection {
                collect_identifiers_inner(&item.expr, idents);
            }
        }
        Expr::Case {
            operand,
            when_then,
            else_expr,
        } => {
            if let Some(expr) = operand {
                collect_identifiers_inner(expr, idents);
            }
            for (when_expr, then_expr) in when_then {
                collect_identifiers_inner(when_expr, idents);
                collect_identifiers_inner(then_expr, idents);
            }
            if let Some(expr) = else_expr {
                collect_identifiers_inner(expr, idents);
            }
        }
        Expr::Literal(_) | Expr::Wildcard => {}
    }
}

fn collect_tables_inner(plan: &LogicalPlan, tables: &mut HashSet<String>) {
    match plan {
        LogicalPlan::Scan { table } | LogicalPlan::IndexScan { table, .. } => {
            tables.insert(table.clone());
        }
        LogicalPlan::Dml { .. } => {}
        LogicalPlan::Derived { input, .. } => collect_tables_inner(input, tables),
        LogicalPlan::Filter { input, .. }
        | LogicalPlan::Projection { input, .. }
        | LogicalPlan::Aggregate { input, .. }
        | LogicalPlan::Distinct { input }
        | LogicalPlan::TopN { input, .. }
        | LogicalPlan::Sort { input, .. }
        | LogicalPlan::Limit { input, .. } => collect_tables_inner(input, tables),
        LogicalPlan::Join { left, right, .. } => {
            collect_tables_inner(left, tables);
            collect_tables_inner(right, tables);
        }
    }
}
