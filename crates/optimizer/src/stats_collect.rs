use chryso_core::ast::{Expr, OrderByExpr};
use chryso_planner::LogicalPlan;
use std::collections::HashSet;

#[derive(Default)]
pub struct StatsRequirements {
    pub tables: HashSet<String>,
    pub columns: HashSet<(String, String)>,
}

pub fn collect_requirements(plan: &LogicalPlan) -> StatsRequirements {
    let mut requirements = StatsRequirements::default();
    collect_tables(plan, &mut requirements.tables);
    collect_columns(plan, &requirements.tables, &mut requirements.columns);
    requirements
}

fn collect_tables(plan: &LogicalPlan, tables: &mut HashSet<String>) {
    match plan {
        LogicalPlan::Scan { table } | LogicalPlan::IndexScan { table, .. } => {
            tables.insert(table.clone());
        }
        LogicalPlan::Dml { .. } => {}
        LogicalPlan::Derived { input, .. } => collect_tables(input, tables),
        LogicalPlan::Filter { input, .. }
        | LogicalPlan::Projection { input, .. }
        | LogicalPlan::Aggregate { input, .. }
        | LogicalPlan::Distinct { input }
        | LogicalPlan::TopN { input, .. }
        | LogicalPlan::Sort { input, .. }
        | LogicalPlan::Limit { input, .. } => collect_tables(input, tables),
        LogicalPlan::Join { left, right, .. } => {
            collect_tables(left, tables);
            collect_tables(right, tables);
        }
    }
}

fn collect_columns(plan: &LogicalPlan, tables: &HashSet<String>, columns: &mut HashSet<(String, String)>) {
    match plan {
        LogicalPlan::Scan { .. } | LogicalPlan::IndexScan { .. } | LogicalPlan::Dml { .. } => {}
        LogicalPlan::Derived { input, .. } => collect_columns(input, tables, columns),
        LogicalPlan::Filter { predicate, input } => {
            collect_expr_columns(predicate, tables, columns);
            collect_columns(input, tables, columns);
        }
        LogicalPlan::Projection { exprs, input } => {
            collect_exprs_columns(exprs, tables, columns);
            collect_columns(input, tables, columns);
        }
        LogicalPlan::Join { on, left, right, .. } => {
            collect_expr_columns(on, tables, columns);
            collect_columns(left, tables, columns);
            collect_columns(right, tables, columns);
        }
        LogicalPlan::Aggregate {
            group_exprs,
            aggr_exprs,
            input,
        } => {
            collect_exprs_columns(group_exprs, tables, columns);
            collect_exprs_columns(aggr_exprs, tables, columns);
            collect_columns(input, tables, columns);
        }
        LogicalPlan::Distinct { input } => collect_columns(input, tables, columns),
        LogicalPlan::TopN { order_by, input, .. } => {
            collect_order_by_columns(order_by, tables, columns);
            collect_columns(input, tables, columns);
        }
        LogicalPlan::Sort { order_by, input } => {
            collect_order_by_columns(order_by, tables, columns);
            collect_columns(input, tables, columns);
        }
        LogicalPlan::Limit { input, .. } => collect_columns(input, tables, columns),
    }
}

fn collect_exprs_columns(exprs: &[Expr], tables: &HashSet<String>, columns: &mut HashSet<(String, String)>) {
    for expr in exprs {
        collect_expr_columns(expr, tables, columns);
    }
}

fn collect_order_by_columns(order_by: &[OrderByExpr], tables: &HashSet<String>, columns: &mut HashSet<(String, String)>) {
    for item in order_by {
        collect_expr_columns(&item.expr, tables, columns);
    }
}

fn collect_expr_columns(expr: &Expr, tables: &HashSet<String>, columns: &mut HashSet<(String, String)>) {
    match expr {
        Expr::Identifier(name) => {
            if let Some((table, column)) = name.split_once('.') {
                if tables.contains(table) {
                    columns.insert((table.to_string(), column.to_string()));
                }
            } else if tables.len() == 1 {
                if let Some(table) = tables.iter().next() {
                    columns.insert((table.clone(), name.to_string()));
                }
            }
        }
        Expr::BinaryOp { left, right, .. } => {
            collect_expr_columns(left, tables, columns);
            collect_expr_columns(right, tables, columns);
        }
        Expr::IsNull { expr, .. } => {
            collect_expr_columns(expr, tables, columns);
        }
        Expr::UnaryOp { expr, .. } => collect_expr_columns(expr, tables, columns),
        Expr::FunctionCall { args, .. } => {
            for arg in args {
                collect_expr_columns(arg, tables, columns);
            }
        }
        Expr::WindowFunction { function, spec } => {
            collect_expr_columns(function, tables, columns);
            collect_exprs_columns(&spec.partition_by, tables, columns);
            collect_order_by_columns(&spec.order_by, tables, columns);
        }
        Expr::Subquery(select) | Expr::Exists(select) => {
            for item in &select.projection {
                collect_expr_columns(&item.expr, tables, columns);
            }
        }
        Expr::InSubquery { expr, subquery } => {
            collect_expr_columns(expr, tables, columns);
            for item in &subquery.projection {
                collect_expr_columns(&item.expr, tables, columns);
            }
        }
        Expr::Case {
            operand,
            when_then,
            else_expr,
        } => {
            if let Some(expr) = operand {
                collect_expr_columns(expr, tables, columns);
            }
            for (when_expr, then_expr) in when_then {
                collect_expr_columns(when_expr, tables, columns);
                collect_expr_columns(then_expr, tables, columns);
            }
            if let Some(expr) = else_expr {
                collect_expr_columns(expr, tables, columns);
            }
        }
        Expr::Literal(_) | Expr::Wildcard => {}
    }
}
