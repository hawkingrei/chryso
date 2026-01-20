use corundum_core::ast::{BinaryOperator, Expr, Literal, OrderByExpr, UnaryOperator};
use corundum_planner::LogicalPlan;

pub fn rewrite_plan(plan: &LogicalPlan) -> LogicalPlan {
    match plan {
        LogicalPlan::Scan { table } => LogicalPlan::Scan { table: table.clone() },
        LogicalPlan::IndexScan {
            table,
            index,
            predicate,
        } => LogicalPlan::IndexScan {
            table: table.clone(),
            index: index.clone(),
            predicate: rewrite_expr(predicate),
        },
        LogicalPlan::Dml { sql } => LogicalPlan::Dml { sql: sql.clone() },
        LogicalPlan::Filter { predicate, input } => LogicalPlan::Filter {
            predicate: rewrite_expr(predicate),
            input: Box::new(rewrite_plan(input.as_ref())),
        },
        LogicalPlan::Projection { exprs, input } => LogicalPlan::Projection {
            exprs: exprs.iter().map(rewrite_expr).collect(),
            input: Box::new(rewrite_plan(input.as_ref())),
        },
        LogicalPlan::Join {
            join_type,
            left,
            right,
            on,
        } => LogicalPlan::Join {
            join_type: *join_type,
            left: Box::new(rewrite_plan(left.as_ref())),
            right: Box::new(rewrite_plan(right.as_ref())),
            on: rewrite_expr(on),
        },
        LogicalPlan::Aggregate {
            group_exprs,
            aggr_exprs,
            input,
        } => LogicalPlan::Aggregate {
            group_exprs: group_exprs.iter().map(rewrite_expr).collect(),
            aggr_exprs: aggr_exprs.iter().map(rewrite_expr).collect(),
            input: Box::new(rewrite_plan(input.as_ref())),
        },
        LogicalPlan::Distinct { input } => LogicalPlan::Distinct {
            input: Box::new(rewrite_plan(input.as_ref())),
        },
        LogicalPlan::TopN {
            order_by,
            limit,
            input,
        } => LogicalPlan::TopN {
            order_by: rewrite_order_by(order_by),
            limit: *limit,
            input: Box::new(rewrite_plan(input.as_ref())),
        },
        LogicalPlan::Sort { order_by, input } => LogicalPlan::Sort {
            order_by: rewrite_order_by(order_by),
            input: Box::new(rewrite_plan(input.as_ref())),
        },
        LogicalPlan::Limit {
            limit,
            offset,
            input,
        } => LogicalPlan::Limit {
            limit: *limit,
            offset: *offset,
            input: Box::new(rewrite_plan(input.as_ref())),
        },
    }
}

pub fn rewrite_expr(expr: &Expr) -> Expr {
    match expr {
        Expr::Identifier(name) => Expr::Identifier(name.clone()),
        Expr::Literal(Literal::String(value)) => Expr::Literal(Literal::String(value.clone())),
        Expr::Literal(Literal::Number(value)) => Expr::Literal(Literal::Number(*value)),
        Expr::Literal(Literal::Bool(value)) => Expr::Literal(Literal::Bool(*value)),
        Expr::UnaryOp { op, expr } => {
            let inner = rewrite_expr(expr);
            match (op, &inner) {
                (UnaryOperator::Neg, Expr::Literal(Literal::Number(value))) => {
                    Expr::Literal(Literal::Number(-value))
                }
                (UnaryOperator::Not, _) => Expr::UnaryOp {
                    op: *op,
                    expr: Box::new(inner),
                },
                _ => Expr::UnaryOp {
                    op: *op,
                    expr: Box::new(inner),
                },
            }
        }
        Expr::BinaryOp { left, op, right } => {
            let left = rewrite_expr(left);
            let right = rewrite_expr(right);
            rewrite_binary(left, *op, right)
        }
        Expr::FunctionCall { name, args } => Expr::FunctionCall {
            name: name.clone(),
            args: args.iter().map(rewrite_expr).collect(),
        },
        Expr::WindowFunction { function, spec } => Expr::WindowFunction {
            function: Box::new(rewrite_expr(function)),
            spec: corundum_core::ast::WindowSpec {
                partition_by: spec.partition_by.iter().map(rewrite_expr).collect(),
                order_by: rewrite_order_by(&spec.order_by),
            },
        },
        Expr::Subquery(select) => Expr::Subquery(select.clone()),
        Expr::Exists(select) => Expr::Exists(select.clone()),
        Expr::InSubquery { expr, subquery } => Expr::InSubquery {
            expr: Box::new(rewrite_expr(expr)),
            subquery: subquery.clone(),
        },
        Expr::Wildcard => Expr::Wildcard,
    }
}

fn rewrite_binary(left: Expr, op: BinaryOperator, right: Expr) -> Expr {
    match (op, &left, &right) {
        (BinaryOperator::Add, Expr::Literal(Literal::Number(0.0)), _) => right,
        (BinaryOperator::Add, _, Expr::Literal(Literal::Number(0.0))) => left,
        (BinaryOperator::Sub, _, Expr::Literal(Literal::Number(0.0))) => left,
        (BinaryOperator::Mul, Expr::Literal(Literal::Number(1.0)), _) => right,
        (BinaryOperator::Mul, _, Expr::Literal(Literal::Number(1.0))) => left,
        (BinaryOperator::Mul, Expr::Literal(Literal::Number(0.0)), _) => {
            Expr::Literal(Literal::Number(0.0))
        }
        (BinaryOperator::Mul, _, Expr::Literal(Literal::Number(0.0))) => {
            Expr::Literal(Literal::Number(0.0))
        }
        (BinaryOperator::Div, _, Expr::Literal(Literal::Number(1.0))) => left,
        (BinaryOperator::Add, Expr::Literal(Literal::Number(a)), Expr::Literal(Literal::Number(b))) => {
            Expr::Literal(Literal::Number(a + b))
        }
        (BinaryOperator::Sub, Expr::Literal(Literal::Number(a)), Expr::Literal(Literal::Number(b))) => {
            Expr::Literal(Literal::Number(a - b))
        }
        (BinaryOperator::Mul, Expr::Literal(Literal::Number(a)), Expr::Literal(Literal::Number(b))) => {
            Expr::Literal(Literal::Number(a * b))
        }
        (BinaryOperator::Div, Expr::Literal(Literal::Number(a)), Expr::Literal(Literal::Number(b))) => {
            if *b == 0.0 {
                Expr::BinaryOp {
                    left: Box::new(left),
                    op,
                    right: Box::new(right),
                }
            } else {
                Expr::Literal(Literal::Number(a / b))
            }
        }
        _ => Expr::BinaryOp {
            left: Box::new(left),
            op,
            right: Box::new(right),
        },
    }
}

fn rewrite_order_by(order_by: &[OrderByExpr]) -> Vec<OrderByExpr> {
    order_by
        .iter()
        .map(|item| OrderByExpr {
            expr: rewrite_expr(&item.expr),
            asc: item.asc,
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::{rewrite_expr, rewrite_plan};
    use corundum_core::ast::{BinaryOperator, Expr, Literal};
    use corundum_planner::LogicalPlan;

    #[test]
    fn folds_numeric_arithmetic() {
        let expr = Expr::BinaryOp {
            left: Box::new(Expr::Literal(Literal::Number(1.0))),
            op: BinaryOperator::Add,
            right: Box::new(Expr::BinaryOp {
                left: Box::new(Expr::Literal(Literal::Number(2.0))),
                op: BinaryOperator::Mul,
                right: Box::new(Expr::Literal(Literal::Number(3.0))),
            }),
        };
        let rewritten = rewrite_expr(&expr);
        match rewritten {
            Expr::Literal(Literal::Number(value)) => assert_eq!(value, 7.0),
            other => panic!("expected folded literal, got {other:?}"),
        }
    }

    #[test]
    fn rewrites_filter_predicate() {
        let plan = LogicalPlan::Filter {
            predicate: Expr::BinaryOp {
                left: Box::new(Expr::Literal(Literal::Number(10.0))),
                op: BinaryOperator::Sub,
                right: Box::new(Expr::Literal(Literal::Number(3.0))),
            },
            input: Box::new(LogicalPlan::Scan {
                table: "t".to_string(),
            }),
        };
        let rewritten = rewrite_plan(&plan);
        match rewritten {
            LogicalPlan::Filter { predicate, .. } => match predicate {
                Expr::Literal(Literal::Number(value)) => assert_eq!(value, 7.0),
                other => panic!("expected folded literal, got {other:?}"),
            },
            other => panic!("unexpected plan: {other:?}"),
        }
    }
}
