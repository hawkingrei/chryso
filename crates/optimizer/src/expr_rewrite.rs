use chryso_core::ast::{BinaryOperator, Expr, Literal, OrderByExpr, UnaryOperator};
use chryso_planner::LogicalPlan;

pub fn rewrite_plan(plan: &LogicalPlan) -> LogicalPlan {
    match plan {
        LogicalPlan::Scan { table } => LogicalPlan::Scan {
            table: table.clone(),
        },
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
        LogicalPlan::Derived {
            input,
            alias,
            column_aliases,
        } => LogicalPlan::Derived {
            input: Box::new(rewrite_plan(input.as_ref())),
            alias: alias.clone(),
            column_aliases: column_aliases.clone(),
        },
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
        Expr::Literal(Literal::Null) => Expr::Literal(Literal::Null),
        Expr::UnaryOp { op, expr } => {
            let inner = rewrite_expr(expr);
            match (op, inner) {
                (UnaryOperator::Neg, Expr::Literal(Literal::Number(value))) => {
                    Expr::Literal(Literal::Number(-value))
                }
                (UnaryOperator::Not, Expr::Literal(Literal::Bool(value))) => {
                    Expr::Literal(Literal::Bool(!value))
                }
                (
                    UnaryOperator::Not,
                    Expr::UnaryOp {
                        op: UnaryOperator::Not,
                        expr,
                    },
                ) => *expr,
                (UnaryOperator::Not, Expr::IsNull { expr, negated }) => Expr::IsNull {
                    expr,
                    negated: !negated,
                },
                (
                    UnaryOperator::Not,
                    Expr::IsDistinctFrom {
                        left,
                        right,
                        negated,
                    },
                ) => Expr::IsDistinctFrom {
                    left,
                    right,
                    negated: !negated,
                },
                (UnaryOperator::Not, Expr::BinaryOp { left, op, right }) => match op {
                    BinaryOperator::And => Expr::BinaryOp {
                        left: Box::new(negate_expr(*left)),
                        op: BinaryOperator::Or,
                        right: Box::new(negate_expr(*right)),
                    },
                    BinaryOperator::Or => Expr::BinaryOp {
                        left: Box::new(negate_expr(*left)),
                        op: BinaryOperator::And,
                        right: Box::new(negate_expr(*right)),
                    },
                    _ => Expr::UnaryOp {
                        op: UnaryOperator::Not,
                        expr: Box::new(Expr::BinaryOp { left, op, right }),
                    },
                },
                (op, inner) => Expr::UnaryOp {
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
        Expr::IsNull { expr, negated } => Expr::IsNull {
            expr: Box::new(rewrite_expr(expr)),
            negated: *negated,
        },
        Expr::IsDistinctFrom {
            left,
            right,
            negated,
        } => Expr::IsDistinctFrom {
            left: Box::new(rewrite_expr(left)),
            right: Box::new(rewrite_expr(right)),
            negated: *negated,
        },
        Expr::FunctionCall { name, args } => Expr::FunctionCall {
            name: name.clone(),
            args: args.iter().map(rewrite_expr).collect(),
        },
        Expr::WindowFunction { function, spec } => Expr::WindowFunction {
            function: Box::new(rewrite_expr(function)),
            spec: chryso_core::ast::WindowSpec {
                partition_by: spec.partition_by.iter().map(rewrite_expr).collect(),
                order_by: rewrite_order_by(&spec.order_by),
                frame: spec.frame.clone(),
            },
        },
        Expr::Subquery(select) => Expr::Subquery(select.clone()),
        Expr::Exists(select) => Expr::Exists(select.clone()),
        Expr::InSubquery { expr, subquery } => Expr::InSubquery {
            expr: Box::new(rewrite_expr(expr)),
            subquery: subquery.clone(),
        },
        Expr::Case {
            operand,
            when_then,
            else_expr,
        } => Expr::Case {
            operand: operand.as_ref().map(|expr| Box::new(rewrite_expr(expr))),
            when_then: when_then
                .iter()
                .map(|(when_expr, then_expr)| (rewrite_expr(when_expr), rewrite_expr(then_expr)))
                .collect(),
            else_expr: else_expr.as_ref().map(|expr| Box::new(rewrite_expr(expr))),
        },
        Expr::Wildcard => Expr::Wildcard,
    }
}

fn rewrite_binary(left: Expr, op: BinaryOperator, right: Expr) -> Expr {
    if let Some(expr) = fold_bool_binary(&left, op, &right) {
        return expr;
    }
    if let Some(expr) = fold_comparison(&left, op, &right) {
        return expr;
    }
    if let Some(expr) = fold_null_aware_binary(&left, op, &right) {
        return expr;
    }
    if let Some(expr) = fold_self_comparison(&left, op, &right) {
        return expr;
    }
    if matches!(op, BinaryOperator::And | BinaryOperator::Or) && left.structural_eq(&right) {
        return left;
    }
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
        (
            BinaryOperator::Add,
            Expr::Literal(Literal::Number(a)),
            Expr::Literal(Literal::Number(b)),
        ) => Expr::Literal(Literal::Number(a + b)),
        (
            BinaryOperator::Sub,
            Expr::Literal(Literal::Number(a)),
            Expr::Literal(Literal::Number(b)),
        ) => Expr::Literal(Literal::Number(a - b)),
        (
            BinaryOperator::Mul,
            Expr::Literal(Literal::Number(a)),
            Expr::Literal(Literal::Number(b)),
        ) => Expr::Literal(Literal::Number(a * b)),
        (
            BinaryOperator::Div,
            Expr::Literal(Literal::Number(a)),
            Expr::Literal(Literal::Number(b)),
        ) => {
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

fn negate_expr(expr: Expr) -> Expr {
    rewrite_expr(&Expr::UnaryOp {
        op: UnaryOperator::Not,
        expr: Box::new(expr),
    })
}

fn fold_bool_binary(left: &Expr, op: BinaryOperator, right: &Expr) -> Option<Expr> {
    let left_bool = match left {
        Expr::Literal(Literal::Bool(value)) => Some(*value),
        _ => None,
    };
    let right_bool = match right {
        Expr::Literal(Literal::Bool(value)) => Some(*value),
        _ => None,
    };
    match op {
        BinaryOperator::And => match (left_bool, right_bool) {
            (Some(true), _) => Some(right.clone()),
            (Some(false), _) => Some(Expr::Literal(Literal::Bool(false))),
            (_, Some(true)) => Some(left.clone()),
            (_, Some(false)) => Some(Expr::Literal(Literal::Bool(false))),
            _ => None,
        },
        BinaryOperator::Or => match (left_bool, right_bool) {
            (Some(true), _) => Some(Expr::Literal(Literal::Bool(true))),
            (Some(false), _) => Some(right.clone()),
            (_, Some(true)) => Some(Expr::Literal(Literal::Bool(true))),
            (_, Some(false)) => Some(left.clone()),
            _ => None,
        },
        _ => None,
    }
}

fn fold_comparison(left: &Expr, op: BinaryOperator, right: &Expr) -> Option<Expr> {
    match (left, right) {
        (Expr::Literal(Literal::Number(left)), Expr::Literal(Literal::Number(right))) => {
            let result = match op {
                BinaryOperator::Eq => Some(left == right),
                BinaryOperator::NotEq => Some(left != right),
                BinaryOperator::Lt => Some(left < right),
                BinaryOperator::LtEq => Some(left <= right),
                BinaryOperator::Gt => Some(left > right),
                BinaryOperator::GtEq => Some(left >= right),
                _ => None,
            };
            result.map(|value| Expr::Literal(Literal::Bool(value)))
        }
        (Expr::Literal(Literal::Bool(left)), Expr::Literal(Literal::Bool(right))) => {
            let result = match op {
                BinaryOperator::Eq => Some(left == right),
                BinaryOperator::NotEq => Some(left != right),
                _ => None,
            };
            result.map(|value| Expr::Literal(Literal::Bool(value)))
        }
        _ => None,
    }
}

fn fold_null_aware_binary(left: &Expr, op: BinaryOperator, right: &Expr) -> Option<Expr> {
    match op {
        BinaryOperator::And | BinaryOperator::Or => {
            let (Some((left_expr, left_negated)), Some((right_expr, right_negated))) =
                (as_is_null(left), as_is_null(right))
            else {
                return None;
            };
            if !left_expr.structural_eq(right_expr) {
                return None;
            }
            if left_negated == right_negated {
                return None;
            }
            Some(match op {
                BinaryOperator::And => Expr::Literal(Literal::Bool(false)),
                BinaryOperator::Or => Expr::Literal(Literal::Bool(true)),
                _ => unreachable!(),
            })
        }
        BinaryOperator::Eq | BinaryOperator::NotEq => {
            if let Some(expr) = fold_is_null_vs_bool(left, op, right) {
                return Some(expr);
            }
            if let Some(expr) = fold_is_null_vs_bool(right, op, left) {
                return Some(expr);
            }
            let (Some((left_expr, left_negated)), Some((right_expr, right_negated))) =
                (as_is_null(left), as_is_null(right))
            else {
                return None;
            };
            if !left_expr.structural_eq(right_expr) {
                return None;
            }
            let equal = left_negated == right_negated;
            let value = match op {
                BinaryOperator::Eq => equal,
                BinaryOperator::NotEq => !equal,
                _ => unreachable!(),
            };
            Some(Expr::Literal(Literal::Bool(value)))
        }
        _ => None,
    }
}

fn fold_is_null_vs_bool(left: &Expr, op: BinaryOperator, right: &Expr) -> Option<Expr> {
    let (expr, negated) = as_is_null(left)?;
    let bool_value = match right {
        Expr::Literal(Literal::Bool(value)) => *value,
        _ => return None,
    };
    let keep_original = match op {
        BinaryOperator::Eq => bool_value,
        BinaryOperator::NotEq => !bool_value,
        _ => return None,
    };
    Some(Expr::IsNull {
        expr: Box::new(expr.clone()),
        negated: if keep_original { negated } else { !negated },
    })
}

fn fold_self_comparison(left: &Expr, op: BinaryOperator, right: &Expr) -> Option<Expr> {
    if !left.structural_eq(right) {
        return None;
    }
    if !expr_is_definitely_non_null(left) {
        return None;
    }
    match op {
        BinaryOperator::Eq | BinaryOperator::LtEq | BinaryOperator::GtEq => {
            Some(Expr::Literal(Literal::Bool(true)))
        }
        BinaryOperator::NotEq | BinaryOperator::Lt | BinaryOperator::Gt => {
            Some(Expr::Literal(Literal::Bool(false)))
        }
        _ => None,
    }
}

fn as_is_null(expr: &Expr) -> Option<(&Expr, bool)> {
    match expr {
        Expr::IsNull { expr, negated } => Some((expr.as_ref(), *negated)),
        _ => None,
    }
}

fn expr_is_definitely_non_null(expr: &Expr) -> bool {
    match expr {
        Expr::Literal(_) | Expr::IsNull { .. } => true,
        Expr::UnaryOp {
            op: UnaryOperator::Not,
            expr,
        }
        | Expr::UnaryOp {
            op: UnaryOperator::Neg,
            expr,
        } => expr_is_definitely_non_null(expr),
        _ => false,
    }
}

fn rewrite_order_by(order_by: &[OrderByExpr]) -> Vec<OrderByExpr> {
    order_by
        .iter()
        .map(|item| OrderByExpr {
            expr: rewrite_expr(&item.expr),
            asc: item.asc,
            nulls_first: item.nulls_first,
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::{rewrite_expr, rewrite_plan};
    use chryso_core::ast::{BinaryOperator, Expr, Literal};
    use chryso_planner::LogicalPlan;

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
    fn folds_boolean_logic() {
        let expr = Expr::BinaryOp {
            left: Box::new(Expr::Identifier("a".to_string())),
            op: BinaryOperator::And,
            right: Box::new(Expr::Literal(Literal::Bool(true))),
        };
        let rewritten = rewrite_expr(&expr);
        match rewritten {
            Expr::Identifier(name) => assert_eq!(name, "a"),
            other => panic!("expected identifier, got {other:?}"),
        }

        let expr = Expr::BinaryOp {
            left: Box::new(Expr::Identifier("a".to_string())),
            op: BinaryOperator::Or,
            right: Box::new(Expr::Literal(Literal::Bool(true))),
        };
        let rewritten = rewrite_expr(&expr);
        match rewritten {
            Expr::Literal(Literal::Bool(value)) => assert!(value),
            other => panic!("expected literal true, got {other:?}"),
        }
    }

    #[test]
    fn folds_boolean_comparisons() {
        let expr = Expr::BinaryOp {
            left: Box::new(Expr::Literal(Literal::Bool(true))),
            op: BinaryOperator::NotEq,
            right: Box::new(Expr::Literal(Literal::Bool(false))),
        };
        let rewritten = rewrite_expr(&expr);
        match rewritten {
            Expr::Literal(Literal::Bool(value)) => assert!(value),
            other => panic!("expected literal true, got {other:?}"),
        }
    }

    #[test]
    fn folds_numeric_comparisons() {
        let expr = Expr::BinaryOp {
            left: Box::new(Expr::Literal(Literal::Number(1.0))),
            op: BinaryOperator::Lt,
            right: Box::new(Expr::Literal(Literal::Number(2.0))),
        };
        let rewritten = rewrite_expr(&expr);
        match rewritten {
            Expr::Literal(Literal::Bool(value)) => assert!(value),
            other => panic!("expected literal true, got {other:?}"),
        }
    }

    #[test]
    fn normalizes_not() {
        let expr = Expr::UnaryOp {
            op: chryso_core::ast::UnaryOperator::Not,
            expr: Box::new(Expr::UnaryOp {
                op: chryso_core::ast::UnaryOperator::Not,
                expr: Box::new(Expr::Identifier("a".to_string())),
            }),
        };
        let rewritten = rewrite_expr(&expr);
        match rewritten {
            Expr::Identifier(name) => assert_eq!(name, "a"),
            other => panic!("expected identifier, got {other:?}"),
        }
    }

    #[test]
    fn applies_de_morgan() {
        let expr = Expr::UnaryOp {
            op: chryso_core::ast::UnaryOperator::Not,
            expr: Box::new(Expr::BinaryOp {
                left: Box::new(Expr::Identifier("a".to_string())),
                op: BinaryOperator::And,
                right: Box::new(Expr::Identifier("b".to_string())),
            }),
        };
        let rewritten = rewrite_expr(&expr);
        match rewritten {
            Expr::BinaryOp {
                op: BinaryOperator::Or,
                left,
                right,
            } => match (*left, *right) {
                (
                    Expr::UnaryOp {
                        op: chryso_core::ast::UnaryOperator::Not,
                        ..
                    },
                    Expr::UnaryOp {
                        op: chryso_core::ast::UnaryOperator::Not,
                        ..
                    },
                ) => {}
                other => panic!("expected negated operands, got {other:?}"),
            },
            other => panic!("expected OR, got {other:?}"),
        }
    }

    #[test]
    fn dedups_boolean_idempotence() {
        let expr = Expr::BinaryOp {
            left: Box::new(Expr::Identifier("a".to_string())),
            op: BinaryOperator::And,
            right: Box::new(Expr::Identifier("a".to_string())),
        };
        let rewritten = rewrite_expr(&expr);
        match rewritten {
            Expr::Identifier(name) => assert_eq!(name, "a"),
            other => panic!("expected identifier, got {other:?}"),
        }
    }

    #[test]
    fn keeps_nullable_self_comparison() {
        let expr = Expr::BinaryOp {
            left: Box::new(Expr::Identifier("a".to_string())),
            op: BinaryOperator::Eq,
            right: Box::new(Expr::Identifier("a".to_string())),
        };
        let rewritten = rewrite_expr(&expr);
        match rewritten {
            Expr::BinaryOp {
                op: BinaryOperator::Eq,
                ..
            } => {}
            other => panic!("expected nullable self comparison to remain, got {other:?}"),
        }
    }

    #[test]
    fn folds_non_null_self_comparison() {
        let expr = Expr::BinaryOp {
            left: Box::new(Expr::IsNull {
                expr: Box::new(Expr::Identifier("a".to_string())),
                negated: false,
            }),
            op: BinaryOperator::Eq,
            right: Box::new(Expr::IsNull {
                expr: Box::new(Expr::Identifier("a".to_string())),
                negated: false,
            }),
        };
        let rewritten = rewrite_expr(&expr);
        match rewritten {
            Expr::Literal(Literal::Bool(value)) => assert!(value),
            other => panic!("expected literal true, got {other:?}"),
        }
    }

    #[test]
    fn folds_null_check_contradictions() {
        let left = Expr::IsNull {
            expr: Box::new(Expr::Identifier("a".to_string())),
            negated: false,
        };
        let right = Expr::IsNull {
            expr: Box::new(Expr::Identifier("a".to_string())),
            negated: true,
        };
        let and_expr = Expr::BinaryOp {
            left: Box::new(left.clone()),
            op: BinaryOperator::And,
            right: Box::new(right.clone()),
        };
        let or_expr = Expr::BinaryOp {
            left: Box::new(left),
            op: BinaryOperator::Or,
            right: Box::new(right),
        };
        match rewrite_expr(&and_expr) {
            Expr::Literal(Literal::Bool(value)) => assert!(!value),
            other => panic!("expected false, got {other:?}"),
        }
        match rewrite_expr(&or_expr) {
            Expr::Literal(Literal::Bool(value)) => assert!(value),
            other => panic!("expected true, got {other:?}"),
        }
    }

    #[test]
    fn folds_is_null_bool_comparison() {
        let expr = Expr::BinaryOp {
            left: Box::new(Expr::IsNull {
                expr: Box::new(Expr::Identifier("a".to_string())),
                negated: false,
            }),
            op: BinaryOperator::Eq,
            right: Box::new(Expr::Literal(Literal::Bool(false))),
        };
        let rewritten = rewrite_expr(&expr);
        match rewritten {
            Expr::IsNull { negated, .. } => assert!(negated),
            other => panic!("expected `is not null`, got {other:?}"),
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
