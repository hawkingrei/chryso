use crate::utils::{collect_identifiers, collect_tables, table_prefix};
use chryso_core::ast::{Expr, OrderByExpr};
use chryso_planner::LogicalPlan;
use std::collections::HashSet;

pub fn prune_plan(plan: &LogicalPlan) -> LogicalPlan {
    match plan {
        LogicalPlan::Projection { exprs, input } => {
            let required = collect_from_exprs(exprs);
            let input = prune_plan_with_required(input.as_ref(), Some(&required));
            LogicalPlan::Projection {
                exprs: exprs.clone(),
                input: Box::new(input),
            }
        }
        _ => prune_plan_with_required(plan, None),
    }
}

fn prune_plan_with_required(plan: &LogicalPlan, required: Option<&HashSet<String>>) -> LogicalPlan {
    match plan {
        LogicalPlan::Scan { table } => prune_scan(table, required),
        LogicalPlan::IndexScan {
            table,
            index,
            predicate,
        } => prune_index_scan(table, index, predicate, required),
        LogicalPlan::Dml { sql } => LogicalPlan::Dml { sql: sql.clone() },
        LogicalPlan::Derived {
            input,
            alias,
            column_aliases,
        } => LogicalPlan::Derived {
            input: Box::new(prune_plan_with_required(input.as_ref(), required)),
            alias: alias.clone(),
            column_aliases: column_aliases.clone(),
        },
        LogicalPlan::Filter { predicate, input } => {
            let next_required = if let Some(required) = required {
                let mut combined = required.clone();
                combined.extend(collect_identifiers(predicate));
                Some(combined)
            } else {
                None
            };
            LogicalPlan::Filter {
                predicate: predicate.clone(),
                input: Box::new(prune_plan_with_required(
                    input.as_ref(),
                    next_required.as_ref(),
                )),
            }
        }
        LogicalPlan::Projection { exprs, input } => {
            let (exprs, input_required) = prune_projection(exprs, required);
            LogicalPlan::Projection {
                exprs,
                input: Box::new(prune_plan_with_required(
                    input.as_ref(),
                    input_required.as_ref(),
                )),
            }
        }
        LogicalPlan::Join {
            join_type,
            left,
            right,
            on,
        } => {
            let mut join_required = HashSet::new();
            if let Some(required) = required {
                join_required.extend(required.iter().cloned());
            }
            join_required.extend(collect_identifiers(on));
            let left_tables = collect_tables(left.as_ref());
            let right_tables = collect_tables(right.as_ref());
            let (left_required, right_required) =
                split_required(&join_required, &left_tables, &right_tables);
            LogicalPlan::Join {
                join_type: *join_type,
                left: Box::new(prune_plan_with_required(
                    left.as_ref(),
                    left_required.as_ref(),
                )),
                right: Box::new(prune_plan_with_required(
                    right.as_ref(),
                    right_required.as_ref(),
                )),
                on: on.clone(),
            }
        }
        LogicalPlan::Aggregate {
            group_exprs,
            aggr_exprs,
            input,
        } => {
            let mut input_required = collect_from_exprs(group_exprs);
            input_required.extend(collect_from_exprs(aggr_exprs));
            LogicalPlan::Aggregate {
                group_exprs: group_exprs.clone(),
                aggr_exprs: aggr_exprs.clone(),
                input: Box::new(prune_plan_with_required(
                    input.as_ref(),
                    Some(&input_required),
                )),
            }
        }
        LogicalPlan::Distinct { input } => LogicalPlan::Distinct {
            input: Box::new(prune_plan_with_required(input.as_ref(), required)),
        },
        LogicalPlan::TopN {
            order_by,
            limit,
            input,
        } => {
            let next_required = merge_order_by(required, order_by);
            LogicalPlan::TopN {
                order_by: order_by.clone(),
                limit: *limit,
                input: Box::new(prune_plan_with_required(
                    input.as_ref(),
                    next_required.as_ref(),
                )),
            }
        }
        LogicalPlan::Sort { order_by, input } => {
            let next_required = merge_order_by(required, order_by);
            LogicalPlan::Sort {
                order_by: order_by.clone(),
                input: Box::new(prune_plan_with_required(
                    input.as_ref(),
                    next_required.as_ref(),
                )),
            }
        }
        LogicalPlan::Limit {
            limit,
            offset,
            input,
        } => LogicalPlan::Limit {
            limit: *limit,
            offset: *offset,
            input: Box::new(prune_plan_with_required(input.as_ref(), required)),
        },
    }
}

fn prune_scan(table: &str, required: Option<&HashSet<String>>) -> LogicalPlan {
    let required = match required {
        Some(required) => required,
        None => {
            return LogicalPlan::Scan {
                table: table.to_string(),
            };
        }
    };
    let exprs = required_exprs_for_table(table, required);
    if exprs.is_empty() {
        LogicalPlan::Scan {
            table: table.to_string(),
        }
    } else {
        LogicalPlan::Projection {
            exprs,
            input: Box::new(LogicalPlan::Scan {
                table: table.to_string(),
            }),
        }
    }
}

fn prune_index_scan(
    table: &str,
    index: &str,
    predicate: &Expr,
    required: Option<&HashSet<String>>,
) -> LogicalPlan {
    let required = match required {
        Some(required) => required,
        None => {
            return LogicalPlan::IndexScan {
                table: table.to_string(),
                index: index.to_string(),
                predicate: predicate.clone(),
            };
        }
    };
    let mut combined = required.clone();
    combined.extend(collect_identifiers(predicate));
    let exprs = required_exprs_for_table(table, &combined);
    if exprs.is_empty() {
        LogicalPlan::IndexScan {
            table: table.to_string(),
            index: index.to_string(),
            predicate: predicate.clone(),
        }
    } else {
        LogicalPlan::Projection {
            exprs,
            input: Box::new(LogicalPlan::IndexScan {
                table: table.to_string(),
                index: index.to_string(),
                predicate: predicate.clone(),
            }),
        }
    }
}

fn prune_projection(
    exprs: &[Expr],
    required: Option<&HashSet<String>>,
) -> (Vec<Expr>, Option<HashSet<String>>) {
    if exprs.iter().any(|expr| matches!(expr, Expr::Wildcard)) {
        let needed = collect_from_exprs(exprs);
        return (exprs.to_vec(), Some(needed));
    }
    let required = match required {
        Some(required) => required,
        None => {
            let needed = collect_from_exprs(exprs);
            return (exprs.to_vec(), Some(needed));
        }
    };
    let mut pruned = Vec::new();
    for expr in exprs {
        match expr {
            Expr::Identifier(name) => {
                if required.contains(name) {
                    pruned.push(expr.clone());
                }
            }
            _ => pruned.push(expr.clone()),
        }
    }
    if pruned.is_empty() {
        pruned = exprs.to_vec();
    }
    let needed = collect_from_exprs(&pruned);
    (pruned, Some(needed))
}

fn merge_order_by(
    required: Option<&HashSet<String>>,
    order_by: &[OrderByExpr],
) -> Option<HashSet<String>> {
    let mut merged = match required {
        Some(required) => required.clone(),
        None => return None,
    };
    for item in order_by {
        merged.extend(collect_identifiers(&item.expr));
    }
    Some(merged)
}

fn required_exprs_for_table(table: &str, required: &HashSet<String>) -> Vec<Expr> {
    let mut idents = required
        .iter()
        .filter_map(|ident| match table_prefix(ident) {
            Some(prefix) if prefix == table => Some(ident.clone()),
            None => Some(ident.clone()),
            _ => None,
        })
        .collect::<Vec<_>>();
    idents.sort();
    idents.into_iter().map(Expr::Identifier).collect()
}

fn split_required(
    required: &HashSet<String>,
    left_tables: &HashSet<String>,
    right_tables: &HashSet<String>,
) -> (Option<HashSet<String>>, Option<HashSet<String>>) {
    let mut left = HashSet::new();
    let mut right = HashSet::new();
    for ident in required {
        match table_prefix(ident) {
            Some(prefix) => {
                let in_left = left_tables.contains(prefix);
                let in_right = right_tables.contains(prefix);
                if in_left && !in_right {
                    left.insert(ident.clone());
                } else if in_right && !in_left {
                    right.insert(ident.clone());
                } else {
                    left.insert(ident.clone());
                    right.insert(ident.clone());
                }
            }
            None => {
                left.insert(ident.clone());
                right.insert(ident.clone());
            }
        }
    }
    let left = if left.is_empty() { None } else { Some(left) };
    let right = if right.is_empty() { None } else { Some(right) };
    (left, right)
}

fn collect_from_exprs(exprs: &[Expr]) -> HashSet<String> {
    let mut out = HashSet::new();
    for expr in exprs {
        out.extend(collect_identifiers(expr));
    }
    out
}

// shared helpers are in utils.rs

#[cfg(test)]
mod tests {
    use super::prune_plan;
    use chryso_core::ast::{BinaryOperator, Expr, Literal};
    use chryso_planner::LogicalPlan;

    #[test]
    fn inserts_projections_for_join_inputs() {
        let plan = LogicalPlan::Projection {
            exprs: vec![Expr::Identifier("t1.id".to_string())],
            input: Box::new(LogicalPlan::Join {
                join_type: chryso_core::ast::JoinType::Inner,
                left: Box::new(LogicalPlan::Scan {
                    table: "t1".to_string(),
                }),
                right: Box::new(LogicalPlan::Scan {
                    table: "t2".to_string(),
                }),
                on: Expr::BinaryOp {
                    left: Box::new(Expr::Identifier("t1.id".to_string())),
                    op: BinaryOperator::Eq,
                    right: Box::new(Expr::Identifier("t2.id".to_string())),
                },
            }),
        };
        let rewritten = prune_plan(&plan);
        let LogicalPlan::Projection { input, .. } = rewritten else {
            panic!("expected projection root");
        };
        let LogicalPlan::Join { left, right, .. } = input.as_ref() else {
            panic!("expected join");
        };
        assert!(matches!(left.as_ref(), LogicalPlan::Projection { .. }));
        assert!(matches!(right.as_ref(), LogicalPlan::Projection { .. }));
    }

    #[test]
    fn keeps_projection_when_required_empty() {
        let plan = LogicalPlan::Projection {
            exprs: vec![Expr::Identifier("id".to_string())],
            input: Box::new(LogicalPlan::Scan {
                table: "t".to_string(),
            }),
        };
        let rewritten = prune_plan(&plan);
        let LogicalPlan::Projection { exprs, .. } = rewritten else {
            panic!("expected projection root");
        };
        assert_eq!(exprs.len(), 1);
    }

    #[test]
    fn index_scan_includes_predicate_columns() {
        let plan = LogicalPlan::Projection {
            exprs: vec![Expr::Identifier("t.id".to_string())],
            input: Box::new(LogicalPlan::IndexScan {
                table: "t".to_string(),
                index: "idx".to_string(),
                predicate: Expr::BinaryOp {
                    left: Box::new(Expr::Identifier("t.region".to_string())),
                    op: BinaryOperator::Eq,
                    right: Box::new(Expr::Literal(Literal::String("APAC".to_string()))),
                },
            }),
        };
        let rewritten = prune_plan(&plan);
        let LogicalPlan::Projection { input, .. } = rewritten else {
            panic!("expected projection root");
        };
        let LogicalPlan::Projection { exprs, .. } = input.as_ref() else {
            panic!("expected projection on index scan");
        };
        assert!(
            exprs
                .iter()
                .any(|expr| matches!(expr, Expr::Identifier(name) if name == "t.region"))
        );
    }
}
