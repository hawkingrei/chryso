use crate::utils::{collect_identifiers, collect_tables, combine_conjuncts, split_conjuncts, table_prefix};
use corundum_core::ast::{BinaryOperator, Expr, Literal};
use corundum_metadata::StatsCache;
use corundum_planner::LogicalPlan;
use std::collections::HashSet;

pub fn enumerate_join_orders(plan: &LogicalPlan, stats: &StatsCache) -> Vec<LogicalPlan> {
    vec![reorder_joins(plan, stats)]
}

fn reorder_joins(plan: &LogicalPlan, stats: &StatsCache) -> LogicalPlan {
    match plan {
        LogicalPlan::Join {
            join_type,
            left,
            right,
            on,
        } => {
            if matches!(join_type, corundum_core::ast::JoinType::Inner) {
                let mut inputs = Vec::new();
                let mut predicates = Vec::new();
                collect_inner_joins(plan, &mut inputs, &mut predicates);
                let mut inputs = inputs
                    .into_iter()
                    .map(|input| reorder_joins(&input, stats))
                    .collect::<Vec<_>>();
                if inputs.len() <= 1 {
                    return inputs.pop().unwrap_or_else(|| plan.clone());
                }
                return build_greedy_join(inputs, predicates, stats);
            }
            let left = reorder_joins(left.as_ref(), stats);
            let right = reorder_joins(right.as_ref(), stats);
            LogicalPlan::Join {
                join_type: *join_type,
                left: Box::new(left),
                right: Box::new(right),
                on: on.clone(),
            }
        }
        LogicalPlan::Filter { predicate, input } => LogicalPlan::Filter {
            predicate: predicate.clone(),
            input: Box::new(reorder_joins(input.as_ref(), stats)),
        },
        LogicalPlan::Projection { exprs, input } => LogicalPlan::Projection {
            exprs: exprs.clone(),
            input: Box::new(reorder_joins(input.as_ref(), stats)),
        },
        LogicalPlan::Aggregate {
            group_exprs,
            aggr_exprs,
            input,
        } => LogicalPlan::Aggregate {
            group_exprs: group_exprs.clone(),
            aggr_exprs: aggr_exprs.clone(),
            input: Box::new(reorder_joins(input.as_ref(), stats)),
        },
        LogicalPlan::Distinct { input } => LogicalPlan::Distinct {
            input: Box::new(reorder_joins(input.as_ref(), stats)),
        },
        LogicalPlan::TopN {
            order_by,
            limit,
            input,
        } => LogicalPlan::TopN {
            order_by: order_by.clone(),
            limit: *limit,
            input: Box::new(reorder_joins(input.as_ref(), stats)),
        },
        LogicalPlan::Sort { order_by, input } => LogicalPlan::Sort {
            order_by: order_by.clone(),
            input: Box::new(reorder_joins(input.as_ref(), stats)),
        },
        LogicalPlan::Limit {
            limit,
            offset,
            input,
        } => LogicalPlan::Limit {
            limit: *limit,
            offset: *offset,
            input: Box::new(reorder_joins(input.as_ref(), stats)),
        },
        LogicalPlan::Derived {
            input,
            alias,
            column_aliases,
        } => LogicalPlan::Derived {
            input: Box::new(reorder_joins(input.as_ref(), stats)),
            alias: alias.clone(),
            column_aliases: column_aliases.clone(),
        },
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
            predicate: predicate.clone(),
        },
        LogicalPlan::Dml { sql } => LogicalPlan::Dml { sql: sql.clone() },
    }
}

fn collect_inner_joins(plan: &LogicalPlan, inputs: &mut Vec<LogicalPlan>, predicates: &mut Vec<Expr>) {
    match plan {
        LogicalPlan::Join {
            join_type,
            left,
            right,
            on,
        } if matches!(join_type, corundum_core::ast::JoinType::Inner) => {
            collect_inner_joins(left.as_ref(), inputs, predicates);
            collect_inner_joins(right.as_ref(), inputs, predicates);
            predicates.extend(split_conjuncts(on));
        }
        _ => inputs.push(plan.clone()),
    }
}

fn build_greedy_join(mut inputs: Vec<LogicalPlan>, predicates: Vec<Expr>, stats: &StatsCache) -> LogicalPlan {
    let mut items = inputs
        .drain(..)
        .map(|plan| JoinItem::new(plan, stats))
        .collect::<Vec<_>>();
    items.sort_by(|a, b| a.estimated_rows.partial_cmp(&b.estimated_rows).unwrap());
    let mut current = items.remove(0);
    let mut remaining_preds = predicates;

    while !items.is_empty() {
        let mut best_index = None;
        let mut best_rows = f64::INFINITY;
        let mut best_connected = false;
        let mut best_score = f64::INFINITY;
        let mut best_key = String::new();
        for (idx, item) in items.iter().enumerate() {
            let (connected, score) =
                connection_score(&current.tables, &item.tables, &remaining_preds);
            let candidate_rows = item.estimated_rows;
            if connected
                && (!best_connected
                    || score < best_score
                    || candidate_rows < best_rows
                    || (score == best_score
                        && candidate_rows == best_rows
                        && (best_key.is_empty() || item.sort_key < best_key)))
            {
                best_connected = true;
                best_rows = candidate_rows;
                best_score = score;
                best_index = Some(idx);
                best_key = item.sort_key.clone();
            } else if !best_connected && candidate_rows < best_rows {
                best_rows = candidate_rows;
                best_score = score;
                best_index = Some(idx);
                best_key = item.sort_key.clone();
            } else if !best_connected
                && candidate_rows == best_rows
                && (best_key.is_empty() || item.sort_key < best_key)
            {
                best_index = Some(idx);
                best_key = item.sort_key.clone();
            }
        }
        let idx = best_index.unwrap_or(0);
        let item = items.remove(idx);
        let (join_preds, leftover) =
            take_connecting_predicates(&current.tables, &item.tables, remaining_preds);
        remaining_preds = leftover;
        let on = combine_conjuncts(join_preds)
            .unwrap_or_else(|| Expr::Literal(Literal::Bool(true)));
        let joined_plan = LogicalPlan::Join {
            join_type: corundum_core::ast::JoinType::Inner,
            left: Box::new(current.plan),
            right: Box::new(item.plan),
            on,
        };
        let estimated_rows = estimate_rows(&joined_plan, stats);
        let mut tables = current.tables;
        tables.extend(item.tables);
        current = JoinItem {
            plan: joined_plan,
            tables,
            estimated_rows,
            sort_key: current.sort_key,
        };
    }

    if let Some(expr) = combine_conjuncts(remaining_preds) {
        LogicalPlan::Filter {
            predicate: expr,
            input: Box::new(current.plan),
        }
    } else {
        current.plan
    }
}

struct JoinItem {
    plan: LogicalPlan,
    tables: HashSet<String>,
    estimated_rows: f64,
    sort_key: String,
}

impl JoinItem {
    fn new(plan: LogicalPlan, stats: &StatsCache) -> Self {
        let tables = collect_tables(&plan);
        let estimated_rows = estimate_rows(&plan, stats);
        let sort_key = tables.iter().min().cloned().unwrap_or_default();
        Self {
            plan,
            tables,
            estimated_rows,
            sort_key,
        }
    }
}

fn connection_score(
    left: &HashSet<String>,
    right: &HashSet<String>,
    predicates: &[Expr],
) -> (bool, f64) {
    let mut connected = false;
    let mut total = 0.0;
    for predicate in predicates {
        let Some(tables) = predicate_tables(predicate) else {
            continue;
        };
        if tables_intersect(left, &tables) && tables_intersect(right, &tables) {
            connected = true;
            total += estimate_predicate_selectivity(predicate);
        }
    }
    if !connected {
        return (false, f64::INFINITY);
    }
    (true, total)
}

fn estimate_predicate_selectivity(predicate: &Expr) -> f64 {
    match predicate {
        Expr::BinaryOp { op, left, right } if matches!(op, BinaryOperator::Eq) => {
            if matches!(left.as_ref(), Expr::Identifier(_))
                && matches!(right.as_ref(), Expr::Identifier(_))
            {
                0.1
            } else if matches!(left.as_ref(), Expr::Identifier(_))
                && matches!(right.as_ref(), Expr::Literal(_))
            {
                0.05
            } else if matches!(right.as_ref(), Expr::Identifier(_))
                && matches!(left.as_ref(), Expr::Literal(_))
            {
                0.05
            } else {
                0.2
            }
        }
        Expr::BinaryOp { op, .. } if matches!(op, BinaryOperator::Lt | BinaryOperator::LtEq | BinaryOperator::Gt | BinaryOperator::GtEq) => 0.3,
        Expr::IsNull { .. } => 0.2,
        Expr::UnaryOp { op, .. } if matches!(op, corundum_core::ast::UnaryOperator::Not) => 0.5,
        Expr::BinaryOp { op, .. } if matches!(op, BinaryOperator::And) => 0.2,
        Expr::BinaryOp { op, .. } if matches!(op, BinaryOperator::Or) => 0.8,
        _ => 0.5,
    }
}

fn take_connecting_predicates(
    left: &HashSet<String>,
    right: &HashSet<String>,
    predicates: Vec<Expr>,
) -> (Vec<Expr>, Vec<Expr>) {
    let mut used = Vec::new();
    let mut remaining = Vec::new();
    for predicate in predicates {
        let Some(tables) = predicate_tables(&predicate) else {
            remaining.push(predicate);
            continue;
        };
        if tables_intersect(left, &tables) && tables_intersect(right, &tables) {
            used.push(predicate);
        } else {
            remaining.push(predicate);
        }
    }
    (used, remaining)
}

fn predicate_tables(expr: &Expr) -> Option<HashSet<String>> {
    let mut tables = HashSet::new();
    let idents = collect_identifiers(expr);
    for ident in idents {
        if let Some(prefix) = table_prefix(&ident) {
            tables.insert(prefix.to_string());
        }
    }
    if tables.is_empty() {
        None
    } else {
        Some(tables)
    }
}
// shared helpers are in utils.rs

fn tables_intersect(left: &HashSet<String>, right: &HashSet<String>) -> bool {
    left.iter().any(|item| right.contains(item))
}

fn estimate_rows(plan: &LogicalPlan, stats: &StatsCache) -> f64 {
    match plan {
        LogicalPlan::Scan { table } | LogicalPlan::IndexScan { table, .. } => stats
            .table_stats(table)
            .map(|stats| stats.row_count)
            .unwrap_or(1000.0),
        LogicalPlan::Filter { input, .. } => estimate_rows(input, stats) * 0.3,
        LogicalPlan::Projection { input, .. } => estimate_rows(input, stats),
        LogicalPlan::Aggregate { input, .. } => (estimate_rows(input, stats) * 0.1).max(1.0),
        LogicalPlan::Distinct { input } => (estimate_rows(input, stats) * 0.3).max(1.0),
        LogicalPlan::TopN { limit, input, .. } => {
            estimate_rows(input, stats).min(*limit as f64)
        }
        LogicalPlan::Sort { input, .. } => estimate_rows(input, stats),
        LogicalPlan::Limit { limit, input, .. } => match limit {
            Some(limit) => estimate_rows(input, stats).min(*limit as f64),
            None => estimate_rows(input, stats),
        },
        LogicalPlan::Join { left, right, .. } => {
            estimate_rows(left, stats) * estimate_rows(right, stats) * 0.1
        }
        LogicalPlan::Derived { input, .. } => estimate_rows(input, stats),
        LogicalPlan::Dml { .. } => 1.0,
    }
}

#[cfg(test)]
mod tests {
    use super::enumerate_join_orders;
    use corundum_metadata::{StatsCache, TableStats};
    use corundum_planner::LogicalPlan;

    #[test]
    fn prefers_smaller_left_input() {
        let plan = LogicalPlan::Join {
            join_type: corundum_core::ast::JoinType::Inner,
            left: Box::new(LogicalPlan::Scan {
                table: "big".to_string(),
            }),
            right: Box::new(LogicalPlan::Scan {
                table: "small".to_string(),
            }),
            on: corundum_core::ast::Expr::BinaryOp {
                left: Box::new(corundum_core::ast::Expr::Identifier("big.id".to_string())),
                op: corundum_core::ast::BinaryOperator::Eq,
                right: Box::new(corundum_core::ast::Expr::Identifier("small.id".to_string())),
            },
        };
        let mut stats = StatsCache::new();
        stats.insert_table_stats("big", TableStats { row_count: 1_000.0 });
        stats.insert_table_stats("small", TableStats { row_count: 10.0 });
        let reordered = enumerate_join_orders(&plan, &stats);
        let join_plan = match &reordered[0] {
            LogicalPlan::Join { .. } => &reordered[0],
            LogicalPlan::Filter { input, .. } => input.as_ref(),
            other => panic!("expected join, got {other:?}"),
        };
        let LogicalPlan::Join { left, right, .. } = join_plan else {
            panic!("expected join");
        };
        let LogicalPlan::Scan { table } = left.as_ref() else {
            panic!("expected scan");
        };
        let LogicalPlan::Scan { table: right_table } = right.as_ref() else {
            panic!("expected scan");
        };
        assert_eq!(table, "small");
        assert_eq!(right_table, "big");
    }

    #[test]
    fn orders_three_way_join_by_stats() {
        let plan = LogicalPlan::Join {
            join_type: corundum_core::ast::JoinType::Inner,
            left: Box::new(LogicalPlan::Join {
                join_type: corundum_core::ast::JoinType::Inner,
                left: Box::new(LogicalPlan::Scan {
                    table: "large".to_string(),
                }),
                right: Box::new(LogicalPlan::Scan {
                    table: "small".to_string(),
                }),
                on: corundum_core::ast::Expr::BinaryOp {
                    left: Box::new(corundum_core::ast::Expr::Identifier("large.id".to_string())),
                    op: corundum_core::ast::BinaryOperator::Eq,
                    right: Box::new(corundum_core::ast::Expr::Identifier("small.id".to_string())),
                },
            }),
            right: Box::new(LogicalPlan::Scan {
                table: "medium".to_string(),
            }),
            on: corundum_core::ast::Expr::BinaryOp {
                left: Box::new(corundum_core::ast::Expr::Identifier("large.id".to_string())),
                op: corundum_core::ast::BinaryOperator::Eq,
                right: Box::new(corundum_core::ast::Expr::Identifier("medium.id".to_string())),
            },
        };
        let mut stats = StatsCache::new();
        stats.insert_table_stats("large", TableStats { row_count: 10_000.0 });
        stats.insert_table_stats("medium", TableStats { row_count: 1_000.0 });
        stats.insert_table_stats("small", TableStats { row_count: 10.0 });

        let reordered = enumerate_join_orders(&plan, &stats);
        let LogicalPlan::Join { left, right, .. } = &reordered[0] else {
            panic!("expected join");
        };
        let LogicalPlan::Join { left: inner_left, right: inner_right, .. } = left.as_ref() else {
            panic!("expected join");
        };
        let LogicalPlan::Scan { table: left_table } = inner_left.as_ref() else {
            panic!("expected scan");
        };
        let LogicalPlan::Scan { table: right_table } = inner_right.as_ref() else {
            panic!("expected scan");
        };
        assert_eq!(left_table, "small");
        assert!(right_table == "medium" || right_table == "large");
        assert!(matches!(right.as_ref(), LogicalPlan::Scan { .. } | LogicalPlan::Join { .. }));
    }

    #[test]
    fn prefers_more_selective_join_predicate() {
        let plan = LogicalPlan::Join {
            join_type: corundum_core::ast::JoinType::Inner,
            left: Box::new(LogicalPlan::Join {
                join_type: corundum_core::ast::JoinType::Inner,
                left: Box::new(LogicalPlan::Scan {
                    table: "t1".to_string(),
                }),
                right: Box::new(LogicalPlan::Scan {
                    table: "t2".to_string(),
                }),
                on: corundum_core::ast::Expr::BinaryOp {
                    left: Box::new(corundum_core::ast::Expr::Identifier("t1.id".to_string())),
                    op: corundum_core::ast::BinaryOperator::Eq,
                    right: Box::new(corundum_core::ast::Expr::Identifier("t2.id".to_string())),
                },
            }),
            right: Box::new(LogicalPlan::Scan {
                table: "t3".to_string(),
            }),
            on: corundum_core::ast::Expr::BinaryOp {
                left: Box::new(corundum_core::ast::Expr::Identifier("t1.id".to_string())),
                op: corundum_core::ast::BinaryOperator::Eq,
                right: Box::new(corundum_core::ast::Expr::Literal(
                    corundum_core::ast::Literal::Number(1.0),
                )),
            },
        };
        let stats = StatsCache::new();
        let reordered = enumerate_join_orders(&plan, &stats);
        let join_plan = match &reordered[0] {
            LogicalPlan::Join { .. } => &reordered[0],
            LogicalPlan::Filter { input, .. } => input.as_ref(),
            other => panic!("expected join, got {other:?}"),
        };
        let LogicalPlan::Join { left, right, .. } = join_plan else {
            panic!("expected join");
        };
        let left_sql = format!("{left:?}");
        let right_sql = format!("{right:?}");
        assert!(left_sql.contains("t3") || right_sql.contains("t3"));
    }
}
