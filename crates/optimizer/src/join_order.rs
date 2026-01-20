use corundum_metadata::StatsCache;
use corundum_planner::LogicalPlan;

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
            let left = reorder_joins(left.as_ref(), stats);
            let right = reorder_joins(right.as_ref(), stats);
            if matches!(join_type, corundum_core::ast::JoinType::Inner) {
                let left_rows = estimate_rows(&left, stats);
                let right_rows = estimate_rows(&right, stats);
                if left_rows > right_rows {
                    return LogicalPlan::Join {
                        join_type: *join_type,
                        left: Box::new(right),
                        right: Box::new(left),
                        on: on.clone(),
                    };
                }
            }
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
            on: corundum_core::ast::Expr::Identifier("big.id = small.id".to_string()),
        };
        let mut stats = StatsCache::new();
        stats.insert_table_stats("big", TableStats { row_count: 1_000.0 });
        stats.insert_table_stats("small", TableStats { row_count: 10.0 });
        let reordered = enumerate_join_orders(&plan, &stats);
        let LogicalPlan::Join { left, right, .. } = &reordered[0] else {
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
}
