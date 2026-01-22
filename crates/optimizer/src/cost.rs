pub use corundum_planner::cost::{Cost, CostModel};
use corundum_metadata::StatsCache;
use corundum_planner::PhysicalPlan;

pub struct UnitCostModel;

impl CostModel for UnitCostModel {
    fn cost(&self, plan: &PhysicalPlan) -> Cost {
        Cost(plan.node_count() as f64 + join_penalty(plan))
    }
}

impl std::fmt::Debug for UnitCostModel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("UnitCostModel")
    }
}

pub struct StatsCostModel<'a> {
    stats: &'a StatsCache,
}

impl<'a> StatsCostModel<'a> {
    pub fn new(stats: &'a StatsCache) -> Self {
        Self { stats }
    }
}

impl CostModel for StatsCostModel<'_> {
    fn cost(&self, plan: &PhysicalPlan) -> Cost {
        Cost(estimate_rows(plan, self.stats) + join_penalty(plan))
    }
}

impl std::fmt::Debug for StatsCostModel<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("StatsCostModel")
    }
}

#[cfg(test)]
mod tests {
    use super::{CostModel, StatsCostModel, StatsCache, UnitCostModel};
    use corundum_metadata::ColumnStats;
    use corundum_planner::PhysicalPlan;

    #[test]
    fn unit_cost_counts_nodes() {
        let plan = PhysicalPlan::Filter {
            predicate: corundum_core::ast::Expr::Identifier("x".to_string()),
            input: Box::new(PhysicalPlan::TableScan {
                table: "t".to_string(),
            }),
        };
        let cost = UnitCostModel.cost(&plan);
        assert_eq!(cost.0, 2.0);
    }

    #[test]
    fn join_algorithm_costs_differ() {
        let left = PhysicalPlan::TableScan {
            table: "t1".to_string(),
        };
        let right = PhysicalPlan::TableScan {
            table: "t2".to_string(),
        };
        let hash = PhysicalPlan::Join {
            join_type: corundum_core::ast::JoinType::Inner,
            algorithm: corundum_planner::JoinAlgorithm::Hash,
            left: Box::new(left.clone()),
            right: Box::new(right.clone()),
            on: corundum_core::ast::Expr::Identifier("t1.id = t2.id".to_string()),
        };
        let nested = PhysicalPlan::Join {
            join_type: corundum_core::ast::JoinType::Inner,
            algorithm: corundum_planner::JoinAlgorithm::NestedLoop,
            left: Box::new(left),
            right: Box::new(right),
            on: corundum_core::ast::Expr::Identifier("t1.id = t2.id".to_string()),
        };
        let model = UnitCostModel;
        assert!(model.cost(&hash).0 < model.cost(&nested).0);
    }

    #[test]
    fn stats_cost_uses_selectivity() {
        let plan = PhysicalPlan::Filter {
            predicate: corundum_core::ast::Expr::BinaryOp {
                left: Box::new(corundum_core::ast::Expr::Identifier(
                    "sales.region".to_string(),
                )),
                op: corundum_core::ast::BinaryOperator::Eq,
                right: Box::new(corundum_core::ast::Expr::Literal(
                    corundum_core::ast::Literal::String("us".to_string()),
                )),
            },
            input: Box::new(PhysicalPlan::TableScan {
                table: "sales".to_string(),
            }),
        };
        let mut stats = StatsCache::new();
        stats.insert_table_stats("sales", corundum_metadata::TableStats { row_count: 100.0 });
        stats.insert_column_stats(
            "sales",
            "region",
            ColumnStats {
                distinct_count: 50.0,
                null_fraction: 0.0,
            },
        );
        let model = StatsCostModel::new(&stats);
        let cost = model.cost(&plan);
        assert!(cost.0 < 5.0);
    }
}

fn join_penalty(plan: &PhysicalPlan) -> f64 {
    match plan {
        PhysicalPlan::Join { algorithm, .. } => match algorithm {
            corundum_planner::JoinAlgorithm::Hash => 1.0,
            corundum_planner::JoinAlgorithm::NestedLoop => 5.0,
        },
        _ => 0.0,
    }
}

fn estimate_rows(plan: &PhysicalPlan, stats: &StatsCache) -> f64 {
    match plan {
        PhysicalPlan::TableScan { table } | PhysicalPlan::IndexScan { table, .. } => stats
            .table_stats(table)
            .map(|stats| stats.row_count)
            .unwrap_or(1000.0),
        PhysicalPlan::Dml { .. } => 1.0,
        PhysicalPlan::Derived { input, .. } => estimate_rows(input, stats),
        PhysicalPlan::Filter { predicate, input } => {
            let base = estimate_rows(input, stats);
            let table = single_table_name(input);
            base * estimate_selectivity(predicate, stats, table.as_deref())
        }
        PhysicalPlan::Projection { input, .. } => estimate_rows(input, stats),
        PhysicalPlan::Join { left, right, .. } => {
            estimate_rows(left, stats) * estimate_rows(right, stats) * 0.1
        }
        PhysicalPlan::Aggregate { input, .. } => (estimate_rows(input, stats) * 0.1).max(1.0),
        PhysicalPlan::Distinct { input } => (estimate_rows(input, stats) * 0.3).max(1.0),
        PhysicalPlan::TopN { limit, input, .. } => estimate_rows(input, stats).min(*limit as f64),
        PhysicalPlan::Sort { input, .. } => estimate_rows(input, stats),
        PhysicalPlan::Limit { limit, input, .. } => match limit {
            Some(limit) => estimate_rows(input, stats).min(*limit as f64),
            None => estimate_rows(input, stats),
        },
    }
}

fn estimate_selectivity(
    predicate: &corundum_core::ast::Expr,
    stats: &StatsCache,
    table: Option<&str>,
) -> f64 {
    use corundum_core::ast::{BinaryOperator, Expr};
    match predicate {
        Expr::BinaryOp { left, op, right } if matches!(op, BinaryOperator::And) => {
            estimate_selectivity(left, stats, table) * estimate_selectivity(right, stats, table)
        }
        Expr::BinaryOp { left, op, right } if matches!(op, BinaryOperator::Or) => {
            let left_sel = estimate_selectivity(left, stats, table);
            let right_sel = estimate_selectivity(right, stats, table);
            (left_sel + right_sel - left_sel * right_sel).min(1.0)
        }
        Expr::IsNull { expr, negated } => {
            let (table_name, column_name) = match expr.as_ref() {
                Expr::Identifier(name) => match name.split_once('.') {
                    Some((prefix, column)) => (Some(prefix), column),
                    None => (table, name.as_str()),
                },
                _ => (table, ""),
            };
            if let (Some(table_name), column_name) = (table_name, column_name) {
                if !column_name.is_empty() {
                    if let Some(stats) = stats.column_stats(table_name, column_name) {
                        let base = stats.null_fraction;
                        return if *negated { 1.0 - base } else { base };
                    }
                }
            }
            if *negated { 0.9 } else { 0.1 }
        }
        Expr::BinaryOp { left, op, right } => {
            if let Some(selectivity) = estimate_eq_selectivity(left, right, stats, table) {
                match op {
                    BinaryOperator::Eq => selectivity,
                    BinaryOperator::NotEq => (1.0 - selectivity).max(0.0),
                    BinaryOperator::Lt
                    | BinaryOperator::LtEq
                    | BinaryOperator::Gt
                    | BinaryOperator::GtEq => 0.3,
                    _ => 0.3,
                }
            } else {
                0.3
            }
        }
        _ => 0.5,
    }
}

fn estimate_eq_selectivity(
    left: &corundum_core::ast::Expr,
    right: &corundum_core::ast::Expr,
    stats: &StatsCache,
    table: Option<&str>,
) -> Option<f64> {
    let (ident, literal) = match (left, right) {
        (corundum_core::ast::Expr::Identifier(name), corundum_core::ast::Expr::Literal(_)) => {
            (name, right)
        }
        (corundum_core::ast::Expr::Literal(_), corundum_core::ast::Expr::Identifier(name)) => {
            (name, left)
        }
        _ => return None,
    };
    let _ = literal;
    let (table_name, column_name) = match ident.split_once('.') {
        Some((prefix, column)) => (Some(prefix), column),
        None => (table, ident.as_str()),
    };
    let table_name = table_name?;
    let stats = stats.column_stats(table_name, column_name)?;
    let distinct = stats.distinct_count.max(1.0);
    Some(1.0 / distinct)
}

fn single_table_name(plan: &PhysicalPlan) -> Option<String> {
    match plan {
        PhysicalPlan::TableScan { table } | PhysicalPlan::IndexScan { table, .. } => {
            Some(table.clone())
        }
        PhysicalPlan::Filter { input, .. }
        | PhysicalPlan::Projection { input, .. }
        | PhysicalPlan::Aggregate { input, .. }
        | PhysicalPlan::Distinct { input }
        | PhysicalPlan::TopN { input, .. }
        | PhysicalPlan::Sort { input, .. }
        | PhysicalPlan::Limit { input, .. }
        | PhysicalPlan::Derived { input, .. } => single_table_name(input),
        PhysicalPlan::Join { .. } | PhysicalPlan::Dml { .. } => None,
    }
}
