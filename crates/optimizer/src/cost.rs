pub use corundum_planner::cost::{Cost, CostModel};
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

#[cfg(test)]
mod tests {
    use super::{CostModel, UnitCostModel};
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
