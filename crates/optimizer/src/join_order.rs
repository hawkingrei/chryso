use corundum_planner::LogicalPlan;

pub fn enumerate_join_orders(plan: &LogicalPlan) -> Vec<LogicalPlan> {
    vec![plan.clone()]
}
