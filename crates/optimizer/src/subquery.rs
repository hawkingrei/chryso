use corundum_planner::LogicalPlan;

pub fn rewrite_correlated_subqueries(plan: &LogicalPlan) -> LogicalPlan {
    plan.clone()
}
