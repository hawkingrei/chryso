use chryso_planner::{LogicalPlan, PhysicalPlan};

pub fn logical_fingerprint(plan: &LogicalPlan) -> String {
    plan.explain(0)
}

pub fn physical_fingerprint(plan: &PhysicalPlan) -> String {
    plan.explain(0)
}
