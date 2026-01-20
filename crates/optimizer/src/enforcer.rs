use crate::properties::PhysicalProperties;
use corundum_planner::PhysicalPlan;

pub fn enforce(plan: PhysicalPlan, _required: &PhysicalProperties) -> PhysicalPlan {
    // Placeholder: insert Sort/Exchange nodes to satisfy properties.
    plan
}
