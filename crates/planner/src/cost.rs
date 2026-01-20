use crate::PhysicalPlan;

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Cost(pub f64);

pub trait CostModel {
    fn cost(&self, plan: &PhysicalPlan) -> Cost;
}

impl PhysicalPlan {
    pub fn node_count(&self) -> usize {
        match self {
            PhysicalPlan::TableScan { .. } => 1,
            PhysicalPlan::IndexScan { .. } => 1,
            PhysicalPlan::Dml { .. } => 1,
            PhysicalPlan::Filter { input, .. } => 1 + input.node_count(),
            PhysicalPlan::Projection { input, .. } => 1 + input.node_count(),
            PhysicalPlan::Join { left, right, .. } => 1 + left.node_count() + right.node_count(),
            PhysicalPlan::Aggregate { input, .. } => 1 + input.node_count(),
            PhysicalPlan::Distinct { input } => 1 + input.node_count(),
            PhysicalPlan::TopN { input, .. } => 1 + input.node_count(),
            PhysicalPlan::Sort { input, .. } => 1 + input.node_count(),
            PhysicalPlan::Limit { input, .. } => 1 + input.node_count(),
        }
    }
}
