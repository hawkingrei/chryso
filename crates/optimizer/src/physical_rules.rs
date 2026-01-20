use corundum_planner::{LogicalPlan, PhysicalPlan};

pub trait PhysicalRule {
    fn name(&self) -> &str;
    fn apply(&self, logical: &LogicalPlan, inputs: &[PhysicalPlan]) -> Vec<PhysicalPlan>;
}

pub struct PhysicalRuleSet {
    rules: Vec<Box<dyn PhysicalRule + Send + Sync>>,
}

impl PhysicalRuleSet {
    pub fn new() -> Self {
        Self { rules: Vec::new() }
    }

    pub fn with_rule(mut self, rule: impl PhysicalRule + Send + Sync + 'static) -> Self {
        self.rules.push(Box::new(rule));
        self
    }

    pub fn apply_all(&self, logical: &LogicalPlan, inputs: &[PhysicalPlan]) -> Vec<PhysicalPlan> {
        let mut plans = Vec::new();
        for rule in &self.rules {
            plans.extend(rule.apply(logical, inputs));
        }
        plans
    }
}

impl Default for PhysicalRuleSet {
    fn default() -> Self {
        PhysicalRuleSet::new()
            .with_rule(ScanRule)
            .with_rule(IndexScanRule)
            .with_rule(DmlRule)
            .with_rule(FilterRule)
            .with_rule(ProjectionRule)
            .with_rule(JoinRule)
            .with_rule(AggregateRule)
            .with_rule(DistinctRule)
            .with_rule(TopNRule)
            .with_rule(SortRule)
            .with_rule(LimitRule)
    }
}

pub struct ScanRule;

impl PhysicalRule for ScanRule {
    fn name(&self) -> &str {
        "scan_rule"
    }

    fn apply(&self, logical: &LogicalPlan, _inputs: &[PhysicalPlan]) -> Vec<PhysicalPlan> {
        let LogicalPlan::Scan { table } = logical else {
            return Vec::new();
        };
        vec![PhysicalPlan::TableScan {
            table: table.clone(),
        }]
    }
}

pub struct IndexScanRule;

impl PhysicalRule for IndexScanRule {
    fn name(&self) -> &str {
        "index_scan_rule"
    }

    fn apply(&self, logical: &LogicalPlan, _inputs: &[PhysicalPlan]) -> Vec<PhysicalPlan> {
        let LogicalPlan::IndexScan {
            table,
            index,
            predicate,
        } = logical
        else {
            return Vec::new();
        };
        vec![PhysicalPlan::IndexScan {
            table: table.clone(),
            index: index.clone(),
            predicate: predicate.clone(),
        }]
    }
}

pub struct DmlRule;

impl PhysicalRule for DmlRule {
    fn name(&self) -> &str {
        "dml_rule"
    }

    fn apply(&self, logical: &LogicalPlan, _inputs: &[PhysicalPlan]) -> Vec<PhysicalPlan> {
        let LogicalPlan::Dml { sql } = logical else {
            return Vec::new();
        };
        vec![PhysicalPlan::Dml { sql: sql.clone() }]
    }
}

pub struct FilterRule;

impl PhysicalRule for FilterRule {
    fn name(&self) -> &str {
        "filter_rule"
    }

    fn apply(&self, logical: &LogicalPlan, inputs: &[PhysicalPlan]) -> Vec<PhysicalPlan> {
        let LogicalPlan::Filter { predicate, .. } = logical else {
            return Vec::new();
        };
        let Some(input) = inputs.first() else {
            return Vec::new();
        };
        vec![PhysicalPlan::Filter {
            predicate: predicate.clone(),
            input: Box::new(input.clone()),
        }]
    }
}

pub struct ProjectionRule;

impl PhysicalRule for ProjectionRule {
    fn name(&self) -> &str {
        "projection_rule"
    }

    fn apply(&self, logical: &LogicalPlan, inputs: &[PhysicalPlan]) -> Vec<PhysicalPlan> {
        let LogicalPlan::Projection { exprs, .. } = logical else {
            return Vec::new();
        };
        let Some(input) = inputs.first() else {
            return Vec::new();
        };
        vec![PhysicalPlan::Projection {
            exprs: exprs.clone(),
            input: Box::new(input.clone()),
        }]
    }
}

pub struct JoinRule;

impl PhysicalRule for JoinRule {
    fn name(&self) -> &str {
        "join_rule"
    }

    fn apply(&self, logical: &LogicalPlan, inputs: &[PhysicalPlan]) -> Vec<PhysicalPlan> {
        let LogicalPlan::Join {
            join_type, on, ..
        } = logical
        else {
            return Vec::new();
        };
        if inputs.len() < 2 {
            return Vec::new();
        }
        vec![
            PhysicalPlan::Join {
                join_type: *join_type,
                algorithm: corundum_planner::JoinAlgorithm::Hash,
                left: Box::new(inputs[0].clone()),
                right: Box::new(inputs[1].clone()),
                on: on.clone(),
            },
            PhysicalPlan::Join {
            join_type: *join_type,
            algorithm: corundum_planner::JoinAlgorithm::NestedLoop,
            left: Box::new(inputs[0].clone()),
            right: Box::new(inputs[1].clone()),
            on: on.clone(),
        },
        ]
    }
}

pub struct AggregateRule;

impl PhysicalRule for AggregateRule {
    fn name(&self) -> &str {
        "aggregate_rule"
    }

    fn apply(&self, logical: &LogicalPlan, inputs: &[PhysicalPlan]) -> Vec<PhysicalPlan> {
        let LogicalPlan::Aggregate {
            group_exprs,
            aggr_exprs,
            ..
        } = logical
        else {
            return Vec::new();
        };
        let Some(input) = inputs.first() else {
            return Vec::new();
        };
        vec![PhysicalPlan::Aggregate {
            group_exprs: group_exprs.clone(),
            aggr_exprs: aggr_exprs.clone(),
            input: Box::new(input.clone()),
        }]
    }
}

pub struct DistinctRule;

impl PhysicalRule for DistinctRule {
    fn name(&self) -> &str {
        "distinct_rule"
    }

    fn apply(&self, logical: &LogicalPlan, inputs: &[PhysicalPlan]) -> Vec<PhysicalPlan> {
        let LogicalPlan::Distinct { .. } = logical else {
            return Vec::new();
        };
        let Some(input) = inputs.first() else {
            return Vec::new();
        };
        vec![PhysicalPlan::Distinct {
            input: Box::new(input.clone()),
        }]
    }
}

pub struct SortRule;

impl PhysicalRule for SortRule {
    fn name(&self) -> &str {
        "sort_rule"
    }

    fn apply(&self, logical: &LogicalPlan, inputs: &[PhysicalPlan]) -> Vec<PhysicalPlan> {
        let LogicalPlan::Sort { order_by, .. } = logical else {
            return Vec::new();
        };
        let Some(input) = inputs.first() else {
            return Vec::new();
        };
        vec![PhysicalPlan::Sort {
            order_by: order_by.clone(),
            input: Box::new(input.clone()),
        }]
    }
}

pub struct TopNRule;

impl PhysicalRule for TopNRule {
    fn name(&self) -> &str {
        "topn_rule"
    }

    fn apply(&self, logical: &LogicalPlan, inputs: &[PhysicalPlan]) -> Vec<PhysicalPlan> {
        let LogicalPlan::TopN { order_by, limit, .. } = logical else {
            return Vec::new();
        };
        let Some(input) = inputs.first() else {
            return Vec::new();
        };
        vec![PhysicalPlan::TopN {
            order_by: order_by.clone(),
            limit: *limit,
            input: Box::new(input.clone()),
        }]
    }
}

pub struct LimitRule;

impl PhysicalRule for LimitRule {
    fn name(&self) -> &str {
        "limit_rule"
    }

    fn apply(&self, logical: &LogicalPlan, inputs: &[PhysicalPlan]) -> Vec<PhysicalPlan> {
        let LogicalPlan::Limit { limit, offset, .. } = logical else {
            return Vec::new();
        };
        let Some(input) = inputs.first() else {
            return Vec::new();
        };
        vec![PhysicalPlan::Limit {
            limit: *limit,
            offset: *offset,
            input: Box::new(input.clone()),
        }]
    }
}

#[cfg(test)]
mod tests {
    use super::{JoinRule, PhysicalRule};
    use corundum_planner::{LogicalPlan, PhysicalPlan};

    #[test]
    fn join_rule_produces_two_algorithms() {
        let logical = LogicalPlan::Join {
            join_type: corundum_core::ast::JoinType::Inner,
            left: Box::new(LogicalPlan::Scan {
                table: "t1".to_string(),
            }),
            right: Box::new(LogicalPlan::Scan {
                table: "t2".to_string(),
            }),
            on: corundum_core::ast::Expr::Identifier("t1.id = t2.id".to_string()),
        };
        let inputs = vec![
            PhysicalPlan::TableScan {
                table: "t1".to_string(),
            },
            PhysicalPlan::TableScan {
                table: "t2".to_string(),
            },
        ];
        let rule = JoinRule;
        let plans = rule.apply(&logical, &inputs);
        assert_eq!(plans.len(), 2);
    }
}
