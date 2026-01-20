use crate::cost::{Cost, CostModel};
use corundum_planner::{LogicalPlan, PhysicalPlan};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct GroupId(usize);

#[derive(Debug)]
pub struct Memo {
    groups: Vec<Group>,
}

impl Memo {
    pub fn new() -> Self {
        Self { groups: Vec::new() }
    }

    pub fn insert(&mut self, plan: &LogicalPlan) -> GroupId {
        let expr = GroupExpr::from_logical(plan, self);
        let group_id = GroupId(self.groups.len());
        self.groups.push(Group {
            expressions: vec![expr],
        });
        group_id
    }

    pub fn best_physical(&self, root: GroupId, cost_model: &dyn CostModel) -> Option<PhysicalPlan> {
        let group = self.groups.get(root.0)?;
        let mut best: Option<(Cost, PhysicalPlan)> = None;
        for expr in &group.expressions {
            if let Some(physical) = expr.to_physical(self) {
                let cost = cost_model.cost(&physical);
                if best.as_ref().map(|(c, _)| cost.0 < c.0).unwrap_or(true) {
                    best = Some((cost, physical));
                }
            }
        }
        best.map(|(_, plan)| plan)
    }

    pub fn explore(&mut self, rules: &crate::rules::RuleSet) {
        let mut new_exprs = Vec::new();
        for group in &self.groups {
            for expr in &group.expressions {
                if let MemoOperator::Logical(plan) = &expr.operator {
                    for rewritten in rules.apply_all(plan) {
                        new_exprs.push(GroupExpr {
                            operator: MemoOperator::Logical(rewritten),
                            children: expr.children.clone(),
                        });
                    }
                }
            }
        }
        for expr in new_exprs {
            self.groups.push(Group {
                expressions: vec![expr],
            });
        }
    }

    #[cfg(test)]
    pub fn group_count(&self) -> usize {
        self.groups.len()
    }
}

#[derive(Debug)]
pub struct Group {
    expressions: Vec<GroupExpr>,
}

#[derive(Debug)]
pub struct GroupExpr {
    operator: MemoOperator,
    children: Vec<GroupId>,
}

impl GroupExpr {
    pub fn from_logical(plan: &LogicalPlan, memo: &mut Memo) -> Self {
        match plan {
            LogicalPlan::Scan { .. } => Self {
                operator: MemoOperator::Logical(plan.clone()),
                children: Vec::new(),
            },
            LogicalPlan::IndexScan { .. } => Self {
                operator: MemoOperator::Logical(plan.clone()),
                children: Vec::new(),
            },
            LogicalPlan::Dml { .. } => Self {
                operator: MemoOperator::Logical(plan.clone()),
                children: Vec::new(),
            },
            LogicalPlan::Filter { input, .. } => {
                let child_group = memo.insert(input);
                Self {
                    operator: MemoOperator::Logical(plan.clone()),
                    children: vec![child_group],
                }
            }
            LogicalPlan::Projection { input, .. } => {
                let child_group = memo.insert(input);
                Self {
                    operator: MemoOperator::Logical(plan.clone()),
                    children: vec![child_group],
                }
            }
            LogicalPlan::Join { left, right, .. } => {
                let left_group = memo.insert(left);
                let right_group = memo.insert(right);
                Self {
                    operator: MemoOperator::Logical(plan.clone()),
                    children: vec![left_group, right_group],
                }
            }
            LogicalPlan::Aggregate { input, .. } => {
                let child_group = memo.insert(input);
                Self {
                    operator: MemoOperator::Logical(plan.clone()),
                    children: vec![child_group],
                }
            }
            LogicalPlan::Distinct { input } => {
                let child_group = memo.insert(input);
                Self {
                    operator: MemoOperator::Logical(plan.clone()),
                    children: vec![child_group],
                }
            }
            LogicalPlan::TopN { input, .. } => {
                let child_group = memo.insert(input);
                Self {
                    operator: MemoOperator::Logical(plan.clone()),
                    children: vec![child_group],
                }
            }
            LogicalPlan::Sort { input, .. } => {
                let child_group = memo.insert(input);
                Self {
                    operator: MemoOperator::Logical(plan.clone()),
                    children: vec![child_group],
                }
            }
            LogicalPlan::Limit { input, .. } => {
                let child_group = memo.insert(input);
                Self {
                    operator: MemoOperator::Logical(plan.clone()),
                    children: vec![child_group],
                }
            }
        }
    }

    pub fn to_physical(&self, memo: &Memo) -> Option<PhysicalPlan> {
        match &self.operator {
            MemoOperator::Logical(plan) => Some(logical_to_physical(plan, memo)),
            MemoOperator::Physical(plan) => Some(plan.clone()),
        }
    }
}

#[derive(Debug, Clone)]
pub enum MemoOperator {
    Logical(LogicalPlan),
    Physical(PhysicalPlan),
}

fn logical_to_physical(logical: &LogicalPlan, memo: &Memo) -> PhysicalPlan {
    match logical {
        LogicalPlan::Scan { table } => PhysicalPlan::TableScan {
            table: table.clone(),
        },
        LogicalPlan::IndexScan {
            table,
            index,
            predicate,
        } => PhysicalPlan::IndexScan {
            table: table.clone(),
            index: index.clone(),
            predicate: predicate.clone(),
        },
        LogicalPlan::Dml { sql } => PhysicalPlan::Dml { sql: sql.clone() },
        LogicalPlan::Filter { predicate, input } => PhysicalPlan::Filter {
            predicate: predicate.clone(),
            input: Box::new(logical_to_physical(input, memo)),
        },
        LogicalPlan::Projection { exprs, input } => PhysicalPlan::Projection {
            exprs: exprs.clone(),
            input: Box::new(logical_to_physical(input, memo)),
        },
        LogicalPlan::Join {
            join_type,
            left,
            right,
            on,
        } => PhysicalPlan::Join {
            join_type: *join_type,
            algorithm: corundum_planner::JoinAlgorithm::Hash,
            left: Box::new(logical_to_physical(left, memo)),
            right: Box::new(logical_to_physical(right, memo)),
            on: on.clone(),
        },
        LogicalPlan::Aggregate {
            group_exprs,
            aggr_exprs,
            input,
        } => PhysicalPlan::Aggregate {
            group_exprs: group_exprs.clone(),
            aggr_exprs: aggr_exprs.clone(),
            input: Box::new(logical_to_physical(input, memo)),
        },
        LogicalPlan::Distinct { input } => PhysicalPlan::Distinct {
            input: Box::new(logical_to_physical(input, memo)),
        },
        LogicalPlan::TopN {
            order_by,
            limit,
            input,
        } => PhysicalPlan::TopN {
            order_by: order_by.clone(),
            limit: *limit,
            input: Box::new(logical_to_physical(input, memo)),
        },
        LogicalPlan::Sort { order_by, input } => PhysicalPlan::Sort {
            order_by: order_by.clone(),
            input: Box::new(logical_to_physical(input, memo)),
        },
        LogicalPlan::Limit {
            limit,
            offset,
            input,
        } => PhysicalPlan::Limit {
            limit: *limit,
            offset: *offset,
            input: Box::new(logical_to_physical(input, memo)),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::Memo;
    use corundum_planner::LogicalPlan;

    #[test]
    fn memo_inserts_child_groups() {
        let plan = LogicalPlan::Filter {
            predicate: corundum_core::ast::Expr::Identifier("x".to_string()),
            input: Box::new(LogicalPlan::Scan {
                table: "users".to_string(),
            }),
        };
        let mut memo = Memo::new();
        memo.insert(&plan);
        assert!(memo.group_count() >= 2);
    }
}
