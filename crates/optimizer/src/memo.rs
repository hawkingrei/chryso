use crate::cost::{Cost, CostModel};
use crate::physical_rules::PhysicalRuleSet;
use crate::{MemoTrace, MemoTraceCandidate, MemoTraceGroup, RuleConfig, RuleContext, SearchBudget};
use chryso_planner::{LogicalPlan, PhysicalPlan};

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

    pub fn trace(&self, physical_rules: &PhysicalRuleSet, cost_model: &dyn CostModel) -> MemoTrace {
        let mut groups = Vec::with_capacity(self.groups.len());
        for (group_id, group) in self.groups.iter().enumerate() {
            let mut candidates = Vec::new();
            for expr in &group.expressions {
                let MemoOperator::Logical(logical) = &expr.operator else {
                    continue;
                };
                let mut inputs = Vec::new();
                let mut missing_input = false;
                for child in &expr.children {
                    if let Some(best) = self.best_physical(*child, cost_model) {
                        inputs.push(best);
                    } else {
                        missing_input = true;
                        break;
                    }
                }
                if missing_input {
                    continue;
                }
                for physical in physical_rules.apply_all(logical, &inputs) {
                    let cost = cost_model.cost(&physical).0;
                    let plan = physical.explain_costed(0, cost_model);
                    candidates.push(MemoTraceCandidate { cost, plan });
                }
            }
            candidates.sort_by(|left, right| {
                left.cost
                    .partial_cmp(&right.cost)
                    .unwrap_or(std::cmp::Ordering::Equal)
                    .then_with(|| left.plan.cmp(&right.plan))
            });
            groups.push(MemoTraceGroup {
                id: group_id,
                candidates,
            });
        }
        MemoTrace { groups }
    }

    pub fn explore(
        &mut self,
        rules: &crate::rules::RuleSet,
        rule_config: &RuleConfig,
        budget: &SearchBudget,
    ) {
        let max_rewrites = budget.max_rewrites.unwrap_or(usize::MAX);
        let mut new_exprs = Vec::new();
        let mut rewrites = 0usize;
        // RuleContext tracks side-channel information (e.g., literal conflicts) while exploring.
        let mut rule_ctx = RuleContext::default();
        for group in &self.groups {
            for expr in &group.expressions {
                if let MemoOperator::Logical(plan) = &expr.operator {
                    for rule in rules.iter() {
                        if rewrites >= max_rewrites {
                            break;
                        }
                        if !rule_config.is_enabled(rule.name()) {
                            continue;
                        }
                        for rewritten in rule.apply(plan, &mut rule_ctx) {
                            if rewrites >= max_rewrites {
                                break;
                            }
                            new_exprs.push(GroupExpr {
                                operator: MemoOperator::Logical(rewritten),
                                children: expr.children.clone(),
                            });
                            rewrites += 1;
                        }
                    }
                }
            }
            if rewrites >= max_rewrites {
                break;
            }
        }
        for expr in new_exprs {
            if self.groups.len() >= budget.max_groups.unwrap_or(usize::MAX) {
                break;
            }
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
            LogicalPlan::Derived { input, .. } => {
                let child_group = memo.insert(input);
                Self {
                    operator: MemoOperator::Logical(plan.clone()),
                    children: vec![child_group],
                }
            }
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
        LogicalPlan::Derived {
            input,
            alias,
            column_aliases,
        } => PhysicalPlan::Derived {
            input: Box::new(logical_to_physical(input, memo)),
            alias: alias.clone(),
            column_aliases: column_aliases.clone(),
        },
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
            algorithm: chryso_planner::JoinAlgorithm::Hash,
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
    use crate::rules::{RemoveTrueFilter, RuleSet};
    use crate::{RuleConfig, SearchBudget};
    use chryso_planner::LogicalPlan;

    #[test]
    fn memo_inserts_child_groups() {
        let plan = LogicalPlan::Filter {
            predicate: chryso_core::ast::Expr::Identifier("x".to_string()),
            input: Box::new(LogicalPlan::Scan {
                table: "users".to_string(),
            }),
        };
        let mut memo = Memo::new();
        memo.insert(&plan);
        assert!(memo.group_count() >= 2);
    }

    #[test]
    fn memo_respects_max_rewrites_budget() {
        let plan = LogicalPlan::Filter {
            predicate: chryso_core::ast::Expr::Literal(chryso_core::ast::Literal::Bool(true)),
            input: Box::new(LogicalPlan::Scan {
                table: "users".to_string(),
            }),
        };
        let rules = RuleSet::new().with_rule(RemoveTrueFilter);
        let mut memo = Memo::new();
        memo.insert(&plan);
        let initial_groups = memo.group_count();
        let budget = SearchBudget {
            max_groups: None,
            max_rewrites: Some(0),
        };
        memo.explore(&rules, &RuleConfig::default(), &budget);
        assert_eq!(memo.group_count(), initial_groups);
    }

    #[test]
    fn memo_respects_max_groups_budget() {
        let plan = LogicalPlan::Filter {
            predicate: chryso_core::ast::Expr::Literal(chryso_core::ast::Literal::Bool(true)),
            input: Box::new(LogicalPlan::Scan {
                table: "users".to_string(),
            }),
        };
        let rules = RuleSet::new().with_rule(RemoveTrueFilter);
        let mut memo = Memo::new();
        memo.insert(&plan);
        let initial_groups = memo.group_count();
        let budget = SearchBudget {
            max_groups: Some(initial_groups),
            max_rewrites: None,
        };
        memo.explore(&rules, &RuleConfig::default(), &budget);
        assert_eq!(memo.group_count(), initial_groups);
    }
}
