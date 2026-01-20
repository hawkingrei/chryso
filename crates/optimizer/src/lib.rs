use corundum_metadata::StatsCache;
use crate::cost::{CostModel, UnitCostModel};
use crate::memo::Memo;
use crate::rules::RuleSet;
use corundum_planner::{LogicalPlan, PhysicalPlan};

pub mod cost;
pub mod estimation;
pub mod enforcer;
pub mod join_order;
pub mod memo;
pub mod physical_rules;
pub mod properties;
pub mod rules;
pub mod subquery;

#[derive(Debug)]
pub struct OptimizerTrace {
    pub applied_rules: Vec<String>,
}

impl OptimizerTrace {
    pub fn new() -> Self {
        Self {
            applied_rules: Vec::new(),
        }
    }
}

#[derive(Debug)]
pub struct OptimizerConfig {
    pub enable_cascades: bool,
    pub enable_properties: bool,
    pub rules: RuleSet,
    pub trace: bool,
    pub debug_rules: bool,
}

impl Default for OptimizerConfig {
    fn default() -> Self {
        Self {
            enable_cascades: true,
            enable_properties: true,
            rules: RuleSet::default(),
            trace: false,
            debug_rules: false,
        }
    }
}

pub struct CascadesOptimizer {
    config: OptimizerConfig,
}

impl CascadesOptimizer {
    pub fn new(config: OptimizerConfig) -> Self {
        Self { config }
    }

    pub fn optimize(&self, logical: &LogicalPlan, stats: &StatsCache) -> PhysicalPlan {
        if self.config.enable_cascades {
            optimize_with_cascades(logical, &self.config, stats).0
        } else {
            logical_to_physical(logical)
        }
    }

    pub fn optimize_with_trace(
        &self,
        logical: &LogicalPlan,
        stats: &StatsCache,
    ) -> (PhysicalPlan, OptimizerTrace) {
        if self.config.enable_cascades {
            optimize_with_cascades(logical, &self.config, stats)
        } else {
            (logical_to_physical(logical), OptimizerTrace::new())
        }
    }
}

fn optimize_with_cascades(
    logical: &LogicalPlan,
    config: &OptimizerConfig,
    _stats: &StatsCache,
) -> (PhysicalPlan, OptimizerTrace) {
    let mut trace = OptimizerTrace::new();
    let logical = apply_rules_recursive(logical, &config.rules, &mut trace, config.debug_rules);
    let logical = crate::subquery::rewrite_correlated_subqueries(&logical);
    let candidates = crate::join_order::enumerate_join_orders(&logical);
    let mut memo = Memo::new();
    let root = memo.insert(candidates.first().unwrap_or(&logical));
    memo.explore(&config.rules);
    let cost_model = UnitCostModel;
    let mut best = memo
        .best_physical(root, &cost_model)
        .unwrap_or_else(|| logical_to_physical(&logical));
    if config.enable_properties {
        let required = crate::properties::PhysicalProperties::default();
        best = crate::enforcer::enforce(best, &required);
    }
    (best, trace)
}

fn apply_rules_recursive(
    plan: &LogicalPlan,
    rules: &RuleSet,
    trace: &mut OptimizerTrace,
    debug_rules: bool,
) -> LogicalPlan {
    let mut rewritten = plan.clone();
    let mut matched = Vec::new();
    for rule in rules.iter() {
        let alternatives = rule.apply(&rewritten);
        if !alternatives.is_empty() {
            matched.push(rule.name().to_string());
            rewritten = alternatives.last().cloned().unwrap_or(rewritten);
        }
    }
    if debug_rules {
        trace.applied_rules.extend(matched);
    }
    match rewritten {
        LogicalPlan::Filter { predicate, input } => LogicalPlan::Filter {
            predicate,
            input: Box::new(apply_rules_recursive(input.as_ref(), rules, trace, debug_rules)),
        },
        LogicalPlan::Projection { exprs, input } => LogicalPlan::Projection {
            exprs,
            input: Box::new(apply_rules_recursive(input.as_ref(), rules, trace, debug_rules)),
        },
        LogicalPlan::Join {
            join_type,
            left,
            right,
            on,
        } => LogicalPlan::Join {
            join_type,
            left: Box::new(apply_rules_recursive(left.as_ref(), rules, trace, debug_rules)),
            right: Box::new(apply_rules_recursive(right.as_ref(), rules, trace, debug_rules)),
            on,
        },
        LogicalPlan::Aggregate {
            group_exprs,
            aggr_exprs,
            input,
        } => LogicalPlan::Aggregate {
            group_exprs,
            aggr_exprs,
            input: Box::new(apply_rules_recursive(input.as_ref(), rules, trace, debug_rules)),
        },
        LogicalPlan::Distinct { input } => LogicalPlan::Distinct {
            input: Box::new(apply_rules_recursive(input.as_ref(), rules, trace, debug_rules)),
        },
        LogicalPlan::TopN {
            order_by,
            limit,
            input,
        } => LogicalPlan::TopN {
            order_by,
            limit,
            input: Box::new(apply_rules_recursive(input.as_ref(), rules, trace, debug_rules)),
        },
        LogicalPlan::Sort { order_by, input } => LogicalPlan::Sort {
            order_by,
            input: Box::new(apply_rules_recursive(input.as_ref(), rules, trace, debug_rules)),
        },
        LogicalPlan::Limit {
            limit,
            offset,
            input,
        } => LogicalPlan::Limit {
            limit,
            offset,
            input: Box::new(apply_rules_recursive(input.as_ref(), rules, trace, debug_rules)),
        },
        other => other,
    }
}

fn logical_to_physical(logical: &LogicalPlan) -> PhysicalPlan {
    let children = match logical {
        LogicalPlan::Scan { .. } => Vec::new(),
        LogicalPlan::IndexScan { .. } => Vec::new(),
        LogicalPlan::Dml { .. } => Vec::new(),
        LogicalPlan::Filter { input, .. } => vec![logical_to_physical(input)],
        LogicalPlan::Projection { input, .. } => vec![logical_to_physical(input)],
        LogicalPlan::Join { left, right, .. } => {
            vec![logical_to_physical(left), logical_to_physical(right)]
        }
        LogicalPlan::Aggregate { input, .. } => vec![logical_to_physical(input)],
        LogicalPlan::Distinct { input } => vec![logical_to_physical(input)],
        LogicalPlan::TopN { input, .. } => vec![logical_to_physical(input)],
        LogicalPlan::Sort { input, .. } => vec![logical_to_physical(input)],
        LogicalPlan::Limit { input, .. } => vec![logical_to_physical(input)],
    };
    let rules = crate::physical_rules::PhysicalRuleSet::default();
    let candidates = rules.apply_all(logical, &children);
    let cost_model = UnitCostModel;
    candidates
        .into_iter()
        .min_by(|left, right| cost_model.cost(left).0.partial_cmp(&cost_model.cost(right).0).unwrap())
        .unwrap_or(PhysicalPlan::TableScan {
            table: "unknown".to_string(),
        })
}
