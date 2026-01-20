use corundum_metadata::StatsCache;
use crate::cost::{CostModel, UnitCostModel};
use crate::memo::Memo;
use crate::rules::RuleSet;
use corundum_planner::{LogicalPlan, PhysicalPlan};

pub mod cost;
pub mod estimation;
pub mod enforcer;
pub mod column_prune;
pub mod join_order;
pub mod stats_collect;
pub mod memo;
pub mod physical_rules;
pub mod properties;
pub mod rules;
pub mod subquery;
pub mod expr_rewrite;

#[derive(Debug)]
pub struct OptimizerTrace {
    pub applied_rules: Vec<String>,
    pub stats_loaded: Vec<String>,
}

impl OptimizerTrace {
    pub fn new() -> Self {
        Self {
            applied_rules: Vec::new(),
            stats_loaded: Vec::new(),
        }
    }
}

pub struct OptimizerConfig {
    pub enable_cascades: bool,
    pub enable_properties: bool,
    pub rules: RuleSet,
    pub trace: bool,
    pub debug_rules: bool,
    pub stats_provider: Option<std::sync::Arc<dyn corundum_metadata::StatsProvider>>,
}

impl std::fmt::Debug for OptimizerConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OptimizerConfig")
            .field("enable_cascades", &self.enable_cascades)
            .field("enable_properties", &self.enable_properties)
            .field("rules", &self.rules)
            .field("trace", &self.trace)
            .field("debug_rules", &self.debug_rules)
            .field("stats_provider", &self.stats_provider.is_some())
            .finish()
    }
}

impl Default for OptimizerConfig {
    fn default() -> Self {
        Self {
            enable_cascades: true,
            enable_properties: true,
            rules: RuleSet::default(),
            trace: false,
            debug_rules: false,
            stats_provider: None,
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

    pub fn optimize(&self, logical: &LogicalPlan, stats: &mut StatsCache) -> PhysicalPlan {
        let _ = ensure_stats(logical, stats, &self.config);
        let logical = crate::expr_rewrite::rewrite_plan(logical);
        let logical = crate::column_prune::prune_plan(&logical);
        if self.config.enable_cascades {
            optimize_with_cascades(&logical, &self.config, stats).0
        } else {
            logical_to_physical(&logical)
        }
    }

    pub fn optimize_with_trace(
        &self,
        logical: &LogicalPlan,
        stats: &mut StatsCache,
    ) -> (PhysicalPlan, OptimizerTrace) {
        let loaded = ensure_stats(logical, stats, &self.config).unwrap_or_default();
        let logical = crate::expr_rewrite::rewrite_plan(logical);
        let logical = crate::column_prune::prune_plan(&logical);
        if self.config.enable_cascades {
            let (plan, mut trace) = optimize_with_cascades(&logical, &self.config, stats);
            trace.stats_loaded = loaded;
            (plan, trace)
        } else {
            let mut trace = OptimizerTrace::new();
            trace.stats_loaded = loaded;
            (logical_to_physical(&logical), trace)
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
    let logical = crate::expr_rewrite::rewrite_plan(&logical);
    let candidates = crate::join_order::enumerate_join_orders(&logical, _stats);
    let mut memo = Memo::new();
    let root = memo.insert(candidates.first().unwrap_or(&logical));
    memo.explore(&config.rules);
    let cost_model: Box<dyn CostModel> = if _stats.is_empty() {
        Box::new(UnitCostModel)
    } else {
        Box::new(cost::StatsCostModel::new(_stats))
    };
    let mut best = memo
        .best_physical(root, cost_model.as_ref())
        .unwrap_or_else(|| logical_to_physical(&logical));
    if config.enable_properties {
        let required = crate::properties::PhysicalProperties::default();
        best = crate::enforcer::enforce(best, &required);
    }
    (best, trace)
}

fn ensure_stats(
    logical: &LogicalPlan,
    stats: &mut StatsCache,
    config: &OptimizerConfig,
) -> corundum_core::CorundumResult<Vec<String>> {
    let Some(provider) = &config.stats_provider else {
        return Ok(Vec::new());
    };
    let requirements = crate::stats_collect::collect_requirements(logical);
    let mut missing_tables = Vec::new();
    for table in &requirements.tables {
        if stats.table_stats(table).is_none() {
            missing_tables.push(table.clone());
        }
    }
    let mut missing_columns = Vec::new();
    for (table, column) in &requirements.columns {
        if stats.column_stats(table, column).is_none() {
            missing_columns.push((table.clone(), column.clone()));
        }
    }
    if missing_tables.is_empty() && missing_columns.is_empty() {
        return Ok(Vec::new());
    }
    provider.load_stats(&missing_tables, &missing_columns, stats)?;
    Ok(missing_tables)
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
    let rewritten = match rewritten {
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
    };
    let mut final_plan = rewritten.clone();
    for rule in rules.iter() {
        let alternatives = rule.apply(&final_plan);
        if !alternatives.is_empty() {
            final_plan = alternatives.last().cloned().unwrap_or(final_plan);
        }
    }
    final_plan
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
        .min_by(|left, right| {
            cost_model
                .cost(left)
                .0
                .partial_cmp(&cost_model.cost(right).0)
                .unwrap()
        })
        .unwrap_or(PhysicalPlan::TableScan {
            table: "unknown".to_string(),
        })
}
