use crate::cost::{CostModel, UnitCostModel};
use crate::memo::Memo;
use crate::rules::{RuleContext, RuleSet};
use chryso_metadata::StatsCache;
use chryso_planner::{LogicalPlan, PhysicalPlan};
use std::collections::HashSet;

pub mod column_prune;
pub mod cost;
pub mod cost_profile;
pub mod enforcer;
pub mod estimation;
pub mod expr_rewrite;
pub mod join_order;
pub mod memo;
pub mod physical_rules;
pub mod plan_fingerprint;
pub mod properties;
pub mod rules;
pub mod stats_collect;
pub mod subquery;
pub mod utils;

pub use cost::CostModelConfig;
pub use cost_profile::CostProfile;
pub use plan_fingerprint::{logical_fingerprint, physical_fingerprint};

#[derive(Debug)]
pub struct OptimizerTrace {
    pub applied_rules: Vec<String>,
    pub stats_loaded: Vec<String>,
    pub conflicting_literals: Vec<(String, String)>,
}

impl OptimizerTrace {
    pub fn new() -> Self {
        Self {
            applied_rules: Vec::new(),
            stats_loaded: Vec::new(),
            conflicting_literals: Vec::new(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct MemoTrace {
    pub groups: Vec<MemoTraceGroup>,
}

#[derive(Debug, Clone)]
pub struct MemoTraceGroup {
    pub id: usize,
    pub candidates: Vec<MemoTraceCandidate>,
}

#[derive(Debug, Clone)]
pub struct MemoTraceCandidate {
    pub cost: f64,
    pub plan: String,
}

impl MemoTrace {
    pub fn format_full(&self) -> String {
        format_memo_trace(self, false)
    }

    pub fn format_best_only(&self) -> String {
        format_memo_trace(self, true)
    }
}

fn format_memo_trace(trace: &MemoTrace, best_only: bool) -> String {
    use std::fmt::Write;

    let mut output = String::new();
    for group in &trace.groups {
        let _ = writeln!(
            &mut output,
            "group={} candidates={}",
            group.id,
            group.candidates.len()
        );
        if best_only {
            if let Some(best) = group.candidates.first() {
                write_candidate(&mut output, best);
            }
            continue;
        }
        for candidate in &group.candidates {
            write_candidate(&mut output, candidate);
        }
    }
    output
}

fn write_candidate(output: &mut String, candidate: &MemoTraceCandidate) {
    use std::fmt::Write;

    let _ = writeln!(output, "  cost={:.3}", candidate.cost);
    for line in candidate.plan.lines() {
        let _ = writeln!(output, "    {line}");
    }
}

pub struct OptimizerConfig {
    pub enable_cascades: bool,
    pub enable_properties: bool,
    pub rules: RuleSet,
    pub rule_config: RuleConfig,
    pub search_budget: SearchBudget,
    pub trace: bool,
    pub debug_rules: bool,
    pub stats_provider: Option<std::sync::Arc<dyn chryso_metadata::StatsProvider>>,
    pub cost_config: Option<CostModelConfig>,
    pub system_params: Option<std::sync::Arc<chryso_core::system_params::SystemParamRegistry>>,
    pub tenant_id: Option<String>,
}

impl std::fmt::Debug for OptimizerConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OptimizerConfig")
            .field("enable_cascades", &self.enable_cascades)
            .field("enable_properties", &self.enable_properties)
            .field("rules", &self.rules)
            .field("rule_config", &self.rule_config)
            .field("search_budget", &self.search_budget)
            .field("trace", &self.trace)
            .field("debug_rules", &self.debug_rules)
            .field("stats_provider", &self.stats_provider.is_some())
            .field("cost_config", &self.cost_config.is_some())
            .field("system_params", &self.system_params.is_some())
            .field("tenant_id", &self.tenant_id)
            .finish()
    }
}

#[derive(Clone, Debug)]
pub struct RuleConfig {
    pub enabled_rules: Option<HashSet<String>>,
    pub disabled_rules: HashSet<String>,
}

impl RuleConfig {
    pub fn is_enabled(&self, name: &str) -> bool {
        if let Some(enabled) = &self.enabled_rules {
            return enabled.contains(name);
        }
        !self.disabled_rules.contains(name)
    }
}

impl Default for RuleConfig {
    fn default() -> Self {
        Self {
            enabled_rules: None,
            disabled_rules: HashSet::new(),
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct SearchBudget {
    pub max_groups: Option<usize>,
    pub max_rewrites: Option<usize>,
}

#[cfg(test)]
mod tests {
    use super::cost::UnitCostModel;
    use super::{CascadesOptimizer, OptimizerConfig};
    use crate::CostModelConfig;
    use chryso_core::ast::{BinaryOperator, Expr, Literal};
    use chryso_metadata::{StatsCache, StatsSnapshot, type_inference::SimpleTypeInferencer};
    use chryso_parser::{Dialect, ParserConfig, SimpleParser, SqlParser};
    use chryso_planner::{LogicalPlan, PhysicalPlan, PlanBuilder};
    use serde_json;
    use std::collections::HashSet;

    #[test]
    fn explain_with_types_and_costs() {
        let sql = "select sum(amount) from sales group by region";
        let parser = SimpleParser::new(ParserConfig {
            dialect: Dialect::Postgres,
        });
        let stmt = parser.parse(sql).expect("parse");
        let logical = PlanBuilder::build(stmt).expect("plan");
        let typed = logical.explain_typed(0, &SimpleTypeInferencer);
        assert!(typed.contains("LogicalAggregate"));

        let physical = CascadesOptimizer::new(OptimizerConfig::default())
            .optimize(&logical, &mut StatsCache::new());
        let costed = physical.explain_costed(0, &UnitCostModel);
        assert!(costed.contains("cost="));
    }

    #[test]
    fn optimizer_respects_disabled_rule() {
        let logical = LogicalPlan::Filter {
            predicate: Expr::Literal(Literal::Bool(true)),
            input: Box::new(LogicalPlan::Scan {
                table: "t".to_string(),
            }),
        };
        let mut config = OptimizerConfig::default();
        config
            .rule_config
            .disabled_rules
            .insert("remove_true_filter".to_string());
        let plan = CascadesOptimizer::new(config).optimize(&logical, &mut StatsCache::new());
        assert!(matches!(plan, PhysicalPlan::Filter { .. }));
    }

    #[test]
    fn optimizer_respects_enabled_rule_allowlist() {
        let logical = LogicalPlan::Filter {
            predicate: Expr::Literal(Literal::Bool(true)),
            input: Box::new(LogicalPlan::Scan {
                table: "t".to_string(),
            }),
        };
        let mut config = OptimizerConfig::default();
        config.rule_config.enabled_rules =
            Some(HashSet::from_iter([String::from("remove_true_filter")]));
        let plan = CascadesOptimizer::new(config).optimize(&logical, &mut StatsCache::new());
        assert!(matches!(plan, PhysicalPlan::TableScan { .. }));
    }

    #[test]
    fn optimize_with_memo_trace_collects_candidates() {
        let sql = "select * from t";
        let parser = SimpleParser::new(ParserConfig {
            dialect: Dialect::Postgres,
        });
        let stmt = parser.parse(sql).expect("parse");
        let logical = PlanBuilder::build(stmt).expect("plan");
        let (physical, trace) = CascadesOptimizer::new(OptimizerConfig::default())
            .optimize_with_memo_trace(&logical, &mut StatsCache::new());
        assert!(matches!(physical, PhysicalPlan::TableScan { .. }));
        assert!(!trace.groups.is_empty());
        let first = &trace.groups[0];
        assert!(!first.candidates.is_empty());
        let formatted = trace.format_best_only();
        assert!(formatted.contains("group="));
        assert!(formatted.contains("cost="));
    }

    #[test]
    fn rules_reach_fixpoint_for_projection_limit_topn() {
        let sql = "select id from t order by id limit 5";
        let parser = SimpleParser::new(ParserConfig {
            dialect: Dialect::Postgres,
        });
        let stmt = parser.parse(sql).expect("parse");
        let logical = PlanBuilder::build(stmt).expect("plan");
        let (physical, _) = CascadesOptimizer::new(OptimizerConfig::default())
            .optimize_with_trace(&logical, &mut StatsCache::new());
        assert!(!matches!(physical, PhysicalPlan::Sort { .. }));
    }

    #[test]
    fn trace_records_literal_conflicts() {
        let logical = LogicalPlan::Filter {
            predicate: Expr::BinaryOp {
                left: Box::new(Expr::BinaryOp {
                    left: Box::new(Expr::Identifier("a".to_string())),
                    op: BinaryOperator::Eq,
                    right: Box::new(Expr::Literal(Literal::Number(1.0))),
                }),
                op: BinaryOperator::And,
                right: Box::new(Expr::BinaryOp {
                    left: Box::new(Expr::Identifier("a".to_string())),
                    op: BinaryOperator::Eq,
                    right: Box::new(Expr::Literal(Literal::Number(2.0))),
                }),
            },
            input: Box::new(LogicalPlan::Scan {
                table: "t".to_string(),
            }),
        };
        let (_, trace) = CascadesOptimizer::new(OptimizerConfig::default())
            .optimize_with_trace(&logical, &mut StatsCache::new());
        assert!(
            trace
                .conflicting_literals
                .iter()
                .any(|(left, right)| left.contains("a = 1") && right.contains("a = 2")),
            "expected conflict pair, got {:?}",
            trace.conflicting_literals
        );
    }

    #[test]
    fn optimizer_uses_stats_snapshot() {
        let snapshot_json = include_str!("../tests/testdata/tpch_scale1.json");
        let snapshot: StatsSnapshot = serde_json::from_str(snapshot_json).expect("snapshot");
        let mut stats = snapshot.to_cache();
        let sql = "select * from orders";
        let parser = SimpleParser::new(ParserConfig {
            dialect: Dialect::Postgres,
        });
        let stmt = parser.parse(sql).expect("parse");
        let logical = PlanBuilder::build(stmt).expect("plan");
        let (physical, _) = CascadesOptimizer::new(OptimizerConfig::default())
            .optimize_with_trace(&logical, &mut stats);
        assert!(matches!(physical, PhysicalPlan::TableScan { .. }));
    }

    #[test]
    fn optimizer_uses_system_param_override() {
        let registry = chryso_core::system_params::SystemParamRegistry::new();
        registry.set_default_param(
            CostModelConfig::PARAM_FILTER,
            chryso_core::system_params::SystemParamValue::Float(9.0),
        );
        let mut config = OptimizerConfig::default();
        config.system_params = Some(std::sync::Arc::new(registry));
        let base = CostModelConfig::default();
        let updated = base.apply_system_params(
            config.system_params.as_ref().unwrap(),
            config.tenant_id.as_deref(),
        );
        assert_eq!(updated.filter, 9.0);
    }
}

impl Default for OptimizerConfig {
    fn default() -> Self {
        Self {
            enable_cascades: true,
            enable_properties: true,
            rules: RuleSet::default(),
            rule_config: RuleConfig::default(),
            search_budget: SearchBudget::default(),
            trace: false,
            debug_rules: false,
            stats_provider: None,
            cost_config: None,
            system_params: None,
            tenant_id: None,
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

    pub fn optimize_with_memo_trace(
        &self,
        logical: &LogicalPlan,
        stats: &mut StatsCache,
    ) -> (PhysicalPlan, MemoTrace) {
        let _ = ensure_stats(logical, stats, &self.config);
        let logical = crate::expr_rewrite::rewrite_plan(logical);
        let logical = crate::column_prune::prune_plan(&logical);
        if self.config.enable_cascades {
            optimize_with_cascades_memo(&logical, &self.config, stats)
        } else {
            let physical = logical_to_physical(&logical);
            (physical, MemoTrace { groups: Vec::new() })
        }
    }
}

fn optimize_with_cascades(
    logical: &LogicalPlan,
    config: &OptimizerConfig,
    _stats: &StatsCache,
) -> (PhysicalPlan, OptimizerTrace) {
    let mut trace = OptimizerTrace::new();
    // Keep rule side effects (e.g., literal conflicts) explicit and thread-safe.
    let mut rule_ctx = RuleContext::default();
    let logical = apply_rules_fixpoint(
        logical,
        &config.rules,
        &config.rule_config,
        &mut trace,
        &mut rule_ctx,
        config.debug_rules,
    );
    trace
        .conflicting_literals
        .extend(rule_ctx.take_literal_conflicts());
    let logical = crate::subquery::rewrite_correlated_subqueries(&logical);
    let logical = crate::expr_rewrite::rewrite_plan(&logical);
    let candidates = crate::join_order::enumerate_join_orders(&logical, _stats);
    let mut memo = Memo::new();
    let root = memo.insert(candidates.first().unwrap_or(&logical));
    memo.explore(&config.rules, &config.rule_config, &config.search_budget);
    let cost_model = build_cost_model(_stats, config);
    let physical_rules = crate::physical_rules::PhysicalRuleSet::default();
    let mut best = memo
        .best_physical(root, &physical_rules, cost_model.as_ref())
        .unwrap_or_else(|| logical_to_physical(&logical));
    if config.enable_properties {
        let required = crate::properties::PhysicalProperties::default();
        best = crate::enforcer::enforce(best, &required);
    }
    (best, trace)
}

fn optimize_with_cascades_memo(
    logical: &LogicalPlan,
    config: &OptimizerConfig,
    _stats: &StatsCache,
) -> (PhysicalPlan, MemoTrace) {
    let mut rule_ctx = RuleContext::default();
    let logical = apply_rules_fixpoint(
        logical,
        &config.rules,
        &config.rule_config,
        &mut OptimizerTrace::new(),
        &mut rule_ctx,
        config.debug_rules,
    );
    let logical = crate::subquery::rewrite_correlated_subqueries(&logical);
    let logical = crate::expr_rewrite::rewrite_plan(&logical);
    let candidates = crate::join_order::enumerate_join_orders(&logical, _stats);
    let mut memo = Memo::new();
    let root = memo.insert(candidates.first().unwrap_or(&logical));
    memo.explore(&config.rules, &config.rule_config, &config.search_budget);
    let cost_model = build_cost_model(_stats, config);
    let physical_rules = crate::physical_rules::PhysicalRuleSet::default();
    let trace = memo.trace(&physical_rules, cost_model.as_ref());
    let mut best = memo
        .best_physical(root, &physical_rules, cost_model.as_ref())
        .unwrap_or_else(|| logical_to_physical(&logical));
    if config.enable_properties {
        let required = crate::properties::PhysicalProperties::default();
        best = crate::enforcer::enforce(best, &required);
    }
    (best, trace)
}

fn build_cost_model<'a>(
    stats: &'a StatsCache,
    config: &'a OptimizerConfig,
) -> Box<dyn CostModel + 'a> {
    let base = config.cost_config.clone().unwrap_or_default();
    let model_config = match &config.system_params {
        Some(registry) => {
            let tenant = config.tenant_id.as_deref();
            base.apply_system_params(registry, tenant)
        }
        None => base,
    };
    let model_config = if model_config.validate().is_ok() {
        model_config
    } else {
        eprintln!("optimizer: invalid cost config detected; falling back to defaults");
        CostModelConfig::default()
    };
    if stats.is_empty() {
        Box::new(cost::UnitCostModelWithConfig::new(model_config))
    } else {
        Box::new(cost::StatsCostModel::with_config(stats, model_config))
    }
}

fn ensure_stats(
    logical: &LogicalPlan,
    stats: &mut StatsCache,
    config: &OptimizerConfig,
) -> chryso_core::ChrysoResult<Vec<String>> {
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
    rule_config: &RuleConfig,
    trace: &mut OptimizerTrace,
    rule_ctx: &mut RuleContext,
    debug_rules: bool,
) -> LogicalPlan {
    let mut rewritten = plan.clone();
    let mut matched = Vec::new();
    for rule in rules.iter() {
        if !rule_config.is_enabled(rule.name()) {
            continue;
        }
        let alternatives = rule.apply(&rewritten, rule_ctx);
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
            input: Box::new(apply_rules_recursive(
                input.as_ref(),
                rules,
                rule_config,
                trace,
                rule_ctx,
                debug_rules,
            )),
        },
        LogicalPlan::Projection { exprs, input } => LogicalPlan::Projection {
            exprs,
            input: Box::new(apply_rules_recursive(
                input.as_ref(),
                rules,
                rule_config,
                trace,
                rule_ctx,
                debug_rules,
            )),
        },
        LogicalPlan::Join {
            join_type,
            left,
            right,
            on,
        } => LogicalPlan::Join {
            join_type,
            left: Box::new(apply_rules_recursive(
                left.as_ref(),
                rules,
                rule_config,
                trace,
                rule_ctx,
                debug_rules,
            )),
            right: Box::new(apply_rules_recursive(
                right.as_ref(),
                rules,
                rule_config,
                trace,
                rule_ctx,
                debug_rules,
            )),
            on,
        },
        LogicalPlan::Aggregate {
            group_exprs,
            aggr_exprs,
            input,
        } => LogicalPlan::Aggregate {
            group_exprs,
            aggr_exprs,
            input: Box::new(apply_rules_recursive(
                input.as_ref(),
                rules,
                rule_config,
                trace,
                rule_ctx,
                debug_rules,
            )),
        },
        LogicalPlan::Distinct { input } => LogicalPlan::Distinct {
            input: Box::new(apply_rules_recursive(
                input.as_ref(),
                rules,
                rule_config,
                trace,
                rule_ctx,
                debug_rules,
            )),
        },
        LogicalPlan::TopN {
            order_by,
            limit,
            input,
        } => LogicalPlan::TopN {
            order_by,
            limit,
            input: Box::new(apply_rules_recursive(
                input.as_ref(),
                rules,
                rule_config,
                trace,
                rule_ctx,
                debug_rules,
            )),
        },
        LogicalPlan::Sort { order_by, input } => LogicalPlan::Sort {
            order_by,
            input: Box::new(apply_rules_recursive(
                input.as_ref(),
                rules,
                rule_config,
                trace,
                rule_ctx,
                debug_rules,
            )),
        },
        LogicalPlan::Limit {
            limit,
            offset,
            input,
        } => LogicalPlan::Limit {
            limit,
            offset,
            input: Box::new(apply_rules_recursive(
                input.as_ref(),
                rules,
                rule_config,
                trace,
                rule_ctx,
                debug_rules,
            )),
        },
        LogicalPlan::Derived {
            input,
            alias,
            column_aliases,
        } => LogicalPlan::Derived {
            input: Box::new(apply_rules_recursive(
                input.as_ref(),
                rules,
                rule_config,
                trace,
                rule_ctx,
                debug_rules,
            )),
            alias,
            column_aliases,
        },
        other => other,
    };
    let mut final_plan = rewritten.clone();
    for rule in rules.iter() {
        if !rule_config.is_enabled(rule.name()) {
            continue;
        }
        let alternatives = rule.apply(&final_plan, rule_ctx);
        if !alternatives.is_empty() {
            final_plan = alternatives.last().cloned().unwrap_or(final_plan);
        }
    }
    final_plan
}

fn apply_rules_fixpoint(
    plan: &LogicalPlan,
    rules: &RuleSet,
    rule_config: &RuleConfig,
    trace: &mut OptimizerTrace,
    rule_ctx: &mut RuleContext,
    debug_rules: bool,
) -> LogicalPlan {
    const MAX_RULE_PASSES: usize = 8;
    let mut current = plan.clone();
    for _ in 0..MAX_RULE_PASSES {
        let before = logical_plan_fingerprint(&current);
        let next =
            apply_rules_recursive(&current, rules, rule_config, trace, rule_ctx, debug_rules);
        let after = logical_plan_fingerprint(&next);
        if before == after {
            return current;
        }
        current = next;
    }
    current
}

fn logical_plan_fingerprint(plan: &LogicalPlan) -> String {
    plan.explain(0)
}

fn logical_to_physical(logical: &LogicalPlan) -> PhysicalPlan {
    let children = match logical {
        LogicalPlan::Scan { .. } => Vec::new(),
        LogicalPlan::IndexScan { .. } => Vec::new(),
        LogicalPlan::Dml { .. } => Vec::new(),
        LogicalPlan::Derived { input, .. } => vec![logical_to_physical(input)],
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
