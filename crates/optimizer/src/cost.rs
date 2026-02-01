use chryso_metadata::StatsCache;
use chryso_planner::PhysicalPlan;
pub use chryso_planner::cost::{Cost, CostModel};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::fs;
use std::path::Path;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct CostModelConfig {
    pub scan: f64,
    pub filter: f64,
    pub projection: f64,
    pub join: f64,
    pub sort: f64,
    pub aggregate: f64,
    pub limit: f64,
    pub derived: f64,
    pub dml: f64,
    pub join_hash_multiplier: f64,
    pub join_nested_multiplier: f64,
    pub max_cost: f64,
}

impl Default for CostModelConfig {
    fn default() -> Self {
        Self {
            scan: 1.0,
            filter: 0.5,
            projection: 0.1,
            join: 5.0,
            sort: 3.0,
            aggregate: 4.0,
            limit: 0.05,
            derived: 0.1,
            dml: 1.0,
            join_hash_multiplier: 1.0,
            join_nested_multiplier: 5.0,
            max_cost: 1.0e18,
        }
    }
}

impl CostModelConfig {
    pub const PARAM_SCAN: &'static str = "optimizer.cost.scan";
    pub const PARAM_FILTER: &'static str = "optimizer.cost.filter";
    pub const PARAM_PROJECTION: &'static str = "optimizer.cost.projection";
    pub const PARAM_JOIN: &'static str = "optimizer.cost.join";
    pub const PARAM_SORT: &'static str = "optimizer.cost.sort";
    pub const PARAM_AGGREGATE: &'static str = "optimizer.cost.aggregate";
    pub const PARAM_LIMIT: &'static str = "optimizer.cost.limit";
    pub const PARAM_DERIVED: &'static str = "optimizer.cost.derived";
    pub const PARAM_DML: &'static str = "optimizer.cost.dml";
    pub const PARAM_JOIN_HASH_MULTIPLIER: &'static str = "optimizer.cost.join_hash_multiplier";
    pub const PARAM_JOIN_NESTED_MULTIPLIER: &'static str = "optimizer.cost.join_nested_multiplier";
    pub const PARAM_MAX_COST: &'static str = "optimizer.cost.max_cost";

    pub fn load_from_path(path: impl AsRef<Path>) -> chryso_core::error::ChrysoResult<Self> {
        let value: CostModelConfig = load_config_from_path(path, "cost config")?;
        value.validate()?;
        Ok(value)
    }

    pub fn validate(&self) -> chryso_core::error::ChrysoResult<()> {
        let mut invalid = Vec::new();
        for (name, value) in [
            ("scan", self.scan),
            ("filter", self.filter),
            ("projection", self.projection),
            ("join", self.join),
            ("sort", self.sort),
            ("aggregate", self.aggregate),
            ("limit", self.limit),
            ("derived", self.derived),
            ("dml", self.dml),
            ("join_hash_multiplier", self.join_hash_multiplier),
            ("join_nested_multiplier", self.join_nested_multiplier),
            ("max_cost", self.max_cost),
        ] {
            if !value.is_finite() || value <= 0.0 {
                invalid.push(name);
            }
        }
        if invalid.is_empty() {
            Ok(())
        } else {
            Err(chryso_core::error::ChrysoError::new(format!(
                "invalid cost config fields: {}",
                invalid.join(", ")
            )))
        }
    }

    pub fn apply_system_params(
        &self,
        registry: &chryso_core::system_params::SystemParamRegistry,
        tenant: Option<&str>,
    ) -> Self {
        let mut updated = self.clone();
        let apply = |key: &str, target: &mut f64| {
            if let Some(value) = registry.get_f64(tenant, key) {
                if value.is_finite() && value > 0.0 {
                    *target = value;
                }
            }
        };
        apply(Self::PARAM_SCAN, &mut updated.scan);
        apply(Self::PARAM_FILTER, &mut updated.filter);
        apply(Self::PARAM_PROJECTION, &mut updated.projection);
        apply(Self::PARAM_JOIN, &mut updated.join);
        apply(Self::PARAM_SORT, &mut updated.sort);
        apply(Self::PARAM_AGGREGATE, &mut updated.aggregate);
        apply(Self::PARAM_LIMIT, &mut updated.limit);
        apply(Self::PARAM_DERIVED, &mut updated.derived);
        apply(Self::PARAM_DML, &mut updated.dml);
        apply(
            Self::PARAM_JOIN_HASH_MULTIPLIER,
            &mut updated.join_hash_multiplier,
        );
        apply(
            Self::PARAM_JOIN_NESTED_MULTIPLIER,
            &mut updated.join_nested_multiplier,
        );
        apply(Self::PARAM_MAX_COST, &mut updated.max_cost);
        updated
    }
}

pub struct UnitCostModel;

impl CostModel for UnitCostModel {
    fn cost(&self, plan: &PhysicalPlan) -> Cost {
        let default = CostModelConfig::default();
        Cost(total_weight(plan, &default))
    }
}

impl std::fmt::Debug for UnitCostModel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("UnitCostModel")
    }
}

pub struct UnitCostModelWithConfig {
    config: CostModelConfig,
}

impl UnitCostModelWithConfig {
    pub fn new(config: CostModelConfig) -> Self {
        Self { config }
    }
}

impl CostModel for UnitCostModelWithConfig {
    fn cost(&self, plan: &PhysicalPlan) -> Cost {
        Cost(total_weight(plan, &self.config))
    }
}

pub struct StatsCostModel<'a> {
    stats: &'a StatsCache,
    config: CostModelConfig,
}

impl<'a> StatsCostModel<'a> {
    pub fn new(stats: &'a StatsCache) -> Self {
        Self {
            stats,
            config: CostModelConfig::default(),
        }
    }

    pub fn with_config(stats: &'a StatsCache, config: CostModelConfig) -> Self {
        let validated = if config.validate().is_ok() {
            config
        } else {
            CostModelConfig::default()
        };
        Self {
            stats,
            config: validated,
        }
    }
}

impl CostModel for StatsCostModel<'_> {
    fn cost(&self, plan: &PhysicalPlan) -> Cost {
        let mut cost = total_stats_cost(plan, self.stats, &self.config);
        if !cost.is_finite() || cost > self.config.max_cost {
            cost = self.config.max_cost;
        }
        Cost(cost)
    }
}

impl std::fmt::Debug for StatsCostModel<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("StatsCostModel")
    }
}

#[cfg(test)]
mod tests {
    use super::{CostModel, CostModelConfig, StatsCache, StatsCostModel, UnitCostModel};
    use chryso_core::system_params::{SystemParamRegistry, SystemParamValue};
    use chryso_metadata::ColumnStats;
    use chryso_planner::PhysicalPlan;

    #[test]
    fn unit_cost_counts_nodes() {
        let plan = PhysicalPlan::Filter {
            predicate: chryso_core::ast::Expr::Identifier("x".to_string()),
            input: Box::new(PhysicalPlan::TableScan {
                table: "t".to_string(),
            }),
        };
        let cost = UnitCostModel.cost(&plan);
        assert_eq!(cost.0, 1.5);
    }

    #[test]
    fn join_algorithm_costs_differ() {
        let left = PhysicalPlan::TableScan {
            table: "t1".to_string(),
        };
        let right = PhysicalPlan::TableScan {
            table: "t2".to_string(),
        };
        let hash = PhysicalPlan::Join {
            join_type: chryso_core::ast::JoinType::Inner,
            algorithm: chryso_planner::JoinAlgorithm::Hash,
            left: Box::new(left.clone()),
            right: Box::new(right.clone()),
            on: chryso_core::ast::Expr::Identifier("t1.id = t2.id".to_string()),
        };
        let nested = PhysicalPlan::Join {
            join_type: chryso_core::ast::JoinType::Inner,
            algorithm: chryso_planner::JoinAlgorithm::NestedLoop,
            left: Box::new(left),
            right: Box::new(right),
            on: chryso_core::ast::Expr::Identifier("t1.id = t2.id".to_string()),
        };
        let model = UnitCostModel;
        assert!(model.cost(&hash).0 < model.cost(&nested).0);
    }

    #[test]
    fn stats_cost_uses_selectivity() {
        let plan = PhysicalPlan::Filter {
            predicate: chryso_core::ast::Expr::BinaryOp {
                left: Box::new(chryso_core::ast::Expr::Identifier(
                    "sales.region".to_string(),
                )),
                op: chryso_core::ast::BinaryOperator::Eq,
                right: Box::new(chryso_core::ast::Expr::Literal(
                    chryso_core::ast::Literal::String("us".to_string()),
                )),
            },
            input: Box::new(PhysicalPlan::TableScan {
                table: "sales".to_string(),
            }),
        };
        let mut stats = StatsCache::new();
        stats.insert_table_stats("sales", chryso_metadata::TableStats { row_count: 100.0 });
        stats.insert_column_stats(
            "sales",
            "region",
            ColumnStats {
                distinct_count: 50.0,
                null_fraction: 0.0,
            },
        );
        let model = StatsCostModel::new(&stats);
        let selective = model.cost(&plan);

        stats.insert_column_stats(
            "sales",
            "region",
            ColumnStats {
                distinct_count: 1.0,
                null_fraction: 0.0,
            },
        );
        let model = StatsCostModel::new(&stats);
        let non_selective = model.cost(&plan);
        assert!(selective.0 < non_selective.0);
    }

    #[test]
    fn config_validation_rejects_non_positive() {
        let mut config = CostModelConfig::default();
        config.join = 0.0;
        let err = config.validate().expect_err("invalid config");
        assert!(err.to_string().contains("join"));
    }

    #[test]
    fn system_params_override_cost_config() {
        let registry = SystemParamRegistry::new();
        registry.set_default_param(CostModelConfig::PARAM_FILTER, SystemParamValue::Float(0.9));
        let config = CostModelConfig::default();
        let updated = config.apply_system_params(&registry, Some("tenant"));
        assert_eq!(updated.filter, 0.9);
    }

    #[test]
    fn system_params_ignore_invalid_values() {
        let registry = SystemParamRegistry::new();
        registry.set_default_param(CostModelConfig::PARAM_SORT, SystemParamValue::Float(0.0));
        let config = CostModelConfig::default();
        let updated = config.apply_system_params(&registry, Some("tenant"));
        assert_eq!(updated.sort, config.sort);
    }
}

pub(crate) fn load_config_from_path<T: DeserializeOwned>(
    path: impl AsRef<Path>,
    label: &str,
) -> chryso_core::error::ChrysoResult<T> {
    let content = fs::read_to_string(path.as_ref()).map_err(|err| {
        chryso_core::error::ChrysoError::new(format!("read {label} failed: {err}"))
    })?;
    if path
        .as_ref()
        .extension()
        .and_then(|ext| ext.to_str())
        .map(|ext| ext.eq_ignore_ascii_case("toml"))
        .unwrap_or(false)
    {
        toml::from_str(&content).map_err(|err| {
            chryso_core::error::ChrysoError::new(format!("parse toml {label} failed: {err}"))
        })
    } else {
        serde_json::from_str(&content).map_err(|err| {
            chryso_core::error::ChrysoError::new(format!("parse json {label} failed: {err}"))
        })
    }
}

fn join_penalty(plan: &PhysicalPlan, config: &CostModelConfig) -> f64 {
    match plan {
        PhysicalPlan::Join {
            algorithm,
            left,
            right,
            ..
        } => {
            let current = match algorithm {
                chryso_planner::JoinAlgorithm::Hash => config.join * config.join_hash_multiplier,
                chryso_planner::JoinAlgorithm::NestedLoop => {
                    config.join * config.join_nested_multiplier
                }
            };
            current + join_penalty(left, config) + join_penalty(right, config)
        }
        PhysicalPlan::Filter { input, .. }
        | PhysicalPlan::Projection { input, .. }
        | PhysicalPlan::Aggregate { input, .. }
        | PhysicalPlan::Distinct { input }
        | PhysicalPlan::TopN { input, .. }
        | PhysicalPlan::Sort { input, .. }
        | PhysicalPlan::Limit { input, .. }
        | PhysicalPlan::Derived { input, .. } => join_penalty(input, config),
        PhysicalPlan::TableScan { .. }
        | PhysicalPlan::IndexScan { .. }
        | PhysicalPlan::Dml { .. } => 0.0,
    }
}

fn node_weight(plan: &PhysicalPlan, config: &CostModelConfig) -> f64 {
    match plan {
        PhysicalPlan::TableScan { .. } | PhysicalPlan::IndexScan { .. } => config.scan,
        PhysicalPlan::Filter { .. } => config.filter,
        PhysicalPlan::Projection { .. } => config.projection,
        PhysicalPlan::Join { .. } => config.join,
        PhysicalPlan::Aggregate { .. } => config.aggregate,
        PhysicalPlan::Distinct { .. } => config.aggregate,
        PhysicalPlan::TopN { .. } => config.sort,
        PhysicalPlan::Sort { .. } => config.sort,
        PhysicalPlan::Limit { .. } => config.limit,
        PhysicalPlan::Derived { .. } => config.derived,
        PhysicalPlan::Dml { .. } => config.dml,
    }
}

fn total_weight(plan: &PhysicalPlan, config: &CostModelConfig) -> f64 {
    // Unit cost uses configurable weights for every node in the tree.
    let base = node_weight(plan, config);
    let children = match plan {
        PhysicalPlan::Join { left, right, .. } => {
            total_weight(left, config) + total_weight(right, config)
        }
        PhysicalPlan::Filter { input, .. }
        | PhysicalPlan::Projection { input, .. }
        | PhysicalPlan::Aggregate { input, .. }
        | PhysicalPlan::Distinct { input }
        | PhysicalPlan::TopN { input, .. }
        | PhysicalPlan::Sort { input, .. }
        | PhysicalPlan::Limit { input, .. }
        | PhysicalPlan::Derived { input, .. } => total_weight(input, config),
        PhysicalPlan::TableScan { .. }
        | PhysicalPlan::IndexScan { .. }
        | PhysicalPlan::Dml { .. } => 0.0,
    };
    base + children + join_penalty(plan, config)
}

fn total_stats_cost(plan: &PhysicalPlan, stats: &StatsCache, config: &CostModelConfig) -> f64 {
    // Stats cost applies selectivity per node and accumulates subtree contributions.
    let rows = estimate_rows(plan, stats);
    let mut cost = rows * node_weight(plan, config) + join_penalty(plan, config);
    cost += match plan {
        PhysicalPlan::Join { left, right, .. } => {
            total_stats_cost(left, stats, config) + total_stats_cost(right, stats, config)
        }
        PhysicalPlan::Filter { input, .. }
        | PhysicalPlan::Projection { input, .. }
        | PhysicalPlan::Aggregate { input, .. }
        | PhysicalPlan::Distinct { input }
        | PhysicalPlan::TopN { input, .. }
        | PhysicalPlan::Sort { input, .. }
        | PhysicalPlan::Limit { input, .. }
        | PhysicalPlan::Derived { input, .. } => total_stats_cost(input, stats, config),
        PhysicalPlan::TableScan { .. }
        | PhysicalPlan::IndexScan { .. }
        | PhysicalPlan::Dml { .. } => 0.0,
    };
    cost
}

fn estimate_rows(plan: &PhysicalPlan, stats: &StatsCache) -> f64 {
    match plan {
        PhysicalPlan::TableScan { table } | PhysicalPlan::IndexScan { table, .. } => stats
            .table_stats(table)
            .map(|stats| stats.row_count)
            .unwrap_or(1000.0),
        PhysicalPlan::Dml { .. } => 1.0,
        PhysicalPlan::Derived { input, .. } => estimate_rows(input, stats),
        PhysicalPlan::Filter { predicate, input } => {
            let base = estimate_rows(input, stats);
            let table = single_table_name(input);
            base * estimate_selectivity(predicate, stats, table.as_deref())
        }
        PhysicalPlan::Projection { input, .. } => estimate_rows(input, stats),
        PhysicalPlan::Join { left, right, .. } => {
            estimate_rows(left, stats) * estimate_rows(right, stats) * 0.1
        }
        PhysicalPlan::Aggregate { input, .. } => (estimate_rows(input, stats) * 0.1).max(1.0),
        PhysicalPlan::Distinct { input } => (estimate_rows(input, stats) * 0.3).max(1.0),
        PhysicalPlan::TopN { limit, input, .. } => estimate_rows(input, stats).min(*limit as f64),
        PhysicalPlan::Sort { input, .. } => estimate_rows(input, stats),
        PhysicalPlan::Limit { limit, input, .. } => match limit {
            Some(limit) => estimate_rows(input, stats).min(*limit as f64),
            None => estimate_rows(input, stats),
        },
    }
}

fn estimate_selectivity(
    predicate: &chryso_core::ast::Expr,
    stats: &StatsCache,
    table: Option<&str>,
) -> f64 {
    use chryso_core::ast::{BinaryOperator, Expr};
    match predicate {
        Expr::BinaryOp { left, op, right } if matches!(op, BinaryOperator::And) => {
            estimate_selectivity(left, stats, table) * estimate_selectivity(right, stats, table)
        }
        Expr::BinaryOp { left, op, right } if matches!(op, BinaryOperator::Or) => {
            let left_sel = estimate_selectivity(left, stats, table);
            let right_sel = estimate_selectivity(right, stats, table);
            (left_sel + right_sel - left_sel * right_sel).min(1.0)
        }
        Expr::IsNull { expr, negated } => {
            let (table_name, column_name) = match expr.as_ref() {
                Expr::Identifier(name) => match name.split_once('.') {
                    Some((prefix, column)) => (Some(prefix), column),
                    None => (table, name.as_str()),
                },
                _ => (table, ""),
            };
            if let (Some(table_name), column_name) = (table_name, column_name) {
                if !column_name.is_empty() {
                    if let Some(stats) = stats.column_stats(table_name, column_name) {
                        let base = stats.null_fraction;
                        return if *negated { 1.0 - base } else { base };
                    }
                }
            }
            if *negated { 0.9 } else { 0.1 }
        }
        Expr::BinaryOp { left, op, right } => {
            if let Some(selectivity) = estimate_eq_selectivity(left, right, stats, table) {
                match op {
                    BinaryOperator::Eq => selectivity,
                    BinaryOperator::NotEq => (1.0 - selectivity).max(0.0),
                    BinaryOperator::Lt
                    | BinaryOperator::LtEq
                    | BinaryOperator::Gt
                    | BinaryOperator::GtEq => 0.3,
                    _ => 0.3,
                }
            } else {
                0.3
            }
        }
        _ => 0.5,
    }
}

fn estimate_eq_selectivity(
    left: &chryso_core::ast::Expr,
    right: &chryso_core::ast::Expr,
    stats: &StatsCache,
    table: Option<&str>,
) -> Option<f64> {
    let (ident, literal) = match (left, right) {
        (chryso_core::ast::Expr::Identifier(name), chryso_core::ast::Expr::Literal(_)) => {
            (name, right)
        }
        (chryso_core::ast::Expr::Literal(_), chryso_core::ast::Expr::Identifier(name)) => {
            (name, left)
        }
        _ => return None,
    };
    let _ = literal;
    let (table_name, column_name) = match ident.split_once('.') {
        Some((prefix, column)) => (Some(prefix), column),
        None => (table, ident.as_str()),
    };
    let table_name = table_name?;
    let stats = stats.column_stats(table_name, column_name)?;
    let distinct = stats.distinct_count.max(1.0);
    Some(1.0 / distinct)
}

fn single_table_name(plan: &PhysicalPlan) -> Option<String> {
    match plan {
        PhysicalPlan::TableScan { table } | PhysicalPlan::IndexScan { table, .. } => {
            Some(table.clone())
        }
        PhysicalPlan::Filter { input, .. }
        | PhysicalPlan::Projection { input, .. }
        | PhysicalPlan::Aggregate { input, .. }
        | PhysicalPlan::Distinct { input }
        | PhysicalPlan::TopN { input, .. }
        | PhysicalPlan::Sort { input, .. }
        | PhysicalPlan::Limit { input, .. }
        | PhysicalPlan::Derived { input, .. } => single_table_name(input),
        PhysicalPlan::Join { .. } | PhysicalPlan::Dml { .. } => None,
    }
}
