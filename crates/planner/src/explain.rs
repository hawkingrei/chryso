use chryso_core::ast::Expr;
use crate::{LogicalPlan, PhysicalPlan};
use chryso_metadata::type_inference::TypeInferencer;
use crate::cost::CostModel;

pub trait CardinalityEstimator {
    fn estimate(&self, plan: &LogicalPlan, stats: &chryso_metadata::StatsCache) -> f64;
}

pub struct NaiveEstimator;

impl CardinalityEstimator for NaiveEstimator {
    fn estimate(&self, plan: &LogicalPlan, stats: &chryso_metadata::StatsCache) -> f64 {
        match plan {
            LogicalPlan::Scan { table } => stats
                .table_stats(table)
                .map(|s| s.row_count)
                .unwrap_or(1000.0),
            LogicalPlan::IndexScan { .. } => 100.0,
            LogicalPlan::Dml { .. } => 1.0,
            LogicalPlan::Derived { input, .. } => self.estimate(input, stats),
            LogicalPlan::Filter { input, .. } => self.estimate(input, stats) * 0.25,
            LogicalPlan::Projection { input, .. } => self.estimate(input, stats),
            LogicalPlan::Join { left, right, .. } => {
                self.estimate(left, stats) * self.estimate(right, stats) * 0.1
            }
            LogicalPlan::Aggregate { input, .. } => self.estimate(input, stats) * 0.1,
            LogicalPlan::Distinct { input } => self.estimate(input, stats) * 0.1,
            LogicalPlan::TopN { limit, .. } => *limit as f64,
            LogicalPlan::Sort { input, .. } => self.estimate(input, stats),
            LogicalPlan::Limit { limit, .. } => limit.unwrap_or(100) as f64,
        }
    }
}

pub struct ExplainFormatter {
    config: ExplainConfig,
}

#[derive(Clone)]
pub struct ExplainConfig {
    pub show_types: bool,
    pub show_costs: bool,
    pub show_cardinality: bool,
    pub compact: bool,
    pub max_expr_length: usize,
}

impl Default for ExplainConfig {
    fn default() -> Self {
        Self {
            show_types: true,
            show_costs: true,
            show_cardinality: false,
            compact: false,
            max_expr_length: 80,
        }
    }
}

impl ExplainFormatter {
    pub fn new(config: ExplainConfig) -> Self {
        Self { config }
    }

    pub fn format_logical_plan(
        &self,
        plan: &LogicalPlan,
        inferencer: &dyn TypeInferencer,
    ) -> String {
        let mut output = String::new();
        output.push_str("=== Logical Plan ===\n");
        self.format_logical_node(plan, 0, inferencer, None, &mut output, true, "");
        output
    }
    
    pub fn format_logical_plan_with_stats(
        &self,
        plan: &LogicalPlan,
        inferencer: &dyn TypeInferencer,
        stats: &chryso_metadata::StatsCache,
    ) -> String {
        let mut output = String::new();
        output.push_str("=== Logical Plan ===\n");
        let estimator = NaiveEstimator;
        self.format_logical_node(plan, 0, inferencer, Some((&estimator, stats)), &mut output, true, "");
        output
    }

    pub fn format_physical_plan(
        &self,
        plan: &PhysicalPlan,
        cost_model: &dyn CostModel,
    ) -> String {
        let mut output = String::new();
        output.push_str("=== Physical Plan ===\n");
        self.format_physical_node(plan, 0, cost_model, None, &mut output, true, "");
        output
    }
    
    pub fn format_physical_plan_with_stats(
        &self,
        plan: &PhysicalPlan,
        cost_model: &dyn CostModel,
        stats: &chryso_metadata::StatsCache,
    ) -> String {
        let mut output = String::new();
        output.push_str("=== Physical Plan ===\n");
        let estimator = NaiveEstimator;
        self.format_physical_node(plan, 0, cost_model, Some((&estimator, stats)), &mut output, true, "");
        output
    }

    fn estimate_physical_cardinality(
        &self,
        plan: &PhysicalPlan,
        estimator: &dyn CardinalityEstimator,
        stats: &chryso_metadata::StatsCache,
    ) -> f64 {
        // Convert physical plan to logical plan for estimation (simplified mapping).
        // This is a basic implementation - in a real system, you'd have more sophisticated mapping.
        match plan {
            PhysicalPlan::TableScan { table } => {
                estimator.estimate(&LogicalPlan::Scan { table: table.clone() }, stats)
            }
            PhysicalPlan::IndexScan { table, .. } => {
                estimator.estimate(&LogicalPlan::Scan { table: table.clone() }, stats) * 0.1
            }
            PhysicalPlan::Dml { .. } => 1.0,
            PhysicalPlan::Derived { input, .. }
            | PhysicalPlan::Projection { input, .. }
            | PhysicalPlan::Sort { input, .. } => {
                self.estimate_physical_cardinality(input, estimator, stats)
            }
            PhysicalPlan::Filter { input, .. } => {
                self.estimate_physical_cardinality(input, estimator, stats) * 0.25
            }
            PhysicalPlan::Join { left, right, .. } => {
                let left_estimate = self.estimate_physical_cardinality(left, estimator, stats);
                let right_estimate = self.estimate_physical_cardinality(right, estimator, stats);
                left_estimate * right_estimate * 0.1
            }
            PhysicalPlan::Aggregate { input, .. } => {
                self.estimate_physical_cardinality(input, estimator, stats) * 0.1
            }
            PhysicalPlan::Distinct { input } => {
                self.estimate_physical_cardinality(input, estimator, stats) * 0.1
            }
            PhysicalPlan::Limit { limit, .. } => limit.unwrap_or(100) as f64,
            PhysicalPlan::TopN { limit, .. } => *limit as f64,
        }
    }

    fn format_logical_node(
        &self,
        plan: &LogicalPlan,
        depth: usize,
        inferencer: &dyn TypeInferencer,
        estimator_stats: Option<(&dyn CardinalityEstimator, &chryso_metadata::StatsCache)>,
        output: &mut String,
        is_last: bool,
        prefix: &str,
    ) {
        let indent = self.get_indent(depth, is_last);
        let node_prefix = format!("{}{}", prefix, indent);
        
        // Get cardinality estimate if available
        let cardinality_str = if self.config.show_cardinality {
            if let Some((estimator, stats)) = estimator_stats {
                let estimate = estimator.estimate(plan, stats);
                format!(", cardinality={:.0}", estimate)
            } else {
                String::new()
            }
        } else {
            String::new()
        };
        
        match plan {
            LogicalPlan::Scan { table } => {
                output.push_str(&format!("{}LogicalScan: table={}{}\n", node_prefix, table, cardinality_str));
            }
            LogicalPlan::IndexScan { table, index, predicate } => {
                let pred_str = self.format_expr(predicate);
                output.push_str(&format!(
                    "{}LogicalIndexScan: table={}, index={}, predicate={}{}\n",
                    node_prefix, table, index, pred_str, cardinality_str
                ));
            }
            LogicalPlan::Dml { sql } => {
                let sql_preview = self.safe_truncate(sql, 50);
                output.push_str(&format!("{}LogicalDml: sql={}{}\n", node_prefix, sql_preview, cardinality_str));
            }
            LogicalPlan::Derived { alias, column_aliases, input } => {
                output.push_str(&format!(
                    "{}LogicalDerived: alias={}, columns=[{}]{}\n",
                    node_prefix,
                    alias,
                    column_aliases.join(", "),
                    cardinality_str
                ));
                let child_prefix = format!("{}{}", prefix, self.get_child_prefix(depth, is_last));
                self.format_logical_node(input, depth + 1, inferencer, estimator_stats, output, true, &child_prefix);
            }
            LogicalPlan::Filter { predicate, input } => {
                let pred_str = self.format_expr(predicate);
                let type_info = if self.config.show_types {
                    format!(", type={:?}", inferencer.infer_expr(predicate))
                } else {
                    String::new()
                };
                output.push_str(&format!(
                    "{}LogicalFilter: predicate={}{}{}\n",
                    node_prefix, pred_str, type_info, cardinality_str
                ));
                let child_prefix = format!("{}{}", prefix, self.get_child_prefix(depth, is_last));
                self.format_logical_node(input, depth + 1, inferencer, estimator_stats, output, true, &child_prefix);
            }
            LogicalPlan::Projection { exprs, input } => {
                let expr_str = self.format_expr_list(exprs);
                let type_info = if self.config.show_types {
                    let types = chryso_metadata::type_inference::expr_types(exprs, inferencer);
                    format!(", types={:?}", types)
                } else {
                    String::new()
                };
                output.push_str(&format!(
                    "{}LogicalProject: expressions={}{}{}\n",
                    node_prefix, expr_str, type_info, cardinality_str
                ));
                let child_prefix = format!("{}{}", prefix, self.get_child_prefix(depth, is_last));
                self.format_logical_node(input, depth + 1, inferencer, estimator_stats, output, true, &child_prefix);
            }
            LogicalPlan::Join { join_type, left, right, on } => {
                let on_str = self.format_expr(on);
                output.push_str(&format!(
                    "{}LogicalJoin: type={:?}, on={}{}\n",
                    node_prefix, join_type, on_str, cardinality_str
                ));
                let child_prefix = format!("{}{}", prefix, self.get_child_prefix(depth, is_last));
                self.format_logical_node(left, depth + 1, inferencer, estimator_stats, output, false, &child_prefix);
                self.format_logical_node(right, depth + 1, inferencer, estimator_stats, output, true, &child_prefix);
            }
            LogicalPlan::Aggregate { group_exprs, aggr_exprs, input } => {
                output.push_str(&format!(
                    "{}LogicalAggregate: group_by=[{}], aggregates=[{}]{}\n",
                    node_prefix,
                    self.format_expr_list(group_exprs),
                    self.format_expr_list(aggr_exprs),
                    cardinality_str
                ));
                let child_prefix = format!("{}{}", prefix, self.get_child_prefix(depth, is_last));
                self.format_logical_node(input, depth + 1, inferencer, estimator_stats, output, true, &child_prefix);
            }
            LogicalPlan::Distinct { input } => {
                output.push_str(&format!("{}LogicalDistinct{}\n", node_prefix, cardinality_str));
                let child_prefix = format!("{}{}", prefix, self.get_child_prefix(depth, is_last));
                self.format_logical_node(input, depth + 1, inferencer, estimator_stats, output, true, &child_prefix);
            }
            LogicalPlan::TopN { order_by, limit, input } => {
                output.push_str(&format!(
                    "{}LogicalTopN: order_by=[{}], limit={}{}\n",
                    node_prefix,
                    self.format_order_by_list(order_by),
                    limit,
                    cardinality_str
                ));
                let child_prefix = format!("{}{}", prefix, self.get_child_prefix(depth, is_last));
                self.format_logical_node(input, depth + 1, inferencer, estimator_stats, output, true, &child_prefix);
            }
            LogicalPlan::Sort { order_by, input } => {
                output.push_str(&format!(
                    "{}LogicalSort: order_by=[{}]{}\n",
                    node_prefix,
                    self.format_order_by_list(order_by),
                    cardinality_str
                ));
                let child_prefix = format!("{}{}", prefix, self.get_child_prefix(depth, is_last));
                self.format_logical_node(input, depth + 1, inferencer, estimator_stats, output, true, &child_prefix);
            }
            LogicalPlan::Limit { limit, offset, input } => {
                let offset_str = offset.map(|o| format!(", offset={}", o)).unwrap_or_default();
                output.push_str(&format!(
                    "{}LogicalLimit: limit={:?}{}{}\n",
                    node_prefix, limit, offset_str, cardinality_str
                ));
                let child_prefix = format!("{}{}", prefix, self.get_child_prefix(depth, is_last));
                self.format_logical_node(input, depth + 1, inferencer, estimator_stats, output, true, &child_prefix);
            }
        }
    }

    fn format_physical_node(
        &self,
        plan: &PhysicalPlan,
        depth: usize,
        cost_model: &dyn CostModel,
        estimator_stats: Option<(&dyn CardinalityEstimator, &chryso_metadata::StatsCache)>,
        output: &mut String,
        is_last: bool,
        prefix: &str,
    ) {
        let indent = self.get_indent(depth, is_last);
        let node_prefix = format!("{}{}", prefix, indent);
        let cost = cost_model.cost(plan);
        let cost_str = if self.config.show_costs {
            format!(", cost={:.2}", cost.0)
        } else {
            String::new()
        };
        
        // Get cardinality estimate if available
        let cardinality_str = if self.config.show_cardinality {
            if let Some((estimator, stats)) = estimator_stats {
                // Convert PhysicalPlan to LogicalPlan for estimation (simplified approach)
                // For now, we'll use a basic estimate based on the physical plan type
                let estimate = self.estimate_physical_cardinality(plan, estimator, stats);
                format!(", cardinality={:.0}", estimate)
            } else {
                String::new()
            }
        } else {
            String::new()
        };
        
        match plan {
            PhysicalPlan::TableScan { table } => {
                output.push_str(&format!("{}TableScan: table={}{}{}\n", node_prefix, table, cost_str, cardinality_str));
            }
            PhysicalPlan::IndexScan { table, index, predicate } => {
                let pred_str = self.format_expr(predicate);
                output.push_str(&format!(
                    "{}IndexScan: table={}, index={}, predicate={}{}{}\n",
                    node_prefix, table, index, pred_str, cost_str, cardinality_str
                ));
            }
            PhysicalPlan::Dml { sql } => {
                let sql_preview = self.safe_truncate(sql, 50);
                output.push_str(&format!("{}Dml: sql={}{}{}\n", node_prefix, sql_preview, cost_str, cardinality_str));
            }
            PhysicalPlan::Derived { alias, column_aliases, input } => {
                output.push_str(&format!(
                    "{}Derived: alias={}, columns=[{}]{}{}\n",
                    node_prefix,
                    alias,
                    column_aliases.join(", "),
                    cost_str,
                    cardinality_str
                ));
                let child_prefix = format!("{}{}", prefix, self.get_child_prefix(depth, is_last));
                self.format_physical_node(input, depth + 1, cost_model, estimator_stats, output, true, &child_prefix);
            }
            PhysicalPlan::Filter { predicate, input } => {
                let pred_str = self.format_expr(predicate);
                output.push_str(&format!(
                    "{}Filter: predicate={}{}{}\n",
                    node_prefix, pred_str, cost_str, cardinality_str
                ));
                let child_prefix = format!("{}{}", prefix, self.get_child_prefix(depth, is_last));
                self.format_physical_node(input, depth + 1, cost_model, estimator_stats, output, true, &child_prefix);
            }
            PhysicalPlan::Projection { exprs, input } => {
                let expr_str = self.format_expr_list(exprs);
                output.push_str(&format!(
                    "{}Project: expressions={}{}{}\n",
                    node_prefix, expr_str, cost_str, cardinality_str
                ));
                let child_prefix = format!("{}{}", prefix, self.get_child_prefix(depth, is_last));
                self.format_physical_node(input, depth + 1, cost_model, estimator_stats, output, true, &child_prefix);
            }
            PhysicalPlan::Join { join_type, algorithm, left, right, on } => {
                let on_str = self.format_expr(on);
                output.push_str(&format!(
                    "{}Join: type={:?}, algorithm={:?}, on={}{}{}\n",
                    node_prefix, join_type, algorithm, on_str, cost_str, cardinality_str
                ));
                let child_prefix = format!("{}{}", prefix, self.get_child_prefix(depth, is_last));
                self.format_physical_node(left, depth + 1, cost_model, estimator_stats, output, false, &child_prefix);
                self.format_physical_node(right, depth + 1, cost_model, estimator_stats, output, true, &child_prefix);
            }
            PhysicalPlan::Aggregate { group_exprs, aggr_exprs, input } => {
                output.push_str(&format!(
                    "{}Aggregate: group_by=[{}], aggregates=[{}]{}{}\n",
                    node_prefix,
                    self.format_expr_list(group_exprs),
                    self.format_expr_list(aggr_exprs),
                    cost_str,
                    cardinality_str
                ));
                let child_prefix = format!("{}{}", prefix, self.get_child_prefix(depth, is_last));
                self.format_physical_node(input, depth + 1, cost_model, estimator_stats, output, true, &child_prefix);
            }
            PhysicalPlan::Sort { order_by, input } => {
                output.push_str(&format!(
                    "{}Sort: order_by=[{}]{}{}\n",
                    node_prefix,
                    self.format_order_by_list(order_by),
                    cost_str,
                    cardinality_str
                ));
                let child_prefix = format!("{}{}", prefix, self.get_child_prefix(depth, is_last));
                self.format_physical_node(input, depth + 1, cost_model, estimator_stats, output, true, &child_prefix);
            }
            PhysicalPlan::TopN { order_by, limit, input } => {
                output.push_str(&format!(
                    "{}TopN: order_by=[{}], limit={}{}{}\n",
                    node_prefix,
                    self.format_order_by_list(order_by),
                    limit,
                    cost_str,
                    cardinality_str
                ));
                let child_prefix = format!("{}{}", prefix, self.get_child_prefix(depth, is_last));
                self.format_physical_node(input, depth + 1, cost_model, estimator_stats, output, true, &child_prefix);
            }
            PhysicalPlan::Limit { limit, offset, input } => {
                let offset_str = offset.map(|o| format!(", offset={}", o)).unwrap_or_default();
                output.push_str(&format!(
                    "{}Limit: limit={:?}{}{}{}\n",
                    node_prefix, limit, offset_str, cost_str, cardinality_str
                ));
                let child_prefix = format!("{}{}", prefix, self.get_child_prefix(depth, is_last));
                self.format_physical_node(input, depth + 1, cost_model, estimator_stats, output, true, &child_prefix);
            }
            PhysicalPlan::Distinct { input } => {
                output.push_str(&format!("{}Distinct{}{}\n", node_prefix, cost_str, cardinality_str));
                let child_prefix = format!("{}{}", prefix, self.get_child_prefix(depth, is_last));
                self.format_physical_node(input, depth + 1, cost_model, estimator_stats, output, true, &child_prefix);
            }

        }
    }

    fn get_indent(&self, depth: usize, is_last: bool) -> String {
        if depth == 0 {
            String::new()
        } else if is_last {
            "└── ".to_string()
        } else {
            "├── ".to_string()
        }
    }

    fn get_child_prefix(&self, depth: usize, is_last: bool) -> String {
        if depth == 0 {
            String::new()
        } else if is_last {
            "    ".to_string()
        } else {
            "│   ".to_string()
        }
    }

    /// Safely truncate a string to the specified length while preserving UTF-8 boundaries.
    fn safe_truncate(&self, text: &str, max_len: usize) -> String {
        if text.len() <= max_len {
            return text.to_string();
        }

        let ellipsis = "...";
        if max_len == 0 {
            return String::new();
        }
        if max_len < ellipsis.len() {
            return ellipsis.chars().take(max_len).collect();
        }

        let truncate_len = max_len - ellipsis.len();
        let mut end = truncate_len.min(text.len());
        while end > 0 && !text.is_char_boundary(end) {
            end -= 1;
        }

        if end == 0 {
            return ellipsis.to_string();
        }

        format!("{}{}", &text[..end], ellipsis)
    }

    fn format_expr(&self, expr: &Expr) -> String {
        let sql = expr.to_sql();
        self.safe_truncate(&sql, self.config.max_expr_length)
    }

    fn format_expr_list(&self, exprs: &[Expr]) -> String {
        if exprs.is_empty() {
            "[]".to_string()
        } else if exprs.len() <= 3 || self.config.compact {
            let expr_strs: Vec<String> = exprs.iter().map(|e| self.format_expr(e)).collect();
            format!("[{}]", expr_strs.join(", "))
        } else {
            let expr_strs: Vec<String> = exprs.iter().map(|e| self.format_expr(e)).collect();
            format!("[{}...] ({} items)", expr_strs[..3].join(", "), exprs.len())
        }
    }

    fn format_order_by_list(&self, order_by: &[chryso_core::ast::OrderByExpr]) -> String {
        if order_by.is_empty() {
            "[]".to_string()
        } else if order_by.len() <= 2 || self.config.compact {
            let items: Vec<String> = order_by.iter().map(|item| {
                let dir = if item.asc { "asc" } else { "desc" };
                let mut rendered = format!("{} {}", self.format_expr(&item.expr), dir);
                if let Some(nulls_first) = item.nulls_first {
                    if nulls_first {
                        rendered.push_str(" nulls first");
                    } else {
                        rendered.push_str(" nulls last");
                    }
                }
                rendered
            }).collect();
            format!("[{}]", items.join(", "))
        } else {
            let items: Vec<String> = order_by.iter().map(|item| {
                let dir = if item.asc { "asc" } else { "desc" };
                let mut rendered = format!("{} {}", self.format_expr(&item.expr), dir);
                if let Some(nulls_first) = item.nulls_first {
                    if nulls_first {
                        rendered.push_str(" nulls first");
                    } else {
                        rendered.push_str(" nulls last");
                    }
                }
                rendered
            }).collect();
            format!("[{}...] ({} items)", items[..2].join(", "), order_by.len())
        }
    }
}

pub fn format_simple_logical_plan(plan: &LogicalPlan) -> String {
    let formatter = ExplainFormatter::new(ExplainConfig {
        show_types: false,
        show_costs: false,
        show_cardinality: false,
        compact: true,
        max_expr_length: 50,
    });
    formatter.format_logical_plan(plan, &chryso_metadata::type_inference::SimpleTypeInferencer)
}

pub fn format_logical_plan_with_stats(plan: &LogicalPlan, stats: &chryso_metadata::StatsCache) -> String {
    let formatter = ExplainFormatter::new(ExplainConfig {
        show_types: true,
        show_costs: false,
        show_cardinality: true,
        compact: false,
        max_expr_length: 80,
    });
    formatter.format_logical_plan_with_stats(plan, &chryso_metadata::type_inference::SimpleTypeInferencer, stats)
}

pub fn format_physical_plan_with_stats(plan: &PhysicalPlan, cost_model: &dyn crate::cost::CostModel, stats: &chryso_metadata::StatsCache) -> String {
    let formatter = ExplainFormatter::new(ExplainConfig {
        show_types: false,
        show_costs: true,
        show_cardinality: true,
        compact: false,
        max_expr_length: 80,
    });
    formatter.format_physical_plan_with_stats(plan, cost_model, stats)
}

pub fn format_simple_physical_plan(plan: &PhysicalPlan) -> String {
    let formatter = ExplainFormatter::new(ExplainConfig {
        show_types: false,
        show_costs: true,
        show_cardinality: false,
        compact: true,
        max_expr_length: 50,
    });
    // Use a simple cost model for basic formatting
    struct SimpleCostModel;
    impl crate::cost::CostModel for SimpleCostModel {
        fn cost(&self, _plan: &PhysicalPlan) -> crate::cost::Cost {
            crate::cost::Cost(1.0)
        }
    }
    formatter.format_physical_plan(plan, &SimpleCostModel)
}

#[cfg(test)]
mod tests {
    use super::*;
    use chryso_core::ast::{Expr, BinaryOperator};
    use crate::{LogicalPlan, PhysicalPlan};

    #[test]
    fn test_format_simple_logical_plan() {
        let plan = LogicalPlan::Filter {
            predicate: Expr::BinaryOp {
                left: Box::new(Expr::Identifier("id".to_string())),
                op: BinaryOperator::Eq,
                right: Box::new(Expr::Literal(chryso_core::ast::Literal::Number(1.0))),
            },
            input: Box::new(LogicalPlan::Scan {
                table: "users".to_string(),
            }),
        };

        let output = format_simple_logical_plan(&plan);
        assert!(output.contains("=== Logical Plan ==="));
        assert!(output.contains("LogicalFilter"));
        assert!(output.contains("id = 1"));
        assert!(output.contains("LogicalScan"));
        assert!(output.contains("users"));
    }

    #[test]
    fn test_format_simple_physical_plan() {
        let plan = PhysicalPlan::Filter {
            predicate: Expr::BinaryOp {
                left: Box::new(Expr::Identifier("id".to_string())),
                op: BinaryOperator::Eq,
                right: Box::new(Expr::Literal(chryso_core::ast::Literal::Number(1.0))),
            },
            input: Box::new(PhysicalPlan::TableScan {
                table: "users".to_string(),
            }),
        };

        let output = format_simple_physical_plan(&plan);
        assert!(output.contains("=== Physical Plan ==="));
        assert!(output.contains("Filter"));
        assert!(output.contains("id = 1"));
        assert!(output.contains("TableScan"));
        assert!(output.contains("users"));
        assert!(output.contains("cost=1.00"));
    }

    #[test]
    fn test_tree_structure_formatting() {
        let plan = LogicalPlan::Limit {
            limit: Some(10),
            offset: None,
            input: Box::new(LogicalPlan::Scan {
                table: "users".to_string(),
            }),
        };

        let config = ExplainConfig::default();
        let formatter = ExplainFormatter::new(config);
        let output = formatter.format_logical_plan(&plan, &chryso_metadata::type_inference::SimpleTypeInferencer);
        
        // Check for tree structure characters
        assert!(output.contains("└──"));
        assert!(!output.contains("├──")); // Should be only one child
    }

    #[test]
    fn test_cardinality_estimation() {
        let plan = LogicalPlan::Scan {
            table: "users".to_string(),
        };

        // Create mock stats
        let mut stats = chryso_metadata::StatsCache::new();
        stats.insert_table_stats("users", chryso_metadata::TableStats { row_count: 5000.0 });

        // Test with cardinality enabled
        let config = ExplainConfig {
            show_types: false,
            show_costs: false,
            show_cardinality: true,
            compact: false,
            max_expr_length: 80,
        };
        let formatter = ExplainFormatter::new(config);
        let output = formatter.format_logical_plan_with_stats(&plan, &chryso_metadata::type_inference::SimpleTypeInferencer, &stats);
        
        // Should contain cardinality estimate
        assert!(output.contains("cardinality=5000"));
        assert!(output.contains("LogicalScan"));
        assert!(output.contains("users"));
    }

    #[test]
    fn test_utf8_truncation_safety() {
        use chryso_core::ast::Literal;
        
        // Create a long expression with Unicode characters using Unicode escape sequences
        let unicode_expr = Expr::Literal(Literal::String(
            "\u{4e2d}\u{6587}\u{5b57}\u{7b26}\u{4e32}\u{5305}\u{542b}\u{5f88}\u{591a}\u{0048}\u{0065}\u{006c}\u{006c}\u{006f}".to_string()
        ));
        
        // Test with short max length to force truncation
        let config = ExplainConfig {
            show_types: false,
            show_costs: false,
            show_cardinality: false,
            compact: true,
            max_expr_length: 20,  // Very short to force truncation
        };
        
        let formatter = ExplainFormatter::new(config);
        let formatted = formatter.format_expr(&unicode_expr);
        
        // Verify it's valid UTF-8
        assert!(std::str::from_utf8(formatted.as_bytes()).is_ok());
        
        // Should be truncated but still valid
        assert!(formatted.ends_with("..."));
        assert!(formatted.len() <= 20);
    }

    #[test]
    fn test_safe_truncate_with_non_ascii() {
        // Test various non-ASCII scenarios using Unicode escape sequences
        let test_cases = vec![
            // Chinese characters
            ("\u{4e2d}\u{6587}\u{5b57}\u{7b26}\u{4e32}\u{5305}\u{542b}\u{5f88}\u{591a}\u{0048}\u{0065}\u{006c}\u{006c}\u{006f}", 20),
            // Mixed ASCII and Unicode
            ("Hello\u{4e16}\u{754c}\u{ff0c}\u{8fd9}\u{662f}\u{4e00}\u{4e2a}mixed\u{5b57}\u{7b26}\u{4e32}with\u{4e2d}\u{6587}", 25),
            // Emoji (4 bytes each)
            ("Hello \u{1f44b} World \u{1f30d} Test \u{1f600} String \u{1f389}", 30),
            // Japanese
            ("\u{3053}\u{3093}\u{306b}\u{3061}\u{306f}\u{4e16}\u{754c}\u{3001}\u{3053}\u{308c}\u{306f}\u{9577}\u{3044}\u{65e5}\u{672c}\u{8a9e}\u{306e}\u{6587}\u{5b57}\u{5217}\u{3067}\u{3059}", 20),
            // Arabic
            ("\u{0645}\u{0631}\u{062d}\u{0628}\u{0627} \u{0628}\u{0627}\u{0644}\u{0639}\u{0627}\u{0644}\u{0645}\u{060c} \u{0647}\u{0630}\u{0647} \u{0647}\u{064a} \u{0633}\u{0644}\u{0633}\u{0644}\u{0629} \u{0646}\u{0635}\u{064a}\u{0629} \u{0637}\u{0648}\u{064a}\u{0644}\u{0629}", 20),
            // Russian
            ("\u{041f}\u{0440}\u{0438}\u{0432}\u{0435}\u{0442} \u{043c}\u{0438}\u{0440}, \u{044d}\u{0442}\u{043e} \u{0434}\u{043b}\u{0438}\u{043d}\u{043d}\u{0430}\u{044f} \u{0441}\u{0442}\u{0440}\u{043e}\u{043a}\u{0430} \u{0442}\u{0435}\u{043a}\u{0441}\u{0442}\u{0430}", 20),
            // Edge case: very short truncation
            ("Hello\u{4e16}\u{754c}", 5),
            // Edge case: exactly at boundary
            ("Hello\u{4e16}\u{754c}Test", 11),
        ];

        let config = ExplainConfig::default();
        let formatter = ExplainFormatter::new(config);

        for (input, max_len) in test_cases {
            let truncated = formatter.safe_truncate(input, max_len);
            
            // Verify UTF-8 validity
            assert!(std::str::from_utf8(truncated.as_bytes()).is_ok(), 
                    "Truncated string must be valid UTF-8");
            
            // Verify length constraint
            assert!(truncated.len() <= max_len, 
                    "Truncated string length {} must not exceed max_len {}", truncated.len(), max_len);
            
            // Verify it ends with ... if truncated
            if input.len() > max_len && max_len >= 3 {
                assert!(truncated.ends_with("..."), 
                        "Truncated string should end with '...'");
                
                // Verify original is longer
                assert!(input.len() > truncated.len(), 
                        "Original should be longer than truncated");
            } else if input.len() > max_len {
                // For max_len < 3, should use dots pattern
                assert!(truncated.chars().all(|c| c == '.'), 
                        "For max_len < 3, should return dots pattern");
            } else {
                // If not truncated, should be identical
                assert_eq!(input, truncated, 
                          "String not exceeding max_len should remain unchanged");
            }
        }
    }

    #[test]
    fn test_safe_truncate_edge_cases() {
        let config = ExplainConfig::default();
        let formatter = ExplainFormatter::new(config);
        
        // Test edge cases for max_len < 3
        let test_cases = vec![
            (0, ""),           // max_len = 0: empty string
            (1, "."),          // max_len = 1: single dot
            (2, ".."),         // max_len = 2: two dots
            (3, "..."),        // max_len = 3: three dots
        ];
        
        for (max_len, expected) in test_cases {
            let result = formatter.safe_truncate("Hello\u{4e16}\u{754c}Test", max_len);
            assert_eq!(result, expected, 
                      "For max_len={}, expected '{}', got '{}'", max_len, expected, result);
            assert_eq!(result.len(), max_len, 
                      "Result length should exactly match max_len");
        }
        
        // Test that max_len >= 3 uses ellipsis truncation
        let result = formatter.safe_truncate("Hello\u{4e16}\u{754c}Test", 4);
        assert_eq!(result.len(), 4, "Should respect max_len=4");
        assert!(result.ends_with("..."), "Should end with ellipsis");
        
        // Test boundary case: exactly 3 characters available for content
        let result = formatter.safe_truncate("Hello\u{4e16}\u{754c}Test", 6);
        assert!(result.len() <= 6, "Should respect max_len=6");
        assert!(result.ends_with("..."), "Should end with ellipsis");
    }

    #[test]
    fn test_dml_preview_with_non_ascii() {
        // Test DML preview with non-ASCII SQL using Unicode escape sequences
        let long_unicode_sql = "INSERT INTO \u{7528}\u{6237}\u{8868} (\u{59d3}\u{540d}, \u{5e74}\u{9f84}, \u{5730}\u{5740}) VALUES ('\u{5f20}\u{4e09}', 25, '\u{5317}\u{4eac}\u{5e02}\u{671d}\u{9633}\u{533a}'), ('\u{674e}\u{56db}', 30, '\u{4e0a}\u{6d77}\u{5e02}\u{6d66}\u{4e1c}\u{65b0}\u{533a}'), ('\u{738b}\u{4e94}', 35, '\u{5e7f}\u{5dde}\u{5e02}\u{5929}\u{6cb3}\u{533a}')";
        
        let dml_plan = LogicalPlan::Dml {
            sql: long_unicode_sql.to_string(),
        };
        
        // Test with short preview length
        let config = ExplainConfig {
            show_types: false,
            show_costs: false,
            show_cardinality: false,
            compact: true,
            max_expr_length: 80,
        };
        
        let formatter = ExplainFormatter::new(config);
        let output = formatter.format_logical_plan(&dml_plan, &chryso_metadata::type_inference::SimpleTypeInferencer);
        
        // Verify output contains truncated DML
        assert!(output.contains("LogicalDml"));
        assert!(output.contains("INSERT INTO"));
        
        // Verify safe truncation (should end with ...)
        let dml_line = output.lines()
            .find(|line| line.contains("LogicalDml"))
            .unwrap();
        
        // The SQL part should be truncated and end with ...
        assert!(dml_line.contains("..."));
        
        // Verify UTF-8 validity of the entire output
        assert!(std::str::from_utf8(output.as_bytes()).is_ok());
    }

    #[test]
    fn test_complex_expression_with_emoji_and_unicode() {
        // Test complex expression formatting with emoji and various Unicode using escape sequences
        use chryso_core::ast::Literal;
        
        let complex_expr = Expr::BinaryOp {
            left: Box::new(Expr::FunctionCall {
                name: "concat".to_string(),
                args: vec![
                    Expr::Literal(Literal::String("Hello \u{1f44b} ".to_string())),
                    Expr::Identifier("name".to_string()),
                    Expr::Literal(Literal::String(" \u{1f30d} \u{6b22}\u{8fce}\u{ff01}".to_string())),
                ],
            }),
            op: chryso_core::ast::BinaryOperator::Eq,
            right: Box::new(Expr::Literal(Literal::String("\u{4f60}\u{597d}\u{4e16}\u{754c} \u{1f389}".to_string()))),
        };
        
        // Test with very short truncation to force Unicode handling
        let config = ExplainConfig {
            show_types: false,
            show_costs: false,
            show_cardinality: false,
            compact: true,
            max_expr_length: 15,  // Very short to test Unicode boundaries
        };
        
        let formatter = ExplainFormatter::new(config);
        let formatted = formatter.format_expr(&complex_expr);
        
        // Verify UTF-8 validity
        assert!(std::str::from_utf8(formatted.as_bytes()).is_ok());
        
        // Should be truncated
        assert!(formatted.ends_with("..."));
        assert!(formatted.len() <= 15);
    }

    #[test]
    fn test_tree_structure_with_nested_left_subtree() {
        // Build a Join structure where left subtree has multiple levels, test if │ appears correctly
        // Join
        // ├── Filter (left subtree, not last)
        // │   └── Scan (users)
        // └── Scan (orders, right subtree, last)
        
        let left_subtree = LogicalPlan::Filter {
            predicate: Expr::BinaryOp {
                left: Box::new(Expr::Identifier("u.age".to_string())),
                op: chryso_core::ast::BinaryOperator::Gt,
                right: Box::new(Expr::Literal(chryso_core::ast::Literal::Number(18.0))),
            },
            input: Box::new(LogicalPlan::Scan {
                table: "users".to_string(),
            }),
        };
        
        let right_subtree = LogicalPlan::Scan {
            table: "orders".to_string(),
        };
        
        let join_plan = LogicalPlan::Join {
            join_type: chryso_core::ast::JoinType::Inner,
            left: Box::new(left_subtree),
            right: Box::new(right_subtree),
            on: Expr::BinaryOp {
                left: Box::new(Expr::Identifier("u.id".to_string())),
                op: chryso_core::ast::BinaryOperator::Eq,
                right: Box::new(Expr::Identifier("o.user_id".to_string())),
            },
        };
        
        let config = ExplainConfig {
            show_types: false,
            show_costs: false,
            show_cardinality: false,
            compact: false,
            max_expr_length: 80,
        };
        
        let formatter = ExplainFormatter::new(config);
        let output = formatter.format_logical_plan(&join_plan, &chryso_metadata::type_inference::SimpleTypeInferencer);
        
        // Verify tree structure correctness
        assert!(output.contains("LogicalJoin"));
        assert!(output.contains("LogicalFilter"));
        assert!(output.contains("LogicalScan: table=users"));
        assert!(output.contains("LogicalScan: table=orders"));
        
        // Key test: verify │ connection line exists on left subtree path
        // Expected structure:
        // LogicalJoin
        // └── LogicalFilter
        //     └── LogicalScan: table=users
        // └── LogicalScan: table=orders
        
        let lines: Vec<&str> = output.lines().collect();
        
        // Find Filter line (should be direct child of Join)
        let filter_line_idx = lines.iter().position(|line| line.contains("LogicalFilter")).unwrap();
        let filter_line = lines[filter_line_idx];
        
        // Find users scan line (child of Filter)
        let users_scan_line_idx = lines.iter().position(|line| line.contains("users")).unwrap();
        let users_scan_line = lines[users_scan_line_idx];
        
        // Verify connection lines
        // Filter line should start with ├── (it's the first child of Join, not last)
        assert!(filter_line.starts_with("├── "));
        
        // users scan line should start with │   └── (it's the only child of Filter, with proper indentation)
        assert!(users_scan_line.starts_with("│   └── "));
        
        // Verify depth consistency
        assert!(users_scan_line_idx > filter_line_idx);
        
        // Verify orders scan line (right child of Join, last one)
        let orders_scan_line_idx = lines.iter().position(|line| line.contains("orders")).unwrap();
        let orders_scan_line = lines[orders_scan_line_idx];
        
        // orders line should start with └── (it's the last child of Join)
        assert!(orders_scan_line.starts_with("└── "));
    }
}
