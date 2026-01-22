use crate::utils::{collect_identifiers, collect_tables, combine_conjuncts, split_conjuncts, table_prefix};
use corundum_core::ast::{BinaryOperator, Expr, Literal};
use corundum_planner::LogicalPlan;

pub trait Rule {
    fn name(&self) -> &str;
    fn apply(&self, plan: &LogicalPlan) -> Vec<LogicalPlan>;
}

pub struct RuleSet {
    rules: Vec<Box<dyn Rule + Send + Sync>>,
}

impl RuleSet {
    pub fn new() -> Self {
        Self { rules: Vec::new() }
    }

    pub fn with_rule(mut self, rule: impl Rule + Send + Sync + 'static) -> Self {
        self.rules.push(Box::new(rule));
        self
    }

    pub fn apply_all(&self, plan: &LogicalPlan) -> Vec<LogicalPlan> {
        let mut results = Vec::new();
        for rule in &self.rules {
            results.extend(rule.apply(plan));
        }
        results
    }

    pub fn iter(&self) -> impl Iterator<Item = &Box<dyn Rule + Send + Sync>> {
        self.rules.iter()
    }
}

impl Default for RuleSet {
    fn default() -> Self {
        RuleSet::new()
            .with_rule(MergeFilters)
            .with_rule(PruneProjection)
            .with_rule(MergeProjections)
            .with_rule(RemoveTrueFilter)
            .with_rule(FilterPushdown)
            .with_rule(FilterJoinPushdown)
            .with_rule(PredicateInference)
            .with_rule(JoinPredicatePushdown)
            .with_rule(FilterOrDedup)
            .with_rule(NormalizePredicates)
            .with_rule(JoinCommute)
            .with_rule(AggregatePredicatePushdown)
            .with_rule(LimitPushdown)
            .with_rule(TopNRule)
    }
}

impl RuleSet {
    pub fn detect_conflicts(&self) -> Vec<String> {
        let mut seen = std::collections::HashMap::new();
        for rule in &self.rules {
            *seen.entry(rule.name().to_string()).or_insert(0usize) += 1;
        }
        seen.into_iter()
            .filter_map(|(name, count)| if count > 1 { Some(name) } else { None })
            .collect()
    }
}

impl std::fmt::Debug for RuleSet {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RuleSet")
            .field("rule_count", &self.rules.len())
            .finish()
    }
}

pub struct NoopRule;

impl Rule for NoopRule {
    fn name(&self) -> &str {
        "noop"
    }

    fn apply(&self, _plan: &LogicalPlan) -> Vec<LogicalPlan> {
        Vec::new()
    }
}

pub struct MergeFilters;

impl Rule for MergeFilters {
    fn name(&self) -> &str {
        "merge_filters"
    }

    fn apply(&self, plan: &LogicalPlan) -> Vec<LogicalPlan> {
        let LogicalPlan::Filter { predicate, input } = plan else {
            return Vec::new();
        };
        let LogicalPlan::Filter {
            predicate: inner_predicate,
            input: inner_input,
        } = input.as_ref()
        else {
            return Vec::new();
        };
        let merged = LogicalPlan::Filter {
            predicate: corundum_core::ast::Expr::BinaryOp {
                left: Box::new(inner_predicate.clone()),
                op: BinaryOperator::And,
                right: Box::new(predicate.clone()),
            },
            input: inner_input.clone(),
        };
        vec![merged]
    }
}

pub struct PruneProjection;

impl Rule for PruneProjection {
    fn name(&self) -> &str {
        "prune_projection"
    }

    fn apply(&self, plan: &LogicalPlan) -> Vec<LogicalPlan> {
        let LogicalPlan::Projection { exprs, input } = plan else {
            return Vec::new();
        };
        if exprs.len() == 1 && matches!(exprs[0], Expr::Wildcard) {
            return vec![(*input.clone())];
        }
        Vec::new()
    }
}

pub struct MergeProjections;

impl Rule for MergeProjections {
    fn name(&self) -> &str {
        "merge_projections"
    }

    fn apply(&self, plan: &LogicalPlan) -> Vec<LogicalPlan> {
        let LogicalPlan::Projection { exprs, input } = plan else {
            return Vec::new();
        };
        let LogicalPlan::Projection {
            exprs: inner_exprs,
            input: inner_input,
        } = input.as_ref()
        else {
            return Vec::new();
        };
        if projection_subset(exprs, inner_exprs) {
            return vec![LogicalPlan::Projection {
                exprs: exprs.clone(),
                input: inner_input.clone(),
            }];
        }
        Vec::new()
    }
}

fn projection_subset(outer: &[Expr], inner: &[Expr]) -> bool {
    let inner_names = inner
        .iter()
        .filter_map(|expr| match expr {
            Expr::Identifier(name) => Some(name),
            _ => None,
        })
        .collect::<std::collections::HashSet<_>>();
    if inner_names.is_empty() {
        return false;
    }
    outer.iter().all(|expr| match expr {
        Expr::Identifier(name) => inner_names.contains(name),
        Expr::Wildcard => true,
        _ => false,
    })
}

pub struct RemoveTrueFilter;

impl Rule for RemoveTrueFilter {
    fn name(&self) -> &str {
        "remove_true_filter"
    }

    fn apply(&self, plan: &LogicalPlan) -> Vec<LogicalPlan> {
        let LogicalPlan::Filter { predicate, input } = plan else {
            return Vec::new();
        };
        match predicate {
            Expr::Literal(corundum_core::ast::Literal::Bool(true)) => vec![*input.clone()],
            _ => Vec::new(),
        }
    }
}

pub struct FilterPushdown;

impl Rule for FilterPushdown {
    fn name(&self) -> &str {
        "filter_pushdown"
    }

    fn apply(&self, plan: &LogicalPlan) -> Vec<LogicalPlan> {
        let LogicalPlan::Filter { predicate, input } = plan else {
            return Vec::new();
        };
        let LogicalPlan::Projection { exprs, input } = input.as_ref() else {
            if let LogicalPlan::Sort { order_by, input } = input.as_ref() {
                return vec![LogicalPlan::Sort {
                    order_by: order_by.clone(),
                    input: Box::new(LogicalPlan::Filter {
                        predicate: predicate.clone(),
                        input: input.clone(),
                    }),
                }];
            }
            return Vec::new();
        };
        if !projection_is_passthrough(exprs) {
            return Vec::new();
        }
        let pushed = LogicalPlan::Projection {
            exprs: exprs.clone(),
            input: Box::new(LogicalPlan::Filter {
                predicate: predicate.clone(),
                input: input.clone(),
            }),
        };
        vec![pushed]
    }
}

pub struct FilterJoinPushdown;

impl Rule for FilterJoinPushdown {
    fn name(&self) -> &str {
        "filter_join_pushdown"
    }

    fn apply(&self, plan: &LogicalPlan) -> Vec<LogicalPlan> {
        let LogicalPlan::Filter { predicate, input } = plan else {
            return Vec::new();
        };
        let LogicalPlan::Join {
            join_type,
            left,
            right,
            on,
        } = input.as_ref()
        else {
            return Vec::new();
        };
        if !matches!(join_type, corundum_core::ast::JoinType::Inner) {
            return Vec::new();
        }

        let left_tables = collect_tables(left.as_ref());
        let right_tables = collect_tables(right.as_ref());
        let mut left_preds = Vec::new();
        let mut right_preds = Vec::new();
        let mut remaining = Vec::new();

        for conjunct in split_conjuncts(predicate) {
            let idents = collect_identifiers(&conjunct);
            if idents.is_empty() {
                remaining.push(conjunct);
                continue;
            }
            let mut side = None;
            let mut ambiguous = false;
            for ident in &idents {
                let Some(prefix) = table_prefix(ident) else {
                    ambiguous = true;
                    break;
                };
                let in_left = left_tables.contains(prefix);
                let in_right = right_tables.contains(prefix);
                if in_left && !in_right {
                    side = match side {
                        None => Some(Side::Left),
                        Some(Side::Left) => Some(Side::Left),
                        Some(Side::Right) => {
                            ambiguous = true;
                            break;
                        }
                    };
                } else if in_right && !in_left {
                    side = match side {
                        None => Some(Side::Right),
                        Some(Side::Right) => Some(Side::Right),
                        Some(Side::Left) => {
                            ambiguous = true;
                            break;
                        }
                    };
                } else {
                    ambiguous = true;
                    break;
                }
            }
            if ambiguous {
                remaining.push(conjunct);
                continue;
            }
            match side {
                Some(Side::Left) => left_preds.push(conjunct),
                Some(Side::Right) => right_preds.push(conjunct),
                None => remaining.push(conjunct),
            }
        }

        if left_preds.is_empty() && right_preds.is_empty() && remaining.is_empty() {
            return Vec::new();
        }

        let new_left = if let Some(expr) = combine_conjuncts(left_preds) {
            LogicalPlan::Filter {
                predicate: expr,
                input: left.clone(),
            }
        } else {
            *left.clone()
        };
        let new_right = if let Some(expr) = combine_conjuncts(right_preds) {
            LogicalPlan::Filter {
                predicate: expr,
                input: right.clone(),
            }
        } else {
            *right.clone()
        };
        let new_on = if let Some(expr) = combine_conjuncts(remaining) {
            Expr::BinaryOp {
                left: Box::new(on.clone()),
                op: BinaryOperator::And,
                right: Box::new(expr),
            }
        } else {
            on.clone()
        };
        let joined = LogicalPlan::Join {
            join_type: *join_type,
            left: Box::new(new_left),
            right: Box::new(new_right),
            on: new_on,
        };
        vec![joined]
    }
}

pub struct JoinPredicatePushdown;

impl Rule for JoinPredicatePushdown {
    fn name(&self) -> &str {
        "join_predicate_pushdown"
    }

    fn apply(&self, plan: &LogicalPlan) -> Vec<LogicalPlan> {
        let LogicalPlan::Join {
            join_type,
            left,
            right,
            on,
        } = plan
        else {
            return Vec::new();
        };
        if !matches!(join_type, corundum_core::ast::JoinType::Inner) {
            return Vec::new();
        }

        let left_tables = collect_tables(left.as_ref());
        let right_tables = collect_tables(right.as_ref());
        let mut left_preds = Vec::new();
        let mut right_preds = Vec::new();
        let mut remaining = Vec::new();

        for conjunct in split_conjuncts(on) {
            let idents = collect_identifiers(&conjunct);
            if idents.is_empty() {
                remaining.push(conjunct);
                continue;
            }
            let mut side = None;
            let mut ambiguous = false;
            for ident in &idents {
                let Some(prefix) = table_prefix(ident) else {
                    ambiguous = true;
                    break;
                };
                let in_left = left_tables.contains(prefix);
                let in_right = right_tables.contains(prefix);
                if in_left && !in_right {
                    side = match side {
                        None => Some(Side::Left),
                        Some(Side::Left) => Some(Side::Left),
                        Some(Side::Right) => {
                            ambiguous = true;
                            break;
                        }
                    };
                } else if in_right && !in_left {
                    side = match side {
                        None => Some(Side::Right),
                        Some(Side::Right) => Some(Side::Right),
                        Some(Side::Left) => {
                            ambiguous = true;
                            break;
                        }
                    };
                } else {
                    ambiguous = true;
                    break;
                }
            }
            if ambiguous {
                remaining.push(conjunct);
                continue;
            }
            match side {
                Some(Side::Left) => left_preds.push(conjunct),
                Some(Side::Right) => right_preds.push(conjunct),
                None => remaining.push(conjunct),
            }
        }

        if left_preds.is_empty() && right_preds.is_empty() {
            return Vec::new();
        }

        let new_left = if let Some(expr) = combine_conjuncts(left_preds) {
            LogicalPlan::Filter {
                predicate: expr,
                input: left.clone(),
            }
        } else {
            *left.clone()
        };
        let new_right = if let Some(expr) = combine_conjuncts(right_preds) {
            LogicalPlan::Filter {
                predicate: expr,
                input: right.clone(),
            }
        } else {
            *right.clone()
        };
        let new_on =
            combine_conjuncts(remaining).unwrap_or_else(|| Expr::Literal(Literal::Bool(true)));
        vec![LogicalPlan::Join {
            join_type: *join_type,
            left: Box::new(new_left),
            right: Box::new(new_right),
            on: new_on,
        }]
    }
}

fn projection_is_passthrough(exprs: &[Expr]) -> bool {
    exprs.iter().all(|expr| match expr {
        Expr::Identifier(_) | Expr::Wildcard => true,
        _ => false,
    })
}

pub struct NormalizePredicates;

impl Rule for NormalizePredicates {
    fn name(&self) -> &str {
        "normalize_predicates"
    }

    fn apply(&self, plan: &LogicalPlan) -> Vec<LogicalPlan> {
        let LogicalPlan::Filter { predicate, input } = plan else {
            return Vec::new();
        };
        let normalized = predicate.normalize();
        if normalized.to_sql() == predicate.to_sql() {
            return Vec::new();
        }
        vec![LogicalPlan::Filter {
            predicate: normalized,
            input: input.clone(),
        }]
    }
}

pub struct PredicateInference;

impl Rule for PredicateInference {
    fn name(&self) -> &str {
        "predicate_inference"
    }

    fn apply(&self, plan: &LogicalPlan) -> Vec<LogicalPlan> {
        match plan {
            LogicalPlan::Filter {
                predicate,
                input,
            } => match input.as_ref() {
                LogicalPlan::Join {
                    join_type,
                    left,
                    right,
                    on,
                } if matches!(join_type, corundum_core::ast::JoinType::Inner) => {
                    let combined = Expr::BinaryOp {
                        left: Box::new(predicate.clone()),
                        op: BinaryOperator::And,
                        right: Box::new(on.clone()),
                    };
                    let (inferred, changed) = infer_predicates(&combined);
                    if !changed {
                        return Vec::new();
                    }
                    let (filter_predicates, join_predicates) =
                        split_predicates_by_source(&inferred, predicate, on);
                    let join_expr = combine_conjuncts(join_predicates)
                        .unwrap_or_else(|| Expr::Literal(Literal::Bool(true)));
                    let join_plan = LogicalPlan::Join {
                        join_type: *join_type,
                        left: left.clone(),
                        right: right.clone(),
                        on: join_expr,
                    };
                    let filter_expr = combine_conjuncts(filter_predicates)
                        .unwrap_or_else(|| Expr::Literal(Literal::Bool(true)));
                    vec![LogicalPlan::Filter {
                        predicate: filter_expr,
                        input: Box::new(join_plan),
                    }]
                }
                _ => {
                    let (predicate, changed) = infer_predicates(predicate);
                    if !changed {
                        return Vec::new();
                    }
                    vec![LogicalPlan::Filter {
                        predicate,
                        input: input.clone(),
                    }]
                }
            },
            LogicalPlan::Join {
                join_type,
                left,
                right,
                on,
            } if matches!(join_type, corundum_core::ast::JoinType::Inner) => {
                let (on, changed) = infer_predicates(on);
                if !changed {
                    return Vec::new();
                }
                vec![LogicalPlan::Join {
                    join_type: *join_type,
                    left: left.clone(),
                    right: right.clone(),
                    on,
                }]
            }
            _ => Vec::new(),
        }
    }
}

pub struct JoinCommute;

impl Rule for JoinCommute {
    fn name(&self) -> &str {
        "join_commute"
    }

    fn apply(&self, plan: &LogicalPlan) -> Vec<LogicalPlan> {
        let LogicalPlan::Join {
            join_type,
            left,
            right,
            on,
        } = plan
        else {
            return Vec::new();
        };
        if !matches!(join_type, corundum_core::ast::JoinType::Inner) {
            return Vec::new();
        }
        vec![LogicalPlan::Join {
            join_type: *join_type,
            left: right.clone(),
            right: left.clone(),
            on: on.clone(),
        }]
    }
}

pub struct FilterOrDedup;

impl Rule for FilterOrDedup {
    fn name(&self) -> &str {
        "filter_or_dedup"
    }

    fn apply(&self, plan: &LogicalPlan) -> Vec<LogicalPlan> {
        let LogicalPlan::Filter { predicate, input } = plan else {
            return Vec::new();
        };
        let Expr::BinaryOp { left, op, right } = predicate else {
            return Vec::new();
        };
        if !matches!(op, BinaryOperator::Or) {
            return Vec::new();
        }
        if left.structural_eq(right) {
            return vec![LogicalPlan::Filter {
                predicate: (*left.clone()),
                input: input.clone(),
            }];
        }
        Vec::new()
    }
}

pub struct LimitPushdown;

impl Rule for LimitPushdown {
    fn name(&self) -> &str {
        "limit_pushdown"
    }

    fn apply(&self, plan: &LogicalPlan) -> Vec<LogicalPlan> {
        let LogicalPlan::Limit {
            limit,
            offset,
            input,
        } = plan
        else {
            return Vec::new();
        };
        if offset.is_some() {
            return Vec::new();
        }
        let inner = input.as_ref();
        match inner {
            LogicalPlan::Filter { predicate, input } => vec![LogicalPlan::Filter {
                predicate: predicate.clone(),
                input: Box::new(LogicalPlan::Limit {
                    limit: *limit,
                    offset: *offset,
                    input: input.clone(),
                }),
            }],
            LogicalPlan::Projection { exprs, input } => vec![LogicalPlan::Projection {
                exprs: exprs.clone(),
                input: Box::new(LogicalPlan::Limit {
                    limit: *limit,
                    offset: *offset,
                    input: input.clone(),
                }),
            }],
            _ => Vec::new(),
        }
    }
}

pub struct TopNRule;

impl Rule for TopNRule {
    fn name(&self) -> &str {
        "topn_rule"
    }

    fn apply(&self, plan: &LogicalPlan) -> Vec<LogicalPlan> {
        let LogicalPlan::Limit {
            limit: Some(limit),
            offset: None,
            input,
        } = plan
        else {
            return Vec::new();
        };
        let LogicalPlan::Sort { order_by, input } = input.as_ref() else {
            return Vec::new();
        };
        vec![LogicalPlan::TopN {
            order_by: order_by.clone(),
            limit: *limit,
            input: input.clone(),
        }]
    }
}

#[cfg(test)]
mod tests {
    use super::{
        FilterJoinPushdown, FilterOrDedup, FilterPushdown, JoinPredicatePushdown, LimitPushdown,
        MergeFilters, MergeProjections, NormalizePredicates, PredicateInference, PruneProjection,
        RemoveTrueFilter, Rule, TopNRule,
    };
    use crate::utils::split_conjuncts;
    use corundum_core::ast::{BinaryOperator, Expr};
    use corundum_planner::LogicalPlan;

    #[test]
    fn merge_filters_combines_predicates() {
        let plan = LogicalPlan::Filter {
            predicate: Expr::Identifier("a".to_string()),
            input: Box::new(LogicalPlan::Filter {
                predicate: Expr::Identifier("b".to_string()),
                input: Box::new(LogicalPlan::Scan {
                    table: "t".to_string(),
                }),
            }),
        };
        let rule = MergeFilters;
        let results = rule.apply(&plan);
        assert_eq!(results.len(), 1);
        let LogicalPlan::Filter { predicate, .. } = &results[0] else {
            panic!("expected filter");
        };
        let Expr::BinaryOp { op, .. } = predicate else {
            panic!("expected binary op");
        };
        assert!(matches!(op, BinaryOperator::And));
    }

    #[test]
    fn prune_projection_removes_star() {
        let plan = LogicalPlan::Projection {
            exprs: vec![Expr::Wildcard],
            input: Box::new(LogicalPlan::Scan {
                table: "t".to_string(),
            }),
        };
        let rule = PruneProjection;
        let results = rule.apply(&plan);
        assert_eq!(results.len(), 1);
    }

    #[test]
    fn filter_pushdown_under_projection() {
        let plan = LogicalPlan::Filter {
            predicate: Expr::Identifier("x".to_string()),
            input: Box::new(LogicalPlan::Projection {
                exprs: vec![Expr::Identifier("x".to_string())],
                input: Box::new(LogicalPlan::Scan {
                    table: "t".to_string(),
                }),
            }),
        };
        let rule = FilterPushdown;
        let results = rule.apply(&plan);
        assert_eq!(results.len(), 1);
    }

    #[test]
    fn filter_pushdown_under_sort() {
        let plan = LogicalPlan::Filter {
            predicate: Expr::Identifier("x".to_string()),
            input: Box::new(LogicalPlan::Sort {
                order_by: vec![corundum_core::ast::OrderByExpr {
                    expr: Expr::Identifier("id".to_string()),
                    asc: true,
                    nulls_first: None,
                }],
                input: Box::new(LogicalPlan::Scan {
                    table: "t".to_string(),
                }),
            }),
        };
        let rule = FilterPushdown;
        let results = rule.apply(&plan);
        assert_eq!(results.len(), 1);
        let LogicalPlan::Sort { input, .. } = &results[0] else {
            panic!("expected sort");
        };
        assert!(matches!(input.as_ref(), LogicalPlan::Filter { .. }));
    }

    #[test]
    fn normalize_predicates_orders_and() {
        let plan = LogicalPlan::Filter {
            predicate: Expr::BinaryOp {
                left: Box::new(Expr::Identifier("b".to_string())),
                op: BinaryOperator::And,
                right: Box::new(Expr::Identifier("a".to_string())),
            },
            input: Box::new(LogicalPlan::Scan {
                table: "t".to_string(),
            }),
        };
        let rule = NormalizePredicates;
        let results = rule.apply(&plan);
        assert_eq!(results.len(), 1);
    }

    #[test]
    fn detect_rule_conflicts() {
        let rules = crate::rules::RuleSet::new()
            .with_rule(MergeFilters)
            .with_rule(MergeFilters);
        let conflicts = rules.detect_conflicts();
        assert_eq!(conflicts, vec!["merge_filters".to_string()]);
    }

    #[test]
    fn topn_rule_rewrites_sort_limit() {
        let plan = LogicalPlan::Limit {
            limit: Some(10),
            offset: None,
            input: Box::new(LogicalPlan::Sort {
                order_by: vec![corundum_core::ast::OrderByExpr {
                    expr: Expr::Identifier("id".to_string()),
                    asc: true,
                    nulls_first: None,
                }],
                input: Box::new(LogicalPlan::Scan {
                    table: "t".to_string(),
                }),
            }),
        };
        let rule = TopNRule;
        let results = rule.apply(&plan);
        assert_eq!(results.len(), 1);
    }

    #[test]
    fn limit_pushdown_under_projection() {
        let plan = LogicalPlan::Limit {
            limit: Some(5),
            offset: None,
            input: Box::new(LogicalPlan::Projection {
                exprs: vec![Expr::Identifier("id".to_string())],
                input: Box::new(LogicalPlan::Scan {
                    table: "t".to_string(),
                }),
            }),
        };
        let rule = LimitPushdown;
        let results = rule.apply(&plan);
        assert_eq!(results.len(), 1);
    }

    #[test]
    fn merge_projections_keeps_outer() {
        let plan = LogicalPlan::Projection {
            exprs: vec![Expr::Identifier("id".to_string())],
            input: Box::new(LogicalPlan::Projection {
                exprs: vec![Expr::Identifier("id".to_string()), Expr::Identifier("name".to_string())],
                input: Box::new(LogicalPlan::Scan {
                    table: "t".to_string(),
                }),
            }),
        };
        let rule = MergeProjections;
        let results = rule.apply(&plan);
        assert_eq!(results.len(), 1);
        let LogicalPlan::Projection { exprs, .. } = &results[0] else {
            panic!("expected projection");
        };
        assert_eq!(exprs.len(), 1);
    }

    #[test]
    fn remove_true_filter() {
        let plan = LogicalPlan::Filter {
            predicate: Expr::Literal(corundum_core::ast::Literal::Bool(true)),
            input: Box::new(LogicalPlan::Scan {
                table: "t".to_string(),
            }),
        };
        let rule = RemoveTrueFilter;
        let results = rule.apply(&plan);
        assert_eq!(results.len(), 1);
        assert!(matches!(results[0], LogicalPlan::Scan { .. }));
    }

    #[test]
    fn remove_true_filter_ignores_binary_predicate() {
        let plan = LogicalPlan::Filter {
            predicate: Expr::BinaryOp {
                left: Box::new(Expr::Literal(corundum_core::ast::Literal::Bool(true))),
                op: BinaryOperator::And,
                right: Box::new(Expr::Identifier("x".to_string())),
            },
            input: Box::new(LogicalPlan::Scan {
                table: "t".to_string(),
            }),
        };
        let rule = RemoveTrueFilter;
        let results = rule.apply(&plan);
        assert!(results.is_empty());
    }

    #[test]
    fn filter_join_pushdown_left() {
        let plan = LogicalPlan::Filter {
            predicate: Expr::BinaryOp {
                left: Box::new(Expr::Identifier("t1.id".to_string())),
                op: BinaryOperator::Eq,
                right: Box::new(Expr::Literal(corundum_core::ast::Literal::Number(1.0))),
            },
            input: Box::new(LogicalPlan::Join {
                join_type: corundum_core::ast::JoinType::Inner,
                left: Box::new(LogicalPlan::Scan {
                    table: "t1".to_string(),
                }),
                right: Box::new(LogicalPlan::Scan {
                    table: "t2".to_string(),
                }),
                on: Expr::Identifier("t1.id = t2.id".to_string()),
            }),
        };
        let rule = FilterJoinPushdown;
        let results = rule.apply(&plan);
        assert_eq!(results.len(), 1);
        let LogicalPlan::Join { left, .. } = &results[0] else {
            panic!("expected join");
        };
        assert!(matches!(left.as_ref(), LogicalPlan::Filter { .. }));
    }

    #[test]
    fn filter_join_pushdown_keeps_cross_predicate() {
        let plan = LogicalPlan::Filter {
            predicate: Expr::BinaryOp {
                left: Box::new(Expr::Identifier("t1.id".to_string())),
                op: BinaryOperator::Eq,
                right: Box::new(Expr::Identifier("t2.id".to_string())),
            },
            input: Box::new(LogicalPlan::Join {
                join_type: corundum_core::ast::JoinType::Inner,
                left: Box::new(LogicalPlan::Scan {
                    table: "t1".to_string(),
                }),
                right: Box::new(LogicalPlan::Scan {
                    table: "t2".to_string(),
                }),
                on: Expr::Identifier("t1.id = t2.id".to_string()),
            }),
        };
        let rule = FilterJoinPushdown;
        let results = rule.apply(&plan);
        assert_eq!(results.len(), 1);
        let LogicalPlan::Join { on, .. } = &results[0] else {
            panic!("expected join");
        };
        let Expr::BinaryOp { op, .. } = on else {
            panic!("expected binary op");
        };
        assert!(matches!(op, BinaryOperator::And));
    }

    #[test]
    fn filter_or_dedup_removes_duplicate_predicates() {
        let plan = LogicalPlan::Filter {
            predicate: Expr::BinaryOp {
                left: Box::new(Expr::Identifier("a".to_string())),
                op: BinaryOperator::Or,
                right: Box::new(Expr::Identifier("a".to_string())),
            },
            input: Box::new(LogicalPlan::Scan {
                table: "t".to_string(),
            }),
        };
        let rule = FilterOrDedup;
        let results = rule.apply(&plan);
        assert_eq!(results.len(), 1);
        let LogicalPlan::Filter { predicate, .. } = &results[0] else {
            panic!("expected filter");
        };
        assert!(matches!(predicate, Expr::Identifier(name) if name == "a"));
    }

    #[test]
    fn join_predicate_pushdown_splits_single_side() {
        let plan = LogicalPlan::Join {
            join_type: corundum_core::ast::JoinType::Inner,
            left: Box::new(LogicalPlan::Scan {
                table: "t1".to_string(),
            }),
            right: Box::new(LogicalPlan::Scan {
                table: "t2".to_string(),
            }),
            on: Expr::BinaryOp {
                left: Box::new(Expr::BinaryOp {
                    left: Box::new(Expr::Identifier("t1.flag".to_string())),
                    op: BinaryOperator::Eq,
                    right: Box::new(Expr::Literal(corundum_core::ast::Literal::Bool(true))),
                }),
                op: BinaryOperator::And,
                right: Box::new(Expr::BinaryOp {
                    left: Box::new(Expr::Identifier("t1.id".to_string())),
                    op: BinaryOperator::Eq,
                    right: Box::new(Expr::Identifier("t2.id".to_string())),
                }),
            },
        };
        let rule = JoinPredicatePushdown;
        let results = rule.apply(&plan);
        assert_eq!(results.len(), 1);
        let LogicalPlan::Join { left, on, .. } = &results[0] else {
            panic!("expected join");
        };
        assert!(matches!(left.as_ref(), LogicalPlan::Filter { .. }));
        assert_eq!(on.to_sql(), "t1.id = t2.id");
    }

    #[test]
    fn predicate_inference_adds_literal_equivalence() {
        let plan = LogicalPlan::Filter {
            predicate: Expr::BinaryOp {
                left: Box::new(Expr::BinaryOp {
                    left: Box::new(Expr::Identifier("a".to_string())),
                    op: BinaryOperator::Eq,
                    right: Box::new(Expr::Identifier("b".to_string())),
                }),
                op: BinaryOperator::And,
                right: Box::new(Expr::BinaryOp {
                    left: Box::new(Expr::Identifier("a".to_string())),
                    op: BinaryOperator::Eq,
                    right: Box::new(Expr::Literal(corundum_core::ast::Literal::Number(1.0))),
                }),
            },
            input: Box::new(LogicalPlan::Scan {
                table: "t".to_string(),
            }),
        };
        let rule = PredicateInference;
        let results = rule.apply(&plan);
        assert_eq!(results.len(), 1);
        let LogicalPlan::Filter { predicate, .. } = &results[0] else {
            panic!("expected filter");
        };
        let conjuncts = split_conjuncts(predicate)
            .into_iter()
            .map(|expr| expr.to_sql())
            .collect::<std::collections::HashSet<_>>();
        assert!(conjuncts.contains("b = 1"));
    }

    #[test]
    fn predicate_inference_on_join() {
        let plan = LogicalPlan::Join {
            join_type: corundum_core::ast::JoinType::Inner,
            left: Box::new(LogicalPlan::Scan {
                table: "t1".to_string(),
            }),
            right: Box::new(LogicalPlan::Scan {
                table: "t2".to_string(),
            }),
            on: Expr::BinaryOp {
                left: Box::new(Expr::BinaryOp {
                    left: Box::new(Expr::Identifier("t1.id".to_string())),
                    op: BinaryOperator::Eq,
                    right: Box::new(Expr::Identifier("t2.id".to_string())),
                }),
                op: BinaryOperator::And,
                right: Box::new(Expr::BinaryOp {
                    left: Box::new(Expr::Identifier("t2.id".to_string())),
                    op: BinaryOperator::Eq,
                    right: Box::new(Expr::Literal(corundum_core::ast::Literal::Number(5.0))),
                }),
            },
        };
        let rule = PredicateInference;
        let results = rule.apply(&plan);
        assert_eq!(results.len(), 1);
        let LogicalPlan::Join { on, .. } = &results[0] else {
            panic!("expected join");
        };
        let conjuncts = split_conjuncts(on)
            .into_iter()
            .map(|expr| expr.to_sql())
            .collect::<std::collections::HashSet<_>>();
        assert!(conjuncts.contains("t1.id = 5"));
    }

    #[test]
    fn predicate_inference_enables_join_pushdown() {
        let plan = LogicalPlan::Join {
            join_type: corundum_core::ast::JoinType::Inner,
            left: Box::new(LogicalPlan::Scan {
                table: "t1".to_string(),
            }),
            right: Box::new(LogicalPlan::Scan {
                table: "t2".to_string(),
            }),
            on: Expr::BinaryOp {
                left: Box::new(Expr::BinaryOp {
                    left: Box::new(Expr::Identifier("t1.id".to_string())),
                    op: BinaryOperator::Eq,
                    right: Box::new(Expr::Identifier("t2.id".to_string())),
                }),
                op: BinaryOperator::And,
                right: Box::new(Expr::BinaryOp {
                    left: Box::new(Expr::Identifier("t1.id".to_string())),
                    op: BinaryOperator::Eq,
                    right: Box::new(Expr::Literal(corundum_core::ast::Literal::Number(42.0))),
                }),
            },
        };
        let inferred = PredicateInference.apply(&plan);
        assert_eq!(inferred.len(), 1);
        let pushed = JoinPredicatePushdown.apply(&inferred[0]);
        assert_eq!(pushed.len(), 1);
        let LogicalPlan::Join { right, .. } = &pushed[0] else {
            panic!("expected join");
        };
        assert!(matches!(right.as_ref(), LogicalPlan::Filter { .. }));
    }

    #[test]
    fn predicate_inference_propagates_across_filter_join() {
        let plan = LogicalPlan::Filter {
            predicate: Expr::BinaryOp {
                left: Box::new(Expr::Identifier("t1.flag".to_string())),
                op: BinaryOperator::Eq,
                right: Box::new(Expr::Literal(corundum_core::ast::Literal::Bool(true))),
            },
            input: Box::new(LogicalPlan::Join {
                join_type: corundum_core::ast::JoinType::Inner,
                left: Box::new(LogicalPlan::Scan {
                    table: "t1".to_string(),
                }),
                right: Box::new(LogicalPlan::Scan {
                    table: "t2".to_string(),
                }),
                on: Expr::BinaryOp {
                    left: Box::new(Expr::BinaryOp {
                        left: Box::new(Expr::Identifier("t1.id".to_string())),
                        op: BinaryOperator::Eq,
                        right: Box::new(Expr::Identifier("t2.id".to_string())),
                    }),
                    op: BinaryOperator::And,
                    right: Box::new(Expr::BinaryOp {
                        left: Box::new(Expr::Identifier("t1.id".to_string())),
                        op: BinaryOperator::Eq,
                        right: Box::new(Expr::Literal(corundum_core::ast::Literal::Number(7.0))),
                    }),
                },
            }),
        };
        let inferred = PredicateInference.apply(&plan);
        assert_eq!(inferred.len(), 1);
        let LogicalPlan::Filter { input, predicate } = &inferred[0] else {
            panic!("expected filter");
        };
        let LogicalPlan::Join { on, .. } = input.as_ref() else {
            panic!("expected join");
        };
        let filter_conjuncts = split_conjuncts(predicate)
            .into_iter()
            .map(|expr| expr.to_sql())
            .collect::<std::collections::HashSet<_>>();
        let join_conjuncts = split_conjuncts(on)
            .into_iter()
            .map(|expr| expr.to_sql())
            .collect::<std::collections::HashSet<_>>();
        assert!(filter_conjuncts.contains("t1.flag = true"));
        assert!(join_conjuncts.contains("t2.id = 7"));
    }

    #[test]
    fn predicate_inference_adds_transitive_equivalence() {
        let plan = LogicalPlan::Filter {
            predicate: Expr::BinaryOp {
                left: Box::new(Expr::BinaryOp {
                    left: Box::new(Expr::Identifier("a".to_string())),
                    op: BinaryOperator::Eq,
                    right: Box::new(Expr::Identifier("b".to_string())),
                }),
                op: BinaryOperator::And,
                right: Box::new(Expr::BinaryOp {
                    left: Box::new(Expr::Identifier("b".to_string())),
                    op: BinaryOperator::Eq,
                    right: Box::new(Expr::Identifier("c".to_string())),
                }),
            },
            input: Box::new(LogicalPlan::Scan {
                table: "t".to_string(),
            }),
        };
        let rule = PredicateInference;
        let results = rule.apply(&plan);
        assert_eq!(results.len(), 1);
        let LogicalPlan::Filter { predicate, .. } = &results[0] else {
            panic!("expected filter");
        };
        let conjuncts = split_conjuncts(predicate)
            .into_iter()
            .map(|expr| expr.to_sql())
            .collect::<std::collections::HashSet<_>>();
        assert!(conjuncts.contains("a = c") || conjuncts.contains("c = a"));
    }
}

pub struct AggregatePredicatePushdown;

impl Rule for AggregatePredicatePushdown {
    fn name(&self) -> &str {
        "aggregate_predicate_pushdown"
    }

    fn apply(&self, plan: &LogicalPlan) -> Vec<LogicalPlan> {
        let LogicalPlan::Filter { predicate, input } = plan else {
            return Vec::new();
        };
        let LogicalPlan::Aggregate {
            group_exprs,
            aggr_exprs,
            input,
        } = input.as_ref()
        else {
            return Vec::new();
        };
        let predicate_idents = collect_identifiers(predicate);
        let group_idents = group_exprs
            .iter()
            .flat_map(collect_identifiers)
            .collect::<std::collections::HashSet<_>>();
        if predicate_idents
            .iter()
            .all(|ident| group_idents.contains(ident))
        {
            return vec![LogicalPlan::Aggregate {
                group_exprs: group_exprs.clone(),
                aggr_exprs: aggr_exprs.clone(),
                input: Box::new(LogicalPlan::Filter {
                    predicate: predicate.clone(),
                    input: input.clone(),
                }),
            }];
        }
        Vec::new()
    }
}

#[derive(Clone, Copy)]
enum Side {
    Left,
    Right,
}

// shared helpers are in utils.rs

fn infer_predicates(predicate: &Expr) -> (Expr, bool) {
    let conjuncts = split_conjuncts(predicate);
    let mut existing = std::collections::HashSet::new();
    for expr in &conjuncts {
        existing.insert(expr.to_sql());
    }

    let mut uf = UnionFind::new();
    let mut literal_bindings = std::collections::HashMap::<String, Literal>::new();
    let mut literal_conflicts = std::collections::HashSet::<String>::new();

    for conjunct in &conjuncts {
        let Expr::BinaryOp { left, op, right } = conjunct else {
            continue;
        };
        if !matches!(op, BinaryOperator::Eq) {
            continue;
        }
        match (left.as_ref(), right.as_ref()) {
            (Expr::Identifier(left), Expr::Identifier(right)) => {
                uf.union(left, right);
            }
            (Expr::Identifier(ident), Expr::Literal(literal))
            | (Expr::Literal(literal), Expr::Identifier(ident)) => {
                uf.add(ident);
                if let Some(existing) = literal_bindings.get(ident) {
                    if !literal_eq(existing, literal) {
                        literal_conflicts.insert(ident.clone());
                    }
                } else {
                    literal_bindings.insert(ident.clone(), literal.clone());
                }
            }
            _ => {}
        }
    }

    let mut class_literals = std::collections::HashMap::<String, Option<Literal>>::new();
    if !literal_conflicts.is_empty() {
        // TODO: surface conflicting literal bindings in optimizer trace.
    }
    for ident in &literal_conflicts {
        let root = uf.find(ident);
        class_literals.insert(root, None);
    }
    for (ident, literal) in &literal_bindings {
        if literal_conflicts.contains(ident) {
            continue;
        }
        let root = uf.find(ident);
        match class_literals.get(&root) {
            None => {
                class_literals.insert(root, Some(literal.clone()));
            }
            Some(Some(existing_literal)) => {
                if !literal_eq(existing_literal, literal) {
                    class_literals.insert(root, None);
                }
            }
            Some(None) => {}
        }
    }

    let mut groups = std::collections::HashMap::<String, Vec<String>>::new();
    for key in uf.keys() {
        let root = uf.find(&key);
        groups.entry(root).or_default().push(key);
    }

    let mut inferred = Vec::new();
    for members in groups.values() {
        if members.len() <= 1 {
            continue;
        }
        let mut members = members.clone();
        members.sort();
        let canonical = members[0].clone();
        for ident in members.into_iter().skip(1) {
            let forward = format!("{canonical} = {ident}");
            let backward = format!("{ident} = {canonical}");
            if existing.contains(&forward) || existing.contains(&backward) {
                continue;
            }
            existing.insert(forward);
            inferred.push(Expr::BinaryOp {
                left: Box::new(Expr::Identifier(canonical.clone())),
                op: BinaryOperator::Eq,
                right: Box::new(Expr::Identifier(ident)),
            });
        }
    }

    for (root, literal) in class_literals {
        let Some(literal) = literal else {
            continue;
        };
        let Some(members) = groups.get(&root) else {
            continue;
        };
        for ident in members {
            let expr = Expr::BinaryOp {
                left: Box::new(Expr::Identifier(ident.clone())),
                op: BinaryOperator::Eq,
                right: Box::new(Expr::Literal(literal.clone())),
            };
            if existing.insert(expr.to_sql()) {
                inferred.push(expr);
            }
        }
    }

    if inferred.is_empty() {
        return (predicate.clone(), false);
    }
    let mut all = conjuncts;
    all.extend(inferred);
    (combine_conjuncts(all).unwrap_or_else(|| predicate.clone()), true)
}

fn split_predicates_by_source(
    combined: &Expr,
    filter_predicate: &Expr,
    join_predicate: &Expr,
) -> (Vec<Expr>, Vec<Expr>) {
    let filter_set = split_conjuncts(filter_predicate)
        .into_iter()
        .map(|expr| expr.to_sql())
        .collect::<std::collections::HashSet<_>>();
    let join_set = split_conjuncts(join_predicate)
        .into_iter()
        .map(|expr| expr.to_sql())
        .collect::<std::collections::HashSet<_>>();
    let filter_prefixes = collect_table_prefixes(filter_predicate);
    let mut filter_preds = Vec::new();
    let mut join_preds = Vec::new();
    for expr in split_conjuncts(combined) {
        let sql = expr.to_sql();
        if join_set.contains(&sql) {
            join_preds.push(expr);
        } else if filter_set.contains(&sql) {
            filter_preds.push(expr);
        } else if is_join_compatible(&expr) {
            join_preds.push(expr);
        } else if is_same_table_predicate(&expr, &filter_prefixes) {
            filter_preds.push(expr);
        } else {
            join_preds.push(expr);
        }
    }
    (filter_preds, join_preds)
}

fn is_join_compatible(expr: &Expr) -> bool {
    let idents = collect_identifiers(expr);
    if idents.len() < 2 {
        return false;
    }
    let mut tables = std::collections::HashSet::new();
    for ident in idents {
        if let Some(prefix) = table_prefix(&ident) {
            tables.insert(prefix.to_string());
        }
    }
    tables.len() >= 2
}

fn collect_table_prefixes(expr: &Expr) -> std::collections::HashSet<String> {
    let idents = collect_identifiers(expr);
    let mut tables = std::collections::HashSet::new();
    for ident in idents {
        if let Some(prefix) = table_prefix(&ident) {
            tables.insert(prefix.to_string());
        }
    }
    tables
}

fn is_same_table_predicate(expr: &Expr, known_tables: &std::collections::HashSet<String>) -> bool {
    let idents = collect_identifiers(expr);
    if idents.is_empty() {
        return false;
    }
    let mut tables = std::collections::HashSet::new();
    for ident in idents {
        let Some(prefix) = table_prefix(&ident) else {
            return false;
        };
        tables.insert(prefix.to_string());
    }
    if tables.len() != 1 {
        return false;
    }
    if known_tables.is_empty() {
        return false;
    }
    tables.is_subset(known_tables)
}

fn literal_eq(left: &Literal, right: &Literal) -> bool {
    match (left, right) {
        (Literal::String(left), Literal::String(right)) => left == right,
        (Literal::Number(left), Literal::Number(right)) => left == right,
        (Literal::Bool(left), Literal::Bool(right)) => left == right,
        _ => false,
    }
}

struct UnionFind {
    parent: std::collections::HashMap<String, String>,
}

impl UnionFind {
    fn new() -> Self {
        Self {
            parent: std::collections::HashMap::new(),
        }
    }

    fn add(&mut self, key: &str) {
        self.parent.entry(key.to_string()).or_insert_with(|| key.to_string());
    }

    fn find(&mut self, key: &str) -> String {
        self.add(key);
        let parent = self.parent.get(key).cloned().unwrap_or_else(|| key.to_string());
        if parent == key {
            return parent;
        }
        let root = self.find(&parent);
        self.parent.insert(key.to_string(), root.clone());
        root
    }

    fn union(&mut self, left: &str, right: &str) {
        let left_root = self.find(left);
        let right_root = self.find(right);
        if left_root != right_root {
            self.parent.insert(left_root, right_root);
        }
    }

    fn keys(&self) -> Vec<String> {
        self.parent.keys().cloned().collect()
    }
}
