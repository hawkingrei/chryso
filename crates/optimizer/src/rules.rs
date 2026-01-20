use corundum_core::ast::{BinaryOperator, Expr};
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
            .with_rule(FilterPushdown)
            .with_rule(FilterJoinPushdown)
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
        let joined = LogicalPlan::Join {
            join_type: *join_type,
            left: Box::new(new_left),
            right: Box::new(new_right),
            on: on.clone(),
        };
        if let Some(expr) = combine_conjuncts(remaining) {
            vec![LogicalPlan::Filter {
                predicate: expr,
                input: Box::new(joined),
            }]
        } else {
            vec![joined]
        }
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
        FilterJoinPushdown, FilterPushdown, LimitPushdown, MergeFilters, MergeProjections,
        NormalizePredicates, PruneProjection, Rule, TopNRule,
    };
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
        let LogicalPlan::Filter { .. } = &results[0] else {
            panic!("expected filter");
        };
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

fn collect_identifiers(expr: &Expr) -> std::collections::HashSet<String> {
    let mut idents = std::collections::HashSet::new();
    collect_identifiers_inner(expr, &mut idents);
    idents
}

#[derive(Clone, Copy)]
enum Side {
    Left,
    Right,
}

fn split_conjuncts(expr: &Expr) -> Vec<Expr> {
    match expr {
        Expr::BinaryOp { left, op, right } if matches!(op, BinaryOperator::And) => {
            let mut out = split_conjuncts(left);
            out.extend(split_conjuncts(right));
            out
        }
        _ => vec![expr.clone()],
    }
}

fn combine_conjuncts(exprs: Vec<Expr>) -> Option<Expr> {
    let mut iter = exprs.into_iter();
    let first = iter.next()?;
    Some(iter.fold(first, |left, right| Expr::BinaryOp {
        left: Box::new(left),
        op: BinaryOperator::And,
        right: Box::new(right),
    }))
}

fn table_prefix(ident: &str) -> Option<&str> {
    ident.split_once('.').map(|(prefix, _)| prefix)
}

fn collect_tables(plan: &LogicalPlan) -> std::collections::HashSet<String> {
    let mut tables = std::collections::HashSet::new();
    collect_tables_inner(plan, &mut tables);
    tables
}

fn collect_tables_inner(plan: &LogicalPlan, tables: &mut std::collections::HashSet<String>) {
    match plan {
        LogicalPlan::Scan { table } | LogicalPlan::IndexScan { table, .. } => {
            tables.insert(table.clone());
        }
        LogicalPlan::Dml { .. } => {}
        LogicalPlan::Filter { input, .. }
        | LogicalPlan::Projection { input, .. }
        | LogicalPlan::Aggregate { input, .. }
        | LogicalPlan::Distinct { input }
        | LogicalPlan::TopN { input, .. }
        | LogicalPlan::Sort { input, .. }
        | LogicalPlan::Limit { input, .. } => collect_tables_inner(input, tables),
        LogicalPlan::Join { left, right, .. } => {
            collect_tables_inner(left, tables);
            collect_tables_inner(right, tables);
        }
    }
}

fn collect_identifiers_inner(expr: &Expr, idents: &mut std::collections::HashSet<String>) {
    match expr {
        Expr::Identifier(name) => {
            idents.insert(name.clone());
        }
        Expr::BinaryOp { left, right, .. } => {
            collect_identifiers_inner(left, idents);
            collect_identifiers_inner(right, idents);
        }
        Expr::UnaryOp { expr, .. } => {
            collect_identifiers_inner(expr, idents);
        }
        Expr::FunctionCall { args, .. } => {
            for arg in args {
                collect_identifiers_inner(arg, idents);
            }
        }
        Expr::WindowFunction { function, spec } => {
            collect_identifiers_inner(function, idents);
            for expr in &spec.partition_by {
                collect_identifiers_inner(expr, idents);
            }
            for expr in &spec.order_by {
                collect_identifiers_inner(&expr.expr, idents);
            }
        }
        Expr::Subquery(select) | Expr::Exists(select) => {
            for item in &select.projection {
                collect_identifiers_inner(&item.expr, idents);
            }
        }
        Expr::InSubquery { expr, subquery } => {
            collect_identifiers_inner(expr, idents);
            for item in &subquery.projection {
                collect_identifiers_inner(&item.expr, idents);
            }
        }
        Expr::Literal(_) | Expr::Wildcard => {}
    }
}
