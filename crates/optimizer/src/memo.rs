use crate::cost::{Cost, CostModel};
use crate::physical_rules::PhysicalRuleSet;
use crate::plan_fingerprint::logical_fingerprint;
use crate::properties::{PhysicalProperties, derive_properties};
use crate::rules::RuleContext;
use crate::{MemoTrace, MemoTraceCandidate, MemoTraceGroup, RuleConfig, SearchBudget};
use chryso_planner::{LogicalPlan, PhysicalPlan};
use std::collections::{HashMap, HashSet};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct GroupId(usize);

#[derive(Debug, Default)]
pub struct Memo {
    groups: Vec<Group>,
    logical_groups: HashMap<String, GroupId>,
}

impl Memo {
    pub fn new() -> Self {
        Self {
            groups: Vec::new(),
            logical_groups: HashMap::new(),
        }
    }

    pub fn insert(&mut self, plan: &LogicalPlan) -> GroupId {
        let fingerprint = logical_fingerprint(plan);
        if let Some(group_id) = self.logical_groups.get(&fingerprint) {
            return *group_id;
        }
        let group_id = GroupId(self.groups.len());
        self.groups.push(Group {
            expressions: Vec::new(),
        });
        self.logical_groups.insert(fingerprint, group_id);
        let expr = GroupExpr::from_logical(plan, self);
        self.groups[group_id.0].expressions.push(expr);
        group_id
    }

    pub fn insert_equivalent(&mut self, group_id: GroupId, plan: &LogicalPlan) -> bool {
        let fingerprint = logical_fingerprint(plan);
        if self.groups[group_id.0]
            .expressions
            .iter()
            .any(|expr| expr.fingerprint() == fingerprint)
        {
            return false;
        }
        if let Some(existing_group) = self.logical_groups.get(&fingerprint).copied() {
            if existing_group == group_id
                || self.groups[group_id.0].expressions.iter().any(
                    |expr| matches!(&expr.operator, MemoOperator::GroupRef(id) if *id == existing_group),
                )
            {
                return false;
            }
            self.groups[group_id.0].expressions.push(GroupExpr {
                operator: MemoOperator::GroupRef(existing_group),
                children: Vec::new(),
            });
            return true;
        }
        let expr = GroupExpr::from_logical(plan, self);
        self.logical_groups.insert(fingerprint, group_id);
        self.groups[group_id.0].expressions.push(expr);
        true
    }

    pub fn best_physical(
        &self,
        root: GroupId,
        physical_rules: &PhysicalRuleSet,
        cost_model: &dyn CostModel,
    ) -> Option<PhysicalPlan> {
        self.best_physical_for_properties(
            root,
            &PhysicalProperties::default(),
            physical_rules,
            cost_model,
        )
    }

    pub fn best_physical_for_properties(
        &self,
        root: GroupId,
        required: &PhysicalProperties,
        physical_rules: &PhysicalRuleSet,
        cost_model: &dyn CostModel,
    ) -> Option<PhysicalPlan> {
        let mut search = SearchContext::default();
        self.optimize_group(root, required, physical_rules, cost_model, &mut search)
            .map(|(_, plan)| plan)
    }

    pub fn best_physical_with_trace(
        &self,
        root: GroupId,
        required: &PhysicalProperties,
        physical_rules: &PhysicalRuleSet,
        cost_model: &dyn CostModel,
    ) -> (Option<PhysicalPlan>, MemoTrace) {
        let mut search = SearchContext::default();
        let plan = self
            .optimize_group(root, required, physical_rules, cost_model, &mut search)
            .map(|(_, plan)| plan);
        (plan, search.into_trace())
    }

    pub fn trace(&self, physical_rules: &PhysicalRuleSet, cost_model: &dyn CostModel) -> MemoTrace {
        let mut search = SearchContext::default();
        for group_id in 0..self.groups.len() {
            let _ = self.optimize_group(
                GroupId(group_id),
                &PhysicalProperties::default(),
                physical_rules,
                cost_model,
                &mut search,
            );
        }
        search.into_trace()
    }

    fn optimize_group(
        &self,
        root: GroupId,
        required: &PhysicalProperties,
        physical_rules: &PhysicalRuleSet,
        cost_model: &dyn CostModel,
        search: &mut SearchContext,
    ) -> Option<(Cost, PhysicalPlan)> {
        let key = (root, required.clone());
        if let Some(cached) = search.cache.get(&key) {
            return cached.clone();
        }
        if !search.in_progress.insert(key.clone()) {
            return None;
        }

        let group = self.groups.get(root.0)?;
        let mut candidates = Vec::new();
        for expr in &group.expressions {
            match &expr.operator {
                MemoOperator::Logical(logical) => {
                    for rule in physical_rules.iter() {
                        for input_requirements in rule.input_requirements(logical, required) {
                            if input_requirements.len() != expr.children.len() {
                                continue;
                            }
                            let mut inputs = Vec::with_capacity(expr.children.len());
                            let mut missing_input = false;
                            for (child, child_required) in
                                expr.children.iter().zip(&input_requirements)
                            {
                                if let Some((_, best_child)) = self.optimize_group(
                                    *child,
                                    child_required,
                                    physical_rules,
                                    cost_model,
                                    search,
                                ) {
                                    inputs.push(best_child);
                                } else {
                                    missing_input = true;
                                    break;
                                }
                            }
                            if missing_input {
                                continue;
                            }
                            for physical in rule.apply(logical, &inputs) {
                                let provided = derive_properties(&physical);
                                let enforced = !provided.satisfies(required);
                                let (physical, delivered) =
                                    crate::enforcer::enforce_with_properties(
                                        physical, &provided, required,
                                    );
                                if !delivered.satisfies(required) {
                                    continue;
                                }
                                let cost = cost_model.cost(&physical);
                                let plan = physical.explain_costed(0, cost_model);
                                let priority = candidates.len();
                                candidates.push(SearchCandidate {
                                    cost,
                                    priority,
                                    physical,
                                    trace: MemoTraceCandidate {
                                        cost: cost.0,
                                        plan,
                                        selected: false,
                                        reason: if enforced {
                                            format!("rule={} enforced", rule.name())
                                        } else {
                                            format!("rule={} delivered", rule.name())
                                        },
                                    },
                                });
                            }
                        }
                    }
                }
                MemoOperator::Physical(plan) => {
                    let provided = derive_properties(plan);
                    let enforced = !provided.satisfies(required);
                    let (physical, delivered) =
                        crate::enforcer::enforce_with_properties(plan.clone(), &provided, required);
                    if delivered.satisfies(required) {
                        let cost = cost_model.cost(&physical);
                        let rendered = physical.explain_costed(0, cost_model);
                        let priority = candidates.len();
                        candidates.push(SearchCandidate {
                            cost,
                            priority,
                            physical,
                            trace: MemoTraceCandidate {
                                cost: cost.0,
                                plan: rendered,
                                selected: false,
                                reason: if enforced {
                                    "physical enforced".to_string()
                                } else {
                                    "physical delivered".to_string()
                                },
                            },
                        });
                    }
                }
                MemoOperator::GroupRef(group_id) => {
                    if let Some((cost, physical)) =
                        self.optimize_group(*group_id, required, physical_rules, cost_model, search)
                    {
                        let priority = candidates.len();
                        candidates.push(SearchCandidate {
                            cost,
                            priority,
                            trace: MemoTraceCandidate {
                                cost: cost.0,
                                plan: physical.explain_costed(0, cost_model),
                                selected: false,
                                reason: format!("group-ref={} delivered", group_id.0),
                            },
                            physical,
                        });
                    }
                }
            }
        }

        candidates.sort_by(|left, right| {
            left.cost
                .0
                .partial_cmp(&right.cost.0)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| left.priority.cmp(&right.priority))
        });
        let mut seen_plans = HashSet::new();
        candidates.retain(|candidate| seen_plans.insert(candidate.trace.plan.clone()));
        if let Some(candidate) = candidates.first_mut() {
            candidate.trace.selected = true;
            candidate.trace.reason.push_str(" selected");
        }
        for candidate in candidates.iter_mut().skip(1) {
            candidate.trace.reason.push_str(" higher-cost");
        }

        let best = candidates
            .first()
            .map(|candidate| (candidate.cost, candidate.physical.clone()));
        search.candidates.insert(
            key.clone(),
            candidates
                .into_iter()
                .map(|candidate| candidate.trace)
                .collect(),
        );
        search.in_progress.remove(&key);
        search.cache.insert(key, best.clone());
        best
    }

    pub fn explore(
        &mut self,
        rules: &crate::rules::RuleSet,
        rule_config: &RuleConfig,
        budget: &SearchBudget,
    ) -> MemoExploreResult {
        let max_rewrites = budget.max_rewrites.unwrap_or(usize::MAX);
        let max_groups = budget.max_groups.unwrap_or(usize::MAX);
        let mut rewrites = 0usize;
        let mut rule_ctx = RuleContext::default();
        let mut applied_rules = Vec::new();
        let mut group_index = 0usize;
        while group_index < self.groups.len() && rewrites < max_rewrites {
            let group_id = GroupId(group_index);
            let mut expr_index = 0usize;
            while expr_index < self.groups[group_index].expressions.len() && rewrites < max_rewrites
            {
                let logical = match &self.groups[group_index].expressions[expr_index].operator {
                    MemoOperator::Logical(plan) => Some(plan.clone()),
                    MemoOperator::Physical(_) | MemoOperator::GroupRef(_) => None,
                };
                expr_index += 1;
                let Some(logical) = logical else {
                    continue;
                };
                for rule in rules.iter() {
                    if rewrites >= max_rewrites {
                        break;
                    }
                    if !rule_config.is_enabled(rule.name()) {
                        continue;
                    }
                    for rewritten in rule.apply(&logical, &mut rule_ctx) {
                        if rewrites >= max_rewrites {
                            break;
                        }
                        let new_groups = self.count_new_groups(&rewritten);
                        if self.groups.len().saturating_add(new_groups) > max_groups {
                            continue;
                        }
                        if self.insert_equivalent(group_id, &rewritten) {
                            rewrites += 1;
                            applied_rules.push(rule.name().to_string());
                        }
                    }
                }
            }
            group_index += 1;
        }
        MemoExploreResult {
            applied_rules,
            conflict_pairs: rule_ctx.take_conflict_pairs(),
        }
    }

    fn count_new_groups(&self, plan: &LogicalPlan) -> usize {
        let mut fingerprints = HashSet::new();
        match plan {
            LogicalPlan::Derived { input, .. }
            | LogicalPlan::Filter { input, .. }
            | LogicalPlan::Projection { input, .. }
            | LogicalPlan::Aggregate { input, .. }
            | LogicalPlan::Distinct { input }
            | LogicalPlan::TopN { input, .. }
            | LogicalPlan::Sort { input, .. }
            | LogicalPlan::Limit { input, .. } => {
                collect_new_group_fingerprints(input, &self.logical_groups, &mut fingerprints);
            }
            LogicalPlan::Join { left, right, .. } => {
                collect_new_group_fingerprints(left, &self.logical_groups, &mut fingerprints);
                collect_new_group_fingerprints(right, &self.logical_groups, &mut fingerprints);
            }
            LogicalPlan::Scan { .. } | LogicalPlan::IndexScan { .. } | LogicalPlan::Dml { .. } => {}
        }
        fingerprints.len()
    }

    #[cfg(test)]
    pub fn group_count(&self) -> usize {
        self.groups.len()
    }
}

#[derive(Debug, Default)]
pub struct MemoExploreResult {
    pub applied_rules: Vec<String>,
    pub conflict_pairs: Vec<crate::rules::ConflictPair>,
}

#[derive(Default)]
struct SearchContext {
    cache: HashMap<SearchKey, Option<(Cost, PhysicalPlan)>>,
    in_progress: HashSet<SearchKey>,
    candidates: HashMap<SearchKey, Vec<MemoTraceCandidate>>,
}

type SearchKey = (GroupId, PhysicalProperties);

struct SearchCandidate {
    cost: Cost,
    priority: usize,
    physical: PhysicalPlan,
    trace: MemoTraceCandidate,
}

impl SearchContext {
    fn into_trace(self) -> MemoTrace {
        let mut entries = self.candidates.into_iter().collect::<Vec<_>>();
        entries.sort_by(
            |((left_group, left_properties), _), ((right_group, right_properties), _)| {
                left_group.0.cmp(&right_group.0).then_with(|| {
                    format!("{left_properties:?}").cmp(&format!("{right_properties:?}"))
                })
            },
        );
        MemoTrace {
            groups: entries
                .into_iter()
                .map(|((group_id, required), candidates)| MemoTraceGroup {
                    id: group_id.0,
                    required_properties: format!("{required:?}"),
                    candidates,
                })
                .collect(),
        }
    }
}

fn collect_new_group_fingerprints(
    plan: &LogicalPlan,
    existing: &HashMap<String, GroupId>,
    fingerprints: &mut HashSet<String>,
) {
    let fingerprint = logical_fingerprint(plan);
    if existing.contains_key(&fingerprint) || !fingerprints.insert(fingerprint) {
        return;
    }
    match plan {
        LogicalPlan::Derived { input, .. }
        | LogicalPlan::Filter { input, .. }
        | LogicalPlan::Projection { input, .. }
        | LogicalPlan::Aggregate { input, .. }
        | LogicalPlan::Distinct { input }
        | LogicalPlan::TopN { input, .. }
        | LogicalPlan::Sort { input, .. }
        | LogicalPlan::Limit { input, .. } => {
            collect_new_group_fingerprints(input, existing, fingerprints);
        }
        LogicalPlan::Join { left, right, .. } => {
            collect_new_group_fingerprints(left, existing, fingerprints);
            collect_new_group_fingerprints(right, existing, fingerprints);
        }
        LogicalPlan::Scan { .. } | LogicalPlan::IndexScan { .. } | LogicalPlan::Dml { .. } => {}
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
    fn fingerprint(&self) -> String {
        match &self.operator {
            MemoOperator::Logical(plan) => logical_fingerprint(plan),
            MemoOperator::Physical(plan) => plan.explain(0),
            MemoOperator::GroupRef(group_id) => format!("group-ref:{}", group_id.0),
        }
    }

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
            MemoOperator::Logical(plan) => Some(logical_to_physical(plan)),
            MemoOperator::Physical(plan) => Some(plan.clone()),
            MemoOperator::GroupRef(group_id) => memo
                .groups
                .get(group_id.0)?
                .expressions
                .iter()
                .find(|expr| !matches!(&expr.operator, MemoOperator::GroupRef(_)))?
                .to_physical(memo),
        }
    }
}

#[derive(Debug, Clone)]
pub enum MemoOperator {
    Logical(LogicalPlan),
    Physical(PhysicalPlan),
    GroupRef(GroupId),
}

fn logical_to_physical(logical: &LogicalPlan) -> PhysicalPlan {
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
            input: Box::new(logical_to_physical(input)),
            alias: alias.clone(),
            column_aliases: column_aliases.clone(),
        },
        LogicalPlan::Filter { predicate, input } => PhysicalPlan::Filter {
            predicate: predicate.clone(),
            input: Box::new(logical_to_physical(input)),
        },
        LogicalPlan::Projection { exprs, input } => PhysicalPlan::Projection {
            exprs: exprs.clone(),
            input: Box::new(logical_to_physical(input)),
        },
        LogicalPlan::Join {
            join_type,
            left,
            right,
            on,
        } => PhysicalPlan::Join {
            join_type: *join_type,
            algorithm: chryso_planner::JoinAlgorithm::Hash,
            left: Box::new(logical_to_physical(left)),
            right: Box::new(logical_to_physical(right)),
            on: on.clone(),
        },
        LogicalPlan::Aggregate {
            group_exprs,
            aggr_exprs,
            input,
        } => PhysicalPlan::Aggregate {
            group_exprs: group_exprs.clone(),
            aggr_exprs: aggr_exprs.clone(),
            input: Box::new(logical_to_physical(input)),
        },
        LogicalPlan::Distinct { input } => PhysicalPlan::Distinct {
            input: Box::new(logical_to_physical(input)),
        },
        LogicalPlan::TopN {
            order_by,
            limit,
            input,
        } => PhysicalPlan::TopN {
            order_by: order_by.clone(),
            limit: *limit,
            input: Box::new(logical_to_physical(input)),
        },
        LogicalPlan::Sort { order_by, input } => PhysicalPlan::Sort {
            order_by: order_by.clone(),
            input: Box::new(logical_to_physical(input)),
        },
        LogicalPlan::Limit {
            limit,
            offset,
            input,
        } => PhysicalPlan::Limit {
            limit: *limit,
            offset: *offset,
            input: Box::new(logical_to_physical(input)),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::{Memo, MemoOperator};
    use crate::cost::{Cost, CostModel, StatsCostModel};
    use crate::physical_rules::PhysicalRuleSet;
    use crate::properties::{Distribution, Ordering, OrderingKey, PhysicalProperties};
    use crate::rules::{RemoveTrueFilter, RuleSet};
    use crate::{RuleConfig, SearchBudget};
    use chryso_metadata::{StatsCache, TableStats};
    use chryso_planner::{LogicalPlan, PhysicalPlan};

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

    #[test]
    fn equivalent_rewrite_stays_in_root_group() {
        let plan = LogicalPlan::Filter {
            predicate: chryso_core::ast::Expr::Literal(chryso_core::ast::Literal::Bool(true)),
            input: Box::new(LogicalPlan::Scan {
                table: "users".to_string(),
            }),
        };
        let rules = RuleSet::new().with_rule(RemoveTrueFilter);
        let mut memo = Memo::new();
        let root = memo.insert(&plan);
        let initial_groups = memo.group_count();
        memo.explore(&rules, &RuleConfig::default(), &SearchBudget::default());
        assert_eq!(memo.group_count(), initial_groups);
        assert_eq!(memo.groups[root.0].expressions.len(), 2);
        assert!(matches!(
            &memo.groups[root.0].expressions[1].operator,
            MemoOperator::GroupRef(_)
        ));
    }

    #[test]
    fn property_search_keeps_separate_child_goals() {
        let plan = LogicalPlan::Filter {
            predicate: chryso_core::ast::Expr::Identifier("t.active".to_string()),
            input: Box::new(LogicalPlan::Scan {
                table: "t".to_string(),
            }),
        };
        let mut memo = Memo::new();
        let root = memo.insert(&plan);
        let mut stats = StatsCache::new();
        stats.insert_table_stats("t", TableStats { row_count: 1_000.0 });
        let cost_model = StatsCostModel::new(&stats);
        let required = PhysicalProperties {
            ordering: Ordering::Sorted(vec![OrderingKey::ascending("t.id")]),
            distribution: Distribution::Any,
        };
        let (best, trace) = memo.best_physical_with_trace(
            root,
            &required,
            &PhysicalRuleSet::default(),
            &cost_model,
        );
        let best = best.expect("best physical plan");
        assert!(matches!(best, PhysicalPlan::Sort { .. }));
        assert!(trace.groups.iter().any(|group| {
            group.required_properties.contains("Sorted")
                && group.candidates.iter().any(|candidate| candidate.selected)
        }));
        assert!(
            trace
                .groups
                .iter()
                .any(|group| group.required_properties.contains("ordering: Any"))
        );
    }

    #[test]
    fn globally_cheaper_property_satisfying_child_wins() {
        struct PropertyPlacementCost;

        impl CostModel for PropertyPlacementCost {
            fn cost(&self, plan: &PhysicalPlan) -> Cost {
                match plan {
                    PhysicalPlan::Filter { input, .. }
                        if matches!(input.as_ref(), PhysicalPlan::Sort { .. }) =>
                    {
                        Cost(5.0)
                    }
                    PhysicalPlan::Sort { input, .. }
                        if matches!(input.as_ref(), PhysicalPlan::Filter { .. }) =>
                    {
                        Cost(10.0)
                    }
                    PhysicalPlan::Sort { .. } => Cost(3.0),
                    _ => Cost(1.0),
                }
            }
        }

        let plan = LogicalPlan::Filter {
            predicate: chryso_core::ast::Expr::Identifier("t.active".to_string()),
            input: Box::new(LogicalPlan::Scan {
                table: "t".to_string(),
            }),
        };
        let mut memo = Memo::new();
        let root = memo.insert(&plan);
        let required = PhysicalProperties {
            ordering: Ordering::Sorted(vec![OrderingKey::ascending("t.id")]),
            distribution: Distribution::Any,
        };
        let best = memo
            .best_physical_for_properties(
                root,
                &required,
                &PhysicalRuleSet::default(),
                &PropertyPlacementCost,
            )
            .expect("best physical plan");
        let PhysicalPlan::Filter { input, .. } = best else {
            panic!("expected property-preserving filter above sorted child");
        };
        assert!(matches!(input.as_ref(), PhysicalPlan::Sort { .. }));
    }

    #[test]
    fn memo_trace_is_deterministic() {
        let plan = LogicalPlan::Scan {
            table: "t".to_string(),
        };
        let mut memo = Memo::new();
        let root = memo.insert(&plan);
        let physical_rules = PhysicalRuleSet::default();
        let stats = StatsCache::new();
        let cost_model = StatsCostModel::new(&stats);
        let (_, left) = memo.best_physical_with_trace(
            root,
            &PhysicalProperties::default(),
            &physical_rules,
            &cost_model,
        );
        let (_, right) = memo.best_physical_with_trace(
            root,
            &PhysicalProperties::default(),
            &physical_rules,
            &cost_model,
        );
        assert_eq!(left.format_full(), right.format_full());
    }
}
