use crate::properties::{Distribution, Ordering, PhysicalProperties, derive_properties};
use chryso_core::ast::OrderByExpr;
use chryso_planner::{ExchangeDistribution, PhysicalPlan};

pub fn enforce(plan: PhysicalPlan, required: &PhysicalProperties) -> PhysicalPlan {
    let provided = derive_properties(&plan);
    enforce_with_properties(plan, &provided, required).0
}

pub fn enforce_with_properties(
    mut plan: PhysicalPlan,
    provided: &PhysicalProperties,
    required: &PhysicalProperties,
) -> (PhysicalPlan, PhysicalProperties) {
    let mut current = provided.clone();

    if !distribution_satisfied(&current.distribution, &required.distribution) {
        let distribution = match &required.distribution {
            Distribution::Any => unreachable!("Any distribution is always satisfied"),
            Distribution::Single => ExchangeDistribution::Single,
            Distribution::Hash(keys) => ExchangeDistribution::Hash(keys.clone()),
        };
        plan = PhysicalPlan::Exchange {
            distribution,
            input: Box::new(plan),
        };
        current = derive_properties(&plan);
    }

    if !ordering_satisfied(&current.ordering, &required.ordering) {
        let Ordering::Sorted(keys) = &required.ordering else {
            unreachable!("Any ordering is always satisfied");
        };
        let order_by = keys
            .iter()
            .map(|key| OrderByExpr {
                expr: key.expression.clone(),
                asc: key.asc,
                nulls_first: key.nulls_first,
            })
            .collect();
        plan = PhysicalPlan::Sort {
            order_by,
            input: Box::new(plan),
        };
        current = derive_properties(&plan);
    }

    (plan, current)
}

fn ordering_satisfied(provided: &Ordering, required: &Ordering) -> bool {
    PhysicalProperties {
        ordering: provided.clone(),
        distribution: Distribution::Any,
    }
    .satisfies(&PhysicalProperties {
        ordering: required.clone(),
        distribution: Distribution::Any,
    })
}

fn distribution_satisfied(provided: &Distribution, required: &Distribution) -> bool {
    PhysicalProperties {
        ordering: Ordering::Any,
        distribution: provided.clone(),
    }
    .satisfies(&PhysicalProperties {
        ordering: Ordering::Any,
        distribution: required.clone(),
    })
}

#[cfg(test)]
mod tests {
    use super::enforce;
    use crate::properties::{
        Distribution, Ordering, OrderingKey, PhysicalProperties, derive_properties,
    };
    use chryso_core::ast::{BinaryOperator, Expr, Literal};
    use chryso_planner::PhysicalPlan;

    #[test]
    fn exchange_precedes_sort_when_both_are_required() {
        let required = PhysicalProperties {
            ordering: Ordering::Sorted(vec![OrderingKey::ascending("id")]),
            distribution: Distribution::Hash(vec!["id".to_string()]),
        };
        let plan = enforce(
            PhysicalPlan::TableScan {
                table: "t".to_string(),
            },
            &required,
        );
        assert!(matches!(plan, PhysicalPlan::Sort { .. }));
        assert!(derive_properties(&plan).satisfies(&required));
        let PhysicalPlan::Sort { input, .. } = plan else {
            unreachable!();
        };
        assert!(matches!(*input, PhysicalPlan::Exchange { .. }));
    }

    #[test]
    fn sort_enforcer_preserves_expression_ast() {
        let expression = Expr::BinaryOp {
            left: Box::new(Expr::Identifier("a".to_string())),
            op: BinaryOperator::Add,
            right: Box::new(Expr::Literal(Literal::Number(1.0))),
        };
        let required = PhysicalProperties {
            ordering: Ordering::Sorted(vec![OrderingKey::new(
                expression.clone(),
                false,
                Some(true),
            )]),
            distribution: Distribution::Any,
        };
        let plan = enforce(
            PhysicalPlan::TableScan {
                table: "t".to_string(),
            },
            &required,
        );
        let PhysicalPlan::Sort { order_by, .. } = plan else {
            panic!("expected sort enforcer");
        };
        assert_eq!(order_by[0].expr.to_sql(), expression.to_sql());
        assert!(!order_by[0].asc);
        assert_eq!(order_by[0].nulls_first, Some(true));
    }
}
