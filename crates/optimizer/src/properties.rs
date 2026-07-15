use chryso_core::ast::{Expr, OrderByExpr};
use chryso_planner::{ExchangeDistribution, PhysicalPlan};
use std::hash::{Hash, Hasher};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Ordering {
    Any,
    Sorted(Vec<OrderingKey>),
}

#[derive(Debug, Clone)]
pub struct OrderingKey {
    pub expression: Expr,
    pub asc: bool,
    pub nulls_first: Option<bool>,
}

impl OrderingKey {
    pub fn ascending(expression: impl Into<String>) -> Self {
        Self {
            expression: Expr::Identifier(expression.into()),
            asc: true,
            nulls_first: None,
        }
    }

    pub fn new(expression: Expr, asc: bool, nulls_first: Option<bool>) -> Self {
        Self {
            expression,
            asc,
            nulls_first,
        }
    }
}

impl PartialEq for OrderingKey {
    fn eq(&self, other: &Self) -> bool {
        self.expression.to_sql() == other.expression.to_sql()
            && self.asc == other.asc
            && self.nulls_first == other.nulls_first
    }
}

impl Eq for OrderingKey {}

impl Hash for OrderingKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.expression.to_sql().hash(state);
        self.asc.hash(state);
        self.nulls_first.hash(state);
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Distribution {
    Any,
    Single,
    Hash(Vec<String>),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PhysicalProperties {
    pub ordering: Ordering,
    pub distribution: Distribution,
}

impl PhysicalProperties {
    pub fn satisfies(&self, required: &Self) -> bool {
        ordering_satisfies(&self.ordering, &required.ordering)
            && distribution_satisfies(&self.distribution, &required.distribution)
    }

    pub fn is_any(&self) -> bool {
        matches!(self.ordering, Ordering::Any) && matches!(self.distribution, Distribution::Any)
    }
}

impl Default for PhysicalProperties {
    fn default() -> Self {
        Self {
            ordering: Ordering::Any,
            distribution: Distribution::Any,
        }
    }
}

pub fn derive_properties(plan: &PhysicalPlan) -> PhysicalProperties {
    match plan {
        PhysicalPlan::Derived { input, .. }
        | PhysicalPlan::Filter { input, .. }
        | PhysicalPlan::Projection { input, .. }
        | PhysicalPlan::Limit { input, .. } => derive_properties(input),
        PhysicalPlan::Sort { order_by, input }
        | PhysicalPlan::TopN {
            order_by, input, ..
        } => {
            let mut properties = derive_properties(input);
            properties.ordering = Ordering::Sorted(ordering_keys(order_by));
            properties
        }
        PhysicalPlan::Exchange { distribution, .. } => PhysicalProperties {
            ordering: Ordering::Any,
            distribution: match distribution {
                ExchangeDistribution::Single => Distribution::Single,
                ExchangeDistribution::Hash(keys) => Distribution::Hash(keys.clone()),
            },
        },
        PhysicalPlan::TableScan { .. }
        | PhysicalPlan::IndexScan { .. }
        | PhysicalPlan::Dml { .. }
        | PhysicalPlan::Join { .. }
        | PhysicalPlan::Aggregate { .. }
        | PhysicalPlan::Distinct { .. } => PhysicalProperties::default(),
    }
}

pub fn ordering_keys(order_by: &[OrderByExpr]) -> Vec<OrderingKey> {
    order_by
        .iter()
        .map(|item| OrderingKey {
            expression: item.expr.clone(),
            asc: item.asc,
            nulls_first: item.nulls_first,
        })
        .collect()
}

fn ordering_satisfies(provided: &Ordering, required: &Ordering) -> bool {
    match required {
        Ordering::Any => true,
        Ordering::Sorted(required) => {
            matches!(provided, Ordering::Sorted(provided) if provided.starts_with(required))
        }
    }
}

fn distribution_satisfies(provided: &Distribution, required: &Distribution) -> bool {
    match required {
        Distribution::Any => true,
        Distribution::Single => matches!(provided, Distribution::Single),
        Distribution::Hash(required) => {
            matches!(provided, Distribution::Hash(provided) if provided == required)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{Distribution, Ordering, OrderingKey, PhysicalProperties};
    use chryso_core::ast::Expr;

    #[test]
    fn sorted_prefix_satisfies_shorter_requirement() {
        let provided = PhysicalProperties {
            ordering: Ordering::Sorted(vec![
                OrderingKey::ascending("a"),
                OrderingKey::ascending("b"),
            ]),
            distribution: Distribution::Any,
        };
        let required = PhysicalProperties {
            ordering: Ordering::Sorted(vec![OrderingKey::ascending("a")]),
            distribution: Distribution::Any,
        };
        assert!(provided.satisfies(&required));
    }

    #[test]
    fn ordering_direction_is_part_of_the_property() {
        let provided = PhysicalProperties {
            ordering: Ordering::Sorted(vec![OrderingKey::ascending("a")]),
            distribution: Distribution::Any,
        };
        let required = PhysicalProperties {
            ordering: Ordering::Sorted(vec![OrderingKey {
                expression: Expr::Identifier("a".to_string()),
                asc: false,
                nulls_first: None,
            }]),
            distribution: Distribution::Any,
        };
        assert!(!provided.satisfies(&required));
    }
}
