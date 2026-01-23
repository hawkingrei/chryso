use chryso_core::error::{ChrysoError, ChrysoResult};
use chryso_metadata::catalog::Catalog;
use crate::LogicalPlan;

pub trait Validator {
    fn validate(&self, plan: &LogicalPlan) -> ChrysoResult<()>;
}

pub struct NameResolver<'a, C: Catalog> {
    catalog: &'a C,
}

impl<'a, C: Catalog> NameResolver<'a, C> {
    pub fn new(catalog: &'a C) -> Self {
        Self { catalog }
    }
}

impl<'a, C: Catalog> Validator for NameResolver<'a, C> {
    fn validate(&self, plan: &LogicalPlan) -> ChrysoResult<()> {
        match plan {
            LogicalPlan::Scan { table } => {
                if self.catalog.has_table(table) {
                    Ok(())
                } else {
                    Err(ChrysoError::new(format!("unknown table {table}")))
                }
            }
            LogicalPlan::IndexScan { table, .. } => {
                if self.catalog.has_table(table) {
                    Ok(())
                } else {
                    Err(ChrysoError::new(format!("unknown table {table}")))
                }
            }
            LogicalPlan::Dml { .. } => Ok(()),
            LogicalPlan::Derived { input, .. } => self.validate(input),
            LogicalPlan::Filter { input, .. } => self.validate(input),
            LogicalPlan::Projection { input, .. } => self.validate(input),
            LogicalPlan::Join { left, right, .. } => {
                self.validate(left)?;
                self.validate(right)
            }
            LogicalPlan::Aggregate { input, .. } => self.validate(input),
            LogicalPlan::Distinct { input } => self.validate(input),
            LogicalPlan::TopN { input, .. } => self.validate(input),
            LogicalPlan::Sort { input, .. } => self.validate(input),
            LogicalPlan::Limit { input, .. } => self.validate(input),
        }
    }
}

pub fn validate_functions(
    plan: &LogicalPlan,
    registry: &chryso_metadata::functions::FunctionRegistry,
) -> ChrysoResult<()> {
    match plan {
        LogicalPlan::Scan { .. } | LogicalPlan::IndexScan { .. } => Ok(()),
        LogicalPlan::Filter { predicate, input } => {
            validate_expr_functions(predicate, registry)?;
            validate_functions(input, registry)
        }
        LogicalPlan::Projection { exprs, input } => {
            for expr in exprs {
                validate_expr_functions(expr, registry)?;
            }
            validate_functions(input, registry)
        }
        LogicalPlan::Join { on, left, right, .. } => {
            validate_expr_functions(on, registry)?;
            validate_functions(left, registry)?;
            validate_functions(right, registry)
        }
        LogicalPlan::Aggregate {
            group_exprs,
            aggr_exprs,
            input,
        } => {
            for expr in group_exprs {
                validate_expr_functions(expr, registry)?;
            }
            for expr in aggr_exprs {
                validate_expr_functions(expr, registry)?;
            }
            validate_functions(input, registry)
        }
        LogicalPlan::Distinct { input }
        | LogicalPlan::Sort { input, .. }
        | LogicalPlan::Limit { input, .. }
        | LogicalPlan::TopN { input, .. } => validate_functions(input, registry),
        LogicalPlan::Derived { input, .. } => validate_functions(input, registry),
        LogicalPlan::Dml { .. } => Ok(()),
    }
}

fn validate_expr_functions(
    expr: &chryso_core::ast::Expr,
    registry: &chryso_metadata::functions::FunctionRegistry,
) -> ChrysoResult<()> {
    match expr {
        chryso_core::ast::Expr::FunctionCall { name, args } => {
            if !registry.is_known(name) {
                return Err(ChrysoError::new(format!("unknown function {name}")));
            }
            for arg in args {
                validate_expr_functions(arg, registry)?;
            }
            Ok(())
        }
        chryso_core::ast::Expr::WindowFunction { function, spec } => {
            validate_expr_functions(function, registry)?;
            for expr in &spec.partition_by {
                validate_expr_functions(expr, registry)?;
            }
            for expr in &spec.order_by {
                validate_expr_functions(&expr.expr, registry)?;
            }
            Ok(())
        }
        chryso_core::ast::Expr::BinaryOp { left, right, .. } => {
            validate_expr_functions(left, registry)?;
            validate_expr_functions(right, registry)
        }
        chryso_core::ast::Expr::IsNull { expr, .. } => validate_expr_functions(expr, registry),
        chryso_core::ast::Expr::UnaryOp { expr, .. } => validate_expr_functions(expr, registry),
        chryso_core::ast::Expr::Subquery(select) | chryso_core::ast::Expr::Exists(select) => {
            for item in &select.projection {
                validate_expr_functions(&item.expr, registry)?;
            }
            Ok(())
        }
        chryso_core::ast::Expr::InSubquery { expr, subquery } => {
            validate_expr_functions(expr, registry)?;
            for item in &subquery.projection {
                validate_expr_functions(&item.expr, registry)?;
            }
            Ok(())
        }
        chryso_core::ast::Expr::Case {
            operand,
            when_then,
            else_expr,
        } => {
            if let Some(expr) = operand {
                validate_expr_functions(expr, registry)?;
            }
            for (when_expr, then_expr) in when_then {
                validate_expr_functions(when_expr, registry)?;
                validate_expr_functions(then_expr, registry)?;
            }
            if let Some(expr) = else_expr {
                validate_expr_functions(expr, registry)?;
            }
            Ok(())
        }
        chryso_core::ast::Expr::Identifier(_)
        | chryso_core::ast::Expr::Literal(_)
        | chryso_core::ast::Expr::Wildcard => Ok(()),
    }
}

#[cfg(test)]
mod function_tests {
    use super::validate_functions;
    use chryso_metadata::functions::FunctionRegistry;
    use crate::LogicalPlan;

    #[test]
    fn rejects_unknown_function() {
        let registry = FunctionRegistry::with_builtins();
        let plan = LogicalPlan::Projection {
            exprs: vec![chryso_core::ast::Expr::FunctionCall {
                name: "udf".to_string(),
                args: Vec::new(),
            }],
            input: Box::new(LogicalPlan::Scan {
                table: "t".to_string(),
            }),
        };
        let err = validate_functions(&plan, &registry).expect_err("should fail");
        assert!(err.to_string().contains("unknown function"));
    }
}

#[cfg(test)]
mod resolver_tests {
    use super::{NameResolver, Validator};
    use chryso_metadata::catalog::{Catalog, MockCatalog, TableSchema};
    use crate::LogicalPlan;

    #[test]
    fn resolve_known_table() {
        let mut catalog = MockCatalog::new();
        catalog.add_table("users", TableSchema { columns: Vec::new() });
        let plan = LogicalPlan::Scan {
            table: "users".to_string(),
        };
        let resolver = NameResolver::new(&catalog);
        assert!(resolver.validate(&plan).is_ok());
        assert!(catalog.has_table("users"));
    }
}
