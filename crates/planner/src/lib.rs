use corundum_core::ast::{Expr, JoinType, OrderByExpr, SelectStatement, Statement};
use corundum_core::CorundumResult;

pub mod cost;
pub mod plan_diff;
pub mod serde;
pub mod validate;

pub use cost::{Cost, CostModel};

#[derive(Debug, Clone)]
pub enum LogicalPlan {
    Scan { table: String },
    IndexScan { table: String, index: String, predicate: Expr },
    Dml { sql: String },
    Filter { predicate: Expr, input: Box<LogicalPlan> },
    Projection { exprs: Vec<Expr>, input: Box<LogicalPlan> },
    Join {
        join_type: JoinType,
        left: Box<LogicalPlan>,
        right: Box<LogicalPlan>,
        on: Expr,
    },
    Aggregate {
        group_exprs: Vec<Expr>,
        aggr_exprs: Vec<Expr>,
        input: Box<LogicalPlan>,
    },
    Distinct {
        input: Box<LogicalPlan>,
    },
    TopN {
        order_by: Vec<OrderByExpr>,
        limit: u64,
        input: Box<LogicalPlan>,
    },
    Sort {
        order_by: Vec<OrderByExpr>,
        input: Box<LogicalPlan>,
    },
    Limit {
        limit: Option<u64>,
        offset: Option<u64>,
        input: Box<LogicalPlan>,
    },
}

#[derive(Debug, Clone)]
pub enum PhysicalPlan {
    TableScan { table: String },
    IndexScan { table: String, index: String, predicate: Expr },
    Dml { sql: String },
    Filter { predicate: Expr, input: Box<PhysicalPlan> },
    Projection { exprs: Vec<Expr>, input: Box<PhysicalPlan> },
    Join {
        join_type: JoinType,
        algorithm: JoinAlgorithm,
        left: Box<PhysicalPlan>,
        right: Box<PhysicalPlan>,
        on: Expr,
    },
    Aggregate {
        group_exprs: Vec<Expr>,
        aggr_exprs: Vec<Expr>,
        input: Box<PhysicalPlan>,
    },
    Distinct {
        input: Box<PhysicalPlan>,
    },
    TopN {
        order_by: Vec<OrderByExpr>,
        limit: u64,
        input: Box<PhysicalPlan>,
    },
    Sort {
        order_by: Vec<OrderByExpr>,
        input: Box<PhysicalPlan>,
    },
    Limit {
        limit: Option<u64>,
        offset: Option<u64>,
        input: Box<PhysicalPlan>,
    },
}

pub struct PlanBuilder;

impl PlanBuilder {
    pub fn build(statement: Statement) -> CorundumResult<LogicalPlan> {
        let statement = corundum_core::ast::normalize_statement(&statement);
        match statement {
            Statement::Select(select) => build_select(select),
            Statement::Explain(_) => Err(corundum_core::CorundumError::new(
                "EXPLAIN is not supported in the planner yet",
            )),
            Statement::CreateTable(_) => Err(corundum_core::CorundumError::new(
                "CREATE TABLE is not supported in the planner yet",
            )),
            Statement::Analyze(_) => Err(corundum_core::CorundumError::new(
                "ANALYZE is not supported in the planner yet",
            )),
            Statement::Insert(_)
            | Statement::Update(_)
            | Statement::Delete(_) => Ok(LogicalPlan::Dml {
                sql: corundum_core::sql_format::format_statement(&statement),
            }),
        }
    }
}

fn build_select(select: SelectStatement) -> CorundumResult<LogicalPlan> {
    let mut plan = build_from(select.from)?;
    if let Some(predicate) = select.selection {
        plan = LogicalPlan::Filter {
            predicate,
            input: Box::new(plan),
        };
    }
    let projection_exprs = select
        .projection
        .into_iter()
        .map(|item| item.expr)
        .collect::<Vec<_>>();
    let has_aggregate = projection_exprs.iter().any(expr_is_aggregate);
    if has_aggregate || !select.group_by.is_empty() {
        let aggr_exprs = projection_exprs.clone();
        plan = LogicalPlan::Aggregate {
            group_exprs: select.group_by,
            aggr_exprs,
            input: Box::new(plan),
        };
        if let Some(having) = select.having {
            plan = LogicalPlan::Filter {
                predicate: having,
                input: Box::new(plan),
            };
        }
    }
    plan = LogicalPlan::Projection {
        exprs: projection_exprs,
        input: Box::new(plan),
    };
    if select.distinct {
        plan = LogicalPlan::Distinct {
            input: Box::new(plan),
        };
    }
    if !select.order_by.is_empty() {
        plan = LogicalPlan::Sort {
            order_by: select.order_by,
            input: Box::new(plan),
        };
    }
    if select.limit.is_some() || select.offset.is_some() {
        plan = LogicalPlan::Limit {
            limit: select.limit,
            offset: select.offset,
            input: Box::new(plan),
        };
    }
    Ok(plan)
}

fn build_from(table: corundum_core::ast::TableRef) -> CorundumResult<LogicalPlan> {
    let mut plan = LogicalPlan::Scan {
        table: table.name,
    };
    for join in table.joins {
        let right = build_from(join.right)?;
        plan = LogicalPlan::Join {
            join_type: join.join_type,
            left: Box::new(plan),
            right: Box::new(right),
            on: join.on,
        };
    }
    Ok(plan)
}

impl LogicalPlan {
    pub fn explain(&self, indent: usize) -> String {
        let padding = " ".repeat(indent);
        match self {
            LogicalPlan::Scan { table } => format!("{padding}LogicalScan table={table}"),
            LogicalPlan::IndexScan { table, index, predicate } => format!(
                "{padding}LogicalIndexScan table={table} index={index} predicate={}",
                fmt_expr(predicate)
            ),
            LogicalPlan::Dml { sql } => format!("{padding}LogicalDml sql={sql}"),
            LogicalPlan::Filter { predicate, input } => format!(
                "{padding}LogicalFilter predicate={}\n{}",
                fmt_expr(predicate),
                input.explain(indent + 2)
            ),
            LogicalPlan::Projection { exprs, input } => format!(
                "{padding}LogicalProject exprs={}\n{}",
                fmt_expr_list(exprs),
                input.explain(indent + 2)
            ),
            LogicalPlan::Join {
                join_type,
                left,
                right,
                on,
            } => format!(
                "{padding}LogicalJoin type={join_type:?} on={}\n{}\n{}",
                fmt_expr(on),
                left.explain(indent + 2),
                right.explain(indent + 2)
            ),
            LogicalPlan::Aggregate {
                group_exprs,
                aggr_exprs,
                input,
            } => format!(
                "{padding}LogicalAggregate group={} aggr={}\n{}",
                fmt_expr_list(group_exprs),
                fmt_expr_list(aggr_exprs),
                input.explain(indent + 2)
            ),
            LogicalPlan::Distinct { input } => format!(
                "{padding}LogicalDistinct\n{}",
                input.explain(indent + 2)
            ),
            LogicalPlan::TopN {
                order_by,
                limit,
                input,
            } => format!(
                "{padding}LogicalTopN order_by={} limit={limit}\n{}",
                fmt_order_by_list(order_by),
                input.explain(indent + 2)
            ),
            LogicalPlan::Sort { order_by, input } => format!(
                "{padding}LogicalSort order_by={}\n{}",
                fmt_order_by_list(order_by),
                input.explain(indent + 2)
            ),
            LogicalPlan::Limit {
                limit,
                offset,
                input,
            } => format!(
                "{padding}LogicalLimit limit={limit:?} offset={offset:?}\n{}",
                input.explain(indent + 2)
            ),
        }
    }

    pub fn explain_typed(
        &self,
        indent: usize,
        inferencer: &dyn corundum_metadata::type_inference::TypeInferencer,
    ) -> String {
        let padding = " ".repeat(indent);
        match self {
            LogicalPlan::Scan { table } => format!("{padding}LogicalScan table={table}"),
            LogicalPlan::IndexScan {
                table,
                index,
                predicate,
            } => format!(
                "{padding}LogicalIndexScan table={table} index={index} predicate={}",
                fmt_expr(predicate)
            ),
            LogicalPlan::Dml { sql } => format!("{padding}LogicalDml sql={sql}"),
            LogicalPlan::Filter { predicate, input } => format!(
                "{padding}LogicalFilter predicate={} type={:?}\n{}",
                fmt_expr(predicate),
                inferencer.infer_expr(predicate),
                input.explain_typed(indent + 2, inferencer)
            ),
            LogicalPlan::Projection { exprs, input } => {
                let types = corundum_metadata::type_inference::expr_types(exprs, inferencer);
                format!(
                    "{padding}LogicalProject exprs={} types={types:?}\n{}",
                    fmt_expr_list(exprs),
                    input.explain_typed(indent + 2, inferencer)
                )
            }
            LogicalPlan::Join {
                join_type,
                left,
                right,
                on,
            } => format!(
                "{padding}LogicalJoin type={join_type:?} on={}\n{}\n{}",
                fmt_expr(on),
                left.explain_typed(indent + 2, inferencer),
                right.explain_typed(indent + 2, inferencer)
            ),
            LogicalPlan::Aggregate {
                group_exprs,
                aggr_exprs,
                input,
            } => format!(
                "{padding}LogicalAggregate group={} aggr={}\n{}",
                fmt_expr_list(group_exprs),
                fmt_expr_list(aggr_exprs),
                input.explain_typed(indent + 2, inferencer)
            ),
            LogicalPlan::Distinct { input } => format!(
                "{padding}LogicalDistinct\n{}",
                input.explain_typed(indent + 2, inferencer)
            ),
            LogicalPlan::TopN {
                order_by,
                limit,
                input,
            } => format!(
                "{padding}LogicalTopN order_by={} limit={limit}\n{}",
                fmt_order_by_list(order_by),
                input.explain_typed(indent + 2, inferencer)
            ),
            LogicalPlan::Sort { order_by, input } => format!(
                "{padding}LogicalSort order_by={}\n{}",
                fmt_order_by_list(order_by),
                input.explain_typed(indent + 2, inferencer)
            ),
            LogicalPlan::Limit {
                limit,
                offset,
                input,
            } => format!(
                "{padding}LogicalLimit limit={limit:?} offset={offset:?}\n{}",
                input.explain_typed(indent + 2, inferencer)
            ),
        }
    }
}

impl PhysicalPlan {
    pub fn explain(&self, indent: usize) -> String {
        let padding = " ".repeat(indent);
        match self {
            PhysicalPlan::TableScan { table } => format!("{padding}TableScan table={table}"),
            PhysicalPlan::IndexScan { table, index, predicate } => format!(
                "{padding}IndexScan table={table} index={index} predicate={}",
                fmt_expr(predicate)
            ),
            PhysicalPlan::Dml { sql } => format!("{padding}Dml sql={sql}"),
            PhysicalPlan::Filter { predicate, input } => format!(
                "{padding}Filter predicate={}\n{}",
                fmt_expr(predicate),
                input.explain(indent + 2)
            ),
            PhysicalPlan::Projection { exprs, input } => format!(
                "{padding}Project exprs={}\n{}",
                fmt_expr_list(exprs),
                input.explain(indent + 2)
            ),
            PhysicalPlan::Join {
                join_type,
                algorithm,
                left,
                right,
                on,
            } => format!(
                "{padding}Join type={join_type:?} algorithm={algorithm:?} on={}\n{}\n{}",
                fmt_expr(on),
                left.explain(indent + 2),
                right.explain(indent + 2)
            ),
            PhysicalPlan::Aggregate {
                group_exprs,
                aggr_exprs,
                input,
            } => format!(
                "{padding}Aggregate group={} aggr={}\n{}",
                fmt_expr_list(group_exprs),
                fmt_expr_list(aggr_exprs),
                input.explain(indent + 2)
            ),
            PhysicalPlan::Distinct { input } => format!(
                "{padding}Distinct\n{}",
                input.explain(indent + 2)
            ),
            PhysicalPlan::TopN {
                order_by,
                limit,
                input,
            } => format!(
                "{padding}TopN order_by={} limit={limit}\n{}",
                fmt_order_by_list(order_by),
                input.explain(indent + 2)
            ),
            PhysicalPlan::Sort { order_by, input } => format!(
                "{padding}Sort order_by={}\n{}",
                fmt_order_by_list(order_by),
                input.explain(indent + 2)
            ),
            PhysicalPlan::Limit {
                limit,
                offset,
                input,
            } => format!(
                "{padding}Limit limit={limit:?} offset={offset:?}\n{}",
                input.explain(indent + 2)
            ),
        }
    }

    pub fn explain_costed(
        &self,
        indent: usize,
        cost_model: &dyn crate::cost::CostModel,
    ) -> String {
        let padding = " ".repeat(indent);
        let cost = cost_model.cost(self).0;
        match self {
            PhysicalPlan::TableScan { table } => {
                format!("{padding}TableScan table={table} cost={cost}")
            }
            PhysicalPlan::IndexScan { table, index, predicate } => format!(
                "{padding}IndexScan table={table} index={index} predicate={} cost={cost}",
                fmt_expr(predicate)
            ),
            PhysicalPlan::Dml { sql } => format!("{padding}Dml sql={sql} cost={cost}"),
            PhysicalPlan::Filter { predicate, input } => format!(
                "{padding}Filter predicate={} cost={cost}\n{}",
                fmt_expr(predicate),
                input.explain_costed(indent + 2, cost_model)
            ),
            PhysicalPlan::Projection { exprs, input } => format!(
                "{padding}Project exprs={} cost={cost}\n{}",
                fmt_expr_list(exprs),
                input.explain_costed(indent + 2, cost_model)
            ),
            PhysicalPlan::Join {
                join_type,
                algorithm,
                left,
                right,
                on,
            } => format!(
                "{padding}Join type={join_type:?} algorithm={algorithm:?} on={} cost={cost}\n{}\n{}",
                fmt_expr(on),
                left.explain_costed(indent + 2, cost_model),
                right.explain_costed(indent + 2, cost_model)
            ),
            PhysicalPlan::Aggregate {
                group_exprs,
                aggr_exprs,
                input,
            } => format!(
                "{padding}Aggregate group={} aggr={} cost={cost}\n{}",
                fmt_expr_list(group_exprs),
                fmt_expr_list(aggr_exprs),
                input.explain_costed(indent + 2, cost_model)
            ),
            PhysicalPlan::Distinct { input } => format!(
                "{padding}Distinct cost={cost}\n{}",
                input.explain_costed(indent + 2, cost_model)
            ),
            PhysicalPlan::TopN {
                order_by,
                limit,
                input,
            } => format!(
                "{padding}TopN order_by={} limit={limit} cost={cost}\n{}",
                fmt_order_by_list(order_by),
                input.explain_costed(indent + 2, cost_model)
            ),
            PhysicalPlan::Sort { order_by, input } => format!(
                "{padding}Sort order_by={} cost={cost}\n{}",
                fmt_order_by_list(order_by),
                input.explain_costed(indent + 2, cost_model)
            ),
            PhysicalPlan::Limit {
                limit,
                offset,
                input,
            } => format!(
                "{padding}Limit limit={limit:?} offset={offset:?} cost={cost}\n{}",
                input.explain_costed(indent + 2, cost_model)
            ),
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub enum JoinAlgorithm {
    Hash,
    NestedLoop,
}

#[cfg(test)]
mod tests {
    use super::PlanBuilder;
    use corundum_metadata::type_inference::SimpleTypeInferencer;
    use corundum_optimizer::cost::UnitCostModel;
    use corundum_parser::{Dialect, ParserConfig, SimpleParser, SqlParser};

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

        let physical = corundum_optimizer::CascadesOptimizer::new(
            corundum_optimizer::OptimizerConfig::default(),
        )
        .optimize(&logical, &mut corundum_metadata::StatsCache::new());
        let costed = physical.explain_costed(0, &UnitCostModel);
        assert!(costed.contains("cost="));
    }
}

fn expr_is_aggregate(expr: &Expr) -> bool {
    match expr {
        Expr::FunctionCall { name, .. } => {
            matches!(
                name.to_ascii_lowercase().as_str(),
                "sum" | "count" | "avg" | "min" | "max"
            )
        }
        _ => false,
    }
}

fn fmt_expr(expr: &Expr) -> String {
    expr.to_sql()
}

fn fmt_expr_list(exprs: &[Expr]) -> String {
    exprs.iter().map(fmt_expr).collect::<Vec<_>>().join(", ")
}

fn fmt_order_by_list(order_by: &[OrderByExpr]) -> String {
    order_by
        .iter()
        .map(|item| {
            let dir = if item.asc { "asc" } else { "desc" };
            format!("{} {dir}", fmt_expr(&item.expr))
        })
        .collect::<Vec<_>>()
        .join(", ")
}
