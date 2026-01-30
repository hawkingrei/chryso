use chryso_core::ChrysoResult;
use chryso_core::ast::Literal;
use chryso_core::ast::{Expr, JoinType, OrderByExpr, SelectStatement, Statement};
use chryso_metadata::type_inference::TypeInferencer;

pub mod cost;
pub mod explain;
pub mod plan_diff;
pub mod serde;
pub mod validate;

pub use cost::{Cost, CostModel};
pub use explain::{
    ExplainConfig, ExplainFormatter, format_simple_logical_plan, format_simple_physical_plan,
};

#[derive(Debug, Clone)]
pub enum LogicalPlan {
    Scan {
        table: String,
    },
    IndexScan {
        table: String,
        index: String,
        predicate: Expr,
    },
    Dml {
        sql: String,
    },
    Derived {
        input: Box<LogicalPlan>,
        alias: String,
        column_aliases: Vec<String>,
    },
    Filter {
        predicate: Expr,
        input: Box<LogicalPlan>,
    },
    Projection {
        exprs: Vec<Expr>,
        input: Box<LogicalPlan>,
    },
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
    TableScan {
        table: String,
    },
    IndexScan {
        table: String,
        index: String,
        predicate: Expr,
    },
    Dml {
        sql: String,
    },
    Derived {
        input: Box<PhysicalPlan>,
        alias: String,
        column_aliases: Vec<String>,
    },
    Filter {
        predicate: Expr,
        input: Box<PhysicalPlan>,
    },
    Projection {
        exprs: Vec<Expr>,
        input: Box<PhysicalPlan>,
    },
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
    pub fn build(statement: Statement) -> ChrysoResult<LogicalPlan> {
        let statement = chryso_core::ast::normalize_statement(&statement);
        let plan = match statement {
            Statement::With(_) => Err(chryso_core::ChrysoError::new(
                "WITH is not supported in the planner yet",
            )),
            Statement::Select(select) => build_select(select),
            Statement::SetOp { .. } => Err(chryso_core::ChrysoError::new(
                "SET operations are not supported in the planner yet",
            )),
            Statement::Explain(_) => Ok(LogicalPlan::Dml {
                sql: chryso_core::sql_format::format_statement(&statement),
            }),
            Statement::CreateTable(_)
            | Statement::DropTable(_)
            | Statement::Truncate(_)
            | Statement::Analyze(_) => Ok(LogicalPlan::Dml {
                sql: chryso_core::sql_format::format_statement(&statement),
            }),
            Statement::Insert(_) | Statement::Update(_) | Statement::Delete(_) => {
                Ok(LogicalPlan::Dml {
                    sql: chryso_core::sql_format::format_statement(&statement),
                })
            }
        }?;
        Ok(simplify_plan(plan))
    }
}

fn build_select(select: SelectStatement) -> ChrysoResult<LogicalPlan> {
    let mut plan = if let Some(from) = select.from {
        build_from(from)?
    } else {
        return Ok(LogicalPlan::Dml {
            sql: chryso_core::sql_format::format_statement(&Statement::Select(select)),
        });
    };
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
        if !select.distinct_on.is_empty() {
            return Err(chryso_core::ChrysoError::new(
                "DISTINCT ON is not supported in the planner yet",
            ));
        }
        plan = LogicalPlan::Distinct {
            input: Box::new(plan),
        };
    }
    if !select.order_by.is_empty() {
        if let (Some(limit), None) = (select.limit, select.offset) {
            plan = LogicalPlan::TopN {
                order_by: select.order_by,
                limit,
                input: Box::new(plan),
            };
        } else {
            plan = LogicalPlan::Sort {
                order_by: select.order_by,
                input: Box::new(plan),
            };
        }
    }
    if select.limit.is_some() || select.offset.is_some() {
        if matches!(plan, LogicalPlan::TopN { .. }) {
            return Ok(plan);
        }
        plan = LogicalPlan::Limit {
            limit: select.limit,
            offset: select.offset,
            input: Box::new(plan),
        };
    }
    Ok(plan)
}

fn build_from(table: chryso_core::ast::TableRef) -> ChrysoResult<LogicalPlan> {
    let mut plan = match table.factor {
        chryso_core::ast::TableFactor::Table { name } => LogicalPlan::Scan { table: name },
        chryso_core::ast::TableFactor::Derived { query } => {
            let alias = table.alias.clone().unwrap_or_else(|| "derived".to_string());
            LogicalPlan::Derived {
                input: Box::new(build_query_plan(*query)?),
                alias,
                column_aliases: table.column_aliases.clone(),
            }
        }
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

fn build_query_plan(statement: Statement) -> ChrysoResult<LogicalPlan> {
    match statement {
        Statement::Select(select) => build_select(select),
        Statement::SetOp { .. } | Statement::With(_) | Statement::Explain(_) => {
            Ok(LogicalPlan::Dml {
                sql: chryso_core::sql_format::format_statement(&statement),
            })
        }
        Statement::CreateTable(_)
        | Statement::DropTable(_)
        | Statement::Truncate(_)
        | Statement::Analyze(_)
        | Statement::Insert(_)
        | Statement::Update(_)
        | Statement::Delete(_) => Err(chryso_core::ChrysoError::new(
            "subquery in FROM must be a query",
        )),
    }
}

fn simplify_plan(plan: LogicalPlan) -> LogicalPlan {
    match plan {
        LogicalPlan::Scan { .. } | LogicalPlan::IndexScan { .. } | LogicalPlan::Dml { .. } => plan,
        LogicalPlan::Derived {
            input,
            alias,
            column_aliases,
        } => LogicalPlan::Derived {
            input: Box::new(simplify_plan(*input)),
            alias,
            column_aliases,
        },
        LogicalPlan::Filter { predicate, input } => {
            let predicate = predicate.normalize();
            let input = simplify_plan(*input);
            if matches!(predicate, Expr::Literal(Literal::Bool(true))) {
                return input;
            }
            LogicalPlan::Filter {
                predicate,
                input: Box::new(input),
            }
        }
        LogicalPlan::Projection { exprs, input } => {
            let exprs = exprs
                .into_iter()
                .map(|expr| expr.normalize())
                .collect::<Vec<_>>();
            let input = simplify_plan(*input);
            if exprs.len() == 1 && matches!(exprs[0], Expr::Wildcard) {
                return input;
            }
            LogicalPlan::Projection {
                exprs,
                input: Box::new(input),
            }
        }
        LogicalPlan::Join {
            join_type,
            left,
            right,
            on,
        } => LogicalPlan::Join {
            join_type,
            left: Box::new(simplify_plan(*left)),
            right: Box::new(simplify_plan(*right)),
            on: on.normalize(),
        },
        LogicalPlan::Aggregate {
            group_exprs,
            aggr_exprs,
            input,
        } => LogicalPlan::Aggregate {
            group_exprs: group_exprs
                .into_iter()
                .map(|expr| expr.normalize())
                .collect(),
            aggr_exprs: aggr_exprs
                .into_iter()
                .map(|expr| expr.normalize())
                .collect(),
            input: Box::new(simplify_plan(*input)),
        },
        LogicalPlan::Distinct { input } => LogicalPlan::Distinct {
            input: Box::new(simplify_plan(*input)),
        },
        LogicalPlan::TopN {
            order_by,
            limit,
            input,
        } => LogicalPlan::TopN {
            order_by: order_by
                .into_iter()
                .map(|item| OrderByExpr {
                    expr: item.expr.normalize(),
                    asc: item.asc,
                    nulls_first: item.nulls_first,
                })
                .collect(),
            limit,
            input: Box::new(simplify_plan(*input)),
        },
        LogicalPlan::Sort { order_by, input } => LogicalPlan::Sort {
            order_by: order_by
                .into_iter()
                .map(|item| OrderByExpr {
                    expr: item.expr.normalize(),
                    asc: item.asc,
                    nulls_first: item.nulls_first,
                })
                .collect(),
            input: Box::new(simplify_plan(*input)),
        },
        LogicalPlan::Limit {
            limit,
            offset,
            input,
        } => LogicalPlan::Limit {
            limit,
            offset,
            input: Box::new(simplify_plan(*input)),
        },
    }
}

impl LogicalPlan {
    pub fn explain(&self, indent: usize) -> String {
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
            LogicalPlan::Derived {
                alias,
                column_aliases,
                input,
            } => format!(
                "{padding}LogicalDerived alias={alias} cols={}\n{}",
                column_aliases.join(","),
                input.explain(indent + 2)
            ),
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
            LogicalPlan::Distinct { input } => {
                format!("{padding}LogicalDistinct\n{}", input.explain(indent + 2))
            }
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

    pub fn explain_formatted(
        &self,
        config: &ExplainConfig,
        inferencer: &dyn TypeInferencer,
    ) -> String {
        let formatter = ExplainFormatter::new(config.clone());
        formatter.format_logical_plan(self, inferencer)
    }

    pub fn explain_typed(
        &self,
        indent: usize,
        inferencer: &dyn chryso_metadata::type_inference::TypeInferencer,
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
            LogicalPlan::Derived {
                alias,
                column_aliases,
                input,
            } => format!(
                "{padding}LogicalDerived alias={alias} cols={}\n{}",
                column_aliases.join(","),
                input.explain_typed(indent + 2, inferencer)
            ),
            LogicalPlan::Filter { predicate, input } => format!(
                "{padding}LogicalFilter predicate={} type={:?}\n{}",
                fmt_expr(predicate),
                inferencer.infer_expr(predicate),
                input.explain_typed(indent + 2, inferencer)
            ),
            LogicalPlan::Projection { exprs, input } => {
                let types = chryso_metadata::type_inference::expr_types(exprs, inferencer);
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
            PhysicalPlan::IndexScan {
                table,
                index,
                predicate,
            } => format!(
                "{padding}IndexScan table={table} index={index} predicate={}",
                fmt_expr(predicate)
            ),
            PhysicalPlan::Dml { sql } => format!("{padding}Dml sql={sql}"),
            PhysicalPlan::Derived {
                alias,
                column_aliases,
                input,
            } => format!(
                "{padding}Derived alias={alias} cols={}\n{}",
                column_aliases.join(","),
                input.explain(indent + 2)
            ),
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
            PhysicalPlan::Distinct { input } => {
                format!("{padding}Distinct\n{}", input.explain(indent + 2))
            }
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

    pub fn explain_costed(&self, indent: usize, cost_model: &dyn crate::cost::CostModel) -> String {
        let padding = " ".repeat(indent);
        let cost = cost_model.cost(self).0;
        match self {
            PhysicalPlan::TableScan { table } => {
                format!("{padding}TableScan table={table} cost={cost}")
            }
            PhysicalPlan::IndexScan {
                table,
                index,
                predicate,
            } => format!(
                "{padding}IndexScan table={table} index={index} predicate={} cost={cost}",
                fmt_expr(predicate)
            ),
            PhysicalPlan::Dml { sql } => format!("{padding}Dml sql={sql} cost={cost}"),
            PhysicalPlan::Derived {
                alias,
                column_aliases,
                input,
            } => format!(
                "{padding}Derived alias={alias} cols={} cost={cost}\n{}",
                column_aliases.join(","),
                input.explain_costed(indent + 2, cost_model)
            ),
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
    use super::{LogicalPlan, PlanBuilder};
    use chryso_parser::{Dialect, ParserConfig, SimpleParser, SqlParser};

    #[test]
    fn planner_simplifies_select_star() {
        let sql = "select * from t";
        let parser = SimpleParser::new(ParserConfig {
            dialect: Dialect::Postgres,
        });
        let stmt = parser.parse(sql).expect("parse");
        let logical = PlanBuilder::build(stmt).expect("plan");
        assert!(matches!(logical, LogicalPlan::Scan { .. }));
    }

    #[test]
    fn planner_removes_true_filter() {
        let sql = "select * from t where not false";
        let parser = SimpleParser::new(ParserConfig {
            dialect: Dialect::Postgres,
        });
        let stmt = parser.parse(sql).expect("parse");
        let logical = PlanBuilder::build(stmt).expect("plan");
        assert!(matches!(logical, LogicalPlan::Scan { .. }));
    }

    #[test]
    fn planner_builds_topn_for_ordered_limit() {
        let sql = "select * from t order by id limit 5";
        let parser = SimpleParser::new(ParserConfig {
            dialect: Dialect::Postgres,
        });
        let stmt = parser.parse(sql).expect("parse");
        let logical = PlanBuilder::build(stmt).expect("plan");
        assert!(matches!(logical, LogicalPlan::TopN { .. }));
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
            let mut rendered = format!("{} {dir}", fmt_expr(&item.expr));
            if let Some(nulls_first) = item.nulls_first {
                if nulls_first {
                    rendered.push_str(" nulls first");
                } else {
                    rendered.push_str(" nulls last");
                }
            }
            rendered
        })
        .collect::<Vec<_>>()
        .join(", ")
}
