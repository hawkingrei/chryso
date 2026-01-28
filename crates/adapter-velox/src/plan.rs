use chryso_planner::PhysicalPlan;

pub fn plan_to_ir(plan: &PhysicalPlan) -> String {
    match plan {
        PhysicalPlan::TableScan { table } => {
            format!(r#"{{"type":"TableScan","table":"{table}"}}"#)
        }
        PhysicalPlan::IndexScan {
            table,
            index,
            predicate,
        } => format!(
            r#"{{"type":"IndexScan","table":"{table}","index":"{index}","predicate":"{}"}}"#,
            predicate.to_sql()
        ),
        PhysicalPlan::Dml { sql } => format!(r#"{{"type":"Dml","sql":"{sql}"}}"#),
        PhysicalPlan::Derived {
            input,
            alias,
            column_aliases,
        } => format!(
            r#"{{"type":"Derived","alias":"{alias}","columns":[{}],"input":{}}}"#,
            quote_list(column_aliases),
            plan_to_ir(input)
        ),
        PhysicalPlan::Filter { predicate, input } => format!(
            r#"{{"type":"Filter","predicate":"{}","input":{}}}"#,
            predicate.to_sql(),
            plan_to_ir(input)
        ),
        PhysicalPlan::Projection { exprs, input } => format!(
            r#"{{"type":"Projection","exprs":[{}],"input":{}}}"#,
            exprs
                .iter()
                .map(|expr| format!("\"{}\"", expr.to_sql()))
                .collect::<Vec<_>>()
                .join(","),
            plan_to_ir(input)
        ),
        PhysicalPlan::Join {
            join_type,
            algorithm,
            left,
            right,
            on,
        } => format!(
            r#"{{"type":"Join","join_type":"{join_type:?}","algorithm":"{algorithm:?}","on":"{}","left":{},"right":{}}}"#,
            on.to_sql(),
            plan_to_ir(left),
            plan_to_ir(right)
        ),
        PhysicalPlan::Aggregate {
            group_exprs,
            aggr_exprs,
            input,
        } => format!(
            r#"{{"type":"Aggregate","group_exprs":[{}],"aggr_exprs":[{}],"input":{}}}"#,
            expr_list(group_exprs),
            expr_list(aggr_exprs),
            plan_to_ir(input)
        ),
        PhysicalPlan::Distinct { input } => {
            format!(r#"{{"type":"Distinct","input":{}}}"#, plan_to_ir(input))
        }
        PhysicalPlan::TopN {
            order_by,
            limit,
            input,
        } => format!(
            r#"{{"type":"TopN","limit":{limit},"order_by":[{}],"input":{}}}"#,
            order_by
                .iter()
                .map(|item| format!("\"{}\"", item.expr.to_sql()))
                .collect::<Vec<_>>()
                .join(","),
            plan_to_ir(input)
        ),
        PhysicalPlan::Sort { order_by, input } => format!(
            r#"{{"type":"Sort","order_by":[{}],"input":{}}}"#,
            order_by
                .iter()
                .map(|item| format!("\"{}\"", item.expr.to_sql()))
                .collect::<Vec<_>>()
                .join(","),
            plan_to_ir(input)
        ),
        PhysicalPlan::Limit {
            limit,
            offset,
            input,
        } => format!(
            r#"{{"type":"Limit","limit":{},"offset":{},"input":{}}}"#,
            limit.map_or("null".to_string(), |value| value.to_string()),
            offset.map_or("null".to_string(), |value| value.to_string()),
            plan_to_ir(input)
        ),
    }
}

fn expr_list(exprs: &[chryso_core::ast::Expr]) -> String {
    exprs
        .iter()
        .map(|expr| format!("\"{}\"", expr.to_sql()))
        .collect::<Vec<_>>()
        .join(",")
}

fn quote_list(values: &[String]) -> String {
    values
        .iter()
        .map(|value| format!("\"{value}\""))
        .collect::<Vec<_>>()
        .join(",")
}
