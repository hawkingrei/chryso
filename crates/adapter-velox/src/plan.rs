use chryso_planner::PhysicalPlan;
use serde_json::{json, Value};

pub fn plan_to_ir(plan: &PhysicalPlan) -> String {
    plan_to_json(plan).to_string()
}

fn plan_to_json(plan: &PhysicalPlan) -> Value {
    match plan {
        PhysicalPlan::TableScan { table } => json!({
            "type": "TableScan",
            "table": table,
        }),
        PhysicalPlan::IndexScan {
            table,
            index,
            predicate,
        } => json!({
            "type": "IndexScan",
            "table": table,
            "index": index,
            "predicate": predicate.to_sql(),
        }),
        PhysicalPlan::Dml { sql } => json!({
            "type": "Dml",
            "sql": sql,
        }),
        PhysicalPlan::Derived {
            input,
            alias,
            column_aliases,
        } => json!({
            "type": "Derived",
            "alias": alias,
            "columns": column_aliases,
            "input": plan_to_json(input),
        }),
        PhysicalPlan::Filter { predicate, input } => json!({
            "type": "Filter",
            "predicate": predicate.to_sql(),
            "input": plan_to_json(input),
        }),
        PhysicalPlan::Projection { exprs, input } => {
            let exprs_sql: Vec<String> = exprs.iter().map(|expr| expr.to_sql()).collect();
            json!({
                "type": "Projection",
                "exprs": exprs_sql,
                "input": plan_to_json(input),
            })
        }
        PhysicalPlan::Join {
            join_type,
            algorithm,
            left,
            right,
            on,
        } => json!({
            "type": "Join",
            "join_type": format!("{join_type:?}"),
            "algorithm": format!("{algorithm:?}"),
            "on": on.to_sql(),
            "left": plan_to_json(left),
            "right": plan_to_json(right),
        }),
        PhysicalPlan::Aggregate {
            group_exprs,
            aggr_exprs,
            input,
        } => {
            let group_sql: Vec<String> =
                group_exprs.iter().map(|expr| expr.to_sql()).collect();
            let aggr_sql: Vec<String> =
                aggr_exprs.iter().map(|expr| expr.to_sql()).collect();
            json!({
                "type": "Aggregate",
                "group_exprs": group_sql,
                "aggr_exprs": aggr_sql,
                "input": plan_to_json(input),
            })
        }
        PhysicalPlan::Distinct { input } => json!({
            "type": "Distinct",
            "input": plan_to_json(input),
        }),
        PhysicalPlan::TopN {
            order_by,
            limit,
            input,
        } => {
            let order_by_sql: Vec<String> =
                order_by.iter().map(|item| item.expr.to_sql()).collect();
            json!({
                "type": "TopN",
                "limit": limit,
                "order_by": order_by_sql,
                "input": plan_to_json(input),
            })
        }
        PhysicalPlan::Sort { order_by, input } => {
            let order_by_sql: Vec<String> =
                order_by.iter().map(|item| item.expr.to_sql()).collect();
            json!({
                "type": "Sort",
                "order_by": order_by_sql,
                "input": plan_to_json(input),
            })
        }
        PhysicalPlan::Limit {
            limit,
            offset,
            input,
        } => json!({
            "type": "Limit",
            "limit": limit,
            "offset": offset,
            "input": plan_to_json(input),
        }),
    }
}
