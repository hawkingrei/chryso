use chryso_planner::PhysicalPlan;
use serde_json::{Value, json};

pub fn plan_to_ir(plan: &PhysicalPlan) -> String {
    plan_to_ir_with_memory_payload(plan, &|_| None)
}

pub fn plan_to_ir_with_memory_payload<F>(plan: &PhysicalPlan, lookup: &F) -> String
where
    F: Fn(&str) -> Option<String>,
{
    plan_to_json(plan, lookup).to_string()
}

fn plan_to_json<F>(plan: &PhysicalPlan, lookup: &F) -> Value
where
    F: Fn(&str) -> Option<String>,
{
    match plan {
        PhysicalPlan::TableScan { table } => {
            let mut node = json!({
                "type": "TableScan",
                "table": table,
                "storage": "memory",
            });
            if let Some(payload) = lookup(table) {
                node["memory_payload"] = json!(payload);
            }
            node
        }
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
            "input": plan_to_json(input, lookup),
        }),
        PhysicalPlan::Filter { predicate, input } => json!({
            "type": "Filter",
            "predicate": predicate.to_sql(),
            "input": plan_to_json(input, lookup),
        }),
        PhysicalPlan::Projection { exprs, input } => {
            let exprs_sql: Vec<String> = exprs.iter().map(|expr| expr.to_sql()).collect();
            json!({
                "type": "Projection",
                "exprs": exprs_sql,
                "input": plan_to_json(input, lookup),
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
            "left": plan_to_json(left, lookup),
            "right": plan_to_json(right, lookup),
        }),
        PhysicalPlan::Aggregate {
            group_exprs,
            aggr_exprs,
            input,
        } => {
            let group_sql: Vec<String> = group_exprs.iter().map(|expr| expr.to_sql()).collect();
            let aggr_sql: Vec<String> = aggr_exprs.iter().map(|expr| expr.to_sql()).collect();
            json!({
                "type": "Aggregate",
                "group_exprs": group_sql,
                "aggr_exprs": aggr_sql,
                "input": plan_to_json(input, lookup),
            })
        }
        PhysicalPlan::Distinct { input } => json!({
            "type": "Distinct",
            "input": plan_to_json(input, lookup),
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
                "input": plan_to_json(input, lookup),
            })
        }
        PhysicalPlan::Sort { order_by, input } => {
            let order_by_sql: Vec<String> =
                order_by.iter().map(|item| item.expr.to_sql()).collect();
            json!({
                "type": "Sort",
                "order_by": order_by_sql,
                "input": plan_to_json(input, lookup),
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
            "input": plan_to_json(input, lookup),
        }),
    }
}
