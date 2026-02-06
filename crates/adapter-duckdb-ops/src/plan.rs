use chryso_core::ast::{JoinType, OrderByExpr};
use chryso_core::error::{ChrysoError, ChrysoResult};
use chryso_planner::PhysicalPlan;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct PlanIr {
    pub nodes: Vec<PlanNode>,
    pub root: usize,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum PlanNode {
    TableScan {
        table: String,
    },
    IndexScan {
        table: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        index: Option<String>,
        predicate: String,
    },
    Dml {
        sql: String,
    },
    Derived {
        input: usize,
        alias: String,
        column_aliases: Vec<String>,
    },
    Filter {
        predicate: String,
        input: usize,
    },
    Projection {
        exprs: Vec<String>,
        input: usize,
    },
    Join {
        join_type: String,
        left: usize,
        right: usize,
        on: String,
    },
    Aggregate {
        group_exprs: Vec<String>,
        aggr_exprs: Vec<String>,
        input: usize,
    },
    Distinct {
        input: usize,
    },
    TopN {
        order_by: Vec<OrderItem>,
        limit: u64,
        input: usize,
    },
    Sort {
        order_by: Vec<OrderItem>,
        input: usize,
    },
    Limit {
        limit: Option<u64>,
        offset: Option<u64>,
        input: usize,
    },
}

#[derive(Debug, Serialize, Deserialize)]
pub struct OrderItem {
    pub expr: String,
    pub asc: bool,
    pub nulls_first: Option<bool>,
}

pub fn plan_to_bytes(plan: &PhysicalPlan) -> ChrysoResult<Vec<u8>> {
    let mut nodes = Vec::new();
    let root = lower_plan(plan, &mut nodes);
    let ir = PlanIr { nodes, root };
    serde_json::to_vec(&ir)
        .map_err(|err| ChrysoError::new(format!("duckdb ops serialize plan failed: {err}")))
}

fn lower_plan(plan: &PhysicalPlan, nodes: &mut Vec<PlanNode>) -> usize {
    match plan {
        PhysicalPlan::TableScan { table } => push_node(
            nodes,
            PlanNode::TableScan {
                table: table.clone(),
            },
        ),
        PhysicalPlan::IndexScan {
            table,
            index,
            predicate,
        } => push_node(
            nodes,
            PlanNode::IndexScan {
                table: table.clone(),
                index: Some(index.clone()),
                predicate: predicate.to_sql(),
            },
        ),
        PhysicalPlan::Dml { sql } => push_node(nodes, PlanNode::Dml { sql: sql.clone() }),
        PhysicalPlan::Derived {
            input,
            alias,
            column_aliases,
        } => {
            let input = lower_plan(input, nodes);
            push_node(
                nodes,
                PlanNode::Derived {
                    input,
                    alias: alias.clone(),
                    column_aliases: column_aliases.clone(),
                },
            )
        }
        PhysicalPlan::Filter { predicate, input } => {
            let input = lower_plan(input, nodes);
            push_node(
                nodes,
                PlanNode::Filter {
                    predicate: predicate.to_sql(),
                    input,
                },
            )
        }
        PhysicalPlan::Projection { exprs, input } => {
            let input_is_aggregate = matches!(input.as_ref(), PhysicalPlan::Aggregate { .. });
            let input = lower_plan(input, nodes);
            let exprs = exprs
                .iter()
                .map(|expr| {
                    let raw = expr.to_sql();
                    if input_is_aggregate {
                        quote_ident(&raw)
                    } else {
                        raw
                    }
                })
                .collect();
            push_node(nodes, PlanNode::Projection { exprs, input })
        }
        PhysicalPlan::Join {
            join_type,
            left,
            right,
            on,
            ..
        } => {
            let left = lower_plan(left, nodes);
            let right = lower_plan(right, nodes);
            push_node(
                nodes,
                PlanNode::Join {
                    join_type: join_type_label(*join_type),
                    left,
                    right,
                    on: on.to_sql(),
                },
            )
        }
        PhysicalPlan::Aggregate {
            group_exprs,
            aggr_exprs,
            input,
        } => {
            let input = lower_plan(input, nodes);
            let group_exprs = group_exprs.iter().map(|expr| expr.to_sql()).collect();
            let aggr_exprs = aggr_exprs.iter().map(|expr| expr.to_sql()).collect();
            push_node(
                nodes,
                PlanNode::Aggregate {
                    group_exprs,
                    aggr_exprs,
                    input,
                },
            )
        }
        PhysicalPlan::Distinct { input } => {
            let input = lower_plan(input, nodes);
            push_node(nodes, PlanNode::Distinct { input })
        }
        PhysicalPlan::TopN {
            order_by,
            limit,
            input,
        } => {
            let input = lower_plan(input, nodes);
            let order_by = lower_order_by(order_by);
            push_node(
                nodes,
                PlanNode::TopN {
                    order_by,
                    limit: *limit,
                    input,
                },
            )
        }
        PhysicalPlan::Sort { order_by, input } => {
            let input = lower_plan(input, nodes);
            let order_by = lower_order_by(order_by);
            push_node(nodes, PlanNode::Sort { order_by, input })
        }
        PhysicalPlan::Limit {
            limit,
            offset,
            input,
        } => {
            let input = lower_plan(input, nodes);
            push_node(
                nodes,
                PlanNode::Limit {
                    limit: *limit,
                    offset: *offset,
                    input,
                },
            )
        }
    }
}

fn push_node(nodes: &mut Vec<PlanNode>, node: PlanNode) -> usize {
    nodes.push(node);
    nodes.len() - 1
}

fn lower_order_by(order_by: &[OrderByExpr]) -> Vec<OrderItem> {
    order_by
        .iter()
        .map(|item| OrderItem {
            expr: item.expr.to_sql(),
            asc: item.asc,
            nulls_first: item.nulls_first,
        })
        .collect()
}

fn join_type_label(join_type: JoinType) -> String {
    match join_type {
        JoinType::Inner => "inner".to_string(),
        JoinType::Left => "left".to_string(),
        JoinType::Right => "right".to_string(),
        JoinType::Full => "full".to_string(),
    }
}

fn quote_ident(value: &str) -> String {
    let mut out = String::with_capacity(value.len() + 2);
    out.push('"');
    for ch in value.chars() {
        if ch == '"' {
            out.push('"');
        }
        out.push(ch);
    }
    out.push('"');
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use chryso_core::ast::{BinaryOperator, Expr, Literal};

    #[test]
    fn plan_to_bytes_roundtrip() {
        let predicate = Expr::BinaryOp {
            left: Box::new(Expr::Identifier("id".to_string())),
            op: BinaryOperator::Eq,
            right: Box::new(Expr::Literal(Literal::Number(1.0))),
        };
        let plan = PhysicalPlan::Filter {
            predicate,
            input: Box::new(PhysicalPlan::TableScan {
                table: "users".to_string(),
            }),
        };
        let bytes = plan_to_bytes(&plan).expect("serialize");
        let decoded: PlanIr = serde_json::from_slice(&bytes).expect("deserialize");
        assert_eq!(decoded.nodes.len(), 2);
        assert_eq!(decoded.root, 1);
    }
}
