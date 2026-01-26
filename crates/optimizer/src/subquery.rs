use chryso_planner::LogicalPlan;

pub fn rewrite_correlated_subqueries(plan: &LogicalPlan) -> LogicalPlan {
    plan.clone()
}

#[cfg(test)]
mod tests {
    use super::rewrite_correlated_subqueries;
    use chryso_core::ast::Expr;
    use chryso_planner::LogicalPlan;

    #[test]
    fn rewrite_correlated_subqueries_is_stable() {
        let plan = LogicalPlan::Filter {
            predicate: Expr::Exists(Box::new(chryso_core::ast::SelectStatement {
                distinct: false,
                distinct_on: Vec::new(),
                projection: vec![chryso_core::ast::SelectItem {
                    expr: Expr::Identifier("id".to_string()),
                    alias: None,
                }],
                from: None,
                selection: None,
                group_by: Vec::new(),
                having: None,
                order_by: Vec::new(),
                limit: None,
                offset: None,
            })),
            input: Box::new(LogicalPlan::Scan {
                table: "t".to_string(),
            }),
        };
        let rewritten = rewrite_correlated_subqueries(&plan);
        assert_eq!(plan.explain(0), rewritten.explain(0));
    }
}
