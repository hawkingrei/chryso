use crate::{LogicalPlan, PhysicalPlan};

pub struct PlanDiff {
    pub left: String,
    pub right: String,
}

pub fn diff_logical(left: &LogicalPlan, right: &LogicalPlan) -> PlanDiff {
    PlanDiff {
        left: left.explain(0),
        right: right.explain(0),
    }
}

pub fn diff_physical(left: &PhysicalPlan, right: &PhysicalPlan) -> PlanDiff {
    PlanDiff {
        left: left.explain(0),
        right: right.explain(0),
    }
}

#[cfg(test)]
mod tests {
    use super::diff_logical;
    use crate::LogicalPlan;

    #[test]
    fn diff_logical_outputs_explain() {
        let left = LogicalPlan::Scan {
            table: "t1".to_string(),
        };
        let right = LogicalPlan::Scan {
            table: "t2".to_string(),
        };
        let diff = diff_logical(&left, &right);
        assert!(diff.left.contains("t1"));
        assert!(diff.right.contains("t2"));
    }
}
