use chryso_metadata::{ColumnStats, StatsCache, TableStats};
use chryso_planner::LogicalPlan;

pub trait CardinalityEstimator {
    fn estimate(&self, plan: &LogicalPlan, stats: &StatsCache) -> f64;
}

pub struct NaiveEstimator;

impl CardinalityEstimator for NaiveEstimator {
    fn estimate(&self, plan: &LogicalPlan, stats: &StatsCache) -> f64 {
        match plan {
            LogicalPlan::Scan { table } => stats
                .table_stats(table)
                .map(|s| s.row_count)
                .unwrap_or(1000.0),
            LogicalPlan::IndexScan { .. } => 100.0,
            LogicalPlan::Dml { .. } => 1.0,
            LogicalPlan::Derived { input, .. } => self.estimate(input, stats),
            LogicalPlan::Filter { input, .. } => self.estimate(input, stats) * 0.25,
            LogicalPlan::Projection { input, .. } => self.estimate(input, stats),
            LogicalPlan::Join { left, right, .. } => {
                self.estimate(left, stats) * self.estimate(right, stats) * 0.1
            }
            LogicalPlan::Aggregate { input, .. } => self.estimate(input, stats) * 0.1,
            LogicalPlan::Distinct { input } => self.estimate(input, stats) * 0.1,
            LogicalPlan::TopN { limit, .. } => *limit as f64,
            LogicalPlan::Sort { input, .. } => self.estimate(input, stats),
            LogicalPlan::Limit { limit, .. } => limit.unwrap_or(100) as f64,
        }
    }
}

pub fn default_table_stats() -> TableStats {
    TableStats { row_count: 1000.0 }
}

pub fn default_column_stats() -> ColumnStats {
    ColumnStats {
        distinct_count: 100.0,
        null_fraction: 0.0,
    }
}

#[cfg(test)]
mod tests {
    use super::{CardinalityEstimator, NaiveEstimator};
    use chryso_metadata::{StatsCache, TableStats};
    use chryso_planner::LogicalPlan;

    #[test]
    fn estimate_scan_uses_stats() {
        let estimator = NaiveEstimator;
        let mut stats = StatsCache::new();
        stats.insert_table_stats("orders", TableStats { row_count: 500.0 });
        let plan = LogicalPlan::Scan {
            table: "orders".to_string(),
        };
        let estimate = estimator.estimate(&plan, &stats);
        assert_eq!(estimate, 500.0);
    }
}
