use crate::{ColumnStats, StatsCache, TableStats};

pub trait Analyzer {
    fn analyze(&self, table: &str) -> (TableStats, Vec<(String, ColumnStats)>);
}

pub fn apply_analyze(
    cache: &mut StatsCache,
    table: &str,
    stats: TableStats,
    columns: Vec<(String, ColumnStats)>,
) {
    cache.insert_table_stats(table, stats);
    for (column, column_stats) in columns {
        cache.insert_column_stats(table, column, column_stats);
    }
}

pub struct MockAnalyzer;

impl Analyzer for MockAnalyzer {
    fn analyze(&self, table: &str) -> (TableStats, Vec<(String, ColumnStats)>) {
        let stats = TableStats { row_count: 100.0 };
        let columns = vec![(
            "id".to_string(),
            ColumnStats {
                distinct_count: 100.0,
                null_fraction: 0.0,
            },
        )];
        let _ = table;
        (stats, columns)
    }
}

#[cfg(test)]
mod tests {
    use super::{apply_analyze, Analyzer, MockAnalyzer};
    use crate::StatsCache;

    #[test]
    fn apply_analyze_updates_cache() {
        let mut cache = StatsCache::new();
        let analyzer = MockAnalyzer;
        let (table_stats, columns) = analyzer.analyze("users");
        apply_analyze(&mut cache, "users", table_stats, columns);
        assert!(cache.table_stats("users").is_some());
        assert!(cache.column_stats("users", "id").is_some());
    }
}
