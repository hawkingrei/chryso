use std::collections::HashMap;

#[derive(Debug, Clone, Default)]
pub struct StatsCache {
    table_stats: HashMap<String, TableStats>,
    column_stats: HashMap<(String, String), ColumnStats>,
}

impl StatsCache {
    pub fn new() -> Self {
        Self {
            table_stats: HashMap::new(),
            column_stats: HashMap::new(),
        }
    }

    pub fn insert_table_stats(&mut self, table: impl Into<String>, stats: TableStats) {
        self.table_stats.insert(table.into(), stats);
    }

    pub fn table_stats(&self, table: &str) -> Option<&TableStats> {
        self.table_stats.get(table)
    }

    pub fn insert_column_stats(
        &mut self,
        table: impl Into<String>,
        column: impl Into<String>,
        stats: ColumnStats,
    ) {
        self.column_stats
            .insert((table.into(), column.into()), stats);
    }

    pub fn column_stats(&self, table: &str, column: &str) -> Option<&ColumnStats> {
        self.column_stats.get(&(table.to_string(), column.to_string()))
    }
}

#[derive(Debug, Clone)]
pub struct TableStats {
    pub row_count: f64,
}

#[derive(Debug, Clone)]
pub struct ColumnStats {
    pub distinct_count: f64,
    pub null_fraction: f64,
}

pub mod analyze;
pub mod catalog;
pub mod functions;
pub mod types;
pub mod type_inference;
pub mod type_coercion;

#[cfg(test)]
mod catalog_tests;

#[cfg(test)]
mod tests {
    use super::{ColumnStats, StatsCache, TableStats};

    #[test]
    fn stats_cache_roundtrip() {
        let mut cache = StatsCache::new();
        cache.insert_table_stats("users", TableStats { row_count: 42.0 });
        cache.insert_column_stats(
            "users",
            "id",
            ColumnStats {
                distinct_count: 40.0,
                null_fraction: 0.0,
            },
        );
        assert_eq!(cache.table_stats("users").unwrap().row_count, 42.0);
        assert_eq!(
            cache.column_stats("users", "id").unwrap().distinct_count,
            40.0
        );
    }
}
