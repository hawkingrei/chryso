use std::cell::RefCell;
use std::collections::HashMap;

#[derive(Debug, Clone)]
pub struct StatsCache {
    table_stats: HashMap<String, TableStats>,
    column_stats: HashMap<(String, String), ColumnStats>,
    table_hits: RefCell<HashMap<String, usize>>,
    column_hits: RefCell<HashMap<(String, String), usize>>,
    max_table_entries: usize,
    max_column_entries: usize,
}

impl StatsCache {
    pub fn new() -> Self {
        Self {
            table_stats: HashMap::new(),
            column_stats: HashMap::new(),
            table_hits: RefCell::new(HashMap::new()),
            column_hits: RefCell::new(HashMap::new()),
            max_table_entries: 128,
            max_column_entries: 1024,
        }
    }

    pub fn new_with_capacity(max_table_entries: usize, max_column_entries: usize) -> Self {
        Self {
            table_stats: HashMap::new(),
            column_stats: HashMap::new(),
            table_hits: RefCell::new(HashMap::new()),
            column_hits: RefCell::new(HashMap::new()),
            max_table_entries,
            max_column_entries,
        }
    }

    pub fn set_capacity(&mut self, max_table_entries: usize, max_column_entries: usize) {
        self.max_table_entries = max_table_entries;
        self.max_column_entries = max_column_entries;
        self.evict_tables_if_needed();
        self.evict_columns_if_needed();
    }

    pub fn is_empty(&self) -> bool {
        self.table_stats.is_empty() && self.column_stats.is_empty()
    }

    pub fn insert_table_stats(&mut self, table: impl Into<String>, stats: TableStats) {
        let table = table.into();
        self.table_stats.insert(table.clone(), stats);
        self.table_hits.borrow_mut().entry(table).or_insert(0);
        self.evict_tables_if_needed();
    }

    pub fn table_stats(&self, table: &str) -> Option<&TableStats> {
        if self.table_stats.contains_key(table) {
            let mut hits = self.table_hits.borrow_mut();
            let entry = hits.entry(table.to_string()).or_insert(0);
            *entry += 1;
        }
        self.table_stats.get(table)
    }

    pub fn insert_column_stats(
        &mut self,
        table: impl Into<String>,
        column: impl Into<String>,
        stats: ColumnStats,
    ) {
        let key = (table.into(), column.into());
        self.column_stats.insert(key.clone(), stats);
        self.column_hits.borrow_mut().entry(key).or_insert(0);
        self.evict_columns_if_needed();
    }

    pub fn column_stats(&self, table: &str, column: &str) -> Option<&ColumnStats> {
        let key = (table.to_string(), column.to_string());
        if self.column_stats.contains_key(&key) {
            let mut hits = self.column_hits.borrow_mut();
            let entry = hits.entry(key.clone()).or_insert(0);
            *entry += 1;
        }
        self.column_stats.get(&key)
    }

    fn evict_tables_if_needed(&mut self) {
        while self.table_stats.len() > self.max_table_entries {
            let key = {
                let hits = self.table_hits.borrow();
                hits.iter()
                    .min_by_key(|(_, hits)| *hits)
                    .map(|(key, _)| key.clone())
            };
            if let Some(key) = key {
                self.table_stats.remove(&key);
                self.table_hits.borrow_mut().remove(&key);
            } else {
                break;
            }
        }
    }

    fn evict_columns_if_needed(&mut self) {
        while self.column_stats.len() > self.max_column_entries {
            let key = {
                let hits = self.column_hits.borrow();
                hits.iter()
                    .min_by_key(|(_, hits)| *hits)
                    .map(|(key, _)| key.clone())
            };
            if let Some(key) = key {
                self.column_stats.remove(&key);
                self.column_hits.borrow_mut().remove(&key);
            } else {
                break;
            }
        }
    }
}

impl Default for StatsCache {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct TableStats {
    pub row_count: f64,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ColumnStats {
    pub distinct_count: f64,
    pub null_fraction: f64,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct StatsSnapshot {
    pub tables: Vec<(String, TableStats)>,
    pub columns: Vec<(String, String, ColumnStats)>,
}

impl StatsSnapshot {
    pub fn to_cache(&self) -> StatsCache {
        let mut cache = StatsCache::new_with_capacity(self.tables.len(), self.columns.len());
        for (table, stats) in &self.tables {
            cache.insert_table_stats(table.clone(), stats.clone());
        }
        for (table, column, stats) in &self.columns {
            cache.insert_column_stats(table.clone(), column.clone(), stats.clone());
        }
        cache
    }

    pub fn from_cache(cache: &StatsCache) -> Self {
        let mut tables = cache
            .table_stats
            .iter()
            .map(|(name, stats)| (name.clone(), stats.clone()))
            .collect::<Vec<_>>();
        let mut columns = cache
            .column_stats
            .iter()
            .map(|((table, column), stats)| (table.clone(), column.clone(), stats.clone()))
            .collect::<Vec<_>>();
        tables.sort_by(|(a, _), (b, _)| a.cmp(b));
        columns.sort_by(|(ta, ca, _), (tb, cb, _)| (ta, ca).cmp(&(tb, cb)));
        Self { tables, columns }
    }

    pub fn load_json(path: impl AsRef<std::path::Path>) -> chryso_core::error::ChrysoResult<Self> {
        let content = std::fs::read_to_string(path.as_ref()).map_err(|err| {
            chryso_core::error::ChrysoError::new(format!("read stats snapshot failed: {err}"))
        })?;
        let snapshot = serde_json::from_str(&content).map_err(|err| {
            chryso_core::error::ChrysoError::new(format!("parse stats snapshot failed: {err}"))
        })?;
        Ok(snapshot)
    }

    pub fn write_json(
        &self,
        path: impl AsRef<std::path::Path>,
    ) -> chryso_core::error::ChrysoResult<()> {
        let content = serde_json::to_string_pretty(self).map_err(|err| {
            chryso_core::error::ChrysoError::new(format!("serialize stats snapshot failed: {err}"))
        })?;
        std::fs::write(path.as_ref(), format!("{content}\n")).map_err(|err| {
            chryso_core::error::ChrysoError::new(format!("write stats snapshot failed: {err}"))
        })?;
        Ok(())
    }
}

pub trait StatsProvider {
    fn load_stats(
        &self,
        tables: &[String],
        columns: &[(String, String)],
        cache: &mut StatsCache,
    ) -> chryso_core::ChrysoResult<()>;
}

pub mod analyze;
pub mod catalog;
pub mod functions;
pub mod type_coercion;
pub mod type_inference;
pub mod types;

#[cfg(test)]
mod catalog_tests;

#[cfg(test)]
mod tests {
    use super::{ColumnStats, StatsCache, StatsSnapshot, TableStats};

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

    #[test]
    fn stats_cache_lfu_eviction() {
        let mut cache = StatsCache::new_with_capacity(1, 1);
        cache.insert_table_stats("t1", TableStats { row_count: 1.0 });
        cache.insert_table_stats("t2", TableStats { row_count: 2.0 });
        assert!(cache.table_stats("t1").is_none() || cache.table_stats("t2").is_none());

        cache.insert_column_stats(
            "t1",
            "c1",
            ColumnStats {
                distinct_count: 1.0,
                null_fraction: 0.0,
            },
        );
        cache.insert_column_stats(
            "t1",
            "c2",
            ColumnStats {
                distinct_count: 2.0,
                null_fraction: 0.0,
            },
        );
        let c1 = cache.column_stats("t1", "c1").is_some();
        let c2 = cache.column_stats("t1", "c2").is_some();
        assert!(c1 ^ c2);
    }

    #[test]
    fn stats_snapshot_is_sorted() {
        let mut cache = StatsCache::new();
        cache.insert_table_stats("b", TableStats { row_count: 2.0 });
        cache.insert_table_stats("a", TableStats { row_count: 1.0 });
        cache.insert_column_stats(
            "b",
            "y",
            ColumnStats {
                distinct_count: 2.0,
                null_fraction: 0.0,
            },
        );
        cache.insert_column_stats(
            "a",
            "z",
            ColumnStats {
                distinct_count: 3.0,
                null_fraction: 0.0,
            },
        );
        cache.insert_column_stats(
            "a",
            "b",
            ColumnStats {
                distinct_count: 4.0,
                null_fraction: 0.0,
            },
        );

        let snapshot = StatsSnapshot::from_cache(&cache);
        let tables = snapshot
            .tables
            .iter()
            .map(|(name, _)| name.as_str())
            .collect::<Vec<_>>();
        let columns = snapshot
            .columns
            .iter()
            .map(|(table, column, _)| (table.as_str(), column.as_str()))
            .collect::<Vec<_>>();

        assert_eq!(tables, vec!["a", "b"]);
        assert_eq!(columns, vec![("a", "b"), ("a", "z"), ("b", "y")]);
    }
}
