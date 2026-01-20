use crate::TableStats;

#[derive(Debug, Clone)]
pub struct TableSchema {
    pub columns: Vec<ColumnSchema>,
}

#[derive(Debug, Clone)]
pub struct ColumnSchema {
    pub name: String,
    pub data_type: String,
}

pub trait Catalog {
    fn table_schema(&self, name: &str) -> Option<TableSchema>;
    fn table_stats(&self, name: &str) -> Option<TableStats>;
    fn has_table(&self, name: &str) -> bool {
        self.table_schema(name).is_some()
    }
}

pub struct EmptyCatalog;

impl Catalog for EmptyCatalog {
    fn table_schema(&self, _name: &str) -> Option<TableSchema> {
        None
    }

    fn table_stats(&self, _name: &str) -> Option<TableStats> {
        None
    }
}

#[derive(Debug, Default)]
pub struct MockCatalog {
    tables: std::collections::HashMap<String, TableSchema>,
    stats: std::collections::HashMap<String, TableStats>,
}

impl MockCatalog {
    pub fn new() -> Self {
        Self {
            tables: std::collections::HashMap::new(),
            stats: std::collections::HashMap::new(),
        }
    }

    pub fn add_table(&mut self, name: impl Into<String>, schema: TableSchema) {
        self.tables.insert(name.into(), schema);
    }

    pub fn add_table_stats(&mut self, name: impl Into<String>, stats: TableStats) {
        self.stats.insert(name.into(), stats);
    }
}

impl Catalog for MockCatalog {
    fn table_schema(&self, name: &str) -> Option<TableSchema> {
        self.tables.get(name).cloned()
    }

    fn table_stats(&self, name: &str) -> Option<TableStats> {
        self.stats.get(name).cloned()
    }
}
