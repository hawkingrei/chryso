use super::catalog::{Catalog, ColumnSchema, MockCatalog, TableSchema};
use super::TableStats;

#[test]
fn mock_catalog_roundtrip() {
    let mut catalog = MockCatalog::new();
    catalog.add_table(
        "users",
        TableSchema {
            columns: vec![ColumnSchema {
                name: "id".to_string(),
                data_type: "int".to_string(),
            }],
        },
    );
    catalog.add_table_stats("users", TableStats { row_count: 12.0 });
    let schema = catalog.table_schema("users").expect("schema");
    assert_eq!(schema.columns.len(), 1);
    let stats = catalog.table_stats("users").expect("stats");
    assert_eq!(stats.row_count, 12.0);
    assert!(catalog.has_table("users"));
}
