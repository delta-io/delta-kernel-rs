use crate::error::Result;

/// Minimal table information returned by [`GetTableClient`].
#[derive(Debug, Clone)]
pub struct TableInfo {
    /// The unique identifier of the table.
    pub table_id: String,
    /// The cloud storage location of the table (the table URI).
    pub storage_location: String,
}

/// Trait for resolving a table name to its [`TableInfo`].
#[allow(async_fn_in_trait)]
pub trait GetTableClient: Send + Sync {
    /// Resolve a fully-qualified table name (e.g. `catalog.schema.table`) to
    /// its [`TableInfo`].
    async fn get_table(&self, table_name: &str) -> Result<TableInfo>;
}
