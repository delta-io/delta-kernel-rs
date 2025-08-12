use serde::{Deserialize, Serialize};
use std::collections::HashMap;

// note this is a subset of actual get tables response
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TablesResponse {
    pub name: String,
    pub catalog_name: String,
    pub schema_name: String,
    pub table_type: String,
    pub data_source_format: String,
    pub storage_location: String,
    pub owner: String,
    #[serde(default)]
    pub properties: HashMap<String, String>,
    pub securable_kind: String,
    pub metastore_id: String,
    pub table_id: String,
    pub schema_id: String,
    pub catalog_id: String,
}

impl TablesResponse {
    pub fn full_name(&self) -> String {
        format!("{}.{}.{}", self.catalog_name, self.schema_name, self.name)
    }

    pub fn is_delta_table(&self) -> bool {
        self.data_source_format.eq_ignore_ascii_case("delta")
    }

    pub fn is_managed_table(&self) -> bool {
        self.table_type.eq_ignore_ascii_case("managed")
    }

    pub fn is_external_table(&self) -> bool {
        self.table_type.eq_ignore_ascii_case("external")
    }
}
