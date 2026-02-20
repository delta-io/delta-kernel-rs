use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt::{self, Display};

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

impl Display for TablesResponse {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "Full Name:        {}", self.full_name())?;
        writeln!(f, "Catalog:          {}", self.catalog_name)?;
        writeln!(f, "Schema:           {}", self.schema_name)?;
        writeln!(f, "Table Name:       {}", self.name)?;
        writeln!(f, "Type:             {}", self.table_type)?;
        writeln!(f, "Format:           {}", self.data_source_format)?;
        writeln!(f, "Storage Location: {}", self.storage_location)?;
        writeln!(f, "Owner:            {}", self.owner)?;
        writeln!(f, "Table ID:         {}", self.table_id)?;
        writeln!(f, "Is Delta Table:   {}", self.is_delta_table())?;
        writeln!(f, "Is Managed:       {}", self.is_managed_table())?;

        if !self.properties.is_empty() {
            writeln!(f)?;
            writeln!(f, "Properties:")?;
            for (key, value) in &self.properties {
                writeln!(f, "  {}: {}", key, value)?;
            }
        }

        Ok(())
    }
}
