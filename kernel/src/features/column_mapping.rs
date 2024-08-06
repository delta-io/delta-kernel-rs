//! Code to handle column mapping, including modes and schema transforms
use std::str::FromStr;

use serde::{Deserialize, Serialize};

use crate::{DeltaResult, Error};

/// Modes of column mapping a table can be in
#[derive(Serialize, Deserialize, Debug, Copy, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub enum ColumnMappingMode {
    /// No column mapping is applied
    None,
    /// Columns are mapped by their field_id in parquet
    Id,
    /// Columns are mapped to a physical name
    Name,
}

// key to look in metadata.configuration for to get column mapping mode
pub(crate) const COLUMN_MAPPING_MODE_KEY: &str = "delta.columnMapping.mode";

impl TryFrom<&str> for ColumnMappingMode {
    type Error = Error;

    fn try_from(s: &str) -> DeltaResult<Self> {
        match s.to_ascii_lowercase().as_str() {
            "none" => Ok(Self::None),
            "id" => Ok(Self::Id),
            "name" => Ok(Self::Name),
            _ => Err(Error::invalid_column_mapping_mode(s)),
        }
    }
}

impl FromStr for ColumnMappingMode {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        s.try_into()
    }
}

impl Default for ColumnMappingMode {
    fn default() -> Self {
        Self::None
    }
}

impl AsRef<str> for ColumnMappingMode {
    fn as_ref(&self) -> &str {
        match self {
            Self::None => "none",
            Self::Id => "id",
            Self::Name => "name",
        }
    }
}
