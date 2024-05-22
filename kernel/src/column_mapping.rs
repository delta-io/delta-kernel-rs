//! Code to handle column mapping, including modes and schema transforms

use crate::{DeltaResult, Error};
use serde::{Deserialize, Serialize};

/// Modes of column mapping a table can be in
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub enum ColumnMappingMode {
    None,
    Id,
    Name,
}

// key to look in metadata.configuration for to get column mapping mode
pub(crate) const COLUMN_MAPPING_MODE_KEY: &str = "delta.columnMapping.mode";

impl TryFrom<&str> for ColumnMappingMode {
    type Error = Error;

    fn try_from(mode: &str) -> DeltaResult<Self> {
        match mode {
            "none" => Ok(ColumnMappingMode::None),
            "id" => Ok(ColumnMappingMode::Id),
            "name" => Ok(ColumnMappingMode::Name),
            _ => Err(Error::invalid_column_mapping_mode(mode)),
        }
    }
}
